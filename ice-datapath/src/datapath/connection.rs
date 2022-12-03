use super::{
    super::{
        access, check_ok, dpdk_bindings,
        dpdk_bindings::{
            rte_eth_dev_socket_id, rte_eth_dev_start, rte_eth_rx_queue_setup,
            rte_eth_tx_queue_setup,
        },
        dpdk_mbuf_slice, ice_bindings,
        ice_bindings::custom_ice_init_tx_queues,
    },
    allocator::IceMempool,
    check, dpdk_check, dpdk_wrapper, sizes,
};
use cornflakes_libos::{
    allocator::{MemoryPoolAllocator, MempoolID},
    datapath::{Datapath, DatapathBufferOps, InlineMode, MetadataOps, ReceivedPkt},
    dynamic_rcsga_hybrid_hdr::HybridArenaRcSgaHdr,
    mem::PGSIZE_2MB,
    utils::AddressInfo,
    ConnID, CopyContext, MsgID,
};

use color_eyre::eyre::{bail, ensure, Result, WrapErr};
use cornflakes_utils::{parse_yaml_map, AppMode};
use eui48::MacAddress;
use hashbrown::HashMap;
use std::{
    boxed::Box,
    ffi::CString,
    io::Write,
    mem::MaybeUninit,
    net::Ipv4Addr,
    ptr,
    sync::Arc,
    time::{Duration, Instant},
};

const MAX_CONCURRENT_CONNECTIONS: usize = 128;
const RECEIVE_BURST_SIZE: usize = 32;
const MAX_BUFFER_SIZE: usize = 16384;
const MEMPOOL_MIN_ELTS: usize = 8192;

/// RX and TX Prefetch, Host, and Write-back threshold values should be
/// carefully set for optimal performance. Consult the network
/// controller's datasheet and supporting DPDK documentation for guidance
/// on how these parameters should be set.
const RX_PTHRESH: u8 = 8;
const RX_HTHRESH: u8 = 8;
const RX_WTHRESH: u8 = 0;

/// These default values are optimized for use with the Intel(R) 82599 10 GbE
/// Controller and the DPDK ixgbe PMD. Consider using other values for other
/// network controllers and/or network drivers.
const TX_PTHRESH: u8 = 0;
const TX_HTHRESH: u8 = 0;
const TX_WTHRESH: u8 = 0;

const RX_RING_SIZE: u16 = 2048;
const TX_RING_SIZE: u16 = 2048;
#[derive(PartialEq, Eq)]
pub struct IceBuffer {
    /// Underlying data pointer.
    data: *mut ::std::os::raw::c_void,
    /// Pointer back to the mempool.
    mempool: *mut ice_bindings::custom_ice_mempool,
    /// Refcnt index
    refcnt_index: usize,
    /// Data length that has been written so far
    data_len: usize,
}

impl Clone for IceBuffer {
    fn clone(&self) -> Self {
        if self.data != std::ptr::null_mut() {
            unsafe {
                ice_bindings::custom_ice_refcnt_update_or_free(
                    self.mempool,
                    self.data,
                    self.refcnt_index as _,
                    1i8,
                );
            }
        }
        IceBuffer {
            data: self.data,
            mempool: self.mempool,
            refcnt_index: self.refcnt_index,
            data_len: self.data_len,
        }
    }
}

impl Default for IceBuffer {
    fn default() -> Self {
        IceBuffer {
            data: std::ptr::null_mut(),
            mempool: std::ptr::null_mut(),
            refcnt_index: 0,
            data_len: 0,
        }
    }
}

impl Drop for IceBuffer {
    fn drop(&mut self) {
        if self.data == std::ptr::null_mut() || self.mempool == std::ptr::null_mut() {
            return;
        }
        // Decrements ref count on underlying metadata
        unsafe {
            ice_bindings::custom_ice_refcnt_update_or_free(
                self.mempool,
                self.data,
                self.refcnt_index as _,
                -1i8,
            );
        }
    }
}

impl IceBuffer {
    pub fn new(
        data: *mut ::std::os::raw::c_void,
        mempool: *mut ice_bindings::custom_ice_mempool,
        index: usize,
    ) -> Self {
        IceBuffer {
            data: data,
            mempool: mempool,
            refcnt_index: index,
            data_len: 0,
        }
    }

    pub fn update_refcnt(&mut self, change: i8) {
        unsafe {
            ice_bindings::custom_ice_refcnt_update_or_free(
                self.mempool,
                self.data,
                self.refcnt_index as _,
                change,
            );
        }
    }

    pub fn get_inner(
        self,
    ) -> (
        *mut ::std::os::raw::c_void,
        *mut ice_bindings::custom_ice_mempool,
        usize,
        usize,
    ) {
        (self.data, self.mempool, self.refcnt_index, self.data_len)
    }

    pub fn mutable_slice(&mut self, start: usize, end: usize) -> Result<&mut [u8]> {
        tracing::debug!(
            start = start,
            end = end,
            item_len = unsafe { access!(self.mempool, item_len, usize) },
            "Getting mutable slice from buffer"
        );
        let buf = unsafe {
            std::slice::from_raw_parts_mut(
                (self.data as *mut u8).offset(start as isize),
                end - start,
            )
        };
        if self.data_len <= end {
            // TODO: is this not a great way to do this?
            self.data_len = end;
        }
        Ok(buf)
    }
}

impl DatapathBufferOps for IceBuffer {
    fn set_len(&mut self, len: usize) {
        self.data_len = len;
    }

    fn get_mutable_slice(&mut self, start: usize, len: usize) -> Result<&mut [u8]> {
        self.mutable_slice(start, start + len)
    }
}

impl std::fmt::Debug for IceBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Mbuf addr: {:?}, data_len: {}", self.data, self.data_len)
    }
}

impl AsRef<[u8]> for IceBuffer {
    fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data as *mut u8, self.data_len) }
    }
}

impl std::io::Write for IceBuffer {
    #[inline]
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let item_len = unsafe { access!(self.mempool, item_len, usize) };
        let bytes_to_write = std::cmp::min(bytes.len(), item_len - self.data_len);
        let buf_addr = (self.data as usize + self.data_len) as *mut u8;
        let mut buf = unsafe { std::slice::from_raw_parts_mut(buf_addr, bytes_to_write) };
        self.data_len += bytes_to_write;
        buf.write(&bytes[0..bytes_to_write])
    }

    #[inline]
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(PartialEq, Eq)]
pub struct DpdkMetadata {
    /// Underlying dpdk mbuf
    pub mbuf: *mut dpdk_bindings::rte_mbuf,
    /// Application data offset
    pub offset: usize,
    /// Application data length
    pub data_len: usize,
}

impl DpdkMetadata {
    /// This constructor is used to create a metadata from a raw mbuf, hence we set the ref count
    /// to 1
    pub fn new_from_recv_mbuf(
        mbuf: *mut dpdk_bindings::rte_mbuf,
        offset: usize,
        data_len: usize,
    ) -> Result<Self> {
        let effective_buf_len =
            // check whether offset <= buf_len && len <= (buf_len - data_off)
            unsafe { access!(mbuf, buf_len, usize) - access!(mbuf, data_off, usize) };
        ensure!(offset <= effective_buf_len, "Data offset too large");
        ensure!(
            data_len <= (effective_buf_len - offset),
            "Data length too large"
        );
        unsafe {
            dpdk_bindings::rte_pktmbuf_refcnt_set(mbuf, 1);
        }
        Ok(DpdkMetadata {
            mbuf: mbuf,
            offset: offset,
            data_len: data_len,
        })
    }

    pub fn get_mbuf(&self) -> *mut dpdk_bindings::rte_mbuf {
        self.mbuf
    }

    pub fn increment_refcnt(&mut self) {
        unsafe {
            dpdk_bindings::rte_pktmbuf_refcnt_update_or_free(self.mbuf, 1);
        }
    }

    unsafe fn effective_buf_len(&self) -> usize {
        access!(self.mbuf, buf_len, usize) - access!(self.mbuf, data_off, usize)
    }
}

impl MetadataOps for DpdkMetadata {
    fn offset(&self) -> usize {
        self.offset
    }

    fn data_len(&self) -> usize {
        self.data_len
    }

    fn set_data_len_and_offset(&mut self, len: usize, offset: usize) -> Result<()> {
        ensure!(
            offset <= unsafe { self.effective_buf_len() },
            "Provided offset too large"
        );
        ensure!(
            len <= unsafe { self.effective_buf_len() - offset },
            "Provided data len too large"
        );
        self.offset = offset;
        self.data_len = len;
        Ok(())
    }
}

impl Default for DpdkMetadata {
    fn default() -> Self {
        DpdkMetadata {
            mbuf: ptr::null_mut(),
            offset: 0,
            data_len: 0,
        }
    }
}

impl Drop for DpdkMetadata {
    fn drop(&mut self) {
        if self.mbuf == ptr::null_mut() {
            return;
        }
        unsafe {
            dpdk_bindings::rte_pktmbuf_refcnt_update_or_free(self.mbuf, -1);
        }
    }
}

impl Clone for DpdkMetadata {
    fn clone(&self) -> DpdkMetadata {
        unsafe {
            if self.mbuf != std::ptr::null_mut() {
                dpdk_bindings::rte_pktmbuf_refcnt_update_or_free(self.mbuf, 1);
            }
        }
        DpdkMetadata {
            mbuf: self.mbuf,
            offset: self.offset,
            data_len: self.data_len,
        }
    }
}

impl std::fmt::Debug for DpdkMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "Mbuf addr: {:?}, data addr: {:?}, data_len: {}",
            self.mbuf,
            self.as_ref().as_ptr(),
            unsafe { access!(self.mbuf, data_len, usize) }
        )?;
        Ok(())
    }
}

impl AsRef<[u8]> for DpdkMetadata {
    fn as_ref(&self) -> &[u8] {
        unsafe { dpdk_mbuf_slice!(self.mbuf, self.offset, self.data_len) }
    }
}

#[derive(PartialEq, Eq)]
pub struct IceCustomMetadata {
    pub data: *mut ::std::os::raw::c_void,
    pub mempool: *mut ice_bindings::custom_ice_mempool,
    pub refcnt_index: usize,
    pub offset: usize,
    pub data_len: usize,
}

impl IceCustomMetadata {
    pub fn new(
        ptr: *mut ::std::os::raw::c_void,
        mempool: *mut ice_bindings::custom_ice_mempool,
        refcnt_index: usize,
        offset: usize,
        data_len: usize,
    ) -> Self {
        // increment reference count
        if ptr != std::ptr::null_mut() {
            unsafe {
                ice_bindings::custom_ice_refcnt_update_or_free(mempool, ptr, refcnt_index as _, 1)
            };
        }

        IceCustomMetadata {
            data: ptr,
            mempool: mempool,
            refcnt_index: refcnt_index,
            offset: offset,
            data_len: data_len,
        }
    }

    pub fn get_dma_addr(&self) -> u64 {
        unsafe {
            ice_bindings::custom_ice_get_dma_addr(
                self.mempool,
                self.data as _,
                self.refcnt_index as _,
                self.offset as _,
            )
        }
    }

    pub fn from_buf(mut ice_buffer: IceBuffer) -> Self {
        ice_buffer.update_refcnt(1);
        let (buf, mempool, refcnt_index, data_len) = ice_buffer.get_inner();
        IceCustomMetadata {
            data: buf,
            mempool: mempool,
            refcnt_index: refcnt_index,
            offset: 0,
            data_len: data_len,
        }
    }
    pub fn increment_refcnt(&mut self) {
        unsafe {
            ice_bindings::custom_ice_refcnt_update_or_free(
                self.mempool,
                self.data,
                self.refcnt_index as _,
                1i8,
            );
        }
    }

    pub fn data(&self) -> *mut ::std::os::raw::c_void {
        self.data
    }
}

impl MetadataOps for IceCustomMetadata {
    fn offset(&self) -> usize {
        self.offset
    }

    fn data_len(&self) -> usize {
        self.data_len
    }

    fn set_data_len_and_offset(&mut self, len: usize, offset: usize) -> Result<()> {
        let item_len = unsafe { access!(self.mempool, item_len) };
        ensure!(offset <= item_len as _, "Offset too large");
        ensure!(
            (offset + len) <= item_len as _,
            "Provided data len too large"
        );
        self.offset = offset;
        self.data_len = len;
        Ok(())
    }
}

// TODO: might be safest not to have a default function
impl Default for IceCustomMetadata {
    fn default() -> Self {
        IceCustomMetadata {
            data: ptr::null_mut(),
            mempool: ptr::null_mut(),
            refcnt_index: 0,
            offset: 0,
            data_len: 0,
        }
    }
}

impl Drop for IceCustomMetadata {
    fn drop(&mut self) {
        if self.data == std::ptr::null_mut() || self.mempool == std::ptr::null_mut() {
            return;
        }
        unsafe {
            ice_bindings::custom_ice_refcnt_update_or_free(
                self.mempool,
                self.data,
                self.refcnt_index as _,
                -1i8,
            );
        }
    }
}

impl Clone for IceCustomMetadata {
    fn clone(&self) -> IceCustomMetadata {
        IceCustomMetadata::new(
            self.data,
            self.mempool,
            self.refcnt_index,
            self.offset,
            self.data_len,
        )
    }
}

impl std::fmt::Debug for IceCustomMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "Data addr: {:?}, mempool: {:?}, refcnt_index: {}, off: {}, len: {}",
            self.data, self.mempool, self.refcnt_index, self.offset, self.data_len
        )
    }
}

impl AsRef<[u8]> for IceCustomMetadata {
    fn as_ref(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                (self.data as *mut u8).offset(self.offset as isize),
                self.data_len,
            )
        }
    }
}

#[derive(PartialEq, Eq)]
pub enum IceMetadata {
    Dpdk(DpdkMetadata),
    Ice(IceCustomMetadata),
}

impl IceMetadata {
    pub fn increment_refcnt(&mut self) {
        match self {
            IceMetadata::Dpdk(dpdk_metadata) => dpdk_metadata.increment_refcnt(),
            IceMetadata::Ice(ice_metadata) => ice_metadata.increment_refcnt(),
        }
    }
}

impl AsRef<[u8]> for IceMetadata {
    fn as_ref(&self) -> &[u8] {
        match self {
            IceMetadata::Dpdk(dpdk_metadata) => dpdk_metadata.as_ref(),
            IceMetadata::Ice(ice_metadata) => ice_metadata.as_ref(),
        }
    }
}

impl Clone for IceMetadata {
    fn clone(&self) -> Self {
        match self {
            IceMetadata::Dpdk(dpdk_metadata) => IceMetadata::Dpdk(dpdk_metadata.clone()),
            IceMetadata::Ice(ice_metadata) => IceMetadata::Ice(ice_metadata.clone()),
        }
    }
}
impl std::fmt::Debug for IceMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            IceMetadata::Dpdk(dpdk_metadata) => dpdk_metadata.fmt(f),
            IceMetadata::Ice(ice_metadata) => ice_metadata.fmt(f),
        }
    }
}

impl Default for IceMetadata {
    fn default() -> Self {
        IceMetadata::Ice(IceCustomMetadata::default())
    }
}

impl MetadataOps for IceMetadata {
    fn offset(&self) -> usize {
        match self {
            IceMetadata::Dpdk(dpdk_metadata) => dpdk_metadata.offset(),
            IceMetadata::Ice(ice_metadata) => ice_metadata.offset(),
        }
    }

    fn data_len(&self) -> usize {
        match self {
            IceMetadata::Dpdk(dpdk_metadata) => dpdk_metadata.data_len(),
            IceMetadata::Ice(ice_metadata) => ice_metadata.data_len(),
        }
    }

    fn set_data_len_and_offset(&mut self, len: usize, offset: usize) -> Result<()> {
        match self {
            IceMetadata::Dpdk(dpdk_metadata) => dpdk_metadata.set_data_len_and_offset(len, offset),
            IceMetadata::Ice(ice_metadata) => ice_metadata.set_data_len_and_offset(len, offset),
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
struct IceGlobalContext {
    global_context_ptr: *mut [u8],
    thread_context_ptr: *mut [u8],
}

unsafe impl Send for IceGlobalContext {}
unsafe impl Sync for IceGlobalContext {}

impl IceGlobalContext {
    fn ptr(&self) -> *mut ice_bindings::custom_ice_global_context {
        self.global_context_ptr as *mut ice_bindings::custom_ice_global_context
    }

    fn global_context_ptr(&self) -> *mut [u8] {
        self.global_context_ptr
    }

    fn thread_context_ptr(&self) -> *mut [u8] {
        self.thread_context_ptr
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct IcePerThreadContext {
    global_context_rc: Arc<IceGlobalContext>,
    context: *mut ice_bindings::custom_ice_per_thread_context,
    queue_id: u16,
    physical_port: u16,
    address_info: AddressInfo,
    recv_mempool: *mut dpdk_bindings::rte_mempool,
}

unsafe impl Send for IcePerThreadContext {}
unsafe impl Sync for IcePerThreadContext {}

impl IcePerThreadContext {
    pub fn get_context_ptr(&self) -> *mut ice_bindings::custom_ice_per_thread_context {
        self.context
    }

    pub fn get_recv_mempool_ptr(&self) -> *mut dpdk_bindings::rte_mempool {
        self.recv_mempool
    }

    pub fn get_queue_id(&self) -> u16 {
        self.queue_id
    }

    pub fn get_address_info(&self) -> &AddressInfo {
        &self.address_info
    }

    pub fn get_dpdk_port(&self) -> u16 {
        self.physical_port
    }
}

impl Drop for IcePerThreadContext {
    fn drop(&mut self) {
        unsafe {
            ice_bindings::custom_ice_teardown(self.context);
        }
        if Arc::<IceGlobalContext>::strong_count(&self.global_context_rc) == 1 {
            // safe to drop global context because this is the last reference

            let thread_ptr = unsafe {
                (*Arc::<IceGlobalContext>::as_ptr(&self.global_context_rc)).thread_context_ptr()
            };

            let global_context_ptr = unsafe {
                (*Arc::<IceGlobalContext>::as_ptr(&self.global_context_rc)).global_context_ptr()
            };

            unsafe {
                let _ = Box::from_raw(thread_ptr);
            }
            unsafe {
                let _ = Box::from_raw(global_context_ptr);
            }
        }
    }
}
#[derive(Debug, Clone)]
pub struct IceDatapathSpecificParams {
    eal_init: Vec<String>,
    dpdk_port: i16,
    our_ip: Ipv4Addr,
    our_eth: MacAddress,
    starting_client_port: u16,
    server_port: u16,
}

impl IceDatapathSpecificParams {
    fn get_eal_params(&self) -> Vec<String> {
        self.eal_init.clone()
    }

    fn set_physical_port(&mut self, port: u16) {
        self.dpdk_port = port as i16;
    }

    fn get_physical_port(&self) -> Result<u16> {
        if self.dpdk_port < 0 {
            bail!("DPDK port not set");
        }
        Ok(self.dpdk_port as u16)
    }

    fn get_ipv4(&self) -> Ipv4Addr {
        self.our_ip.clone()
    }

    pub fn get_mac(&self) -> MacAddress {
        self.our_eth.clone()
    }

    pub fn get_client_port(&self) -> u16 {
        self.starting_client_port
    }

    pub fn get_server_port(&self) -> u16 {
        self.server_port
    }
}

pub struct IceConnection {
    /// Per thread context
    thread_context: IcePerThreadContext,
    /// Server or client mode
    mode: AppMode,
    /// Current window of outstanding packets
    outgoing_window: HashMap<(MsgID, ConnID), Instant>,
    /// Active connections: current connection IDs mapped to addresses.
    active_connections: [Option<(
        AddressInfo,
        [u8; cornflakes_libos::utils::TOTAL_UDP_HEADER_SIZE],
    )>; MAX_CONCURRENT_CONNECTIONS],
    /// Map from address info to connection id
    address_to_conn_id: HashMap<AddressInfo, ConnID>,
    /// Allocator for outgoing packets
    allocator: MemoryPoolAllocator<IceMempool>,
    /// Threshold for copying a segment or leaving as a separate scatter-gather entry
    copying_threshold: usize,
    /// Threshold for maximum segments when sending a scatter-gather array.
    max_segments: usize,
    /// Array of mbuf pointers used to receive packets
    recv_mbufs: [*mut dpdk_bindings::rte_mbuf; RECEIVE_BURST_SIZE],
    /// Has outstanding queued data
    has_queued_data: bool,
}

impl IceConnection {
    fn _debug_check_received_pkt(
        &mut self,
        i: usize,
        prev_id: Option<(usize, MsgID, ConnID)>,
        recv_mbufs: &[*mut dpdk_bindings::rte_mbuf; RECEIVE_BURST_SIZE],
        num_received: u16,
    ) -> Result<Option<ReceivedPkt<Self>>> {
        let recv_mbuf = recv_mbufs[i];
        let eth_hdr = unsafe {
            dpdk_mbuf_slice!(
                recv_mbuf,
                0,
                cornflakes_libos::utils::ETHERNET2_HEADER2_SIZE
            )
        };
        let (src_eth, _) = match cornflakes_libos::utils::check_eth_hdr(
            eth_hdr,
            &self.thread_context.address_info.ether_addr,
        ) {
            Ok(r) => r,
            Err(_) => {
                return Ok(None);
            }
        };

        let ipv4_hdr = unsafe {
            dpdk_mbuf_slice!(
                recv_mbuf,
                cornflakes_libos::utils::ETHERNET2_HEADER2_SIZE,
                cornflakes_libos::utils::IPV4_HEADER2_SIZE
            )
        };
        let (src_ip, _) = match cornflakes_libos::utils::check_ipv4_hdr(
            ipv4_hdr,
            &self.thread_context.address_info.ipv4_addr,
        ) {
            Ok(r) => r,
            Err(_) => {
                return Ok(None);
            }
        };

        let udp_hdr = unsafe {
            dpdk_mbuf_slice!(
                recv_mbuf,
                cornflakes_libos::utils::ETHERNET2_HEADER2_SIZE
                    + cornflakes_libos::utils::IPV4_HEADER2_SIZE,
                cornflakes_libos::utils::UDP_HEADER2_SIZE
            )
        };

        let (src_port, _, data_len) = match cornflakes_libos::utils::check_udp_hdr(
            udp_hdr,
            self.thread_context.address_info.udp_port,
        ) {
            Ok(p) => p,
            Err(_) => {
                return Ok(None);
            }
        };

        // check if this address info is within a current conn_id
        let src_addr = cornflakes_libos::utils::AddressInfo::new(src_port, src_ip, src_eth);
        let conn_id = self
            .connect(src_addr)
            .wrap_err("TOO MANY CONCURRENT CONNECTIONS")?;

        let msg_id = unsafe {
            let slice = dpdk_mbuf_slice!(
                recv_mbuf,
                cornflakes_libos::utils::TOTAL_UDP_HEADER_SIZE,
                cornflakes_libos::utils::HEADER_ID_SIZE
            );
            cornflakes_libos::utils::parse_msg_id(slice)
        };
        if let Some((old_idx, old_msg_id, old_conn_id)) = prev_id {
            if old_msg_id == msg_id && old_conn_id == conn_id {
                tracing::info!(
                    num_received,
                    conn_id,
                    "Current packet address: {:?}, old packet address: {:?}, msg id: {}, idx: {}, old_idx: {}",
                    recv_mbufs[i],
                    recv_mbufs[old_idx],
                    msg_id,
                    i,
                    old_idx
                );
            }
        }

        let datapath_metadata = IceMetadata::Dpdk(DpdkMetadata::new_from_recv_mbuf(
            recv_mbuf,
            cornflakes_libos::utils::TOTAL_HEADER_SIZE,
            data_len,
        )?);

        let received_pkt = ReceivedPkt::new(vec![datapath_metadata], msg_id, conn_id);
        Ok(Some(received_pkt))
    }

    fn check_received_pkt(&mut self, i: usize) -> Result<Option<ReceivedPkt<Self>>> {
        tracing::debug!("Checking received packet");
        let recv_mbuf = self.recv_mbufs[i];
        let eth_hdr = unsafe {
            dpdk_mbuf_slice!(
                recv_mbuf,
                0,
                cornflakes_libos::utils::ETHERNET2_HEADER2_SIZE
            )
        };
        let (src_eth, _) = match cornflakes_libos::utils::check_eth_hdr(
            eth_hdr,
            &self.thread_context.address_info.ether_addr,
        ) {
            Ok(r) => r,
            Err(_) => {
                return Ok(None);
            }
        };

        let ipv4_hdr = unsafe {
            dpdk_mbuf_slice!(
                recv_mbuf,
                cornflakes_libos::utils::ETHERNET2_HEADER2_SIZE,
                cornflakes_libos::utils::IPV4_HEADER2_SIZE
            )
        };
        let (src_ip, _) = match cornflakes_libos::utils::check_ipv4_hdr(
            ipv4_hdr,
            &self.thread_context.address_info.ipv4_addr,
        ) {
            Ok(r) => r,
            Err(_) => {
                tracing::debug!("IP hdr wrong");
                return Ok(None);
            }
        };

        let udp_hdr = unsafe {
            dpdk_mbuf_slice!(
                recv_mbuf,
                cornflakes_libos::utils::ETHERNET2_HEADER2_SIZE
                    + cornflakes_libos::utils::IPV4_HEADER2_SIZE,
                cornflakes_libos::utils::UDP_HEADER2_SIZE
            )
        };

        let (src_port, _, data_len) = match cornflakes_libos::utils::check_udp_hdr(
            udp_hdr,
            self.thread_context.address_info.udp_port,
        ) {
            Ok(p) => p,
            Err(_) => {
                tracing::debug!("UDP hdr wrong");
                return Ok(None);
            }
        };

        tracing::debug!("Data len in udp hdr: {:?}", data_len);

        // check if this address info is within a current conn_id
        let src_addr = cornflakes_libos::utils::AddressInfo::new(src_port, src_ip, src_eth);
        let conn_id = self
            .connect(src_addr)
            .wrap_err("TOO MANY CONCURRENT CONNECTIONS")?;

        let msg_id = unsafe {
            let slice = dpdk_mbuf_slice!(
                recv_mbuf,
                cornflakes_libos::utils::TOTAL_UDP_HEADER_SIZE,
                cornflakes_libos::utils::HEADER_ID_SIZE
            );
            cornflakes_libos::utils::parse_msg_id(slice)
        };

        let datapath_metadata = IceMetadata::Dpdk(DpdkMetadata::new_from_recv_mbuf(
            recv_mbuf,
            cornflakes_libos::utils::TOTAL_HEADER_SIZE,
            data_len,
        )?);

        let received_pkt = ReceivedPkt::new(vec![datapath_metadata], msg_id, conn_id);
        Ok(Some(received_pkt))
    }

    fn insert_into_outgoing_map(&mut self, msg_id: MsgID, conn_id: ConnID) {
        if self.mode == AppMode::Client {
            if !self.outgoing_window.contains_key(&(msg_id, conn_id)) {
                self.outgoing_window
                    .insert((msg_id, conn_id), Instant::now());
            } else {
                tracing::error!(msg_id, conn_id, "Already sent");
            }
        }
    }

    /// Copies udp header into the front of the given IceBuffer
    fn copy_hdr(
        &self,
        data_buffer: &mut IceBuffer,
        conn_id: ConnID,
        msg_id: MsgID,
        data_len: usize,
    ) -> Result<usize> {
        let hdr_bytes: &[u8; cornflakes_libos::utils::TOTAL_UDP_HEADER_SIZE] =
            match &self.active_connections[conn_id as usize] {
                Some((_, hdr_bytes_vec)) => hdr_bytes_vec,
                None => {
                    bail!("Could not find address for connID");
                }
            };
        unsafe {
            ice_bindings::fill_in_hdrs(
                data_buffer
                    .mutable_slice(0, cornflakes_libos::utils::TOTAL_HEADER_SIZE)?
                    .as_mut_ptr() as _,
                hdr_bytes.as_ptr() as _,
                msg_id,
                data_len,
            );
        }
        Ok(cornflakes_libos::utils::TOTAL_HEADER_SIZE)
    }

    fn poll_for_completions(&self) -> Result<()> {
        // TODO: fill in logic to check the tx queue's ring buffer to see whether
        // (a) we should process any completions (enough is in flight)
        // (b) The last descriptor to check for completions from (so we don't query the value of
        // the `done` bit on every descriptor)
        // (c) See if that last descriptor is done -- if that is done, the rest is done
        unimplemented!();
    }

    fn post_curr_transmissions(&self) -> Result<()> {
        // TODO: fill in logic to post whatever has been queued up to on the ring buffer
        unimplemented!();
    }

    fn post_ice_metadata(&self, metadata: &mut IceMetadata, tx_id: u64, last_tx_id: u64) -> Result<()> {
        // increment reference count for this metadata as we are posting on the NIC
        metadata.increment_refcnt();
        let per_thread_context = self.thread_context.get_context_ptr();
        // function to post the packet to tx_ring
        let ice_metadata = match metadata {
            IceMetadata::Dpdk(_) => {
                bail!("Cannot be dpdk buffer in posting");
            }
            IceMetadata::Ice(custom_ice) => custom_ice
        };
        unsafe {
            ice_bindings::custom_ice_post_data_segment(per_thread_context, 
                ice_metadata.get_dma_addr(), ice_metadata.data_len() as _, tx_id as _, last_tx_id as _);
        }
        Ok(())
    }
}

impl Datapath for IceConnection {
    type DatapathBuffer = IceBuffer;
    type DatapathMetadata = IceMetadata;
    // callback entry is &mut u16: representing the index into the ring buffer being posted
    type CallbackEntryState = *mut u16;

    type PerThreadContext = IcePerThreadContext;
    type DatapathSpecificParams = IceDatapathSpecificParams;

    fn parse_config_file(
        config_file: &str,
        our_ip: &Ipv4Addr,
    ) -> Result<Self::DatapathSpecificParams> {
        let (ip_to_mac, _mac_to_ip, udp_port, client_port) =
            parse_yaml_map(config_file).wrap_err("Failed to parse yaml mapping")?;

        let eal_init = dpdk_wrapper::parse_eal_init(config_file)?;

        // since eal init has not been run yet, we cannot run dpdk get macaddr
        let eth_addr = match ip_to_mac.get(our_ip) {
            Some(e) => e.clone(),
            None => {
                bail!("Could not find eth addr for passed in ipv4 addr {:?} in config_file ip_to_mac map: {:?}", our_ip, ip_to_mac);
            }
        };

        Ok(IceDatapathSpecificParams {
            eal_init: eal_init,
            dpdk_port: -1,
            our_ip: our_ip.clone(),
            our_eth: eth_addr,
            starting_client_port: client_port,
            server_port: udp_port,
        })
    }

    fn compute_affinity(
        datapath_params: &Self::DatapathSpecificParams,
        num_queues: usize,
        _remote_ip: Option<Ipv4Addr>,
        app_mode: AppMode,
    ) -> Result<Vec<AddressInfo>> {
        // TODO: how do we compute affinity for more than one queue for mlx5
        if num_queues > 1 {
            bail!("Currently, ice datapath does not support more than one queue");
        }
        match app_mode {
            AppMode::Client => Ok(vec![AddressInfo::new(
                datapath_params.get_client_port(),
                datapath_params.get_ipv4(),
                datapath_params.get_mac(),
            )]),

            AppMode::Server => Ok(vec![AddressInfo::new(
                datapath_params.get_server_port(),
                datapath_params.get_ipv4(),
                datapath_params.get_mac(),
            )]),
        }
    }

    fn global_init(
        num_queues: usize,
        datapath_params: &mut Self::DatapathSpecificParams,
        addresses: Vec<AddressInfo>,
    ) -> Result<Vec<Self::PerThreadContext>> {
        // run eal init / dpdk initialization steps
        let eal_args = datapath_params.get_eal_params();
        let mut args = vec![];
        let mut ptrs = vec![];
        for entry in eal_args.iter() {
            let s = CString::new(entry.as_str()).unwrap();
            ptrs.push(s.as_ptr() as *mut u8);
            args.push(s);
        }

        unsafe {
            let ret = dpdk_bindings::rte_eal_init(ptrs.len() as i32, ptrs.as_mut_ptr() as *mut _);
            tracing::info!("Eal init returned {}", ret);
        }
        tracing::debug!("DPDK init args: {:?}", args);

        // Find and set physical port
        let nb_ports = unsafe { dpdk_bindings::rte_eth_dev_count_avail() };
        if nb_ports <= 0 {
            bail!("DPDK GLOBAL INIT: No physical ports available.");
        }
        tracing::info!(
            "DPDK reports that {} ports (interfaces) are available",
            nb_ports
        );
        datapath_params.set_physical_port(nb_ports - 1);

        // allocate the global context inside of an arc
        let global_context: Arc<IceGlobalContext> = {
            unsafe {
                let global_context_size = ice_bindings::custom_ice_get_global_context_size();
                let thread_context_size =
                    ice_bindings::custom_ice_get_per_thread_context_size(num_queues as _);
                let global_context_box: Box<[u8]> =
                    vec![0; global_context_size as _].into_boxed_slice();
                let thread_context_box: Box<[u8]> =
                    vec![0; thread_context_size as _].into_boxed_slice();

                let global_context_ptr = Box::<[u8]>::into_raw(global_context_box);
                let thread_context_ptr = Box::<[u8]>::into_raw(thread_context_box);
                ice_bindings::custom_ice_init_global_context(
                    num_queues as _,
                    global_context_ptr as _,
                    thread_context_ptr as _,
                );

                Arc::new(IceGlobalContext {
                    global_context_ptr: global_context_ptr,
                    thread_context_ptr: thread_context_ptr,
                })
            }
        };

        // for each core, initialize a native memory pool for receiving packets
        let mut ret: Vec<Self::PerThreadContext> = Vec::with_capacity(num_queues);
        for (i, addr) in addresses.into_iter().enumerate() {
            let recv_mempool =
                dpdk_wrapper::create_recv_mempool(&format!("recv_mbuf_pool_{}", i)).wrap_err(
                    format!("Not able create recv mbuf pool {} in global init", i),
                )?;
            let global_context_copy = global_context.clone();
            let thread_context_ptr = unsafe {
                ice_bindings::custom_ice_get_per_thread_context(
                    (*Arc::<IceGlobalContext>::as_ptr(&global_context_copy)).ptr(),
                    i as u64,
                )
            };

            ret.push(IcePerThreadContext {
                global_context_rc: global_context_copy,
                context: thread_context_ptr,
                queue_id: i as _,
                address_info: addr,
                recv_mempool: recv_mempool,
                physical_port: datapath_params.get_physical_port()?,
            });
        }

        assert!(unsafe {
            dpdk_bindings::rte_eth_dev_is_valid_port(datapath_params.get_physical_port()?) == 1
        });

        let mut rx_conf: MaybeUninit<dpdk_bindings::rte_eth_rxconf> = MaybeUninit::zeroed();
        unsafe {
            (*rx_conf.as_mut_ptr()).rx_thresh.pthresh = RX_PTHRESH;
            (*rx_conf.as_mut_ptr()).rx_thresh.hthresh = RX_HTHRESH;
            (*rx_conf.as_mut_ptr()).rx_thresh.wthresh = RX_WTHRESH;
            (*rx_conf.as_mut_ptr()).rx_free_thresh = 32;
        }

        let mut tx_conf: MaybeUninit<dpdk_bindings::rte_eth_txconf> = MaybeUninit::zeroed();
        unsafe {
            (*tx_conf.as_mut_ptr()).tx_thresh.pthresh = TX_PTHRESH;
            (*tx_conf.as_mut_ptr()).tx_thresh.hthresh = TX_HTHRESH;
            (*tx_conf.as_mut_ptr()).tx_thresh.wthresh = TX_WTHRESH;
        }

        unsafe {
            dpdk_bindings::eth_dev_configure_ice(
                datapath_params.get_physical_port()?,
                num_queues as _,
                num_queues as _,
            )
        };

        let socket_id = unsafe {
            dpdk_check_not_failed!(
                rte_eth_dev_socket_id(datapath_params.get_physical_port()?),
                "Port id is out of range"
            )
        } as u32;

        for per_thread_context in ret.iter() {
            dpdk_check_not_errored!(rte_eth_rx_queue_setup(
                datapath_params.get_physical_port()?,
                per_thread_context.get_queue_id(),
                RX_RING_SIZE,
                socket_id,
                rx_conf.as_mut_ptr(),
                per_thread_context.get_recv_mempool_ptr()
            ));

            dpdk_check_not_errored!(rte_eth_tx_queue_setup(
                datapath_params.get_physical_port()?,
                per_thread_context.get_queue_id(),
                TX_RING_SIZE,
                socket_id,
                tx_conf.as_mut_ptr()
            ));
        }

        // start ethernet port
        dpdk_check_not_errored!(rte_eth_dev_start(datapath_params.get_physical_port()?));

        dpdk_wrapper::wait_for_link_status_up(datapath_params.get_physical_port()?)?;

        // init ice queue pointer to point to DPDK initialized tx queue
        unsafe {
            check_ok!(custom_ice_init_tx_queues(
                (*Arc::<IceGlobalContext>::as_ptr(&global_context)).ptr(),
                datapath_params.get_physical_port()? as _,
                socket_id as _
            ));
        }

        Ok(ret)
    }

    fn per_thread_init(
        datapath_params: Self::DatapathSpecificParams,
        context: Self::PerThreadContext,
        mode: cornflakes_utils::AppMode,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        let rx_mempool = IceMempool::new_from_dpdk_ptr(context.get_recv_mempool_ptr() as _);
        // allocate a tx pool
        let mempool_params =
            sizes::MempoolAllocationParams::new(MEMPOOL_MIN_ELTS, PGSIZE_2MB, MAX_BUFFER_SIZE)
                .wrap_err("Incorrect mempool allocation params")?;
        let tx_mempool = IceMempool::new(&mempool_params, false)?;
        let allocator = MemoryPoolAllocator::new(rx_mempool, tx_mempool)?;
        Ok(IceConnection {
            thread_context: context,
            mode: mode,
            outgoing_window: HashMap::default(),
            active_connections: [None; MAX_CONCURRENT_CONNECTIONS],
            address_to_conn_id: HashMap::default(),
            allocator: allocator,
            copying_threshold: 256,
            max_segments: 32,
            recv_mbufs: [ptr::null_mut(); RECEIVE_BURST_SIZE],
            has_queued_data: false,
        })
    }

    /// "Open" a connection to the other side.
    /// Args:
    /// @addr: Address information to connect to. Returns a unique "connection" ID.
    fn connect(&mut self, addr: AddressInfo) -> Result<ConnID> {
        if self.address_to_conn_id.contains_key(&addr) {
            return Ok(*self.address_to_conn_id.get(&addr).unwrap());
        } else {
            if self.address_to_conn_id.len() >= MAX_CONCURRENT_CONNECTIONS {
                bail!("too many concurrent connections; cannot connect to more");
            }
            let mut idx: Option<usize> = None;
            for (i, addr_option) in self.active_connections.iter().enumerate() {
                match addr_option {
                    Some(_) => {}
                    None => {
                        self.address_to_conn_id.insert(addr.clone(), i);
                        idx = Some(i);
                        break;
                    }
                }
            }
            match idx {
                Some(i) => {
                    let mut bytes: [u8; cornflakes_libos::utils::TOTAL_UDP_HEADER_SIZE] =
                        [0u8; cornflakes_libos::utils::TOTAL_UDP_HEADER_SIZE];
                    let header_info = cornflakes_libos::utils::HeaderInfo::new(
                        self.thread_context.get_address_info().clone(),
                        addr.clone(),
                    );
                    // write in the header to these bytes, assuming data length of 0
                    // data length is updated at runtime and checksums are updated on specific
                    // transmissions
                    cornflakes_libos::utils::write_eth_hdr(
                        &header_info,
                        &mut bytes[0..cornflakes_libos::utils::ETHERNET2_HEADER2_SIZE],
                    )?;
                    cornflakes_libos::utils::write_ipv4_hdr(
                        &header_info,
                        &mut bytes[cornflakes_libos::utils::ETHERNET2_HEADER2_SIZE
                            ..(cornflakes_libos::utils::ETHERNET2_HEADER2_SIZE
                                + cornflakes_libos::utils::IPV4_HEADER2_SIZE)],
                        42,
                    )?;
                    cornflakes_libos::utils::write_udp_hdr(
                        &header_info,
                        &mut bytes[(cornflakes_libos::utils::ETHERNET2_HEADER2_SIZE
                            + cornflakes_libos::utils::IPV4_HEADER2_SIZE)
                            ..(cornflakes_libos::utils::ETHERNET2_HEADER2_SIZE
                                + cornflakes_libos::utils::IPV4_HEADER2_SIZE
                                + cornflakes_libos::utils::UDP_HEADER2_SIZE)],
                        42,
                    )?;
                    self.active_connections[i] = Some((addr, bytes));
                    return Ok(i);
                }
                None => {
                    bail!("too many concurrent connections; cannot connect to more");
                }
            }
        }
    }

    /// Echo the specified packet back to the  source.
    /// Args:
    /// @pkts: Vector of received packet objects to echo back.
    fn echo(&mut self, pkts: Vec<ReceivedPkt<Self>>) -> Result<()>
    where
        Self: Sized,
    {
        let mut send_mbufs: [[*mut dpdk_bindings::rte_mbuf; RECEIVE_BURST_SIZE]; 32] =
            [[ptr::null_mut(); RECEIVE_BURST_SIZE]; 32];
        let pkts_len = pkts.len();
        for (i, mut pkt) in pkts.into_iter().enumerate() {
            let msg_id = pkt.msg_id();
            for (scatter_index, ref mut dpdk_metadata) in pkt.iter_mut().enumerate() {
                let mbuf = match dpdk_metadata {
                    IceMetadata::Dpdk(dpdk) => dpdk.get_mbuf(),
                    IceMetadata::Ice(ice) => {
                        tracing::warn!("Echo on ice should always be called with popped packets, that are allocated by dpdk");
                        unreachable!();
                    }
                };
                tracing::debug!(
                    "Echoing packet with id {}, mbuf addr {:?}, refcnt {}",
                    msg_id,
                    mbuf,
                    unsafe { access!(mbuf, refcnt, u16) }
                );

                // flip headers on packet
                if scatter_index == 0 {
                    // flip headers assumes the header is right at the buf addr of the packet
                    unsafe {
                        dpdk_bindings::flip_headers(mbuf);
                    }
                }
                // increment ref count so it does not get dropped here
                dpdk_metadata.increment_refcnt();
                send_mbufs[scatter_index as usize][i as usize] = mbuf;
            }
        }
        let mut num_sent: u16 = 0;
        while (num_sent as usize) < pkts_len {
            let mbuf_ptr = &mut send_mbufs[0][num_sent as usize] as _;
            let sent = unsafe {
                dpdk_bindings::rte_eth_tx_burst(
                    self.thread_context.get_dpdk_port(),
                    self.thread_context.get_queue_id(),
                    mbuf_ptr,
                    pkts_len as u16 - num_sent,
                )
            };
            num_sent += sent;
            if (num_sent as usize) != pkts_len {
                tracing::debug!(
                    "Failed to send {} mbufs, sent {}, {} so far",
                    pkts_len,
                    sent,
                    num_sent
                );
            }
        }
        Ok(())
    }

    fn queue_cornflakes_obj<'arena>(
        &mut self,
        _msg_id: MsgID,
        _conn_id: ConnID,
        _copy_context: &mut CopyContext<'arena, Self>,
        _cornflakes_obj: impl HybridArenaRcSgaHdr<'arena, Self>,
        _end_batch: bool,
    ) -> Result<()>
    where
        Self: Sized,
    {
        // check the number of segments this object needs
        // wait until that number of segments is available (poll for completions)
        // before polling, post what you have (if you have something to post)
        // then get current value txq->tx_tail
        // skip 1 for the header
        // if copy context has data, skip one more
        // then define and call the callback
        let per_thread_context = self.thread_context.get_context_ptr();
        let last_id = 0;
        let mut callback = |metadata_mbuf: &IceMetadata, ring_buffer_id: *mut u16| -> Result<()> {
            let mut metadata_clone = metadata_mbuf.clone();
            metadata_clone.increment_refcnt();
            let custom_ice_res: Result<IceCustomMetadata> = match metadata_clone {
                IceMetadata::Dpdk(_) => {
                    bail!("Cannot be dpdk buffer in callback");
                }
                IceMetadata::Ice(custom_ice) => Ok(custom_ice),
            };
            let custom_ice = custom_ice_res?;

            let physaddr = custom_ice.get_dma_addr();
            let len = custom_ice.as_ref().len();
            // we need length & physical address to post

            unsafe {
                ice_bindings::custom_ice_post_data_segment(
                    per_thread_context as _,
                    physaddr,
                    len as _,
                    *ring_buffer_id,
                    last_id,
                );
            }
            Ok(())
        };

        // call the callback
        // circle back and write packet and object header into first segment
        // optionally write copied data into second segment
        // update txqueue's txq_tail value (/ finish single transmission)
        // if end batch is true:
        //  post what you have
        //
        // TODO: implement this
        unimplemented!();
    }

    fn queue_single_buffer_with_copy(
        &mut self,
        buf: (MsgID, ConnID, &[u8]),
        end_batch: bool,
    ) -> Result<()> {
        let per_thread_context = self.thread_context.get_context_ptr();

        let msg_id = buf.0;
        let conn_id = buf.1;
        let buf_arr = buf.2;

        // determine num tx descriptors necessary for buffer
        let num_required = 1;

        // determine num tx descriptors available
        let mut txd_avail = unsafe { ice_bindings::custom_ice_get_txd_avail(per_thread_context) };

        let cur_tx_id = unsafe { ice_bindings::get_current_tx_id(per_thread_context) };
        let last_tx_id = unsafe { ice_bindings::get_last_tx_id_needed(per_thread_context, num_required as _) };
        // wait until enough tx descriptors are available
        while num_required > txd_avail {
            // function to get number of tx descriptors available
            if self.has_queued_data {
                unsafe {
                    ice_bindings::post_queued_segments(per_thread_context, last_tx_id as _)
                }
                self.has_queued_data = false;
            }
            if unsafe {
                ice_bindings::custom_ice_tx_cleanup(per_thread_context) != 0
            } {
                tracing::debug!("custom_ice_tx_cleanup failed to clean");
            }

            txd_avail = unsafe { ice_bindings::custom_ice_get_txd_avail(per_thread_context) };
        }
        
        // allocate IceBuffer
        let mut data_buffer = {
            match self.allocator.allocate_tx_buffer()? {
                Some(data_buf) => data_buf,
                None => {
                    bail!("No tx mempools to allocate outgoing packet");
                }
            }
        };

        // write header
        self.copy_hdr(&mut data_buffer, conn_id, msg_id, buf_arr.len())?;

        // write data buffer
        ensure!(
            data_buffer.write(buf_arr)? == buf_arr.len(),
            "Could not copy whole buffer into allocated buffer"
        );
        let mut ice_metadata = IceMetadata::Ice(IceCustomMetadata::from_buf(data_buffer));
        let _ = self.post_ice_metadata(&mut ice_metadata, cur_tx_id, last_tx_id);

        // finish queueing buffer
        unsafe {
            ice_bindings::finish_single_transmission(per_thread_context, last_tx_id as _);
        }

        // end batch
        if end_batch {
            println!("batch being ended");
            unsafe {
                ice_bindings::post_queued_segments(per_thread_context, last_tx_id as _);
            }
            self.has_queued_data = false;
        }
        println!("queue_single_buffer_with_copy finished");
        Ok(())
    }

    fn push_buffers_with_copy(&mut self, _: &[(u32, usize, &[u8])]) -> Result<()> {
        unimplemented!();
    }

    fn push_rc_sgas(
        &mut self,
        _: &mut [(u32, usize, cornflakes_libos::RcSga<'_, Self>)],
    ) -> Result<()> {
        unimplemented!()
    }

    fn push_ordered_sgas(
        &mut self,
        _: &[(u32, usize, cornflakes_libos::OrderedSga<'_>)],
    ) -> Result<()> {
        unimplemented!()
    }
    /// @sgas: Vector of (msg id, connection id, raw address scatter-gather arrays) to send.
    fn push_sgas(&mut self, _sgas: &[(MsgID, ConnID, cornflakes_libos::Sga)]) -> Result<()> {
        unimplemented!();
    }

    /// Listen for new received packets and pop out with durations.
    fn pop_with_durations(&mut self) -> Result<Vec<(ReceivedPkt<Self>, Duration)>>
    where
        Self: Sized,
    {
        let num_received = unsafe {
            dpdk_bindings::rte_eth_rx_burst(
                self.thread_context.get_dpdk_port(),
                self.thread_context.get_queue_id(),
                self.recv_mbufs.as_mut_ptr(),
                RECEIVE_BURST_SIZE as _,
            )
        };

        let mut ret: Vec<(ReceivedPkt<Self>, Duration)> = Vec::with_capacity(RECEIVE_BURST_SIZE);
        for i in 0..num_received as usize {
            if let Some(received_pkt) = self
                .check_received_pkt(i)
                .wrap_err(format!("Error checking received pkt {}", i))?
            {
                tracing::debug!(
                    "Received pkt with msg ID {}, conn ID {}",
                    received_pkt.msg_id(),
                    received_pkt.conn_id(),
                );
                match self
                    .outgoing_window
                    .remove(&(received_pkt.msg_id(), received_pkt.conn_id()))
                {
                    Some(start_time) => {
                        let dur = start_time.elapsed();
                        ret.push((received_pkt, dur));
                    }
                    None => {
                        // free the rest of the packets
                        tracing::warn!(
                            "Cannot find msg id {} and conn id {} in outgoing window",
                            received_pkt.msg_id(),
                            received_pkt.conn_id()
                        );
                        unsafe {
                            dpdk_bindings::rte_pktmbuf_free(self.recv_mbufs[i]);
                        }
                    }
                }
            } else {
                tracing::debug!("Received invalid packet at addr {:?}", self.recv_mbufs[i]);
                unsafe {
                    dpdk_bindings::rte_pktmbuf_free(self.recv_mbufs[i]);
                }
            }
            self.recv_mbufs[i] = ptr::null_mut();
        }
        Ok(ret)
    }

    /// Listen for new received packets and pop them out.
    fn pop(&mut self) -> Result<Vec<ReceivedPkt<Self>>>
    where
        Self: Sized,
    {
        let num_received = unsafe {
            dpdk_bindings::rte_eth_rx_burst(
                self.thread_context.get_dpdk_port(),
                self.thread_context.get_queue_id(),
                self.recv_mbufs.as_mut_ptr(),
                RECEIVE_BURST_SIZE as _,
            )
        };

        if num_received == 0 {
            return Ok(vec![]);
        }

        let mut ret: Vec<ReceivedPkt<Self>> = Vec::with_capacity(RECEIVE_BURST_SIZE);

        for i in 0..num_received as usize {
            if let Some(received_pkt) = self
                .check_received_pkt(i)
                .wrap_err(format!("Error checking received pkt {}", i))?
            {
                tracing::debug!(
                    "Received pkt with msg ID {}, conn ID {}",
                    received_pkt.msg_id(),
                    received_pkt.conn_id()
                );
                ret.push(received_pkt);
            } else {
                unsafe {
                    dpdk_bindings::rte_pktmbuf_free(self.recv_mbufs[i]);
                }
            }
            self.recv_mbufs[i] = ptr::null_mut();
        }
        Ok(ret)
    }

    /// Check if any outstanding packets have timed out.
    fn timed_out(&self, time_out: Duration) -> Result<Vec<(MsgID, ConnID)>> {
        let mut timed_out: Vec<(MsgID, ConnID)> = Vec::default();
        for ((id, conn_id), start) in self.outgoing_window.iter() {
            if start.elapsed().as_nanos() > time_out.as_nanos() {
                tracing::debug!(elapsed = ?start.elapsed().as_nanos(), id = *id, "Timing out");
                timed_out.push((*id, *conn_id));
            }
        }
        Ok(timed_out)
    }

    /// Checks whether input buffer is registered.
    /// Args:
    /// @buf: slice to check if address is registered or not.
    fn is_registered(&self, buf: &[u8]) -> bool {
        self.allocator.is_registered(buf)
    }

    /// Allocate a datapath buffer with the given size and alignment.
    /// Args:
    /// @size: minimum size of buffer to be allocated.
    fn allocate(&mut self, size: usize) -> Result<Option<Self::DatapathBuffer>> {
        self.allocator.allocate_buffer(size)
    }

    /// Allocate a tx buffer with MTU size (max packet size).
    fn allocate_tx_buffer(&mut self) -> Result<(Option<Self::DatapathBuffer>, usize)> {
        Ok((
            self.allocator.allocate_tx_buffer()?,
            <Self as Datapath>::max_packet_size(),
        ))
    }

    /// Consume a datapath buffer and returns a metadata object that owns the underlying
    /// buffer.
    /// Args:
    /// @buf: Datapath buffer object.
    fn get_metadata(&self, buf: Self::DatapathBuffer) -> Result<Option<Self::DatapathMetadata>> {
        Ok(Some(IceMetadata::Ice(IceCustomMetadata::from_buf(buf))))
    }

    /// Takes a buffer and recovers underlying metadata if it is refcounted.
    /// Args:
    /// @buf: Buffer.
    fn recover_metadata(&self, buf: &[u8]) -> Result<Option<Self::DatapathMetadata>> {
        self.allocator.recover_buffer(buf)
    }

    /// Elastically add a memory pool with a particular size.
    /// Will add a new region of memory registered with the NIC.
    /// Args:
    /// @size: element size
    /// @min_elts: minimum number of elements in the memory pool.
    ///
    /// Returns:
    /// Vector of memory pool IDs for mempools that were created (datapath may have a maximum size
    /// for the memory pool).
    fn add_memory_pool(&mut self, size: usize, min_elts: usize) -> Result<Vec<MempoolID>> {
        unimplemented!();
    }

    /// Checks whether datapath has mempool of size size given (must be power of 2).
    fn has_mempool(&self, _size: usize) -> bool {
        unimplemented!();
    }

    /// Register given mempool ID
    fn register_mempool(&mut self, _id: MempoolID) -> Result<()> {
        unimplemented!();
    }

    /// Unregister given mempool ID
    fn unregister_mempool(&mut self, _id: MempoolID) -> Result<()> {
        unimplemented!();
    }

    fn header_size(&self) -> usize {
        unimplemented!();
    }

    /// Number of cycles in a second
    fn timer_hz(&self) -> u64 {
        unimplemented!();
    }

    /// Convert cycles to ns.
    fn cycles_to_ns(&self, _t: u64) -> u64 {
        unimplemented!();
    }

    /// Current cycles.
    fn current_cycles(&self) -> u64 {
        unimplemented!();
    }

    /// Set copying threshold for serialization.
    fn set_copying_threshold(&mut self, threshold: usize) {
        self.copying_threshold = threshold;
    }

    /// Get current copying threshold for serialization.
    fn get_copying_threshold(&self) -> usize {
        self.copying_threshold
    }

    /// Sets maximum segments sent in a packet.
    fn set_max_segments(&mut self, max_entries: usize) {
        self.max_segments = max_entries;
    }

    /// Gets current maximum segments
    fn get_max_segments(&self) -> usize {
        self.max_segments
    }

    /// Set inline mode (may not be available in all datapaths)
    fn set_inline_mode(&mut self, _mode: InlineMode) {}

    /// Packet processing batch size.
    fn batch_size() -> usize {
        32
    }

    /// Maximum possible scatter gather elements.
    fn max_scatter_gather_entries() -> usize {
        64
    }

    /// Maximum possible packet size
    fn max_packet_size() -> usize {
        8192
    }
}
