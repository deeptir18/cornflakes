use super::{
    super::dpdk_bindings::*, allocator::MempoolInfo, dpdk_check, dpdk_utils::*, wrapper::*,
};
use cornflakes_libos::{
    allocator::{align_up, MemoryPoolAllocator, MempoolID},
    datapath::{Datapath, ExposeMempoolID, InlineMode, MetadataOps, ReceivedPkt},
    utils::AddressInfo,
    ConnID, MsgID, OrderedSga, RcSga, RcSge, Sga, Sge, USING_REF_COUNTING,
};

use color_eyre::eyre::{bail, ensure, Result, WrapErr};
use cornflakes_utils::{parse_yaml_map, AppMode};
use eui48::MacAddress;
use hashbrown::HashMap;
use std::{
    ffi::CString,
    io::Write,
    mem::MaybeUninit,
    net::Ipv4Addr,
    ptr,
    time::{Duration, Instant},
};

const MAX_CONCURRENT_CONNECTIONS: usize = 128;

const RECEIVE_BURST_SIZE: usize = 32;
const SEND_BURST_SIZE: usize = 32;
const MAX_SCATTERS: usize = 32;
const MEMPOOL_MAX_SIZE: usize = 65536;

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
pub struct DpdkBuffer {
    /// Underlying allocated mbuf
    mbuf: *mut rte_mbuf,
    /// Mempool ID
    mempool_id: MempoolID,
}

impl ExposeMempoolID for DpdkBuffer {
    fn set_mempool_id(&mut self, id: MempoolID) {
        self.mempool_id = id;
    }

    fn get_mempool_id(&self) -> MempoolID {
        self.mempool_id
    }
}

impl DpdkBuffer {
    pub fn new(mbuf: *mut rte_mbuf, mempool_id: MempoolID) -> Self {
        DpdkBuffer {
            mbuf: mbuf,
            mempool_id: mempool_id,
        }
    }

    pub fn get_inner(self) -> *mut rte_mbuf {
        self.mbuf
    }

    fn effective_buf_len(&self) -> usize {
        unsafe { access!(self.mbuf, buf_len, usize) - access!(self.mbuf, data_off, usize) }
    }

    pub fn mutable_slice(&mut self, start: usize, end: usize) -> Result<&mut [u8]> {
        let effective_buf_len = self.effective_buf_len();
        if start > effective_buf_len || end > effective_buf_len {
            bail!(
                "Invalid bounds for buf of effective len {}",
                effective_buf_len
            );
        }
        let buf = unsafe { mbuf_mut_slice!(self.mbuf, start, end - start) };
        unsafe {
            if access!(self.mbuf, data_len, usize) <= end {
                write_struct_field!(self.mbuf, data_len, end);
            }
        }
        Ok(buf)
    }

    /// Copies contents of buffer into mbuf at offset (until buffer space is available).
    /// Returns amount written.
    pub fn copy_data(&mut self, buf: &[u8], offset: usize) -> Result<usize> {
        let to_write = std::cmp::min(self.effective_buf_len() - offset, buf.len());
        let mut_slice = unsafe { mbuf_mut_slice!(self.mbuf, offset, to_write) };
        unsafe {
            rte_memcpy_wrapper(
                mut_slice.as_mut_ptr() as _,
                buf[0..to_write].as_ptr() as _,
                to_write,
            );
            let new_data_len =
                std::cmp::max(access!(self.mbuf, data_len, usize), offset + buf.len());
            write_struct_field!(self.mbuf, data_len, new_data_len);
        }
        Ok(to_write)
    }
}

impl std::fmt::Debug for DpdkBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Mbuf addr: {:?}, data_len: {}", self.mbuf, unsafe {
            access!(self.mbuf, data_len, usize)
        })
    }
}

impl AsRef<[u8]> for DpdkBuffer {
    fn as_ref(&self) -> &[u8] {
        unsafe { mbuf_slice!(self.mbuf, 0, access!(self.mbuf, data_len, usize)) }
    }
}

impl Write for DpdkBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let to_write = std::cmp::min(self.effective_buf_len(), buf.len());
        let mut mut_slice = unsafe { mbuf_mut_slice!(self.mbuf, 0, to_write) };
        let written = mut_slice.write(&buf[0..to_write])?;
        unsafe { write_struct_field!(self.mbuf, data_len, written) };
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(PartialEq, Eq)]
pub struct RteMbufMetadata {
    /// Underlying mbuf
    mbuf: *mut rte_mbuf,
    /// Data offset for application
    offset: usize,
    /// Application data length
    data_len: usize,
}

impl RteMbufMetadata {
    pub fn from_dpdk_buf(dpdk_buffer: DpdkBuffer) -> Self {
        // data_len already set in this mbuf
        let mbuf = dpdk_buffer.get_inner();

        // increment the reference count of the underlying mbuf
        unsafe {
            if USING_REF_COUNTING {
                rte_pktmbuf_refcnt_set(mbuf, 1)
            }
        };
        let data_len = unsafe { access!(mbuf, data_len, usize) };

        RteMbufMetadata {
            mbuf: mbuf,
            offset: 0,
            data_len: data_len,
        }
    }

    pub fn new(mbuf: *mut rte_mbuf, data_offset: usize, data_len: Option<usize>) -> Result<Self> {
        // check whether: data_len <= (effective buf_len - new data_offset)
        // data_offset >= 0 and data_offset <= (effective_buf_len())
        let effective_buf_len =
            unsafe { access!(mbuf, buf_len, usize) - access!(mbuf, data_off, usize) };
        ensure!(data_offset <= effective_buf_len, "Data offset too large");

        unsafe {
            if USING_REF_COUNTING {
                rte_pktmbuf_refcnt_set(mbuf, 1)
            }
        };

        let len = match data_len {
            Some(x) => {
                ensure!(x <= (effective_buf_len - data_offset), "Data len too large");
                x
            }
            None => unsafe {
                // set to current data length - provided offset
                access!(mbuf, data_len, usize) - data_offset
            },
        };

        Ok(RteMbufMetadata {
            mbuf: mbuf,
            offset: data_offset,
            data_len: len,
        })
    }

    pub fn increment_refcnt(&mut self) {
        unsafe {
            if USING_REF_COUNTING {
                rte_pktmbuf_refcnt_update_or_free(self.mbuf, 1)
            }
        };
    }

    pub fn update_metadata(
        &mut self,
        pkt_len: u32,
        next: *mut rte_mbuf,
        lkey: Option<u32>,
        refers_to_another: bool,
        nb_segs: u16,
    ) {
        unsafe {
            (*self.mbuf).pkt_len = pkt_len;
            (*self.mbuf).next = next;
            (*self.mbuf).nb_segs = nb_segs;
            match lkey {
                Some(x) => {
                    set_lkey(self.mbuf, x);
                }
                None => {
                    set_lkey_not_present(self.mbuf);
                }
            }
            match refers_to_another {
                true => {
                    set_refers_to_another(self.mbuf, 1);
                }
                false => {
                    set_refers_to_another(self.mbuf, 2);
                }
            }
        }
    }

    pub fn get_inner(&self) -> *mut rte_mbuf {
        self.mbuf
    }

    unsafe fn effective_buf_len(&self) -> usize {
        access!(self.mbuf, buf_len, usize) - access!(self.mbuf, data_off, usize)
    }
}

impl MetadataOps for RteMbufMetadata {
    fn offset(&self) -> usize {
        self.offset
    }

    fn data_len(&self) -> usize {
        unsafe { access!(self.mbuf, data_len, usize) }
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

impl Default for RteMbufMetadata {
    fn default() -> Self {
        RteMbufMetadata {
            mbuf: ptr::null_mut(),
            offset: 0,
            data_len: 0,
        }
    }
}

impl Drop for RteMbufMetadata {
    fn drop(&mut self) {
        unsafe {
            // TODO: doesn't make sense to have this flag. should remove
            if USING_REF_COUNTING {
                rte_pktmbuf_refcnt_update_or_free(self.mbuf, -1);
            } else {
                rte_pktmbuf_free(self.mbuf);
            }
        }
    }
}

impl Clone for RteMbufMetadata {
    fn clone(&self) -> RteMbufMetadata {
        unsafe {
            if USING_REF_COUNTING {
                rte_pktmbuf_refcnt_update_or_free(self.mbuf, 1);
            }
        }
        RteMbufMetadata {
            mbuf: self.mbuf,
            offset: self.offset,
            data_len: self.data_len,
        }
    }
}

impl std::fmt::Debug for RteMbufMetadata {
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

impl AsRef<[u8]> for RteMbufMetadata {
    fn as_ref(&self) -> &[u8] {
        unsafe { mbuf_slice!(self.mbuf, self.offset, self.data_len) }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct DpdkPerThreadContext {
    /// Queue id
    queue_id: u16,
    /// Physical port
    physical_port: u16,
    /// Source address info
    address_info: AddressInfo,
    /// Basic mempool for receiving packets
    recv_mempool: *mut rte_mempool,
    /// Empty mempool for allocating external buffers
    extbuf_mempool: *mut rte_mempool,
}

unsafe impl Send for DpdkPerThreadContext {}
unsafe impl Sync for DpdkPerThreadContext {}

impl DpdkPerThreadContext {
    pub fn get_recv_mempool(&self) -> *mut rte_mempool {
        self.recv_mempool
    }

    pub fn get_extbuf_mempool(&self) -> *mut rte_mempool {
        self.extbuf_mempool
    }

    pub fn get_address_info(&self) -> &AddressInfo {
        &self.address_info
    }

    pub fn get_queue_id(&self) -> u16 {
        self.queue_id
    }

    pub fn get_physical_port(&self) -> u16 {
        self.physical_port
    }
}

impl Drop for DpdkPerThreadContext {
    fn drop(&mut self) {
        tracing::info!("In drop for dpdk  per thread context");
        unsafe {
            rte_mempool_free(self.extbuf_mempool);
        };
    }
}

#[derive(Debug, Clone)]
pub struct DpdkDatapathSpecificParams {
    eal_init: Vec<String>,
    dpdk_port: i16,
    our_ip: Ipv4Addr,
    our_eth: MacAddress,
    starting_client_port: u16,
    server_port: u16,
}

impl DpdkDatapathSpecificParams {
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

pub struct DpdkConnection {
    /// Per thread context
    thread_context: DpdkPerThreadContext,
    /// Server or client mode
    mode: AppMode,
    /// Current window of outstanding packets (used to keep track of rtts)
    outgoing_window: HashMap<(MsgID, ConnID), Instant>,
    /// Active connections: current connection IDs mapped to addresses.
    active_connections: [Option<(
        AddressInfo,
        [u8; cornflakes_libos::utils::TOTAL_UDP_HEADER_SIZE],
    )>; MAX_CONCURRENT_CONNECTIONS],
    /// Map from address info to connection id
    address_to_conn_id: HashMap<AddressInfo, ConnID>,
    /// Allocator for outgoing packets
    allocator: MemoryPoolAllocator<MempoolInfo>,
    /// Threshold for copying a segment or leaving as a separate scatter-gather entry
    copying_threshold: usize,
    /// Threshold for maximum segments when sending a scatter-gather array.
    max_segments: usize,
    /// Array of mbuf pointers used to receive packets
    recv_mbufs: [*mut rte_mbuf; RECEIVE_BURST_SIZE],
    /// Array of mbuf pointers used to send packets
    send_mbufs: [[*mut rte_mbuf; SEND_BURST_SIZE]; MAX_SCATTERS],
}

impl DpdkConnection {
    fn _debug_check_received_pkt(
        &mut self,
        i: usize,
        prev_id: Option<(usize, MsgID, ConnID)>,
        recv_mbufs: &[*mut rte_mbuf; RECEIVE_BURST_SIZE],
        num_received: u16,
    ) -> Result<Option<ReceivedPkt<Self>>> {
        let recv_mbuf = recv_mbufs[i];
        let eth_hdr = unsafe {
            mbuf_slice!(
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
            mbuf_slice!(
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
            mbuf_slice!(
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
            let slice = mbuf_slice!(
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

        let datapath_metadata = RteMbufMetadata::new(
            recv_mbuf,
            cornflakes_libos::utils::TOTAL_HEADER_SIZE,
            Some(data_len),
        )?;

        let received_pkt = ReceivedPkt::new(vec![datapath_metadata], msg_id, conn_id);
        Ok(Some(received_pkt))
    }

    fn check_received_pkt(&mut self, i: usize) -> Result<Option<ReceivedPkt<Self>>> {
        let recv_mbuf = self.recv_mbufs[i];
        let eth_hdr = unsafe {
            mbuf_slice!(
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
            mbuf_slice!(
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
            mbuf_slice!(
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

        tracing::debug!("Data len in udp hdr: {:?}", data_len);

        // check if this address info is within a current conn_id
        let src_addr = cornflakes_libos::utils::AddressInfo::new(src_port, src_ip, src_eth);
        let conn_id = self
            .connect(src_addr)
            .wrap_err("TOO MANY CONCURRENT CONNECTIONS")?;

        let msg_id = unsafe {
            let slice = mbuf_slice!(
                recv_mbuf,
                cornflakes_libos::utils::TOTAL_UDP_HEADER_SIZE,
                cornflakes_libos::utils::HEADER_ID_SIZE
            );
            cornflakes_libos::utils::parse_msg_id(slice)
        };

        let datapath_metadata = RteMbufMetadata::new(
            recv_mbuf,
            cornflakes_libos::utils::TOTAL_HEADER_SIZE,
            Some(data_len),
        )?;

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

    /// Takes zero-copy buffer (represented by RteMbufMetadata),
    /// copies data into an external mbuf, and updates all the relevant metadata.
    /// Arguments:
    /// @original_mbuf_metadata: RteMbufMetadata representing original mbuf, data_offset and
    /// data_len of application payload.
    /// @pkt_idx: index of packet in send mbufs (within burst).
    /// @nb_segs: Number of segments so far.
    fn place_zero_copy_buf_into_send_mbufs(
        &mut self,
        original_mbuf_metadata: &mut RteMbufMetadata,
        pkt_idx: usize,
        nb_segs: &mut usize,
        pkt_len: &mut usize,
    ) -> Result<()> {
        // allocate external buffer
        let external_mbuf = unsafe {
            let buf = rte_pktmbuf_alloc(self.thread_context.extbuf_mempool);
            if buf.is_null() {
                bail!("Could not allocate external buffer");
            }
            buf
        };
        // increment metadata of original mbuf
        original_mbuf_metadata.increment_refcnt();
        // update metadata in external buffer
        unsafe {
            (*external_mbuf).buf_iova = (*original_mbuf_metadata.get_inner()).buf_iova;
            (*external_mbuf).buf_addr = (*original_mbuf_metadata.get_inner()).buf_addr;
            (*external_mbuf).data_len = original_mbuf_metadata.data_len() as _;
            (*external_mbuf).data_off = (*original_mbuf_metadata.get_inner()).data_off
                + original_mbuf_metadata.offset() as u16;
            (*external_mbuf).next = ptr::null_mut();
            set_refers_to_another(external_mbuf, 1);
            (*external_mbuf).nb_segs = 1;
            set_lkey_not_present(external_mbuf);
        }

        self.send_mbufs[*nb_segs][pkt_idx] = external_mbuf;
        if *nb_segs > 0 {
            unsafe {
                (*(self.send_mbufs[*nb_segs - 1][pkt_idx])).next = external_mbuf;
            }
        }
        *nb_segs += 1;
        *pkt_len += original_mbuf_metadata.data_len();
        Ok(())
    }

    /// Consumes dpdk buffer (with new data), turns into RteMbufMetadata, increments ref count and metadata, and
    /// places buffer into send_mbufs
    /// Arguments:
    /// @dpdk_buffer: DpdkBuffer to consume
    /// @pkt_idx: index of packet in send mbufs array
    /// @nb_segs: Pointer to total number of segments so far in this packet
    /// @pkt_len: Pointer to packet len so far in this packet.
    fn place_copy_buf_into_send_mbufs(
        &mut self,
        dpdk_buffer: DpdkBuffer,
        pkt_idx: usize,
        nb_segs: &mut usize,
        pkt_len: &mut usize,
    ) -> Result<()> {
        let mut metadata_mbuf = RteMbufMetadata::from_dpdk_buf(dpdk_buffer);
        metadata_mbuf.increment_refcnt();
        metadata_mbuf.update_metadata(
            metadata_mbuf.data_len() as _, // pkt_len for this mbuf
            ptr::null_mut(),               // next pointer for this mbuf
            None,                          // lkey not present
            false,                         // refers to another = false
            1,                             // nb segs = 1
        );
        let mbuf = metadata_mbuf.get_inner();
        self.send_mbufs[*nb_segs][pkt_idx] = mbuf;
        // update next pointer for previous mbuf
        if *nb_segs > 0 {
            unsafe {
                (*(self.send_mbufs[*nb_segs - 1][pkt_idx])).next = mbuf;
            }
        }
        *nb_segs += 1;
        *pkt_len += cornflakes_libos::utils::TOTAL_HEADER_SIZE;
        Ok(())
    }
    fn write_header_and_return_new_buffer(
        &mut self,
        conn_id: ConnID,
        msg_id: MsgID,
        buffer_size: usize,
        data_len: usize,
    ) -> Result<DpdkBuffer> {
        let mut dpdk_buffer = match self.allocator.allocate_tx_buffer(buffer_size)? {
            Some(x) => x,
            None => {
                bail!("Error allocating mbuf to copy header into");
            }
        };
        let mutable_slice =
            dpdk_buffer.mutable_slice(0, cornflakes_libos::utils::TOTAL_HEADER_SIZE)?;
        self.copy_hdr(conn_id, msg_id, mutable_slice, data_len)?;
        Ok(dpdk_buffer)
    }

    fn zero_copy_rc_seg(&self, seg: &RcSge<Self>) -> bool {
        match seg {
            RcSge::RawRef(_) => false,
            RcSge::RefCounted(mbuf_metadata) => {
                mbuf_metadata.data_len() > self.copying_threshold
                    && self.allocator.is_registered(seg.as_ref())
            }
        }
    }

    fn zero_copy_seg(&self, seg: &Sge) -> bool {
        let buf = seg.addr();
        buf.len() >= self.copying_threshold && self.allocator.is_registered(buf)
    }

    fn post_sga(
        &mut self,
        posting_idx: usize,
        msg_id: MsgID,
        conn_id: ConnID,
        sga: &Sga,
    ) -> Result<()> {
        let mut sga_idx = 0;
        let mut written_header = false;
        let mut nb_segs = 0;
        let mut pkt_len = 0;
        let msg_size = sga.data_len();

        while sga_idx < sga.len() {
            let curr_seg = sga.get(sga_idx);
            if self.zero_copy_seg(curr_seg) {
                if !written_header {
                    let dpdk_buffer = self.write_header_and_return_new_buffer(
                        conn_id,
                        msg_id,
                        cornflakes_libos::utils::TOTAL_HEADER_SIZE,
                        msg_size,
                    )?;
                    self.place_copy_buf_into_send_mbufs(
                        dpdk_buffer,
                        posting_idx,
                        &mut nb_segs,
                        &mut pkt_len,
                    )?;
                    written_header = true;
                }
                // make a zero copy segment
                let mut original_mbuf_metadata =
                    match self.allocator.recover_buffer(sga.get(sga_idx).addr())? {
                        Some(x) => x,
                        None => {
                            bail!("Failed to recover mbuf metadata for given buffer");
                        }
                    };
                self.place_zero_copy_buf_into_send_mbufs(
                    &mut original_mbuf_metadata,
                    posting_idx,
                    &mut nb_segs,
                    &mut pkt_len,
                )?;
                sga_idx += 1;
            } else {
                let mut curr_idx = sga_idx;
                let mut data_segment_length = 0;
                while !self.zero_copy_seg(sga.get(curr_idx)) && curr_idx < sga.len() {
                    curr_idx += 1;
                    data_segment_length += sga.get(curr_idx).len();
                }

                let (mut copy_offset, mut dpdk_buffer) = match written_header {
                    false => {
                        written_header = true;
                        (
                            cornflakes_libos::utils::TOTAL_HEADER_SIZE,
                            self.write_header_and_return_new_buffer(
                                conn_id,
                                msg_id,
                                cornflakes_libos::utils::TOTAL_HEADER_SIZE + data_segment_length,
                                msg_size,
                            )?,
                        )
                    }
                    true => {
                        match self
                            .allocator
                            .allocate_buffer(data_segment_length)
                            .wrap_err("Could not allocate dpdk buffer to copy into")?
                        {
                            Some(x) => (0, x),
                            None => {
                                bail!("Error allocating mbuf to copy into");
                            }
                        }
                    }
                };

                for curr_seg_idx in sga_idx..curr_idx {
                    copy_offset +=
                        dpdk_buffer.copy_data(sga.get(curr_seg_idx).addr(), copy_offset)?;
                }

                self.place_copy_buf_into_send_mbufs(
                    dpdk_buffer,
                    posting_idx,
                    &mut nb_segs,
                    &mut pkt_len,
                )?;
                sga_idx = curr_idx
            }
        }

        // update metadata of first mbuf to reflect pkt_len and nb_segs
        unsafe {
            (*(self.send_mbufs[0][posting_idx])).pkt_len = pkt_len as _;
            (*(self.send_mbufs[0][posting_idx])).nb_segs = nb_segs as _;
        }
        Ok(())
    }

    fn post_rc_sga(
        &mut self,
        posting_idx: usize,
        msg_id: MsgID,
        conn_id: ConnID,
        rc_sga: &mut RcSga<Self>,
    ) -> Result<()> {
        let mut sga_idx = 0;
        let mut written_header = false;
        let mut nb_segs = 0;
        let mut pkt_len = 0;
        let data_len = rc_sga.data_len();

        while sga_idx < rc_sga.len() {
            let curr_seg = rc_sga.get_mut(sga_idx);
            if self.zero_copy_rc_seg(curr_seg) {
                if !written_header {
                    let dpdk_buffer = self.write_header_and_return_new_buffer(
                        conn_id,
                        msg_id,
                        cornflakes_libos::utils::TOTAL_HEADER_SIZE,
                        data_len,
                    )?;
                    self.place_copy_buf_into_send_mbufs(
                        dpdk_buffer,
                        posting_idx,
                        &mut nb_segs,
                        &mut pkt_len,
                    )?;
                    written_header = true;
                }
                // make a zero copy segment
                let mut original_mbuf_metadata = curr_seg.inner_datapath_pkt_mut().unwrap();
                self.place_zero_copy_buf_into_send_mbufs(
                    &mut original_mbuf_metadata,
                    posting_idx,
                    &mut nb_segs,
                    &mut pkt_len,
                )?;
                sga_idx += 1;
            } else {
                let mut curr_idx = sga_idx;
                let mut data_segment_length = 0;
                while !self.zero_copy_rc_seg(rc_sga.get(curr_idx)) && curr_idx < rc_sga.len() {
                    curr_idx += 1;
                    data_segment_length += rc_sga.get(curr_idx).len();
                }

                let (mut copy_offset, mut dpdk_buffer) = match written_header {
                    false => {
                        written_header = true;
                        (
                            cornflakes_libos::utils::TOTAL_HEADER_SIZE,
                            self.write_header_and_return_new_buffer(
                                conn_id,
                                msg_id,
                                cornflakes_libos::utils::TOTAL_HEADER_SIZE + data_segment_length,
                                data_len,
                            )?,
                        )
                    }
                    true => {
                        match self
                            .allocator
                            .allocate_buffer(data_segment_length)
                            .wrap_err("Could not allocate dpdk buffer to copy into")?
                        {
                            Some(x) => (0, x),
                            None => {
                                bail!("Error allocating mbuf to copy into");
                            }
                        }
                    }
                };

                for curr_seg_idx in sga_idx..curr_idx {
                    copy_offset +=
                        dpdk_buffer.copy_data(rc_sga.get(curr_seg_idx).addr(), copy_offset)?;
                }

                self.place_copy_buf_into_send_mbufs(
                    dpdk_buffer,
                    posting_idx,
                    &mut nb_segs,
                    &mut pkt_len,
                )?;
                sga_idx = curr_idx
            }
        }

        // update metadata of first mbuf to reflect pkt_len and nb_segs
        unsafe {
            (*(self.send_mbufs[0][posting_idx])).pkt_len = pkt_len as _;
            (*(self.send_mbufs[0][posting_idx])).nb_segs = nb_segs as _;
        }
        Ok(())
    }

    fn send_current_mbufs(&mut self, ct: u16) -> Result<()> {
        let mut num_sent: u16 = 0;
        for i in 0..ct {
            let head_mbuf = self.send_mbufs[0][i as usize];
            unsafe {
                tracing::debug!(pkt_len = access!(head_mbuf, pkt_len, usize), data_len = access!(head_mbuf, data_len, usize), nb_segs = access!(head_mbuf, nb_segs, usize), next=? access!(head_mbuf, next, *mut rte_mbuf), send_index = i, "Head mbuf metadata to send");
            }

            // TODO: also print the segments
        }
        while num_sent < ct {
            let mbuf_ptr = &mut self.send_mbufs[0][num_sent as usize] as _;
            let sent = unsafe {
                rte_eth_tx_burst(
                    self.thread_context.get_physical_port(),
                    self.thread_context.get_queue_id(),
                    mbuf_ptr,
                    ct - num_sent,
                )
            };
            num_sent += sent;
            if num_sent != ct {
                tracing::debug!(
                    "Failed to send {} mbufs, sent {}, {} so far",
                    ct,
                    sent,
                    num_sent
                );
            }
        }
        Ok(())
    }

    fn copy_hdr(
        &self,
        conn_id: ConnID,
        msg_id: MsgID,
        buffer: &mut [u8],
        data_len: usize,
    ) -> Result<()> {
        let hdr_bytes: &[u8; cornflakes_libos::utils::TOTAL_UDP_HEADER_SIZE] =
            match &self.active_connections[conn_id as usize] {
                Some((_, hdr_bytes_vec)) => hdr_bytes_vec,
                None => {
                    bail!("Could not find address for connID");
                }
            };
        unsafe {
            fill_in_hdrs_dpdk(
                buffer.as_mut_ptr() as _,
                hdr_bytes.as_ptr() as _,
                msg_id,
                data_len as _,
            );
        }
        Ok(())
    }
}

impl Datapath for DpdkConnection {
    type DatapathBuffer = DpdkBuffer;

    type DatapathMetadata = RteMbufMetadata;

    type PerThreadContext = DpdkPerThreadContext;

    type DatapathSpecificParams = DpdkDatapathSpecificParams;

    /// IP not required as dpdk has ability to auto-detect the ethernet address
    fn parse_config_file(
        config_file: &str,
        our_ip: &Ipv4Addr,
    ) -> Result<Self::DatapathSpecificParams> {
        let (ip_to_mac, _mac_to_ip, udp_port, client_port) =
            parse_yaml_map(config_file).wrap_err("Failed to parse yaml mapping")?;

        let eal_init = parse_eal_init(config_file)?;

        // since eal init has not been run yet, we cannot run dpdk get macaddr
        let eth_addr = match ip_to_mac.get(our_ip) {
            Some(e) => e.clone(),
            None => {
                bail!("Could not find eth addr for passed in ipv4 addr {:?} in config_file ip_to_mac map: {:?}", our_ip, ip_to_mac);
            }
        };

        Ok(DpdkDatapathSpecificParams {
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
        remote_ip: Option<Ipv4Addr>,
        app_mode: AppMode,
    ) -> Result<Vec<AddressInfo>> {
        let my_eth = datapath_params.get_mac();
        let my_starting_ip = datapath_params.get_ipv4();
        let my_port = match app_mode {
            AppMode::Server => datapath_params.get_server_port(),
            AppMode::Client => datapath_params.get_client_port(),
        };

        if app_mode == AppMode::Server {
            // TODO: implement more than one queue on the server side
            if num_queues > 1 {
                bail!("Currently, only 1 queue supported on server side");
            }

            let addr_info = AddressInfo::new(my_port, my_starting_ip, my_eth);
            return Ok(vec![addr_info]);
        }

        let server_ip = match remote_ip {
            Some(x) => x,
            None => {
                bail!("For client mode, must specify server ip to compute affinity");
            }
        };

        let server_port = datapath_params.get_server_port();

        let mut recv_addrs: Vec<AddressInfo> = Vec::with_capacity(num_queues);
        let in_recv_addrs = |ip: &[u8; 4], port: u16, ref_addrs: &Vec<AddressInfo>| -> bool {
            for current_addr in ref_addrs.iter() {
                if current_addr.ipv4_addr.octets() == *ip && current_addr.udp_port == port {
                    return true;
                }
            }
            return false;
        };

        for queue_id in 0..num_queues as u16 {
            let mut cur_octets = my_starting_ip.octets();
            let cur_port = my_port;
            while unsafe {
                compute_flow_affinity(
                    ip_from_octets(&cur_octets),
                    ip_from_octets(&server_ip.octets()),
                    cur_port,
                    server_port,
                    num_queues,
                )
            } != queue_id as u32
                || in_recv_addrs(&cur_octets, cur_port, &recv_addrs)
            {
                cur_octets[3] += 1;
            }
            tracing::info!(queue_id = queue_id, octets = ?cur_octets, port = cur_port, "Chosen addr pair");
            let queue_addr_info = AddressInfo::new(
                cur_port,
                Ipv4Addr::new(cur_octets[0], cur_octets[1], cur_octets[2], cur_octets[3]),
                my_eth,
            );
            recv_addrs.push(queue_addr_info);
        }

        Ok(recv_addrs)
    }

    fn global_init(
        num_queues: usize,
        datapath_params: &mut Self::DatapathSpecificParams,
        addresses: Vec<AddressInfo>,
    ) -> Result<Vec<Self::PerThreadContext>> {
        // run eal init
        let eal_args = datapath_params.get_eal_params();
        let mut args = vec![];
        let mut ptrs = vec![];
        for entry in eal_args.iter() {
            let s = CString::new(entry.as_str()).unwrap();
            ptrs.push(s.as_ptr() as *mut u8);
            args.push(s);
        }

        unsafe {
            let ret = rte_eal_init(ptrs.len() as i32, ptrs.as_mut_ptr() as *mut _);
            tracing::info!("Eal init returned {}", ret);
        }
        tracing::debug!("DPDK init args: {:?}", args);

        // Find and set physical port
        let nb_ports = unsafe { rte_eth_dev_count_avail() };
        if nb_ports <= 0 {
            bail!("DPDK GLOBAL INIT: No physical ports available.");
        }
        tracing::info!(
            "DPDK reports that {} ports (interfaces) are available",
            nb_ports
        );
        datapath_params.set_physical_port(nb_ports - 1);

        // for each core, initialize a native memory pool and external buffer memory pool
        let mut ret: Vec<Self::PerThreadContext> = Vec::with_capacity(num_queues);
        for (i, addr) in addresses.into_iter().enumerate() {
            let recv_mempool = create_recv_mempool(&format!("recv_mbuf_pool_{}", i)).wrap_err(
                format!("Not able create recv mbuf pool {} in global init", i),
            )?;

            let extbuf_mempool = create_extbuf_pool(&format!("extbuf_mempool_{}", i)).wrap_err(
                format!("Not able to create extbuf mempool {} in global init", i),
            )?;

            ret.push(DpdkPerThreadContext {
                queue_id: i as _,
                address_info: addr,
                recv_mempool: recv_mempool,
                extbuf_mempool: extbuf_mempool,
                physical_port: datapath_params.get_physical_port()?,
            });
        }

        assert!(unsafe { rte_eth_dev_is_valid_port(datapath_params.get_physical_port()?) == 1 });

        let mut rx_conf: MaybeUninit<rte_eth_rxconf> = MaybeUninit::zeroed();
        unsafe {
            (*rx_conf.as_mut_ptr()).rx_thresh.pthresh = RX_PTHRESH;
            (*rx_conf.as_mut_ptr()).rx_thresh.hthresh = RX_HTHRESH;
            (*rx_conf.as_mut_ptr()).rx_thresh.wthresh = RX_WTHRESH;
            (*rx_conf.as_mut_ptr()).rx_free_thresh = 32;
        }

        let mut tx_conf: MaybeUninit<rte_eth_txconf> = MaybeUninit::zeroed();
        unsafe {
            (*tx_conf.as_mut_ptr()).tx_thresh.pthresh = TX_PTHRESH;
            (*tx_conf.as_mut_ptr()).tx_thresh.hthresh = TX_HTHRESH;
            (*tx_conf.as_mut_ptr()).tx_thresh.wthresh = TX_WTHRESH;
        }

        unsafe {
            eth_dev_configure(
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
                per_thread_context.get_recv_mempool()
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

        // disable rx/tx flow control
        // TODO: why?

        let mut fc_conf: MaybeUninit<rte_eth_fc_conf> = MaybeUninit::zeroed();
        dpdk_check_not_errored!(rte_eth_dev_flow_ctrl_get(
            datapath_params.get_physical_port()?,
            fc_conf.as_mut_ptr()
        ));
        unsafe {
            (*fc_conf.as_mut_ptr()).mode = rte_eth_fc_mode_RTE_FC_NONE;
        }
        dpdk_check_not_errored!(rte_eth_dev_flow_ctrl_set(
            datapath_params.get_physical_port()?,
            fc_conf.as_mut_ptr()
        ));

        wait_for_link_status_up(datapath_params.get_physical_port()?)?;

        Ok(ret)
    }

    fn per_thread_init(
        _datapath_params: Self::DatapathSpecificParams,
        context: Self::PerThreadContext,
        mode: AppMode,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        let mut allocator = MemoryPoolAllocator::default();
        allocator.add_recv_mempool(MempoolInfo::new(context.get_recv_mempool())?);
        Ok(DpdkConnection {
            thread_context: context,
            mode: mode,
            outgoing_window: HashMap::default(),
            active_connections: [None; MAX_CONCURRENT_CONNECTIONS],
            address_to_conn_id: HashMap::default(),
            allocator: allocator,
            copying_threshold: 256,
            max_segments: 33,
            recv_mbufs: [ptr::null_mut(); RECEIVE_BURST_SIZE],
            send_mbufs: [[ptr::null_mut(); SEND_BURST_SIZE]; MAX_SCATTERS],
        })
    }

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

    fn push_buffers_with_copy(&mut self, pkts: &[(MsgID, ConnID, &[u8])]) -> Result<()> {
        for (i, (msg_id, conn_id, buf)) in pkts.iter().enumerate() {
            self.insert_into_outgoing_map(*msg_id, *conn_id);
            // allocate buffer to copy data into
            let mut dpdk_buffer = match self
                .allocator
                .allocate_buffer(buf.len() + cornflakes_libos::utils::TOTAL_HEADER_SIZE)
                .wrap_err(format!(
                    "Could not allocate buf to copy into for buf size {}",
                    buf.len()
                ))? {
                Some(buf) => buf,
                None => {
                    bail!(
                        "Could not allocate buffer to copy into for buf size: {}",
                        buf.len()
                    );
                }
            };
            // write header into the dpdk buffer
            let mut mutable_slice =
                dpdk_buffer.mutable_slice(0, cornflakes_libos::utils::TOTAL_HEADER_SIZE)?;
            self.copy_hdr(*conn_id, *msg_id, &mut mutable_slice, buf.len())
                .wrap_err("Could not copy header into mutable slice")?;
            dpdk_buffer.copy_data(buf, cornflakes_libos::utils::TOTAL_HEADER_SIZE)?;

            // turn dpdk buffer back into metadata object
            let mut metadata_mbuf = match self.get_metadata(dpdk_buffer)? {
                Some(x) => x,
                None => {
                    bail!("Failed to find corresponding metadata for allocated dpdk buffer")
                }
            };
            // update metadata on packet required to send out
            metadata_mbuf.update_metadata(
                (buf.len() + cornflakes_libos::utils::TOTAL_HEADER_SIZE) as _,
                ptr::null_mut(),
                None,
                false,
                1,
            );

            // update refcount, because this RteMbufMetadata will be dropped after this loop
            metadata_mbuf.increment_refcnt();

            self.send_mbufs[0][i] = metadata_mbuf.get_inner();
        }

        self.send_current_mbufs(pkts.len() as _)
            .wrap_err("Could not send mbufs in push buffers with copy function")?;

        Ok(())
    }

    fn echo(&mut self, pkts: Vec<ReceivedPkt<Self>>) -> Result<()>
    where
        Self: Sized,
    {
        for (i, pkt) in pkts.iter().enumerate() {
            for (scatter_index, dpdk_buffer) in pkt.iter().enumerate() {
                let mbuf = dpdk_buffer.get_inner();
                tracing::debug!(
                    "Echoing packet with id {}, mbuf addr {:?}, refcnt {}",
                    pkt.msg_id(),
                    mbuf,
                    unsafe { access!(mbuf, refcnt, u16) }
                );

                // flip headers on packet
                if scatter_index == 0 {
                    // flip headers assumes the header is right at the buf addr of the packet
                    unsafe {
                        flip_headers(mbuf, pkt.msg_id());
                    }
                }
                // increment ref count so it does not get dropped here
                unsafe {
                    rte_pktmbuf_refcnt_update_or_free(mbuf, 1);
                }
                self.send_mbufs[scatter_index as usize][i as usize] = mbuf;
            }
        }
        self.send_current_mbufs(pkts.len() as u16)
            .wrap_err("Failed to send packets from echo")?;
        Ok(())
    }

    fn push_rc_sgas(&mut self, rc_sgas: &mut [(MsgID, ConnID, RcSga<Self>)]) -> Result<()>
    where
        Self: Sized,
    {
        for (pkt_idx, (msg, conn, rc_sga)) in rc_sgas.iter_mut().enumerate() {
            self.insert_into_outgoing_map(*msg, *conn);
            self.post_rc_sga(pkt_idx, *msg, *conn, rc_sga)
                .wrap_err(format!("Failed to process sga {}", pkt_idx))?;
        }
        // send all current mbufs
        self.send_current_mbufs(rc_sgas.len() as _)
            .wrap_err(format!("Failed to send current mbufs"))?;
        Ok(())
    }

    fn push_ordered_sgas(&mut self, _ordered_sgas: &[(MsgID, ConnID, OrderedSga)]) -> Result<()> {
        unimplemented!();
    }

    fn push_sgas(&mut self, sgas: &[(MsgID, ConnID, Sga)]) -> Result<()> {
        for (pkt_idx, (msg, conn, sga)) in sgas.iter().enumerate() {
            self.insert_into_outgoing_map(*msg, *conn);
            self.post_sga(pkt_idx, *msg, *conn, sga)
                .wrap_err(format!("Failed to process sga {}", pkt_idx))?;
        }
        // send all current mbufs
        self.send_current_mbufs(sgas.len() as _)
            .wrap_err(format!("Failed to send current mbufs"))?;
        Ok(())
    }

    fn pop_with_durations(&mut self) -> Result<Vec<(ReceivedPkt<Self>, Duration)>>
    where
        Self: Sized,
    {
        let num_received = unsafe {
            rte_eth_rx_burst(
                self.thread_context.get_physical_port(),
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
                            rte_pktmbuf_free(self.recv_mbufs[i]);
                        }
                    }
                }
            } else {
                tracing::debug!("Received invalid packet at addr {:?}", self.recv_mbufs[i]);
                unsafe {
                    rte_pktmbuf_free(self.recv_mbufs[i]);
                }
            }
            self.recv_mbufs[i] = ptr::null_mut();
        }
        Ok(ret)
    }

    fn pop(&mut self) -> Result<Vec<ReceivedPkt<Self>>>
    where
        Self: Sized,
    {
        let num_received = unsafe {
            rte_eth_rx_burst(
                self.thread_context.get_physical_port(),
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
                    rte_pktmbuf_free(self.recv_mbufs[i]);
                }
            }
            self.recv_mbufs[i] = ptr::null_mut();
        }
        Ok(ret)
    }

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

    fn is_registered(&self, buf: &[u8]) -> bool {
        self.allocator.is_registered(buf)
    }

    fn allocate(&mut self, size: usize) -> Result<Option<Self::DatapathBuffer>> {
        self.allocator.allocate_buffer(size)
    }

    fn get_metadata(&self, buf: Self::DatapathBuffer) -> Result<Option<Self::DatapathMetadata>> {
        Ok(Some(RteMbufMetadata::from_dpdk_buf(buf)))
    }

    fn recover_metadata(&self, buf: &[u8]) -> Result<Option<Self::DatapathMetadata>> {
        self.allocator.recover_metadata(buf)
    }

    fn add_tx_mempool(&mut self, value_size: usize, min_elts: usize) -> Result<()> {
        let name = format!(
            "thread_{}_mempool_id_{}",
            self.thread_context.get_queue_id(),
            self.allocator.num_mempools_so_far()
        );
        tracing::info!(name = ?name, value_size, min_elts, "Adding mempool");
        let (num_values, num_mempools) = {
            let log2 = (min_elts as f64).log2().ceil() as u32;
            let num_elts = usize::pow(2, log2) as usize;
            let num_mempools = {
                if num_elts < MEMPOOL_MAX_SIZE {
                    1
                } else {
                    num_elts / MEMPOOL_MAX_SIZE
                }
            };
            (num_elts / num_mempools, num_mempools)
        };
        let actual_value_size = align_up(value_size + RTE_PKTMBUF_HEADROOM as usize, 256);
        tracing::info!(
            "Creating {} mempools of amount {}, actual value size {}",
            num_mempools,
            num_values,
            actual_value_size
        );
        for i in 0..num_mempools {
            let mempool_name = format!("{}_{}", name, i);
            let mempool = create_mempool(
                &mempool_name,
                1,
                actual_value_size,
                num_values - 1,
            )
            .wrap_err(format!(
                "Unable to add mempool {:?} to mempool allocator; actual value_size {}, num_values {}",
                name,actual_value_size, num_values
            ))?;
            tracing::debug!(
                "Created mempool avail count: {}, in_use count: {}",
                unsafe { rte_mempool_avail_count(mempool) },
                unsafe { rte_mempool_in_use_count(mempool) },
            );
            let _ = self
                .allocator
                .add_tx_mempool(actual_value_size, MempoolInfo::new(mempool)?);
        }
        Ok(())
    }

    fn add_memory_pool(&mut self, value_size: usize, min_elts: usize) -> Result<Vec<MempoolID>> {
        let mut ret: Vec<MempoolID> = Vec::default();
        //let num_values = (min_num_values as f64 * 1.20) as usize;
        // find optimal number of values above this
        let name = format!(
            "thread_{}_mempool_id_{}",
            self.thread_context.get_queue_id(),
            self.allocator.num_mempools_so_far()
        );
        tracing::info!(name = ?name, value_size, min_elts, "Adding mempool");
        let (num_values, num_mempools) = {
            let log2 = (min_elts as f64).log2().ceil() as u32;
            let num_elts = usize::pow(2, log2) as usize;
            let num_mempools = {
                if num_elts < MEMPOOL_MAX_SIZE {
                    1
                } else {
                    num_elts / MEMPOOL_MAX_SIZE
                }
            };
            (num_elts / num_mempools, num_mempools)
        };
        let actual_value_size = align_up(value_size + RTE_PKTMBUF_HEADROOM as usize, 256);
        tracing::info!(
            "Creating {} mempools of amount {}, actual value size {}",
            num_mempools,
            num_values,
            actual_value_size
        );
        for i in 0..num_mempools {
            let mempool_name = format!("{}_{}", name, i);
            let mempool = create_mempool(
                &mempool_name,
                1,
                actual_value_size,
                num_values - 1,
            )
            .wrap_err(format!(
                "Unable to add mempool {:?} to mempool allocator; actual value_size {}, num_values {}",
                name,actual_value_size, num_values
            ))?;
            tracing::debug!(
                "Created mempool avail count: {}, in_use count: {}",
                unsafe { rte_mempool_avail_count(mempool) },
                unsafe { rte_mempool_in_use_count(mempool) },
            );
            let id = self
                .allocator
                .add_mempool(actual_value_size, MempoolInfo::new(mempool)?)
                .wrap_err(format!(
                    "Unable to add mempool {:?} to mempool allocator; value_size {}, num_values {}",
                    name, actual_value_size, num_values
                ))?;
            ret.push(id);
        }
        Ok(ret)
    }

    fn register_mempool(&mut self, id: MempoolID) -> Result<()> {
        self.allocator.register(id, ())
    }

    fn unregister_mempool(&mut self, id: MempoolID) -> Result<()> {
        self.allocator.unregister(id)
    }

    fn header_size(&self) -> usize {
        cornflakes_libos::utils::TOTAL_HEADER_SIZE
    }

    fn timer_hz(&self) -> u64 {
        unsafe { rte_get_timer_hz() }
    }

    fn cycles_to_ns(&self, t: u64) -> u64 {
        unsafe { ((t * rte_get_timer_hz()) as f64 / 1_000_000_000.0) as u64 }
    }

    fn current_cycles(&self) -> u64 {
        unsafe { rte_get_timer_cycles() }
    }

    fn set_copying_threshold(&mut self, thresh: usize) {
        self.copying_threshold = thresh;
    }

    fn get_copying_threshold(&self) -> usize {
        self.copying_threshold
    }

    fn set_max_segments(&mut self, segs: usize) {
        self.max_segments = segs;
    }

    fn get_max_segments(&self) -> usize {
        self.max_segments
    }
    fn set_inline_mode(&mut self, _inline_mode: InlineMode) {}
}
