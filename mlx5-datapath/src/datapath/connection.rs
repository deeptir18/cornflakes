use super::{
    super::{access, check_ok, mlx5_bindings::*},
    allocator::DataMempool,
    check, sizes,
};
use cornflakes_libos::{
    allocator::{MemoryPoolAllocator, MempoolID},
    datapath::{Datapath, DatapathBufferOps, InlineMode, MetadataOps, ReceivedPkt},
    dynamic_rcsga_hybrid_hdr::HybridArenaRcSgaHdr,
    dynamic_sga_hdr::SgaHeaderRepr,
    mem::PGSIZE_2MB,
    utils::AddressInfo,
    ArenaDatapathSga, ArenaOrderedRcSga, ArenaOrderedSga, ConnID, CopyContext, MsgID, OrderedRcSga,
    OrderedSga, RcSga, RcSge, SerializationInfo, Sga,
};

use color_eyre::eyre::{bail, ensure, Result, WrapErr};
use cornflakes_utils::{parse_yaml_map, AppMode};
use eui48::MacAddress;
use hashbrown::HashMap;
use std::{
    boxed::Box,
    ffi::CString,
    fs::read_to_string,
    io::Write,
    mem::MaybeUninit,
    net::Ipv4Addr,
    path::Path,
    ptr,
    sync::Arc,
    time::{Duration, Instant},
};
use yaml_rust::{Yaml, YamlLoader};

const MAX_CONCURRENT_CONNECTIONS: usize = 128;
const COMPLETION_BUDGET: usize = 32;
const RECEIVE_BURST_SIZE: usize = 32;
const MAX_BUFFER_SIZE: usize = 16384;
const MEMPOOL_MIN_ELTS: usize = 8192;

#[derive(PartialEq, Eq)]
pub struct Mlx5Buffer {
    /// Underlying data pointer
    data: *mut ::std::os::raw::c_void,
    /// Pointer back to the data and metadata pool pair
    mempool: *mut registered_mempool,
    /// Refcnt index
    refcnt_index: usize,
    /// Data len,
    data_len: usize,
}

impl Clone for Mlx5Buffer {
    fn clone(&self) -> Self {
        if self.data != std::ptr::null_mut() {
            unsafe {
                custom_mlx5_refcnt_update_or_free(
                    self.mempool,
                    self.data,
                    self.refcnt_index as _,
                    1i8,
                );
            }
        }
        Mlx5Buffer {
            data: self.data,
            mempool: self.mempool,
            refcnt_index: self.refcnt_index,
            data_len: self.data_len,
        }
    }
}

impl Default for Mlx5Buffer {
    fn default() -> Self {
        Mlx5Buffer {
            data: std::ptr::null_mut(),
            mempool: std::ptr::null_mut(),
            refcnt_index: 0,
            data_len: 0,
        }
    }
}

impl Drop for Mlx5Buffer {
    fn drop(&mut self) {
        if self.data == std::ptr::null_mut() || self.mempool == std::ptr::null_mut() {
            return;
        }
        // Decrements ref count on underlying metadata
        unsafe {
            custom_mlx5_refcnt_update_or_free(
                self.mempool,
                self.data,
                self.refcnt_index as _,
                -1i8,
            );
        }
    }
}

impl Mlx5Buffer {
    pub fn new(
        data: *mut ::std::os::raw::c_void,
        mempool: *mut registered_mempool,
        index: usize,
        data_len: usize,
    ) -> Self {
        unsafe {
            custom_mlx5_refcnt_update_or_free(mempool, data, index as _, 1);
        }
        Mlx5Buffer {
            data: data,
            mempool: mempool,
            refcnt_index: index,
            data_len: data_len,
        }
    }

    pub fn read_refcnt(&self) -> u16 {
        unsafe { custom_mlx5_refcnt_read(self.mempool, self.refcnt_index as _) }
    }

    pub fn update_refcnt(&mut self, change: i8) {
        unsafe {
            custom_mlx5_refcnt_update_or_free(
                self.mempool,
                self.data,
                self.refcnt_index as _,
                change,
            );
        }
    }

    pub fn get_inner_ref(
        &self,
    ) -> (
        *mut ::std::os::raw::c_void,
        *mut registered_mempool,
        usize,
        usize,
    ) {
        (self.data, self.mempool, self.refcnt_index, self.data_len)
    }
    pub fn get_inner(
        self,
    ) -> (
        *mut ::std::os::raw::c_void,
        *mut registered_mempool,
        usize,
        usize,
    ) {
        (self.data, self.mempool, self.refcnt_index, self.data_len)
    }

    pub fn get_mempool(&self) -> *mut registered_mempool {
        self.mempool
    }

    pub fn mutable_slice(&mut self, start: usize, end: usize) -> Result<&mut [u8]> {
        tracing::debug!(
            start = start,
            end = end,
            item_len = unsafe { access!(get_data_mempool(self.mempool), item_len, usize) },
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

    pub fn set_len(&mut self, len: usize) {
        self.data_len = len;
    }
}

impl DatapathBufferOps for Mlx5Buffer {
    fn set_len(&mut self, len: usize) {
        self.data_len = len;
    }

    fn get_mutable_slice(&mut self, start: usize, len: usize) -> Result<&mut [u8]> {
        self.mutable_slice(start, start + len)
    }
}

impl std::fmt::Debug for Mlx5Buffer {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Mbuf addr: {:?}, data_len: {}", self.data, self.data_len)
    }
}
impl AsRef<[u8]> for Mlx5Buffer {
    fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data as *mut u8, self.data_len) }
    }
}

impl std::io::Write for Mlx5Buffer {
    #[inline]
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let item_len = unsafe { access!(get_data_mempool(self.mempool), item_len, usize) };
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

/// MbufMetadata struct wraps around mbuf data structure.
/// Points to metadata object, which further contains a pointer to:
/// (a) Actual data (which could be allocated separately from mbuf metadata)
/// (b) Another mbuf metadata.
#[derive(PartialEq, Eq)]
pub struct MbufMetadata {
    /// Pointer to beginning of allocated data.
    pub data: *mut ::std::os::raw::c_void,
    /// Pointer to registered mempool.
    pub mempool: *mut registered_mempool,
    /// Reference count index.
    pub refcnt_index: usize,
    /// Application data offset
    pub offset: usize,
    /// Application data length
    pub len: usize,
}

impl MbufMetadata {
    pub fn new(
        data: *mut ::std::os::raw::c_void,
        mempool: *mut registered_mempool,
        refcnt_index: usize,
        offset: usize,
        len: usize,
    ) -> Self {
        if data != std::ptr::null_mut() {
            unsafe {
                custom_mlx5_refcnt_update_or_free(mempool, data, refcnt_index as _, 1i8);
            }
        }
        MbufMetadata {
            data: data,
            mempool: mempool,
            refcnt_index: refcnt_index,
            offset: offset,
            len: len,
        }
    }

    pub fn from_buf(mut mlx5_buffer: Mlx5Buffer) -> Self {
        mlx5_buffer.update_refcnt(1);
        let (buf, registered_mempool, refcnt_index, data_len) = mlx5_buffer.get_inner();
        MbufMetadata {
            data: buf,
            mempool: registered_mempool,
            refcnt_index: refcnt_index,
            offset: 0,
            len: data_len,
        }
    }

    pub fn increment_refcnt(&mut self) {
        unsafe {
            custom_mlx5_refcnt_update_or_free(self.mempool, self.data, self.refcnt_index as _, 1i8);
        }
    }

    pub fn mempool(&self) -> *mut registered_mempool {
        self.mempool
    }

    pub fn data(&self) -> *mut ::std::os::raw::c_void {
        self.data
    }
}

impl MetadataOps for MbufMetadata {
    fn get_refcnt(&self) -> u16 {
        unsafe { custom_mlx5_refcnt_read(self.mempool, self.refcnt_index as _) }
    }

    fn offset(&self) -> usize {
        self.offset
    }

    fn data_len(&self) -> usize {
        self.len
    }

    fn set_data_len_and_offset(&mut self, len: usize, offset: usize) -> Result<()> {
        let item_len = unsafe { (*self.mempool).data_mempool.item_len };
        ensure!(offset <= item_len as _, "Offset too large");
        ensure!(
            (offset + len) <= item_len as _,
            "Provided data len too large"
        );
        self.offset = offset;
        self.len = len;
        Ok(())
    }
}

// TODO: might be safest not to have a default function
impl Default for MbufMetadata {
    fn default() -> Self {
        MbufMetadata {
            data: ptr::null_mut(),
            mempool: ptr::null_mut(),
            refcnt_index: 0,
            offset: 0,
            len: 0,
        }
    }
}

impl Drop for MbufMetadata {
    fn drop(&mut self) {
        if self.data == std::ptr::null_mut() || self.mempool == std::ptr::null_mut() {
            return;
        }
        unsafe {
            custom_mlx5_refcnt_update_or_free(
                self.mempool,
                self.data,
                self.refcnt_index as _,
                -1i8,
            );
        }
    }
}

impl Clone for MbufMetadata {
    fn clone(&self) -> MbufMetadata {
        MbufMetadata::new(
            self.data,
            self.mempool,
            self.refcnt_index,
            self.offset,
            self.len,
        )
    }
}

impl std::fmt::Debug for MbufMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "Data addr: {:?}, mempool: {:?}, refcnt_index: {}, off: {}, len: {}",
            self.data, self.mempool, self.refcnt_index, self.offset, self.len
        )
    }
}

impl AsRef<[u8]> for MbufMetadata {
    fn as_ref(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                (self.data as *mut u8).offset(self.offset as isize),
                self.len,
            )
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
struct Mlx5GlobalContext {
    global_context_ptr: *mut [u8],
    thread_context_ptr: *mut [u8],
}

impl Mlx5GlobalContext {
    fn ptr(&self) -> *mut custom_mlx5_global_context {
        self.global_context_ptr as *mut custom_mlx5_global_context
    }

    fn global_context_ptr(&self) -> *mut [u8] {
        self.global_context_ptr
    }

    fn thread_context_ptr(&self) -> *mut [u8] {
        self.thread_context_ptr
    }
}

unsafe impl Send for Mlx5GlobalContext {}
unsafe impl Sync for Mlx5GlobalContext {}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Mlx5PerThreadContext {
    /// Reference counted version of global context pointer
    global_context_rc: Arc<Mlx5GlobalContext>,
    /// Queue id
    queue_id: u16,
    /// Source address info (ethernet, ip, port)
    address_info: AddressInfo,
    /// Pointer to datapath specific thread information
    context: *mut custom_mlx5_per_thread_context,
    /// Receive mempool ptr
    rx_mempool_ptr: *mut [u8],
}

unsafe impl Send for Mlx5PerThreadContext {}
unsafe impl Sync for Mlx5PerThreadContext {}

impl Mlx5PerThreadContext {
    pub fn get_context_ptr(&self) -> *mut custom_mlx5_per_thread_context {
        self.context
    }

    pub fn get_recv_mempool_ptr(&self) -> *mut [u8] {
        self.rx_mempool_ptr
    }

    pub fn get_queue_id(&self) -> u16 {
        self.queue_id
    }

    pub fn get_address_info(&self) -> &AddressInfo {
        &self.address_info
    }
}

impl Drop for Mlx5PerThreadContext {
    fn drop(&mut self) {
        unsafe {
            custom_mlx5_teardown(self.context);
        }
        if Arc::<Mlx5GlobalContext>::strong_count(&self.global_context_rc) == 1 {
            // safe to drop global context because this is the last reference

            let thread_ptr = unsafe {
                (*Arc::<Mlx5GlobalContext>::as_ptr(&self.global_context_rc)).thread_context_ptr()
            };

            let global_context_ptr = unsafe {
                (*Arc::<Mlx5GlobalContext>::as_ptr(&self.global_context_rc)).global_context_ptr()
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
pub struct Mlx5DatapathSpecificParams {
    custom_mlx5_pci_addr: MaybeUninit<custom_mlx5_pci_addr>,
    eth_addr: MaybeUninit<eth_addr>,
    our_ip: Ipv4Addr,
    our_eth: MacAddress,
    client_port: u16,
    server_port: u16,
}

impl Mlx5DatapathSpecificParams {
    pub unsafe fn get_custom_mlx5_pci_addr(&mut self) -> *mut custom_mlx5_pci_addr {
        self.custom_mlx5_pci_addr.as_mut_ptr()
    }

    pub unsafe fn get_eth_addr(&mut self) -> *mut eth_addr {
        self.eth_addr.as_mut_ptr()
    }

    pub fn get_ipv4(&self) -> Ipv4Addr {
        self.our_ip.clone()
    }

    pub fn get_mac(&self) -> MacAddress {
        self.our_eth.clone()
    }

    pub fn get_client_port(&self) -> u16 {
        self.client_port
    }

    pub fn get_server_port(&self) -> u16 {
        self.server_port
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct RecvMbufArray {
    array_ptr: *mut [u8],
}

impl RecvMbufArray {
    pub fn new(size: usize) -> RecvMbufArray {
        let array_ptr_box =
            vec![0u8; size * std::mem::size_of::<recv_mbuf_info>()].into_boxed_slice();
        let array_ptr = Box::<[u8]>::into_raw(array_ptr_box);
        for i in 0..size {
            let ptr = unsafe {
                (array_ptr as *mut u8).offset((i * std::mem::size_of::<recv_mbuf_info>()) as isize)
                    as *mut recv_mbuf_info
            };
            unsafe {
                (*ptr).buf_addr = ptr::null_mut();
                (*ptr).mempool = ptr::null_mut();
                (*ptr).ref_count_index = 0;
                (*ptr).rss_hash = 0;
            }
        }
        RecvMbufArray {
            array_ptr: array_ptr,
        }
    }

    pub fn as_recv_mbuf_info_array_ptr(&self) -> *mut recv_mbuf_info {
        self.array_ptr as *mut recv_mbuf_info
    }

    pub fn get(&self, i: usize) -> *mut recv_mbuf_info {
        unsafe {
            (self.array_ptr as *mut u8).offset((i * std::mem::size_of::<recv_mbuf_info>()) as isize)
                as *mut recv_mbuf_info
        }
    }

    pub fn clear(&mut self, i: usize) {
        let ptr = unsafe {
            (self.array_ptr as *mut u8).offset((i * std::mem::size_of::<recv_mbuf_info>()) as isize)
                as *mut recv_mbuf_info
        };
        unsafe {
            (*ptr).buf_addr = ptr::null_mut();
            (*ptr).mempool = ptr::null_mut();
            (*ptr).ref_count_index = 0;
            (*ptr).rss_hash = 0;
        }
    }
}

impl Drop for RecvMbufArray {
    fn drop(&mut self) {
        unsafe {
            let _ = Box::from_raw(self.array_ptr);
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Mlx5Connection {
    /// Per thread context.
    thread_context: Mlx5PerThreadContext,
    /// Server or client mode
    mode: AppMode,
    /// Current window of outstanding packets (used for keeping track of RTTs).
    outgoing_window: HashMap<(MsgID, ConnID), Instant>,
    /// Active connections:  current connection IDs mapped to addresses.
    /// TODO: write code to make it array instead of vector.
    active_connections: [Option<(
        AddressInfo,
        [u8; cornflakes_libos::utils::TOTAL_UDP_HEADER_SIZE],
    )>; MAX_CONCURRENT_CONNECTIONS],
    /// Map from AddressInfo to connection id
    address_to_conn_id: HashMap<AddressInfo, ConnID>,
    /// Allocator for outgoing mbufs and packets.
    allocator: MemoryPoolAllocator<DataMempool>,
    /// Threshold for copying a segment or leaving as a separate scatter-gather entry.
    copying_threshold: usize,
    /// Threshold for max number of segments when sending.
    max_segments: usize,
    /// Inline mode,
    inline_mode: InlineMode,
    /// Maximum data that can be inlined
    max_inline_size: usize,
    /// Array of mbuf pointers used to receive packets
    recv_mbufs: RecvMbufArray,
    /// Data used to construct outgoing segment
    first_ctrl_seg: *mut mlx5_wqe_ctrl_seg,
    /// Array of mbuf metadatas to store while finishing serialization
    mbuf_metadatas: [Option<MbufMetadata>; 32],
    /// header buffer to use while posting entries
    header_buffer: Vec<u8>,
}

impl Mlx5Connection {
    fn process_warmup_noop(&mut self, pkt: ReceivedPkt<Self>) -> Result<()> {
        self.echo(vec![pkt])?;
        Ok(())
    }

    fn finish_transmission(&mut self, num_required: usize, end_batch: bool) -> Result<()> {
        // finish the transmission
        unsafe {
            custom_mlx5_finish_single_transmission(
                self.thread_context.get_context_ptr(),
                num_required as _,
            );
        }

        if end_batch {
            if !self.first_ctrl_seg.is_null() {
                let _ = self.post_curr_transmissions(Some(self.first_ctrl_seg));
                self.poll_for_completions()?;
                self.first_ctrl_seg = ptr::null_mut();
            }
        }
        Ok(())
    }

    fn post_ctrl_segment(
        &mut self,
        num_required: usize,
        inline_len: usize,
        total_num_entries: usize,
    ) -> Result<()> {
        // fill in hdr segments
        let num_octowords =
            unsafe { custom_mlx5_num_octowords(inline_len as _, total_num_entries as _) };
        tracing::debug!(
            inline_len,
            total_num_entries,
            num_octowords,
            num_required,
            "Header info"
        );
        let ctrl_seg = unsafe {
            custom_mlx5_fill_in_hdr_segment(
                self.thread_context.get_context_ptr(),
                num_octowords as _,
                num_required as _,
                inline_len as _,
                total_num_entries as _,
                MLX5_ETH_WQE_L3_CSUM as i32 | MLX5_ETH_WQE_L4_CSUM as i32,
            )
        };
        if ctrl_seg.is_null() {
            bail!("Error posting header segment for sga");
        }

        if self.first_ctrl_seg == ptr::null_mut() {
            self.first_ctrl_seg = ctrl_seg;
        }
        Ok(())
    }
    /// Post current transmissions and return number of available wqes.
    fn post_curr_transmissions_and_get_available_wqes(&mut self) -> Result<usize> {
        if self.first_ctrl_seg != ptr::null_mut() {
            if unsafe {
                custom_mlx5_post_transmissions(
                    self.thread_context.get_context_ptr(),
                    self.first_ctrl_seg,
                ) != 0
            } {
                bail!("Failed to post transmissions so far");
            } else {
                self.first_ctrl_seg = ptr::null_mut();
            }
        }
        self.poll_for_completions()?;
        Ok(
            unsafe { custom_mlx5_num_wqes_available(self.thread_context.get_context_ptr()) }
                as usize,
        )
    }

    fn insert_into_outgoing_map(&mut self, msg_id: MsgID, conn_id: ConnID) {
        if self.mode == AppMode::Client {
            if !self.outgoing_window.contains_key(&(msg_id, conn_id)) {
                self.outgoing_window
                    .insert((msg_id, conn_id), Instant::now());
            }
        }
    }

    fn check_received_pkt(&mut self, pkt_index: usize) -> Result<Option<ReceivedPkt<Self>>> {
        let recv_mbuf = self.recv_mbufs.get(pkt_index);
        let eth_hdr = unsafe {
            recv_mbuf_slice!(
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
            recv_mbuf_slice!(
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
            recv_mbuf_slice!(
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
            let slice = recv_mbuf_slice!(
                recv_mbuf,
                cornflakes_libos::utils::TOTAL_UDP_HEADER_SIZE,
                cornflakes_libos::utils::HEADER_ID_SIZE
            );
            cornflakes_libos::utils::parse_msg_id(&slice)
        };

        ensure!(
            data_len
                == unsafe {
                    (*recv_mbuf).pkt_len as usize - cornflakes_libos::utils::TOTAL_HEADER_SIZE
                },
            format!(
                "Data len in udp header {} doesn't match pkt_len minus header size {}",
                data_len,
                unsafe {
                    (*recv_mbuf).pkt_len as usize - cornflakes_libos::utils::TOTAL_HEADER_SIZE
                }
            )
        );
        let datapath_metadata = MbufMetadata::new(
            unsafe { (*recv_mbuf).buf_addr },
            unsafe { (*recv_mbuf).mempool },
            unsafe { (*recv_mbuf).ref_count_index as _ },
            cornflakes_libos::utils::TOTAL_HEADER_SIZE,
            data_len,
        );
        tracing::debug!(
            data_len = data_len,
            msg_id = msg_id,
            conn_id = conn_id,
            "Received packet"
        );

        let received_pkt = ReceivedPkt::new(vec![datapath_metadata], msg_id, conn_id);
        Ok(Some(received_pkt))
    }

    fn zero_copy_rc_seg(&self, seg: &RcSge<Self>) -> bool {
        match seg {
            RcSge::RawRef(_) => false,
            RcSge::RefCounted(metadata) => metadata.data_len() >= self.copying_threshold,
        }
    }

    fn zero_copy_seg(&self, seg: &[u8]) -> bool {
        #[cfg(feature = "profiler")]
        demikernel::timer!("Zero copy seg function");
        return seg.len() >= self.copying_threshold && self.is_registered(seg);
    }

    fn copy_hdr(
        &self,
        data_buffer: &mut Mlx5Buffer,
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
            fill_in_hdrs(
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

    fn inline_hdr(
        &self,
        conn_id: ConnID,
        msg_id: MsgID,
        inline_len: usize,
        data_len: usize,
    ) -> Result<()> {
        // inline ethernet header
        let hdr_bytes: &[u8; cornflakes_libos::utils::TOTAL_UDP_HEADER_SIZE] =
            match &self.active_connections[conn_id as usize] {
                Some((_, hdr_bytes_vec)) => hdr_bytes_vec,
                None => {
                    bail!("Could not find address for connID");
                }
            };

        let eth_hdr = hdr_bytes[0..cornflakes_libos::utils::ETHERNET2_HEADER2_SIZE].as_ptr()
            as *const eth_hdr;
        unsafe {
            custom_mlx5_inline_eth_hdr(
                self.thread_context.get_context_ptr(),
                eth_hdr,
                inline_len as _,
            );
        }

        // inline ip hdr
        let ip_hdr = hdr_bytes[cornflakes_libos::utils::ETHERNET2_HEADER2_SIZE
            ..(cornflakes_libos::utils::IPV4_HEADER2_SIZE
                + cornflakes_libos::utils::ETHERNET2_HEADER2_SIZE)]
            .as_ptr() as *const ip_hdr;
        unsafe {
            custom_mlx5_inline_ipv4_hdr(
                self.thread_context.get_context_ptr(),
                ip_hdr,
                data_len as _,
                inline_len as _,
            );
        }

        // inline udp hdr
        let udp_hdr = hdr_bytes[(cornflakes_libos::utils::IPV4_HEADER2_SIZE
            + cornflakes_libos::utils::ETHERNET2_HEADER2_SIZE)
            ..cornflakes_libos::utils::TOTAL_UDP_HEADER_SIZE]
            .as_ptr() as *const udp_hdr;
        unsafe {
            custom_mlx5_inline_udp_hdr(
                self.thread_context.get_context_ptr(),
                udp_hdr,
                data_len as _,
                inline_len as _,
            );
        }

        // write packet id
        unsafe {
            custom_mlx5_inline_packet_id(self.thread_context.get_context_ptr(), msg_id as _);
        }
        Ok(())
    }

    /// Depending on configured inline mode and first entry length,
    /// inlines the packet header and first entry,
    /// Returns whether header was inlined and whether first entry was inlined.   
    fn inline_sga_with_copy_if_necessary(
        &self,
        conn_id: ConnID,
        msg_id: MsgID,
        inline_len: usize,
        data_len: usize,
        sga: &ArenaOrderedSga,
    ) -> Result<(bool, usize)> {
        tracing::debug!(inline_len, data_len, "In inline sga with copy if necessary");
        match self.inline_mode {
            InlineMode::Nothing => Ok((false, 0)),
            InlineMode::PacketHeader => {
                self.inline_hdr(conn_id, msg_id, inline_len, data_len)?;
                Ok((true, 0))
            }
            InlineMode::ObjectHeader => {
                if inline_len > 0 && inline_len <= self.max_inline_size {
                    self.inline_hdr(conn_id, msg_id, inline_len, data_len)?;
                    let mut offset = cornflakes_libos::utils::TOTAL_HEADER_SIZE;
                    for seg in sga.iter().take(sga.len()) {
                        unsafe {
                            custom_mlx5_copy_inline_data(
                                self.thread_context.get_context_ptr(),
                                offset as _,
                                seg.addr().as_ptr() as _,
                                seg.len() as _,
                                inline_len as _,
                            );
                            offset += seg.len();
                        }
                    }
                    return Ok((true, sga.len()));
                } else {
                    return Ok((false, 0));
                }
            }
        }
    }

    /// Depending on configured inline mode and first entry length,
    /// inlines the packet header and first entry,
    /// Returns whether header was inlined and whether first entry was inlined.   
    fn inline_proto_if_necessary<T>(
        &self,
        conn_id: ConnID,
        msg_id: MsgID,
        inline_len: usize,
        data_len: usize,
        _proto: &T,
    ) -> Result<(bool, usize)>
    where
        T: protobuf::Message,
    {
        match self.inline_mode {
            InlineMode::Nothing => Ok((false, 0)),
            InlineMode::PacketHeader => {
                self.inline_hdr(conn_id, msg_id, inline_len, data_len)?;
                Ok((true, 0))
            }
            InlineMode::ObjectHeader => {
                // unreachable: at this point, we don't know how to inline the protobuf header
                // directly, because, protobuf expects a contiguous buffer to write into
                if (cornflakes_libos::utils::TOTAL_HEADER_SIZE + data_len) <= self.max_inline_size {
                    unimplemented!();
                } else {
                    return Ok((false, 0));
                }
            }
        }
    }

    /// Depending on configured inline mode and first entry length,
    /// inlines the packet header and first entry,
    /// Returns whether header was inlined and whether first entry was inlined.   
    fn inline_single_buffer_if_necessary(
        &self,
        conn_id: ConnID,
        msg_id: MsgID,
        inline_len: usize,
        data_len: usize,
        hdr: &[u8],
    ) -> Result<(bool, usize)> {
        match self.inline_mode {
            InlineMode::Nothing => Ok((false, 0)),
            InlineMode::PacketHeader => {
                self.inline_hdr(conn_id, msg_id, inline_len, data_len)?;
                Ok((true, 0))
            }
            InlineMode::ObjectHeader => {
                if (cornflakes_libos::utils::TOTAL_HEADER_SIZE + hdr.len()) <= self.max_inline_size
                {
                    self.inline_hdr(conn_id, msg_id, inline_len, data_len)?;
                    unsafe {
                        custom_mlx5_copy_inline_data(
                            self.thread_context.get_context_ptr(),
                            cornflakes_libos::utils::TOTAL_HEADER_SIZE as _,
                            hdr.as_ptr() as _,
                            hdr.len() as _,
                            inline_len as _,
                        );
                        return Ok((true, 1));
                    }
                } else {
                    return Ok((false, 0));
                }
            }
        }
    }

    /// Depending on configured inline mode and first entry length,
    /// inlines the packet header and first entry,
    /// Returns whether header was inlined and whether first entry was inlined.   
    fn inline_hdr_if_necessary(
        &self,
        conn_id: ConnID,
        msg_id: MsgID,
        inline_len: usize,
        data_len: usize,
        hdr: &[u8],
    ) -> Result<(bool, usize)> {
        match self.inline_mode {
            InlineMode::Nothing => Ok((false, 0)),
            InlineMode::PacketHeader => {
                self.inline_hdr(conn_id, msg_id, inline_len, data_len)?;
                Ok((true, 0))
            }
            InlineMode::ObjectHeader => {
                self.inline_hdr(conn_id, msg_id, inline_len, data_len)?;
                if (cornflakes_libos::utils::TOTAL_HEADER_SIZE + hdr.len()) <= self.max_inline_size
                {
                    unsafe {
                        custom_mlx5_copy_inline_data(
                            self.thread_context.get_context_ptr(),
                            cornflakes_libos::utils::TOTAL_HEADER_SIZE as _,
                            hdr.as_ptr() as _,
                            hdr.len() as _,
                            inline_len as _,
                        );
                        return Ok((true, 1));
                    }
                }
                return Ok((true, 0));
            }
        }
    }

    fn poll_for_completions(&self) -> Result<()> {
        // check for completions: ONLY WHEN IN FLIGHT > THRESH
        if unsafe {
            custom_mlx5_process_completions(
                self.thread_context.get_context_ptr(),
                COMPLETION_BUDGET as _,
            )
        } != 0
        {
            bail!("Unsafe processing completions");
        }
        return Ok(());
    }

    /// Rings doorbells for current transmissions
    /// Then checks for completions.
    fn post_curr_transmissions(
        &self,
        first_ctrl_seg: Option<*mut mlx5_wqe_ctrl_seg>,
    ) -> Result<Option<*mut mlx5_wqe_ctrl_seg>> {
        let mut ret: Option<*mut mlx5_wqe_ctrl_seg> = first_ctrl_seg;
        match ret {
            Some(seg) => {
                if unsafe {
                    custom_mlx5_post_transmissions(self.thread_context.get_context_ptr(), seg) != 0
                } {
                    bail!("Failed to post transmissions so far");
                }
                ret = None;
            }
            None => {}
        }

        return Ok(ret);
    }

    /// Posts mbuf onto ring buffer, and increments reference count on mbuf
    fn post_mbuf_metadata(
        &self,
        metadata_mbuf: &mut MbufMetadata,
        curr_dpseg: *mut mlx5_wqe_data_seg,
        curr_completion: *mut custom_mlx5_transmission_info,
    ) -> (*mut mlx5_wqe_data_seg, *mut custom_mlx5_transmission_info) {
        metadata_mbuf.increment_refcnt();
        tracing::debug!(
            len = metadata_mbuf.data_len(),
            off = metadata_mbuf.offset(),
            buf =? metadata_mbuf.as_ref().as_ptr(),
            "posting dpseg"
        );
        unsafe {
            (
                custom_mlx5_add_dpseg(
                    self.thread_context.get_context_ptr(),
                    curr_dpseg,
                    metadata_mbuf.data(),
                    metadata_mbuf.mempool(),
                    metadata_mbuf.offset() as _,
                    metadata_mbuf.data_len() as _,
                ),
                custom_mlx5_add_completion_info(
                    self.thread_context.get_context_ptr(),
                    curr_completion,
                    metadata_mbuf.data(),
                    metadata_mbuf.mempool(),
                ),
            )
        }
    }

    /// Post ordered sgas. This function is only called when there is space for these sgas in the
    /// ring buffer.
    fn post_ordered_sgas(&mut self, sgas: &[(MsgID, ConnID, OrderedSga)]) -> Result<()> {
        // fill in the hdr segment
        let mut first_ctrl_seg = ptr::null_mut();
        for (sgas_idx, (msg_id, conn_id, ordered_sga)) in sgas.iter().enumerate() {
            let (inline_len, num_segs) = self.ordered_sga_shape(ordered_sga);
            let num_octowords =
                unsafe { custom_mlx5_num_octowords(inline_len as _, num_segs as _) };
            let num_wqes_required = unsafe { custom_mlx5_num_wqes_required(num_octowords) };
            tracing::debug!(
                inline_len,
                num_segs,
                num_octowords,
                num_wqes_required,
                "Header info"
            );
            let ctrl_seg = unsafe {
                custom_mlx5_fill_in_hdr_segment(
                    self.thread_context.get_context_ptr(),
                    num_octowords as _,
                    num_wqes_required as _,
                    inline_len as _,
                    num_segs as _,
                    MLX5_ETH_WQE_L3_CSUM as i32 | MLX5_ETH_WQE_L4_CSUM as i32,
                )
            };
            if ctrl_seg.is_null() {
                bail!("Error posting header segment for sga");
            }

            if sgas_idx == 0 {
                first_ctrl_seg = ctrl_seg;
            }

            let data_len = ordered_sga.sga().data_len() + ordered_sga.get_hdr().len();
            let (header_written, entry_idx) = self.inline_hdr_if_necessary(
                *conn_id,
                *msg_id,
                inline_len,
                data_len,
                ordered_sga.get_hdr(),
            )?;

            // TODO: temporary hack for different code surrounding inlining first entry
            let inlined_obj_hdr = entry_idx == 1;

            let first_zero_copy_seg = ordered_sga.num_copy_entries();
            let allocation_size = ordered_sga.copy_length()
                - (inlined_obj_hdr as usize * ordered_sga.get_hdr().len())
                + (!header_written as usize * cornflakes_libos::utils::TOTAL_HEADER_SIZE);
            let mut dpseg = unsafe {
                custom_mlx5_dpseg_start(self.thread_context.get_context_ptr(), inline_len as _)
            };
            let mut completion =
                unsafe { custom_mlx5_completion_start(self.thread_context.get_context_ptr()) };
            if allocation_size > 0 {
                let mut data_buffer = match self.allocator.allocate_tx_buffer()? {
                    Some(buf) => buf,
                    None => {
                        bail!("No tx mempools to allocate outgoing packet");
                    }
                };
                let mut offset = 0;
                if !header_written {
                    self.copy_hdr(&mut data_buffer, *conn_id, *msg_id, data_len)?;
                    offset += cornflakes_libos::utils::TOTAL_HEADER_SIZE;
                }
                if !inlined_obj_hdr {
                    let data_slice =
                        data_buffer.mutable_slice(offset, offset + ordered_sga.get_hdr().len())?;
                    data_slice.copy_from_slice(ordered_sga.get_hdr());
                    offset += ordered_sga.get_hdr().len();
                }
                for seg in ordered_sga.sga().iter().take(first_zero_copy_seg) {
                    tracing::debug!("Writing into slice [{}, {}]", offset, offset + seg.len());
                    let dst = data_buffer.mutable_slice(offset, offset + seg.len())?;
                    unsafe {
                        mlx5_rte_memcpy(dst.as_mut_ptr() as _, seg.addr().as_ptr() as _, seg.len());
                    }
                    offset += seg.len();
                }

                let mut metadata_mbuf = MbufMetadata::from_buf(data_buffer);

                let (curr_dpseg, curr_completion) =
                    self.post_mbuf_metadata(&mut metadata_mbuf, dpseg, completion);
                dpseg = curr_dpseg;
                completion = curr_completion;
            }

            // rest are zero-copy segments
            for seg in ordered_sga
                .sga()
                .iter()
                .take(ordered_sga.len())
                .skip(first_zero_copy_seg)
            {
                let curr_seg = seg.addr();
                let mut mbuf_metadata = match self.allocator.recover_buffer(curr_seg)? {
                    Some(x) => x,
                    None => {
                        bail!("Failed to recover mbuf metadata for seg{:?}", curr_seg);
                    }
                };
                let (curr_dpseg, curr_completion) =
                    self.post_mbuf_metadata(&mut mbuf_metadata, dpseg, completion);

                dpseg = curr_dpseg;
                completion = curr_completion;
            }

            unsafe {
                custom_mlx5_finish_single_transmission(
                    self.thread_context.get_context_ptr(),
                    num_wqes_required,
                );
            }
        }

        if !first_ctrl_seg.is_null() {
            tracing::debug!("Posting transmissions with ctrl seg {:?}", first_ctrl_seg);
            let _ = self.post_curr_transmissions(Some(first_ctrl_seg));
        }
        Ok(())
    }

    /// Recursively tries to push all sgas until all are sent.
    fn push_ordered_sgas_recursive(&mut self, sgas: &[(MsgID, ConnID, OrderedSga)]) -> Result<()> {
        let curr_available_wqes: usize =
            unsafe { custom_mlx5_num_wqes_available(self.thread_context.get_context_ptr()) }
                as usize;
        let mut stopping_index = sgas.len();
        let mut total_wqes_so_far = 0;
        for (i, (_, _, sga)) in sgas.iter().enumerate() {
            let num_required = self.wqes_required_ordered_sga(sga);
            if (total_wqes_so_far + num_required) > curr_available_wqes {
                stopping_index = std::cmp::max(i - 1, 0);
                break;
            }
            total_wqes_so_far += num_required
        }
        tracing::debug!(
            curr_available_wqes,
            total_wqes_so_far,
            stopping_index,
            sgas_len = sgas.len(),
            "After looping on ordered sga slice",
        );
        if stopping_index == 0 {
            // poll for completions, call this function again
            self.poll_for_completions()?;
            return self.push_ordered_sgas_recursive(sgas);
        } else if stopping_index < sgas.len() {
            // post sgas until i - 1, call this function again
            self.post_ordered_sgas(&sgas[0..stopping_index])?;
            self.poll_for_completions()?;
            return self.push_ordered_sgas_recursive(&sgas[stopping_index..sgas.len()]);
        } else {
            // post all sgas, poll for completions
            tracing::debug!("Posting all sgas");
            self.post_ordered_sgas(sgas)?;
            self.poll_for_completions()?;
            return Ok(());
        }
    }

    /// Assuming that there are enough descriptors to transmit this sga, post the rc sga.
    fn post_rc_sga(
        &mut self,
        rc_sga: &mut RcSga<Self>,
        conn_id: ConnID,
        msg_id: MsgID,
        num_octowords: u64,
        num_wqes_required: u64,
        inline_len: usize,
        num_segs: usize,
    ) -> Result<*mut mlx5_wqe_ctrl_seg> {
        // can process this sga
        let ctrl_seg = unsafe {
            custom_mlx5_fill_in_hdr_segment(
                self.thread_context.get_context_ptr(),
                num_octowords as _,
                num_wqes_required as _,
                inline_len as _,
                num_segs as _,
                MLX5_ETH_WQE_L3_CSUM as i32 | MLX5_ETH_WQE_L4_CSUM as i32,
            )
        };
        if ctrl_seg.is_null() {
            bail!("Error posting header segment for sga.");
        }

        let data_len = rc_sga.data_len();
        let (mut header_written, mut entry_idx) = self.inline_hdr_if_necessary(
            conn_id,
            msg_id,
            inline_len,
            data_len,
            rc_sga.get(0).addr(),
        )?;

        // get first data segment and corresponding completion segment on ring buffers
        let mut curr_data_seg: *mut mlx5_wqe_data_seg = unsafe {
            custom_mlx5_dpseg_start(self.thread_context.get_context_ptr(), inline_len as _)
        };
        let mut curr_completion: *mut custom_mlx5_transmission_info =
            unsafe { custom_mlx5_completion_start(self.thread_context.get_context_ptr()) };

        // fill in all entries
        while entry_idx < rc_sga.len() {
            if self.zero_copy_rc_seg(rc_sga.get(entry_idx)) {
                if entry_idx == 0 && !header_written {
                    // write in header
                    let mut data_buffer = match self.allocator.allocate_tx_buffer()? {
                        Some(buf) => buf,
                        None => {
                            bail!("No tx mempools to allocate outgoing packet");
                        }
                    };
                    // copy in the header into a buffer
                    self.copy_hdr(&mut data_buffer, conn_id, msg_id, data_len)?;
                    // attach this data buffer to a metadata buffer
                    let mut metadata_mbuf = MbufMetadata::from_buf(data_buffer);
                    let (curr_dpseg, completion) =
                        self.post_mbuf_metadata(&mut metadata_mbuf, curr_data_seg, curr_completion);
                    curr_data_seg = curr_dpseg;
                    curr_completion = completion;

                    header_written = true;
                }

                let mut mbuf_metadata = rc_sga.get_mut(entry_idx).inner_datapath_pkt_mut().unwrap();
                mbuf_metadata.increment_refcnt();
                let (curr_dpseg, completion) =
                    self.post_mbuf_metadata(&mut mbuf_metadata, curr_data_seg, curr_completion);
                curr_data_seg = curr_dpseg;
                curr_completion = completion;
                entry_idx += 1;
            } else {
                // iterate forward, figure out size of further zero-copy segments
                let mut curr_idx = entry_idx;
                while curr_idx < rc_sga.len() && !self.zero_copy_rc_seg(rc_sga.get(curr_idx)) {
                    curr_idx += 1;
                }

                // allocate an mbuf that can fit this amount of data and copy data to it
                let mut data_buffer = match self.allocator.allocate_tx_buffer()? {
                    Some(buf) => buf,
                    None => {
                        bail!("No tx mempools to allocate outgoing packet");
                    }
                };
                let mut write_offset = match !header_written && entry_idx == 0 {
                    true => {
                        // copy in the header into a buffer
                        self.copy_hdr(&mut data_buffer, conn_id, msg_id, data_len)?
                    }
                    false => 0,
                };

                // iterate over segments and copy segments into data buffer
                for idx in entry_idx..curr_idx {
                    let curr_seg = rc_sga.get(idx).addr();
                    // copy into the destination
                    let dst =
                        data_buffer.mutable_slice(write_offset, write_offset + curr_seg.len())?;
                    unsafe {
                        mlx5_rte_memcpy(
                            dst.as_mut_ptr() as _,
                            curr_seg.as_ref().as_ptr() as _,
                            curr_seg.len(),
                        );
                    }

                    write_offset += curr_seg.len();
                }

                // attach this data buffer to a metadata buffer
                let mut metadata_mbuf = MbufMetadata::from_buf(data_buffer);
                let (curr_dpseg, completion) =
                    self.post_mbuf_metadata(&mut metadata_mbuf, curr_data_seg, curr_completion);
                curr_data_seg = curr_dpseg;
                curr_completion = completion;
                entry_idx = curr_idx;
            }
        }

        // finish the transmission
        unsafe {
            custom_mlx5_finish_single_transmission(
                self.thread_context.get_context_ptr(),
                num_wqes_required,
            );
        }

        return Ok(ctrl_seg);
    }

    fn sga_with_copy_shape(&self, sga: &ArenaOrderedSga) -> (usize, usize) {
        match self.inline_mode {
            InlineMode::Nothing => (0, 1),
            InlineMode::PacketHeader => (cornflakes_libos::utils::TOTAL_HEADER_SIZE, 1),
            InlineMode::ObjectHeader => {
                if (cornflakes_libos::utils::TOTAL_HEADER_SIZE + sga.data_len())
                    < self.max_inline_size
                {
                    (
                        cornflakes_libos::utils::TOTAL_HEADER_SIZE + sga.data_len(),
                        0,
                    )
                } else {
                    (0, 1)
                }
            }
        }
    }

    fn protobuf_shape<T>(&self, proto: &T) -> (usize, usize)
    where
        T: protobuf::Message,
    {
        let buf_len = proto.compute_size() as usize;
        match self.inline_mode {
            InlineMode::Nothing => (0, 1),
            InlineMode::PacketHeader => (cornflakes_libos::utils::TOTAL_HEADER_SIZE, 1),
            InlineMode::ObjectHeader => {
                if (cornflakes_libos::utils::TOTAL_HEADER_SIZE + buf_len) < self.max_inline_size {
                    (cornflakes_libos::utils::TOTAL_HEADER_SIZE + buf_len, 0)
                } else {
                    (0, 1)
                }
            }
        }
    }

    fn single_buffer_shape(&self, buf: &[u8]) -> (usize, usize) {
        match self.inline_mode {
            InlineMode::Nothing => (0, 1),
            InlineMode::PacketHeader => (cornflakes_libos::utils::TOTAL_HEADER_SIZE, 1),
            InlineMode::ObjectHeader => {
                if (cornflakes_libos::utils::TOTAL_HEADER_SIZE + buf.len()) < self.max_inline_size {
                    (cornflakes_libos::utils::TOTAL_HEADER_SIZE + buf.len(), 0)
                } else {
                    (0, 1)
                }
            }
        }
    }

    /// Returns (inline length, num_segs) for ordered sga.
    #[inline]
    fn ordered_rcsga_shape(&self, ordered_sga: &OrderedRcSga<Self>) -> (usize, usize) {
        match self.inline_mode {
            InlineMode::Nothing => (0, ordered_sga.num_zero_copy_entries() + 1),
            InlineMode::PacketHeader => (
                cornflakes_libos::utils::TOTAL_HEADER_SIZE,
                ordered_sga.num_zero_copy_entries()
                    + (ordered_sga.num_zero_copy_entries() < ordered_sga.len()
                        || ordered_sga.get_hdr().len() > 0) as usize,
            ),
            InlineMode::ObjectHeader => {
                match (ordered_sga.get_hdr().len() + cornflakes_libos::utils::TOTAL_HEADER_SIZE)
                    <= self.max_inline_size
                {
                    true => (
                        cornflakes_libos::utils::TOTAL_HEADER_SIZE + ordered_sga.get_hdr().len(),
                        ordered_sga.num_zero_copy_entries()
                            + ((ordered_sga.num_zero_copy_entries() < (ordered_sga.len()))
                                as usize),
                    ),
                    false => (
                        cornflakes_libos::utils::TOTAL_HEADER_SIZE,
                        ordered_sga.num_zero_copy_entries()
                            + (ordered_sga.num_zero_copy_entries() < ordered_sga.len()
                                || ordered_sga.get_hdr().len() > 0)
                                as usize,
                    ),
                }
            }
        }
    }

    /// Returns (inline length, num_segs) for ordered sga.
    #[inline]
    fn arena_ordered_rcsga_shape(&self, ordered_sga: &ArenaOrderedRcSga<Self>) -> (usize, usize) {
        match self.inline_mode {
            InlineMode::Nothing => (0, ordered_sga.num_zero_copy_entries() + 1),
            InlineMode::PacketHeader => (
                cornflakes_libos::utils::TOTAL_HEADER_SIZE,
                ordered_sga.num_zero_copy_entries()
                    + (ordered_sga.num_zero_copy_entries() < ordered_sga.len()
                        || ordered_sga.get_hdr().len() > 0) as usize,
            ),
            InlineMode::ObjectHeader => {
                match (ordered_sga.get_hdr().len() + cornflakes_libos::utils::TOTAL_HEADER_SIZE)
                    <= self.max_inline_size
                {
                    true => (
                        cornflakes_libos::utils::TOTAL_HEADER_SIZE + ordered_sga.get_hdr().len(),
                        ordered_sga.num_zero_copy_entries()
                            + ((ordered_sga.num_zero_copy_entries() < (ordered_sga.len()))
                                as usize),
                    ),
                    false => (
                        cornflakes_libos::utils::TOTAL_HEADER_SIZE,
                        ordered_sga.num_zero_copy_entries()
                            + (ordered_sga.num_zero_copy_entries() < ordered_sga.len()
                                || ordered_sga.get_hdr().len() > 0)
                                as usize,
                    ),
                }
            }
        }
    }

    /// Returns inline length and total number of scatter-gather entries given serialization information.
    fn cornflakes_hybrid_object_shape(
        &self,
        serialization_info: &SerializationInfo,
    ) -> (usize, usize) {
        match self.inline_mode {
            InlineMode::Nothing => (0, 1 + serialization_info.num_zero_copy_entries),
            InlineMode::PacketHeader => {
                let num_extra_entries = (serialization_info.header_size > 0
                    || serialization_info.copy_length > 0)
                    as usize;
                (
                    cornflakes_libos::utils::TOTAL_HEADER_SIZE,
                    num_extra_entries + serialization_info.num_zero_copy_entries,
                )
            }
            InlineMode::ObjectHeader => {
                let num_extra_entries = (serialization_info.copy_length > 0) as usize;
                (
                    cornflakes_libos::utils::TOTAL_HEADER_SIZE + serialization_info.header_size,
                    num_extra_entries + serialization_info.num_zero_copy_entries,
                )
            }
        }
    }

    fn cornflakes_obj_shape(
        &self,
        header_len: usize,
        num_copy_entries: usize,
        num_zero_copy_entries: usize,
    ) -> (usize, usize) {
        match self.inline_mode {
            InlineMode::Nothing => (0, 1 + num_copy_entries + num_zero_copy_entries),
            InlineMode::PacketHeader => {
                if header_len > 0 {
                    (
                        cornflakes_libos::utils::TOTAL_HEADER_SIZE,
                        1 + num_copy_entries + num_zero_copy_entries,
                    )
                } else {
                    (
                        cornflakes_libos::utils::TOTAL_HEADER_SIZE,
                        num_copy_entries + num_zero_copy_entries,
                    )
                }
            }
            InlineMode::ObjectHeader => (
                cornflakes_libos::utils::TOTAL_HEADER_SIZE + header_len,
                num_copy_entries + num_zero_copy_entries,
            ),
        }
    }

    // TODO: doesn't work when there's more then 1 MTU of copied stuff
    fn arena_datapath_sga_shape(
        &self,
        arena_datapath_sga: &ArenaDatapathSga<Self>,
    ) -> (usize, usize) {
        match self.inline_mode {
            InlineMode::Nothing => {
                let mut extra = 1;
                if arena_datapath_sga.copy_len() > 0 {
                    extra += 1;
                }
                (0, arena_datapath_sga.num_zero_copy_entries() + extra)
            }
            InlineMode::PacketHeader => {
                let mut extra = 0;
                if arena_datapath_sga.get_header().len() > 0 {
                    extra += 1;
                }
                if arena_datapath_sga.copy_len() > 0 {
                    extra += 1;
                }
                (
                    cornflakes_libos::utils::TOTAL_HEADER_SIZE,
                    arena_datapath_sga.num_zero_copy_entries() + extra,
                )
            }
            InlineMode::ObjectHeader => match arena_datapath_sga.copy_len() > 0 {
                true => (
                    cornflakes_libos::utils::TOTAL_HEADER_SIZE
                        + arena_datapath_sga.get_header().len(),
                    arena_datapath_sga.num_zero_copy_entries() + 1,
                ),
                false => (
                    cornflakes_libos::utils::TOTAL_HEADER_SIZE
                        + arena_datapath_sga.get_header().len(),
                    arena_datapath_sga.num_zero_copy_entries(),
                ),
            },
        }
    }

    fn arena_ordered_sga_shape(&self, ordered_sga: &ArenaOrderedSga) -> (usize, usize) {
        match self.inline_mode {
            InlineMode::Nothing => (0, ordered_sga.num_zero_copy_entries() + 1),
            InlineMode::PacketHeader => (
                cornflakes_libos::utils::TOTAL_HEADER_SIZE,
                ordered_sga.num_zero_copy_entries()
                    + (ordered_sga.num_zero_copy_entries() < ordered_sga.len()
                        || ordered_sga.get_hdr().len() > 0) as usize,
            ),
            InlineMode::ObjectHeader => {
                match (ordered_sga.get_hdr().len() + cornflakes_libos::utils::TOTAL_HEADER_SIZE)
                    <= self.max_inline_size
                {
                    true => (
                        cornflakes_libos::utils::TOTAL_HEADER_SIZE + ordered_sga.get_hdr().len(),
                        ordered_sga.num_zero_copy_entries()
                            + ((ordered_sga.num_zero_copy_entries() < (ordered_sga.len()))
                                as usize),
                    ),
                    false => (
                        cornflakes_libos::utils::TOTAL_HEADER_SIZE,
                        ordered_sga.num_zero_copy_entries()
                            + (ordered_sga.num_zero_copy_entries() < ordered_sga.len()
                                || ordered_sga.get_hdr().len() > 0)
                                as usize,
                    ),
                }
            }
        }
    }

    /// Returns (inline length, num_segs) for ordered sga.
    fn ordered_sga_shape(&self, ordered_sga: &OrderedSga) -> (usize, usize) {
        match self.inline_mode {
            InlineMode::Nothing => (0, ordered_sga.num_zero_copy_entries() + 1),
            InlineMode::PacketHeader => (
                cornflakes_libos::utils::TOTAL_HEADER_SIZE,
                ordered_sga.num_zero_copy_entries()
                    + (ordered_sga.num_zero_copy_entries() < ordered_sga.len()
                        || ordered_sga.get_hdr().len() > 0) as usize,
            ),
            InlineMode::ObjectHeader => {
                match (ordered_sga.get_hdr().len() + cornflakes_libos::utils::TOTAL_HEADER_SIZE)
                    <= self.max_inline_size
                {
                    true => (
                        cornflakes_libos::utils::TOTAL_HEADER_SIZE + ordered_sga.get_hdr().len(),
                        ordered_sga.num_zero_copy_entries()
                            + ((ordered_sga.num_zero_copy_entries() < (ordered_sga.len()))
                                as usize),
                    ),
                    false => (
                        cornflakes_libos::utils::TOTAL_HEADER_SIZE,
                        ordered_sga.num_zero_copy_entries()
                            + (ordered_sga.num_zero_copy_entries() < ordered_sga.len()
                                || ordered_sga.get_hdr().len() > 0)
                                as usize,
                    ),
                }
            }
        }
    }

    #[inline]
    fn wqes_required_protobuf<T>(&self, proto: &T) -> usize
    where
        T: protobuf::Message,
    {
        let (inline_len, num_segs) = self.protobuf_shape(proto);
        unsafe {
            custom_mlx5_num_wqes_required(custom_mlx5_num_octowords(inline_len as _, num_segs as _))
                as usize
        }
    }

    #[inline]
    fn wqes_required_sga_with_copy(&self, sga: &ArenaOrderedSga) -> usize {
        let (inline_len, num_segs) = self.sga_with_copy_shape(sga);
        unsafe {
            custom_mlx5_num_wqes_required(custom_mlx5_num_octowords(inline_len as _, num_segs as _))
                as _
        }
    }

    fn wqes_required_single_buffer(&self, buf: &[u8]) -> usize {
        let (inline_len, num_segs) = self.single_buffer_shape(buf);
        unsafe {
            custom_mlx5_num_wqes_required(custom_mlx5_num_octowords(inline_len as _, num_segs as _))
                as _
        }
    }

    fn wqes_required_arena_datapath_sga(
        &self,
        arena_datapath_sga: &ArenaDatapathSga<Self>,
    ) -> usize {
        let (inline_len, num_segs) = self.arena_datapath_sga_shape(arena_datapath_sga);
        unsafe {
            custom_mlx5_num_wqes_required(custom_mlx5_num_octowords(inline_len as _, num_segs as _))
                as _
        }
    }

    fn wqes_required_arena_ordered_sga(&self, ordered_sga: &ArenaOrderedSga) -> usize {
        let (inline_len, num_segs) = self.arena_ordered_sga_shape(ordered_sga);
        unsafe {
            custom_mlx5_num_wqes_required(custom_mlx5_num_octowords(inline_len as _, num_segs as _))
                as _
        }
    }

    fn wqes_required_ordered_rcsga(&self, ordered_sga: &OrderedRcSga<Self>) -> usize {
        let (inline_len, num_segs) = self.ordered_rcsga_shape(ordered_sga);
        unsafe {
            custom_mlx5_num_wqes_required(custom_mlx5_num_octowords(inline_len as _, num_segs as _))
                as _
        }
    }

    fn wqes_required_arena_ordered_rcsga(&self, ordered_sga: &ArenaOrderedRcSga<Self>) -> usize {
        let (inline_len, num_segs) = self.arena_ordered_rcsga_shape(ordered_sga);
        unsafe {
            custom_mlx5_num_wqes_required(custom_mlx5_num_octowords(inline_len as _, num_segs as _))
                as _
        }
    }

    fn wqes_required_ordered_sga(&self, ordered_sga: &OrderedSga) -> usize {
        let (inline_len, num_segs) = self.ordered_sga_shape(ordered_sga);
        unsafe {
            custom_mlx5_num_wqes_required(custom_mlx5_num_octowords(inline_len as _, num_segs as _))
                as _
        }
    }

    /// Assuming that there are enough descriptors to transmit this sga, post the sga.
    fn post_sga(
        &mut self,
        sga: &Sga,
        conn_id: ConnID,
        msg_id: MsgID,
        num_octowords: u64,
        num_wqes_required: u64,
        inline_len: usize,
        num_segs: usize,
    ) -> Result<*mut mlx5_wqe_ctrl_seg> {
        // can process this sga
        let ctrl_seg = unsafe {
            custom_mlx5_fill_in_hdr_segment(
                self.thread_context.get_context_ptr(),
                num_octowords as _,
                num_wqes_required as _,
                inline_len as _,
                num_segs as _,
                MLX5_ETH_WQE_L3_CSUM as i32 | MLX5_ETH_WQE_L4_CSUM as i32,
            )
        };
        if ctrl_seg.is_null() {
            bail!("Error posting header segment for sga.");
        }

        let data_len = sga.data_len();
        let (mut header_written, mut entry_idx) =
            self.inline_hdr_if_necessary(conn_id, msg_id, inline_len, data_len, sga.get(0).addr())?;

        // get first data segment and corresponding completion segment on ring buffers
        let mut curr_data_seg: *mut mlx5_wqe_data_seg = unsafe {
            custom_mlx5_dpseg_start(self.thread_context.get_context_ptr(), inline_len as _)
        };
        let mut curr_completion: *mut custom_mlx5_transmission_info =
            unsafe { custom_mlx5_completion_start(self.thread_context.get_context_ptr()) };

        // fill in all entries
        while entry_idx < sga.len() {
            // TODO: can make this more optimal to not search through mempools twice
            if self.zero_copy_seg(sga.get(entry_idx).addr()) {
                if entry_idx == 0 && !header_written {
                    // write in header
                    let mut data_buffer = match self.allocator.allocate_tx_buffer()? {
                        Some(buf) => buf,
                        None => {
                            bail!("No tx mempools to allocate outgoing packet");
                        }
                    };
                    // copy in the header into a buffer
                    self.copy_hdr(&mut data_buffer, conn_id, msg_id, data_len)?;
                    // attach this data buffer to a metadata buffer
                    let mut metadata_mbuf = MbufMetadata::from_buf(data_buffer);
                    let (dpseg, completion) =
                        self.post_mbuf_metadata(&mut metadata_mbuf, curr_data_seg, curr_completion);
                    curr_data_seg = dpseg;
                    curr_completion = completion;
                    header_written = true;
                }

                let curr_seg = sga.get(entry_idx).addr();
                let mut mbuf_metadata = self.allocator.recover_buffer(curr_seg)?.unwrap();
                let (dpseg, completion) =
                    self.post_mbuf_metadata(&mut mbuf_metadata, curr_data_seg, curr_completion);
                curr_data_seg = dpseg;
                curr_completion = completion;
                entry_idx += 1;
            } else {
                // iterate forward, figure out size of further zero-copy segments
                tracing::debug!(entry_idx, "Not zero-copy segment post_sga");
                let mut curr_idx = entry_idx;
                let mut mbuf_length = match !header_written && curr_idx == 0 {
                    true => cornflakes_libos::utils::TOTAL_HEADER_SIZE,
                    false => 0,
                };
                tracing::debug!(mbuf_length, "Finding forward index");
                while curr_idx < sga.len() && !self.zero_copy_seg(sga.get(curr_idx).addr()) {
                    let curr_seg = sga.get(curr_idx).addr();
                    mbuf_length += curr_seg.len();
                    curr_idx += 1;
                }

                // allocate an mbuf that can fit this amount of data and copy data to it
                let mut data_buffer = match self.allocator.allocate_tx_buffer()? {
                    Some(buf) => buf,
                    None => {
                        bail!("No tx mempools to allocate outgoing packet");
                    }
                };
                tracing::debug!(
                    header_written,
                    entry_idx,
                    curr_idx,
                    mbuf_length,
                    "Allocated buffer to write in data"
                );
                let mut write_offset = match !header_written && entry_idx == 0 {
                    true => {
                        // copy in the header into a buffer
                        tracing::debug!("COPYING HEADER INTO DATA BUFFER");
                        self.copy_hdr(&mut data_buffer, conn_id, msg_id, data_len)?
                    }
                    false => 0,
                };

                // iterate over segments and copy segments into data buffer
                for idx in entry_idx..curr_idx {
                    let curr_seg = sga.get(idx).addr();
                    // copy into the destination
                    let dst =
                        data_buffer.mutable_slice(write_offset, write_offset + curr_seg.len())?;
                    unsafe {
                        mlx5_rte_memcpy(
                            dst.as_mut_ptr() as _,
                            curr_seg.as_ptr() as _,
                            curr_seg.len(),
                        );
                    }

                    write_offset += curr_seg.len();
                }

                // attach this data buffer to a metadata buffer
                let mut metadata_mbuf = MbufMetadata::from_buf(data_buffer);
                let (dpseg, completion) =
                    self.post_mbuf_metadata(&mut metadata_mbuf, curr_data_seg, curr_completion);
                curr_data_seg = dpseg;
                curr_completion = completion;
                entry_idx = curr_idx;
            }
        }

        // finish the transmission
        unsafe {
            custom_mlx5_finish_single_transmission(
                self.thread_context.get_context_ptr(),
                num_wqes_required,
            );
        }

        return Ok(ctrl_seg);
    }

    /// Returns inline size, sga idx to start iterating from, num segs for header
    fn calculate_inline_size(&self, first_entry_size: usize) -> (usize, usize, usize) {
        let mut sga_idx = 0;
        let (inline_size, num_segs) = match self.inline_mode {
            InlineMode::Nothing => (0, 1),
            InlineMode::PacketHeader => (cornflakes_libos::utils::TOTAL_HEADER_SIZE, 0),
            InlineMode::ObjectHeader => {
                let inline_size = first_entry_size + cornflakes_libos::utils::TOTAL_HEADER_SIZE;
                match inline_size <= self.max_inline_size {
                    true => {
                        sga_idx += 1;
                        (inline_size, 0)
                    }
                    false => (cornflakes_libos::utils::TOTAL_HEADER_SIZE, 0),
                }
            }
        };
        (inline_size, sga_idx, num_segs)
    }

    /// Given a reference-counted scatter-gather array,
    /// calculate inline size and number of data segments to tell the NIC.
    /// Returns:
    /// Result<(inline size, number of data segments)>
    fn calculate_shape_rc(&self, rc_sga: &RcSga<Self>) -> Result<(usize, usize)> {
        let (inline_size, sga_idx, mut num_segs) = self.calculate_inline_size(rc_sga.get(0).len());
        let mut prev_copy = (num_segs == 1) as _;
        for rc_sga in rc_sga.iter().take(rc_sga.len()).skip(sga_idx) {
            match self.zero_copy_rc_seg(rc_sga) {
                true => {
                    num_segs += 1;
                    prev_copy = false;
                }
                false => {
                    if !prev_copy {
                        num_segs += 1;
                    }
                    prev_copy = true;
                }
            }
        }
        Ok((inline_size, num_segs))
    }

    /// Given a scatter-gather array,
    /// calculates inline size and number of data segments to the tell the NIC.
    /// Returns:
    /// Result<(inline size, number of data segments)>
    fn calculate_shape(&self, sga: &Sga) -> Result<(usize, usize)> {
        let (inline_size, sga_idx, mut num_segs) = self.calculate_inline_size(sga.get(0).len());
        let mut prev_copy = (num_segs == 1) as _;
        for seg in sga.iter().take(sga.len()).skip(sga_idx) {
            match self.zero_copy_seg(seg.addr()) {
                true => {
                    num_segs += 1;
                    prev_copy = false;
                }
                false => {
                    if !prev_copy {
                        num_segs += 1;
                    }
                    prev_copy = true;
                }
            }
        }
        Ok((inline_size, num_segs))
    }
}

fn parse_pci_addr(config_path: &str) -> Result<String> {
    let file_str = read_to_string(Path::new(&config_path))?;
    let yamls = match YamlLoader::load_from_str(&file_str) {
        Ok(docs) => docs,
        Err(e) => {
            bail!("Could not parse config yaml: {:?}", e);
        }
    };
    let yaml = &yamls[0];
    match yaml["mlx5"].as_hash() {
        Some(map) => match map.get(&Yaml::from_str("pci_addr")) {
            Some(val) => {
                return Ok(val.as_str().unwrap().to_string());
            }
            None => {
                bail!("Yaml mlx5 config has no pci_addr entry");
            }
        },
        None => {
            bail!("Yaml has no mlx5 entry");
        }
    }
}

impl Datapath for Mlx5Connection {
    type DatapathBuffer = Mlx5Buffer;

    type DatapathMetadata = MbufMetadata;

    type CallbackEntryState = (*mut mlx5_wqe_data_seg, *mut custom_mlx5_transmission_info);

    type PerThreadContext = Mlx5PerThreadContext;

    type DatapathSpecificParams = Mlx5DatapathSpecificParams;

    fn parse_config_file(
        config_file: &str,
        our_ip: &Ipv4Addr,
    ) -> Result<Self::DatapathSpecificParams> {
        // parse the IP to Mac hashmap
        let (ip_to_mac, _mac_to_ip, server_port, client_port) =
            parse_yaml_map(config_file).wrap_err("Failed to parse yaml map")?;

        let custom_mlx5_pci_addr =
            parse_pci_addr(config_file).wrap_err("Failed to parse pci addr from config")?;

        // for this datapath, knowing our IP address is required (to find our mac address)
        let eth_addr = match ip_to_mac.get(our_ip) {
            Some(e) => e.clone(),
            None => {
                bail!("Could not find eth addr for passed in ipv4 addr {:?} in config_file ip_to_mac map: {:?}", our_ip, ip_to_mac);
            }
        };

        // convert pci addr and eth addr to C structs
        let mut ether_addr: MaybeUninit<eth_addr> = MaybeUninit::zeroed();
        // copy given mac addr into the c struct
        unsafe {
            mlx5_rte_memcpy(
                ether_addr.as_mut_ptr() as _,
                eth_addr.as_bytes().as_ptr() as _,
                6,
            );
        }

        let pci_str = CString::new(custom_mlx5_pci_addr.as_str()).expect("CString::new failed");
        let mut custom_mlx5_pci_addr_c: MaybeUninit<custom_mlx5_pci_addr> = MaybeUninit::zeroed();
        unsafe {
            custom_mlx5_pci_str_to_addr(
                pci_str.as_ptr() as _,
                custom_mlx5_pci_addr_c.as_mut_ptr() as _,
            );
        }
        Ok(Mlx5DatapathSpecificParams {
            custom_mlx5_pci_addr: custom_mlx5_pci_addr_c,
            eth_addr: ether_addr,
            our_ip: our_ip.clone(),
            our_eth: eth_addr.clone(),
            client_port: client_port,
            server_port: server_port,
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
            bail!("Currently, mlx5 datapath does not support more than one queue");
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
        // do time init
        unsafe {
            custom_mlx5_time_init();
        }

        ensure!(
            num_queues == addresses.len(),
            format!(
                "AddressInfo vector length {} must be equal to num queues {}",
                addresses.len(),
                num_queues
            )
        );
        // allocate the global context
        let (global_context, rx_mempool_ptrs): (Arc<Mlx5GlobalContext>, Vec<*mut [u8]>) = {
            unsafe {
                // create a box for both the per-thread and global memory
                let global_context_size = custom_mlx5_get_global_context_size();
                let thread_context_size = custom_mlx5_get_per_thread_context_size(num_queues as _);
                let global_context_box: Box<[u8]> =
                    vec![0; global_context_size as _].into_boxed_slice();
                let thread_context_box: Box<[u8]> =
                    vec![0; thread_context_size as _].into_boxed_slice();

                let global_context_ptr = Box::<[u8]>::into_raw(global_context_box);
                let thread_context_ptr = Box::<[u8]>::into_raw(thread_context_box);

                custom_mlx5_alloc_global_context(
                    num_queues as _,
                    global_context_ptr as _,
                    thread_context_ptr as _,
                );

                let mut rx_mempool_ptrs: Vec<*mut [u8]> = Vec::with_capacity(num_queues as _);
                for i in 0..num_queues {
                    let rx_mempool_box: Box<[u8]> =
                        vec![0; custom_mlx5_get_registered_mempool_size() as _].into_boxed_slice();
                    let rx_mempool_ptr = Box::<[u8]>::into_raw(rx_mempool_box);
                    tracing::debug!("Allocated rx mempool ptr at {:?}", rx_mempool_ptr);
                    custom_mlx5_set_rx_mempool_ptr(
                        global_context_ptr as _,
                        i as _,
                        rx_mempool_ptr as _,
                    );
                    rx_mempool_ptrs.push(rx_mempool_ptr);
                }

                // initialize ibv context
                check_ok!(custom_mlx5_init_ibv_context(
                    global_context_ptr as _,
                    datapath_params.get_custom_mlx5_pci_addr()
                ));

                // initialize and register the rx mempools
                let rx_mempool_params: sizes::MempoolAllocationParams =
                    sizes::MempoolAllocationParams::new(
                        sizes::RX_MEMPOOL_MIN_NUM_ITEMS,
                        sizes::RX_MEMPOOL_DATA_PGSIZE,
                        sizes::RX_MEMPOOL_DATA_LEN,
                    )
                    .wrap_err("Incorrect rx allocation params")?;
                check_ok!(custom_mlx5_init_rx_mempools(
                    global_context_ptr as _,
                    rx_mempool_params.get_item_len() as _,
                    rx_mempool_params.get_num_items() as _,
                    rx_mempool_params.get_data_pgsize() as _,
                    ibv_access_flags_IBV_ACCESS_LOCAL_WRITE as _
                ));

                // init queues
                for i in 0..num_queues {
                    let per_thread_context =
                        custom_mlx5_get_per_thread_context(global_context_ptr as _, i as u64);
                    check_ok!(custom_mlx5_init_rxq(per_thread_context));
                    check_ok!(custom_mlx5_init_txq(per_thread_context));
                }

                // init queue steering
                check_ok!(custom_mlx5_qs_init_flows(
                    global_context_ptr as _,
                    datapath_params.get_eth_addr()
                ));
                (
                    Arc::new(Mlx5GlobalContext {
                        global_context_ptr: global_context_ptr,
                        thread_context_ptr: thread_context_ptr,
                    }),
                    rx_mempool_ptrs,
                )
            }
        };
        let per_thread_contexts: Vec<Mlx5PerThreadContext> = addresses
            .into_iter()
            .enumerate()
            .map(|(i, addr)| {
                let global_context_copy = global_context.clone();
                let context_ptr = unsafe {
                    custom_mlx5_get_per_thread_context(
                        (*Arc::<Mlx5GlobalContext>::as_ptr(&global_context_copy)).ptr(),
                        i as u64,
                    )
                };
                let rx_mempool_ptr = rx_mempool_ptrs[i];
                tracing::debug!(rx_mempool_ptr =? rx_mempool_ptr, thread_context_ptr =? context_ptr, "Registering per thread context");
                Mlx5PerThreadContext {
                    global_context_rc: global_context_copy,
                    queue_id: i as u16,
                    address_info: addr,
                    context: context_ptr,
                    rx_mempool_ptr: rx_mempool_ptr,
                }
            })
            .collect();
        Ok(per_thread_contexts)
    }

    fn per_thread_init(
        _datapath_params: Self::DatapathSpecificParams,
        context: Self::PerThreadContext,
        mode: AppMode,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        let rx_mempool = DataMempool::new_from_ptr(context.get_recv_mempool_ptr());
        // allocate a tx mempool
        let mempool_params =
            sizes::MempoolAllocationParams::new(MEMPOOL_MIN_ELTS, PGSIZE_2MB, MAX_BUFFER_SIZE)
                .wrap_err("Incorrect mempool allocation params")?;
        tracing::debug!(mempool_params = ?mempool_params, "Adding tx mempool");
        let tx_mempool = DataMempool::new(&mempool_params, &context, false)?;

        let allocator = MemoryPoolAllocator::new(rx_mempool, tx_mempool)?;

        Ok(Mlx5Connection {
            thread_context: context,
            mode: mode,
            outgoing_window: HashMap::default(),
            active_connections: [None; MAX_CONCURRENT_CONNECTIONS],
            address_to_conn_id: HashMap::default(),
            allocator: allocator,
            copying_threshold: 256,
            max_segments: 32,
            inline_mode: InlineMode::default(),
            max_inline_size: 256,
            recv_mbufs: RecvMbufArray::new(RECEIVE_BURST_SIZE),
            first_ctrl_seg: ptr::null_mut(),
            mbuf_metadatas: Default::default(),
            header_buffer: vec![0u8; Self::max_packet_size()],
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

    fn push_buffers_with_copy_iterator<'a>(
        &mut self,
        mut pkts: impl Iterator<Item = (MsgID, ConnID, &'a [u8])>,
    ) -> Result<()> {
        let mut first_ctrl_seg: Option<*mut mlx5_wqe_ctrl_seg> = None;
        while let Some((msg_id, conn_id, buf)) = pkts.next() {
            self.insert_into_outgoing_map(msg_id, conn_id);

            let (buf_size, inline_len) = match self.inline_mode {
                InlineMode::Nothing => (buf.len() + cornflakes_libos::utils::TOTAL_HEADER_SIZE, 0),
                InlineMode::PacketHeader => (buf.len(), cornflakes_libos::utils::TOTAL_HEADER_SIZE),
                InlineMode::ObjectHeader => {
                    match (buf.len() + cornflakes_libos::utils::TOTAL_HEADER_SIZE)
                        > self.max_inline_size
                    {
                        true => (buf.len(), cornflakes_libos::utils::TOTAL_HEADER_SIZE),
                        false => (0, buf.len() + cornflakes_libos::utils::TOTAL_HEADER_SIZE),
                    }
                }
            };

            let num_segs = match buf_size == 0 {
                true => 0,
                false => 1,
            };

            let num_octowords =
                unsafe { custom_mlx5_num_octowords(inline_len as _, num_segs as _) };

            let num_wqes_required = unsafe { custom_mlx5_num_wqes_required(num_octowords as _) };
            tracing::debug!(
                num_wqes_required = num_wqes_required,
                available = unsafe {
                    custom_mlx5_tx_descriptors_available(
                        self.thread_context.get_context_ptr(),
                        num_wqes_required,
                    )
                },
                "Pkt to send"
            );
            if unsafe {
                custom_mlx5_tx_descriptors_available(
                    self.thread_context.get_context_ptr(),
                    num_wqes_required,
                ) != 1
            } {
                first_ctrl_seg = self.post_curr_transmissions(first_ctrl_seg)?;
                self.poll_for_completions()?;
            } else {
                let ctrl_seg = unsafe {
                    custom_mlx5_fill_in_hdr_segment(
                        self.thread_context.get_context_ptr(),
                        num_octowords as _,
                        num_wqes_required as _,
                        inline_len as _,
                        num_segs as _,
                        MLX5_ETH_WQE_L3_CSUM as i32 | MLX5_ETH_WQE_L4_CSUM as i32,
                    )
                };
                if first_ctrl_seg == None {
                    first_ctrl_seg = Some(ctrl_seg);
                }
                // add next segment
                let mut written_header = false;
                let allocation_size;
                match self.inline_mode {
                    InlineMode::Nothing => {
                        allocation_size = cornflakes_libos::utils::TOTAL_HEADER_SIZE + buf.len();
                    }
                    InlineMode::PacketHeader | InlineMode::ObjectHeader => {
                        // inline packet header
                        self.inline_hdr(conn_id, msg_id, inline_len, buf.len())?;
                        written_header = true;

                        if self.inline_mode == InlineMode::ObjectHeader
                            && (cornflakes_libos::utils::TOTAL_HEADER_SIZE + buf.len()
                                <= self.max_inline_size)
                        {
                            unsafe {
                                custom_mlx5_copy_inline_data(
                                    self.thread_context.get_context_ptr(),
                                    cornflakes_libos::utils::TOTAL_UDP_HEADER_SIZE as _,
                                    buf.as_ptr() as _,
                                    buf.len() as _,
                                    inline_len as _,
                                );
                            }
                            allocation_size = buf.len();
                        } else {
                            allocation_size = 0;
                        }
                    }
                }

                // now add the dpseg (if necessary)
                if allocation_size > 0 {
                    // copy data into mbuf
                    // allocate an mbuf that can fit this amount of data and copy data to it
                    let mut data_buffer = match self.allocator.allocate_tx_buffer()? {
                        Some(buf) => buf,
                        None => {
                            bail!("No tx mempools to allocate outgoing packet");
                        }
                    };
                    let write_offset = match !written_header {
                        true => {
                            // copy in the header into a buffer
                            self.copy_hdr(&mut data_buffer, conn_id, msg_id, buf.len())?;
                            cornflakes_libos::utils::TOTAL_HEADER_SIZE
                        }
                        false => 0,
                    };
                    let dst = data_buffer.mutable_slice(write_offset, write_offset + buf.len())?;
                    unsafe {
                        mlx5_rte_memcpy(dst.as_mut_ptr() as _, buf.as_ptr() as _, buf.len());
                    }

                    // now put this inside an mbuf and post it.
                    // attach this data buffer to a metadata buffer
                    let metadata_mbuf = MbufMetadata::from_buf(data_buffer);
                    // post this data buffer to the ring buffer
                    unsafe {
                        let dpseg = custom_mlx5_dpseg_start(
                            self.thread_context.get_context_ptr(),
                            inline_len as _,
                        );
                        let completion =
                            custom_mlx5_completion_start(self.thread_context.get_context_ptr());
                        custom_mlx5_add_dpseg(
                            self.thread_context.get_context_ptr(),
                            dpseg,
                            metadata_mbuf.data(),
                            metadata_mbuf.mempool(),
                            metadata_mbuf.offset() as _,
                            metadata_mbuf.data_len() as _,
                        );

                        custom_mlx5_add_completion_info(
                            self.thread_context.get_context_ptr(),
                            completion,
                            metadata_mbuf.data(),
                            metadata_mbuf.mempool(),
                        );

                        // finish transmission
                        custom_mlx5_finish_single_transmission(
                            self.thread_context.get_context_ptr(),
                            num_wqes_required,
                        );
                    }
                }
            }
        }
        let _ = self.post_curr_transmissions(first_ctrl_seg)?;
        self.poll_for_completions()?;
        Ok(())
    }

    fn push_buffers_with_copy(&mut self, pkts: &[(MsgID, ConnID, &[u8])]) -> Result<()> {
        tracing::debug!("Pushing batch of pkts of length {}", pkts.len());
        let mut pkt_idx = 0;
        let mut first_ctrl_seg: Option<*mut mlx5_wqe_ctrl_seg> = None;
        while pkt_idx < pkts.len() {
            let (msg_id, conn_id, buf) = pkts[pkt_idx];
            self.insert_into_outgoing_map(msg_id, conn_id);

            let (buf_size, inline_len) = match self.inline_mode {
                InlineMode::Nothing => (buf.len() + cornflakes_libos::utils::TOTAL_HEADER_SIZE, 0),
                InlineMode::PacketHeader => (buf.len(), cornflakes_libos::utils::TOTAL_HEADER_SIZE),
                InlineMode::ObjectHeader => {
                    match (buf.len() + cornflakes_libos::utils::TOTAL_HEADER_SIZE)
                        > self.max_inline_size
                    {
                        true => (buf.len(), cornflakes_libos::utils::TOTAL_HEADER_SIZE),
                        false => (0, buf.len() + cornflakes_libos::utils::TOTAL_HEADER_SIZE),
                    }
                }
            };

            let num_segs = match buf_size == 0 {
                true => 0,
                false => 1,
            };

            let num_octowords =
                unsafe { custom_mlx5_num_octowords(inline_len as _, num_segs as _) };

            let num_wqes_required = unsafe { custom_mlx5_num_wqes_required(num_octowords as _) };
            tracing::debug!(
                num_wqes_required = num_wqes_required,
                available = unsafe {
                    custom_mlx5_tx_descriptors_available(
                        self.thread_context.get_context_ptr(),
                        num_wqes_required,
                    )
                },
                "Pkt to send"
            );
            if unsafe {
                custom_mlx5_tx_descriptors_available(
                    self.thread_context.get_context_ptr(),
                    num_wqes_required,
                ) != 1
            } {
                first_ctrl_seg = self.post_curr_transmissions(first_ctrl_seg)?;
                self.poll_for_completions()?;
            } else {
                let ctrl_seg = unsafe {
                    custom_mlx5_fill_in_hdr_segment(
                        self.thread_context.get_context_ptr(),
                        num_octowords as _,
                        num_wqes_required as _,
                        inline_len as _,
                        num_segs as _,
                        MLX5_ETH_WQE_L3_CSUM as i32 | MLX5_ETH_WQE_L4_CSUM as i32,
                    )
                };
                if first_ctrl_seg == None {
                    first_ctrl_seg = Some(ctrl_seg);
                }
                // add next segment
                let mut written_header = false;
                let allocation_size;
                match self.inline_mode {
                    InlineMode::Nothing => {
                        allocation_size = cornflakes_libos::utils::TOTAL_HEADER_SIZE + buf.len();
                    }
                    InlineMode::PacketHeader | InlineMode::ObjectHeader => {
                        // inline packet header
                        self.inline_hdr(conn_id, msg_id, inline_len, buf.len())?;
                        written_header = true;

                        if self.inline_mode == InlineMode::ObjectHeader
                            && (cornflakes_libos::utils::TOTAL_HEADER_SIZE + buf.len()
                                <= self.max_inline_size)
                        {
                            unsafe {
                                custom_mlx5_copy_inline_data(
                                    self.thread_context.get_context_ptr(),
                                    cornflakes_libos::utils::TOTAL_UDP_HEADER_SIZE as _,
                                    buf.as_ptr() as _,
                                    buf.len() as _,
                                    inline_len as _,
                                );
                            }
                            allocation_size = buf.len();
                        } else {
                            allocation_size = 0;
                        }
                    }
                }

                // now add the dpseg (if necessary)
                if allocation_size > 0 {
                    // copy data into mbuf
                    // allocate an mbuf that can fit this amount of data and copy data to it
                    let mut data_buffer = match self.allocator.allocate_tx_buffer()? {
                        Some(buf) => buf,
                        None => {
                            bail!("No tx mempools to allocate outgoing packet");
                        }
                    };
                    let write_offset = match !written_header {
                        true => {
                            // copy in the header into a buffer
                            self.copy_hdr(&mut data_buffer, conn_id, msg_id, buf.len())?;
                            cornflakes_libos::utils::TOTAL_HEADER_SIZE
                        }
                        false => 0,
                    };
                    let dst = data_buffer.mutable_slice(write_offset, write_offset + buf.len())?;
                    unsafe {
                        mlx5_rte_memcpy(dst.as_mut_ptr() as _, buf.as_ptr() as _, buf.len());
                    }

                    // now put this inside an mbuf and post it.
                    // attach this data buffer to a metadata buffer
                    let metadata_mbuf = MbufMetadata::from_buf(data_buffer);

                    // post this data buffer to the ring buffer
                    unsafe {
                        let dpseg = custom_mlx5_dpseg_start(
                            self.thread_context.get_context_ptr(),
                            inline_len as _,
                        );
                        let completion =
                            custom_mlx5_completion_start(self.thread_context.get_context_ptr());
                        custom_mlx5_add_dpseg(
                            self.thread_context.get_context_ptr(),
                            dpseg,
                            metadata_mbuf.data(),
                            metadata_mbuf.mempool(),
                            metadata_mbuf.offset() as _,
                            metadata_mbuf.data_len() as _,
                        );

                        custom_mlx5_add_completion_info(
                            self.thread_context.get_context_ptr(),
                            completion,
                            metadata_mbuf.data(),
                            metadata_mbuf.mempool(),
                        );

                        // finish transmission
                        custom_mlx5_finish_single_transmission(
                            self.thread_context.get_context_ptr(),
                            num_wqes_required,
                        );
                    }
                }
                pkt_idx += 1;
            }
        }
        let _ = self.post_curr_transmissions(first_ctrl_seg)?;
        self.poll_for_completions()?;
        Ok(())
    }

    fn echo(&mut self, mut pkts: Vec<ReceivedPkt<Self>>) -> Result<()>
    where
        Self: Sized,
    {
        // iterate over pkts, flip the header in these packets, and transmit them back
        // need to post to both the normal ring buffer and the completions ring buffer
        let mut pkt_idx = 0;
        let mut first_ctrl_seg: Option<*mut mlx5_wqe_ctrl_seg> = None;
        while pkt_idx < pkts.len() {
            let received_pkt = &mut pkts[pkt_idx];
            let inline_len = 0;
            let num_segs = received_pkt.num_segs();
            let num_octowords =
                unsafe { custom_mlx5_num_octowords(inline_len as _, num_segs as _) };

            let num_wqes_required = unsafe { custom_mlx5_num_wqes_required(num_octowords as _) };

            if unsafe {
                custom_mlx5_tx_descriptors_available(
                    self.thread_context.get_context_ptr(),
                    num_wqes_required,
                ) != 1
            } {
                first_ctrl_seg = self.post_curr_transmissions(first_ctrl_seg)?;
                self.poll_for_completions()?;
            } else {
                // ASSUMES THAT THE PACKET HAS A FULL HEADER TO FLIP
                unsafe {
                    flip_headers(received_pkt.seg(0).data());
                    tracing::debug!(
                        num_segs,
                        num_octowords,
                        num_wqes_required,
                        data =? received_pkt.seg(0).data,
                        offset = received_pkt.seg(0).offset,
                        "Echoing received packet"
                    );
                    let ctrl_seg = custom_mlx5_fill_in_hdr_segment(
                        self.thread_context.get_context_ptr(),
                        num_octowords as _,
                        num_wqes_required as _,
                        inline_len as _,
                        num_segs as _,
                        MLX5_ETH_WQE_L3_CSUM as i32 | MLX5_ETH_WQE_L4_CSUM as i32,
                    );
                    if first_ctrl_seg == None {
                        first_ctrl_seg = Some(ctrl_seg);
                    }
                    // add a dpseg and a completion info for each received packet in the mbuf
                    let mut curr_dpseg =
                        custom_mlx5_dpseg_start(self.thread_context.get_context_ptr(), 0);
                    let mut curr_completion =
                        custom_mlx5_completion_start(self.thread_context.get_context_ptr());
                    for seg in received_pkt.iter_mut() {
                        seg.increment_refcnt();
                        curr_dpseg = custom_mlx5_add_dpseg(
                            self.thread_context.get_context_ptr(),
                            curr_dpseg,
                            seg.data(),
                            seg.mempool(),
                            0,
                            (seg.data_len() + cornflakes_libos::utils::TOTAL_HEADER_SIZE) as _,
                        );

                        curr_completion = custom_mlx5_add_completion_info(
                            self.thread_context.get_context_ptr(),
                            curr_completion,
                            seg.data(),
                            seg.mempool(),
                        );
                    }

                    // now finish the transmission
                    custom_mlx5_finish_single_transmission(
                        self.thread_context.get_context_ptr(),
                        num_wqes_required,
                    );
                }
                pkt_idx += 1;
            }
        }
        let _ = self.post_curr_transmissions(first_ctrl_seg)?;
        self.poll_for_completions()?;
        Ok(())
    }

    fn push_rc_sgas(&mut self, rc_sgas: &mut [(MsgID, ConnID, RcSga<Self>)]) -> Result<()>
    where
        Self: Sized,
    {
        tracing::debug!(len = rc_sgas.len(), "Pushing rc_sgas");
        let mut first_ctrl_seg: Option<*mut mlx5_wqe_ctrl_seg> = None;
        let mut sga_idx = 0;
        while sga_idx < rc_sgas.len() {
            tracing::debug!(sga_idx = sga_idx, "In rc sga process loop");
            let (msg_id, conn_id, ref mut sga) = &mut rc_sgas[sga_idx];
            self.insert_into_outgoing_map(*msg_id, *conn_id);
            let (inline_len, num_segs) = self.calculate_shape_rc(&sga)?;
            tracing::debug!(
                inline_len = inline_len,
                num_segs = num_segs,
                "Params for rc sga"
            );
            let num_octowords =
                unsafe { custom_mlx5_num_octowords(inline_len as _, num_segs as _) };
            let num_wqes_required = unsafe { custom_mlx5_num_wqes_required(num_octowords as _) };

            tracing::debug!(
                inline_len = inline_len,
                num_segs = num_segs,
                num_octowords = num_octowords,
                num_wqes_required = num_wqes_required,
                "Parameters for rc sga"
            );
            if unsafe {
                custom_mlx5_tx_descriptors_available(
                    self.thread_context.get_context_ptr(),
                    num_wqes_required,
                ) != 1
            } {
                first_ctrl_seg = self.post_curr_transmissions(first_ctrl_seg)?;
                self.poll_for_completions()?;
            } else {
                let ctrl_seg = self.post_rc_sga(
                    sga,
                    *conn_id,
                    *msg_id,
                    num_octowords,
                    num_wqes_required,
                    inline_len,
                    num_segs,
                )?;
                if first_ctrl_seg == None {
                    first_ctrl_seg = Some(ctrl_seg)
                }
                sga_idx += 1;
            }
        }
        let _ = self.post_curr_transmissions(first_ctrl_seg)?;
        self.poll_for_completions()?;
        Ok(())
    }

    fn push_ordered_sgas(&mut self, sgas: &[(MsgID, ConnID, OrderedSga)]) -> Result<()> {
        self.push_ordered_sgas_recursive(sgas)
    }

    fn serialize_and_send<'a>(
        &mut self,
        mut objects: impl Iterator<Item = Result<(MsgID, ConnID, impl SgaHeaderRepr<'a>)>>,
    ) -> Result<()>
    where
        Self: Sized,
    {
        let _curr_available_wqes: usize =
            unsafe { custom_mlx5_num_wqes_available(self.thread_context.get_context_ptr()) }
                as usize;
        let _first_ctrl_seg: *mut mlx5_wqe_ctrl_seg = ptr::null_mut();
        let _num_wqes_used_so_far = 0;
        let mut obj = {
            #[cfg(feature = "profiler")]
            demikernel::timer!("iterator next");
            objects.next()
        };

        // TODO: unimplemented!

        while let Some(res) = obj {
            let (_msg_id, _conn_id, object) = res?;
            let _header_size = object.total_header_size(false, true);

            obj = objects.next();
        }

        Ok(())
    }

    fn queue_protobuf_message<T>(&mut self, sga: (MsgID, ConnID, &T), end_batch: bool) -> Result<()>
    where
        T: protobuf::Message,
    {
        #[cfg(feature = "profiler")]
        demikernel::timer!("Queue protobuf message");
        let mut curr_available_wqes: usize =
            unsafe { custom_mlx5_num_wqes_available(self.thread_context.get_context_ptr()) }
                as usize;
        let msg_id = sga.0;
        let conn_id = sga.1;
        let object = sga.2;

        let num_required = self.wqes_required_protobuf(object);
        while num_required > curr_available_wqes {
            if self.first_ctrl_seg != ptr::null_mut() {
                if unsafe {
                    custom_mlx5_post_transmissions(
                        self.thread_context.get_context_ptr(),
                        self.first_ctrl_seg,
                    ) != 0
                } {
                    bail!("Failed to post transmissions so far");
                } else {
                    self.first_ctrl_seg = ptr::null_mut();
                }
            }
            self.poll_for_completions()?;
            curr_available_wqes =
                unsafe { custom_mlx5_num_wqes_available(self.thread_context.get_context_ptr()) }
                    as usize;
        }

        let (inline_len, _num_segs, num_wqes_required) = {
            let (inline_len, num_segs) = self.protobuf_shape(object);
            let num_octowords =
                unsafe { custom_mlx5_num_octowords(inline_len as _, num_segs as _) };
            let num_wqes_required = num_required as u64;
            tracing::debug!(
                inline_len,
                num_segs,
                num_octowords,
                num_wqes_required,
                "Header info"
            );
            let ctrl_seg = unsafe {
                custom_mlx5_fill_in_hdr_segment(
                    self.thread_context.get_context_ptr(),
                    num_octowords as _,
                    num_wqes_required as _,
                    inline_len as _,
                    num_segs as _,
                    MLX5_ETH_WQE_L3_CSUM as i32 | MLX5_ETH_WQE_L4_CSUM as i32,
                )
            };
            if ctrl_seg.is_null() {
                bail!("Error posting header segment for sga");
            }

            if self.first_ctrl_seg == ptr::null_mut() {
                self.first_ctrl_seg = ctrl_seg;
            }
            (inline_len, num_segs, num_wqes_required)
        };
        let data_len = object.compute_size() as usize;
        let (header_written, entry_idx) =
            self.inline_proto_if_necessary(conn_id, msg_id, inline_len, data_len, object)?;
        // TODO: temporary hack for different code surrounding inlining first entry
        let inlined_obj_hdr = entry_idx > 0;
        tracing::debug!(
            inlined_obj_hdr,
            header_written,
            entry_idx,
            "Calculating allocation size"
        );

        let allocation_size = data_len - (inlined_obj_hdr as usize * data_len)
            + (!header_written as usize * cornflakes_libos::utils::TOTAL_HEADER_SIZE);
        let dpseg = unsafe {
            custom_mlx5_dpseg_start(self.thread_context.get_context_ptr(), inline_len as _)
        };
        let completion =
            unsafe { custom_mlx5_completion_start(self.thread_context.get_context_ptr()) };
        tracing::debug!("Allocation size: {}", allocation_size);
        if allocation_size > 0 {
            let mut data_buffer = {
                match self.allocator.allocate_tx_buffer()? {
                    Some(buf) => buf,
                    None => {
                        bail!(
                            "No tx mempools to allocate outgoing packet of size: {}",
                            allocation_size
                        );
                    }
                }
            };
            let mut offset = 0;
            if !header_written {
                self.copy_hdr(&mut data_buffer, conn_id, msg_id, data_len)?;
                offset += cornflakes_libos::utils::TOTAL_HEADER_SIZE;
            }
            if !inlined_obj_hdr {
                // copy protobuf object into datapath buffer
                let mutable_slice = data_buffer.mutable_slice(offset, offset + data_len)?;
                let mut coded_output_stream = protobuf::CodedOutputStream::bytes(mutable_slice);
                object.write_to(&mut coded_output_stream)?;
                coded_output_stream.flush()?;
            }

            let mut metadata_mbuf = MbufMetadata::from_buf(data_buffer);

            let _ = self.post_mbuf_metadata(&mut metadata_mbuf, dpseg, completion);
        }

        unsafe {
            custom_mlx5_finish_single_transmission(
                self.thread_context.get_context_ptr(),
                num_wqes_required as _,
            );
        }

        if end_batch {
            if !self.first_ctrl_seg.is_null() {
                let _ = self.post_curr_transmissions(Some(self.first_ctrl_seg));
                self.poll_for_completions()?;
                self.first_ctrl_seg = ptr::null_mut();
            }
        }
        Ok(())
    }

    /// Queues tuple buffer onto ring buffer.
    /// If no more space, if current first ctrl seg exists, rings doorbell and polls for
    /// completions.
    /// If end batch is true -- rings doorbell and polls for completions after posting this buffer.
    fn queue_sga_with_copy(
        &mut self,
        sga: (MsgID, ConnID, &ArenaOrderedSga),
        end_batch: bool,
    ) -> Result<()> {
        let mut curr_available_wqes: usize =
            unsafe { custom_mlx5_num_wqes_available(self.thread_context.get_context_ptr()) }
                as usize;
        let msg_id = sga.0;
        let conn_id = sga.1;
        let sga = sga.2;
        tracing::debug!(
            data_len = sga.data_len(),
            segments = sga.len(),
            "Sending sga with copy"
        );

        let num_required = self.wqes_required_sga_with_copy(&sga);

        while num_required > curr_available_wqes {
            if self.first_ctrl_seg != ptr::null_mut() {
                if unsafe {
                    custom_mlx5_post_transmissions(
                        self.thread_context.get_context_ptr(),
                        self.first_ctrl_seg,
                    ) != 0
                } {
                    bail!("Failed to post transmissions so far");
                } else {
                    self.first_ctrl_seg = ptr::null_mut();
                }
            }
            self.poll_for_completions()?;
            curr_available_wqes =
                unsafe { custom_mlx5_num_wqes_available(self.thread_context.get_context_ptr()) }
                    as usize;
        }
        let (inline_len, _num_segs, num_wqes_required) = {
            #[cfg(feature = "profiler")]
            demikernel::timer!("Ordered sga and filling in ctrl and ether seg");

            let (inline_len, num_segs) = self.sga_with_copy_shape(&sga);
            let num_octowords =
                unsafe { custom_mlx5_num_octowords(inline_len as _, num_segs as _) };
            let num_wqes_required = num_required as u64;
            tracing::debug!(
                inline_len,
                num_segs,
                num_octowords,
                num_wqes_required,
                "Header info"
            );
            let ctrl_seg = unsafe {
                custom_mlx5_fill_in_hdr_segment(
                    self.thread_context.get_context_ptr(),
                    num_octowords as _,
                    num_wqes_required as _,
                    inline_len as _,
                    num_segs as _,
                    MLX5_ETH_WQE_L3_CSUM as i32 | MLX5_ETH_WQE_L4_CSUM as i32,
                )
            };
            if ctrl_seg.is_null() {
                bail!("Error posting header segment for sga");
            }

            if self.first_ctrl_seg == ptr::null_mut() {
                self.first_ctrl_seg = ctrl_seg;
            }
            (inline_len, num_segs, num_wqes_required)
        };
        let data_len = sga.data_len();
        let (header_written, entry_idx) =
            self.inline_sga_with_copy_if_necessary(conn_id, msg_id, inline_len, data_len, &sga)?;

        // TODO: temporary hack for different code surrounding inlining first entry
        let inlined_obj_hdr = entry_idx > 0;
        tracing::debug!(
            inlined_obj_hdr,
            header_written,
            entry_idx,
            "Calculating allocation size"
        );

        let allocation_size = data_len - (inlined_obj_hdr as usize * data_len)
            + (!header_written as usize * cornflakes_libos::utils::TOTAL_HEADER_SIZE);
        let dpseg = unsafe {
            custom_mlx5_dpseg_start(self.thread_context.get_context_ptr(), inline_len as _)
        };
        let completion =
            unsafe { custom_mlx5_completion_start(self.thread_context.get_context_ptr()) };
        tracing::debug!("Allocation size: {}", allocation_size);
        if allocation_size > 0 {
            #[cfg(feature = "profiler")]
            demikernel::timer!("Copying stuff");

            let mut data_buffer = {
                #[cfg(feature = "profiler")]
                demikernel::timer!("allocating stuff to copy into");
                match self.allocator.allocate_tx_buffer()? {
                    Some(buf) => buf,
                    None => {
                        bail!(
                            "No tx mempools to allocate outgoing packet of size: {}",
                            allocation_size
                        );
                    }
                }
            };
            let mut offset = 0;
            if !header_written {
                self.copy_hdr(&mut data_buffer, conn_id, msg_id, data_len)?;
                offset += cornflakes_libos::utils::TOTAL_HEADER_SIZE;
            }
            if !inlined_obj_hdr {
                for seg in sga.iter().take(sga.len()) {
                    let data_slice = data_buffer.mutable_slice(offset, offset + seg.len())?;
                    data_slice.copy_from_slice(seg.addr());
                    offset += seg.len();
                }
            }

            let mut metadata_mbuf = MbufMetadata::from_buf(data_buffer);
            let _ = self.post_mbuf_metadata(&mut metadata_mbuf, dpseg, completion);
        }

        unsafe {
            custom_mlx5_finish_single_transmission(
                self.thread_context.get_context_ptr(),
                num_wqes_required as _,
            );
        }

        if end_batch {
            if !self.first_ctrl_seg.is_null() {
                let _ = self.post_curr_transmissions(Some(self.first_ctrl_seg));
                self.poll_for_completions()?;
                self.first_ctrl_seg = ptr::null_mut();
            }
        }
        Ok(())
    }

    /// Queues single buffer onto ring buffer.
    /// If no more space, if current first ctrl seg exists, rings doorbell and polls for
    /// completions.
    /// If end batch is true -- rings doorbell and polls for completions after posting this buffer.
    fn queue_single_buffer_with_copy(
        &mut self,
        sga: (MsgID, ConnID, &[u8]),
        end_batch: bool,
    ) -> Result<()> {
        #[cfg(feature = "profiler")]
        demikernel::timer!("Queue single buffer with copy");
        let mut curr_available_wqes: usize =
            unsafe { custom_mlx5_num_wqes_available(self.thread_context.get_context_ptr()) }
                as usize;
        let msg_id = sga.0;
        let conn_id = sga.1;
        let buf = sga.2;

        let num_required = self.wqes_required_single_buffer(buf);

        while num_required > curr_available_wqes {
            if self.first_ctrl_seg != ptr::null_mut() {
                if unsafe {
                    custom_mlx5_post_transmissions(
                        self.thread_context.get_context_ptr(),
                        self.first_ctrl_seg,
                    ) != 0
                } {
                    bail!("Failed to post transmissions so far");
                } else {
                    self.first_ctrl_seg = ptr::null_mut();
                }
            }
            self.poll_for_completions()?;
            curr_available_wqes =
                unsafe { custom_mlx5_num_wqes_available(self.thread_context.get_context_ptr()) }
                    as usize;
        }
        let (inline_len, _num_segs, num_wqes_required) = {
            let (inline_len, num_segs) = self.single_buffer_shape(buf);
            let num_octowords =
                unsafe { custom_mlx5_num_octowords(inline_len as _, num_segs as _) };
            let num_wqes_required = num_required as u64;
            tracing::debug!(
                inline_len,
                num_segs,
                num_octowords,
                num_wqes_required,
                "Header info"
            );
            let ctrl_seg = unsafe {
                custom_mlx5_fill_in_hdr_segment(
                    self.thread_context.get_context_ptr(),
                    num_octowords as _,
                    num_wqes_required as _,
                    inline_len as _,
                    num_segs as _,
                    MLX5_ETH_WQE_L3_CSUM as i32 | MLX5_ETH_WQE_L4_CSUM as i32,
                )
            };
            if ctrl_seg.is_null() {
                bail!("Error posting header segment for sga");
            }

            if self.first_ctrl_seg == ptr::null_mut() {
                self.first_ctrl_seg = ctrl_seg;
            }
            (inline_len, num_segs, num_wqes_required)
        };
        let (header_written, entry_idx) =
            self.inline_single_buffer_if_necessary(conn_id, msg_id, inline_len, buf.len(), buf)?;

        // TODO: temporary hack for different code surrounding inlining first entry
        let inlined_obj_hdr = entry_idx == 1;

        let allocation_size = buf.len() - (inlined_obj_hdr as usize * buf.len())
            + (!header_written as usize * cornflakes_libos::utils::TOTAL_HEADER_SIZE);
        let dpseg = unsafe {
            custom_mlx5_dpseg_start(self.thread_context.get_context_ptr(), inline_len as _)
        };
        let completion =
            unsafe { custom_mlx5_completion_start(self.thread_context.get_context_ptr()) };
        tracing::debug!("Allocation size: {}", allocation_size);
        if allocation_size > 0 {
            let mut data_buffer = {
                match self.allocator.allocate_tx_buffer()? {
                    Some(buf) => buf,
                    None => {
                        bail!("No tx mempools to allocate outgoing packet");
                    }
                }
            };
            if !header_written {
                self.copy_hdr(&mut data_buffer, conn_id, msg_id, buf.len())?;
            }
            if !inlined_obj_hdr {
                ensure!(
                    data_buffer.write(buf)? == buf.len(),
                    "Could not copy whole buffer into allocated buffer"
                );
            }
            let mut metadata_mbuf = MbufMetadata::from_buf(data_buffer);
            let _ = self.post_mbuf_metadata(&mut metadata_mbuf, dpseg, completion);
        }

        unsafe {
            custom_mlx5_finish_single_transmission(
                self.thread_context.get_context_ptr(),
                num_wqes_required as _,
            );
        }

        if end_batch {
            if !self.first_ctrl_seg.is_null() {
                let _ = self.post_curr_transmissions(Some(self.first_ctrl_seg));
                self.poll_for_completions()?;
                self.first_ctrl_seg = ptr::null_mut();
            }
        }
        Ok(())
    }

    fn prepare_single_buffer_with_udp_header(
        &mut self,
        addr: (ConnID, MsgID),
        data_len: usize,
    ) -> Result<Self::DatapathBuffer> {
        let mut data_buffer = {
            match self.allocator.allocate_tx_buffer()? {
                Some(buf) => buf,
                None => {
                    bail!("No tx mempools to allocate outgoing packet");
                }
            }
        };
        // copy UDP header with provided data length
        self.copy_hdr(&mut data_buffer, addr.0, addr.1, data_len)?;
        // set length on buffer
        data_buffer.set_len(cornflakes_libos::utils::TOTAL_HEADER_SIZE);
        Ok(data_buffer)
    }

    fn transmit_single_datapath_buffer_with_header(
        &mut self,
        data_buffer: Box<Self::DatapathBuffer>,
        end_batch: bool,
    ) -> Result<()> {
        // assume no inlining
        let num_required = 1;
        let mut curr_available_wqes =
            unsafe { custom_mlx5_num_wqes_available(self.thread_context.get_context_ptr()) }
                as usize;

        while num_required > curr_available_wqes {
            if self.first_ctrl_seg != ptr::null_mut() {
                if unsafe {
                    custom_mlx5_post_transmissions(
                        self.thread_context.get_context_ptr(),
                        self.first_ctrl_seg,
                    ) != 0
                } {
                    bail!("Failed to post transmissions so far");
                } else {
                    self.first_ctrl_seg = ptr::null_mut();
                }
            }
            self.poll_for_completions()?;
            curr_available_wqes =
                unsafe { custom_mlx5_num_wqes_available(self.thread_context.get_context_ptr()) }
                    as usize;
        }
        let inline_len = 0;
        let num_segs = 1;
        let num_octowords = unsafe { custom_mlx5_num_octowords(inline_len, num_segs) };
        let ctrl_seg = unsafe {
            custom_mlx5_fill_in_hdr_segment(
                self.thread_context.get_context_ptr(),
                num_octowords as _,
                num_required as _,
                inline_len as _,
                num_segs as _,
                MLX5_ETH_WQE_L3_CSUM as i32 | MLX5_ETH_WQE_L4_CSUM as i32,
            )
        };
        if ctrl_seg.is_null() {
            bail!("Error posting header segment for sga");
        }

        if self.first_ctrl_seg == ptr::null_mut() {
            self.first_ctrl_seg = ctrl_seg;
        }
        let dpseg = unsafe { custom_mlx5_dpseg_start(self.thread_context.get_context_ptr(), 0) };
        let completion =
            unsafe { custom_mlx5_completion_start(self.thread_context.get_context_ptr()) };
        let mut metadata_mbuf = MbufMetadata::from_buf(*data_buffer);
        let _ = self.post_mbuf_metadata(&mut metadata_mbuf, dpseg, completion);

        unsafe {
            custom_mlx5_finish_single_transmission(
                self.thread_context.get_context_ptr(),
                num_required as _,
            );
        }

        if end_batch {
            if !self.first_ctrl_seg.is_null() {
                let _ = self.post_curr_transmissions(Some(self.first_ctrl_seg));
                self.poll_for_completions()?;
                self.first_ctrl_seg = ptr::null_mut();
            }
        }
        Ok(())
    }

    /// Pushes sga onto ring buffer.
    /// If no more space, if current first ctrl seg exists, rings doorbell and polls for
    /// completions.
    /// If end batch is true -- rings doorbell and polls for completions after posting this SGA.
    fn queue_ordered_rcsga(
        &mut self,
        sga: (MsgID, ConnID, OrderedRcSga<Self>),
        end_batch: bool,
    ) -> Result<()> {
        #[cfg(feature = "profiler")]
        demikernel::timer!("queue ordered rcsga");

        let mut curr_available_wqes: usize =
            unsafe { custom_mlx5_num_wqes_available(self.thread_context.get_context_ptr()) }
                as usize;
        let msg_id = sga.0;
        let conn_id = sga.1;
        let ordered_sga = sga.2;

        let num_required = {
            #[cfg(feature = "profiler")]
            demikernel::timer!("Calculating wqes required for sga");
            self.wqes_required_ordered_rcsga(&ordered_sga)
        };

        while num_required > curr_available_wqes {
            if self.first_ctrl_seg != ptr::null_mut() {
                if unsafe {
                    custom_mlx5_post_transmissions(
                        self.thread_context.get_context_ptr(),
                        self.first_ctrl_seg,
                    ) != 0
                } {
                    bail!("Failed to post transmissions so far");
                } else {
                    self.first_ctrl_seg = ptr::null_mut();
                }
            }
            self.poll_for_completions()?;
            curr_available_wqes =
                unsafe { custom_mlx5_num_wqes_available(self.thread_context.get_context_ptr()) }
                    as usize;
        }
        let (inline_len, _num_segs, num_wqes_required) = {
            #[cfg(feature = "profiler")]
            demikernel::timer!("Ordered sga and filling in ctrl and ether seg");

            let (inline_len, num_segs) = self.ordered_rcsga_shape(&ordered_sga);
            let num_octowords =
                unsafe { custom_mlx5_num_octowords(inline_len as _, num_segs as _) };
            let num_wqes_required = num_required as u64;
            tracing::debug!(
                inline_len,
                num_segs,
                num_octowords,
                num_wqes_required,
                "Header info"
            );
            let ctrl_seg = unsafe {
                custom_mlx5_fill_in_hdr_segment(
                    self.thread_context.get_context_ptr(),
                    num_octowords as _,
                    num_wqes_required as _,
                    inline_len as _,
                    num_segs as _,
                    MLX5_ETH_WQE_L3_CSUM as i32 | MLX5_ETH_WQE_L4_CSUM as i32,
                )
            };
            if ctrl_seg.is_null() {
                bail!("Error posting header segment for sga");
            }

            if self.first_ctrl_seg == ptr::null_mut() {
                self.first_ctrl_seg = ctrl_seg;
            }
            (inline_len, num_segs, num_wqes_required)
        };
        let data_len = ordered_sga.data_len();
        let (header_written, entry_idx) = self.inline_hdr_if_necessary(
            conn_id,
            msg_id,
            inline_len,
            data_len,
            ordered_sga.get_hdr(),
        )?;

        // TODO: temporary hack for different code surrounding inlining first entry
        let inlined_obj_hdr = entry_idx == 1;

        let first_zero_copy_seg = ordered_sga.num_copy_entries();
        let allocation_size = ordered_sga.copy_length()
            - (inlined_obj_hdr as usize * ordered_sga.get_hdr().len())
            + (!header_written as usize * cornflakes_libos::utils::TOTAL_HEADER_SIZE);
        let mut dpseg = unsafe {
            custom_mlx5_dpseg_start(self.thread_context.get_context_ptr(), inline_len as _)
        };
        let mut completion =
            unsafe { custom_mlx5_completion_start(self.thread_context.get_context_ptr()) };
        tracing::debug!("Allocation size: {}", allocation_size);
        if allocation_size > 0 {
            #[cfg(feature = "profiler")]
            demikernel::timer!("Copying stuff");

            let mut data_buffer = {
                #[cfg(feature = "profiler")]
                demikernel::timer!("allocating stuff to copy into");
                match self.allocator.allocate_tx_buffer()? {
                    Some(buf) => buf,
                    None => {
                        bail!("No tx mempools to allocate outgoing packet");
                    }
                }
            };
            let mut offset = 0;
            if !header_written {
                self.copy_hdr(&mut data_buffer, conn_id, msg_id, data_len)?;
                offset += cornflakes_libos::utils::TOTAL_HEADER_SIZE;
            }
            if !inlined_obj_hdr {
                let data_slice =
                    data_buffer.mutable_slice(offset, offset + ordered_sga.get_hdr().len())?;
                data_slice.copy_from_slice(ordered_sga.get_hdr());
                offset += ordered_sga.get_hdr().len();
            }
            {
                #[cfg(feature = "profiler")]
                demikernel::timer!("Copying copy buffers");
                for seg in ordered_sga.iter().take(first_zero_copy_seg) {
                    tracing::debug!("Writing into slice [{}, {}]", offset, offset + seg.len());
                    let dst = data_buffer.mutable_slice(offset, offset + seg.len())?;
                    unsafe {
                        mlx5_rte_memcpy(dst.as_mut_ptr() as _, seg.addr().as_ptr() as _, seg.len());
                    }
                    offset += seg.len();
                }
            }
            let mut metadata_mbuf = MbufMetadata::from_buf(data_buffer);

            let (curr_dpseg, curr_completion) =
                self.post_mbuf_metadata(&mut metadata_mbuf, dpseg, completion);
            dpseg = curr_dpseg;
            completion = curr_completion;
        }

        // rest are zero-copy segments
        {
            #[cfg(feature = "profiler")]
            demikernel::timer!("process zero copy buffers");
            for mbuf_metadata in ordered_sga
                .iter()
                .skip(first_zero_copy_seg)
                .take(ordered_sga.len() - first_zero_copy_seg)
            {
                match mbuf_metadata {
                    RcSge::RefCounted(mbuf_metadata) => {
                        let mut mbuf_copy = mbuf_metadata.clone();
                        mbuf_copy.increment_refcnt();
                        tracing::debug!(seg =? mbuf_metadata.as_ref().as_ptr(), "Cur posting seg");
                        let (curr_dpseg, curr_completion) = {
                            tracing::debug!(
                                len = mbuf_metadata.data_len(),
                                off = mbuf_metadata.offset(),
                                buf =? mbuf_metadata.as_ref().as_ptr(),
                                "posting dpseg"
                            );
                            unsafe {
                                (
                                    custom_mlx5_add_dpseg(
                                        self.thread_context.get_context_ptr(),
                                        dpseg,
                                        mbuf_metadata.data(),
                                        mbuf_metadata.mempool(),
                                        mbuf_metadata.offset() as _,
                                        mbuf_metadata.data_len() as _,
                                    ),
                                    custom_mlx5_add_completion_info(
                                        self.thread_context.get_context_ptr(),
                                        completion,
                                        mbuf_metadata.data(),
                                        mbuf_metadata.mempool(),
                                    ),
                                )
                            }
                        };
                        dpseg = curr_dpseg;
                        completion = curr_completion;
                    }
                    RcSge::RawRef(_) => {
                        bail!("Non-zero copy segments cannot come after zero copy segments");
                    }
                }
            }
        }
        unsafe {
            custom_mlx5_finish_single_transmission(
                self.thread_context.get_context_ptr(),
                num_wqes_required as _,
            );
        }

        if end_batch {
            if !self.first_ctrl_seg.is_null() {
                let _ = self.post_curr_transmissions(Some(self.first_ctrl_seg));
                self.poll_for_completions()?;
                self.first_ctrl_seg = ptr::null_mut();
            }
        }
        Ok(())
    }

    fn queue_datapath_buffer(
        &mut self,
        msg_id: MsgID,
        conn_id: ConnID,
        mut datapath_buffer: Self::DatapathBuffer,
        end_batch: bool,
    ) -> Result<()> {
        #[cfg(feature = "profiler")]
        demikernel::timer!("queue datapath buffer");

        let mut curr_available_wqes: usize =
            unsafe { custom_mlx5_num_wqes_available(self.thread_context.get_context_ptr()) }
                as usize;

        let num_required = 1;
        let num_octowords = unsafe { custom_mlx5_num_octowords(0, 1) };
        while num_required > curr_available_wqes {
            if self.first_ctrl_seg != ptr::null_mut() {
                if unsafe {
                    custom_mlx5_post_transmissions(
                        self.thread_context.get_context_ptr(),
                        self.first_ctrl_seg,
                    ) != 0
                } {
                    bail!("Failed to post transmissions so far");
                } else {
                    self.first_ctrl_seg = ptr::null_mut();
                }
            }
            self.poll_for_completions()?;
            curr_available_wqes =
                unsafe { custom_mlx5_num_wqes_available(self.thread_context.get_context_ptr()) }
                    as usize;
        }

        let ctrl_seg = unsafe {
            custom_mlx5_fill_in_hdr_segment(
                self.thread_context.get_context_ptr(),
                num_octowords as _,
                num_required as _,
                0,
                1,
                MLX5_ETH_WQE_L3_CSUM as i32 | MLX5_ETH_WQE_L4_CSUM as i32,
            )
        };
        if ctrl_seg.is_null() {
            bail!("Error posting header segment for sga");
        }

        if self.first_ctrl_seg == ptr::null_mut() {
            self.first_ctrl_seg = ctrl_seg;
        }
        // for queue datapath buffer, copy the header directly into the front
        let data_len = datapath_buffer.as_ref().len() - cornflakes_libos::utils::TOTAL_HEADER_SIZE;
        self.copy_hdr(&mut datapath_buffer, conn_id, msg_id, data_len)?;
        let mut metadata_mbuf = MbufMetadata::from_buf(datapath_buffer);

        let dpseg = unsafe { custom_mlx5_dpseg_start(self.thread_context.get_context_ptr(), 0) };
        let completion =
            unsafe { custom_mlx5_completion_start(self.thread_context.get_context_ptr()) };
        let _ = self.post_mbuf_metadata(&mut metadata_mbuf, dpseg, completion);

        unsafe {
            custom_mlx5_finish_single_transmission(
                self.thread_context.get_context_ptr(),
                num_required as _,
            );
        }

        if end_batch {
            if !self.first_ctrl_seg.is_null() {
                let _ = self.post_curr_transmissions(Some(self.first_ctrl_seg));
                self.poll_for_completions()?;
                self.first_ctrl_seg = ptr::null_mut();
            }
        }
        Ok(())
    }

    fn queue_metadata_vec(
        &mut self,
        msg_id: MsgID,
        conn_id: ConnID,
        metadata_vec: Vec<Self::DatapathMetadata>,
        end_batch: bool,
    ) -> Result<()> {
        #[cfg(feature = "profiler")]
        demikernel::timer!("queue metadata vec");

        let mut curr_available_wqes: usize =
            unsafe { custom_mlx5_num_wqes_available(self.thread_context.get_context_ptr()) }
                as usize;

        let (num_octowords, num_required, inline_len, allocation_size, num_segs) =
            match self.inline_mode {
                InlineMode::Nothing => {
                    let num_segs = metadata_vec.len() + 1;
                    let num_octowords = unsafe { custom_mlx5_num_octowords(0, num_segs as u64) };
                    let num_required = unsafe { custom_mlx5_num_wqes_required(num_octowords as _) };
                    (
                        num_octowords,
                        num_required,
                        0,
                        cornflakes_libos::utils::TOTAL_HEADER_SIZE,
                        num_segs,
                    )
                }
                InlineMode::PacketHeader => {
                    let num_segs = metadata_vec.len();
                    let num_octowords = unsafe {
                        custom_mlx5_num_octowords(
                            cornflakes_libos::utils::TOTAL_HEADER_SIZE as u64,
                            num_segs as u64,
                        )
                    };
                    let num_required = unsafe { custom_mlx5_num_wqes_required(num_octowords as _) };
                    let inline_len = cornflakes_libos::utils::TOTAL_HEADER_SIZE;
                    (num_octowords, num_required, inline_len, 0, num_segs)
                }
                InlineMode::ObjectHeader => {
                    unimplemented!();
                }
            };
        while num_required as usize > curr_available_wqes {
            if self.first_ctrl_seg != ptr::null_mut() {
                if unsafe {
                    custom_mlx5_post_transmissions(
                        self.thread_context.get_context_ptr(),
                        self.first_ctrl_seg,
                    ) != 0
                } {
                    bail!("Failed to post transmissions so far");
                } else {
                    self.first_ctrl_seg = ptr::null_mut();
                }
            }
            self.poll_for_completions()?;
            curr_available_wqes =
                unsafe { custom_mlx5_num_wqes_available(self.thread_context.get_context_ptr()) }
                    as usize;
        }

        let ctrl_seg = unsafe {
            custom_mlx5_fill_in_hdr_segment(
                self.thread_context.get_context_ptr(),
                num_octowords as _,
                num_required as _,
                0,
                num_segs as _,
                MLX5_ETH_WQE_L3_CSUM as i32 | MLX5_ETH_WQE_L4_CSUM as i32,
            )
        };
        if ctrl_seg.is_null() {
            bail!("Error posting header segment.");
        }

        if self.first_ctrl_seg == ptr::null_mut() {
            self.first_ctrl_seg = ctrl_seg;
        }

        let mut dpseg =
            unsafe { custom_mlx5_dpseg_start(self.thread_context.get_context_ptr(), inline_len) };
        let mut completion =
            unsafe { custom_mlx5_completion_start(self.thread_context.get_context_ptr()) };
        // inline or copy header as necessary
        let data_len: usize = metadata_vec
            .iter()
            .map(|seg| seg.as_ref().len())
            .sum::<usize>();
        let _ = self.inline_hdr_if_necessary(conn_id, msg_id, inline_len, data_len, &[])?;
        if allocation_size > 0 {
            let mut datapath_buffer = {
                #[cfg(feature = "profiler")]
                demikernel::timer!("allocating stuff to copy into");
                match self.allocator.allocate_tx_buffer()? {
                    Some(buf) => buf,
                    None => {
                        bail!("No tx mempools to allocate outgoing packet");
                    }
                }
            };
            self.copy_hdr(&mut datapath_buffer, conn_id, msg_id, data_len)?;
            let mut metadata_mbuf = MbufMetadata::from_buf(datapath_buffer);
            let (curr_dpseg, curr_completion) =
                self.post_mbuf_metadata(&mut metadata_mbuf, dpseg, completion);
            dpseg = curr_dpseg;
            completion = curr_completion;
        }
        // iterate over entries and post in sequence
        for mut metadata in metadata_vec.into_iter() {
            let (curr_dpseg, curr_completion) =
                self.post_mbuf_metadata(&mut metadata, dpseg, completion);
            dpseg = curr_dpseg;
            completion = curr_completion;
        }

        unsafe {
            custom_mlx5_finish_single_transmission(
                self.thread_context.get_context_ptr(),
                num_required as _,
            );
        }

        if end_batch {
            if !self.first_ctrl_seg.is_null() {
                let _ = self.post_curr_transmissions(Some(self.first_ctrl_seg));
                self.poll_for_completions()?;
                self.first_ctrl_seg = ptr::null_mut();
            }
        }
        Ok(())
    }

    fn queue_cornflakes_arena_object<'arena>(
        &mut self,
        msg_id: MsgID,
        conn_id: ConnID,
        cornflakes_obj: impl cornflakes_libos::dynamic_object_arena_hdr::CornflakesArenaObject<
            'arena,
            Self,
        >,
        end_batch: bool,
    ) -> Result<()>
    where
        Self: Sized,
    {
        #[cfg(feature = "profiler")]
        demikernel::timer!("queue cornflakes hybrid obj");
        tracing::debug!(msg_id, conn_id, end_batch, "Queue cornflakes hybrid obj");
        let mut curr_available_wqes: usize =
            unsafe { custom_mlx5_num_wqes_available(self.thread_context.get_context_ptr()) }
                as usize;

        let serialization_info = cornflakes_obj.get_serialization_info();
        let (inline_len, total_num_entries) =
            self.cornflakes_hybrid_object_shape(&serialization_info);
        let num_required = unsafe {
            custom_mlx5_num_wqes_required(custom_mlx5_num_octowords(
                inline_len as _,
                total_num_entries as _,
            )) as usize
        };
        tracing::debug!(
            inline_len,
            total_num_entries,
            num_required,
            "Number of wqes needed for posting"
        );
        while num_required > curr_available_wqes {
            curr_available_wqes = self.post_curr_transmissions_and_get_available_wqes()?;
        }

        self.post_ctrl_segment(num_required, inline_len, total_num_entries)?;

        // first, iterate over the entries to fill the headers
        let mut ring_buffer_state = (
            unsafe {
                custom_mlx5_dpseg_start(self.thread_context.get_context_ptr(), inline_len as _)
            },
            unsafe { custom_mlx5_completion_start(self.thread_context.get_context_ptr()) },
        );
        tracing::debug!("Ring buffer state: {:?}", ring_buffer_state);

        let first_copy_dpseg = ring_buffer_state.0;
        let first_copy_completion = ring_buffer_state.1;
        let mut skip_entry = false;
        if serialization_info.copy_length > 0 {
            skip_entry = true;
        }
        if serialization_info.header_size > 0 && self.inline_mode != InlineMode::ObjectHeader {
            skip_entry = true;
        }
        let entries_to_skip = (skip_entry) as usize;
        tracing::debug!("Entries to skip: {}", entries_to_skip);
        for _ in 0..entries_to_skip {
            ring_buffer_state.0 = unsafe {
                custom_mlx5_advance_dpseg(
                    self.thread_context.get_context_ptr(),
                    ring_buffer_state.0,
                )
            };
            ring_buffer_state.1 = unsafe {
                custom_mlx5_advance_completion_info(
                    self.thread_context.get_context_ptr(),
                    ring_buffer_state.1,
                )
            };
        }
        tracing::debug!("Ring buffer state after advancing: {:?}", ring_buffer_state);
        // callback to post metadata onto the ring buffer
        let mut callback = |metadata_mbuf: &MbufMetadata,
                            ring_buffer_state: &mut (
            *mut mlx5_wqe_data_seg,
            *mut custom_mlx5_transmission_info,
        )|
         -> Result<()> {
            let mut metadata_clone = metadata_mbuf.clone();
            metadata_clone.increment_refcnt();
            tracing::debug!(
                len = metadata_mbuf.data_len(),
                off = metadata_mbuf.offset(),
                buf =? metadata_mbuf.as_ref().as_ptr(),
                "posting dpseg in callback"
            );
            unsafe {
                ring_buffer_state.0 = custom_mlx5_add_dpseg(
                    self.thread_context.get_context_ptr(),
                    ring_buffer_state.0,
                    metadata_mbuf.data(),
                    metadata_mbuf.mempool(),
                    metadata_mbuf.offset() as _,
                    metadata_mbuf.data_len() as _,
                );
                ring_buffer_state.1 = custom_mlx5_add_completion_info(
                    self.thread_context.get_context_ptr(),
                    ring_buffer_state.1,
                    metadata_mbuf.data(),
                    metadata_mbuf.mempool(),
                );
            }
            Ok(())
        };

        // call  iterate on entries and allocate extra buffer space based on inline mode
        match self.inline_mode {
            InlineMode::Nothing => {
                // allocate buffer for packet header, object header and copied data
                let mut allocated_header_buffer = {
                    match self.allocator.allocate_tx_buffer()? {
                        Some(buf) => buf,
                        None => {
                            bail!("No tx mempools to allocate outgoing packet");
                        }
                    }
                };
                let data_len = serialization_info.header_size
                    + serialization_info.copy_length
                    + serialization_info.zero_copy_length;
                // copy header
                self.copy_hdr(&mut allocated_header_buffer, conn_id, msg_id, data_len)?;

                let header_buffer = allocated_header_buffer.mutable_slice(
                    cornflakes_libos::utils::TOTAL_HEADER_SIZE,
                    cornflakes_libos::utils::TOTAL_HEADER_SIZE
                        + serialization_info.header_size
                        + serialization_info.copy_length,
                )?;
                let mut copy_buffer: Option<&mut [u8]> = None;
                let mut cur_copy_offset = 0;
                let mut cur_zero_copy_offset = 0;
                cornflakes_obj.iterate_over_entries(
                    &serialization_info,
                    header_buffer,
                    &mut copy_buffer,
                    0,
                    cornflakes_obj.dynamic_header_start(),
                    &mut cur_copy_offset,
                    &mut cur_zero_copy_offset,
                    &mut callback,
                    &mut ring_buffer_state,
                )?;
                // set length of copied segment with udp header size + object header + copied data
                allocated_header_buffer.set_len(
                    cornflakes_libos::utils::TOTAL_HEADER_SIZE
                        + serialization_info.header_size
                        + serialization_info.copy_length,
                );
                // reset ring buffer state to first entry that was skipped
                ring_buffer_state.0 = first_copy_dpseg;
                ring_buffer_state.1 = first_copy_completion;
                // turn the data buffer into metadata and post it
                let mut metadata_mbuf = MbufMetadata::from_buf(allocated_header_buffer);
                let (_curr_dpseg, _curr_completion) = self.post_mbuf_metadata(
                    &mut metadata_mbuf,
                    ring_buffer_state.0,
                    ring_buffer_state.1,
                );
            }
            InlineMode::PacketHeader => {
                let data_len = serialization_info.header_size
                    + serialization_info.copy_length
                    + serialization_info.zero_copy_length;
                // inline (just) packet header
                let _ = self.inline_hdr_if_necessary(conn_id, msg_id, inline_len, data_len, &[])?;
                // buffer should have space for object header and copied data
                let mut allocated_header_buffer = {
                    match self.allocator.allocate_tx_buffer()? {
                        Some(buf) => buf,
                        None => {
                            bail!("No tx mempools to allocate outgoing packet");
                        }
                    }
                };
                let header_buffer = allocated_header_buffer.mutable_slice(
                    0,
                    serialization_info.header_size + serialization_info.copy_length,
                )?;
                let mut copy_buffer: Option<&mut [u8]> = None;
                let mut cur_copy_offset = 0;
                let mut cur_zero_copy_offset = 0;
                cornflakes_obj.iterate_over_entries(
                    &serialization_info,
                    header_buffer,
                    &mut copy_buffer,
                    0,
                    cornflakes_obj.dynamic_header_start(),
                    &mut cur_copy_offset,
                    &mut cur_zero_copy_offset,
                    &mut callback,
                    &mut ring_buffer_state,
                )?;
                allocated_header_buffer
                    .set_len(serialization_info.header_size + serialization_info.copy_length);
                // reset ring buffer state
                ring_buffer_state.0 = first_copy_dpseg;
                ring_buffer_state.1 = first_copy_completion;
                // turn the data buffer into metadata and post it
                let mut metadata_mbuf = MbufMetadata::from_buf(allocated_header_buffer);
                let (_curr_dpseg, _curr_completion) = self.post_mbuf_metadata(
                    &mut metadata_mbuf,
                    ring_buffer_state.0,
                    ring_buffer_state.1,
                );
            }
            InlineMode::ObjectHeader => {
                let mut allocated_buffer = match serialization_info.copy_length > 0 {
                    true => match self.allocator.allocate_tx_buffer()? {
                        Some(buf) => Some(buf),
                        None => {
                            bail!("No tx mempools to allocate outgoing packet");
                        }
                    },
                    false => None,
                };
                let mut copy_buffer: Option<&mut [u8]> = match allocated_buffer {
                    Some(ref mut x) => Some(x.mutable_slice(0, serialization_info.copy_length)?),
                    None => None,
                };

                let mut cur_copy_offset = 0;
                let mut cur_zero_copy_offset = 0;
                cornflakes_obj.iterate_over_entries(
                    &serialization_info,
                    &mut self.header_buffer.as_mut_slice()[0..serialization_info.header_size],
                    &mut copy_buffer,
                    0,
                    cornflakes_obj.dynamic_header_start(),
                    &mut cur_copy_offset,
                    &mut cur_zero_copy_offset,
                    &mut callback,
                    &mut ring_buffer_state,
                )?;
                let data_len = serialization_info.header_size
                    + serialization_info.copy_length
                    + serialization_info.zero_copy_length;
                let _ = self.inline_hdr_if_necessary(
                    conn_id,
                    msg_id,
                    inline_len,
                    data_len,
                    &self.header_buffer[0..serialization_info.header_size],
                )?;

                // post the copy data onto the ring buffer if necessary
                match allocated_buffer {
                    Some(mut x) => {
                        x.set_len(serialization_info.copy_length);
                        // reset the dpseg
                        ring_buffer_state.0 = first_copy_dpseg;
                        ring_buffer_state.1 = first_copy_completion;
                        let mut metadata_mbuf = MbufMetadata::from_buf(x);
                        let (_curr_dpseg, _curr_completion) = self.post_mbuf_metadata(
                            &mut metadata_mbuf,
                            ring_buffer_state.0,
                            ring_buffer_state.1,
                        );
                    }
                    None => {}
                }
            }
        }

        // finish transmission
        self.finish_transmission(num_required, end_batch)?;
        Ok(())
    }

    fn queue_cornflakes_hybrid_object(
        &mut self,
        msg_id: MsgID,
        conn_id: ConnID,
        cornflakes_obj: impl cornflakes_libos::dynamic_object_hdr::CornflakesObject<Self>,
        end_batch: bool,
    ) -> Result<()>
    where
        Self: Sized,
    {
        #[cfg(feature = "profiler")]
        demikernel::timer!("queue cornflakes hybrid obj");
        tracing::debug!(msg_id, conn_id, end_batch, "Queue cornflakes hybrid obj");
        let mut curr_available_wqes: usize =
            unsafe { custom_mlx5_num_wqes_available(self.thread_context.get_context_ptr()) }
                as usize;

        let serialization_info = cornflakes_obj.get_serialization_info();
        let (inline_len, total_num_entries) =
            self.cornflakes_hybrid_object_shape(&serialization_info);
        let num_required = unsafe {
            custom_mlx5_num_wqes_required(custom_mlx5_num_octowords(
                inline_len as _,
                total_num_entries as _,
            )) as usize
        };
        tracing::debug!(
            inline_len,
            total_num_entries,
            num_required,
            "Number of wqes needed for posting"
        );
        while num_required > curr_available_wqes {
            curr_available_wqes = self.post_curr_transmissions_and_get_available_wqes()?;
        }

        self.post_ctrl_segment(num_required, inline_len, total_num_entries)?;

        // first, iterate over the entries to fill the headers
        let mut ring_buffer_state = (
            unsafe {
                custom_mlx5_dpseg_start(self.thread_context.get_context_ptr(), inline_len as _)
            },
            unsafe { custom_mlx5_completion_start(self.thread_context.get_context_ptr()) },
        );
        tracing::debug!("Ring buffer state: {:?}", ring_buffer_state);

        let first_copy_dpseg = ring_buffer_state.0;
        let first_copy_completion = ring_buffer_state.1;
        let mut skip_entry = false;
        if serialization_info.copy_length > 0 {
            skip_entry = true;
        }
        if serialization_info.header_size > 0 && self.inline_mode != InlineMode::ObjectHeader {
            skip_entry = true;
        }
        let entries_to_skip = (skip_entry) as usize;
        tracing::debug!("Entries to skip: {}", entries_to_skip);
        for _ in 0..entries_to_skip {
            ring_buffer_state.0 = unsafe {
                custom_mlx5_advance_dpseg(
                    self.thread_context.get_context_ptr(),
                    ring_buffer_state.0,
                )
            };
            ring_buffer_state.1 = unsafe {
                custom_mlx5_advance_completion_info(
                    self.thread_context.get_context_ptr(),
                    ring_buffer_state.1,
                )
            };
        }
        tracing::debug!("Ring buffer state after advancing: {:?}", ring_buffer_state);
        // callback to post metadata onto the ring buffer
        let mut callback = |metadata_mbuf: &MbufMetadata,
                            ring_buffer_state: &mut (
            *mut mlx5_wqe_data_seg,
            *mut custom_mlx5_transmission_info,
        )|
         -> Result<()> {
            let mut metadata_clone = metadata_mbuf.clone();
            metadata_clone.increment_refcnt();
            tracing::debug!(
                len = metadata_mbuf.data_len(),
                off = metadata_mbuf.offset(),
                buf =? metadata_mbuf.as_ref().as_ptr(),
                "posting dpseg in callback"
            );
            unsafe {
                ring_buffer_state.0 = custom_mlx5_add_dpseg(
                    self.thread_context.get_context_ptr(),
                    ring_buffer_state.0,
                    metadata_mbuf.data(),
                    metadata_mbuf.mempool(),
                    metadata_mbuf.offset() as _,
                    metadata_mbuf.data_len() as _,
                );
                ring_buffer_state.1 = custom_mlx5_add_completion_info(
                    self.thread_context.get_context_ptr(),
                    ring_buffer_state.1,
                    metadata_mbuf.data(),
                    metadata_mbuf.mempool(),
                );
            }
            Ok(())
        };

        // call  iterate on entries and allocate extra buffer space based on inline mode
        match self.inline_mode {
            InlineMode::Nothing => {
                // allocate buffer for packet header, object header and copied data
                let mut allocated_header_buffer = {
                    match self.allocator.allocate_tx_buffer()? {
                        Some(buf) => buf,
                        None => {
                            bail!("No tx mempools to allocate outgoing packet");
                        }
                    }
                };
                let data_len = serialization_info.header_size
                    + serialization_info.copy_length
                    + serialization_info.zero_copy_length;
                // copy header
                self.copy_hdr(&mut allocated_header_buffer, conn_id, msg_id, data_len)?;

                let header_buffer = allocated_header_buffer.mutable_slice(
                    cornflakes_libos::utils::TOTAL_HEADER_SIZE,
                    cornflakes_libos::utils::TOTAL_HEADER_SIZE
                        + serialization_info.header_size
                        + serialization_info.copy_length,
                )?;
                let mut copy_buffer: Option<&mut [u8]> = None;
                let mut cur_copy_offset = 0;
                let mut cur_zero_copy_offset = 0;
                cornflakes_obj.iterate_over_entries(
                    &serialization_info,
                    header_buffer,
                    &mut copy_buffer,
                    0,
                    cornflakes_obj.dynamic_header_start(),
                    &mut cur_copy_offset,
                    &mut cur_zero_copy_offset,
                    &mut callback,
                    &mut ring_buffer_state,
                )?;
                // set length of copied segment with udp header size + object header + copied data
                allocated_header_buffer.set_len(
                    cornflakes_libos::utils::TOTAL_HEADER_SIZE
                        + serialization_info.header_size
                        + serialization_info.copy_length,
                );
                // reset ring buffer state to first entry that was skipped
                ring_buffer_state.0 = first_copy_dpseg;
                ring_buffer_state.1 = first_copy_completion;
                // turn the data buffer into metadata and post it
                let mut metadata_mbuf = MbufMetadata::from_buf(allocated_header_buffer);
                let (_curr_dpseg, _curr_completion) = self.post_mbuf_metadata(
                    &mut metadata_mbuf,
                    ring_buffer_state.0,
                    ring_buffer_state.1,
                );
            }
            InlineMode::PacketHeader => {
                let data_len = serialization_info.header_size
                    + serialization_info.copy_length
                    + serialization_info.zero_copy_length;
                // inline (just) packet header
                let _ = self.inline_hdr_if_necessary(conn_id, msg_id, inline_len, data_len, &[])?;
                // buffer should have space for object header and copied data
                let mut allocated_header_buffer = {
                    match self.allocator.allocate_tx_buffer()? {
                        Some(buf) => buf,
                        None => {
                            bail!("No tx mempools to allocate outgoing packet");
                        }
                    }
                };
                let header_buffer = allocated_header_buffer.mutable_slice(
                    0,
                    serialization_info.header_size + serialization_info.copy_length,
                )?;
                let mut copy_buffer: Option<&mut [u8]> = None;
                let mut cur_copy_offset = 0;
                let mut cur_zero_copy_offset = 0;
                cornflakes_obj.iterate_over_entries(
                    &serialization_info,
                    header_buffer,
                    &mut copy_buffer,
                    0,
                    cornflakes_obj.dynamic_header_start(),
                    &mut cur_copy_offset,
                    &mut cur_zero_copy_offset,
                    &mut callback,
                    &mut ring_buffer_state,
                )?;
                allocated_header_buffer
                    .set_len(serialization_info.header_size + serialization_info.copy_length);
                // reset ring buffer state
                ring_buffer_state.0 = first_copy_dpseg;
                ring_buffer_state.1 = first_copy_completion;
                // turn the data buffer into metadata and post it
                let mut metadata_mbuf = MbufMetadata::from_buf(allocated_header_buffer);
                let (_curr_dpseg, _curr_completion) = self.post_mbuf_metadata(
                    &mut metadata_mbuf,
                    ring_buffer_state.0,
                    ring_buffer_state.1,
                );
            }
            InlineMode::ObjectHeader => {
                let mut allocated_buffer = match serialization_info.copy_length > 0 {
                    true => match self.allocator.allocate_tx_buffer()? {
                        Some(buf) => Some(buf),
                        None => {
                            bail!("No tx mempools to allocate outgoing packet");
                        }
                    },
                    false => None,
                };
                let mut copy_buffer: Option<&mut [u8]> = match allocated_buffer {
                    Some(ref mut x) => Some(x.mutable_slice(0, serialization_info.copy_length)?),
                    None => None,
                };

                let mut cur_copy_offset = 0;
                let mut cur_zero_copy_offset = 0;
                cornflakes_obj.iterate_over_entries(
                    &serialization_info,
                    &mut self.header_buffer.as_mut_slice()[0..serialization_info.header_size],
                    &mut copy_buffer,
                    0,
                    cornflakes_obj.dynamic_header_start(),
                    &mut cur_copy_offset,
                    &mut cur_zero_copy_offset,
                    &mut callback,
                    &mut ring_buffer_state,
                )?;
                let data_len = serialization_info.header_size
                    + serialization_info.copy_length
                    + serialization_info.zero_copy_length;
                let _ = self.inline_hdr_if_necessary(
                    conn_id,
                    msg_id,
                    inline_len,
                    data_len,
                    &self.header_buffer[0..serialization_info.header_size],
                )?;

                // post the copy data onto the ring buffer if necessary
                match allocated_buffer {
                    Some(mut x) => {
                        x.set_len(serialization_info.copy_length);
                        // reset the dpseg
                        ring_buffer_state.0 = first_copy_dpseg;
                        ring_buffer_state.1 = first_copy_completion;
                        let mut metadata_mbuf = MbufMetadata::from_buf(x);
                        let (_curr_dpseg, _curr_completion) = self.post_mbuf_metadata(
                            &mut metadata_mbuf,
                            ring_buffer_state.0,
                            ring_buffer_state.1,
                        );
                    }
                    None => {}
                }
            }
        }

        // finish transmission
        self.finish_transmission(num_required, end_batch)?;

        Ok(())
    }

    fn queue_cornflakes_obj<'arena>(
        &mut self,
        msg_id: MsgID,
        conn_id: ConnID,
        copy_context: &mut CopyContext<'arena, Self>,
        cornflakes_obj: impl HybridArenaRcSgaHdr<'arena, Self>,
        end_batch: bool,
    ) -> Result<()>
    where
        Self: Sized,
    {
        #[cfg(feature = "profiler")]
        demikernel::timer!("queue cornflakes obj");
        tracing::debug!(msg_id, conn_id, end_batch, "Queue cornflakes obj");
        let mut curr_available_wqes: usize =
            unsafe { custom_mlx5_num_wqes_available(self.thread_context.get_context_ptr()) }
                as usize;

        let num_zero_copy_entries = { cornflakes_obj.num_zero_copy_scatter_gather_entries() };
        let mut num_copy_entries = copy_context.len();
        if copy_context.data_len() == 0 {
            // doesn't work when there is a "raw" cf bytes
            num_copy_entries = 0;
        }
        let header_len = { cornflakes_obj.total_header_size(false, false) };
        let (inline_len, total_num_entries) =
            self.cornflakes_obj_shape(header_len, num_copy_entries, num_zero_copy_entries);
        let num_required = unsafe {
            custom_mlx5_num_wqes_required(custom_mlx5_num_octowords(
                inline_len as _,
                total_num_entries as _,
            )) as usize
        };
        tracing::debug!(
            inline_len,
            total_num_entries,
            num_required,
            "Number of wqes needed for posting"
        );
        {
            while num_required > curr_available_wqes {
                curr_available_wqes = self.post_curr_transmissions_and_get_available_wqes()?;
            }
        }

        // fill in hdr segments
        let num_octowords =
            unsafe { custom_mlx5_num_octowords(inline_len as _, total_num_entries as _) };
        tracing::debug!(
            inline_len,
            total_num_entries,
            num_octowords,
            num_required,
            "Header info"
        );
        let ctrl_seg = unsafe {
            custom_mlx5_fill_in_hdr_segment(
                self.thread_context.get_context_ptr(),
                num_octowords as _,
                num_required as _,
                inline_len as _,
                total_num_entries as _,
                MLX5_ETH_WQE_L3_CSUM as i32 | MLX5_ETH_WQE_L4_CSUM as i32,
            )
        };
        if ctrl_seg.is_null() {
            bail!("Error posting header segment for sga");
        }

        if self.first_ctrl_seg == ptr::null_mut() {
            self.first_ctrl_seg = ctrl_seg;
        }

        // first, iterate over the entries to fill the headers
        let mut ring_buffer_state = (
            unsafe {
                custom_mlx5_dpseg_start(self.thread_context.get_context_ptr(), inline_len as _)
            },
            unsafe { custom_mlx5_completion_start(self.thread_context.get_context_ptr()) },
        );
        tracing::debug!("Ring buffer state: {:?}", ring_buffer_state);

        let first_copy_dpseg = ring_buffer_state.0;
        let first_copy_completion = ring_buffer_state.1;
        let mut entries_to_skip = num_copy_entries;
        // if not inlining anything, or need to write the packet header, add 1
        if (header_len > 0 && self.inline_mode != InlineMode::ObjectHeader)
            || (self.inline_mode == InlineMode::Nothing)
        {
            entries_to_skip += 1;
        }
        tracing::debug!("Entries to skip: {}", entries_to_skip);
        for _ in 0..entries_to_skip {
            ring_buffer_state.0 = unsafe {
                custom_mlx5_advance_dpseg(
                    self.thread_context.get_context_ptr(),
                    ring_buffer_state.0,
                )
            };
            ring_buffer_state.1 = unsafe {
                custom_mlx5_advance_completion_info(
                    self.thread_context.get_context_ptr(),
                    ring_buffer_state.1,
                )
            };
        }
        tracing::debug!("Ring buffer state after advancing: {:?}", ring_buffer_state);
        // callback to post metadata onto the ring buffer
        let mut callback = |metadata_mbuf: &MbufMetadata,
                            ring_buffer_state: &mut (
            *mut mlx5_wqe_data_seg,
            *mut custom_mlx5_transmission_info,
        )|
         -> Result<()> {
            let mut metadata_clone = metadata_mbuf.clone();
            metadata_clone.increment_refcnt();
            tracing::debug!(
                len = metadata_mbuf.data_len(),
                off = metadata_mbuf.offset(),
                buf =? metadata_mbuf.as_ref().as_ptr(),
                "posting dpseg in callback"
            );
            unsafe {
                ring_buffer_state.0 = custom_mlx5_add_dpseg(
                    self.thread_context.get_context_ptr(),
                    ring_buffer_state.0,
                    metadata_mbuf.data(),
                    metadata_mbuf.mempool(),
                    metadata_mbuf.offset() as _,
                    metadata_mbuf.data_len() as _,
                );
                ring_buffer_state.1 = custom_mlx5_add_completion_info(
                    self.thread_context.get_context_ptr(),
                    ring_buffer_state.1,
                    metadata_mbuf.data(),
                    metadata_mbuf.mempool(),
                );
            }
            Ok(())
        };

        let mut cur_entry_ptr: usize = header_len + copy_context.data_len();
        match self.inline_mode {
            InlineMode::Nothing => {
                let mut allocated_header_buffer = {
                    match self.allocator.allocate_tx_buffer()? {
                        Some(buf) => buf,
                        None => {
                            bail!("No tx mempools to allocate outgoing packet");
                        }
                    }
                };
                let data_len = {
                    cornflakes_obj.iterate_over_entries(
                        copy_context,
                        header_len,
                        allocated_header_buffer.mutable_slice(
                            cornflakes_libos::utils::TOTAL_HEADER_SIZE,
                            cornflakes_libos::utils::TOTAL_HEADER_SIZE + header_len,
                        )?,
                        0,
                        cornflakes_obj.dynamic_header_start(),
                        &mut cur_entry_ptr,
                        &mut callback,
                        &mut ring_buffer_state,
                    )? + header_len
                };

                // copy the packet header into the beginning of the buffer
                self.copy_hdr(&mut allocated_header_buffer, conn_id, msg_id, data_len)?;
                allocated_header_buffer
                    .set_len(header_len + cornflakes_libos::utils::TOTAL_HEADER_SIZE);
                // reset ring buffer state
                ring_buffer_state.0 = first_copy_dpseg;
                ring_buffer_state.1 = first_copy_completion;
                // turn the data buffer into metadata and post it
                let mut metadata_mbuf = MbufMetadata::from_buf(allocated_header_buffer);

                let (curr_dpseg, curr_completion) = self.post_mbuf_metadata(
                    &mut metadata_mbuf,
                    ring_buffer_state.0,
                    ring_buffer_state.1,
                );
                ring_buffer_state.0 = curr_dpseg;
                ring_buffer_state.1 = curr_completion;
            }
            InlineMode::PacketHeader => {
                let mut allocated_header_buffer = {
                    match self.allocator.allocate_tx_buffer()? {
                        Some(buf) => buf,
                        None => {
                            bail!("No tx mempools to allocate outgoing packet");
                        }
                    }
                };
                let data_len = cornflakes_obj.iterate_over_entries(
                    copy_context,
                    header_len,
                    allocated_header_buffer.mutable_slice(0, header_len)?,
                    0,
                    cornflakes_obj.dynamic_header_start(),
                    &mut cur_entry_ptr,
                    &mut callback,
                    &mut ring_buffer_state,
                )? + header_len;
                // inline (just) packet header
                let _ = self.inline_hdr_if_necessary(
                    conn_id,
                    msg_id,
                    inline_len,
                    data_len,
                    &allocated_header_buffer.as_ref()[0..header_len],
                )?;
                allocated_header_buffer.set_len(header_len);
                // reset ring buffer state
                ring_buffer_state.0 = first_copy_dpseg;
                ring_buffer_state.1 = first_copy_completion;
                // turn the data buffer into metadata and post it
                let mut metadata_mbuf = MbufMetadata::from_buf(allocated_header_buffer);

                let (curr_dpseg, curr_completion) = self.post_mbuf_metadata(
                    &mut metadata_mbuf,
                    ring_buffer_state.0,
                    ring_buffer_state.1,
                );
                ring_buffer_state.0 = curr_dpseg;
                ring_buffer_state.1 = curr_completion;
            }
            InlineMode::ObjectHeader => {
                // fill in cornflakes object, and then inline the header
                let data_len = cornflakes_obj.iterate_over_entries(
                    copy_context,
                    header_len,
                    &mut self.header_buffer.as_mut_slice()[0..header_len],
                    0,
                    cornflakes_obj.dynamic_header_start(),
                    &mut cur_entry_ptr,
                    &mut callback,
                    &mut ring_buffer_state,
                )? + header_len;
                // inline packet header and object header
                let _ = self.inline_hdr_if_necessary(
                    conn_id,
                    msg_id,
                    inline_len,
                    data_len,
                    &self.header_buffer[0..header_len],
                )?;

                // reset the dpseg
                ring_buffer_state.0 = first_copy_dpseg;
                ring_buffer_state.1 = first_copy_completion;
            }
        }

        // now, copy the context
        if copy_context.data_len() > 0 {
            for serialization_copy_buf in copy_context.copy_buffers_slice().iter() {
                let buffer = serialization_copy_buf.get_buffer();
                let mut metadata_mbuf = MbufMetadata::from_buf(buffer);
                let (curr_dpseg, curr_completion) = self.post_mbuf_metadata(
                    &mut metadata_mbuf,
                    ring_buffer_state.0,
                    ring_buffer_state.1,
                );
                ring_buffer_state.0 = curr_dpseg;
                ring_buffer_state.1 = curr_completion;
            }
        }

        // finish the transmission
        unsafe {
            custom_mlx5_finish_single_transmission(
                self.thread_context.get_context_ptr(),
                num_required as _,
            );
        }

        if end_batch {
            if !self.first_ctrl_seg.is_null() {
                let _ = self.post_curr_transmissions(Some(self.first_ctrl_seg));
                self.poll_for_completions()?;
                self.first_ctrl_seg = ptr::null_mut();
            }
        }

        Ok(())
    }

    fn queue_arena_datapath_sga<'a>(
        &mut self,
        sga: (MsgID, ConnID, ArenaDatapathSga<'a, Self>),
        end_batch: bool,
    ) -> Result<()> {
        tracing::debug!(msg_id = sga.0, conn_id = sga.1, ordered_sga =? sga.2, "Sending sga");
        #[cfg(feature = "profiler")]
        demikernel::timer!("queue arena datapath sga");
        let mut curr_available_wqes: usize =
            unsafe { custom_mlx5_num_wqes_available(self.thread_context.get_context_ptr()) }
                as usize;
        let msg_id = sga.0;
        let conn_id = sga.1;
        let mut arena_datapath_sga = sga.2;

        let num_required = {
            #[cfg(feature = "profiler")]
            demikernel::timer!("Calculating wqes required for sga");
            self.wqes_required_arena_datapath_sga(&arena_datapath_sga)
        };

        while num_required > curr_available_wqes {
            if self.first_ctrl_seg != ptr::null_mut() {
                if unsafe {
                    custom_mlx5_post_transmissions(
                        self.thread_context.get_context_ptr(),
                        self.first_ctrl_seg,
                    ) != 0
                } {
                    bail!("Failed to post transmissions so far");
                } else {
                    self.first_ctrl_seg = ptr::null_mut();
                }
            }
            self.poll_for_completions()?;
            curr_available_wqes =
                unsafe { custom_mlx5_num_wqes_available(self.thread_context.get_context_ptr()) }
                    as usize;
        }
        let (inline_len, num_wqes_required) = {
            #[cfg(feature = "profiler")]
            demikernel::timer!("Ordered sga and filling in ctrl and ether seg");

            let (inline_len, num_segs) = self.arena_datapath_sga_shape(&arena_datapath_sga);
            let num_octowords =
                unsafe { custom_mlx5_num_octowords(inline_len as _, num_segs as _) };
            let num_wqes_required = num_required as u64;
            tracing::debug!(
                inline_len,
                num_segs,
                num_octowords,
                num_wqes_required,
                "Header info"
            );
            let ctrl_seg = unsafe {
                custom_mlx5_fill_in_hdr_segment(
                    self.thread_context.get_context_ptr(),
                    num_octowords as _,
                    num_wqes_required as _,
                    inline_len as _,
                    num_segs as _,
                    MLX5_ETH_WQE_L3_CSUM as i32 | MLX5_ETH_WQE_L4_CSUM as i32,
                )
            };
            if ctrl_seg.is_null() {
                bail!("Error posting header segment for sga");
            }

            if self.first_ctrl_seg == ptr::null_mut() {
                self.first_ctrl_seg = ctrl_seg;
            }
            (inline_len, num_wqes_required)
        };

        let data_len = arena_datapath_sga.data_len();
        let (header_written, entry_idx) = self.inline_hdr_if_necessary(
            conn_id,
            msg_id,
            inline_len,
            data_len,
            arena_datapath_sga.get_header(),
        )?;

        // TODO: temporary hack for different code surrounding inlining first entry
        let inlined_obj_hdr = entry_idx == 1;

        let allocation_size = (!inlined_obj_hdr as usize * arena_datapath_sga.get_header().len())
            + (!header_written as usize * cornflakes_libos::utils::TOTAL_HEADER_SIZE);
        let mut dpseg = unsafe {
            custom_mlx5_dpseg_start(self.thread_context.get_context_ptr(), inline_len as _)
        };
        let mut completion =
            unsafe { custom_mlx5_completion_start(self.thread_context.get_context_ptr()) };
        tracing::debug!("Allocation size: {}", allocation_size);
        if allocation_size > 0 {
            #[cfg(feature = "profiler")]
            demikernel::timer!("Copying stuff");

            let mut data_buffer = {
                #[cfg(feature = "profiler")]
                demikernel::timer!("allocating stuff to copy into");
                match self.allocator.allocate_tx_buffer()? {
                    Some(buf) => buf,
                    None => {
                        bail!("No tx mempools to allocate outgoing packet");
                    }
                }
            };
            if !header_written {
                self.copy_hdr(&mut data_buffer, conn_id, msg_id, data_len)?;
            }
            if !inlined_obj_hdr {
                let hdr = arena_datapath_sga.get_header();
                ensure!(
                    data_buffer.write(hdr)? == hdr.len(),
                    "Failed to copy full object header into data buffer"
                );
            }

            // turn the data buffer into metadata and post it
            let mut metadata_mbuf = MbufMetadata::from_buf(data_buffer);

            let (curr_dpseg, curr_completion) =
                self.post_mbuf_metadata(&mut metadata_mbuf, dpseg, completion);
            dpseg = curr_dpseg;
            completion = curr_completion;
        }

        // if copy length in mbuf is more then one, clone underlying mbuf and post it
        if arena_datapath_sga.copy_len() > 0 {
            for serialization_copy_buf in arena_datapath_sga
                .copy_context()
                .copy_buffers_slice()
                .iter()
            {
                let buffer = serialization_copy_buf.get_buffer();
                let mut metadata_mbuf = MbufMetadata::from_buf(buffer);
                let (curr_dpseg, curr_completion) =
                    self.post_mbuf_metadata(&mut metadata_mbuf, dpseg, completion);
                dpseg = curr_dpseg;
                completion = curr_completion;
            }
        }

        // rest are zero copy segs
        for metadata_mbuf in arena_datapath_sga.zero_copy_entries_mut_slice().iter_mut() {
            let (curr_dpseg, curr_completion) =
                self.post_mbuf_metadata(metadata_mbuf, dpseg, completion);
            dpseg = curr_dpseg;
            completion = curr_completion;
        }

        // end
        unsafe {
            custom_mlx5_finish_single_transmission(
                self.thread_context.get_context_ptr(),
                num_wqes_required as _,
            );
        }

        if end_batch {
            if !self.first_ctrl_seg.is_null() {
                let _ = self.post_curr_transmissions(Some(self.first_ctrl_seg));
                self.poll_for_completions()?;
                self.first_ctrl_seg = ptr::null_mut();
            }
        }

        Ok(())
    }

    /// Pushes sga onto ring buffer.
    /// If no more space, if current first ctrl seg exists, rings doorbell and polls for
    /// completions.
    /// If end batch is true -- rings doorbell and polls for completions after posting this SGA.
    fn queue_arena_ordered_rcsga(
        &mut self,
        sga: (MsgID, ConnID, ArenaOrderedRcSga<Self>),
        end_batch: bool,
    ) -> Result<()> {
        tracing::debug!(msg_id = sga.0, conn_id = sga.1, ordered_sga =? sga.2, "Sending sga");
        #[cfg(feature = "profiler")]
        demikernel::timer!("queue arena ordered rcsga");

        let mut curr_available_wqes: usize =
            unsafe { custom_mlx5_num_wqes_available(self.thread_context.get_context_ptr()) }
                as usize;
        let msg_id = sga.0;
        let conn_id = sga.1;
        let ordered_sga = sga.2;

        let num_required = {
            #[cfg(feature = "profiler")]
            demikernel::timer!("Calculating wqes required for sga");
            self.wqes_required_arena_ordered_rcsga(&ordered_sga)
        };

        while num_required > curr_available_wqes {
            if self.first_ctrl_seg != ptr::null_mut() {
                if unsafe {
                    custom_mlx5_post_transmissions(
                        self.thread_context.get_context_ptr(),
                        self.first_ctrl_seg,
                    ) != 0
                } {
                    bail!("Failed to post transmissions so far");
                } else {
                    self.first_ctrl_seg = ptr::null_mut();
                }
            }
            self.poll_for_completions()?;
            curr_available_wqes =
                unsafe { custom_mlx5_num_wqes_available(self.thread_context.get_context_ptr()) }
                    as usize;
        }
        let (inline_len, _num_segs, num_wqes_required) = {
            #[cfg(feature = "profiler")]
            demikernel::timer!("Ordered sga and filling in ctrl and ether seg");

            let (inline_len, num_segs) = self.arena_ordered_rcsga_shape(&ordered_sga);
            let num_octowords =
                unsafe { custom_mlx5_num_octowords(inline_len as _, num_segs as _) };
            let num_wqes_required = num_required as u64;
            tracing::debug!(
                inline_len,
                num_segs,
                num_octowords,
                num_wqes_required,
                "Header info"
            );
            let ctrl_seg = unsafe {
                custom_mlx5_fill_in_hdr_segment(
                    self.thread_context.get_context_ptr(),
                    num_octowords as _,
                    num_wqes_required as _,
                    inline_len as _,
                    num_segs as _,
                    MLX5_ETH_WQE_L3_CSUM as i32 | MLX5_ETH_WQE_L4_CSUM as i32,
                )
            };
            if ctrl_seg.is_null() {
                bail!("Error posting header segment for sga");
            }

            if self.first_ctrl_seg == ptr::null_mut() {
                self.first_ctrl_seg = ctrl_seg;
            }
            (inline_len, num_segs, num_wqes_required)
        };
        let data_len = ordered_sga.data_len();
        let (header_written, entry_idx) = self.inline_hdr_if_necessary(
            conn_id,
            msg_id,
            inline_len,
            data_len,
            ordered_sga.get_hdr(),
        )?;

        // TODO: temporary hack for different code surrounding inlining first entry
        let inlined_obj_hdr = entry_idx == 1;

        let first_zero_copy_seg = ordered_sga.num_copy_entries();
        let allocation_size = ordered_sga.copy_length()
            - (inlined_obj_hdr as usize * ordered_sga.get_hdr().len())
            + (!header_written as usize * cornflakes_libos::utils::TOTAL_HEADER_SIZE);
        let mut dpseg = unsafe {
            custom_mlx5_dpseg_start(self.thread_context.get_context_ptr(), inline_len as _)
        };
        let mut completion =
            unsafe { custom_mlx5_completion_start(self.thread_context.get_context_ptr()) };
        tracing::debug!("Allocation size: {}", allocation_size);
        if allocation_size > 0 {
            #[cfg(feature = "profiler")]
            demikernel::timer!("Copying stuff");

            let mut data_buffer = {
                #[cfg(feature = "profiler")]
                demikernel::timer!("allocating stuff to copy into");
                match self.allocator.allocate_tx_buffer()? {
                    Some(buf) => buf,
                    None => {
                        bail!("No tx mempools to allocate outgoing packet");
                    }
                }
            };
            if !header_written {
                self.copy_hdr(&mut data_buffer, conn_id, msg_id, data_len)?;
            }
            if !inlined_obj_hdr {
                let hdr = ordered_sga.get_hdr();
                ensure!(
                    data_buffer.write(hdr)? == hdr.len(),
                    "Failed to copy full object header into data buffer"
                );
            }
            {
                #[cfg(feature = "profiler")]
                demikernel::timer!("Copying copy buffers");
                for seg in ordered_sga.iter().take(first_zero_copy_seg) {
                    ensure!(
                        data_buffer.write(seg.addr())? == seg.len(),
                        "Failed to copy segment into data buffer"
                    );
                }
            }
            let mut metadata_mbuf = MbufMetadata::from_buf(data_buffer);
            tracing::debug!(metadata_mbuf =? metadata_mbuf, "Calling post mbuf metadata for copied data");
            let (curr_dpseg, curr_completion) =
                self.post_mbuf_metadata(&mut metadata_mbuf, dpseg, completion);
            dpseg = curr_dpseg;
            completion = curr_completion;
        }

        // rest are zero-copy segments
        {
            #[cfg(feature = "profiler")]
            demikernel::timer!("process zero copy buffers");
            for mbuf_metadata in ordered_sga
                .iter()
                .skip(first_zero_copy_seg)
                .take(ordered_sga.len() - first_zero_copy_seg)
            {
                match mbuf_metadata {
                    RcSge::RefCounted(mbuf_metadata) => {
                        let mut mbuf_copy = mbuf_metadata.clone();
                        mbuf_copy.increment_refcnt();
                        tracing::debug!(seg =? mbuf_metadata.as_ref().as_ptr(), "Cur posting seg");
                        let (curr_dpseg, curr_completion) = {
                            tracing::debug!(
                                len = mbuf_metadata.data_len(),
                                off = mbuf_metadata.offset(),
                                buf =? mbuf_metadata.as_ref().as_ptr(),
                                "posting dpseg"
                            );
                            unsafe {
                                (
                                    custom_mlx5_add_dpseg(
                                        self.thread_context.get_context_ptr(),
                                        dpseg,
                                        mbuf_metadata.data(),
                                        mbuf_metadata.mempool(),
                                        mbuf_metadata.offset() as _,
                                        mbuf_metadata.data_len() as _,
                                    ),
                                    custom_mlx5_add_completion_info(
                                        self.thread_context.get_context_ptr(),
                                        completion,
                                        mbuf_metadata.data(),
                                        mbuf_metadata.mempool(),
                                    ),
                                )
                            }
                        };
                        dpseg = curr_dpseg;
                        completion = curr_completion;
                    }
                    RcSge::RawRef(_) => {
                        bail!("Non-zero copy segments cannot come after zero copy segments");
                    }
                }
            }
        }
        // end

        unsafe {
            custom_mlx5_finish_single_transmission(
                self.thread_context.get_context_ptr(),
                num_wqes_required as _,
            );
        }

        if end_batch {
            if !self.first_ctrl_seg.is_null() {
                let _ = self.post_curr_transmissions(Some(self.first_ctrl_seg));
                self.poll_for_completions()?;
                self.first_ctrl_seg = ptr::null_mut();
            }
        }

        Ok(())
    }
    /// Pushes sga onto ring buffer.
    /// If no more space, if current first ctrl seg exists, rings doorbell and polls for
    /// completions.
    /// If end batch is true -- rings doorbell and polls for completions after posting this SGA.
    fn queue_arena_ordered_sga(
        &mut self,
        sga: (MsgID, ConnID, ArenaOrderedSga),
        end_batch: bool,
    ) -> Result<()> {
        #[cfg(feature = "profiler")]
        demikernel::timer!("queue arena ordered sga");

        let mut curr_available_wqes: usize =
            unsafe { custom_mlx5_num_wqes_available(self.thread_context.get_context_ptr()) }
                as usize;
        let msg_id = sga.0;
        let conn_id = sga.1;
        let mut ordered_sga = sga.2;

        let allocator = &self.allocator;
        let mbuf_metadatas = &mut self.mbuf_metadatas;
        {
            #[cfg(feature = "profiler")]
            demikernel::timer!("finish serialization");
            ordered_sga.reorder_by_size_and_registration_and_finish_serialization(
                allocator,
                self.copying_threshold,
                mbuf_metadatas,
                self.copying_threshold == std::usize::MAX,
            )?;
        }

        let num_required = {
            #[cfg(feature = "profiler")]
            demikernel::timer!("Calculating wqes required for sga");
            self.wqes_required_arena_ordered_sga(&ordered_sga)
        };

        while num_required > curr_available_wqes {
            if self.first_ctrl_seg != ptr::null_mut() {
                if unsafe {
                    custom_mlx5_post_transmissions(
                        self.thread_context.get_context_ptr(),
                        self.first_ctrl_seg,
                    ) != 0
                } {
                    bail!("Failed to post transmissions so far");
                } else {
                    self.first_ctrl_seg = ptr::null_mut();
                }
            }
            self.poll_for_completions()?;
            curr_available_wqes =
                unsafe { custom_mlx5_num_wqes_available(self.thread_context.get_context_ptr()) }
                    as usize;
        }
        let (inline_len, _num_segs, num_wqes_required) = {
            #[cfg(feature = "profiler")]
            demikernel::timer!("Ordered sga and filling in ctrl and ether seg");

            let (inline_len, num_segs) = self.arena_ordered_sga_shape(&ordered_sga);
            let num_octowords =
                unsafe { custom_mlx5_num_octowords(inline_len as _, num_segs as _) };
            let num_wqes_required = num_required as u64;
            tracing::debug!(
                inline_len,
                num_segs,
                num_octowords,
                num_wqes_required,
                "Header info"
            );
            let ctrl_seg = unsafe {
                custom_mlx5_fill_in_hdr_segment(
                    self.thread_context.get_context_ptr(),
                    num_octowords as _,
                    num_wqes_required as _,
                    inline_len as _,
                    num_segs as _,
                    MLX5_ETH_WQE_L3_CSUM as i32 | MLX5_ETH_WQE_L4_CSUM as i32,
                )
            };
            if ctrl_seg.is_null() {
                bail!("Error posting header segment for sga");
            }

            if self.first_ctrl_seg == ptr::null_mut() {
                self.first_ctrl_seg = ctrl_seg;
            }
            (inline_len, num_segs, num_wqes_required)
        };
        let data_len = ordered_sga.data_len();
        let (header_written, entry_idx) = self.inline_hdr_if_necessary(
            conn_id,
            msg_id,
            inline_len,
            data_len,
            ordered_sga.get_hdr(),
        )?;

        // TODO: temporary hack for different code surrounding inlining first entry
        let inlined_obj_hdr = entry_idx == 1;

        let first_zero_copy_seg = ordered_sga.num_copy_entries();
        let allocation_size = ordered_sga.copy_length()
            - (inlined_obj_hdr as usize * ordered_sga.get_hdr().len())
            + (!header_written as usize * cornflakes_libos::utils::TOTAL_HEADER_SIZE);
        let mut dpseg = unsafe {
            custom_mlx5_dpseg_start(self.thread_context.get_context_ptr(), inline_len as _)
        };
        let mut completion =
            unsafe { custom_mlx5_completion_start(self.thread_context.get_context_ptr()) };
        tracing::debug!("Allocation size: {}", allocation_size);
        if allocation_size > 0 {
            #[cfg(feature = "profiler")]
            demikernel::timer!("Copying stuff");

            let mut data_buffer = {
                #[cfg(feature = "profiler")]
                demikernel::timer!("allocating stuff to copy into");
                match self.allocator.allocate_tx_buffer()? {
                    Some(metadata) => metadata,
                    None => {
                        bail!("No tx mempools to allocate outgoing packet");
                    }
                }
            };
            let mut offset = 0;
            if !header_written {
                self.copy_hdr(&mut data_buffer, conn_id, msg_id, data_len)?;
                offset += cornflakes_libos::utils::TOTAL_HEADER_SIZE;
            }
            if !inlined_obj_hdr {
                let data_slice =
                    data_buffer.mutable_slice(offset, offset + ordered_sga.get_hdr().len())?;
                data_slice.copy_from_slice(ordered_sga.get_hdr());
                offset += ordered_sga.get_hdr().len();
            }
            {
                #[cfg(feature = "profiler")]
                demikernel::timer!("Copying copy buffers");
                for seg in ordered_sga.iter().take(first_zero_copy_seg) {
                    tracing::debug!("Writing into slice [{}, {}]", offset, offset + seg.len());
                    let dst = data_buffer.mutable_slice(offset, offset + seg.len())?;
                    unsafe {
                        mlx5_rte_memcpy(dst.as_mut_ptr() as _, seg.addr().as_ptr() as _, seg.len());
                    }
                    offset += seg.len();
                }
            }
            let mut metadata_mbuf = MbufMetadata::from_buf(data_buffer);
            let (curr_dpseg, curr_completion) =
                self.post_mbuf_metadata(&mut metadata_mbuf, dpseg, completion);
            dpseg = curr_dpseg;
            completion = curr_completion;
        }

        // rest are zero-copy segments
        {
            #[cfg(feature = "profiler")]
            demikernel::timer!("process zero copy buffers");
            for mbuf_metadata_option in self
                .mbuf_metadatas
                .iter_mut()
                .skip(first_zero_copy_seg)
                .take(ordered_sga.len() - first_zero_copy_seg)
            {
                match mbuf_metadata_option {
                    Some(ref mut mbuf_metadata) => {
                        mbuf_metadata.increment_refcnt();
                        tracing::debug!(seg =? mbuf_metadata.as_ref().as_ptr(), "Cur posting seg");
                        let (curr_dpseg, curr_completion) = {
                            tracing::debug!(
                                len = mbuf_metadata.data_len(),
                                off = mbuf_metadata.offset(),
                                buf =? mbuf_metadata.as_ref().as_ptr(),
                                "posting dpseg"
                            );
                            unsafe {
                                (
                                    custom_mlx5_add_dpseg(
                                        self.thread_context.get_context_ptr(),
                                        dpseg,
                                        mbuf_metadata.data(),
                                        mbuf_metadata.mempool(),
                                        mbuf_metadata.offset() as _,
                                        mbuf_metadata.data_len() as _,
                                    ),
                                    custom_mlx5_add_completion_info(
                                        self.thread_context.get_context_ptr(),
                                        completion,
                                        mbuf_metadata.data(),
                                        mbuf_metadata.mempool(),
                                    ),
                                )
                            }
                        };
                        dpseg = curr_dpseg;
                        completion = curr_completion;
                        *mbuf_metadata_option = None;
                    }
                    None => {
                        bail!("Segment should exist");
                    }
                }
                *mbuf_metadata_option = None;
            }
        }
        unsafe {
            custom_mlx5_finish_single_transmission(
                self.thread_context.get_context_ptr(),
                num_wqes_required as _,
            );
        }

        if end_batch {
            if !self.first_ctrl_seg.is_null() {
                let _ = self.post_curr_transmissions(Some(self.first_ctrl_seg));
                self.poll_for_completions()?;
                self.first_ctrl_seg = ptr::null_mut();
            }
        }
        Ok(())
    }

    fn push_arena_ordered_sgas_iterator<'sge>(
        &self,
        mut sgas: impl Iterator<Item = Result<(MsgID, ConnID, ArenaOrderedSga<'sge>)>>,
    ) -> Result<()> {
        #[cfg(feature = "profiler")]
        demikernel::timer!("Push arena sgas iterator func");
        let mut curr_available_wqes: usize =
            unsafe { custom_mlx5_num_wqes_available(self.thread_context.get_context_ptr()) }
                as usize;

        let mut first_ctrl_seg: *mut mlx5_wqe_ctrl_seg = ptr::null_mut();
        let mut num_wqes_used_so_far = 0;
        let mut _sent = 0;
        let mut obj = {
            #[cfg(feature = "profiler")]
            demikernel::timer!("iterator next");
            sgas.next()
        };
        while let Some(res) = obj {
            #[cfg(feature = "profiler")]
            demikernel::timer!("Processing per sga loop");
            let (msg_id, conn_id, ordered_sga) = res?;

            let num_required = {
                #[cfg(feature = "profiler")]
                demikernel::timer!("Calculating wqes required for sga");
                self.wqes_required_arena_ordered_sga(&ordered_sga)
            };
            while (num_wqes_used_so_far + num_required) > curr_available_wqes {
                #[cfg(feature = "profiler")]
                demikernel::timer!("Checking for available wqes");
                if first_ctrl_seg != ptr::null_mut() {
                    if unsafe {
                        custom_mlx5_post_transmissions(
                            self.thread_context.get_context_ptr(),
                            first_ctrl_seg,
                        ) != 0
                    } {
                        bail!("Failed to post transmissions so far");
                    } else {
                        first_ctrl_seg = ptr::null_mut();
                        num_wqes_used_so_far = 0;
                    }
                }
                self.poll_for_completions()?;
                curr_available_wqes = unsafe {
                    custom_mlx5_num_wqes_available(self.thread_context.get_context_ptr())
                } as usize;
            }
            let (inline_len, _num_segs, num_wqes_required) = {
                #[cfg(feature = "profiler")]
                demikernel::timer!("Ordered sga and filling in ctrl and ether seg");

                num_wqes_used_so_far += num_required;
                let (inline_len, num_segs) = self.arena_ordered_sga_shape(&ordered_sga);
                let num_octowords =
                    unsafe { custom_mlx5_num_octowords(inline_len as _, num_segs as _) };
                let num_wqes_required = num_required as u64;
                tracing::debug!(
                    inline_len,
                    num_segs,
                    num_octowords,
                    num_wqes_required,
                    "Header info"
                );
                let ctrl_seg = unsafe {
                    custom_mlx5_fill_in_hdr_segment(
                        self.thread_context.get_context_ptr(),
                        num_octowords as _,
                        num_wqes_required as _,
                        inline_len as _,
                        num_segs as _,
                        MLX5_ETH_WQE_L3_CSUM as i32 | MLX5_ETH_WQE_L4_CSUM as i32,
                    )
                };
                if ctrl_seg.is_null() {
                    bail!("Error posting header segment for sga");
                }

                if first_ctrl_seg == ptr::null_mut() {
                    first_ctrl_seg = ctrl_seg;
                }
                (inline_len, num_segs, num_wqes_required)
            };
            let data_len = ordered_sga.data_len();
            let (header_written, entry_idx) = self.inline_hdr_if_necessary(
                conn_id,
                msg_id,
                inline_len,
                data_len,
                ordered_sga.get_hdr(),
            )?;

            // TODO: temporary hack for different code surrounding inlining first entry
            let inlined_obj_hdr = entry_idx == 1;

            let first_zero_copy_seg = ordered_sga.num_copy_entries();
            let allocation_size = ordered_sga.copy_length()
                - (inlined_obj_hdr as usize * ordered_sga.get_hdr().len())
                + (!header_written as usize * cornflakes_libos::utils::TOTAL_HEADER_SIZE);
            let mut dpseg = unsafe {
                custom_mlx5_dpseg_start(self.thread_context.get_context_ptr(), inline_len as _)
            };
            let mut completion =
                unsafe { custom_mlx5_completion_start(self.thread_context.get_context_ptr()) };
            tracing::debug!("Allocation size: {}", allocation_size);
            if allocation_size > 0 {
                #[cfg(feature = "profiler")]
                demikernel::timer!("Copying stuff");

                let mut data_buffer = {
                    #[cfg(feature = "profiler")]
                    demikernel::timer!("allocating stuff to copy into");
                    match self.allocator.allocate_tx_buffer()? {
                        Some(buf) => buf,
                        None => {
                            bail!("No tx mempools to allocate outgoing packet");
                        }
                    }
                };
                let mut offset = 0;
                if !header_written {
                    self.copy_hdr(&mut data_buffer, conn_id, msg_id, data_len)?;
                    offset += cornflakes_libos::utils::TOTAL_HEADER_SIZE;
                }
                if !inlined_obj_hdr {
                    let data_slice =
                        data_buffer.mutable_slice(offset, offset + ordered_sga.get_hdr().len())?;
                    data_slice.copy_from_slice(ordered_sga.get_hdr());
                    offset += ordered_sga.get_hdr().len();
                }
                for seg in ordered_sga.iter().take(first_zero_copy_seg) {
                    tracing::debug!("Writing into slice [{}, {}]", offset, offset + seg.len());
                    let dst = data_buffer.mutable_slice(offset, offset + seg.len())?;
                    unsafe {
                        mlx5_rte_memcpy(dst.as_mut_ptr() as _, seg.addr().as_ptr() as _, seg.len());
                    }
                    offset += seg.len();
                }

                let mut metadata_mbuf = MbufMetadata::from_buf(data_buffer);
                let (curr_dpseg, curr_completion) =
                    self.post_mbuf_metadata(&mut metadata_mbuf, dpseg, completion);
                dpseg = curr_dpseg;
                completion = curr_completion;
            }

            // rest are zero-copy segments
            for seg in ordered_sga.iter().skip(first_zero_copy_seg) {
                #[cfg(feature = "profiler")]
                demikernel::timer!("work per zero copy sga");
                let curr_seg = seg.addr();
                tracing::debug!(seg =? seg.addr().as_ptr(), "Cur posting seg");

                let mut mbuf_metadata = match self.allocator.recover_buffer(curr_seg)? {
                    Some(x) => x,
                    None => {
                        bail!("Failed to recover mbuf metadata for seg{:?}", curr_seg);
                    }
                };
                let (curr_dpseg, curr_completion) =
                    self.post_mbuf_metadata(&mut mbuf_metadata, dpseg, completion);

                dpseg = curr_dpseg;
                completion = curr_completion;
            }

            unsafe {
                custom_mlx5_finish_single_transmission(
                    self.thread_context.get_context_ptr(),
                    num_wqes_required as _,
                );
            }
            _sent += 1;
            obj = {
                #[cfg(feature = "profiler")]
                demikernel::timer!("iterator next");
                sgas.next()
            };
        }

        if !first_ctrl_seg.is_null() {
            let _ = self.post_curr_transmissions(Some(first_ctrl_seg));
            self.poll_for_completions()?;
        }

        Ok(())
    }

    fn push_ordered_sgas_iterator<'sge>(
        &self,
        mut sgas: impl Iterator<Item = Result<(MsgID, ConnID, OrderedSga<'sge>)>>,
    ) -> Result<()> {
        #[cfg(feature = "profiler")]
        demikernel::timer!("Push sgas iterator func");
        let mut curr_available_wqes: usize =
            unsafe { custom_mlx5_num_wqes_available(self.thread_context.get_context_ptr()) }
                as usize;

        let mut first_ctrl_seg: *mut mlx5_wqe_ctrl_seg = ptr::null_mut();
        let mut num_wqes_used_so_far = 0;
        let mut _sent = 0;
        let mut obj = {
            #[cfg(feature = "profiler")]
            demikernel::timer!("iterator next");
            sgas.next()
        };
        while let Some(res) = obj {
            #[cfg(feature = "profiler")]
            demikernel::timer!("Processing per sga loop");
            let (msg_id, conn_id, ordered_sga) = res?;

            let num_required = {
                #[cfg(feature = "profiler")]
                demikernel::timer!("Calculating wqes required for sga");
                self.wqes_required_ordered_sga(&ordered_sga)
            };
            while (num_wqes_used_so_far + num_required) > curr_available_wqes {
                #[cfg(feature = "profiler")]
                demikernel::timer!("Checking for available wqes");
                if first_ctrl_seg != ptr::null_mut() {
                    if unsafe {
                        custom_mlx5_post_transmissions(
                            self.thread_context.get_context_ptr(),
                            first_ctrl_seg,
                        ) != 0
                    } {
                        bail!("Failed to post transmissions so far");
                    } else {
                        first_ctrl_seg = ptr::null_mut();
                        num_wqes_used_so_far = 0;
                    }
                }
                self.poll_for_completions()?;
                curr_available_wqes = unsafe {
                    custom_mlx5_num_wqes_available(self.thread_context.get_context_ptr())
                } as usize;
            }
            let (inline_len, _num_segs, num_wqes_required) = {
                #[cfg(feature = "profiler")]
                demikernel::timer!("Ordered sga and filling in ctrl and ether seg");

                num_wqes_used_so_far += num_required;
                let (inline_len, num_segs) = self.ordered_sga_shape(&ordered_sga);
                let num_octowords =
                    unsafe { custom_mlx5_num_octowords(inline_len as _, num_segs as _) };
                let num_wqes_required = num_required as u64;
                tracing::debug!(
                    inline_len,
                    num_segs,
                    num_octowords,
                    num_wqes_required,
                    "Header info"
                );
                let ctrl_seg = unsafe {
                    custom_mlx5_fill_in_hdr_segment(
                        self.thread_context.get_context_ptr(),
                        num_octowords as _,
                        num_wqes_required as _,
                        inline_len as _,
                        num_segs as _,
                        MLX5_ETH_WQE_L3_CSUM as i32 | MLX5_ETH_WQE_L4_CSUM as i32,
                    )
                };
                if ctrl_seg.is_null() {
                    bail!("Error posting header segment for sga");
                }

                if first_ctrl_seg == ptr::null_mut() {
                    first_ctrl_seg = ctrl_seg;
                }
                (inline_len, num_segs, num_wqes_required)
            };
            let data_len = ordered_sga.sga().data_len() + ordered_sga.get_hdr().len();
            let (header_written, entry_idx) = self.inline_hdr_if_necessary(
                conn_id,
                msg_id,
                inline_len,
                data_len,
                ordered_sga.get_hdr(),
            )?;

            // TODO: temporary hack for different code surrounding inlining first entry
            let inlined_obj_hdr = entry_idx == 1;

            let first_zero_copy_seg = ordered_sga.num_copy_entries();
            let allocation_size = ordered_sga.copy_length()
                - (inlined_obj_hdr as usize * ordered_sga.get_hdr().len())
                + (!header_written as usize * cornflakes_libos::utils::TOTAL_HEADER_SIZE);
            let mut dpseg = unsafe {
                custom_mlx5_dpseg_start(self.thread_context.get_context_ptr(), inline_len as _)
            };
            let mut completion =
                unsafe { custom_mlx5_completion_start(self.thread_context.get_context_ptr()) };
            tracing::debug!("Allocation size: {}", allocation_size);
            if allocation_size > 0 {
                #[cfg(feature = "profiler")]
                demikernel::timer!("Copying stuff");

                let mut data_buffer = {
                    #[cfg(feature = "profiler")]
                    demikernel::timer!("allocating stuff to copy into");
                    match self.allocator.allocate_tx_buffer()? {
                        Some(buf) => buf,
                        None => {
                            bail!("No tx mempools to allocate outgoing packet");
                        }
                    }
                };
                let mut offset = 0;
                if !header_written {
                    self.copy_hdr(&mut data_buffer, conn_id, msg_id, data_len)?;
                    offset += cornflakes_libos::utils::TOTAL_HEADER_SIZE;
                }
                if !inlined_obj_hdr {
                    let data_slice =
                        data_buffer.mutable_slice(offset, offset + ordered_sga.get_hdr().len())?;
                    data_slice.copy_from_slice(ordered_sga.get_hdr());
                    offset += ordered_sga.get_hdr().len();
                }
                for seg in ordered_sga.sga().iter().take(first_zero_copy_seg) {
                    tracing::debug!("Writing into slice [{}, {}]", offset, offset + seg.len());
                    let dst = data_buffer.mutable_slice(offset, offset + seg.len())?;
                    unsafe {
                        mlx5_rte_memcpy(dst.as_mut_ptr() as _, seg.addr().as_ptr() as _, seg.len());
                    }
                    offset += seg.len();
                }

                let mut metadata_mbuf = MbufMetadata::from_buf(data_buffer);
                let (curr_dpseg, curr_completion) =
                    self.post_mbuf_metadata(&mut metadata_mbuf, dpseg, completion);
                dpseg = curr_dpseg;
                completion = curr_completion;
            }

            // rest are zero-copy segments
            for seg in ordered_sga
                .sga()
                .iter()
                .take(ordered_sga.len())
                .skip(first_zero_copy_seg)
            {
                #[cfg(feature = "profiler")]
                demikernel::timer!("work per zero copy sga");
                let curr_seg = seg.addr();
                tracing::debug!(seg =? seg.addr().as_ptr(), "Cur posting seg");

                let mut mbuf_metadata = match self.allocator.recover_buffer(curr_seg)? {
                    Some(x) => x,
                    None => {
                        bail!("Failed to recover mbuf metadata for seg{:?}", curr_seg);
                    }
                };
                let (curr_dpseg, curr_completion) =
                    self.post_mbuf_metadata(&mut mbuf_metadata, dpseg, completion);

                dpseg = curr_dpseg;
                completion = curr_completion;
            }

            unsafe {
                custom_mlx5_finish_single_transmission(
                    self.thread_context.get_context_ptr(),
                    num_wqes_required as _,
                );
            }
            _sent += 1;
            obj = {
                #[cfg(feature = "profiler")]
                demikernel::timer!("iterator next");
                sgas.next()
            };
        }

        if !first_ctrl_seg.is_null() {
            let _ = self.post_curr_transmissions(Some(first_ctrl_seg));
            self.poll_for_completions()?;
        }

        Ok(())
    }

    fn push_sgas(&mut self, sgas: &[(MsgID, ConnID, Sga)]) -> Result<()> {
        let mut first_ctrl_seg: Option<*mut mlx5_wqe_ctrl_seg> = None;
        let mut sga_idx = 0;
        tracing::debug!(len = sgas.len(), "Pushing sgas");
        while sga_idx < sgas.len() {
            let (msg_id, conn_id, sga) = &sgas[sga_idx];
            self.insert_into_outgoing_map(*msg_id, *conn_id);
            let (inline_len, num_segs) = self.calculate_shape(&sga)?;
            let num_octowords =
                unsafe { custom_mlx5_num_octowords(inline_len as _, num_segs as _) };
            let num_wqes_required = unsafe { custom_mlx5_num_wqes_required(num_octowords as _) };

            tracing::debug!(
                id = *msg_id,
                inline_len = inline_len,
                num_segs = num_segs,
                num_octowords = num_octowords,
                num_wqes_required = num_wqes_required,
                "Calculated shape for sga transmission"
            );
            // enough descriptors to transmit this sga?
            if unsafe {
                custom_mlx5_tx_descriptors_available(
                    self.thread_context.get_context_ptr(),
                    num_wqes_required,
                ) != 1
            } {
                first_ctrl_seg = self.post_curr_transmissions(first_ctrl_seg)?;
                self.poll_for_completions()?;
            } else {
                let ctrl_seg = self.post_sga(
                    sga,
                    *conn_id,
                    *msg_id,
                    num_octowords,
                    num_wqes_required,
                    inline_len,
                    num_segs,
                )?;
                if first_ctrl_seg == None {
                    first_ctrl_seg = Some(ctrl_seg);
                }

                sga_idx += 1;
            }
        }
        tracing::debug!("Posting transmissions with ctrl seg {:?}", first_ctrl_seg);
        let _ = self.post_curr_transmissions(first_ctrl_seg)?;
        self.poll_for_completions()?;

        Ok(())
    }

    fn pop_with_durations(&mut self) -> Result<Vec<(ReceivedPkt<Self>, Duration)>>
    where
        Self: Sized,
    {
        let received = unsafe {
            custom_mlx5_gather_rx(
                self.thread_context.get_context_ptr(),
                self.recv_mbufs.as_recv_mbuf_info_array_ptr(),
                RECEIVE_BURST_SIZE as _,
            )
        };
        let mut ret: Vec<(ReceivedPkt<Self>, Duration)> = Vec::with_capacity(RECEIVE_BURST_SIZE);
        for i in 0..received as usize {
            if let Some(received_pkt) = self
                .check_received_pkt(i)
                .wrap_err("Error receiving packets")?
            {
                let dur = match self
                    .outgoing_window
                    .remove(&(received_pkt.msg_id(), received_pkt.conn_id()))
                {
                    Some(start_time) => start_time.elapsed(),
                    None => {
                        // free the rest of the packets
                        for _ in (i + 1)..received as usize {
                            let recv_info = self.recv_mbufs.get(i);
                            unsafe {
                                custom_mlx5_refcnt_update_or_free(
                                    (*recv_info).mempool,
                                    (*recv_info).buf_addr,
                                    (*recv_info).ref_count_index as _,
                                    -1i8,
                                );
                            }
                            self.recv_mbufs.clear(i);
                        }
                        bail!(
                            "Cannot find msg id {} and conn id {} in outgoing window",
                            received_pkt.msg_id(),
                            received_pkt.conn_id()
                        );
                    }
                };
                ret.push((received_pkt, dur));
            } else {
                // free the mbuf
                let recv_info = self.recv_mbufs.get(i);
                unsafe {
                    custom_mlx5_refcnt_update_or_free(
                        (*recv_info).mempool,
                        (*recv_info).buf_addr,
                        (*recv_info).ref_count_index as _,
                        -1i8,
                    );
                }
                self.recv_mbufs.clear(i);
            }
        }
        Ok(ret)
    }

    // TODO: if the packet is a no-op, simply respond to the caller
    fn pop(&mut self) -> Result<Vec<ReceivedPkt<Self>>>
    where
        Self: Sized,
    {
        let received = unsafe {
            custom_mlx5_gather_rx(
                self.thread_context.get_context_ptr(),
                self.recv_mbufs.as_recv_mbuf_info_array_ptr(),
                RECEIVE_BURST_SIZE as _,
            )
        };
        let mut ret: Vec<ReceivedPkt<Self>> = Vec::with_capacity(RECEIVE_BURST_SIZE);
        for i in 0..received as usize {
            if let Some(received_pkt) = self
                .check_received_pkt(i)
                .wrap_err("Error receiving packets")?
            {
                // if is NO-OP, just return a NO-OP to the caller
                if received_pkt.is_noop() {
                    tracing::debug!("Processing NO-OP");
                    self.process_warmup_noop(received_pkt)?;
                } else {
                    ret.push(received_pkt);
                }
            } else {
                // free the mbuf
                let recv_info = self.recv_mbufs.get(i);
                unsafe {
                    custom_mlx5_refcnt_update_or_free(
                        (*recv_info).mempool,
                        (*recv_info).buf_addr,
                        (*recv_info).ref_count_index as _,
                        -1i8,
                    );
                }
                self.recv_mbufs.clear(i);
            }
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
        #[cfg(feature = "profiler")]
        demikernel::timer!("Is registered function");
        self.allocator.is_registered(buf)
    }

    fn allocate(&mut self, size: usize) -> Result<Option<Self::DatapathBuffer>> {
        self.allocator.allocate_buffer(size)
    }

    fn allocate_tx_buffer(&mut self) -> Result<(Option<Self::DatapathBuffer>, usize)> {
        Ok((
            self.allocator.allocate_tx_buffer()?,
            <Self as Datapath>::max_packet_size(),
        ))
    }

    fn get_metadata(&self, buf: Self::DatapathBuffer) -> Result<Option<Self::DatapathMetadata>> {
        Ok(Some(MbufMetadata::from_buf(buf)))
    }

    #[inline]
    fn recover_metadata(&self, buf: &[u8]) -> Result<Option<Self::DatapathMetadata>> {
        self.allocator.recover_buffer(buf)
    }

    fn allocate_fallback_mempools(&mut self, mempool_ids: &mut Vec<MempoolID>) -> Result<()> {
        for size in self.allocator.get_cur_sizes().iter() {
            tracing::info!("Allocating one more mempool with size {}", *size);
            mempool_ids.append(&mut self.add_memory_pool_with_size(*size)?);
        }
        Ok(())
    }

    fn add_memory_pool(&mut self, size: usize, min_elts: usize) -> Result<Vec<MempoolID>> {
        // use 2MB pages for data, 2MB pages for metadata (?)
        //println!("In add memory pool: size {}, min_elts {}", size, min_elts);
        let actual_size = cornflakes_libos::allocator::align_to_pow2(size);
        let mempool_params = sizes::MempoolAllocationParams::new(min_elts, PGSIZE_2MB, actual_size)
            .wrap_err("Incorrect mempool allocation params")?;
        //println!("About to call data mempool new  past params check");
        tracing::info!(mempool_params = ?mempool_params, "Adding mempool");
        let data_mempool = DataMempool::new(&mempool_params, &self.thread_context, true)?;
        let id = self
            .allocator
            .add_mempool(mempool_params.get_item_len(), data_mempool)?;

        Ok(vec![id])
    }

    #[inline]
    fn has_mempool(&self, size: usize) -> bool {
        return self.allocator.has_mempool(size);
    }

    fn register_mempool(&mut self, id: MempoolID) -> Result<()> {
        self.allocator
            .register(id, self.thread_context.get_context_ptr())
    }

    fn unregister_mempool(&mut self, id: MempoolID) -> Result<()> {
        self.allocator.unregister(id)
    }

    fn header_size(&self) -> usize {
        cornflakes_libos::utils::TOTAL_HEADER_SIZE
    }

    fn timer_hz(&self) -> u64 {
        unsafe { ns_to_cycles(1_000_000_000) }
    }

    fn cycles_to_ns(&self, t: u64) -> u64 {
        unsafe { cycles_to_ns(t) }
    }

    fn current_cycles(&self) -> u64 {
        unsafe { current_cycles() }
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

    fn set_inline_mode(&mut self, inline_mode: InlineMode) {
        self.inline_mode = inline_mode;
    }

    fn batch_size() -> usize {
        RECEIVE_BURST_SIZE
    }

    fn max_scatter_gather_entries() -> usize {
        33
    }
}
