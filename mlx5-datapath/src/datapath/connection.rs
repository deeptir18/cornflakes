use super::{
    super::{access, check_ok, mbuf_slice, mlx5_bindings::*},
    allocator::DataMempool,
    check, sizes,
};
use cornflakes_libos::{
    allocator::{MemoryPoolAllocator, MempoolID},
    datapath::{Datapath, InlineMode, MetadataOps, ReceivedPkt},
    mem::{PGSIZE_2MB, PGSIZE_4KB},
    serialize::Serializable,
    utils::AddressInfo,
    ConnID, MsgID, RcSga, RcSge, Sga, USING_REF_COUNTING,
};

use byteorder::{ByteOrder, NetworkEndian};
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

#[cfg(feature = "profiler")]
use perftools;

const MAX_CONCURRENT_CONNECTIONS: usize = 128;
const COMPLETION_BUDGET: usize = 32;
const RECEIVE_BURST_SIZE: usize = 32;

pub fn hello_world() {
    tracing::info!("Hello from this crate");
}

#[derive(PartialEq, Eq)]
pub struct Mlx5Buffer {
    /// Underlying data pointer
    data: *mut ::std::os::raw::c_void,
    /// Pointer back to the data and metadata pool pair
    mempool: *mut registered_mempool,
    /// Data len,
    data_len: usize,
}

impl Mlx5Buffer {
    pub fn new(
        data: *mut ::std::os::raw::c_void,
        mempool: *mut registered_mempool,
        data_len: usize,
    ) -> Self {
        Mlx5Buffer {
            data: data,
            mempool: mempool,
            data_len: data_len,
        }
    }

    pub fn get_inner(self) -> (*mut ::std::os::raw::c_void, *mut registered_mempool, usize) {
        (self.data, self.mempool, self.data_len)
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
        let item_len = unsafe { access!(get_data_mempool(self.mempool), item_len, usize) };
        if start > item_len || end > item_len {
            bail!("Invalid bounnds for buf of len {}", item_len);
        }
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

impl Write for Mlx5Buffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // only write the maximum amount
        let data_mempool = unsafe { get_data_mempool(self.mempool) };
        let written = std::cmp::min(unsafe { access!(data_mempool, item_len, usize) }, buf.len());
        let mut mut_slice =
            unsafe { std::slice::from_raw_parts_mut(self.data as *mut u8, self.data_len) };
        let written = mut_slice.write(&buf[0..written])?;
        self.data_len = written;
        Ok(written)
    }

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
    /// Pointer to allocated mbuf metadata.
    pub mbuf: *mut custom_mlx5_mbuf,
    /// Application data offset
    pub offset: usize,
    /// Application data length
    pub len: usize,
}

impl MbufMetadata {
    pub fn from_buf(mlx5_buffer: Mlx5Buffer) -> Result<Option<Self>> {
        let (buf, registered_mempool, data_len) = mlx5_buffer.get_inner();
        let metadata_buf = unsafe { alloc_metadata(registered_mempool, buf) };
        if metadata_buf.is_null() {
            // drop the data buffer
            unsafe {
                custom_mlx5_mempool_free(buf, get_data_mempool(registered_mempool));
            }
            return Ok(None);
        }

        ensure!(!metadata_buf.is_null(), "Allocated metadata buffer is null");
        unsafe {
            init_metadata(
                metadata_buf,
                buf,
                get_data_mempool(registered_mempool),
                get_metadata_mempool(registered_mempool),
                data_len,
                0,
            );
        }
        Ok(Some(MbufMetadata::new(metadata_buf, 0, Some(data_len))?))
    }

    pub fn increment_refcnt(&mut self) {
        tracing::debug!(mbuf =? self.mbuf, "Incrementing ref count on mbuf");
        unsafe {
            custom_mlx5_mbuf_refcnt_update_or_free(self.mbuf, 1);
        }
    }

    /// Initializes metadata object from existing metadata mbuf in c.
    /// Args:
    /// @mbuf: mbuf structure that contains metadata
    /// @data_offset: Application data offset into this buffer.
    /// @data_len: Optional application data length into the buffer.
    pub fn new(
        mbuf: *mut custom_mlx5_mbuf,
        data_offset: usize,
        data_len: Option<usize>,
    ) -> Result<Self> {
        tracing::debug!("Returning mbuf {:?}", mbuf);
        ensure!(
            data_offset <= unsafe { access!(mbuf, data_buf_len, usize) },
            format!(
                "Data offset too large: off: {}, data_buf_len: {}",
                data_offset,
                unsafe { access!(mbuf, data_buf_len, usize) }
            ),
        );
        unsafe {
            custom_mlx5_mbuf_refcnt_update_or_free(mbuf, 1);
        }
        let len = match data_len {
            Some(x) => {
                ensure!(
                    x <= unsafe { access!(mbuf, data_buf_len, usize) - data_offset },
                    "Data len to large"
                );
                x
            }
            None => unsafe { access!(mbuf, data_len, usize) - data_offset },
        };
        Ok(MbufMetadata {
            mbuf: mbuf,
            offset: data_offset,
            len: len,
        })
    }

    pub fn mbuf(&self) -> *mut custom_mlx5_mbuf {
        self.mbuf
    }
}

impl MetadataOps for MbufMetadata {
    fn offset(&self) -> usize {
        self.offset
    }

    fn data_len(&self) -> usize {
        self.len
    }

    fn set_data_len_and_offset(&mut self, len: usize, offset: usize) -> Result<()> {
        ensure!(
            offset <= unsafe { access!(self.mbuf, data_buf_len, usize) },
            "Offset too large"
        );
        ensure!(
            len <= unsafe { access!(self.mbuf, data_buf_len, usize) - offset },
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
            mbuf: ptr::null_mut(),
            offset: 0,
            len: 0,
        }
    }
}

impl Drop for MbufMetadata {
    fn drop(&mut self) {
        unsafe {
            if USING_REF_COUNTING {
                tracing::debug!(mbuf =? self.mbuf, "Dropping");
                custom_mlx5_mbuf_refcnt_update_or_free(self.mbuf, -1);
            } else {
                custom_mlx5_mbuf_free(self.mbuf);
            }
        }
    }
}

impl Clone for MbufMetadata {
    fn clone(&self) -> MbufMetadata {
        unsafe {
            if USING_REF_COUNTING {
                tracing::debug!(mbuf = ?self.mbuf, "Incrementing ref count");
                custom_mlx5_mbuf_refcnt_update_or_free(self.mbuf, 1);
            }
        }
        MbufMetadata {
            mbuf: self.mbuf,
            offset: self.offset,
            len: self.len,
        }
    }
}

impl std::fmt::Debug for MbufMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Mbuf addr: {:?}, off: {}", self.mbuf, self.offset)
    }
}

impl AsRef<[u8]> for MbufMetadata {
    fn as_ref(&self) -> &[u8] {
        unsafe { mbuf_slice!(self.mbuf, self.offset, self.len) }
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
                Box::from_raw(thread_ptr);
            }
            unsafe {
                Box::from_raw(global_context_ptr);
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
    /// Inline mode,
    inline_mode: InlineMode,
    /// Maximum data that can be inlined
    max_inline_size: usize,
    /// Array of mbuf pointers used to receive packets
    recv_mbufs: [*mut custom_mlx5_mbuf; RECEIVE_BURST_SIZE],
}

impl Mlx5Connection {
    fn insert_into_outgoing_map(&mut self, msg_id: MsgID, conn_id: ConnID) {
        if self.mode == AppMode::Client {
            if !self.outgoing_window.contains_key(&(msg_id, conn_id)) {
                self.outgoing_window
                    .insert((msg_id, conn_id), Instant::now());
            }
        }
    }

    fn check_received_pkt(&mut self, pkt_index: usize) -> Result<Option<ReceivedPkt<Self>>> {
        let recv_mbuf = self.recv_mbufs[pkt_index];
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
            NetworkEndian::read_u32(&slice[0..4])
        };

        tracing::debug!("Received packet with length of {}", data_len);
        let datapath_metadata = MbufMetadata::new(
            recv_mbuf,
            cornflakes_libos::utils::TOTAL_HEADER_SIZE,
            Some(data_len),
        )?;

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
        &mut self,
        conn_id: ConnID,
        msg_id: MsgID,
        inline_len: usize,
        data_len: usize,
    ) -> Result<()> {
        // inline ethernet header
        let hdr_bytes: &mut [u8; cornflakes_libos::utils::TOTAL_UDP_HEADER_SIZE] =
            match &mut self.active_connections[conn_id as usize] {
                Some((_, hdr_bytes_vec)) => hdr_bytes_vec,
                None => {
                    bail!("Could not find address for connID");
                }
            };

        let eth_hdr = hdr_bytes[0..cornflakes_libos::utils::ETHERNET2_HEADER2_SIZE].as_mut_ptr()
            as *mut eth_hdr;
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
            .as_mut_ptr() as *mut ip_hdr;
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
            .as_mut_ptr() as *mut udp_hdr;
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

    /// Rings doorbells for current transmissions
    /// Then checks for completions.
    fn post_curr_transmissions(
        &mut self,
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

        return Ok(ret);
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

        let mut entry_idx = 0;
        let mut header_written = false;
        let data_len = rc_sga.data_len();
        match self.inline_mode {
            InlineMode::Nothing => {}
            InlineMode::PacketHeader => {
                self.inline_hdr(conn_id, msg_id, inline_len, data_len)?;
                header_written = true;
            }
            InlineMode::FirstEntry => {
                self.inline_hdr(conn_id, msg_id, inline_len, data_len)?;
                header_written = true;
                if (cornflakes_libos::utils::TOTAL_HEADER_SIZE + rc_sga.get(0).len())
                    <= self.max_inline_size
                {
                    // inline whatever is in the first scatter-gather entry
                    let first_entry = rc_sga.get(0).addr();
                    unsafe {
                        custom_mlx5_copy_inline_data(
                            self.thread_context.get_context_ptr(),
                            cornflakes_libos::utils::TOTAL_HEADER_SIZE as _,
                            first_entry.as_ptr() as _,
                            first_entry.len() as _,
                            inline_len as _,
                        );
                    }
                    entry_idx += 1;
                }
            }
        }

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
                    let mut data_buffer = match self
                        .allocator
                        .allocate_buffer(cornflakes_libos::utils::TOTAL_HEADER_SIZE)?
                    {
                        Some(buf) => buf,
                        None => {
                            bail!("No tx mempools to allocate outgoing packet");
                        }
                    };
                    // copy in the header into a buffer
                    self.copy_hdr(&mut data_buffer, conn_id, msg_id, data_len)?;
                    // attach this data buffer to a metadata buffer
                    let mut metadata_mbuf = match MbufMetadata::from_buf(data_buffer)? {
                        Some(m) => m,
                        None => {
                            bail!(
                            "Could not allocate corresponding metadata for allocated data buffer"
                        );
                        }
                    };
                    metadata_mbuf.increment_refcnt();

                    // post this data buffer to the ring buffer
                    unsafe {
                        curr_data_seg = custom_mlx5_add_dpseg(
                            self.thread_context.get_context_ptr(),
                            curr_data_seg,
                            metadata_mbuf.mbuf(),
                            metadata_mbuf.offset() as _,
                            metadata_mbuf.data_len() as _,
                        );

                        curr_completion = custom_mlx5_add_completion_info(
                            self.thread_context.get_context_ptr(),
                            curr_completion,
                            metadata_mbuf.mbuf(),
                        );
                    }

                    header_written = true;
                }
                let mbuf_metadata = rc_sga.get_mut(entry_idx).inner_datapath_pkt_mut().unwrap();
                mbuf_metadata.increment_refcnt();

                curr_data_seg = unsafe {
                    custom_mlx5_add_dpseg(
                        self.thread_context.get_context_ptr(),
                        curr_data_seg,
                        mbuf_metadata.mbuf(),
                        mbuf_metadata.offset() as _,
                        mbuf_metadata.data_len() as _,
                    )
                };

                curr_completion = unsafe {
                    custom_mlx5_add_completion_info(
                        self.thread_context.get_context_ptr(),
                        curr_completion,
                        mbuf_metadata.mbuf(),
                    )
                };
                entry_idx += 1;
            } else {
                // iterate forward, figure out size of further zero-copy segments
                let mut curr_idx = entry_idx;
                let mut mbuf_length = match !header_written && curr_idx == 0 {
                    true => cornflakes_libos::utils::TOTAL_HEADER_SIZE,
                    false => 0,
                };
                while !self.zero_copy_rc_seg(rc_sga.get(curr_idx)) && curr_idx < rc_sga.len() {
                    mbuf_length += rc_sga.get(curr_idx).len();
                    curr_idx += 1;
                }

                // allocate an mbuf that can fit this amount of data and copy data to it
                let mut data_buffer = match self.allocator.allocate_buffer(mbuf_length)? {
                    Some(buf) => buf,
                    None => {
                        bail!("No tx mempools to allocate outgoing packet");
                    }
                };
                let mut write_offset = match !header_written && curr_idx == 0 {
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
                let mut metadata_mbuf = match MbufMetadata::from_buf(data_buffer)? {
                    Some(m) => m,
                    None => {
                        bail!(
                            "Could not allocate corresponding metadata for allocated data buffer"
                        );
                    }
                };
                // increment refcount as MbufMetadata may be dropped before data is sent
                metadata_mbuf.increment_refcnt();

                // post this data buffer to the ring buffer
                unsafe {
                    curr_data_seg = custom_mlx5_add_dpseg(
                        self.thread_context.get_context_ptr(),
                        curr_data_seg,
                        metadata_mbuf.mbuf(),
                        metadata_mbuf.offset() as _,
                        metadata_mbuf.data_len() as _,
                    );

                    curr_completion = custom_mlx5_add_completion_info(
                        self.thread_context.get_context_ptr(),
                        curr_completion,
                        metadata_mbuf.mbuf(),
                    );
                }

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

        let mut entry_idx = 0;
        let mut header_written = false;
        let data_len = sga.data_len();
        match self.inline_mode {
            InlineMode::Nothing => {}
            InlineMode::PacketHeader => {
                self.inline_hdr(conn_id, msg_id, inline_len, data_len)?;
                header_written = true;
            }
            InlineMode::FirstEntry => {
                self.inline_hdr(conn_id, msg_id, inline_len, data_len)?;
                header_written = true;
                if (cornflakes_libos::utils::TOTAL_HEADER_SIZE + sga.get(0).len())
                    <= self.max_inline_size
                {
                    // inline whatever is in the first scatter-gather entry
                    let first_entry = sga.get(0).addr();
                    unsafe {
                        custom_mlx5_copy_inline_data(
                            self.thread_context.get_context_ptr(),
                            cornflakes_libos::utils::TOTAL_HEADER_SIZE as _,
                            first_entry.as_ptr() as _,
                            first_entry.len() as _,
                            inline_len as _,
                        );
                    }
                    entry_idx += 1;
                }
            }
        }

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
                    let mut data_buffer = match self
                        .allocator
                        .allocate_buffer(cornflakes_libos::utils::TOTAL_HEADER_SIZE)?
                    {
                        Some(buf) => buf,
                        None => {
                            bail!("No tx mempools to allocate outgoing packet");
                        }
                    };
                    // copy in the header into a buffer
                    self.copy_hdr(&mut data_buffer, conn_id, msg_id, data_len)?;
                    // attach this data buffer to a metadata buffer
                    let mut metadata_mbuf = match MbufMetadata::from_buf(data_buffer)? {
                        Some(m) => m,
                        None => {
                            bail!(
                            "Could not allocate corresponding metadata for allocated data buffer"
                        );
                        }
                    };
                    metadata_mbuf.increment_refcnt();

                    // post this data buffer to the ring buffer
                    unsafe {
                        curr_data_seg = custom_mlx5_add_dpseg(
                            self.thread_context.get_context_ptr(),
                            curr_data_seg,
                            metadata_mbuf.mbuf(),
                            metadata_mbuf.offset() as _,
                            metadata_mbuf.data_len() as _,
                        );

                        curr_completion = custom_mlx5_add_completion_info(
                            self.thread_context.get_context_ptr(),
                            curr_completion,
                            metadata_mbuf.mbuf(),
                        );
                    }

                    header_written = true;
                }

                let curr_seg = sga.get(entry_idx).addr();
                let mut mbuf_metadata = self.allocator.recover_buffer(curr_seg)?.unwrap();
                mbuf_metadata.increment_refcnt();
                curr_data_seg = unsafe {
                    custom_mlx5_add_dpseg(
                        self.thread_context.get_context_ptr(),
                        curr_data_seg,
                        mbuf_metadata.mbuf(),
                        mbuf_metadata.offset() as _,
                        mbuf_metadata.data_len() as _,
                    )
                };

                curr_completion = unsafe {
                    custom_mlx5_add_completion_info(
                        self.thread_context.get_context_ptr(),
                        curr_completion,
                        mbuf_metadata.mbuf(),
                    )
                };
                entry_idx += 1;
            } else {
                // iterate forward, figure out size of further zero-copy segments
                let mut curr_idx = entry_idx;
                let mut mbuf_length = match !header_written && curr_idx == 0 {
                    true => cornflakes_libos::utils::TOTAL_HEADER_SIZE,
                    false => 0,
                };
                while !self.zero_copy_seg(sga.get(curr_idx).addr()) && curr_idx < sga.len() {
                    let curr_seg = sga.get(curr_idx).addr();
                    mbuf_length += curr_seg.len();
                    curr_idx += 1;
                }

                // allocate an mbuf that can fit this amount of data and copy data to it
                let mut data_buffer = match self.allocator.allocate_buffer(mbuf_length)? {
                    Some(buf) => buf,
                    None => {
                        bail!("No tx mempools to allocate outgoing packet");
                    }
                };
                let mut write_offset = match !header_written && curr_idx == 0 {
                    true => {
                        // copy in the header into a buffer
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
                let mut metadata_mbuf = match MbufMetadata::from_buf(data_buffer)? {
                    Some(m) => m,
                    None => {
                        bail!(
                            "Could not allocate corresponding metadata for allocated data buffer"
                        );
                    }
                };
                metadata_mbuf.increment_refcnt();

                // post this data buffer to the ring buffer
                unsafe {
                    curr_data_seg = custom_mlx5_add_dpseg(
                        self.thread_context.get_context_ptr(),
                        curr_data_seg,
                        metadata_mbuf.mbuf(),
                        metadata_mbuf.offset() as _,
                        metadata_mbuf.data_len() as _,
                    );

                    curr_completion = custom_mlx5_add_completion_info(
                        self.thread_context.get_context_ptr(),
                        curr_completion,
                        metadata_mbuf.mbuf(),
                    );
                }

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

    fn calculate_inline_size(&self, first_entry_size: usize) -> (usize, usize, usize) {
        let mut sga_idx = 0;
        let (inline_size, num_segs) = match self.inline_mode {
            InlineMode::Nothing => (0, 1),
            InlineMode::PacketHeader => (cornflakes_libos::utils::TOTAL_HEADER_SIZE, 0),
            InlineMode::FirstEntry => {
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
        let (inline_size, mut sga_idx, mut num_segs) =
            self.calculate_inline_size(rc_sga.get(0).len());
        while sga_idx < rc_sga.len() {
            if self.zero_copy_rc_seg(rc_sga.get(sga_idx)) {
                num_segs += 1;
                sga_idx += 1;
            } else {
                let mut cur_idx = sga_idx;
                while !self.zero_copy_rc_seg(rc_sga.get(cur_idx)) {
                    cur_idx += 1;
                }
                if !(sga_idx == 0 && num_segs == 1) {
                    // past first entry (which can be copied with object header)
                    num_segs += 1;
                }
                sga_idx = cur_idx;
            }
        }
        Ok((inline_size, num_segs))
    }

    /// Given a scatter-gather array,
    /// calculates inline size and number of data segments to the tell the NIC.
    /// Returns:
    /// Result<(inline size, number of data segments)>
    fn calculate_shape(&self, sga: &Sga) -> Result<(usize, usize)> {
        let (inline_size, mut sga_idx, mut num_segs) = self.calculate_inline_size(sga.get(0).len());

        while sga_idx < sga.len() {
            let curr_seg = sga.get(sga_idx);
            if self.zero_copy_seg(curr_seg.addr()) {
                // each zero copy seg is counted as a separate segment
                sga_idx += 1;
                num_segs += 1;
                tracing::debug!(
                    "Adding zero-copy seg for seg {:?}",
                    curr_seg.addr().as_ptr()
                );
            } else {
                // iterate until the next non-zero copy segment
                let mut forward_index = sga_idx;
                while forward_index < sga.len() {
                    let seg = sga.get(sga_idx);
                    if !self.zero_copy_seg(seg.addr()) {
                        forward_index += 1;
                    } else {
                        if !(sga_idx == 0 && num_segs == 1) {
                            // past first entry (which can be copied into header segment)
                            num_segs += 1;
                        }
                        sga_idx = forward_index;
                        break;
                    }
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
                        sizes::RX_MEMPOOL_METADATA_PGSIZE,
                        sizes::RX_MEMPOOL_DATA_PGSIZE,
                        sizes::RX_MEMPOOL_DATA_LEN,
                    )
                    .wrap_err("Incorrect rx allocation params")?;
                check_ok!(custom_mlx5_init_rx_mempools(
                    global_context_ptr as _,
                    rx_mempool_params.get_item_len() as _,
                    rx_mempool_params.get_num_items() as _,
                    rx_mempool_params.get_data_pgsize() as _,
                    rx_mempool_params.get_metadata_pgsize() as _,
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
        let mut allocator = MemoryPoolAllocator::default();
        let mempool = DataMempool::new_from_ptr(context.get_recv_mempool_ptr());
        allocator.add_recv_mempool(mempool);

        Ok(Mlx5Connection {
            thread_context: context,
            mode: mode,
            outgoing_window: HashMap::default(),
            active_connections: [None; MAX_CONCURRENT_CONNECTIONS],
            address_to_conn_id: HashMap::default(),
            allocator: allocator,
            copying_threshold: 256,
            inline_mode: InlineMode::default(),
            max_inline_size: 256,
            recv_mbufs: [std::ptr::null_mut(); RECEIVE_BURST_SIZE],
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

    fn push_buffers_with_copy(&mut self, pkts: Vec<(MsgID, ConnID, &[u8])>) -> Result<()> {
        tracing::debug!("Pushing batch of pkts of length {}", pkts.len());
        let mut pkt_idx = 0;
        let mut first_ctrl_seg: Option<*mut mlx5_wqe_ctrl_seg> = None;
        while pkt_idx < pkts.len() {
            let (msg_id, conn_id, buf) = pkts[pkt_idx];
            self.insert_into_outgoing_map(msg_id, conn_id);

            let (buf_size, inline_len) = match self.inline_mode {
                InlineMode::Nothing => (buf.len() + cornflakes_libos::utils::TOTAL_HEADER_SIZE, 0),
                InlineMode::PacketHeader => (buf.len(), cornflakes_libos::utils::TOTAL_HEADER_SIZE),
                InlineMode::FirstEntry => {
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
                    InlineMode::PacketHeader | InlineMode::FirstEntry => {
                        // inline packet header
                        self.inline_hdr(conn_id, msg_id, inline_len, buf.len())?;
                        written_header = true;

                        if self.inline_mode == InlineMode::FirstEntry
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
                    let mut data_buffer = match self.allocator.allocate_buffer(allocation_size)? {
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
                    let mut metadata_mbuf = match MbufMetadata::from_buf(data_buffer)? {
                        Some(m) => m,
                        None => {
                            bail!(
                            "Could not allocate corresponding metadata for allocated data buffer"
                        );
                        }
                    };
                    metadata_mbuf.increment_refcnt();

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
                            metadata_mbuf.mbuf(),
                            metadata_mbuf.offset() as _,
                            metadata_mbuf.data_len() as _,
                        );

                        custom_mlx5_add_completion_info(
                            self.thread_context.get_context_ptr(),
                            completion,
                            metadata_mbuf.mbuf(),
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
            } else {
                // ASSUMES THAT THE PACKET HAS A FULL HEADER TO FLIP
                unsafe {
                    flip_headers(received_pkt.seg(0).mbuf());
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
                            seg.mbuf(),
                            seg.offset() as _,
                            seg.data_len() as _,
                        );

                        curr_completion = custom_mlx5_add_completion_info(
                            self.thread_context.get_context_ptr(),
                            curr_completion,
                            seg.mbuf(),
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
        Ok(())
    }

    fn serialize_and_send(
        &mut self,
        _objects: &Vec<(MsgID, ConnID, impl Serializable<Self>)>,
    ) -> Result<()>
    where
        Self: Sized,
    {
        Ok(())
    }

    fn push_rc_sgas(&mut self, rc_sgas: &mut Vec<(MsgID, ConnID, RcSga<Self>)>) -> Result<()>
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
        Ok(())
    }

    fn push_sgas(&mut self, sgas: &Vec<(MsgID, ConnID, Sga)>) -> Result<()> {
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

        Ok(())
    }

    fn pop_with_durations(&mut self) -> Result<Vec<(ReceivedPkt<Self>, Duration)>>
    where
        Self: Sized,
    {
        let received = unsafe {
            custom_mlx5_gather_rx(
                self.thread_context.get_context_ptr(),
                self.recv_mbufs.as_mut_ptr(),
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
                            unsafe {
                                custom_mlx5_free_mbuf(self.recv_mbufs[i]);
                            }
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
                unsafe {
                    custom_mlx5_free_mbuf(self.recv_mbufs[i]);
                }
                self.recv_mbufs[i] = ptr::null_mut();
            }
        }
        Ok(ret)
    }

    // TODO: potential optimization to provide the vector the keep received packets
    fn pop(&mut self) -> Result<Vec<ReceivedPkt<Self>>>
    where
        Self: Sized,
    {
        let received = unsafe {
            custom_mlx5_gather_rx(
                self.thread_context.get_context_ptr(),
                self.recv_mbufs.as_mut_ptr(),
                RECEIVE_BURST_SIZE as _,
            )
        };
        let mut ret: Vec<ReceivedPkt<Self>> = Vec::with_capacity(RECEIVE_BURST_SIZE);
        for i in 0..received as usize {
            if let Some(received_pkt) = self
                .check_received_pkt(i)
                .wrap_err("Error receiving packets")?
            {
                ret.push(received_pkt);
            } else {
                // free the mbuf
                unsafe {
                    custom_mlx5_free_mbuf(self.recv_mbufs[i]);
                }
                self.recv_mbufs[i] = ptr::null_mut();
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
        self.allocator.is_registered(buf)
    }

    fn allocate(&mut self, size: usize) -> Result<Option<Self::DatapathBuffer>> {
        self.allocator.allocate_buffer(size)
    }

    fn get_metadata(&self, buf: Self::DatapathBuffer) -> Result<Option<Self::DatapathMetadata>> {
        MbufMetadata::from_buf(buf)
    }

    fn add_memory_pool(&mut self, size: usize, min_elts: usize) -> Result<Vec<MempoolID>> {
        // use 2MB pages for data, 2MB pages for metadata (?)
        let metadata_pgsize = match min_elts > 8192 {
            true => PGSIZE_4KB,
            false => PGSIZE_2MB,
        };
        let mempool_params =
            sizes::MempoolAllocationParams::new(min_elts, metadata_pgsize, PGSIZE_2MB, size)
                .wrap_err("Incorrect mempool allocation params")?;
        let data_mempool = DataMempool::new(&mempool_params, &self.thread_context)?;
        let id = self
            .allocator
            .add_mempool(mempool_params.get_item_len(), data_mempool)?;

        Ok(vec![id])
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

    fn set_inline_mode(&mut self, inline_mode: InlineMode) {
        self.inline_mode = inline_mode;
    }
}
