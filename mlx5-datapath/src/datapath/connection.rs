use super::{
    super::{access, mbuf_slice, mlx5_bindings::*},
    wrapper,
};
use cornflakes_libos::{
    datapath,
    datapath::{Datapath, MetadataOps, ReceivedPkt},
    serialize::Serializable,
    utils::AddressInfo,
    ConnID, MsgID, RcSga, Sga, USING_REF_COUNTING,
};

use color_eyre::eyre::{bail, ensure, Result, WrapErr};
use cornflakes_utils::{parse_yaml_map, AppMode};
use eui48::MacAddress;
use std::{
    collections::HashMap,
    io::Write,
    net::Ipv4Addr,
    ptr,
    time::{Duration, Instant},
};

#[cfg(feature = "profiler")]
use perftools;

#[derive(PartialEq, Eq)]
pub struct Mlx5Buffer {
    /// Underlying data pointer
    data: *mut ::std::os::raw::c_void,
    /// Pointer back to the memory pool
    mempool: *mut mempool,
    /// Data len,
    data_len: usize,
}

impl Mlx5Buffer {
    pub fn alloc(data_mempool: *mut mempool) -> Result<Self> {
        let buf = unsafe { alloc_buf(data_mempool) };
        ensure!(!buf.is_null(), "Allocated data buffer is null");
        Ok(Mlx5Buffer {
            data: buf,
            mempool: data_mempool,
            data_len: 0,
        })
    }

    pub fn get_inner(self) -> (*mut ::std::os::raw::c_void, *mut mempool, usize) {
        (self.data, self.mempool, self.data_len)
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

impl AsMut<[u8]> for Mlx5Buffer {
    fn as_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.data as *mut u8, self.data_len) }
    }
}

impl Write for Mlx5Buffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // only write the maximum amount
        let written = std::cmp::min(unsafe { access!(self.mempool, item_len, usize) }, buf.len());
        let written = self.as_mut().write(&buf[0..written])?;
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
    pub mbuf: *mut mbuf,
    /// Application data offset
    pub offset: usize,
    /// Application data length
    pub len: usize,
}

impl MbufMetadata {
    /// Initializes a metadata object and consumes the given datapath buffer.
    /// By doing this, the underlying datapath buffer is no longer writable.
    /// Args:
    /// @metadata_mempool: mempool to allocate metadata buffer from
    /// @datapath_buffer: Mlx5Buffer containing data to point to.
    fn from_buf(metadata_mempool: *mut mempool, datapath_buffer: Mlx5Buffer) -> Result<Self> {
        let metadata_mbuf = unsafe { alloc_metadata(metadata_mempool) };
        ensure!(!metadata_mbuf.is_null(), "Not able to allocate metadata");

        // consume the underlying buffer: cannot write to the underlying buffer anymore
        let (buf, mempool, data_len) = datapath_buffer.get_inner();
        unsafe {
            // init the metadata mbuf
            init_metadata(metadata_mbuf, buf, mempool, metadata_mempool, data_len, 0);
            // update the refcnt to 1
            mbuf_refcnt_update_or_free(metadata_mbuf, 1);
        }
        Ok(MbufMetadata {
            mbuf: metadata_mbuf,
            offset: 0,
            len: data_len,
        })
    }

    /// Initializes metadata object from existing metadata mbuf in c.
    /// Used for received mbufs: the datapath indpendently allocates the data and metadata buffer.
    /// Args:
    /// @mbuf: mbuf structure that contains metadata
    /// @data_offset: Application data offset into this buffer.
    /// @data_len: Optional application data length into the buffer.
    fn new(mbuf: *mut mbuf, data_offset: usize, data_len: Option<usize>) -> Self {
        unsafe {
            mbuf_refcnt_update_or_free(mbuf, 1);
        }
        let len = match data_len {
            Some(x) => x,
            None => unsafe { (*mbuf).data_len as usize },
        };
        MbufMetadata {
            mbuf: mbuf,
            offset: data_offset,
            len: len,
        }
    }
}

impl MetadataOps for MbufMetadata {
    fn set_offset(&mut self, off: usize) -> Result<()> {
        ensure!(
            off < unsafe { access!(self.mbuf, data_buf_len, usize) },
            "Offset too large"
        );
        self.offset = off;
        Ok(())
    }

    fn set_data_len(&mut self, data_len: usize) -> Result<()> {
        ensure!(
            data_len <= unsafe { access!(self.mbuf, data_buf_len, usize) },
            "Data_len too large"
        );
        self.len = data_len;
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
                mbuf_refcnt_update_or_free(self.mbuf, -1);
            } else {
                mbuf_free(self.mbuf);
            }
        }
    }
}

impl Clone for MbufMetadata {
    fn clone(&self) -> MbufMetadata {
        unsafe {
            if USING_REF_COUNTING {
                mbuf_refcnt_update_or_free(self.mbuf, 1);
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
pub struct Mlx5PerThreadContext {
    /// Queue id
    queue_id: u16,
    /// Source address info (ethernet, ip, port)
    address_info: AddressInfo,
    /// Pointer to datapath specific thread information
    context: *mut mlx5_per_thread_context,
}

unsafe impl Send for Mlx5PerThreadContext {}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Mlx5GlobalContext {
    /// Pointer to datapath specific global state
    context: *mut mlx5_global_context,
}

#[derive(Debug, Clone)]
pub struct Mlx5DatapathSpecificParams {
    pci_addr: pci_addr,
    eth_addr: eth_addr,
    server_port: u16,
}

unsafe impl Send for Mlx5GlobalContext {}

pub struct Mlx5Connection {
    /// Per thread context.
    thread_context: Mlx5PerThreadContext,
    /// Scatter-gather mode turned on or off.
    use_scatter_gather: bool,
    /// Server or client mode
    mode: AppMode,
    /// Map of IP address to ethernet addresses loaded at config time.
    ip_to_mac: HashMap<Ipv4Addr, MacAddress>,
    /// Current window of outstanding packets (used for keeping track of RTTs).
    outgoing_window: HashMap<MsgID, Instant>,
}

impl Datapath for Mlx5Connection {
    type DatapathBuffer = Mlx5Buffer;
    type DatapathMetadata = MbufMetadata;

    type PerThreadContext = Mlx5PerThreadContext;

    type GlobalContext = Mlx5GlobalContext;

    type DatapathSpecificParams = Mlx5DatapathSpecificParams;

    fn parse_config_file(
        config_file: &str,
        our_ip: Option<Ipv4Addr>,
    ) -> Result<Self::DatapathSpecificParams> {
        unimplemented!();
    }

    fn compute_affinity(num_queues: usize, remote_ip: Ipv4Addr) -> Result<Vec<AddressInfo>> {
        unimplemented!();
    }

    fn global_init(
        num_queues: usize,
        datapath_params: Self::DatapathSpecificParams,
    ) -> Result<(Self::GlobalContext, Vec<Self::PerThreadContext>)> {
        unimplemented!();
    }

    fn global_teardown(context: Self::GlobalContext) -> Result<()> {
        unimplemented!();
    }

    fn per_thread_init(context: Self::PerThreadContext) -> Result<Self>
    where
        Self: Sized,
    {
        unimplemented!();
    }

    fn connect(&mut self, addr: AddressInfo) -> Result<ConnID> {
        Ok(0)
    }

    fn push_buffers(&mut self, pkts: Vec<(MsgID, ConnID, &[u8])>) -> Result<()> {
        Ok(())
    }

    fn echo(&mut self, pkts: Vec<ReceivedPkt<Self>>) -> Result<()>
    where
        Self: Sized,
    {
        Ok(())
    }

    fn serialize_and_send(
        &mut self,
        objects: &Vec<(MsgID, ConnID, impl Serializable<Self>)>,
    ) -> Result<()>
    where
        Self: Sized,
    {
        Ok(())
    }

    fn push_rc_sgas(&mut self, rc_sgas: &Vec<(MsgID, ConnID, RcSga<Self>)>) -> Result<()>
    where
        Self: Sized,
    {
        Ok(())
    }

    fn push_sgas(&mut self, sgas: &Vec<(MsgID, ConnID, Sga)>) -> Result<()> {
        Ok(())
    }

    fn pop_with_durations(&mut self) -> Result<Vec<(ReceivedPkt<Self>, Duration)>>
    where
        Self: Sized,
    {
        Ok(Vec::default())
    }

    fn pop(&mut self) -> Result<Vec<ReceivedPkt<Self>>>
    where
        Self: Sized,
    {
        Ok(Vec::default())
    }

    fn timed_out(&self, time_out: Duration) -> Result<Vec<MsgID>> {
        Ok(Vec::default())
    }

    fn allocate(&self, size: usize, alignment: usize) -> Result<Option<Self::DatapathBuffer>> {
        Ok(None)
    }

    fn get_metadata(&self, buf: Self::DatapathBuffer) -> Result<Self::DatapathMetadata> {
        unimplemented!();
    }

    fn add_memory_pool(&self, size: usize, min_elts: usize) -> Result<()> {
        Ok(())
    }

    fn timer_hz(&self) -> u64 {
        0
    }

    fn current_cycles(&self) -> u64 {
        0
    }
}
