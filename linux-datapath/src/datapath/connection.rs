use bytes::{BufMut, Bytes, BytesMut};
use color_eyre::eyre::{bail, ensure, Result};
use cornflakes_libos::{
    allocator::MempoolID,
    datapath::{Datapath, ExposeMempoolID, InlineMode, MetadataOps, ReceivedPkt},
    utils::AddressInfo,
    ConnID, MsgID, OrderedSga, RcSga, Sga,
};
use std::{io::Write, net::Ipv4Addr, time::Duration};

#[derive(PartialEq, Eq, Debug, Clone, Default)]
pub struct MutableByteBuffer {
    buf: BytesMut,
}

impl ExposeMempoolID for MutableByteBuffer {
    fn set_mempool_id(&mut self, _id: MempoolID) {}

    fn get_mempool_id(&self) -> MempoolID {
        0
    }
}

impl MutableByteBuffer {
    pub fn new(buf: &[u8]) -> Self {
        MutableByteBuffer {
            buf: BytesMut::from(buf),
        }
    }

    pub fn new_with_capacity(data_len: usize) -> Self {
        // TODO: bad because this is a stack allocation?
        MutableByteBuffer {
            buf: BytesMut::from(vec![0u8; data_len].as_slice()),
        }
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn get_inner(self) -> BytesMut {
        self.buf
    }

    pub fn mutable_slice(&mut self, start: usize, end: usize) -> Result<&mut [u8]> {
        if start > self.buf.len() || end > self.buf.len() || end > start {
            bail!("Invalid bounds for MutableByteBuffer");
        }
        Ok(&mut self.buf.as_mut()[start..end])
    }
}

impl AsRef<[u8]> for MutableByteBuffer {
    fn as_ref(&self) -> &[u8] {
        self.buf.as_ref()
    }
}

impl Write for MutableByteBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let to_write = std::cmp::min(buf.len(), self.buf.len());
        self.buf.as_mut().write(&buf[0..to_write])
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(PartialEq, Eq, Clone, Debug, Default)]
pub struct ByteBuffer {
    /// Actual data: not write-able.
    pub bytes: Bytes,
    /// Application data offset
    pub offset: usize,
    /// Application data length
    pub len: usize,
}

impl ByteBuffer {
    pub fn from_buf(buffer: MutableByteBuffer) -> Self {
        let len = buffer.len();
        let buf = buffer.get_inner().freeze();
        ByteBuffer {
            bytes: buf,
            offset: 0,
            len: len,
        }
    }

    pub fn from_raw_buf(buffer: &[u8]) -> Self {
        let mut bytes = BytesMut::with_capacity(buffer.len());
        bytes.copy_from_slice(buffer);
        let buf = bytes.freeze();
        ByteBuffer {
            bytes: buf,
            offset: 0,
            len: buffer.len(),
        }
    }

    pub fn new(buf: &[u8], data_offset: usize, data_len: Option<usize>) -> Result<Self> {
        ensure!(data_offset <= buf.len(), "Provided data offset too large");
        let len = match data_len {
            Some(x) => {
                ensure!(x <= buf.len() - data_offset, "Data len to large");
                x
            }
            None => buf.len() - data_offset,
        };
        let mut bytes_mut = BytesMut::with_capacity(buf.len());
        bytes_mut.put(buf);
        Ok(ByteBuffer {
            bytes: bytes_mut.freeze(),
            offset: data_offset,
            len: len,
        })
    }
}

impl AsRef<[u8]> for ByteBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.bytes.as_ref()[self.offset..(self.offset + self.len)]
    }
}
impl MetadataOps for ByteBuffer {
    fn offset(&self) -> usize {
        self.offset
    }

    fn data_len(&self) -> usize {
        self.len
    }

    fn set_data_len_and_offset(&mut self, len: usize, offset: usize) -> Result<()> {
        ensure!(offset <= self.bytes.len(), "Offset too large");
        ensure!(
            len <= self.bytes.len() - offset,
            "Provided data len too large"
        );
        self.offset = offset;
        self.len = len;
        Ok(())
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct LinuxPerThreadContext {
    // TODO: insert per thread context
}

#[derive(Debug, Clone)]
pub struct LinuxDatapathSpecificParams {
    // TODO: insert datapath specific params:
    // Server UDP port, Server IP address, potentially server interface name
}

pub struct LinuxConnection {
    // TODO: insert linux connection params
}

impl LinuxConnection {
    fn _insert_into_outgoing_map(&mut self, _msg_id: MsgID, _conn_id: ConnID) {
        unimplemented!();
    }

    fn _check_received_pkt(&self) -> Result<Option<ReceivedPkt<Self>>> {
        unimplemented!();
    }
}

impl Datapath for LinuxConnection {
    type DatapathBuffer = MutableByteBuffer;

    type DatapathMetadata = ByteBuffer;

    type PerThreadContext = LinuxPerThreadContext;

    type DatapathSpecificParams = LinuxDatapathSpecificParams;

    fn parse_config_file(
        _config_file: &str,
        _our_ip: &Ipv4Addr,
    ) -> Result<Self::DatapathSpecificParams> {
        unimplemented!();
    }

    fn compute_affinity(
        _datapath_params: &Self::DatapathSpecificParams,
        _num_queues: usize,
        _remote_ip: Option<Ipv4Addr>,
        _app_mode: cornflakes_utils::AppMode,
    ) -> Result<Vec<AddressInfo>> {
        unimplemented!();
    }

    fn global_init(
        _num_queues: usize,
        _datapath_params: &mut Self::DatapathSpecificParams,
        _addresses: Vec<AddressInfo>,
    ) -> Result<Vec<Self::PerThreadContext>> {
        unimplemented!();
    }

    fn per_thread_init(
        _datapath_params: Self::DatapathSpecificParams,
        _context: Self::PerThreadContext,
        _mode: cornflakes_utils::AppMode,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        unimplemented!();
    }

    fn connect(&mut self, _addr: AddressInfo) -> Result<ConnID> {
        unimplemented!();
    }

    fn push_buffers_with_copy(&mut self, _pkts: &[(MsgID, ConnID, &[u8])]) -> Result<()> {
        unimplemented!();
    }

    fn echo(&mut self, mut _pkts: Vec<ReceivedPkt<Self>>) -> Result<()>
    where
        Self: Sized,
    {
        unimplemented!();
    }

    fn push_rc_sgas(&mut self, _rc_sgas: &mut [(MsgID, ConnID, RcSga<Self>)]) -> Result<()>
    where
        Self: Sized,
    {
        unimplemented!();
    }

    fn push_ordered_sgas(&mut self, _ordered_sgas: &[(MsgID, ConnID, OrderedSga)]) -> Result<()> {
        unimplemented!();
    }

    fn push_sgas(&mut self, _sgas: &[(MsgID, ConnID, Sga)]) -> Result<()> {
        unimplemented!();
    }

    fn pop_with_durations(&mut self) -> Result<Vec<(ReceivedPkt<Self>, Duration)>>
    where
        Self: Sized,
    {
        unimplemented!();
    }

    fn pop(&mut self) -> Result<Vec<ReceivedPkt<Self>>>
    where
        Self: Sized,
    {
        unimplemented!();
    }

    fn timed_out(&self, _time_out: Duration) -> Result<Vec<(MsgID, ConnID)>> {
        unimplemented!();
    }

    fn is_registered(&self, _buf: &[u8]) -> bool {
        false
    }

    fn allocate(&mut self, _size: usize) -> Result<Option<Self::DatapathBuffer>> {
        unimplemented!();
    }

    fn get_metadata(&self, buf: Self::DatapathBuffer) -> Result<Option<Self::DatapathMetadata>> {
        Ok(Some(ByteBuffer::from_buf(buf)))
    }

    fn recover_metadata(&self, buf: &[u8]) -> Result<Option<Self::DatapathMetadata>> {
        Ok(Some(ByteBuffer::from_raw_buf(buf)))
    }

    fn add_tx_mempool(&mut self, _size: usize, _min_elts: usize) -> Result<()> {
        unimplemented!();
    }

    fn add_memory_pool(&mut self, _size: usize, _min_elts: usize) -> Result<Vec<MempoolID>> {
        unimplemented!();
    }

    fn register_mempool(&mut self, _id: MempoolID) -> Result<()> {
        unimplemented!();
    }

    fn unregister_mempool(&mut self, _id: MempoolID) -> Result<()> {
        unimplemented!();
    }

    fn header_size(&self) -> usize {
        unimplemented!();
    }

    fn timer_hz(&self) -> u64 {
        unimplemented!();
    }

    fn cycles_to_ns(&self, _t: u64) -> u64 {
        unimplemented!();
    }

    fn current_cycles(&self) -> u64 {
        unimplemented!();
    }

    fn set_copying_threshold(&mut self, _threshold: usize) {}

    fn get_copying_threshold(&self) -> usize {
        std::usize::MAX
    }

    fn set_max_segments(&mut self, _segs: usize) {}

    fn get_max_segments(&self) -> usize {
        std::usize::MAX
    }

    fn set_inline_mode(&mut self, _mode: InlineMode) {}

    fn max_packet_size() -> usize {
        1500
    }
}
