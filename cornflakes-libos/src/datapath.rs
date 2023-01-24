use super::{
    allocator::MempoolID, dynamic_rcsga_hybrid_hdr::HybridArenaRcSgaHdr,
    dynamic_sga_hdr::SgaHeaderRepr, utils::AddressInfo, ArenaDatapathSga, ArenaOrderedRcSga,
    ArenaOrderedSga, ConnID, CopyContext, MsgID, OrderedRcSga, OrderedSga, RcSga, Sga,
};
use color_eyre::eyre::{bail, Result};
use std::{io::Write, net::Ipv4Addr, str::FromStr, time::Duration};
use zero_copy_cache::data_structures::Segment;

#[derive(PartialEq, Eq, Debug, Copy, Clone)]
pub enum InlineMode {
    Nothing,
    PacketHeader,
    ObjectHeader,
}

impl Default for InlineMode {
    fn default() -> Self {
        InlineMode::Nothing
    }
}

impl FromStr for InlineMode {
    type Err = color_eyre::eyre::Error;

    fn from_str(s: &str) -> Result<InlineMode> {
        match s {
            "nothing" | "Nothing" | "0" | "NOTHING" => Ok(InlineMode::Nothing),
            "packetheader" | "PacketHeader" | "PACKETHEADER" | "packet_header" => {
                Ok(InlineMode::PacketHeader)
            }
            "objectheader" | "object_header" | "OBJECTHEADER" | "ObjectHeader" => {
                Ok(InlineMode::ObjectHeader)
            }
            x => {
                bail!("Unknown inline mode: {:?}", x);
            }
        }
    }
}

/// Represents if app is using:
/// (1) Scatter-gather API without manual ref counting
/// (2) Manually Reference counted scatter-gather API
/// (3) Pushing a single buffer to be copied
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum PushBufType {
    Sga,
    RcSga,
    SingleBuf,
    OrderedSga,
    Object,
    ArenaOrderedSga,
    Echo,
}

impl FromStr for PushBufType {
    type Err = color_eyre::eyre::Error;

    fn from_str(s: &str) -> Result<PushBufType> {
        match s {
            "sga" | "SGA" | "Sga" => Ok(PushBufType::Sga),
            "rcsga" | "RcSga" | "RCSGA" => Ok(PushBufType::RcSga),
            "singlebuf" | "single_buf" | "SingleBuf" | "SINGLEBUF" => Ok(PushBufType::SingleBuf),
            "orderedsga" | "ordered_sga" | "OrderedSga" | "ORDEREDSGA" => {
                Ok(PushBufType::OrderedSga)
            }
            "arenaorderedsga" | "arena_ordered_sga" | "ArenaOrderedSga" | "ARENAORDEREDSGA" => {
                Ok(PushBufType::ArenaOrderedSga)
            }
            "echo" | "ECHO" | "Echo" => Ok(PushBufType::Echo),
            "object" | "OBJECT" | "Object" => Ok(PushBufType::Object),
            x => {
                bail!("Unknown push buf type: {:?}", x);
            }
        }
    }
}

pub struct ReceivedPkt<D>
where
    D: Datapath,
{
    pkts: Vec<D::DatapathMetadata>,
    id: MsgID,
    conn: ConnID,
}

impl<D> ReceivedPkt<D>
where
    D: Datapath,
{
    pub fn new(pkts: Vec<D::DatapathMetadata>, id: MsgID, conn_id: ConnID) -> Self {
        ReceivedPkt {
            pkts: pkts,
            id: id,
            conn: conn_id,
        }
    }

    pub fn data_len(&self) -> usize {
        let sum: usize = self.pkts.iter().map(|pkt| pkt.data_len()).sum();
        sum
    }

    pub fn conn_id(&self) -> ConnID {
        self.conn
    }

    pub fn msg_id(&self) -> MsgID {
        self.id
    }

    pub fn num_segs(&self) -> usize {
        self.pkts.len()
    }

    pub fn seg(&self, idx: usize) -> &D::DatapathMetadata {
        &self.pkts[idx]
    }

    pub fn iter(&self) -> std::slice::Iter<D::DatapathMetadata> {
        self.pkts.iter()
    }

    pub fn iter_mut(&mut self) -> std::slice::IterMut<D::DatapathMetadata> {
        self.pkts.iter_mut()
    }

    pub fn contiguous_datapath_metadata_from_buf(
        &self,
        buf: &[u8],
    ) -> Result<Option<D::DatapathMetadata>> {
        let buf_ptr = buf.as_ptr() as usize;
        let buf_end = buf.as_ptr() as usize + buf.len();
        for pkt in self.pkts.iter() {
            tracing::debug!(buf_ptr =? buf.as_ptr(), buf_len = buf.len(), pkt_ptr =? pkt.as_ref().as_ptr(), pkt_data_len = pkt.data_len(), "Recovering contiguous metadata from buf");
            let ref_buf = pkt.as_ref().as_ptr() as usize;
            let ref_buf_end = pkt.as_ref().as_ptr() as usize + pkt.data_len();
            if buf_ptr >= ref_buf && buf_end <= ref_buf_end {
                let mut cloned_metadata = pkt.clone();
                cloned_metadata.set_data_len_and_offset(
                    buf.len(),
                    cloned_metadata.offset() + (buf_ptr - ref_buf),
                )?;
                return Ok(Some(cloned_metadata));
            }
        }
        return Ok(None);
    }

    /// Given a start index and length, return a datapath metadata object referring to the given
    /// contiguous slice within the packet if it exists.
    /// Arguments:
    /// @start - start index into received packet bytes.
    /// @len - length of desired contiguous slice
    pub fn contiguous_datapath_metadata(
        &self,
        start: usize,
        len: usize,
    ) -> Result<Option<D::DatapathMetadata>> {
        let mut cur_seg_offset = 0;
        for idx in 0..self.pkts.len() {
            if start >= cur_seg_offset && start < (cur_seg_offset + self.pkts[idx].data_len()) {
                if (start + len) > (cur_seg_offset + self.pkts[idx].data_len()) {
                    // bounds not a contiguous slice
                    return Ok(None);
                } else {
                    let slice_offset = start - cur_seg_offset;
                    let mut cloned_metadata = self.pkts[idx].clone();
                    cloned_metadata
                        .set_data_len_and_offset(len, cloned_metadata.offset() + slice_offset)?;
                    return Ok(Some(cloned_metadata));
                }
            }
            // TODO: is there a "we've gotten past this slice" condition we can check?
            cur_seg_offset += self.pkts[idx].data_len();
        }
        return Ok(None);
    }

    /// Given a start index and length, returns a contiguous slice within the packet if it exists.
    /// Arguments:
    /// @start - start index into received packet bytes.
    /// @len - length of desired contiguous slice.
    pub fn contiguous_slice(&self, start: usize, len: usize) -> Option<&[u8]> {
        let mut cur_seg_offset = 0;
        for idx in 0..self.pkts.len() {
            if start >= cur_seg_offset && start < (cur_seg_offset + self.pkts[idx].data_len()) {
                if (start + len) > (cur_seg_offset + self.pkts[idx].data_len()) {
                    // bounds not a contiguous slice
                    return None;
                } else {
                    let slice_offset = start - cur_seg_offset;
                    return Some(&self.pkts[idx].as_ref()[slice_offset..(slice_offset + len)]);
                }
            }
            cur_seg_offset += self.pkts[idx].data_len();
        }

        return None;
    }

    pub fn flatten(&self) -> Vec<u8> {
        let bytes: Vec<u8> = self
            .pkts
            .iter()
            .map(|pkt| pkt.as_ref().to_vec())
            .flatten()
            .collect();
        bytes
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct CornflakesSegment {
    mempool_id: MempoolID,
    page_size: usize,
}

impl Segment for CornflakesSegment {
    fn get_segment_id(&self) -> i64 {
        self.mempool_id as _
    }

    fn get_page_size(&self) -> u64 {
        self.page_size as _
    }
}

impl CornflakesSegment {
    pub fn new(id: MempoolID, page_size: usize) -> Self {
        CornflakesSegment {
            mempool_id: id,
            page_size,
        }
    }
}

pub enum MetadataStatus<D>
where
    D: Datapath,
{
    /// Allocated by allocator & is zero-copy-able
    Pinned((D::DatapathMetadata, CornflakesSegment)),
    /// Allocated by allocator but currently not pinned
    UnPinned((D::DatapathMetadata, CornflakesSegment)),
    /// Not allocated by allocator
    Arbitrary,
}

/// Functionality accessible to higher level application on top of datapath metadata objects.
pub trait MetadataOps {
    fn offset(&self) -> usize;

    fn data_len(&self) -> usize;

    fn set_data_len_and_offset(&mut self, data_len: usize, offset: usize) -> Result<()>;
}

pub trait DatapathBufferOps {
    fn set_len(&mut self, len: usize);

    fn get_mutable_slice(&mut self, start: usize, len: usize) -> Result<&mut [u8]>;
}

pub trait Datapath {
    /// Mutable buffer type that can be written into.
    type DatapathBuffer: AsRef<[u8]>
        + Write
        + PartialEq
        + Eq
        + std::fmt::Debug
        + DatapathBufferOps
        + Default
        + Clone;

    /// Metadata that wraps around a datapath buffer.
    type DatapathMetadata: AsRef<[u8]>
        + PartialEq
        + Eq
        + Clone
        + std::fmt::Debug
        + MetadataOps
        + Default;

    type CallbackEntryState;

    /// Any per thread context required by the datapath per thread.
    type PerThreadContext: Send + Clone;

    /// Any datapath specific parameters.
    type DatapathSpecificParams: Send + Clone;

    /// Parse the given yaml file and return all the datapath specific information necessary for
    /// initialization.
    /// Args:
    /// @config_file: Yaml config file.
    /// @our_ip: Optional ip address of this machine ( potentially necessary for self identification for ethernet
    /// address).
    fn parse_config_file(
        config_file: &str,
        our_ip: &Ipv4Addr,
    ) -> Result<Self::DatapathSpecificParams>;

    /// Given a remote IP address, compute a source IP and port for each queue
    /// Such that receiving a packet with that IP and port as the destination,
    /// and remote_ip as source, will be hashed to that queue ID.
    fn compute_affinity(
        datapath_params: &Self::DatapathSpecificParams,
        num_queues: usize,
        remote_ip: Option<Ipv4Addr>,
        app_mode: cornflakes_utils::AppMode,
    ) -> Result<Vec<AddressInfo>>;

    /// Any global initialization required by this datapath.
    /// Initialization might include: memory registration per queue,
    /// and flow initialization.
    /// Args:
    /// @num_queues: Number of queues to initialize.
    /// @datapath_params: Parsed datapath parameters.
    fn global_init(
        num_queues: usize,
        datapath_params: &mut Self::DatapathSpecificParams,
        addresses: Vec<AddressInfo>,
    ) -> Result<Vec<Self::PerThreadContext>>;

    /// Per thread initialization for a particular queue.
    /// Args:
    /// @config_file: Configuration information in YAML format.
    /// @context: Specific, per thread context for this queue.
    /// @mode: Server or client mode.
    fn per_thread_init(
        datapath_params: Self::DatapathSpecificParams,
        context: Self::PerThreadContext,
        mode: cornflakes_utils::AppMode,
    ) -> Result<Self>
    where
        Self: Sized;

    /// "Open" a connection to the other side.
    /// Args:
    /// @addr: Address information to connect to. Returns a unique "connection" ID.
    fn connect(&mut self, addr: AddressInfo) -> Result<ConnID>;

    /// Send multiple buffers to the specified address.
    /// Args:
    /// @pkts: Vector of (msg id, buffer, connection id) to send.
    fn push_buffers_with_copy(&mut self, pkts: &[(MsgID, ConnID, &[u8])]) -> Result<()>;

    /// Send multiple buffers to the specified address.
    /// Args:
    /// @pkts: Vector of (msg id, buffer, connection id) to send.
    fn push_buffers_with_copy_iterator<'a>(
        &mut self,
        pkts: impl Iterator<Item = (MsgID, ConnID, &'a [u8])>,
    ) -> Result<()> {
        let buffers: Vec<(MsgID, ConnID, &[u8])> = pkts.collect();
        self.push_buffers_with_copy(buffers.as_slice())
    }

    /// Echo the specified packet back to the  source.
    /// Args:
    /// @pkts: Vector of received packet objects to echo back.
    fn echo(&mut self, pkts: Vec<ReceivedPkt<Self>>) -> Result<()>
    where
        Self: Sized;

    /// Serialize and send serializable objects.
    /// Args:
    /// @objects: Vector of (msg id, connection id, serializable objects) to send.
    fn serialize_and_send<'a>(
        &mut self,
        _objects: impl Iterator<Item = Result<(MsgID, ConnID, impl SgaHeaderRepr<'a>)>>,
    ) -> Result<()>
    where
        Self: Sized,
    {
        Ok(())
    }

    /// Send as a reference counted scatter-gather array.
    /// Args:
    /// @rc_sgas: Vector of (msg id, connection id, reference counted scatter-gather arrays) to send.
    /// Must be mutable in order to correctly increment the reference counts on the underlying
    /// buffers.
    fn push_rc_sgas(&mut self, rc_sgas: &mut [(MsgID, ConnID, RcSga<Self>)]) -> Result<()>
    where
        Self: Sized;

    /// Send scatter-gather arrays that are ORDERED.
    /// OrderedSgas must be ordered such that the last "num_zero_copy_entries()" are
    /// zero-copy-able and pass the to-copy or not heuristics.
    /// First sga.len() - num_zero_copy_entries() will be copied together.
    fn push_ordered_sgas(&mut self, ordered_sgas: &[(MsgID, ConnID, OrderedSga)]) -> Result<()>;

    /// Push an iterator over ordered SGAS.
    fn push_ordered_sgas_iterator<'sge>(
        &self,
        _ordered_sgas: impl Iterator<Item = Result<(MsgID, ConnID, OrderedSga<'sge>)>>,
    ) -> Result<()> {
        Ok(())
    }

    /// Assumes that there is space at the front to write in packet header.
    /// Used for baseline implementation.
    fn queue_datapath_buffer(
        &mut self,
        _msg_id: MsgID,
        _conn_id: ConnID,
        _datapath_buffer: Self::DatapathBuffer,
        _end_batch: bool,
    ) -> Result<()> {
        unimplemented!();
    }

    fn queue_metadata_vec(
        &mut self,
        _msg_id: MsgID,
        _conn_id: ConnID,
        _metadata_vec: Vec<Self::DatapathMetadata>,
        _end_batch: bool,
    ) -> Result<()> {
        unimplemented!();
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
        unimplemented!();
    }

    fn queue_arena_datapath_sga<'a>(
        &mut self,
        _sga: (MsgID, ConnID, ArenaDatapathSga<'a, Self>),
        _end_batch: bool,
    ) -> Result<()>
    where
        Self: Sized,
    {
        unimplemented!();
    }

    fn queue_sga_with_copy(
        &mut self,
        _buf: (MsgID, ConnID, &ArenaOrderedSga),
        _end_batch: bool,
    ) -> Result<()> {
        unimplemented!();
    }

    fn queue_single_buffer_with_copy(
        &mut self,
        _buf: (MsgID, ConnID, &[u8]),
        _end_batch: bool,
    ) -> Result<()> {
        unimplemented!();
    }

    fn queue_protobuf_message<O>(
        &mut self,
        _message: (MsgID, ConnID, &O),
        _end_batch: bool,
    ) -> Result<()>
    where
        O: protobuf::Message,
    {
        unimplemented!();
    }

    fn queue_arena_ordered_sga(
        &mut self,
        _arena_ordered_sga: (MsgID, ConnID, ArenaOrderedSga),
        _end_batch: bool,
    ) -> Result<()>
    where
        Self: Sized,
    {
        unimplemented!();
    }

    fn queue_ordered_rcsga(
        &mut self,
        _ordered_rcsga: (MsgID, ConnID, OrderedRcSga<Self>),
        _end_batch: bool,
    ) -> Result<()>
    where
        Self: Sized,
    {
        unimplemented!();
    }

    fn queue_arena_ordered_rcsga(
        &mut self,
        _arena_ordered_rcsga: (MsgID, ConnID, ArenaOrderedRcSga<Self>),
        _end_batch: bool,
    ) -> Result<()>
    where
        Self: Sized,
    {
        unimplemented!();
    }

    fn push_arena_ordered_sgas_iterator<'sge>(
        &self,
        _arena_ordered_sgas: impl Iterator<Item = Result<(MsgID, ConnID, ArenaOrderedSga<'sge>)>>,
    ) -> Result<()> {
        Ok(())
    }

    /// Send scatter-gather arrays of addresses.
    /// Args:
    /// @sgas: Vector of (msg id, connection id, raw address scatter-gather arrays) to send.
    fn push_sgas(&mut self, sgas: &[(MsgID, ConnID, Sga)]) -> Result<()>;

    /// Listen for new received packets and pop out with durations.
    fn pop_with_durations(&mut self) -> Result<Vec<(ReceivedPkt<Self>, Duration)>>
    where
        Self: Sized;

    /// Listen for new received packets and pop them out.
    fn pop(&mut self) -> Result<Vec<ReceivedPkt<Self>>>
    where
        Self: Sized;

    /// Check if any outstanding packets have timed out.
    fn timed_out(&self, time_out: Duration) -> Result<Vec<(MsgID, ConnID)>>;

    /// Checks whether input buffer is registered.
    /// Args:
    /// @buf: slice to check if address is registered or not.
    fn is_registered(&self, buf: &[u8]) -> bool;

    /// Allocate a datapath buffer with the given size and alignment.
    /// Args:
    /// @size: minimum size of buffer to be allocated.
    fn allocate(&mut self, size: usize) -> Result<Option<Self::DatapathBuffer>>;

    /// Allocate a tx buffer with MTU size (max packet size).
    fn allocate_tx_buffer(&mut self) -> Result<(Option<Self::DatapathBuffer>, usize)>;

    /// Consume a datapath buffer and returns a metadata object that owns the underlying
    /// buffer.
    /// Args:
    /// @buf: Datapath buffer object.
    fn get_metadata(&self, buf: Self::DatapathBuffer) -> Result<Option<Self::DatapathMetadata>>;

    /// Takes a buffer and recovers underlying metadata if it is refcounted.
    /// Args:
    /// @buf: Buffer.
    fn recover_metadata(&self, buf: &[u8]) -> Result<Option<Self::DatapathMetadata>>;

    /// Takes a buffer and recovers underlying metadata, along with information about whether it is
    /// currently pinned.
    /// Args:
    /// @buf: Buffer.
    fn recover_metadata_with_status(&self, _buf: &[u8]) -> Result<MetadataStatus<Self>>
    where
        Self: Sized,
    {
        unimplemented!();
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
    fn add_memory_pool(&mut self, size: usize, min_elts: usize) -> Result<Vec<MempoolID>>;

    /// Checks whether datapath has mempool of size size given (must be power of 2).
    fn has_mempool(&self, size: usize) -> bool;

    /// Register given mempool ID
    fn register_mempool(&mut self, id: MempoolID) -> Result<()>;

    /// Unregister given mempool ID
    fn unregister_mempool(&mut self, id: MempoolID) -> Result<()>;

    fn header_size(&self) -> usize;

    /// Number of cycles in a second
    fn timer_hz(&self) -> u64;

    /// Convert cycles to ns.
    fn cycles_to_ns(&self, t: u64) -> u64;

    /// Current cycles.
    fn current_cycles(&self) -> u64;

    /// Set copying threshold for serialization.
    fn set_copying_threshold(&mut self, threshold: usize);

    /// Get current copying threshold for serialization.
    fn get_copying_threshold(&self) -> usize;

    /// Sets maximum segments sent in a packet.
    fn set_max_segments(&mut self, max_entries: usize);

    /// Gets current maximum segments
    fn get_max_segments(&self) -> usize;

    /// Set inline mode (may not be available in all datapaths)
    fn set_inline_mode(&mut self, mode: InlineMode);

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
