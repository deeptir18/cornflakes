use super::{serialize::Serializable, utils::AddressInfo, ConnID, MsgID, RcSga, Sga};
use color_eyre::eyre::Result;
use std::{io::Write, net::Ipv4Addr, time::Duration};

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
}

/// Functionality accessible to higher level application on top of datapath metadata objects.
pub trait MetadataOps {
    fn offset(&self) -> usize;

    fn data_len(&self) -> usize;

    fn set_offset(&mut self, off: usize) -> Result<()>;

    fn set_data_len(&mut self, data_len: usize) -> Result<()>;
}

pub trait Datapath {
    /// Mutable buffer type that can be written into.
    type DatapathBuffer: AsMut<[u8]> + Write + PartialEq + Eq + std::fmt::Debug;

    /// Metadata that wraps around a datapath buffer.
    type DatapathMetadata: AsRef<[u8]> + PartialEq + Eq + Clone + std::fmt::Debug + MetadataOps;

    /// Any per thread context required by the datapath per thread.
    type PerThreadContext: Send + Clone;

    /// Global context.
    type GlobalContext: Send + Clone;

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
        our_ip: Option<Ipv4Addr>,
    ) -> Result<Self::DatapathSpecificParams>;

    /// Given a remote IP address, compute a source IP and port for each queue
    /// Such that receiving a packet with that IP and port as the destination,
    /// and remote_ip as source, will be hashed to that queue ID.
    fn compute_affinity(
        datapath_params: &Self::DatapathSpecificParams,
        num_queues: usize,
        remote_ip: Ipv4Addr,
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
    ) -> Result<(Self::GlobalContext, Vec<Self::PerThreadContext>)>;

    /// Any global teardown required by this datapath.
    /// Args:
    /// @global_context: Global context object returned by global_init.
    fn global_teardown(context: Self::GlobalContext) -> Result<()>;

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
    fn push_buffers_with_copy(&mut self, pkts: Vec<(MsgID, ConnID, &[u8])>) -> Result<()>;

    /// Echo the specified packet back to the  source.
    /// Args:
    /// @pkts: Vector of received packet objects to echo back.
    fn echo(&mut self, pkts: Vec<ReceivedPkt<Self>>) -> Result<()>
    where
        Self: Sized;

    /// Serialize and send serializable objects.
    /// Args:
    /// @objects: Vector of (msg id, connection id, serializable objects) to send.
    fn serialize_and_send(
        &mut self,
        objects: &Vec<(MsgID, ConnID, impl Serializable<Self>)>,
    ) -> Result<()>
    where
        Self: Sized;

    /// Send as a reference counted scatter-gather array.
    /// Args:
    /// @rc_sgas: Vector of (msg id, connection id, reference counted scatter-gather arrays) to send.
    fn push_rc_sgas(&mut self, rc_sgas: &Vec<(MsgID, ConnID, RcSga<Self>)>) -> Result<()>
    where
        Self: Sized;

    /// Send scatter-gather arrays of addresses.
    /// Args:
    /// @sgas: Vector of (msg id, connection id, raw address scatter-gather arrays) to send.
    fn push_sgas(&mut self, sgas: &Vec<(MsgID, ConnID, Sga)>) -> Result<()>;

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
    /// @alignment: alignment size to align up to.
    fn allocate(&mut self, size: usize, alignment: usize) -> Result<Option<Self::DatapathBuffer>>;

    /// Consume a datapath buffer and returns a metadata object that owns the underlying
    /// buffer.
    /// Args:
    /// @buf: Datapath buffer object.
    fn get_metadata(&self, buf: Self::DatapathBuffer) -> Result<Option<Self::DatapathMetadata>>;

    /// Elastically add a memory pool with a particular size.
    /// Will add a new region of memory registered with the NIC.
    /// Args:
    /// @size: element size
    /// @min_elts: minimum number of elements in the memory pool.
    fn add_memory_pool(&mut self, size: usize, min_elts: usize) -> Result<()>;

    /// Convert cycles to ns.
    fn cycles_to_ns(&self, t: u64) -> u64;

    /// Current cycles.
    fn current_cycles(&self) -> u64;
}
