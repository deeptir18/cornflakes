//! Welcome to cornflakes!
//!
//! This crate, cornflakes-libos, implements the networking layer for cornflakes.
//! This includes:
//!  1. An interface for datapaths to implement.
//!  2. DPDK bindings, which are used to implement the DPDK datapath.
//!  3. A DPDK based datapath.
pub mod dpdk_bindings;
pub mod dpdk_libos;
pub mod mem;
pub mod timing;
pub mod utils;

use color_eyre::eyre::{bail, Result, WrapErr};
use mem::MmapMetadata;
use std::{
    io::Write,
    net::Ipv4Addr,
    ops::{Fn, FnMut},
    slice::Iter,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use timing::HistogramWrapper;

pub type MsgID = u32;

/// Trait defining functionality any ``scatter-gather'' types should have,
/// that datapaths can access or modify about these types,
/// to be able to be able to send scattered memory packets, or receive datapath packets.
/// For transmitting packets, most systems can just use the Cornflake struct, which implements this
/// trait.
pub trait ScatterGather {
    /// Pointer type, to refer to scattered memory segments
    type Ptr: AsRef<[u8]> + PtrAttributes;

    /// A collection type over the pointer type.
    type Collection: IntoIterator<Item = Self::Ptr>;

    /// Returns ID field for this packet.
    /// Should be unique per connection.
    fn get_id(&self) -> MsgID;

    /// Sets the id field for this packet.
    fn set_id(&mut self, id: MsgID);

    /// Returns total number of memory regions represented by this packet.
    fn num_segments(&self) -> usize;

    /// Returns total number of borrowed memory regions represented by this packet.
    fn num_borrowed_segments(&self) -> usize;

    /// Amount of data in total represented by this packet.
    fn data_len(&self) -> usize;

    /// Returns an iterator over the scattered memory regions this packet represents.
    fn collection(&self) -> Self::Collection;

    /// Returns item at index
    fn index(&self, idx: usize) -> &Self::Ptr;

    /// Applies the provided closure on all of the pointer types.
    fn iter_apply(&self, consume_element: impl FnMut(&Self::Ptr) -> Result<()>) -> Result<()>;

    /// Returns a buffer where all the scatter-gather entries are copied into a contiguous array.
    fn contiguous_repr(&self) -> Vec<u8>;
}

/// Trait defining functionality any _received_ packets should have.
/// So the receiver can access address and any other relevant information.
pub trait ReceivedPacket {
    fn get_addr(&self) -> &utils::AddressInfo;

    fn get_pkt_buffer(&self) -> &[u8];

    fn get_corn_type(&self) -> CornType;

    fn len(&self) -> usize;
}

/// Whether an underlying buffer is borrowed or
/// actually owned (most likely on the heap).
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum CornType {
    Registered,
    Normal,
}

/// Must be implemented by any Ptr type referred to in the ScatterGather trait.
pub trait PtrAttributes {
    fn buf_type(&self) -> CornType;
    fn buf_size(&self) -> usize;
}

/// Represents either a borrowed piece of memory.
/// Or an owned value.
/// TODO: having this be an enum might double storage necessary for IOvecs
#[derive(Clone, PartialEq, Eq, Copy)]
pub enum CornPtr<'registered, 'normal> {
    /// Reference to some other memory (used for zero-copy send).
    Registered(&'registered [u8]),
    /// "Normal" Reference to un-registered memory.
    Normal(&'normal [u8]),
}

impl<'registered, 'normal> Default for CornPtr<'registered, 'normal> {
    fn default() -> Self {
        CornPtr::Normal(&[])
    }
}

impl<'registered, 'normal> AsRef<[u8]> for CornPtr<'registered, 'normal> {
    fn as_ref(&self) -> &[u8] {
        match self {
            CornPtr::Registered(buf) => buf,
            CornPtr::Normal(buf) => buf,
        }
    }
}

impl<'registered, 'normal> PtrAttributes for CornPtr<'registered, 'normal> {
    fn buf_type(&self) -> CornType {
        match self {
            CornPtr::Registered(_) => CornType::Registered,
            CornPtr::Normal(_) => CornType::Normal,
        }
    }

    fn buf_size(&self) -> usize {
        match self {
            CornPtr::Registered(buf) => buf.len(),
            CornPtr::Normal(buf) => buf.len(),
        }
    }
}

/// A Cornflake represents a general-purpose scatter-gather array.
/// Datapaths must be able to send and receive cornflakes.
/// TODO: might not be necessary to separately keep track of lengths.
#[derive(Clone, Eq, PartialEq)]
pub struct Cornflake<'registered, 'normal> {
    /// Id for message. If None, datapath doesn't need to keep track of per-packet timeouts.
    id: MsgID,
    /// Pointers to scattered memory segments.
    entries: Vec<CornPtr<'registered, 'normal>>,
    /// Num borrowed segments
    num_borrowed: usize,
    /// Data size
    data_size: usize,
    /// Number of total filled entries
    num_filled: usize,
}

impl<'registered, 'normal> Default for Cornflake<'registered, 'normal> {
    fn default() -> Self {
        Cornflake {
            id: 0,
            entries: Vec::new(),
            num_borrowed: 0,
            data_size: 0,
            num_filled: 0,
        }
    }
}

impl<'registered, 'normal> ScatterGather for Cornflake<'registered, 'normal> {
    /// Pointer type is reference to CornPtr.
    type Ptr = CornPtr<'registered, 'normal>;
    /// Can return an iterator over CornPtr references.
    type Collection = Vec<CornPtr<'registered, 'normal>>;

    /// Returns the id of this cornflake.
    fn get_id(&self) -> MsgID {
        self.id
    }

    /// Sets the id.
    fn set_id(&mut self, id: MsgID) {
        self.id = id;
    }

    /// Returns number of entries in this cornflake.
    fn num_segments(&self) -> usize {
        self.num_filled
    }

    /// Returns the number of borrowed memory regions in this cornflake.
    fn num_borrowed_segments(&self) -> usize {
        self.num_borrowed
    }

    /// Amount of data represented by this scatter-gather array.
    fn data_len(&self) -> usize {
        self.data_size
    }

    /// Exposes an iterator over the entries in the scatter-gather array.
    fn collection(&self) -> Self::Collection {
        let mut vec = Vec::new();
        for i in 0..self.num_filled {
            vec.push(self.entries[i].clone());
        }
        vec
    }

    /// Returns CornPtr at Index
    fn index(&self, idx: usize) -> &Self::Ptr {
        &self.entries[idx]
    }

    /// Apply an iterator to entries of the scatter-gather array, without consuming the
    /// scatter-gather array.
    fn iter_apply(&self, mut consume_element: impl FnMut(&Self::Ptr) -> Result<()>) -> Result<()> {
        for i in 0..self.num_filled {
            let entry = &self.entries[i];
            consume_element(entry).wrap_err(format!(
                "Unable to run function on pointer {} in cornflake",
                i
            ))?;
        }
        Ok(())
    }

    fn contiguous_repr(&self) -> Vec<u8> {
        let mut ret: Vec<u8> = Vec::new();
        for i in 0..self.num_filled {
            let entry = &self.entries[i];
            ret.extend_from_slice(entry.as_ref());
        }
        ret
    }
}

impl<'registered, 'normal> Cornflake<'registered, 'normal> {
    /// Returns a cornflake with this many entries.
    pub fn with_capacity(capacity: usize) -> Cornflake<'registered, 'normal> {
        Cornflake {
            id: 0,
            entries: vec![CornPtr::default(); capacity],
            num_borrowed: 0,
            data_size: 0,
            num_filled: 0,
        }
    }

    /// Adds a scatter-gather entry to this cornflake.
    /// Passes ownership of the CornPtr.
    /// Arguments:
    /// * ptr - CornPtr<'a> representing owned or borrowed memory.
    /// * length - usize representing length of memory region.
    pub fn add_entry(&mut self, ptr: CornPtr<'registered, 'normal>) {
        self.data_size += ptr.buf_size();
        if ptr.buf_type() == CornType::Registered {
            self.num_borrowed += 1;
        }
        if self.num_filled == self.entries.len() {
            self.entries.push(ptr);
        } else {
            self.entries[self.num_filled] = ptr;
        }
        self.num_filled += 1;
    }

    pub fn iter(&self) -> Iter<CornPtr<'registered, 'normal>> {
        self.entries.iter()
    }

    pub fn get(&self, idx: usize) -> Result<&CornPtr<'registered, 'normal>> {
        if idx >= self.entries.len() {
            bail!(
                "Trying to get {} idx from CornPtr, but it only has length {}.",
                idx,
                self.entries.len()
            );
        }

        Ok(&self.entries[idx])
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct DatapathMempoolOptions {
    pub payload: Vec<u8>,
    pub idx: usize, // cannot be 0
}

impl DatapathMempoolOptions {
    pub fn new(payload: Vec<u8>, idx: usize) -> Self {
        DatapathMempoolOptions {
            payload: payload,
            idx: idx,
        }
    }
}

// how do we make it such that the programmer DOESN'T need to know what is registered and what is
#[derive(Debug, PartialEq, Eq)]
pub enum RcCornPtr<'a, D>
where
    D: Datapath,
{
    RawRef(&'a [u8]),     // used for object header potentially
    RefCounted(CfBuf<D>), // used for any datapath value
}

impl<'a, D> Clone for RcCornPtr<'a, D>
where
    D: Datapath,
{
    fn clone(&self) -> Self {
        match self {
            RcCornPtr::RawRef(buf) => RcCornPtr::RawRef(buf),
            RcCornPtr::RefCounted(buf) => RcCornPtr::RefCounted(buf.clone()),
        }
    }
}

impl<'a, D> Default for RcCornPtr<'a, D>
where
    D: Datapath,
{
    fn default() -> Self {
        RcCornPtr::RawRef(&[])
    }
}

impl<'a, D> AsRef<[u8]> for RcCornPtr<'a, D>
where
    D: Datapath,
{
    fn as_ref(&self) -> &[u8] {
        match self {
            RcCornPtr::RawRef(buf) => buf,
            RcCornPtr::RefCounted(buf) => buf.as_ref(),
        }
    }
}

impl<'a, D> PtrAttributes for RcCornPtr<'a, D>
where
    D: Datapath,
{
    fn buf_type(&self) -> CornType {
        match self {
            RcCornPtr::RefCounted(_) => CornType::Registered,
            RcCornPtr::RawRef(_) => CornType::Normal,
        }
    }

    fn buf_size(&self) -> usize {
        match self {
            RcCornPtr::RefCounted(buf) => buf.len(),
            RcCornPtr::RawRef(buf) => buf.len(),
        }
    }
}

#[derive(Default, Clone, PartialEq, Eq)]
pub struct RcCornflake<'a, D>
where
    D: Datapath,
{
    /// Id for message. If None, datapath doesn't need to keep track of per-packet timeouts.
    id: MsgID,
    /// Pointers to scattered memory segments.
    entries: Vec<RcCornPtr<'a, D>>,
    /// Data size represented by this cornflake (in ttoal)
    data_size: usize,
    /// Number that refer to ref-counted regions.
    num_refcnted: usize,
    /// Number of total filled entries
    num_filled: usize,
}

impl<'a, D> RcCornflake<'a, D>
where
    D: Datapath,
{
    pub fn new() -> Self {
        RcCornflake {
            id: MsgID::default(),
            entries: Vec::default(),
            data_size: 0,
            num_refcnted: 0,
            num_filled: 0,
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        RcCornflake {
            id: MsgID::default(),
            entries: vec![RcCornPtr::RawRef(&[]); cap],
            data_size: 0,
            num_refcnted: 0,
            num_filled: 0,
        }
    }

    pub fn add_entry(&mut self, entry: RcCornPtr<'a, D>) {
        self.data_size += entry.buf_size();
        self.num_filled += 1;
        if entry.buf_type() == CornType::Registered {
            self.num_refcnted += 1;
        }
        self.entries.push(entry);
    }
}

impl<'a, D> ScatterGather for RcCornflake<'a, D>
where
    D: Datapath,
{
    /// Pointer type is reference to CornPtr.
    type Ptr = RcCornPtr<'a, D>;
    /// Can return an iterator over CornPtr references.
    type Collection = Vec<RcCornPtr<'a, D>>;

    /// Returns the id of this cornflake.
    fn get_id(&self) -> MsgID {
        self.id
    }

    /// Sets the id.
    fn set_id(&mut self, id: MsgID) {
        self.id = id;
    }

    /// Returns number of entries in this cornflake.
    fn num_segments(&self) -> usize {
        self.num_filled
    }

    /// Returns the number of borrowed memory regions in this cornflake.
    fn num_borrowed_segments(&self) -> usize {
        self.num_refcnted
    }

    /// Amount of data represented by this scatter-gather array.
    fn data_len(&self) -> usize {
        self.data_size
    }

    /// Exposes an iterator over the entries in the scatter-gather array.
    fn collection(&self) -> Self::Collection {
        let mut vec = Vec::new();
        for i in 0..self.num_filled {
            vec.push(self.entries[i].clone());
        }
        vec
    }

    /// Returns CornPtr at Index
    fn index(&self, idx: usize) -> &Self::Ptr {
        &self.entries[idx]
    }

    /// Apply an iterator to entries of the scatter-gather array, without consuming the
    /// scatter-gather array.
    fn iter_apply(&self, mut consume_element: impl FnMut(&Self::Ptr) -> Result<()>) -> Result<()> {
        for i in 0..self.num_filled {
            let entry = &self.entries[i];
            consume_element(entry).wrap_err(format!(
                "Unable to run function on pointer {} in cornflake",
                i
            ))?;
        }
        Ok(())
    }

    fn contiguous_repr(&self) -> Vec<u8> {
        let mut ret: Vec<u8> = Vec::new();
        for i in 0..self.num_filled {
            let entry = &self.entries[i];
            ret.extend_from_slice(entry.as_ref());
        }
        ret
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct CfBuf<D>
where
    D: Datapath,
{
    buf: D::DatapathPkt,
}

impl<D> CfBuf<D>
where
    D: Datapath,
{
    pub fn new(buf: D::DatapathPkt) -> Self {
        CfBuf { buf: buf }
    }

    pub fn allocate(conn: &mut D, size: usize, alignment: usize) -> Result<Self> {
        let mut buf = conn.allocate(size, alignment)?;
        // TODO: should we be allocating the ref count here?
        // Or should the "datapath" be doing it
        buf.increment_rc();
        Ok(CfBuf { buf: buf })
    }

    pub fn len(&self) -> usize {
        self.buf.as_ref().len()
    }
}

impl<D> Clone for CfBuf<D>
where
    D: Datapath,
{
    fn clone(&self) -> CfBuf<D> {
        CfBuf {
            buf: self.buf.clone(),
        }
    }
}

impl<D> Write for CfBuf<D>
where
    D: Datapath,
{
    // TODO: should this just overwrite the entire buffer?
    // Should it keep an index of what has been written of sorts?
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buf.as_mut().write(&buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<D> AsRef<[u8]> for CfBuf<D>
where
    D: Datapath,
{
    fn as_ref(&self) -> &[u8] {
        self.buf.as_ref()
    }
}

impl<D> AsMut<[u8]> for CfBuf<D>
where
    D: Datapath,
{
    fn as_mut(&mut self) -> &mut [u8] {
        self.buf.as_mut()
    }
}

/// Some datapaths have their own ref counting schema,
/// So this trait exposes ref counting over those schemes.
pub trait RefCnt {
    fn count_rc(&self) -> usize;

    fn change_rc(&mut self, amt: isize);

    fn increase_rc(&mut self, amt: usize) {
        self.change_rc(amt as isize);
    }

    fn decrease_rc(&mut self, amt: usize) {
        self.change_rc((amt as isize) * -1);
    }

    fn increment_rc(&mut self) {
        self.change_rc(1);
    }

    fn decrement_rc(&mut self) {
        self.change_rc(-1);
    }
}

/// Set of functions each datapath must implement.
/// Datapaths must be able to send a receive packets,
/// as well as optionally keep track of per-packet timeouts.
pub trait Datapath {
    /// Each datapath must expose a received packet type that implements the ScatterGather trait
    /// and the ReceivedPacket trait.
    type ReceivedPkt: ScatterGather + ReceivedPacket;
    type DatapathPkt: AsRef<[u8]> + AsMut<[u8]> + RefCnt + PartialEq + Eq + Clone + std::fmt::Debug;

    /// Send a single buffer to the specified address.
    fn push_buf(&mut self, buf: (MsgID, &[u8]), addr: utils::AddressInfo) -> Result<()>;

    /// Send a scatter-gather array to the specified address.
    fn push_sgas(&mut self, sga: &Vec<(impl ScatterGather, utils::AddressInfo)>) -> Result<()>;

    /// Receive the next packet (from any) underlying `connection`, if any.
    /// Application is responsible for freeing any memory referred to by Cornflake.
    /// None response means no packet received.
    fn pop(&mut self) -> Result<Vec<(Self::ReceivedPkt, Duration)>>;

    /// Check if any outstanding packets have timed-out.
    fn timed_out(&self, time_out: Duration) -> Result<Vec<MsgID>>;

    /// Some datapaths use specific timing functions.
    /// This provides an interface to access time.
    fn current_cycles(&self) -> u64;

    /// Number of cycles per second.
    fn timer_hz(&self) -> u64;

    /// Max scatter-gather entries.
    fn max_scatter_entries(&self) -> usize;

    /// Max packet len.
    fn max_packet_len(&self) -> usize;

    /// Register external pages.
    ///
    /// Arguments:
    /// * mmap metadata (contains the beginning, end, alignment of region)
    fn register_external_region(&mut self, metadata: &mut MmapMetadata) -> Result<()>;

    /// Unregister external pages.
    ///
    /// Arguments:
    /// * mmap metadata (contains beginning, end, alignment of region)
    fn unregister_external_region(&mut self, metadata: &MmapMetadata) -> Result<()>;

    /// For debugging purposes, get timers to print at end of execution.
    fn get_timers(&self) -> Vec<Arc<Mutex<HistogramWrapper>>>;

    /// Get destination address information from Ipv4 Address
    fn get_outgoing_addr_from_ip(&self, dst_addr: Ipv4Addr) -> Result<utils::AddressInfo>;

    /// What is the transport header size for this datapath
    fn get_header_size(&self) -> usize;

    /// Init mempools
    fn init_native_mempools(&mut self, mempools: &Vec<DatapathMempoolOptions>) -> Result<()>;

    /// Get a packet from one of the mempools.
    fn alloc_datapath_pkt(&self, mempool_id: usize) -> Result<Self::DatapathPkt>;

    /// Free a packet from the mempool
    fn free_datapath_pkt(&self, pkt: Self::DatapathPkt) -> Result<()>;

    /// For natively allocated mbufs: what is the buffer size?
    fn native_buf_size(&self) -> usize;

    fn allocate(&self, size: usize, alignment: usize) -> Result<Self::DatapathPkt>;
}

pub fn high_timeout_at_start(received: usize) -> Duration {
    match received < 200 {
        true => Duration::new(10, 0),
        false => Duration::new(0, 1000000),
    }
}

pub fn no_retries_timeout(_received: usize) -> Duration {
    Duration::new(20, 0)
}

/// For applications that want to follow a simple open-loop request processing model at the client,
/// they can implement this trait that defines how the next message to be sent is produced,
/// how received messages are processed.
pub trait ClientSM {
    type Datapath: Datapath;

    /// Server ip.
    fn server_ip(&self) -> Ipv4Addr;

    /// Generate next request to be sent and send it with the provided callback.
    fn get_next_msg(&mut self) -> Result<(MsgID, &[u8])>;

    /// What to do with a received request.
    fn process_received_msg(
        &mut self,
        sga: <<Self as ClientSM>::Datapath as Datapath>::ReceivedPkt,
        rtt: Duration,
    ) -> Result<()>;

    /// What to do when a particular message times out.
    fn msg_timeout_cb(&mut self, id: MsgID) -> Result<(MsgID, &[u8])>;

    /// Initializes any internal state with any datapath specific configuration,
    /// e.g., registering external memory.
    fn init(&mut self, connection: &mut Self::Datapath) -> Result<()>;

    fn cleanup(&mut self, connection: &mut Self::Datapath) -> Result<()>;

    fn received_so_far(&self) -> usize;

    fn run_closed_loop(
        &mut self,
        datapath: &mut Self::Datapath,
        num_pkts: u64,
        time_out: impl Fn(usize) -> Duration,
    ) -> Result<()> {
        let mut recved = 0;
        let server_ip = self.server_ip();
        let addr_info = datapath.get_outgoing_addr_from_ip(server_ip)?;

        while recved < num_pkts {
            datapath.push_buf(self.get_next_msg()?, addr_info.clone())?;
            let recved_pkts = loop {
                let pkts = datapath.pop()?;
                if pkts.len() > 0 {
                    break pkts;
                }
                for id in datapath.timed_out(time_out(self.received_so_far()))?.iter() {
                    datapath.push_buf(self.msg_timeout_cb(*id)?, addr_info.clone())?;
                }
            };

            for (pkt, rtt) in recved_pkts.into_iter() {
                let msg_id = pkt.get_id();
                self.process_received_msg(pkt, rtt).wrap_err(format!(
                    "Error in processing received response for pkt {}.",
                    msg_id
                ))?;
                recved += 1;
            }
        }
        Ok(())
    }

    /// Run open loop client
    fn run_open_loop(
        &mut self,
        datapath: &mut Self::Datapath,
        intersend_rate: u64,
        total_time: u64,
        time_out: impl Fn(usize) -> Duration,
        no_retries: bool,
    ) -> Result<()> {
        let freq = datapath.timer_hz();
        let cycle_wait = (((intersend_rate * freq) as f64) / 1e9) as u64;
        let server_ip = self.server_ip();
        let addr_info = datapath.get_outgoing_addr_from_ip(server_ip)?;
        let time_start = Instant::now();
        //let mut last_called_pop = Instant::now();

        let start = datapath.current_cycles();
        while datapath.current_cycles() < (total_time * freq + start) {
            // Send the next message
            tracing::debug!(time = ?time_start.elapsed(), "About to send next packet");
            datapath.push_buf(self.get_next_msg()?, addr_info.clone())?;
            let last_sent = datapath.current_cycles();

            while datapath.current_cycles() <= last_sent + cycle_wait {
                //tracing::debug!(last_called = ?last_called_pop.elapsed(), "Calling pop on client");
                //last_called_pop = Instant::now();
                let recved_pkts = datapath.pop()?;
                for (pkt, rtt) in recved_pkts.into_iter() {
                    let msg_id = pkt.get_id();
                    self.process_received_msg(pkt, rtt).wrap_err(format!(
                        "Error in processing received response for pkt {}.",
                        msg_id
                    ))?;
                }

                if !no_retries {
                    for id in datapath.timed_out(time_out(self.received_so_far()))?.iter() {
                        datapath.push_buf(self.msg_timeout_cb(*id)?, addr_info.clone())?;
                    }
                }
            }
        }

        Ok(())
    }
}

/// For server applications that want to follow a simple state-machine request processing model.
pub trait ServerSM {
    type Datapath: Datapath;

    /// Process incoming message, possibly mutating internal state.
    /// Then send the next message with the given callback.
    ///
    /// Arguments:
    /// * sga - Something that implements ScatterGather + ReceivedPkt; used to query the received
    /// message id, received payload, and received message header information.
    /// * send_fn - A send callback to send a response to this request back.
    fn process_requests(
        &mut self,
        sga: Vec<(
            <<Self as ServerSM>::Datapath as Datapath>::ReceivedPkt,
            Duration,
        )>,
        datapath: &mut Self::Datapath,
    ) -> Result<()>;

    /// Runs the state machine, which responds to requests in a single-threaded fashion.
    /// Loops on calling the datapath pop function, and then responds to each valid packet.
    /// Returns a result on error.
    ///
    /// Arguments:
    /// * datapath - Object that implements Datapath trait, representing a connection using the
    /// underlying networking stack.
    fn run_state_machine(&mut self, datapath: &mut Self::Datapath) -> Result<()> {
        loop {
            let pkts = datapath.pop()?;
            if pkts.len() > 0 {
                // give ownership of the received packets to the app.
                self.process_requests(pkts, datapath)?;
            }
        }
    }

    /// Initializes any internal state with any datapath specific configuration,
    /// e.g., registering external memory.
    fn init(&mut self, connection: &mut Self::Datapath) -> Result<()>;

    fn cleanup(&mut self, connection: &mut Self::Datapath) -> Result<()>;

    fn get_histograms(&self) -> Vec<Arc<Mutex<HistogramWrapper>>>;
}
