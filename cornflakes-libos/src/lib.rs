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
pub mod utils;

use color_eyre::eyre::{Result, WrapErr};
use hdrhistogram::Histogram;
use mem::MmapMetadata;
use std::{
    net::Ipv4Addr,
    ops::FnMut,
    rc::Rc,
    time::{Duration, Instant},
};

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

    /// Applies the provided closure on all of the pointer types.
    fn iter_apply(&self, consume_element: impl FnMut(&Self::Ptr) -> Result<()>) -> Result<()>;

    /// Returns a buffer where all the scatter-gather entries are copied into a contiguous array.
    fn contiguous_repr(&self) -> Vec<u8>;
}

/// Trait defining functionality any _received_ packets should have.
/// So the receiver can access address and any other relevant information.
pub trait ReceivedPacket {
    fn get_addr(&self) -> utils::AddressInfo;
}

/// Whether an underlying buffer is borrowed or
/// actually owned (most likely on the heap).
pub enum CornType {
    Borrowed,
    Owned,
}

/// Must be implemented by any Ptr type referred to in the ScatterGather trait.
pub trait PtrAttributes {
    fn buf_type(&self) -> CornType;
    fn buf_size(&self) -> usize;
}

/// Represents either a borrowed piece of memory.
/// Or an owned value.
/// TODO: having this be an enum might double storage necessary for IOvecs
#[derive(Clone, PartialEq, Eq)]
pub enum CornPtr<'a> {
    /// Reference to some other memory (used for zero-copy send).
    Borrowed(&'a [u8]),
    /// "Owned" Reference to un-registered memory.
    /// TODO: does this ever need to be mutable?
    /// Or will it always be immutable?
    Owned(Rc<Vec<u8>>),
}

impl<'a> AsRef<[u8]> for CornPtr<'a> {
    fn as_ref(&self) -> &[u8] {
        match self {
            CornPtr::Borrowed(buf) => buf,
            CornPtr::Owned(buf) => buf.as_ref().as_ref(),
        }
    }
}

impl<'a> PtrAttributes for CornPtr<'a> {
    fn buf_type(&self) -> CornType {
        match self {
            CornPtr::Borrowed(_) => CornType::Borrowed,
            CornPtr::Owned(_) => CornType::Owned,
        }
    }

    fn buf_size(&self) -> usize {
        match self {
            CornPtr::Borrowed(buf) => buf.len(),
            CornPtr::Owned(buf) => buf.as_ref().len(),
        }
    }
}

/// A Cornflake represents a general-purpose scatter-gather array.
/// Datapaths must be able to send and receive cornflakes.
/// TODO: might not be necessary to separately keep track of lengths.
#[derive(Clone, Eq, PartialEq)]
pub struct Cornflake<'a> {
    /// Id for message. If None, datapath doesn't need to keep track of per-packet timeouts.
    id: MsgID,
    /// Pointers to scattered memory segments.
    entries: Vec<CornPtr<'a>>,
}

impl<'a> Default for Cornflake<'a> {
    fn default() -> Self {
        Cornflake {
            id: 0,
            entries: Vec::new(),
        }
    }
}

impl<'a> ScatterGather for Cornflake<'a> {
    /// Pointer type is reference to CornPtr.
    type Ptr = CornPtr<'a>;
    /// Can return an iterator over CornPtr references.
    type Collection = Vec<Self::Ptr>;

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
        self.entries.len()
    }

    /// Returns the number of borrowed memory regions in this cornflake.
    fn num_borrowed_segments(&self) -> usize {
        self.entries
            .iter()
            .filter(|ptr| match ptr {
                CornPtr::Borrowed(_) => true,
                CornPtr::Owned(_) => false,
            })
            .map(|_| 1)
            .sum::<usize>()
    }

    /// Amount of data represented by this scatter-gather array.
    fn data_len(&self) -> usize {
        self.collection().iter().map(|ptr| ptr.buf_size()).sum()
    }

    /// Exposes an iterator over the entries in the scatter-gather array.
    fn collection(&self) -> Self::Collection {
        self.entries.clone()
    }

    /// Apply an iterator to entries of the scatter-gather array, without consuming the
    /// scatter-gather array.
    fn iter_apply(&self, mut consume_element: impl FnMut(&Self::Ptr) -> Result<()>) -> Result<()> {
        for (i, entry) in self.entries.iter().enumerate() {
            consume_element(&entry).wrap_err(format!(
                "Unable to run function on pointer {} in cornflake",
                i
            ))?;
        }
        Ok(())
    }

    fn contiguous_repr(&self) -> Vec<u8> {
        let mut ret: Vec<u8> = Vec::new();
        for entry in self.entries.iter() {
            ret.extend_from_slice(entry.as_ref());
        }
        ret
    }
}

impl<'a> Cornflake<'a> {
    /// Adds a scatter-gather entry to this cornflake.
    /// Passes ownership of the CornPtr.
    /// Arguments:
    /// * ptr - CornPtr<'a> representing owned or borrowed memory.
    /// * length - usize representing length of memory region.
    pub fn add_entry(&mut self, ptr: CornPtr<'a>) {
        self.entries.push(ptr);
    }
}

/// Set of functions each datapath must implement.
/// Datapaths must be able to send a receive packets,
/// as well as optionally keep track of per-packet timeouts.
pub trait Datapath {
    /// Each datapath must expose a received packet type that implements the ScatterGather trait
    /// and the ReceivedPacket trait.
    type ReceivedPkt: ScatterGather + ReceivedPacket;

    /// Send a scatter-gather array to the specified address.
    fn push_sga(&mut self, sga: impl ScatterGather, addr: Ipv4Addr) -> Result<()>;

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
    fn register_external_region(&mut self, metadata: MmapMetadata) -> Result<()>;

    /// Unregister external pages.
    ///
    /// Arguments:
    /// * mmap metadata (contains beginning, end, alignment of region)
    fn unregister_external_region(&mut self, metadata: MmapMetadata) -> Result<()>;
}

/// For applications that want to follow a simple open-loop request processing model at the client,
/// they can implement this trait that defines how the next message to be sent is produced,
/// how received messages are processed.
pub trait ClientSM {
    type Datapath: Datapath;
    type OutgoingMsg: ScatterGather;

    /// Server ip.
    fn server_ip(&self) -> Ipv4Addr;

    /// Generate next request to be sent and send it with the provided callback.
    fn send_next_msg(&mut self, send_fn: impl FnMut(Self::OutgoingMsg) -> Result<()>)
        -> Result<()>;

    /// What to do with a received request.
    fn process_received_msg(
        &mut self,
        sga: <<Self as ClientSM>::Datapath as Datapath>::ReceivedPkt,
        rtt: Duration,
    ) -> Result<()>;

    /// What to do when a particular message times out.
    fn msg_timeout_cb(
        &mut self,
        id: MsgID,
        send_fn: impl FnMut(Self::OutgoingMsg) -> Result<()>,
    ) -> Result<()>;

    /// Initializes any internal state with any datapath specific configuration,
    /// e.g., registering external memory.
    fn init(&mut self, connection: &mut Self::Datapath) -> Result<()>;

    fn run_closed_loop(
        &mut self,
        datapath: &mut Self::Datapath,
        num_pkts: u64,
        time_out: Duration,
    ) -> Result<()> {
        let mut recved = 0;
        let server_ip = self.server_ip();

        while recved < num_pkts {
            self.send_next_msg(|sga| datapath.push_sga(sga, server_ip))?;
            let recved_pkts = loop {
                let pkts = datapath.pop()?;
                if pkts.len() > 0 {
                    break pkts;
                }
                for id in datapath.timed_out(time_out)?.iter() {
                    self.msg_timeout_cb(*id, |sga| datapath.push_sga(sga, server_ip))?;
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
        time_out: Duration,
    ) -> Result<()> {
        let freq = datapath.timer_hz();
        let cycle_wait = (((intersend_rate * freq) as f64) / 1e9) as u64;
        let server_ip = self.server_ip();
        let time_start = Instant::now();
        let mut last_called_pop = Instant::now();

        let start = datapath.current_cycles();
        while datapath.current_cycles() < (total_time * freq + start) {
            // Send the next message
            tracing::debug!(time = ?time_start.elapsed(), "About to send next packet");
            self.send_next_msg(|sga| datapath.push_sga(sga, server_ip))?;
            let last_sent = datapath.current_cycles();

            while datapath.current_cycles() <= last_sent + cycle_wait {
                tracing::debug!(last_called = ?last_called_pop.elapsed(), "Calling pop on client");
                last_called_pop = Instant::now();
                let recved_pkts = datapath.pop()?;
                for (pkt, rtt) in recved_pkts.into_iter() {
                    let msg_id = pkt.get_id();
                    self.process_received_msg(pkt, rtt).wrap_err(format!(
                        "Error in processing received response for pkt {}.",
                        msg_id
                    ))?;
                }

                for id in datapath.timed_out(time_out)?.iter() {
                    self.msg_timeout_cb(*id, |sga| datapath.push_sga(sga, server_ip))?;
                }
            }
        }
        Ok(())

        // send first message
        // while time < total_time:
        // while time < time to send next packet:
        //  try:
        //  datapath.pop()
        //  check:
        //  has anything timed out?
        //      if so, call the callback on all the id's that have timed out.
        //
    }
}

/// For server applications that want to follow a simple state-machine request processing model.
pub trait ServerSM {
    type Datapath: Datapath;
    type OutgoingMsg: ScatterGather;

    /// Process incoming message, possibly mutating internal state.
    /// Then send the next message with the given callback.
    ///
    /// Arguments:
    /// * sga - Something that implements ScatterGather + ReceivedPkt; used to query the received
    /// message id, received payload, and received message header information.
    /// * send_fn - A send callback to send a response to this request back.
    fn process_request(
        &mut self,
        sga: &<<Self as ServerSM>::Datapath as Datapath>::ReceivedPkt,
        send_fn: impl FnMut(Self::OutgoingMsg, Ipv4Addr) -> Result<()>,
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
            for (pkt, _) in pkts.iter() {
                self.process_request(pkt, |sga, ip_addr| {
                    datapath
                        .push_sga(sga, ip_addr)
                        .wrap_err("Failed to send pkt response.")
                })?;
            }
        }
    }

    /// Initializes any internal state with any datapath specific configuration,
    /// e.g., registering external memory.
    fn init(&mut self, connection: &mut Self::Datapath) -> Result<()>;
}

pub trait RTTHistogram {
    fn get_histogram(&mut self) -> &mut Histogram<u64>;

    fn add_latency(&mut self, val: u64) -> Result<()> {
        tracing::debug!(val_ns = val, "Adding latency to hist");
        self.get_histogram().record(val)?;
        Ok(())
    }

    fn dump(&mut self, msg: &str) {
        tracing::info!(
            msg,
            p5_ns = self.get_histogram().value_at_quantile(0.05),
            p25_ns = self.get_histogram().value_at_quantile(0.25),
            p50_ns = self.get_histogram().value_at_quantile(0.5),
            p75_ns = self.get_histogram().value_at_quantile(0.75),
            p95_ns = self.get_histogram().value_at_quantile(0.95),
            pkts_sent = self.get_histogram().len(),
            min_ns = self.get_histogram().min(),
            max_ns = self.get_histogram().max(),
            avg_ns = ?self.get_histogram().mean(),
        );
    }
}
