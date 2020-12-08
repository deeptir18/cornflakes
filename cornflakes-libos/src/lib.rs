//! Welcome to cornflakes!
//!
//! This crate, cornflakes-libos, implements the networking layer for cornflakes.
//! This includes:
//!  1. An interface for datapaths to implement.
//!  2. DPDK bindings, which are used to implement the DPDK datapath.
//!  3. A DPDK based datapath.
pub mod dpdk_bindings;
pub mod dpdk_libos;
pub mod utils;

use color_eyre::eyre::Result;
use core::slice::Iter;
use std::{net::Ipv4Addr, time::Duration};

pub type MsgID = u32;

/// Represents either a borrowed piece of memory.
/// Or an owned value.
pub enum CornPtr<'a> {
    /// Reference to some other memory (used for zero-copy send).
    Borrowed(&'a [u8]),
    /// Owned heap memory (typically for receive path).
    Owned(Box<[u8]>),
}

/// A Cornflake represents a scatter-gather array.
/// Datapaths must be able to send and receive cornflakes.
/// TODO: might not be necessary to separately keep track of lengths.
pub struct Cornflake<'a> {
    /// Id for message. If None, datapath doesn't need to keep track of per-packet timeouts.
    id: MsgID,
    /// Pointers to scattered memory segments.
    entries: Vec<(CornPtr<'a>, usize)>,
}

impl<'a> Default for Cornflake<'a> {
    fn default() -> Self {
        Cornflake {
            id: 0,
            entries: Vec::new(),
        }
    }
}

impl<'a> Cornflake<'a> {
    /// Returns the id of this cornflake.
    pub fn get_id(&self) -> MsgID {
        self.id
    }

    /// Sets the id.
    pub fn set_id(&mut self, id: MsgID) {
        self.id = id;
    }

    /// Returns number of entries in this cornflake.
    pub fn num_entries(&self) -> usize {
        self.entries.len()
    }

    /// Returns the number of scatter-gathers needed to represent this data structure.
    /// Number of separate borrowed memory regions, plus 1 for the header.
    pub fn num_scattered_entries(&self) -> usize {
        self.entries
            .iter()
            .filter(|(ptr, _)| match ptr {
                CornPtr::Borrowed(_) => true,
                CornPtr::Owned(_) => false,
            })
            .map(|_| 1)
            .sum::<usize>()
            + 1
    }

    /// Adds a scatter-gather entry to this cornflake.
    /// Passes ownership of the CornPtr.
    /// Arguments:
    /// * ptr - CornPtr<'a> representing owned or borrowed memory.
    /// * length - usize representing length of memory region.
    pub fn add_scatter(&mut self, ptr: CornPtr<'a>, length: usize) {
        self.entries.push((ptr, length));
    }

    /// Amount of data represented by this scatter-gather array.
    pub fn data_len(&self) -> usize {
        self.entries.iter().map(|(_, len)| len).sum()
    }

    /// Exposes an iterator over the entries in the scatter-gather array.
    pub fn iter(&self) -> Iter<(CornPtr<'a>, usize)> {
        self.entries.iter()
    }
}

/// Set of functions each datapath must implement.
/// Datapaths must be able to send a receive packets,
/// as well as optionally keep track of per-packet timeouts.
pub trait Datapath {
    /// Send a scatter-gather array to the specified address.
    fn push_sga(&mut self, sga: &Cornflake, addr: Ipv4Addr) -> Result<()>;

    /// Receive the next packet (from any) underlying `connection`, if any.
    /// Application is responsible for freeing any memory referred to by Cornflake.
    /// None response means no packet received.
    fn pop(&mut self) -> Result<Vec<(Cornflake, Duration)>>;

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
}

/// For applications that want to follow a simple open-loop request processing model at the client,
/// they can implement this trait that defines how the next message to be sent is produced,
/// how received messages are processed.
pub trait ClientSM {
    /// Server ip.
    fn server_ip(&self) -> Ipv4Addr;

    /// Generate next request to be sent.
    fn generate_next_sga(&mut self) -> &Cornflake;

    /// What to do with a received request.
    /// Passes ownership of the underlying data to this object.
    fn process_cornflake(&mut self, sga: Cornflake) -> Result<()>;
}

/// For applications that send requests to the server at a constant rate,
/// they can use the open loop functionality defined here to implement their app.
pub fn open_loop_client(
    datapath: &mut impl Datapath,
    client: &mut impl ClientSM,
    intersend_rate: u64,
    total_time: u64,
) -> Result<()> {
    unimplemented!();
}
