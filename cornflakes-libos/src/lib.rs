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
use std::{net::Ipv4Addr, time::Duration};

const MAX_ENTRIES: u8 = 64;
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
pub struct Cornflake<'a> {
    /// Id for message. If None, datapath doesn't need to keep track of per-packet timeouts.
    id: MsgID,
    /// Pointers to scattered memory segments.
    entries: Vec<CornPtr<'a>>,
    /// Lenghts of corresponding segments
    lengths: Vec<u32>,
}

impl<'a> Default for Cornflake<'a> {
    fn default() -> Self {
        Cornflake {
            id: 0,
            entries: Vec::new(),
            lengths: Vec::new(),
        }
    }
}

impl<'a> Cornflake<'a> {
    pub fn get_id(&self) -> MsgID {
        self.id
    }

    pub fn set_id(&mut self, id: MsgID) {
        self.id = id;
    }

    pub fn num_entries(&self) -> usize {
        self.entries.len()
    }

    pub fn add_scatter(&mut self, ptr: CornPtr<'a>, length: u32) {
        self.entries.push(ptr);
        self.lengths.push(length);
    }

    pub fn get_entries(&self) -> &Vec<CornPtr<'a>> {
        &self.entries
    }

    pub fn data_len(&self) -> usize {
        self.lengths.iter().sum::<u32>() as usize
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
    fn pop(&mut self) -> Result<Option<(Cornflake, Duration)>>;

    /// Check if any outstanding packets have timed-out.
    /// TODO: should this return a vec?
    fn timed_out(&self, time_out: Duration) -> Result<Vec<MsgID>>;

    /// Some datapaths use specific timing functions.
    /// This provides an interface to access time.
    fn current_cycles(&self) -> u64;

    /// Number of cycles per second.
    fn timer_hz(&self) -> u64;
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
