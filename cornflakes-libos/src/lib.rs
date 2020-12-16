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

use color_eyre::eyre::{Result, WrapErr};
use std::{net::Ipv4Addr, ops::FnMut, time::Duration};

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

    // Applies the provided closure on all of the pointer types.
    fn iter_apply(&self, consume_element: impl FnMut(&Self::Ptr) -> Result<()>) -> Result<()>;
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
    /// Owned heap memory (typically for receive path).
    Owned(Box<[u8]>),
}

impl<'a> AsRef<[u8]> for CornPtr<'a> {
    fn as_ref(&self) -> &[u8] {
        match self {
            CornPtr::Borrowed(buf) => buf,
            CornPtr::Owned(buf) => buf.as_ref(),
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
    /// Each datapath must expose a received packet type that implements the ScatterGather trait.
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
}

/// For applications that want to follow a simple open-loop request processing model at the client,
/// they can implement this trait that defines how the next message to be sent is produced,
/// how received messages are processed.
pub trait ClientSM {
    type T: ScatterGather;
    /// Server ip.
    fn server_ip(&self) -> Ipv4Addr;

    /// Generate next request to be sent.
    fn generate_next_sga(&mut self) -> Self::T;

    /// What to do with a received request.
    /// Passes ownership of the underlying data to this object.
    fn process_cornflake(&mut self, sga: Cornflake) -> Result<()>;
}

/// For applications that send requests to the server at a constant rate,
/// they can use the open loop functionality defined here to implement their app.
pub fn open_loop_client(
    _datapath: &mut impl Datapath,
    _client: &mut impl ClientSM,
    _intersend_rate: u64,
    _total_time: u64,
) -> Result<()> {
    unimplemented!();
}
