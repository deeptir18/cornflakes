//pub mod capnproto;
pub mod client;
pub mod cornflakes_dynamic;
//pub mod cornflakes_fixed;
//pub mod flatbuffers;
//pub mod protobuf;
pub mod server;

// TODO: though capnpc 0.14^ supports generating nested namespace files
// there seems to be a bug in the code generation, so must include it at crate root
mod ds_query_capnp {
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(dead_code)]
    #![allow(improper_ctypes)]
    include!(concat!(env!("OUT_DIR"), "/ds_query_capnp.rs"));
}

use color_eyre::eyre::{bail, Result};
use cornflakes_libos::{mem::MmapMetadata, Cornflake, Datapath, ScatterGather};
use cornflakes_utils::SimpleMessageType;
use std::{io::Write, iter::repeat, slice};

const ALIGN_SIZE: usize = 64;

#[derive(Debug, Eq, PartialEq)]
pub enum EchoMode {
    Client,
    Server,
}

impl std::str::FromStr for EchoMode {
    type Err = color_eyre::eyre::Error;
    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "client" | "CLIENT" | "Client" => EchoMode::Client,
            "server" | "SERVER" | "Server" => EchoMode::Server,
            x => bail!("{} EchoMode unknown", x),
        })
    }
}

/// Trait that encompasses methods for the server to deserialize and echo back messages.
pub trait CerealizeMessage<DatapathImpl>
where
    DatapathImpl: Datapath,
{
    /// Serialization library might require long-lived context to serialize an object, so it
    /// doesn't go out of scope.
    type Ctx;
    /// Returns a new server serializer.
    fn new(
        message_type: SimpleMessageType,
        field_sizes: Vec<usize>,
        mmap_metadata: MmapMetadata,
        deserialize_received: bool, // whether the server should deserialize the incoming message
        use_native_buffers: bool,   // whether to allocate external memory or not
        prepend_header: bool, // whether the header (and packet header) should be prepended to the first segment in the data structure
    ) -> Result<Self>
    where
        Self: Sized;
    /// Message type that this server returns.
    fn message_type(&self) -> SimpleMessageType;
    /// Send back message type in a scatter-gather array.
    fn process_msg<'registered, 'normal: 'registered>(
        &self,
        recved_msg: &'registered DatapathImpl::ReceivedPkt,
        ctx: &'normal mut Self::Ctx,
        transport_header: usize,
    ) -> Result<Cornflake<'registered, 'normal>>;

    fn new_context(&self, conn: &DatapathImpl) -> Result<Self::Ctx>;

    // if necessary, initialize any datapath-specific data for this serialization method
    fn init_datapath(&self, conn: &mut DatapathImpl) -> Result<()>;
}

/// the client eventually ALSO needs to be able to transmit complex data structures.
pub trait CerealizeClient<'normal, DatapathImpl>
where
    DatapathImpl: Datapath,
{
    /// Context for serializer to build a message.
    type Ctx;
    /// Outgoing Message
    type OutgoingMsg: ScatterGather + Clone;
    /// Returns a new serializer.
    fn new(
        our_message_type: SimpleMessageType,
        server_message_type: SimpleMessageType,
        field_sizes: Vec<usize>,
        server_field_sizes: Vec<usize>,
        mmap_metadata: MmapMetadata,
    ) -> Result<Self>
    where
        Self: Sized;
    /// Initializes the object header.
    fn init(&mut self, ctx: &'normal mut Self::Ctx) -> Result<()>;
    /// Returns server message type. Can be different if client is sending "single" payload but
    /// expecting actual message in return (for microbenchmark).
    fn server_message_type(&self) -> SimpleMessageType;
    /// Returns type of message.
    fn our_message_type(&self) -> SimpleMessageType;
    /// produce the sga to echo
    fn get_sga(&self) -> Result<Self::OutgoingMsg>;
    /// check echoed payload: based on server message type
    fn check_echoed_payload(&self, recved_msg: &DatapathImpl::ReceivedPkt) -> Result<()>;
    /// new context
    fn new_context(&self) -> Self::Ctx;
}

// Given a message type, and a total size, returns a vector representing all the data fields
// (leaves) and how large they should be
pub fn get_equal_fields(message_type: SimpleMessageType, size: usize) -> Vec<usize> {
    match message_type {
        SimpleMessageType::Single => {
            vec![size]
        }
        SimpleMessageType::List(list_elts) => {
            let divided_size: usize = size / list_elts;
            let elts: Vec<usize> = repeat(divided_size).take(list_elts).collect();
            elts
        }
        SimpleMessageType::Tree(depth) => {
            let num_elts = 2_usize.pow(depth as u32 + 1);
            let divided_size: usize = size / num_elts;
            let elts: Vec<usize> = repeat(divided_size).take(num_elts).collect();
            elts
        }
    }
}

fn align_up(x: usize, align_size: usize) -> usize {
    // find value aligned up to align_size
    let divisor = x / align_size;
    if (divisor * align_size) < x {
        return (divisor + 1) * align_size;
    } else {
        assert!(divisor * align_size == x);
        return x;
    }
}

// Initializes a payload of size size
// Here, payload is alphabet repeated.
fn init_payload(size: usize) -> Vec<u8> {
    let alpha = "abcdefghijklmnopqrstuvwxyz";
    let repeats = (size as f64 / 26.0).ceil() as usize;
    let repeated = alpha.repeat(repeats);
    (repeated.as_bytes()[0..size]).to_vec()
}

fn init_payloads_as_vec(sizes: &Vec<usize>) -> Vec<Vec<u8>> {
    sizes.iter().map(|size| init_payload(*size)).collect()
}

/*fn get_payloads_as_vec(payloads: &Vec<(*const u8, usize)>) -> Vec<Vec<u8>> {
    let ret: Vec<Vec<u8>> = payloads
        .iter()
        .map(|(ptr, size)| {
            let mut vec: Vec<u8> = Vec::with_capacity(*size);
            vec.extend_from_slice(unsafe { slice::from_raw_parts(*ptr, *size) });
            vec
        })
        .collect();
    ret
}*/

// Initialize payloads in this mmap data.
fn init_payloads(
    sizes: &Vec<usize>,
    mmap_metadata: &MmapMetadata,
) -> Result<Vec<(*const u8, usize)>> {
    let actual_alloc_boundaries: Vec<usize> =
        sizes.iter().map(|x| align_up(*x, ALIGN_SIZE)).collect();
    let total: usize = actual_alloc_boundaries.iter().sum();
    if total > mmap_metadata.length {
        bail!(
            "Cannot init payloads: Alloc_size: {} > metadata length: {}.",
            total,
            mmap_metadata.length
        );
    }
    let mut ret: Vec<(*const u8, usize)> = Vec::new();
    let mut current_offset = 0;
    for size in actual_alloc_boundaries.iter() {
        let ptr = unsafe { mmap_metadata.ptr.offset(current_offset as isize) };
        current_offset += size;
        ret.push((ptr, *size));

        // write abcd pattern into this
        let payload = init_payload(*size);
        let mut slice = unsafe { slice::from_raw_parts_mut(ptr as *mut u8, *size) };
        slice.write_all(payload.as_slice())?;
    }
    Ok(ret)
}
