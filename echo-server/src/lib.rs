pub mod capnproto;
pub mod client;
pub mod cornflakes;
pub mod flatbuffers;
pub mod protobuf;
pub mod server;

// TODO: though capnpc 0.14^ supports generating nested namespace files
// there seems to be a bug in the code generation, so must include it at crate root
mod echo_capnp {
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(dead_code)]
    #![allow(improper_ctypes)]
    include!(concat!(env!("OUT_DIR"), "/echo_capnp.rs"));
}

use color_eyre::eyre::{bail, Result};
use cornflakes_libos::{mem::MmapMetadata, Cornflake, Datapath, ScatterGather};
use cornflakes_utils::SimpleMessageType;
use memmap::MmapMut;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::{io::Write, iter, iter::repeat, slice};

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
    /// TODO: does Ctx need to implement any trait?
    type Ctx;
    /// Message type
    fn message_type(&self) -> SimpleMessageType;
    /// Echo the received message into a corresponding scatter-gather array.
    fn process_msg<'registered, 'normal: 'registered>(
        &self,
        recved_msg: &'registered DatapathImpl::ReceivedPkt,
        ctx: &'normal mut Self::Ctx,
    ) -> Result<Cornflake<'registered, 'normal>>;

    fn new_context(&self) -> Self::Ctx;
}

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
        message_type: SimpleMessageType,
        field_sizes: Vec<usize>,
        mmap_metadata: MmapMetadata,
        mmap_mut: &mut MmapMut,
    ) -> Result<Self>
    where
        Self: Sized;
    /// Initializes the object header.
    fn init(&mut self, ctx: &'normal mut Self::Ctx);
    /// Returns type of message.
    fn message_type(&self) -> SimpleMessageType;
    /// payload sizes
    fn payload_sizes(&self) -> Vec<usize>;
    /// produce the sga to echo
    fn get_sga(&self) -> Result<Self::OutgoingMsg>;
    /// check echoed payload
    fn check_echoed_payload(&self, recved_msg: &DatapathImpl::ReceivedPkt) -> Result<()>;
    /// new context
    fn new_context(&self) -> Self::Ctx;
}

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
            let num_elts = 2_usize.pow(depth as u32);
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

fn init_payloads_as_vec(sizes: &Vec<usize>) -> Vec<Vec<u8>> {
    let mut ret: Vec<Vec<u8>> = Vec::with_capacity(sizes.len());
    for size in sizes.iter() {
        ret.push(vec![1u8; *size]);
    }
    ret
}

fn init_payloads(
    sizes: &Vec<usize>,
    mmap_metadata: &MmapMetadata,
    _mmap_mut: &mut MmapMut,
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
    let mut rng = thread_rng();
    for size in actual_alloc_boundaries.iter() {
        let ptr = unsafe { mmap_metadata.ptr.offset(current_offset as isize) };
        current_offset += size;
        ret.push((ptr, *size));

        // write in some random garbage to these values
        let chars: String = iter::repeat(())
            .map(|()| rng.sample(Alphanumeric))
            .map(char::from)
            .take(*size)
            .collect();
        let mut slice = unsafe { slice::from_raw_parts_mut(ptr as *mut u8, *size) };
        slice.write_all(chars.as_str().as_bytes())?;
    }
    Ok(ret)
}
