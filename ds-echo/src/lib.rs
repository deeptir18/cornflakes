//pub mod client;
pub mod cornflakes_dynamic;
pub mod server;

use color_eyre::eyre::{bail, Result};
use cornflakes_libos::{
    datapath::{Datapath, ReceivedPkt},
    ConnID, MsgID, OrderedSga, RcSga,
};
use cornflakes_utils::SimpleMessageType;
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
pub trait ServerCerealizeMessage<D>
where
    D: Datapath,
{
    // for
    type SingleBufCtx: Clone + AsRef<[u8]>;

    // new single buf context
    fn new_single_buf_ctx_vec(&self, size: usize) -> Vec<(MsgID, ConnID, Self::SingleBufCtx)>;

    /// Message type
    fn message_type(&self) -> SimpleMessageType;

    fn process_request_single_buf(
        &self,
        pkt: &ReceivedPkt<D>,
        buf: &mut Self::SingleBufCtx,
    ) -> Result<()> {
        Ok(())
    }

    fn process_request_rcsga(
        &self,
        pkt: &ReceivedPkt<D>,
        rcsga: &mut RcSga<D>,
        header_buffer: &mut [u8],
    ) -> Result<()> {
        Ok(())
    }

    fn return_rcsga(&self, pkt: &ReceivedPkt<D>) -> Result<(RcSga<D>, Vec<u8>)>;

    fn process_request_sga(
        &mut self,
        pkt: &ReceivedPkt<D>,
        sga: &mut OrderedSga,
        header_buffer: &mut [u8],
    ) -> Result<()> {
        Ok(())
    }

    fn return_sga(&self, pkt: &ReceivedPkt<D>) -> (OrderedSga, Vec<u8>);
}

/*pub trait CerealizeClient<'normal, DatapathImpl>
where
    DatapathImpl: Datapath,
{
    /// Context for serializer to build a message.
    type Ctx;

    /// Returns a new serializer.
    fn new(message_type: SimpleMessageType, field_sizes: Vec<usize>) -> Result<Self>
    where
        Self: Sized;
    /// Initializes the object header.
    fn init(&mut self, ctx: &'normal mut Self::Ctx) -> Result<()>;
    /// Returns type of message.
    fn message_type(&self) -> SimpleMessageType;
    /// payload sizes
    fn payload_sizes(&self) -> Vec<usize>;
    /// produce the payload to echo.
    fn get_msg(&self) -> Result<Vec<u8>>;
    /// check echoed payload
    fn check_echoed_payload(&self, recved_msg: &ReceivedPkt<DatapathImpl>) -> Result<()>;
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

fn init_payloads_as_vec(sizes: &Vec<usize>) -> Vec<Vec<u8>> {
    let mut ret: Vec<Vec<u8>> = Vec::with_capacity(sizes.len());
    for size in sizes.iter() {
        ret.push(vec![1u8; *size]);
    }
    ret
}

fn get_payloads_as_vec(payloads: &Vec<(*const u8, usize)>) -> Vec<Vec<u8>> {
    let ret: Vec<Vec<u8>> = payloads
        .iter()
        .map(|(ptr, size)| {
            let mut vec: Vec<u8> = Vec::with_capacity(*size);
            vec.extend_from_slice(unsafe { slice::from_raw_parts(*ptr, *size) });
            vec
        })
        .collect();
    ret
}

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
    let mut rng = thread_rng();
    for (idx, size) in actual_alloc_boundaries.iter().enumerate() {
        let payload_size = sizes[idx];
        assert!(payload_size <= *size);
        let ptr = unsafe { mmap_metadata.ptr.offset(current_offset as isize) };
        current_offset += size;
        ret.push((ptr, payload_size));

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
}*/
