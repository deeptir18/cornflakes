pub mod capnproto;
pub mod cornflakes_dynamic;
pub mod echo;
pub mod flatbuffers;
pub mod protobuf;
pub mod run_datapath;
mod echo_capnp {
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(dead_code)]
    #![allow(improper_ctypes)]
    include!(concat!(env!("OUT_DIR"), "/echo_capnp.rs"));
}

use byteorder::{BigEndian, ByteOrder};
use color_eyre::eyre::{bail, Result};
use cornflakes_libos::{
    datapath::{Datapath, ReceivedPkt},
    state_machine::client::ClientSM,
    timing::{ManualHistogram, SizedManualHistogram},
    utils::AddressInfo,
    MsgID,
};
use cornflakes_utils::{SimpleMessageType, TreeDepth};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::{iter, iter::repeat, marker::PhantomData};

const ALIGN_SIZE: usize = 64;
pub const REQ_TYPE_SIZE: usize = 4;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum MsgType {
    Single,
    List(u16),
    Tree(u16),
}

fn read_message_type<D: Datapath>(packet: &ReceivedPkt<D>) -> Result<SimpleMessageType> {
    let buf = &packet.seg(0).as_ref();
    let msg_type = &buf[0..2];
    let size = &buf[2..4];

    match (BigEndian::read_u16(msg_type), BigEndian::read_u16(size)) {
        (0, 0) => Ok(SimpleMessageType::Single),
        (1, size) => Ok(SimpleMessageType::List(size as _)),
        (2, size) => {
            let tree_depth = match size {
                1 => TreeDepth::One,
                2 => TreeDepth::Two,
                3 => TreeDepth::Three,
                4 => TreeDepth::Four,
                5 => TreeDepth::Five,
                _ => {
                    bail!("Tree depth of 6 or greater not supported");
                }
            };
            Ok(SimpleMessageType::Tree(tree_depth))
        }
        _ => {
            bail!("unrecognized message type for ds echo app.");
        }
    }
}

/// Writes message type into first four bytes of provided buffer.
fn write_message_type(msg_type: SimpleMessageType, buf: &mut [u8]) {
    match msg_type {
        SimpleMessageType::Single => {
            BigEndian::write_u16(&mut buf[0..2], 0);
            BigEndian::write_u16(&mut buf[2..4], 0);
        }
        SimpleMessageType::List(size) => {
            BigEndian::write_u16(&mut buf[0..2], 1);
            BigEndian::write_u16(&mut buf[2..4], size as _);
        }
        SimpleMessageType::Tree(depth) => {
            let size = match depth {
                TreeDepth::One => 1,
                TreeDepth::Two => 2,
                TreeDepth::Three => 3,
                TreeDepth::Four => 4,
                TreeDepth::Five => 5,
            };
            BigEndian::write_u16(&mut buf[0..2], 2);
            BigEndian::write_u16(&mut buf[2..4], size);
        }
    }
}

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

pub trait ClientCerealizeMessage<D>
where
    D: Datapath,
{
    fn new() -> Self
    where
        Self: Sized;

    fn check_echoed_payload(
        &self,
        recved_msg: &ReceivedPkt<D>,
        bytes_to_check: (SimpleMessageType, &Vec<Vec<u8>>),
        datapath: &D,
    ) -> Result<bool>;

    fn get_serialized_bytes(
        ty: SimpleMessageType,
        input: &Vec<Vec<u8>>,
        datapath: &D,
    ) -> Result<Vec<u8>>;
}

pub struct EchoClient<C, D>
where
    C: ClientCerealizeMessage<D>,
    D: Datapath,
{
    cerealizer: C,
    last_sent_id: MsgID,
    received: usize,
    num_retried: usize,
    num_timed_out: usize,
    bytes_to_check: Vec<(SimpleMessageType, Vec<Vec<u8>>)>,
    bytes_to_transmit: Vec<Vec<u8>>,
    server_addr: AddressInfo,
    rtts: ManualHistogram,
    sized_rtts: SizedManualHistogram,
    recording_size_rtts: bool,
    _datapath: PhantomData<D>,
}

impl<C, D> EchoClient<C, D>
where
    C: ClientCerealizeMessage<D>,
    D: Datapath,
{
    pub fn new(
        server_addr: AddressInfo,
        sizes: Vec<(SimpleMessageType, Vec<usize>)>,
        max_num_requests: usize,
        datapath: &D,
    ) -> Result<EchoClient<C, D>> {
        let bytes_to_check: Vec<(SimpleMessageType, Vec<Vec<u8>>)> = sizes
            .into_iter()
            .map(|(t, size_vec)| (t, init_payloads(size_vec)))
            .collect();
        let serialized_bytes: Result<Vec<Vec<u8>>> = bytes_to_check
            .iter()
            .map(|(t, bytes)| {
                let mut type_buf: Vec<u8> = vec![0u8; REQ_TYPE_SIZE];
                write_message_type(*t, &mut type_buf.as_mut_slice());
                let mut serialized_bytes = C::get_serialized_bytes(*t, bytes, datapath)?;
                type_buf.append(&mut serialized_bytes);
                Ok(type_buf)
            })
            .collect();
        tracing::debug!("Serialized bytes: {:?}", serialized_bytes);
        Ok(EchoClient {
            cerealizer: C::new(),
            last_sent_id: 0,
            received: 0,
            num_retried: 0,
            num_timed_out: 0,
            bytes_to_check: bytes_to_check,
            bytes_to_transmit: serialized_bytes?,
            server_addr: server_addr,
            rtts: ManualHistogram::new(max_num_requests),
            sized_rtts: SizedManualHistogram::new(16384, max_num_requests),
            recording_size_rtts: false,
            _datapath: PhantomData,
        })
    }

    fn get_bytes_to_check(&self, msg_id: MsgID) -> (SimpleMessageType, &Vec<Vec<u8>>) {
        (
            self.bytes_to_check[msg_id as usize % self.bytes_to_check.len()].0,
            &self.bytes_to_check[msg_id as usize % self.bytes_to_check.len()].1,
        )
    }

    fn get_bytes_to_transmit(&self, msg_id: MsgID) -> &[u8] {
        &self.bytes_to_transmit[msg_id as usize % self.bytes_to_transmit.len()].as_slice()
    }
}

impl<C, D> ClientSM for EchoClient<C, D>
where
    C: ClientCerealizeMessage<D>,
    D: Datapath,
{
    type Datapath = D;

    fn get_current_id(&self) -> u32 {
        self.last_sent_id
    }

    fn increment_id(&mut self) {
        self.last_sent_id += 1;
    }

    fn increment_uniq_received(&mut self) {
        self.received += 1;
    }

    fn increment_uniq_sent(&mut self) {
        self.last_sent_id += 1;
    }

    fn increment_num_timed_out(&mut self) {
        self.num_timed_out += 1;
    }

    fn increment_num_retried(&mut self) {
        self.num_retried += 1;
    }

    fn uniq_sent_so_far(&self) -> usize {
        self.last_sent_id as usize
    }

    fn uniq_received_so_far(&self) -> usize {
        self.received
    }

    fn num_retried(&self) -> usize {
        self.num_retried
    }

    fn num_timed_out(&self) -> usize {
        self.num_timed_out
    }

    fn get_sized_rtts(&self) -> &cornflakes_libos::timing::SizedManualHistogram {
        &self.sized_rtts
    }

    fn get_mut_sized_rtts(&mut self) -> &mut cornflakes_libos::timing::SizedManualHistogram {
        &mut self.sized_rtts
    }

    fn set_recording_size_rtts(&mut self) {
        self.recording_size_rtts = true;
    }

    fn recording_size_rtts(&self) -> bool {
        self.recording_size_rtts
    }

    fn get_mut_rtts(&mut self) -> &mut ManualHistogram {
        &mut self.rtts
    }

    fn server_addr(&self) -> AddressInfo {
        self.server_addr.clone()
    }

    fn get_next_msg(
        &mut self,
        _datapath: &<Self as ClientSM>::Datapath,
    ) -> Result<Option<(MsgID, &[u8])>> {
        Ok(Some((
            self.last_sent_id,
            self.get_bytes_to_transmit(self.last_sent_id),
        )))
    }

    fn process_received_msg(
        &mut self,
        sga: ReceivedPkt<<Self as ClientSM>::Datapath>,
        datapath: &<Self as ClientSM>::Datapath,
    ) -> Result<bool> {
        // if in debug mode, check whether the bytes are what they should be
        tracing::debug!(id = sga.msg_id(), size = sga.data_len(), "Received sga");
        if cfg!(debug_assertions) {
            if !self.cerealizer.check_echoed_payload(
                &sga,
                self.get_bytes_to_check(sga.msg_id()),
                datapath,
            )? {
                tracing::warn!(id = sga.msg_id(), "Payloads not equal");
                bail!("Payloads not equal");
            } else {
                tracing::info!(id = sga.msg_id(), "Passed test");
            }
        }
        Ok(true)
    }

    fn init(&mut self, _connection: &mut Self::Datapath) -> Result<()> {
        tracing::info!(size = self.bytes_to_transmit.len(), "Bytes to transmit");
        Ok(())
    }

    fn cleanup(&mut self, _connection: &mut Self::Datapath) -> Result<()> {
        Ok(())
    }

    fn msg_timeout_cb(&mut self, id: MsgID, _datapath: &Self::Datapath) -> Result<&[u8]> {
        tracing::info!(id, last_sent = self.last_sent_id, "Retry callback");
        Ok(self.get_bytes_to_transmit(self.last_sent_id))
    }
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

/// Given sizes for each field, return bytes.
fn init_payloads(sizes: Vec<usize>) -> Vec<Vec<u8>> {
    let actual_alloc_boundaries: Vec<usize> =
        sizes.iter().map(|x| align_up(*x, ALIGN_SIZE)).collect();
    let mut ret: Vec<Vec<u8>> = Vec::new();
    let mut rng = thread_rng();
    for (idx, size) in actual_alloc_boundaries.iter().enumerate() {
        let payload_size = sizes[idx];
        assert!(payload_size <= *size);
        let chars: String = iter::repeat(())
            .map(|()| rng.sample(Alphanumeric))
            .map(char::from)
            .take(*size)
            .collect();
        tracing::debug!(idx, size, "chars: {:?}", chars.as_bytes().to_vec());
        ret.push(chars.as_bytes().to_vec());
    }
    ret
}
