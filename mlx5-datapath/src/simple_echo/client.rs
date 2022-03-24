use super::RequestShape;
use color_eyre::eyre::{bail, Result};
use cornflakes_libos::{
    datapath::{Datapath, ReceivedPkt},
    state_machine::client::ClientSM,
    timing::ManualHistogram,
    utils::AddressInfo,
    MsgID,
};
use std::iter::Iterator;

pub struct SimpleEchoClient {
    last_sent_id: MsgID,
    received: usize,
    bytes_to_transmit: Vec<u8>,
    server_addr: AddressInfo,
    rtts: ManualHistogram,
}

impl SimpleEchoClient {
    pub fn new(
        server_addr: AddressInfo,
        request_shape: RequestShape,
        max_num_requests: usize,
    ) -> SimpleEchoClient {
        SimpleEchoClient {
            last_sent_id: 0,
            received: 0,
            bytes_to_transmit: request_shape
                .generate_bytes()
                .into_iter()
                .flatten()
                .collect(),
            server_addr: server_addr,
            rtts: ManualHistogram::new(max_num_requests),
        }
    }
}
