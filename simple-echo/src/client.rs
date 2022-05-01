use super::RequestShape;
use color_eyre::eyre::Result;
use cornflakes_libos::{
    datapath::{Datapath, ReceivedPkt},
    state_machine::client::ClientSM,
    timing::ManualHistogram,
    utils::AddressInfo,
    MsgID,
};
use std::{iter::Iterator, marker::PhantomData};

pub struct SimpleEchoClient<D>
where
    D: Datapath,
{
    last_sent_id: MsgID,
    received: usize,
    num_retried: usize,
    num_timed_out: usize,
    bytes_to_transmit: Vec<u8>,
    server_addr: AddressInfo,
    rtts: ManualHistogram,
    _datapath: PhantomData<D>,
}

impl<D> SimpleEchoClient<D>
where
    D: Datapath,
{
    pub fn new(
        server_addr: AddressInfo,
        request_shape: &RequestShape,
        max_num_requests: usize,
    ) -> SimpleEchoClient<D> {
        SimpleEchoClient {
            last_sent_id: 0,
            received: 0,
            num_retried: 0,
            num_timed_out: 0,
            bytes_to_transmit: request_shape
                .generate_bytes()
                .into_iter()
                .flatten()
                .collect(),
            server_addr: server_addr,
            rtts: ManualHistogram::new(max_num_requests),
            _datapath: PhantomData::default(),
        }
    }
}

impl<D> ClientSM for SimpleEchoClient<D>
where
    D: Datapath,
{
    type Datapath = D;

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

    fn get_mut_rtts(&mut self) -> &mut ManualHistogram {
        &mut self.rtts
    }

    fn server_addr(&self) -> AddressInfo {
        self.server_addr.clone()
    }

    fn get_next_msg(&mut self) -> Result<Option<(MsgID, &[u8])>> {
        Ok(Some((
            self.last_sent_id,
            &self.bytes_to_transmit.as_slice(),
        )))
    }

    fn process_received_msg(
        &mut self,
        sga: ReceivedPkt<<Self as ClientSM>::Datapath>,
    ) -> Result<()> {
        // if in debug mode, check whether the bytes are what they should be
        tracing::debug!(id = sga.msg_id(), "Received sga");
        if cfg!(debug_assertions) {
            let bytes = sga.flatten();
            assert!(bytes == self.bytes_to_transmit);
        }
        Ok(())
    }

    fn init(&mut self, connection: &mut Self::Datapath) -> Result<()> {
        connection.add_memory_pool(self.bytes_to_transmit.len() * 2, 8192)?;
        Ok(())
    }

    fn cleanup(&mut self, _connection: &mut Self::Datapath) -> Result<()> {
        Ok(())
    }

    fn msg_timeout_cb(&mut self, id: MsgID) -> Result<&[u8]> {
        tracing::info!(id, last_sent = self.last_sent_id, "Retry callback");
        Ok(&self.bytes_to_transmit.as_slice())
    }
}
