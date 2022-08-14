use bytes::Bytes;
use cornflakes_libos::{
    datapath::{Datapath, ReceivedPkt},
    state_machine::client::ClientSM,
    timing::ManualHistogram,
    utils::AddressInfo,
    MsgID,
};
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SgBenchClient<D>
where
    D: Datapath,
{
    /// How server initializes memory on other sizes (for checking)
    server_payload_regions: Vec<Bytes>,
    /// requests
    requests: Vec<Bytes>,
    /// Echo mode on server
    echo_mode: bool,
    /// For client loadgen
    _datapath: PhantomData<D>,
    received: usize,
    num_retried: usize,
    num_timed_out: usize,
    server_addr: AddressInfo,
    rtts: ManualHistogram,
}

impl<D> SgBenchClient<D>
where
    D: Datapath,
{
    pub fn new(
        segment_size: usize,
        num_segments: usize,
        array_size: usize,
        send_packet_size: usize,
        random_seed: usize,
    ) -> Result<Self> {
    }
}

impl<D> ClientSM for SgBenchClient<D>
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

    fn get_next_msg(
        &mut self,
        datapath: &<Self as ClientSM>::Datapath,
    ) -> Result<Option<(MsgID, &[u8])>> {
        // based on the current index in the segment sequence, generate bytes to send server
    }

    fn process_received_msg(
        &mut self,
        sga: ReceivedPkt<<Self as ClientSM>::Datapath>,
        _datapath: &<Self as ClientSM>::Datapath,
    ) -> Result<bool> {
        // based on the request ID of the received message
        // check that the sequence of bytes is correct
        // if 'echo mode' - remember that the sequence of bytes will be different
    }

    fn init(&mut self, connection: &mut Self::Datapath) -> Result<()> {
        connection.add_tx_mempool(8192, 8192)?;
        Ok(())
    }

    fn cleanup(&mut self, _connection: &mut Self::Datapath) -> Result<()> {
        Ok(())
    }

    fn msg_timeout_cb(&mut self, id: MsgID, datapath: &Self::Datapath) -> Result<&[u8]> {
        let req = match self.outgoing_requests.get(&id) {
            Some(r) => r.clone(),
            None => {
                bail!("Cannot find data for msg # {} to send retry", id);
            }
        };
    }
}
