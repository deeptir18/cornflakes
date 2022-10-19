use super::{ClientCerealizeMessage, REQ_TYPE_SIZE};
use color_eyre::eyre::Result;
use cornflakes_libos::{
    datapath::{Datapath, PushBufType, ReceivedPkt},
    state_machine::server::ServerSM,
};
use cornflakes_utils::SimpleMessageType;
use std::marker::PhantomData;

pub struct RawEcho<D>
where
    D: Datapath,
{
    _phantom_data: PhantomData<D>,
}

impl<D> RawEcho<D>
where
    D: Datapath,
{
    pub fn new(_push_buf_type: PushBufType) -> Self {
        RawEcho {
            _phantom_data: PhantomData::default(),
        }
    }

    pub fn set_with_copy(&mut self) {}
}

impl<D> ServerSM for RawEcho<D>
where
    D: Datapath,
{
    type Datapath = D;
    fn push_buf_type(&self) -> PushBufType {
        PushBufType::Echo
    }

    #[inline]
    fn process_requests_echo(
        &mut self,
        pkts: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        datapath: &mut Self::Datapath,
    ) -> Result<()> {
        datapath.echo(pkts)?;
        Ok(())
    }
}

pub struct RawEchoClient {}
impl<D> ClientCerealizeMessage<D> for RawEchoClient
where
    D: Datapath,
{
    fn new() -> Self
    where
        Self: Sized,
    {
        RawEchoClient {}
    }

    fn check_echoed_payload(
        &self,
        pkt: &ReceivedPkt<D>,
        bytes_to_check: (SimpleMessageType, &Vec<Vec<u8>>),
        _datapath: &D,
    ) -> Result<bool> {
        let (_ty, input) = bytes_to_check;
        let total_bytes = input.clone().into_iter().flatten().collect::<Vec<u8>>();
        let received_bytes = pkt.seg(0).as_ref()[REQ_TYPE_SIZE..].to_vec();
        if received_bytes != total_bytes {
            return Ok(false);
        }
        return Ok(true);
    }

    fn get_serialized_bytes(
        _ty: SimpleMessageType,
        input: &Vec<Vec<u8>>,
        _datapath: &D,
    ) -> Result<Vec<u8>> {
        let total_bytes = input.clone().into_iter().flatten().collect::<Vec<u8>>();
        Ok(total_bytes)
    }
}
