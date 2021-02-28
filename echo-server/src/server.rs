use super::CerealizeMessage;
use color_eyre::eyre::Result;
use cornflakes_libos::{
    timing::HistogramWrapper, Cornflake, Datapath, ReceivedPacket, ScatterGather, ServerSM,
};
use std::{
    marker::PhantomData,
    net::Ipv4Addr,
    sync::{Arc, Mutex},
};

pub struct EchoServer<S, D> {
    serializer: S,
    _marker: PhantomData<D>,
}

impl<S, D> EchoServer<S, D>
where
    S: CerealizeMessage<D>,
    D: Datapath,
{
    pub fn new(serializer: S) -> EchoServer<S, D> {
        EchoServer {
            serializer: serializer,
            _marker: PhantomData,
        }
    }
}

impl<S, D> ServerSM for EchoServer<S, D>
where
    S: CerealizeMessage<D>,
    D: Datapath,
{
    type Datapath = D;

    fn init(&mut self, _connection: &mut Self::Datapath) -> Result<()> {
        // for this app, no need to do datapath specific initialization
        // application doesn't end up using any external memory
        Ok(())
    }

    fn process_request(
        &mut self,
        sga: &<<Self as ServerSM>::Datapath as Datapath>::ReceivedPkt,
        mut send_fn: impl FnMut(Cornflake, Ipv4Addr) -> Result<()>,
    ) -> Result<()> {
        let mut ctx = self.serializer.new_context();
        let mut out_sga = self.serializer.process_msg(sga, &mut ctx)?;
        out_sga.set_id(sga.get_id());
        send_fn(out_sga, sga.get_addr().ipv4_addr)
    }

    fn get_histograms(&self) -> Vec<Arc<Mutex<HistogramWrapper>>> {
        Vec::default()
    }
}
