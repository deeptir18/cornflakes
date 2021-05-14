use super::CerealizeMessage;
use color_eyre::eyre::Result;
use cornflakes_libos::{
    timing::HistogramWrapper, utils::AddressInfo, Cornflake, Datapath, ReceivedPacket,
    ScatterGather, ServerSM,
};
use std::{
    marker::PhantomData,
    sync::{Arc, Mutex},
    time::Duration,
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

    fn cleanup(&mut self, _connection: &mut Self::Datapath) -> Result<()> {
        Ok(())
    }

    fn process_requests(
        &mut self,
        sgas: &Vec<(
            <<Self as ServerSM>::Datapath as Datapath>::ReceivedPkt,
            Duration,
        )>,
        mut send_fn: impl FnMut(&Vec<(Cornflake, AddressInfo)>) -> Result<()>,
    ) -> Result<()> {
        let mut out_sgas: Vec<(Cornflake, AddressInfo)> = Vec::with_capacity(sgas.len());
        let mut contexts: Vec<S::Ctx> = (0..sgas.len())
            .map(|_i| self.serializer.new_context())
            .collect();
        for (in_sga, ctx) in sgas.iter().zip(contexts.iter_mut()) {
            let mut out_sga = self.serializer.process_msg(&in_sga.0, ctx)?;
            out_sga.set_id(in_sga.0.get_id());
            out_sgas.push((out_sga, in_sga.0.get_addr().clone()));
        }
        send_fn(&out_sgas)
    }

    fn get_histograms(&self) -> Vec<Arc<Mutex<HistogramWrapper>>> {
        Vec::default()
    }
}
