use super::{get_equal_fields, CerealizeMessage};
use color_eyre::eyre::{Result, WrapErr};
use cornflakes_libos::{
    mem::MmapMetadata, timing::HistogramWrapper, utils::AddressInfo, Cornflake, Datapath,
    ReceivedPkt, ScatterGather, ServerSM,
};
use cornflakes_utils::SimpleMessageType;
use std::{
    marker::PhantomData,
    sync::{Arc, Mutex},
    time::Duration,
};
pub const NUM_PAGES: usize = 30;

pub struct EchoServer<S, D> {
    serializer: S,
    metadata: MmapMetadata,
    _marker: PhantomData<D>,
}

impl<S, D> EchoServer<S, D>
where
    S: CerealizeMessage<D>,
    D: Datapath,
{
    pub fn new(
        message: SimpleMessageType,
        size: usize,
        deserialize_received: bool,
    ) -> Result<EchoServer<S, D>> {
        let metadata = MmapMetadata::new(NUM_PAGES)?;
        let serializer = S::new(
            message,
            get_equal_fields(message, size),
            metadata.clone(),
            deserialize_received,
        )?;
        Ok(EchoServer {
            serializer: serializer,
            metadata: metadata,
            _marker: PhantomData,
        })
    }
}

impl<S, D> ServerSM for EchoServer<S, D>
where
    S: CerealizeMessage<D>,
    D: Datapath,
{
    type Datapath = D;

    fn init(&mut self, connection: &mut Self::Datapath) -> Result<()> {
        connection
            .register_external_region(&mut self.metadata)
            .wrap_err("Failed to register external region.")?;
        Ok(())
    }

    fn cleanup(&mut self, connection: &mut Self::Datapath) -> Result<()> {
        connection
            .unregister_external_region(&self.metadata)
            .wrap_err("Failed to unregister external region.")?;
        self.metadata.free_mmap();
        Ok(())
    }

    fn process_requests(
        &mut self,
        sgas: Vec<(ReceivedPkt<<Self as ServerSM>::Datapath>, Duration)>,
        datapath: &mut D,
    ) -> Result<()> {
        let mut out_sgas: Vec<(Cornflake, AddressInfo)> = Vec::with_capacity(sgas.len());
        let contexts_res: Result<Vec<S::Ctx>> = (0..sgas.len())
            .map(|_i| self.serializer.new_context())
            .collect();
        let mut contexts = contexts_res?;
        for (in_sga, ctx) in sgas.iter().zip(contexts.iter_mut()) {
            let mut out_sga = self.serializer.process_msg(&in_sga.0, ctx)?;
            out_sga.set_id(in_sga.0.get_id());
            out_sgas.push((out_sga, in_sga.0.get_addr().clone()));
        }
        datapath
            .push_sgas(&out_sgas)
            .wrap_err("Unable to run push_sgas in datapath")?;
        Ok(())
    }

    fn get_histograms(&self) -> Vec<Arc<Mutex<HistogramWrapper>>> {
        Vec::default()
    }
}
