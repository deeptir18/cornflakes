use super::CerealizeClient;
use color_eyre::eyre::Result;
use cornflakes_libos::{
    mem::{mmap_new, MmapMetadata},
    timing::{HistogramWrapper, RTTHistogram},
    ClientSM, Datapath, MsgID, ScatterGather,
};
use cornflakes_utils::SimpleMessageType;
use memmap::MmapMut;
use std::{marker::PhantomData, net::Ipv4Addr, ops::FnMut, time::Duration};

pub const NUM_PAGES: usize = 100;

pub struct EchoClient<S, D> {
    serializer: S,
    server_ip: Ipv4Addr,
    sent: usize,
    recved: usize,
    retries: usize,
    last_sent_id: u32,
    rtts: HistogramWrapper,
    metadata: MmapMetadata,
    _mmap_mut: MmapMut,
    _marker: PhantomData<D>,
}

impl<S, D> EchoClient<S, D>
where
    S: CerealizeClient<D>,
    D: Datapath,
{
    pub fn new(
        server_ip: Ipv4Addr,
        message: SimpleMessageType,
        sizes: Vec<usize>,
    ) -> Result<EchoClient<S, D>> {
        let (metadata, mut mmap_mut) = mmap_new(NUM_PAGES)?;
        let serializer = S::new(message, sizes, metadata.clone(), &mut mmap_mut)?;
        Ok(EchoClient {
            serializer: serializer,
            server_ip: server_ip,
            sent: 0,
            recved: 0,
            retries: 0,
            last_sent_id: 0,
            rtts: HistogramWrapper::new("Echo-Client-RTTs")?,
            metadata: metadata,
            _mmap_mut: mmap_mut,
            _marker: PhantomData,
        })
    }

    pub fn dump_stats(&mut self) {
        tracing::info!(
            sent = self.sent,
            received = self.recved,
            retries = self.retries,
            unique_sent = self.last_sent_id,
            "High level sending stats",
        );
        self.rtts.dump("End-to-end DPDK echo client RTTs:");
    }
}

impl<S, D> ClientSM for EchoClient<S, D>
where
    S: CerealizeClient<D>,
    D: Datapath,
{
    type OutgoingMsg = S::OutgoingMsg;
    type Datapath = D;
    fn server_ip(&self) -> Ipv4Addr {
        self.server_ip
    }

    fn send_next_msg(
        &mut self,
        mut send_fn: impl FnMut(Self::OutgoingMsg) -> Result<()>,
    ) -> Result<()> {
        self.last_sent_id += 1;
        self.sent += 1;
        let mut ctx = self.serializer.new_context();
        let mut out_sga = self.serializer.get_sga(&mut ctx)?;
        out_sga.set_id(self.last_sent_id);
        send_fn(out_sga)
    }

    fn process_received_msg(
        &mut self,
        sga: <<Self as ClientSM>::Datapath as Datapath>::ReceivedPkt,
        rtt: Duration,
    ) -> Result<()> {
        self.recved += 1;
        self.rtts.record(rtt.as_nanos() as u64)?;
        if cfg!(debug_assertions) {
            // check the payload that was echoed back is correct
            self.serializer.check_echoed_payload(&sga)?;
        }
        Ok(())
    }

    fn init(&mut self, connection: &mut Self::Datapath) -> Result<()> {
        connection.register_external_region(self.metadata.clone())?;
        Ok(())
    }

    fn msg_timeout_cb(
        &mut self,
        id: MsgID,
        mut send_fn: impl FnMut(Self::OutgoingMsg) -> Result<()>,
    ) -> Result<()> {
        tracing::info!(id, last_sent = self.last_sent_id, "Retry callback");
        self.retries += 1;
        let mut ctx = self.serializer.new_context();
        let mut sga = self.serializer.get_sga(&mut ctx)?;
        sga.set_id(id);
        send_fn(sga)
    }
}
