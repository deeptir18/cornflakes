use super::CerealizeClient;
use color_eyre::eyre::{Result, WrapErr};
use cornflakes_libos::{
    mem::MmapMetadata, timing::ManualHistogram, ClientSM, Datapath, MsgID, ReceivedPkt,
};
use cornflakes_utils::SimpleMessageType;
use std::{marker::PhantomData, net::Ipv4Addr, time::Duration};

pub const NUM_PAGES: usize = 30;

pub struct EchoClient<'normal, S, D>
where
    S: CerealizeClient<'normal, D>,
    D: Datapath,
{
    serializer: S,
    server_ip: Ipv4Addr,
    sent: usize,
    recved: usize,
    retries: usize,
    last_sent_id: u32,
    rtts: ManualHistogram,
    metadata: MmapMetadata,
    _marker: PhantomData<&'normal D>,
    buffer: Vec<u8>,
}

impl<'normal, S, D> EchoClient<'normal, S, D>
where
    S: CerealizeClient<'normal, D>,
    D: Datapath,
{
    pub fn new(
        server_ip: Ipv4Addr,
        message: SimpleMessageType,
        sizes: Vec<usize>,
        rtts: ManualHistogram,
    ) -> Result<EchoClient<'normal, S, D>> {
        let metadata = MmapMetadata::new(NUM_PAGES)?;
        let serializer = S::new(message, sizes, metadata.clone())?;
        Ok(EchoClient {
            serializer: serializer,
            server_ip: server_ip,
            sent: 0,
            recved: 0,
            retries: 0,
            last_sent_id: 0,
            rtts: rtts,
            metadata: metadata,
            _marker: PhantomData,
            buffer: Vec::default(),
        })
    }

    pub fn new_context(&self) -> S::Ctx {
        self.serializer.new_context()
    }

    pub fn init_state(&mut self, ctx: &'normal mut S::Ctx, connection: &mut D) -> Result<()> {
        self.init(connection)?;
        self.serializer.init(ctx)?;
        // store the payload we will echo.
        self.buffer = self.serializer.get_msg()?;
        Ok(())
    }

    pub fn sort_rtts(&mut self, start_cutoff: usize) -> Result<()> {
        self.rtts.sort_and_truncate(start_cutoff)?;
        Ok(())
    }

    pub fn log_rtts(&mut self, path: &str, start_cutoff: usize) -> Result<()> {
        self.rtts.sort_and_truncate(start_cutoff)?;
        self.rtts.log_truncated_to_file(path, start_cutoff)?;
        Ok(())
    }

    pub fn dump(&mut self, path: Option<String>, total_time: Duration) -> Result<()> {
        self.rtts.sort()?;
        tracing::info!(
            sent = self.sent,
            received = self.recved,
            retries = self.retries,
            unique_sent = self.last_sent_id,
            total_time = ?total_time.as_secs_f64(),
            "High level sending stats",
        );
        self.rtts.dump("End-to-end DPDK echo client RTTs:")?;

        match path {
            Some(p) => {
                self.rtts.log_to_file(&p)?;
            }
            None => {}
        }
        Ok(())
    }

    pub fn get_num_sent(&self, start_cutoff: usize) -> usize {
        (self.last_sent_id - 1) as usize - start_cutoff
    }

    pub fn get_num_retries(&self) -> usize {
        self.retries as _
    }

    pub fn get_mut_rtts(&mut self) -> &mut ManualHistogram {
        &mut self.rtts
    }

    pub fn get_num_recved(&self, start_cutoff: usize) -> usize {
        self.recved as usize - start_cutoff
    }
}

impl<'normal, S, D> ClientSM for EchoClient<'normal, S, D>
where
    S: CerealizeClient<'normal, D>,
    D: Datapath,
{
    type Datapath = D;

    fn server_ip(&self) -> Ipv4Addr {
        self.server_ip
    }

    fn received_so_far(&self) -> usize {
        self.recved
    }

    fn get_next_msg(&mut self) -> Result<Option<(MsgID, &[u8])>> {
        self.last_sent_id += 1;
        self.sent += 1;
        Ok(Some((self.last_sent_id, &self.buffer.as_slice())))
    }

    fn process_received_msg(
        &mut self,
        sga: ReceivedPkt<<Self as ClientSM>::Datapath>,
        rtt: Duration,
    ) -> Result<()> {
        self.recved += 1;
        tracing::debug!("Receiving {}th packet", self.recved);
        self.rtts.record(rtt.as_nanos() as u64);
        if cfg!(debug_assertions) {
            // check the payload that was echoed back is correct
            self.serializer.check_echoed_payload(&sga)?;
        }
        Ok(())
    }

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

    fn msg_timeout_cb(&mut self, id: MsgID) -> Result<(MsgID, &[u8])> {
        tracing::info!(id, last_sent = self.last_sent_id, "Retry callback");
        self.retries += 1;
        Ok((id, &self.buffer.as_slice()))
    }
}
