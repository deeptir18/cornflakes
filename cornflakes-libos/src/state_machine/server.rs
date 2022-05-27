#[cfg(feature = "profiler")]
use perftools;
#[cfg(feature = "profiler")]
const PROFILER_DEPTH: usize = 10;
use super::super::{
    datapath::{Datapath, PushBufType, ReceivedPkt},
    ArenaOrderedSga,
};
use color_eyre::eyre::Result;
use std::time::Instant;
pub trait ServerSM {
    type Datapath: Datapath;

    fn push_buf_type(&self) -> PushBufType;

    fn process_requests_ordered_sga(
        &mut self,
        pkts: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        datapath: &mut Self::Datapath,
    ) -> Result<()>;

    fn process_requests_object(
        &mut self,
        _pkts: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        _datapath: &mut Self::Datapath,
    ) -> Result<()> {
        unimplemented!();
    }

    fn process_requests_sga(
        &mut self,
        sga: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        datapath: &mut Self::Datapath,
    ) -> Result<()>;

    fn process_requests_rc_sga(
        &mut self,
        sga: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        datapath: &mut Self::Datapath,
    ) -> Result<()>;

    fn process_requests_single_buf(
        &mut self,
        sga: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        datapath: &mut Self::Datapath,
    ) -> Result<()>;

    fn process_requests_arena_ordered_sga(
        &mut self,
        _sga: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        _datapath: &mut Self::Datapath,
        _arena: &mut bumpalo::Bump,
    ) -> Result<()> {
        unimplemented!();
    }

    fn run_state_machine(&mut self, datapath: &mut Self::Datapath) -> Result<()> {
        // run profiler from here
        #[cfg(feature = "profiler")]
        perftools::profiler::reset();
        let mut _last_log: Instant = Instant::now();
        let mut _requests_processed = 0;

        let mut arena = bumpalo::Bump::with_capacity(
            ArenaOrderedSga::arena_size(
                Self::Datapath::batch_size(),
                Self::Datapath::max_packet_size(),
                Self::Datapath::max_scatter_gather_entries(),
            ) * 100,
        );
        loop {
            #[cfg(feature = "profiler")]
            perftools::timer!("Run state machine loop");

            #[cfg(feature = "profiler")]
            {
                if _last_log.elapsed() > std::time::Duration::from_secs(5) {
                    let d = Instant::now() - _last_log;
                    tracing::info!(
                        "Server processed {} # of reqs since last dump at rate of {:.2} reqs/s",
                        _requests_processed,
                        _requests_processed as f64 / d.as_secs_f64()
                    );
                    perftools::profiler::write(&mut std::io::stdout(), Some(PROFILER_DEPTH))
                        .unwrap();
                    perftools::profiler::reset();
                    _requests_processed = 0;
                    _last_log = Instant::now();
                }
            }

            let pkts = {
                #[cfg(feature = "profiler")]
                perftools::timer!("Datapath pop");
                datapath.pop()?
            };
            if pkts.len() > 0 {
                match self.push_buf_type() {
                    PushBufType::SingleBuf => {
                        self.process_requests_single_buf(pkts, datapath)?;
                    }
                    PushBufType::Sga => {
                        self.process_requests_sga(pkts, datapath)?;
                    }
                    PushBufType::RcSga => {
                        self.process_requests_rc_sga(pkts, datapath)?;
                    }
                    PushBufType::OrderedSga => {
                        #[cfg(feature = "profiler")]
                        perftools::timer!("App process requests ordered sga");
                        self.process_requests_ordered_sga(pkts, datapath)?;
                    }
                    PushBufType::Object => {
                        #[cfg(feature = "profiler")]
                        perftools::timer!("App process requests object");
                        self.process_requests_object(pkts, datapath)?;
                    }
                    PushBufType::ArenaOrderedSga => {
                        self.process_requests_arena_ordered_sga(pkts, datapath, &mut arena)?;
                    }
                }
            }
        }
    }

    /// Initializes any internal state with any datapath specific configuration,
    /// e.g., registering external memory.
    /// By default, adds tx mempools
    fn init(&mut self, connection: &mut Self::Datapath) -> Result<()> {
        let mut buf_size = 256;
        let max_size = 8192;
        let min_elts = 8192;
        loop {
            // add a tx pool with buf size
            tracing::info!("Adding memory pool of size {}", buf_size);
            connection.add_memory_pool(buf_size, min_elts)?;
            buf_size *= 2;
            if buf_size > max_size {
                break;
            }
        }
        Ok(())
    }

    /// Cleanup any state.
    fn cleanup(&mut self, _connection: &mut Self::Datapath) -> Result<()> {
        Ok(())
    }
}
