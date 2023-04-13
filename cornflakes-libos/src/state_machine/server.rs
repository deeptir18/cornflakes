#[cfg(feature = "profiler")]
use demikernel::perftools;
#[cfg(feature = "profiler")]
const PROFILER_DEPTH: usize = 10;
use super::super::{
    datapath::{Datapath, PushBufType, ReceivedPkt},
    ArenaOrderedSga,
};
use color_eyre::eyre::Result;
use std::{fs::File, io::Write, time::Instant};
pub trait ServerSM {
    type Datapath: Datapath;

    fn push_buf_type(&self) -> PushBufType;

    fn process_requests_hybrid_arena_sga(
        &mut self,
        _pkts: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        _datapath: &mut Self::Datapath,
        _arena: &mut bumpalo::Bump,
    ) -> Result<()> {
        unimplemented!();
    }

    fn process_requests_hybrid_object(
        &mut self,
        _pkts: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        _datapath: &mut Self::Datapath,
    ) -> Result<()> {
        unimplemented!();
    }

    fn process_requests_hybrid_arena_object(
        &mut self,
        _pkts: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        _datapath: &mut Self::Datapath,
        _arena: &mut bumpalo::Bump,
    ) -> Result<()> {
        unimplemented!();
    }

    fn process_requests_echo(
        &mut self,
        _pkts: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        _datapath: &mut Self::Datapath,
    ) -> Result<()> {
        unimplemented!();
    }

    fn process_requests_ordered_sga(
        &mut self,
        _pkts: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        _datapath: &mut Self::Datapath,
    ) -> Result<()> {
        unimplemented!();
    }

    fn process_requests_object(
        &mut self,
        _pkts: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        _datapath: &mut Self::Datapath,
        _arena: &mut bumpalo::Bump,
    ) -> Result<()> {
        unimplemented!();
    }

    fn process_requests_sga(
        &mut self,
        _sga: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        _datapath: &mut Self::Datapath,
    ) -> Result<()> {
        unimplemented!();
    }

    fn process_requests_rc_sga(
        &mut self,
        _sga: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        _datapath: &mut Self::Datapath,
    ) -> Result<()> {
        unimplemented!();
    }

    fn process_requests_single_buf(
        &mut self,
        _sga: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        _datapath: &mut Self::Datapath,
    ) -> Result<()> {
        unimplemented!();
    }

    fn process_requests_arena_ordered_sga(
        &mut self,
        _sga: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        _datapath: &mut Self::Datapath,
        _arena: &mut bumpalo::Bump,
    ) -> Result<()> {
        unimplemented!();
    }

    fn write_ready(&self, ready_file: Option<String>) -> Result<()> {
        match ready_file {
            Some(r) => {
                let mut file = File::create(r)?;
                file.write_all(b"ready\n")?;
            }
            None => {}
        }
        Ok(())
    }

    fn run_state_machine_baseline(&mut self, datapath: &mut Self::Datapath) -> Result<()> {
        // run profiler from here
        #[cfg(feature = "profiler")]
        perftools::profiler::reset();
        let mut _last_log: Instant = Instant::now();
        let mut _requests_processed = 0;

        loop {
            #[cfg(feature = "profiler")]
            demikernel::timer!("Run state machine loop");

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
                demikernel::timer!("Datapath pop");
                datapath.pop()?
            };
            if pkts.len() > 0 {
                match self.push_buf_type() {
                    PushBufType::SingleBuf => {
                        #[cfg(feature = "profiler")]
                        demikernel::timer!("Process requests singlebuf");
                        self.process_requests_single_buf(pkts, datapath)?;
                    }
                    PushBufType::Echo => {
                        self.process_requests_echo(pkts, datapath)?;
                    }
                    _ => {
                        unreachable!();
                    }
                }
            }
        }
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
            ) * 10000,
        );

        loop {
            #[cfg(feature = "profiler")]
            demikernel::timer!("Run state machine loop");

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
                demikernel::timer!("Datapath pop");
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
                        self.process_requests_ordered_sga(pkts, datapath)?;
                    }
                    PushBufType::HybridObject => {
                        #[cfg(feature = "profiler")]
                        demikernel::timer!("Process requests hybrid object");
                        self.process_requests_hybrid_object(pkts, datapath)?;
                    }
                    PushBufType::HybridArenaObject => {
                        #[cfg(feature = "profiler")]
                        demikernel::timer!("Process requests hybrid arena object");
                        self.process_requests_hybrid_arena_object(pkts, datapath, &mut arena)?;
                        #[cfg(feature = "profiler")]
                        demikernel::timer!("Arena reset");
                        arena.reset();
                    }
                    PushBufType::HybridArenaSga => {
                        self.process_requests_hybrid_arena_sga(pkts, datapath, &mut arena)?;
                        arena.reset();
                    }
                    PushBufType::Object => {
                        {
                            #[cfg(feature = "profiler")]
                            demikernel::timer!("Process requests object");
                            self.process_requests_object(pkts, datapath, &mut arena)?;
                        }
                        {
                            #[cfg(feature = "profiler")]
                            demikernel::timer!("Arena reset");
                            arena.reset();
                        }
                    }
                    PushBufType::ArenaOrderedSga => {
                        self.process_requests_arena_ordered_sga(pkts, datapath, &mut arena)?;
                    }
                    PushBufType::Echo => {
                        self.process_requests_echo(pkts, datapath)?;
                    }
                }
            }
        }
    }

    /// Initializes any internal state with any datapath specific configuration,
    /// e.g., registering external memory.
    /// By default, adds tx mempools
    fn init(&mut self, _connection: &mut Self::Datapath) -> Result<()> {
        Ok(())
    }

    /// Cleanup any state.
    fn cleanup(&mut self, _connection: &mut Self::Datapath) -> Result<()> {
        Ok(())
    }
}
