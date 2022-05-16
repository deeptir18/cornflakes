use super::super::datapath::{Datapath, PushBufType, ReceivedPkt};
use color_eyre::eyre::Result;
pub trait ServerSM {
    type Datapath: Datapath;

    fn push_buf_type(&self) -> PushBufType;

    fn process_requests_ordered_sga(
        &mut self,
        pkts: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        datapath: &mut Self::Datapath,
    ) -> Result<()>;

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

    fn run_state_machine(&mut self, datapath: &mut Self::Datapath) -> Result<()> {
        loop {
            let pkts = datapath.pop()?;
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
