use super::super::{
    datapath::{Datapath, ReceivedPkt},
    utils::AddressInfo,
    MsgID,
};
use color_eyre::eyre::Result;
use std::time::Duration;

pub trait ClientSM {
    type Datapath: Datapath;

    fn server_addr(&self) -> AddressInfo;

    fn get_next_msg(&mut self) -> Result<Option<(MsgID, &[u8])>>;

    fn process_received_msg(
        &mut self,
        sga: ReceivedPkt<<Self as ClientSM>::Datapath>,
        rtt: Duration,
    ) -> Result<()>;

    /// What to do when a particular message times out.
    fn msg_timeout_cb(&mut self, id: MsgID) -> Result<(MsgID, &[u8])>;

    /// Initializes any internal state with any datapath specific configuration,
    /// e.g., registering external memory.
    fn init(&mut self, connection: &mut Self::Datapath) -> Result<()>;

    fn cleanup(&mut self, connection: &mut Self::Datapath) -> Result<()>;

    fn uniq_received_so_far(&self) -> usize;

    fn uniq_sent_so_far(&self) -> usize;

    fn num_timed_out(&self) -> usize;

    /// Run open loop client
    fn run_closed_loop(
        &mut self,
        datapath: &mut Self::Datapath,
        num_pkts: u64,
        time_out: impl Fn(usize) -> Duration,
        server_addr: &AddressInfo,
    ) -> Result<()> {
        unimplemented!();
    }
}
