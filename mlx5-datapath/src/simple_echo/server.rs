use super::{super::datapath::connection::Mlx5Connection, RequestShape};
use color_eyre::eyre::{bail, Result, WrapErr};
use cornflakes_libos::{
    datapath::{Datapath, PushBufType, ReceivedPkt},
    state_machine::server::ServerSM,
    ConnID, MsgID, RcSga, RcSge, Sga, Sge,
};

pub struct SimpleEchoServer {
    push_mode: PushBufType,
    range_vec: Vec<(usize, usize)>,
}

impl SimpleEchoServer {
    pub fn new(push_mode: PushBufType, request_shape: RequestShape) -> SimpleEchoServer {
        SimpleEchoServer {
            push_mode: push_mode,
            range_vec: request_shape.range_vec(),
        }
    }
}

impl ServerSM for SimpleEchoServer {
    type Datapath = Mlx5Connection;

    fn push_buf_type(&self) -> PushBufType {
        self.push_mode
    }

    fn process_requests_sga(
        &mut self,
        pkts: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        datapath: &mut Self::Datapath,
    ) -> Result<()> {
        let outgoing_sgas_result: Result<Vec<(MsgID, ConnID, Sga)>> = pkts.iter().map(|pkt| {
            let sge_results: Result<Vec<Sge>> = self.range_vec.iter().map(|range| {
                match pkt.contiguous_slice(range.0, range.1) {
                    Some(s) => Ok(Sge::new(s)),
                    None => {
                        bail!(format!("Could not get contiguous slice out of received pkt length {} for range [{}, {}]", pkt.data_len(), range.1, range.1));
                    }
                }
            }).collect();
            Ok((pkt.msg_id(), pkt.conn_id(), Sga::with_entries(sge_results?)))
        }).collect();
        datapath
            .push_sgas(&outgoing_sgas_result?)
            .wrap_err("Failed to push sgas")?;
        Ok(())
    }

    fn process_requests_rc_sga(
        &mut self,
        pkts: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        datapath: &mut Self::Datapath,
    ) -> Result<()> {
        let outgoing_rc_sgas_result: Result<Vec<(MsgID, ConnID, RcSga<<Self as ServerSM>::Datapath>)>> = pkts
            .iter()
            .map(|pkt| {
                let rc_sge_results: Result<Vec<RcSge<<Self as ServerSM>::Datapath>>> =
                    self.range_vec.iter().map(|range| {
                        match pkt.contiguous_datapath_metadata(range.0, range.1) {
                            Some(d) => Ok(RcSge::RefCounted(d)),
                            None => {
                                bail!(format!("Could not get contiguous datapath metadata out of received pkt length {} for range [{}, {}]", pkt.data_len(), range.0, range.1));
                            }
                        }
                    }).collect();
                Ok((
                    pkt.msg_id(),
                    pkt.conn_id(),
                    RcSga::with_entries(rc_sge_results?),
                ))
            })
            .collect();
        datapath
            .push_rc_sgas(&outgoing_rc_sgas_result?)
            .wrap_err("Failed to push rc sgas")?;
        Ok(())
    }

    fn process_requests_single_buf(
        &mut self,
        sga: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        datapath: &mut Self::Datapath,
    ) -> Result<()> {
        Ok(())
    }

    fn init(&mut self, _connection: &mut Self::Datapath) -> Result<()> {
        // TODO: do we need to add a transmit memory pool?
        Ok(())
    }

    fn cleanup(&mut self, _connection: &mut Self::Datapath) -> Result<()> {
        Ok(())
    }
}
