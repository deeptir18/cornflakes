use super::RequestShape;
use bytes::{BufMut, Bytes, BytesMut};
use color_eyre::eyre::{bail, Result, WrapErr};
use cornflakes_libos::{
    datapath::{Datapath, PushBufType, ReceivedPkt},
    state_machine::server::ServerSM,
    ConnID, MsgID, OrderedSga, RcSga, RcSge, Sga, Sge,
};
use std::marker::PhantomData;

pub struct SimpleEchoServer<D>
where
    D: Datapath,
{
    push_mode: PushBufType,
    range_vec: Vec<(usize, usize)>,
    response_data_len: usize,
    _datapath: PhantomData<D>,
}

impl<D> SimpleEchoServer<D>
where
    D: Datapath,
{
    pub fn new(push_mode: PushBufType, request_shape: RequestShape) -> SimpleEchoServer<D> {
        SimpleEchoServer {
            push_mode: push_mode,
            range_vec: request_shape.range_vec(),
            response_data_len: request_shape.total_data_len(),
            _datapath: PhantomData::default(),
        }
    }
}

impl<D> ServerSM for SimpleEchoServer<D>
where
    D: Datapath,
{
    type Datapath = D;

    fn push_buf_type(&self) -> PushBufType {
        self.push_mode
    }

    fn process_requests_ordered_sga(
        &mut self,
        pkts: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        datapath: &mut Self::Datapath,
    ) -> Result<()> {
        let outgoing_sgas_result: Result<Vec<(MsgID, ConnID, OrderedSga)>> = pkts.iter().map(|pkt| {
            let sge_results: Result<Vec<Sge>> = self.range_vec.iter().map(|range| {
                tracing::debug!("Getting contiguous slice out of received pkt [{}, {}]", range.0, range.0 + range.1);
                match pkt.contiguous_slice(range.0, range.1) {
                    Some(s) => Ok(Sge::new(s)),
                    None => {
                        bail!(format!("Could not get contiguous slice out of received pkt length {} for range [{}, {}]", pkt.data_len(), range.0, range.0 + range.1));
                    }
                }
            }).collect();
            let sga = Sga::with_entries(sge_results?);
            let num_copy_entries = sga.iter().take_while(|seg| !(seg.len() > datapath.get_copying_threshold())).count();
            tracing::debug!(num_copy_entries, sga_len = sga.len(),"Constructing ordered sga");
            Ok((pkt.msg_id(), pkt.conn_id(), OrderedSga::new(sga, num_copy_entries)))
        }).collect();
        let outgoing_sgas = outgoing_sgas_result?;
        let mut outgoing_pkts: Vec<(MsgID, ConnID, &OrderedSga)> =
            Vec::with_capacity(outgoing_sgas.len());
        for (msg, conn, sga) in outgoing_sgas.iter() {
            outgoing_pkts.push((*msg, *conn, &sga));
        }
        datapath
            .push_ordered_sgas(&outgoing_pkts.as_slice())
            .wrap_err("Failed to push sgas")?;
        Ok(())
    }

    fn process_requests_sga(
        &mut self,
        pkts: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        datapath: &mut Self::Datapath,
    ) -> Result<()> {
        let outgoing_sgas_result: Result<Vec<(MsgID, ConnID, Sga)>> = pkts.iter().map(|pkt| {
            let sge_results: Result<Vec<Sge>> = self.range_vec.iter().map(|range| {
                tracing::debug!("Getting contiguous slice out of received pkt [{}, {}]", range.0, range.0 + range.1);
                match pkt.contiguous_slice(range.0, range.1) {
                    Some(s) => Ok(Sge::new(s)),
                    None => {
                        bail!(format!("Could not get contiguous slice out of received pkt length {} for range [{}, {}]", pkt.data_len(), range.0, range.0 + range.1));
                    }
                }
            }).collect();
            Ok((pkt.msg_id(), pkt.conn_id(), Sga::with_entries(sge_results?)))
        }).collect();
        let outgoing_sgas = outgoing_sgas_result?;
        let mut outgoing_pkts: Vec<(MsgID, ConnID, &Sga)> = Vec::with_capacity(outgoing_sgas.len());
        for (msg, conn, sga) in outgoing_sgas.iter() {
            outgoing_pkts.push((*msg, *conn, &sga));
        }
        datapath
            .push_sgas(&outgoing_pkts.as_slice())
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
                        match pkt.contiguous_datapath_metadata(range.0, range.1)? {
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
        let mut outgoing_rc_sgas = outgoing_rc_sgas_result?;
        let mut outgoing_pkts: Vec<(MsgID, ConnID, &mut RcSga<<Self as ServerSM>::Datapath>)> =
            Vec::with_capacity(outgoing_rc_sgas.len());
        for (msg, conn, ref mut rcsga) in outgoing_rc_sgas.iter_mut() {
            outgoing_pkts.push((*msg, *conn, rcsga));
        }
        datapath
            .push_rc_sgas(&mut outgoing_pkts.as_mut_slice())
            .wrap_err("Failed to push rc sgas")?;
        Ok(())
    }

    fn process_requests_single_buf(
        &mut self,
        pkts: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        datapath: &mut Self::Datapath,
    ) -> Result<()> {
        tracing::debug!("Processing batch of pkts of length {}", pkts.len());
        let outgoing_bufs_result: Result<Vec<(MsgID, ConnID, Bytes)>> = pkts
            .iter()
            .map(|pkt| {
                let mut bytes = BytesMut::with_capacity(self.response_data_len);
                for range in self.range_vec.iter() {
                    let slice = match pkt.contiguous_slice(range.0, range.1) {
                        Some(s) => s,
                        None => {
                            bail!(format!("Could not get contiguous slice in received pkt of total len {} for range  [{}, {}]", pkt.data_len(), range.0, range.1));
                        }
                    };
                    bytes.put(slice);
                }
                Ok((pkt.msg_id(), pkt.conn_id(), bytes.freeze()))
            })
            .collect();
        let outgoing_bufs = outgoing_bufs_result?;
        let mut outgoing_pkts: Vec<(MsgID, ConnID, &[u8])> =
            Vec::with_capacity(outgoing_bufs.len());
        for (msg, conn, bytes) in outgoing_bufs.iter() {
            outgoing_pkts.push((*msg, *conn, bytes.as_ref()));
        }
        /*let outgoing_pkts: Vec<(MsgID, ConnID, &[u8])> = outgoing_bufs
        .iter()
        .map(|(msg, conn, bytes)| (*msg, *conn, bytes.as_ref()))
        .collect();*/
        datapath
            .push_buffers_with_copy(&outgoing_pkts.as_slice())
            .wrap_err("Failed to push buffers with copy to the datapath")?;
        Ok(())
    }

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

    fn cleanup(&mut self, _connection: &mut Self::Datapath) -> Result<()> {
        Ok(())
    }
}
