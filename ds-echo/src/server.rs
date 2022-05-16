use super::ServerCerealizeMessage;
use color_eyre::eyre::Result;
use cornflakes_libos::{
    datapath::{Datapath, PushBufType, ReceivedPkt},
    state_machine::server::ServerSM,
    ConnID, MsgID, OrderedSga, RcSga, RcSge,
};
use std::marker::PhantomData;

pub struct EchoServer<'a, S, D>
where
    S: ServerCerealizeMessage<D>,
    D: Datapath,
{
    serializer: S,
    push_buf_type: PushBufType,
    echo_pkt_mode: bool,
    reuse_ctx: bool,
    sgas: Vec<OrderedSga<'a>>,
    rcsgas: Vec<RcSga<'a, D>>,
    buffers: Vec<Vec<u8>>,
    _marker: PhantomData<D>,
}

impl<'a, S, D> EchoServer<'a, S, D>
where
    S: ServerCerealizeMessage<D>,
    D: Datapath,
{
    pub fn new(serializer: S, push_buf_mode: PushBufType, reuse_ctx: bool) -> EchoServer<'a, S, D> {
        let max_entries = D::max_scatter_gather_entries();
        let batch_size = D::batch_size();
        let max_data_size = D::max_packet_size();
        let rcsgas: Vec<RcSga<D>> = (0..max_entries)
            .map(|_| RcSga::allocate(max_entries))
            .collect();
        let sgas = vec![OrderedSga::allocate(max_entries); batch_size];
        let bufs = vec![vec![0u8; max_data_size]; batch_size];
        EchoServer {
            serializer: serializer,
            push_buf_type: push_buf_mode,
            echo_pkt_mode: false,
            reuse_ctx: reuse_ctx,
            sgas: sgas,
            rcsgas: rcsgas,
            buffers: bufs,
            _marker: PhantomData,
        }
    }

    pub fn set_echo_pkt_mode(&mut self) {
        self.echo_pkt_mode = true;
    }
}

impl<'a, S, D> ServerSM for EchoServer<'a, S, D>
where
    S: ServerCerealizeMessage<D>,
    D: Datapath,
{
    type Datapath = D;

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

    fn push_buf_type(&self) -> PushBufType {
        self.push_buf_type
    }

    fn cleanup(&mut self, _connection: &mut Self::Datapath) -> Result<()> {
        Ok(())
    }

    fn process_requests_single_buf(
        &mut self,
        pkts: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        datapath: &mut Self::Datapath,
    ) -> Result<()> {
        Ok(())
    }

    fn process_requests_rc_sga(
        &mut self,
        pkts: Vec<ReceivedPkt<Self::Datapath>>,
        datapath: &mut Self::Datapath,
    ) -> Result<()> {
        if self.reuse_ctx {
            // reuse existing sgas
            for ((pkt, rcsga), buffer) in pkts
                .iter()
                .zip(self.rcsgas.iter_mut().take(pkts.len()))
                .zip(self.buffers.iter_mut().take(pkts.len()))
            {
                self.serializer.process_request_rcsga(pkt, rcsga, buffer)?;
            }
            let mut send_buffers: Vec<(MsgID, ConnID, &mut RcSga<Self::Datapath>)> = pkts
                .iter()
                .zip(self.rcsgas.iter_mut())
                .enumerate()
                .map(|(i, (pkt, rcsga))| (pkt.msg_id(), pkt.conn_id(), rcsga))
                .collect();
            datapath.push_rc_sgas(send_buffers.as_mut_slice())?;
        } else {
            let mut rcsgas: Vec<RcSga<Self::Datapath>> = Vec::with_capacity(pkts.len());
            let mut buffers: Vec<Vec<u8>> = Vec::with_capacity(pkts.len());
            for pkt in pkts.iter() {
                let (rcsga, header_buffer) = self.serializer.return_rcsga(pkt)?;
                buffers.push(header_buffer);
                rcsgas.push(rcsga);
            }

            let mut send_buffers: Vec<(MsgID, ConnID, &mut RcSga<Self::Datapath>)> = pkts
                .iter()
                .zip(rcsgas.iter_mut())
                .zip(buffers.iter())
                .enumerate()
                .map(|(i, ((pkt, rcsga), buffer))| {
                    rcsga.replace(0, RcSge::RawRef(buffer));
                    (pkt.msg_id(), pkt.conn_id(), rcsga)
                })
                .collect();

            datapath.push_rc_sgas(send_buffers.as_mut_slice())?;
        }

        Ok(())
    }

    fn process_requests_sga(
        &mut self,
        pkts: Vec<ReceivedPkt<Self::Datapath>>,
        datapath: &mut Self::Datapath,
    ) -> Result<()> {
        Ok(())
    }

    fn process_requests_ordered_sga(
        &mut self,
        pkts: Vec<ReceivedPkt<Self::Datapath>>,
        datapath: &mut Self::Datapath,
    ) -> Result<()> {
        Ok(())
    }
}
