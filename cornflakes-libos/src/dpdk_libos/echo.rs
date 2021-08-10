/// This module contains a test DPDK echo server and client to test basic functionality.
use super::super::{
    mem,
    timing::{record, HistogramWrapper, RTTHistogram},
    utils::AddressInfo,
    ClientSM, CornPtr, Cornflake, Datapath, MsgID, PtrAttributes, ReceivedPkt, ScatterGather,
    ServerSM,
};
use super::connection::DPDKConnection;
use color_eyre::eyre::{bail, Result, WrapErr};
use hashbrown::HashMap;
use hdrhistogram::Histogram;
use std::{
    io::Write,
    net::Ipv4Addr,
    /*rc::Rc,*/ slice,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

const SERVER_PROCESSING_LATENCY: &str = "SERVER_PROC_LATENCY";

pub struct EchoClient<'a, 'b> {
    sent: usize,
    recved: usize,
    last_sent_id: MsgID,
    retries: usize,
    buffer: Vec<u8>,
    sga: Cornflake<'a, 'b>,
    server_ip: Ipv4Addr,
    rtts: Histogram<u64>,
    external_memory: Option<mem::MmapMetadata>,
}

impl<'a, 'b> EchoClient<'a, 'b> {
    pub fn new(
        size: usize,
        server_ip: Ipv4Addr,
        zero_copy: bool,
        payload: &'b [u8],
    ) -> Result<EchoClient<'a, 'b>> {
        let (sga, external_memory) = match zero_copy {
            true => {
                let mut metadata = mem::MmapMetadata::new(10)?;
                assert!(size <= metadata.length);
                let payload = vec![b'a'; size];
                let buf = metadata.get_full_buf()?;
                (&mut buf[0..payload.len()]).write_all(payload.as_ref())?;
                tracing::debug!(size = size, buf_len = buf.len(), "client buf len");
                let cornptr =
                    unsafe { CornPtr::Registered(slice::from_raw_parts(metadata.ptr, size)) };
                let mut cornflake = Cornflake::default();
                cornflake.add_entry(cornptr);
                (cornflake, Some(metadata))
            }
            false => {
                let cornptr = CornPtr::Normal(payload.as_ref());
                let mut cornflake = Cornflake::default();
                cornflake.add_entry(cornptr);
                (cornflake, None)
            }
        };
        Ok(EchoClient {
            sent: 0,
            recved: 0,
            last_sent_id: 0,
            retries: 0,
            buffer: sga.contiguous_repr(),
            sga: sga,
            server_ip: server_ip,
            rtts: Histogram::new_with_max(10_000_000_000, 2).unwrap(),
            external_memory: external_memory,
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
        self.dump("End-to-end DPDK echo client RTTs:");
    }

    pub fn get_num_recved(&self) -> usize {
        self.recved
    }
}

impl<'a, 'b> ClientSM for EchoClient<'a, 'b> {
    type Datapath = DPDKConnection;

    fn init(&mut self, connection: &mut Self::Datapath) -> Result<()> {
        match self.external_memory {
            Some(ref mut metadata) => {
                connection.register_external_region(metadata)?;
            }
            None => {}
        }
        Ok(())
    }

    fn received_so_far(&self) -> usize {
        self.recved
    }

    fn cleanup(&mut self, connection: &mut Self::Datapath) -> Result<()> {
        match self.external_memory {
            Some(ref mut metadata) => {
                connection.unregister_external_region(metadata)?;
                metadata.free_mmap();
            }
            None => {}
        }
        Ok(())
    }

    fn server_ip(&self) -> Ipv4Addr {
        self.server_ip
    }

    fn get_next_msg(&mut self) -> Result<(MsgID, &[u8])> {
        tracing::debug!(id = self.last_sent_id + 1, "Sending");
        self.last_sent_id += 1;
        self.sent += 1;
        Ok((self.last_sent_id, &self.buffer.as_slice()))
    }

    fn process_received_msg(
        &mut self,
        sga: ReceivedPkt<<Self as ClientSM>::Datapath>,
        time: Duration,
    ) -> Result<()> {
        self.recved += 1;
        self.add_latency(time.as_nanos() as u64)?;

        tracing::debug!(
            msg_id = sga.get_id(),
            num_segments = sga.num_segments(),
            data_len = sga.data_len(),
            "Received sga",
        );

        if cfg!(debug_assertions) {
            // check that the payloads equal each other
            assert!(
                self.sga.contiguous_repr() == sga.contiguous_repr(),
                "Received payload does not match our payload."
            );
        }

        Ok(())
    }

    fn msg_timeout_cb(&mut self, id: MsgID) -> Result<(MsgID, &[u8])> {
        tracing::info!(id, last_sent = self.last_sent_id, "Retry callback");
        self.retries += 1;
        Ok((id, &self.buffer.as_slice()))
    }
}

impl<'a, 'b> RTTHistogram for EchoClient<'a, 'b> {
    fn get_histogram_mut(&mut self) -> &mut Histogram<u64> {
        &mut self.rtts
    }

    fn get_histogram(&self) -> &Histogram<u64> {
        &self.rtts
    }
}

pub struct EchoServer {
    histograms: HashMap<String, Arc<Mutex<HistogramWrapper>>>,
}

impl EchoServer {
    pub fn new() -> Result<EchoServer> {
        let mut histograms: HashMap<String, Arc<Mutex<HistogramWrapper>>> = HashMap::default();
        if cfg!(feature = "timers") {
            histograms.insert(
                SERVER_PROCESSING_LATENCY.to_string(),
                Arc::new(Mutex::new(HistogramWrapper::new(
                    SERVER_PROCESSING_LATENCY,
                )?)),
            );
        }
        Ok(EchoServer {
            histograms: histograms,
        })
    }

    fn get_timer(&self, name: &str, cond: bool) -> Result<Option<Arc<Mutex<HistogramWrapper>>>> {
        if !cond {
            return Ok(None);
        }
        match self.histograms.get(name) {
            Some(h) => Ok(Some(h.clone())),
            None => bail!("Timer {} not in histograms."),
        }
    }
}

impl ServerSM for EchoServer {
    type Datapath = DPDKConnection;

    fn init(&mut self, _connection: &mut Self::Datapath) -> Result<()> {
        Ok(())
    }

    fn cleanup(&mut self, _connection: &mut Self::Datapath) -> Result<()> {
        Ok(())
    }

    fn process_requests(
        &mut self,
        sgas: Vec<(ReceivedPkt<<Self as ServerSM>::Datapath>, Duration)>,
        conn: &mut Self::Datapath,
    ) -> Result<()> {
        let proc_timer = self.get_timer(SERVER_PROCESSING_LATENCY, cfg!(feature = "timers"))?;
        let start = Instant::now();
        let mut out_sgas: Vec<(Cornflake, AddressInfo)> = Vec::with_capacity(sgas.len());
        for in_sga in sgas.iter() {
            let mut cornflake = Cornflake::with_capacity(1);
            let cornptr = CornPtr::Registered(in_sga.0.index(0).as_ref());
            tracing::debug!(
                input_cornptr_length = cornptr.buf_size(),
                "length of input cornptr packet"
            );
            cornflake.add_entry(cornptr);
            cornflake.set_id(in_sga.0.get_id());
            tracing::debug!(
                "Setting cornptr to {:?}; usize: {:?}",
                in_sga.0.index(0).as_ref().as_ptr(),
                in_sga.0.index(0).as_ref().as_ptr() as usize
            );
            out_sgas.push((cornflake, in_sga.0.get_addr().clone()));
        }
        if cfg!(feature = "timers") {
            record(proc_timer, start.elapsed().as_nanos() as u64)?;
        }
        conn.push_sgas(&out_sgas)
            .wrap_err("Unable to push sgas out in datapath.")?;
        Ok(())
    }

    fn get_histograms(&self) -> Vec<Arc<Mutex<HistogramWrapper>>> {
        self.histograms.iter().map(|(_, h)| h.clone()).collect()
    }
}
