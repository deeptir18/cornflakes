/// This module contains a test DPDK echo server and client to test basic functionality.
use super::super::{
    mem,
    timing::{record, HistogramWrapper, RTTHistogram},
    utils::AddressInfo,
    ClientSM, CornPtr, Cornflake, Datapath, MsgID, PtrAttributes, ReceivedPacket, ScatterGather,
    ServerSM,
};
use super::connection::DPDKConnection;
use color_eyre::eyre::{bail, Result};
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

/*fn simple_cornflake(payload: &'a Vec<u8>) -> Cornflake<'a> {
    let cornptr = CornPtr::Normal(payload.as_ref());
    let mut cornflake = Cornflake::default();
    cornflake.add_entry(cornptr);
    cornflake
}*/

pub struct EchoClient<'a, 'b> {
    sent: usize,
    recved: usize,
    last_sent_id: MsgID,
    retries: usize,
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
                let mut metadata = mem::mmap_manual(10)?;
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

    pub fn get_num_sent(&self) -> usize {
        self.sent
    }
}

impl<'a, 'b> ClientSM for EchoClient<'a, 'b> {
    type Datapath = DPDKConnection;
    type OutgoingMsg = Cornflake<'a, 'b>;

    fn init(&mut self, connection: &mut Self::Datapath) -> Result<()> {
        match self.external_memory {
            Some(ref metadata) => {
                connection.register_external_region(metadata.clone())?;
            }
            None => {}
        }
        Ok(())
    }

    fn server_ip(&self) -> Ipv4Addr {
        self.server_ip
    }

    fn send_next_msg(
        &mut self,
        mut send_fn: impl FnMut(Self::OutgoingMsg) -> Result<()>,
    ) -> Result<()> {
        tracing::debug!(id = self.last_sent_id + 1, "Sending");
        self.last_sent_id += 1;
        self.sga.set_id(self.last_sent_id);
        self.sent += 1;
        send_fn(self.sga.clone())
    }

    fn process_received_msg(
        &mut self,
        sga: <<Self as ClientSM>::Datapath as Datapath>::ReceivedPkt,
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

    fn msg_timeout_cb(
        &mut self,
        id: MsgID,
        mut send_fn: impl FnMut(Self::OutgoingMsg) -> Result<()>,
    ) -> Result<()> {
        tracing::info!(id, last_sent = self.last_sent_id, "Retry callback");
        self.retries += 1;
        self.sga.set_id(id);
        send_fn(self.sga.clone())
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
    //type OutgoingMsg = Cornflake<'a>;

    fn init(&mut self, _connection: &mut Self::Datapath) -> Result<()> {
        Ok(())
    }

    fn process_requests(
        &mut self,
        sgas: &Vec<(
            <<Self as ServerSM>::Datapath as Datapath>::ReceivedPkt,
            Duration,
        )>,
        mut send_fn: impl FnMut(&Vec<(Cornflake, AddressInfo)>) -> Result<()>,
    ) -> Result<()> {
        let proc_timer = self.get_timer(SERVER_PROCESSING_LATENCY, cfg!(feature = "timers"))?;
        let start = Instant::now();
        let mut out_sgas: Vec<(Cornflake, AddressInfo)> = Vec::with_capacity(sgas.len());
        for in_sga in sgas.iter() {
            let mut cornflake = Cornflake::with_capacity(1);
            cornflake.add_entry(in_sga.0.get_corn_ptr());
            tracing::debug!(
                input_cornptr_length = in_sga.0.get_corn_ptr().buf_size(),
                "length of input cornptr packet"
            );
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
        send_fn(&out_sgas)
    }

    fn get_histograms(&self) -> Vec<Arc<Mutex<HistogramWrapper>>> {
        self.histograms.iter().map(|(_, h)| h.clone()).collect()
    }
}
