/// This module contains a test DPDK echo server and client to test basic functionality.
use super::super::{
    ClientSM, CornPtr, Cornflake, Datapath, MsgID, RTTHistogram, ReceivedPacket, ScatterGather,
    ServerSM,
};
use super::connection::DPDKConnection;
use color_eyre::eyre::Result;
use hdrhistogram::Histogram;
use std::{net::Ipv4Addr, rc::Rc, time::Duration};

fn simple_cornflake(size: usize) -> Cornflake<'static> {
    let cornptr = CornPtr::Owned(Rc::new(vec![b'a'; size]));
    let mut cornflake = Cornflake::default();
    cornflake.add_entry(cornptr);
    cornflake
}

pub struct EchoClient<'a> {
    sent: usize,
    recved: usize,
    last_sent_id: MsgID,
    retries: usize,
    sga: Cornflake<'a>,
    server_ip: Ipv4Addr,
    rtts: Histogram<u64>,
}

impl<'a> EchoClient<'a> {
    pub fn new(size: usize, server_ip: Ipv4Addr) -> EchoClient<'a> {
        EchoClient {
            sent: 0,
            recved: 0,
            last_sent_id: 0,
            retries: 0,
            sga: simple_cornflake(size),
            server_ip: server_ip,
            rtts: Histogram::new_with_max(10_000_000_000, 2).unwrap(),
        }
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
}

impl<'a> ClientSM for EchoClient<'a> {
    type Datapath = DPDKConnection;
    type OutgoingMsg = Cornflake<'a>;

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
        Ok(())
    }

    fn msg_timeout_cb(
        &mut self,
        id: MsgID,
        mut send_fn: impl FnMut(Self::OutgoingMsg) -> Result<()>,
    ) -> Result<()> {
        tracing::debug!(id, "Retry callback");
        self.retries += 1;
        self.sga.set_id(id);
        send_fn(self.sga.clone())
    }
}

impl<'a> RTTHistogram for EchoClient<'a> {
    fn get_histogram(&mut self) -> &mut Histogram<u64> {
        &mut self.rtts
    }
}

pub struct EchoServer<'a> {
    sga: Cornflake<'a>,
}

impl<'a> EchoServer<'a> {
    pub fn new(size: usize) -> EchoServer<'a> {
        EchoServer {
            sga: simple_cornflake(size),
        }
    }
}

impl<'a> ServerSM for EchoServer<'a> {
    type Datapath = DPDKConnection;
    type OutgoingMsg = Cornflake<'a>;

    fn process_request(
        &mut self,
        sga: &<<Self as ServerSM>::Datapath as Datapath>::ReceivedPkt,
        mut send_fn: impl FnMut(Self::OutgoingMsg, Ipv4Addr) -> Result<()>,
    ) -> Result<()> {
        let addr = sga.get_addr();
        let mut out_sga = self.sga.clone();
        out_sga.set_id(sga.get_id());
        send_fn(out_sga, addr.ipv4_addr)
    }
}
