/// This module contains a test DPDK echo server and client to test basic functionality.
use super::super::{
    mem, ClientSM, CornPtr, Cornflake, Datapath, MsgID, RTTHistogram, ReceivedPacket,
    ScatterGather, ServerSM,
};
use super::connection::DPDKConnection;
use color_eyre::eyre::Result;
use hdrhistogram::Histogram;
use memmap::MmapMut;
use std::{io::Write, net::Ipv4Addr, rc::Rc, slice, time::Duration};

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
    external_memory: Option<(mem::MmapMetadata, MmapMut)>,
}

impl<'a> EchoClient<'a> {
    pub fn new(size: usize, server_ip: Ipv4Addr, zero_copy: bool) -> Result<EchoClient<'a>> {
        let (sga, external_memory) = match zero_copy {
            true => {
                let (metadata, mut mmap) = mem::mmap_new(100)?;
                assert!(size <= metadata.length);
                let payload = vec![b'a'; size];
                (&mut mmap[..]).write_all(payload.as_ref())?;
                let cornptr =
                    unsafe { CornPtr::Borrowed(slice::from_raw_parts(metadata.ptr, size)) };
                let mut cornflake = Cornflake::default();
                cornflake.add_entry(cornptr);
                (cornflake, Some((metadata, mmap)))
            }
            false => (simple_cornflake(size), None),
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
}

impl<'a> ClientSM for EchoClient<'a> {
    type Datapath = DPDKConnection;
    type OutgoingMsg = Cornflake<'a>;

    fn init(&mut self, connection: &mut Self::Datapath) -> Result<()> {
        match self.external_memory {
            Some((ref metadata, _)) => {
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
    external_memory: Option<(mem::MmapMetadata, MmapMut)>,
}

impl<'a> EchoServer<'a> {
    pub fn new(size: usize, zero_copy: bool) -> Result<EchoServer<'a>> {
        let (sga, external_memory) = match zero_copy {
            true => {
                let (metadata, mut mmap) = mem::mmap_new(100)?;
                assert!(size <= metadata.length);
                let payload = vec![b'a'; size];
                (&mut mmap[..]).write_all(payload.as_ref())?;
                // this application is just testing if external memory works at all
                // so we are just initializing the external memory unsafely
                let cornptr =
                    unsafe { CornPtr::Borrowed(slice::from_raw_parts(metadata.ptr, size)) };
                let mut cornflake = Cornflake::default();
                cornflake.add_entry(cornptr);
                (cornflake, Some((metadata, mmap)))
            }
            false => (simple_cornflake(size), None),
        };
        Ok(EchoServer {
            sga: sga,
            external_memory: external_memory,
        })
    }
}

impl<'a> ServerSM for EchoServer<'a> {
    type Datapath = DPDKConnection;
    type OutgoingMsg = Cornflake<'a>;

    fn init(&mut self, connection: &mut Self::Datapath) -> Result<()> {
        match self.external_memory {
            Some((ref metadata, _)) => {
                connection.register_external_region(metadata.clone())?;
            }
            None => {}
        }
        Ok(())
    }

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
