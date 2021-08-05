pub mod ycsb_parser;
use byteorder::{BigEndian, ByteOrder, LittleEndian};
use color_eyre::eyre::{bail, Result, WrapErr};
use cornflakes_libos::{
    timing::{HistogramWrapper, ManualHistogram},
    utils::AddressInfo,
    CfBuf, ClientSM, Cornflake, Datapath, MsgID, ReceivedPacket, ScatterGather, ServerSM,
};
use hashbrown::HashMap;
use std::{
    fs::File,
    io::{prelude::*, BufReader, Write},
    marker::PhantomData,
    net::Ipv4Addr,
    sync::{Arc, Mutex},
    time::Duration,
};
use ycsb_parser::YCSBRequest;

pub const CF_ID_SIZE: usize = 4;
pub const REQ_TYPE_SIZE: usize = 4;
pub const MAX_REQ_SIZE: usize = 9216;
pub const ALIGN_SIZE: usize = 256;

pub struct YCSBClient<S, D>
where
    S: SerializedRequestGenerator,
    D: Datapath,
{
    /// Actual serializer.
    serializer: S,
    /// How large are the values are we using?
    /// Required to calculate the serialized object size.
    value_size: usize,
    /// Number of values in GetM or PutM request (required for framing).
    num_values: usize,
    /// Which client id this is. Only transmit requests for your client ID.
    client_id: usize,
    /// Thread id. Only transmit request for your thread ID.
    thread_id: usize,
    /// Requests for this client, this thread.
    requests: Vec<String>,
    server_ip: Ipv4Addr,
    sent: usize,
    recved: usize,
    retries: usize,
    last_sent_id: usize,
    rtts: ManualHistogram,
    _marker: PhantomData<D>,
    request_data: Vec<u8>,
}

impl<S, D> YCSBClient<S, D>
where
    S: SerializedRequestGenerator,
    D: Datapath,
{
    pub fn new(
        client_id: usize,
        value_size: usize,
        num_values: usize,
        trace_file: &str,
        thread_id: usize,
        total_threads: usize,
        server_ip: Ipv4Addr,
        rtts: ManualHistogram,
    ) -> Result<Self> {
        let mut req_lines: Vec<String> = Vec::default();
        let file = File::open(trace_file)?;
        let reader = BufReader::new(file);

        let mut cur_thread_id = 0;
        for line_res in reader.lines() {
            let line = line_res?;
            let req = YCSBRequest::new(&line, num_values, value_size)?;

            if req.client_id != client_id {
                continue;
            }

            if cur_thread_id != thread_id {
                continue;
            }

            cur_thread_id = (cur_thread_id + 1) % total_threads;
            req_lines.push(line.to_string());
        }

        Ok(YCSBClient {
            serializer: S::new(),
            value_size: value_size,
            num_values: num_values,
            client_id: client_id,
            thread_id: thread_id,
            requests: req_lines,
            server_ip: server_ip,
            sent: 0,
            recved: 0,
            retries: 0,
            last_sent_id: 0,
            rtts: rtts,
            _marker: PhantomData,
            request_data: vec![0u8; 9216],
        })
    }

    pub fn dump(&mut self, path: Option<String>, total_time: Duration) -> Result<()> {
        self.rtts.sort();
        tracing::info!(
            thread = self.thread_id,
            client_id = self.client_id,
            sent = self.sent,
            received = self.recved,
            retries = self.retries,
            unique_sent = self.last_sent_id,
            total_time = ?total_time.as_secs_f64(),
            "High level sending stats",
        );
        self.rtts.dump("End-to-end kv client RTTs:")?;

        match path {
            Some(p) => {
                self.rtts.log_to_file(&p)?;
            }
            None => {}
        }
        Ok(())
    }

    pub fn get_num_recved(&self) -> usize {
        self.recved
    }
}

impl<S, D> ClientSM for YCSBClient<S, D>
where
    S: SerializedRequestGenerator,
    D: Datapath,
{
    type Datapath = D;

    fn server_ip(&self) -> Ipv4Addr {
        self.server_ip
    }

    fn received_so_far(&self) -> usize {
        self.recved
    }

    fn get_next_msg(&mut self) -> Result<(MsgID, &[u8])> {
        self.last_sent_id += 1;
        let mut req = YCSBRequest::new(
            &self.requests[self.last_sent_id - 1],
            self.num_values,
            self.value_size,
        )?;
        let size = self.serializer.write_next_framed_request(
            self.last_sent_id - 1,
            &mut self.request_data.as_mut_slice(),
            &mut req,
        )?;
        Ok((
            self.last_sent_id as u32,
            &self.request_data.as_slice()[CF_ID_SIZE..size],
        ))
    }

    fn process_received_msg(
        &mut self,
        _sga: <<Self as ClientSM>::Datapath as Datapath>::ReceivedPkt,
        rtt: Duration,
    ) -> Result<()> {
        self.recved += 1;
        tracing::debug!("Receiving {}th packet", self.recved);
        self.rtts.record(rtt.as_nanos() as u64);
        Ok(())
    }

    fn init(&mut self, _connection: &mut Self::Datapath) -> Result<()> {
        Ok(())
    }

    fn cleanup(&mut self, _connection: &mut Self::Datapath) -> Result<()> {
        Ok(())
    }

    fn msg_timeout_cb(&mut self, id: MsgID) -> Result<(MsgID, &[u8])> {
        tracing::info!(id, last_sent = self.last_sent_id, "Retry callback");
        self.retries += 1;
        let mut req = YCSBRequest::new(
            &self.requests[self.last_sent_id - 1],
            self.num_values,
            self.value_size,
        )?;
        let size = self.serializer.write_next_framed_request(
            self.last_sent_id - 1,
            &mut self.request_data.as_mut_slice(),
            &mut req,
        )?;
        Ok((id, &self.request_data.as_slice()[CF_ID_SIZE..size]))
    }
}

// Each client serialization library must implement this to generate framed requests for the server
// to parse.
pub trait SerializedRequestGenerator {
    /// New serializer
    fn new() -> Self
    where
        Self: Sized;

    /// Get the next request, in bytes.
    /// Buf starts ahead of whatever message framing is required.
    fn write_next_request<'a>(&self, buf: &mut [u8], req: &mut YCSBRequest<'a>) -> Result<usize>;

    /// Returns the request size.
    fn write_next_framed_request(
        &self,
        id: usize,
        buf: &mut [u8],
        req_data: &mut YCSBRequest,
    ) -> Result<usize> {
        // Write in id in first four bytes (little endian).
        LittleEndian::write_u32(&mut buf[0..4], id as u32);
        // Write in the request type (big endian. hardware might read these fields?).
        match req_data.req_type {
            MsgType::Get(size) => {
                BigEndian::write_u16(&mut buf[4..6], 0);
                BigEndian::write_u16(&mut buf[6..8], size as u16);
            }
            MsgType::Put(size) => {
                BigEndian::write_u16(&mut buf[4..6], 1);
                BigEndian::write_u16(&mut buf[6..8], size as u16);
            }
        }
        Ok(self.write_next_request(&mut buf[8..], req_data)? + CF_ID_SIZE + REQ_TYPE_SIZE)
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum MsgType {
    Get(usize),
    Put(usize),
}

pub trait KVSerializer<D>
where
    D: Datapath,
{
    type HeaderCtx;

    fn new(serialize_to_native_buffers: bool) -> Result<Self>
    where
        Self: Sized;

    /// Peforms get request
    fn handle_get(
        &self,
        pkt: D::ReceivedPkt,
        map: &HashMap<String, CfBuf<D>>,
        num_values: usize,
    ) -> Result<(Self::HeaderCtx, Cornflake)>;

    fn handle_put(
        &self,
        pkt: D::ReceivedPkt,
        map: &mut HashMap<String, CfBuf<D>>,
        num_values: usize,
    ) -> Result<(Self::HeaderCtx, Cornflake)>;

    /// Integrates the header ctx object into the cornflake.
    fn process_header(&self, ctx: &Self::HeaderCtx, cornflake: &mut Cornflake) -> Result<()>;
}

pub struct KVServer<S, D>
where
    D: Datapath,
    S: KVSerializer<D>,
{
    map: HashMap<String, CfBuf<D>>,
    serializer: S,
}

impl<S, D> KVServer<S, D>
where
    D: Datapath,
    S: KVSerializer<D>,
{
    pub fn new(serialize_to_native_buffers: bool) -> Result<Self> {
        let serializer = S::new(serialize_to_native_buffers)
            .wrap_err("Could not initialize server serializer.")?;
        Ok(KVServer {
            map: HashMap::default(),
            serializer: serializer,
        })
    }

    pub fn load(
        &mut self,
        trace_file: &str,
        connection: &mut D,
        value_size: usize,
        num_values: usize,
    ) -> Result<()> {
        // do something with the trace file here
        let file = File::open(trace_file)?;
        let reader = BufReader::new(file);

        for line_res in reader.lines() {
            let line = line_res?;
            let mut req = YCSBRequest::new(&line, num_values, value_size)?;
            match req.get_type() {
                MsgType::Get(_) => {
                    bail!("Loading trace file cannot have a get!");
                }
                MsgType::Put(_) => {
                    while let Some((key, val)) = req.next() {
                        // allocate a CfBuf from the datapath.
                        let mut buffer = CfBuf::allocate(connection, value_size, ALIGN_SIZE)
                            .wrap_err("Failed to allocate CfBuf")?;
                        // write in the value to the buffer
                        if buffer
                            .write(val.as_bytes())
                            .wrap_err("Failed to write bytes into CfBuf.")?
                            != val.len()
                        {
                            bail!("Failed to write all of the value bytes into CfBuf.");
                        }
                        // insert into the hash map
                        self.map.insert(key, buffer);
                    }
                }
            }
        }
        Ok(())
    }
}

impl<S, D> ServerSM for KVServer<S, D>
where
    D: Datapath,
    S: KVSerializer<D>,
{
    type Datapath = D;

    fn init(&mut self, _connection: &mut Self::Datapath) -> Result<()> {
        Ok(())
    }

    fn cleanup(&mut self, _connection: &mut Self::Datapath) -> Result<()> {
        Ok(())
    }

    fn process_requests(
        &mut self,
        sgas: Vec<(
            <<Self as ServerSM>::Datapath as Datapath>::ReceivedPkt,
            Duration,
        )>,
        conn: &mut D,
    ) -> Result<()> {
        let mut out_sgas: Vec<(Cornflake, AddressInfo)> = Vec::with_capacity(sgas.len());
        let mut contexts: Vec<S::HeaderCtx> = Vec::default();
        for (in_sga, _) in sgas.into_iter() {
            // process the framing in the msg
            let msg_type = {
                // read the first byte of the packet to determine the request type
                let msg_type_buf = &in_sga.index(0).as_ref()[0..2];
                let msg_size_buf = &in_sga.index(0).as_ref()[2..4];
                match (
                    BigEndian::read_u16(msg_type_buf),
                    BigEndian::read_u16(msg_size_buf),
                ) {
                    (0, size) => MsgType::Get(size as usize),
                    (1, size) => MsgType::Put(size as usize),
                    _ => {
                        bail!("unrecognized message type for kv store app.");
                    }
                }
            };
            let addr = in_sga.get_addr().clone();
            let (header_ctx, mut cf) = match msg_type {
                MsgType::Get(size) => self.serializer.handle_get(in_sga, &self.map, size),
                MsgType::Put(size) => self.serializer.handle_put(in_sga, &mut self.map, size),
            }?;
            self.serializer.process_header(&header_ctx, &mut cf)?;
            contexts.push(header_ctx);
            out_sgas.push((cf, addr));
        }

        conn.push_sgas(&out_sgas)
            .wrap_err("Unable to send response sgas in datapath.")?;
        Ok(())
    }

    fn get_histograms(&self) -> Vec<Arc<Mutex<HistogramWrapper>>> {
        Vec::default()
    }
}
