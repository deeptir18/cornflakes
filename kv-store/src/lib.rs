pub mod cornflakes_dynamic;
pub mod ycsb_parser;
use byteorder::{BigEndian, ByteOrder};
use color_eyre::eyre::{bail, eyre, Result, WrapErr};
use cornflakes_libos::{
    timing::{HistogramWrapper, ManualHistogram},
    utils::AddressInfo,
    CfBuf, ClientSM, Datapath, MsgID, RcCornflake, ReceivedPkt, ScatterGather, ServerSM,
};
use hashbrown::HashMap;
use std::{
    fs::File,
    io::{prelude::*, BufReader, Lines, Write},
    marker::PhantomData,
    net::Ipv4Addr,
    sync::{Arc, Mutex},
    time::Duration,
};
use ycsb_parser::YCSBRequest;

pub const REQ_TYPE_SIZE: usize = 4;
pub const MAX_REQ_SIZE: usize = 9216;
pub const ALIGN_SIZE: usize = 256;

fn read_msg_framing<D>(in_sga: &ReceivedPkt<D>) -> Result<MsgType>
where
    D: Datapath,
{
    // read the first byte of the packet to determine the request type
    let msg_type_buf = &in_sga.index(0).as_ref()[0..2];
    let msg_size_buf = &in_sga.index(0).as_ref()[2..4];
    match (
        BigEndian::read_u16(msg_type_buf),
        BigEndian::read_u16(msg_size_buf),
    ) {
        (0, size) => Ok(MsgType::Get(size as usize)),
        (1, size) => Ok(MsgType::Put(size as usize)),
        _ => {
            bail!("unrecognized message type for kv store app.");
        }
    }
}

/// Iterator over query file.
/// Ensures that the queries are
pub struct QueryIterator {
    client_id: usize,
    thread_id: usize,
    total_num_threads: usize,
    total_num_clients: usize,
    cur_thread_id: usize,
    cur_client_id: usize,
    lines: Lines<BufReader<File>>,
}

impl QueryIterator {
    pub fn new(
        client_id: usize,
        thread_id: usize,
        total_num_threads: usize,
        total_num_clients: usize,
        trace_file: &str,
    ) -> Result<Self> {
        let file = File::open(trace_file)?;
        let reader = BufReader::new(file);

        Ok(QueryIterator {
            client_id: client_id,
            thread_id: thread_id,
            total_num_threads: total_num_threads,
            total_num_clients: total_num_clients,
            cur_thread_id: 0,
            cur_client_id: 0,
            lines: reader.lines(),
        })
    }

    fn get_client_id(&self) -> usize {
        self.client_id
    }

    fn get_thread_id(&self) -> usize {
        self.thread_id
    }

    fn increment(&mut self) {
        self.increment_client_id_counter();
        if self.client_id == 0 {
            // increment thread when we reach the next client
            self.increment_thread_id_counter();
        }
    }

    fn increment_client_id_counter(&mut self) {
        self.cur_client_id = (self.cur_client_id + 1) % self.total_num_clients;
    }

    fn increment_thread_id_counter(&mut self) {
        self.cur_thread_id = (self.cur_thread_id + 1) % self.total_num_threads;
    }
}

impl Iterator for QueryIterator {
    type Item = Result<String>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // find the next request with our client and thread id
            if self.cur_client_id == self.client_id && self.cur_thread_id == self.thread_id {
                if let Some(parsed_line_res) = self.lines.next() {
                    match parsed_line_res {
                        Ok(s) => {
                            self.increment();
                            return Some(Ok(s));
                        }
                        Err(e) => {
                            return Some(Err(eyre!(format!(
                                "Could not get next line in iterator: {}",
                                e
                            ))));
                        }
                    }
                } else {
                    return None;
                }
            } else {
                if let Some(_) = self.lines.next() {
                    self.increment();
                } else {
                    return None;
                }
            }
        }
    }
}

pub struct YCSBClient<S, D>
where
    S: SerializedRequestGenerator<D>,
    D: Datapath,
{
    /// Actual serializer.
    serializer: S,
    /// How large are the values are we using?
    /// Required to calculate the serialized object size.
    value_size: usize,
    /// Number of values in GetM or PutM request (required for framing).
    num_values: usize,
    /// Iterator over queries.
    queries: QueryIterator,
    /// Which server to send to.
    server_ip: Ipv4Addr,
    /// Currently send window.
    in_flight: HashMap<MsgID, String>,
    /// Sent so far
    sent: usize,
    /// Received so far.
    recved: usize,
    /// Number of retries.
    retries: usize,
    /// Last send message id.
    last_sent_id: usize,
    /// RTTs of requests.
    rtts: ManualHistogram,
    /// Buffer used to store serialized request.
    request_data: Vec<u8>,
    /// Using retries or not.
    using_retries: bool,
    /// If in debug, keep track of MsgID -> MsgType
    message_info: HashMap<MsgID, MsgType>,
    _marker: PhantomData<D>,
}

impl<S, D> YCSBClient<S, D>
where
    S: SerializedRequestGenerator<D>,
    D: Datapath,
{
    pub fn new(
        client_id: usize,
        value_size: usize,
        num_values: usize,
        trace_file: &str,
        thread_id: usize,
        total_threads: usize,
        total_clients: usize,
        server_ip: Ipv4Addr,
        rtts: ManualHistogram,
        using_retries: bool,
    ) -> Result<Self> {
        tracing::info!(
            client_id = client_id,
            value_size = value_size,
            num_values = num_values,
            thread_id = thread_id,
            total_threads = total_threads,
            total_clients = total_clients,
            "Initializing YCSB client"
        );

        let query_iterator = QueryIterator::new(
            client_id,
            thread_id,
            total_threads,
            total_clients,
            trace_file,
        )?;

        Ok(YCSBClient {
            serializer: S::new_request_generator(),
            value_size: value_size,
            num_values: num_values,
            queries: query_iterator,
            server_ip: server_ip,
            sent: 0,
            recved: 0,
            retries: 0,
            last_sent_id: 0,
            rtts: rtts,
            request_data: vec![0u8; 9216],
            in_flight: HashMap::default(),
            using_retries: using_retries,
            message_info: HashMap::default(),
            _marker: PhantomData,
        })
    }

    pub fn dump(
        &mut self,
        path: Option<String>,
        total_time: Duration,
        start_cutoff: usize,
    ) -> Result<()> {
        self.rtts.sort_and_truncate(start_cutoff)?;
        tracing::info!(
            thread = self.queries.get_thread_id(),
            client_id = self.queries.get_client_id(),
            sent = self.sent,
            received = self.recved - start_cutoff,
            retries = self.retries,
            unique_sent = self.last_sent_id - 1 - start_cutoff,
            total_time = ?total_time.as_secs_f64(),
            "High level sending stats",
        );
        self.rtts.dump("End-to-end kv client RTTs:")?;

        match path {
            Some(p) => {
                self.rtts.log_truncated_to_file(&p, start_cutoff)?;
            }
            None => {}
        }
        Ok(())
    }

    pub fn get_num_recved(&self, start_cutoff: usize) -> usize {
        self.recved - start_cutoff
    }
}

impl<S, D> ClientSM for YCSBClient<S, D>
where
    S: SerializedRequestGenerator<D>,
    D: Datapath,
{
    type Datapath = D;

    fn server_ip(&self) -> Ipv4Addr {
        self.server_ip
    }

    fn received_so_far(&self) -> usize {
        self.recved
    }

    fn get_next_msg(&mut self) -> Result<Option<(MsgID, &[u8])>> {
        if let Some(next_line_res) = self.queries.next() {
            let next_line = next_line_res.wrap_err("Not able to get next line from iterator")?;
            self.last_sent_id += 1;
            let mut req = YCSBRequest::new(
                &next_line,
                self.num_values,
                self.value_size,
                (self.last_sent_id - 1) as MsgID,
            )?;
            tracing::debug!("About to send: {:?}", req);
            let size = self
                .serializer
                .write_next_framed_request(&mut self.request_data.as_mut_slice(), &mut req)?;
            if self.using_retries {
                // insert into in flight map
                if !(self.in_flight.contains_key(&(self.last_sent_id as u32 - 1))) {
                    self.in_flight
                        .insert(self.last_sent_id as u32 - 1, next_line.to_string());
                }
            }

            if cfg!(debug_assertions) {
                if !(self
                    .message_info
                    .contains_key(&(self.last_sent_id as u32 - 1)))
                {
                    self.message_info
                        .insert(self.last_sent_id as u32 - 1, req.get_type());
                }
            }

            tracing::debug!("Returning msg to send");
            Ok(Some((
                self.last_sent_id as u32 - 1,
                &self.request_data.as_slice()[0..size],
            )))
        } else {
            return Ok(None);
        }
    }

    fn process_received_msg(
        &mut self,
        sga: ReceivedPkt<<Self as ClientSM>::Datapath>,
        rtt: Duration,
    ) -> Result<()> {
        self.recved += 1;
        tracing::debug!("Receiving {}th packet", self.recved);

        // if debug, deserialize and check the message has the right dimensions
        if cfg!(debug_assertions) {
            if let Some(msg_type) = self.message_info.remove(&sga.get_id()) {
                // run some kind of ``check''
                if !self
                    .serializer
                    .check_recved_msg(&sga, msg_type, self.value_size)?
                {
                    bail!("Msg check failed");
                } else {
                    tracing::debug!("PASSED THE RECV MESSAGE CHECK");
                }
            } else {
                bail!("Received ID not in message map: {}", sga.get_id());
            }
        }
        if self.using_retries {
            if let Some(_) = self.in_flight.remove(&sga.get_id()) {
            } else {
                bail!("Received ID not in in flight map: {}", sga.get_id());
            }
        }
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
        if let Some(line) = self.in_flight.get(&id) {
            let mut req = YCSBRequest::new(&line, self.num_values, self.value_size, id)?;
            let size = self
                .serializer
                .write_next_framed_request(&mut self.request_data.as_mut_slice(), &mut req)?;
            Ok((id, &self.request_data.as_slice()[0..size]))
        } else {
            bail!("Cannot find data for msg # {} to send retry", id);
        }
    }
}

// Each client serialization library must implement this to generate framed requests for the server
// to parse.
pub trait SerializedRequestGenerator<D>
where
    D: Datapath,
{
    /// New serializer
    fn new_request_generator() -> Self
    where
        Self: Sized;

    /// Check the received message
    fn check_recved_msg(
        &self,
        pkt: &ReceivedPkt<D>,
        msg_type: MsgType,
        value_size: usize,
    ) -> Result<bool>;

    /// Get the next request, in bytes.
    /// Buf starts ahead of whatever message framing is required.
    fn write_next_request<'a>(&self, buf: &mut [u8], req: &mut YCSBRequest<'a>) -> Result<usize>;

    /// Returns the request size.
    fn write_next_framed_request(
        &self,
        buf: &mut [u8],
        req_data: &mut YCSBRequest,
    ) -> Result<usize> {
        // Write in the request type (big endian. hardware might read these fields?).
        match req_data.req_type {
            MsgType::Get(size) => {
                BigEndian::write_u16(&mut buf[0..2], 0);
                BigEndian::write_u16(&mut buf[2..4], size as u16);
            }
            MsgType::Put(size) => {
                BigEndian::write_u16(&mut buf[0..2], 1);
                BigEndian::write_u16(&mut buf[2..4], size as u16);
            }
        }
        Ok(self.write_next_request(&mut buf[4..], req_data)? + REQ_TYPE_SIZE)
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

    fn new_server(serialize_to_native_buffers: bool) -> Result<Self>
    where
        Self: Sized;

    /// Peforms get request
    fn handle_get<'a>(
        &self,
        pkt: ReceivedPkt<D>,
        map: &HashMap<String, CfBuf<D>>,
        num_values: usize,
        offset: usize, // to account for any framing
    ) -> Result<(Self::HeaderCtx, RcCornflake<'a, D>)>;

    fn handle_put<'a>(
        &self,
        pkt: ReceivedPkt<D>,
        map: &mut HashMap<String, CfBuf<D>>,
        num_values: usize,
        offset: usize,
    ) -> Result<(Self::HeaderCtx, RcCornflake<'a, D>)>;

    // Integrates the header ctx object into the cornflake.
    fn process_header<'a>(
        &self,
        ctx: &'a Self::HeaderCtx,
        cornflake: &mut RcCornflake<'a, D>,
    ) -> Result<()>;
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
        let serializer = S::new_server(serialize_to_native_buffers)
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
        let mut cur_idx = 0;

        for line_res in reader.lines() {
            let line = line_res?;
            let mut req = YCSBRequest::new(&line, num_values, value_size, cur_idx)?;
            cur_idx += 1;
            match req.get_type() {
                MsgType::Get(_) => {
                    bail!("Loading trace file cannot have a get!");
                }
                MsgType::Put(_) => {
                    while let Some((key, val)) = req.next() {
                        // allocate a CfBuf from the datapath.
                        let mut buffer =
                            CfBuf::allocate(connection, value_size, ALIGN_SIZE).wrap_err(
                                format!("Failed to allocate CfBuf for req # {}", req.get_id()),
                            )?;
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
        tracing::info!("Done loading keys into kv store");
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
        sgas: Vec<(ReceivedPkt<<Self as ServerSM>::Datapath>, Duration)>,
        conn: &mut D,
    ) -> Result<()> {
        let mut out_sgas: Vec<(RcCornflake<D>, AddressInfo)> = Vec::with_capacity(sgas.len());
        let mut contexts: Vec<S::HeaderCtx> = Vec::default();
        for (in_sga, _) in sgas.into_iter() {
            // process the framing in the msg
            let msg_type = read_msg_framing(&in_sga)?;
            tracing::debug!("Parsed {:?} request", msg_type);
            let id = in_sga.get_id();
            let addr = in_sga.get_addr().clone();
            let (header_ctx, mut cf) = match msg_type {
                MsgType::Get(size) => {
                    self.serializer
                        .handle_get(in_sga, &self.map, size, REQ_TYPE_SIZE)
                }
                MsgType::Put(size) => {
                    self.serializer
                        .handle_put(in_sga, &mut self.map, size, REQ_TYPE_SIZE)
                }
            }?;
            cf.set_id(id);
            contexts.push(header_ctx);
            out_sgas.push((cf, addr));
        }

        for i in 0..out_sgas.len() {
            let (cf, _addr) = &mut out_sgas[i];
            let ctx = &contexts[i];
            self.serializer.process_header(ctx, cf)?;
        }

        conn.push_sgas(&out_sgas)
            .wrap_err("Unable to send response sgas in datapath.")?;
        Ok(())
    }

    fn get_histograms(&self) -> Vec<Arc<Mutex<HistogramWrapper>>> {
        Vec::default()
    }
}

impl<S, D> Drop for KVServer<S, D>
where
    S: KVSerializer<D>,

    D: Datapath,
{
    fn drop(&mut self) {
        tracing::debug!("In drop for KV Server");
    }
}
