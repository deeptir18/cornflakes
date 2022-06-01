pub mod cornflakes_dynamic;
pub mod retwis;
pub mod run_datapath;
pub mod ycsb;

use byteorder::{BigEndian, ByteOrder};
use color_eyre::eyre::{bail, Result};
use cornflakes_libos::{
    allocator::MempoolID,
    datapath::{Datapath, ReceivedPkt},
    state_machine::client::ClientSM,
    timing::ManualHistogram,
    utils::AddressInfo,
    MsgID,
};
use hashbrown::HashMap;
use std::{
    fs::File,
    io::{prelude::*, BufReader},
    marker::PhantomData,
};

const MIN_MEMPOOL_SIZE: usize = 8192;

fn align_to_power(size: usize) -> Result<usize> {
    let available_sizes = [32, 64, 128, 256, 512, 1024, 2048, 4096, 8192];
    for mempool_size in available_sizes.iter() {
        if *mempool_size >= size {
            return Ok(*mempool_size);
        }
    }
    bail!("Provided size {} too large; larger than 8192", size);
}

// 8 bytes at front of message for framing
pub const REQ_TYPE_SIZE: usize = 4;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum MsgType {
    Get,               // single get
    Put,               // single put
    GetM(u16),         // multiple key get
    PutM(u16),         // multiple key put
    GetList(u16),      // get list
    PutList(u16),      // put list
    AppendToList(u16), // append to list
}

impl MsgType {
    /// Reads first four bytes of packet to determine message type.
    fn from_packet<D: Datapath>(packet: &ReceivedPkt<D>) -> Result<Self> {
        let buf = &packet.seg(0).as_ref();
        let msg_type = &buf[0..2];
        let size = &buf[2..4];

        match (BigEndian::read_u16(msg_type), BigEndian::read_u16(size)) {
            (0, 1) => Ok(MsgType::Get),
            (1, 1) => Ok(MsgType::Put),
            (2, size) => Ok(MsgType::GetM(size)),
            (3, size) => Ok(MsgType::PutM(size)),
            (4, size) => Ok(MsgType::GetList(size)),
            (5, size) => Ok(MsgType::PutList(size)),
            (6, size) => Ok(MsgType::AppendToList(size)),
            _ => {
                bail!("unrecognized message type for kv store app.");
            }
        }
    }

    /// Writes message type into first four bytes of provided buffer.
    fn to_buf(&self, buf: &mut [u8]) {
        match self {
            MsgType::Get => {
                BigEndian::write_u16(&mut buf[0..2], 0);
                BigEndian::write_u16(&mut buf[2..4], 1);
            }
            MsgType::Put => {
                BigEndian::write_u16(&mut buf[0..2], 1);
                BigEndian::write_u16(&mut buf[2..4], 1);
            }
            MsgType::GetM(size) => {
                BigEndian::write_u16(&mut buf[0..2], 2);
                BigEndian::write_u16(&mut buf[2..4], *size);
            }
            MsgType::PutM(size) => {
                BigEndian::write_u16(&mut buf[0..2], 3);
                BigEndian::write_u16(&mut buf[2..4], *size);
            }
            MsgType::GetList(size) => {
                BigEndian::write_u16(&mut buf[0..2], 4);
                BigEndian::write_u16(&mut buf[2..4], *size);
            }
            MsgType::PutList(size) => {
                BigEndian::write_u16(&mut buf[0..2], 5);
                BigEndian::write_u16(&mut buf[2..4], *size);
            }
            MsgType::AppendToList(size) => {
                BigEndian::write_u16(&mut buf[0..2], 6);
                BigEndian::write_u16(&mut buf[2..4], *size);
            }
        }
    }
}

pub struct KVServer<D>
where
    D: Datapath,
{
    map: HashMap<String, D::DatapathBuffer>,
}

impl<D> KVServer<D>
where
    D: Datapath,
{
    pub fn new() -> Self {
        KVServer {
            map: HashMap::default(),
        }
    }

    pub fn get_map(&self) -> &HashMap<String, D::DatapathBuffer> {
        &self.map
    }

    pub fn get_mut_map(&mut self) -> &mut HashMap<String, D::DatapathBuffer> {
        &mut self.map
    }

    pub fn get(&self, key: &str) -> Option<&D::DatapathBuffer> {
        self.map.get(key)
    }

    pub fn insert(&mut self, key: String, value: D::DatapathBuffer) {
        self.map.insert(key, value);
    }
}

pub struct ListKVServer<D>
where
    D: Datapath,
{
    map: HashMap<String, Vec<D::DatapathBuffer>>,
}

impl<D> ListKVServer<D>
where
    D: Datapath,
{
    fn new() -> Self {
        ListKVServer {
            map: HashMap::default(),
        }
    }

    pub fn get_map(&self) -> &HashMap<String, Vec<D::DatapathBuffer>> {
        &self.map
    }

    pub fn get_mut_map(&mut self) -> &mut HashMap<String, Vec<D::DatapathBuffer>> {
        &mut self.map
    }

    pub fn get(&self, key: &str) -> Option<&Vec<D::DatapathBuffer>> {
        self.map.get(key)
    }

    pub fn insert(&mut self, key: String, value: Vec<D::DatapathBuffer>) {
        self.map.insert(key, value);
    }

    pub fn append(&mut self, key: String, value: D::DatapathBuffer) {
        match self.map.get_mut(&key) {
            Some(list) => {
                list.push(value);
            }
            None => {
                let mut vec = Vec::with_capacity(1);
                vec.push(value);
                self.map.insert(key, vec);
            }
        }
    }
}

fn allocate_datapath_buffer<D>(
    datapath: &mut D,
    size: usize,
    mempool_ids: &mut Vec<MempoolID>,
) -> Result<D::DatapathBuffer>
where
    D: Datapath,
{
    match datapath.allocate(size)? {
        Some(buf) => Ok(buf),
        None => {
            let aligned_size = align_to_power(size)?;
            mempool_ids.append(&mut datapath.add_memory_pool(aligned_size, MIN_MEMPOOL_SIZE)?);
            match datapath.allocate(size)? {
                Some(buf) => Ok(buf),
                None => {
                    unreachable!();
                }
            }
        }
    }
}

pub trait ServerLoadGenerator<D>
where
    D: Datapath,
{
    type RequestLine: Clone + std::fmt::Debug + PartialEq + Eq;

    fn new_ref_kv_state(
        &self,
        file: &str,
    ) -> Result<(HashMap<String, String>, HashMap<String, Vec<String>>)> {
        let mut kv_server: HashMap<String, String> = HashMap::default();
        let mut list_kv_server: HashMap<String, Vec<String>> = HashMap::default();
        self.load_ref_kv_file(file, &mut kv_server, &mut list_kv_server)?;
        Ok((kv_server, list_kv_server))
    }

    fn new_kv_state(
        &self,
        file: &str,
        datapath: &mut D,
    ) -> Result<(KVServer<D>, ListKVServer<D>, Vec<MempoolID>)> {
        let mut kv_server = KVServer::new();
        let mut list_kv_server = ListKVServer::new();
        let mut mempool_ids: Vec<MempoolID> = Vec::default();
        self.load_file(
            file,
            &mut kv_server,
            &mut list_kv_server,
            &mut mempool_ids,
            datapath,
        )?;
        Ok((kv_server, list_kv_server, mempool_ids))
    }

    fn read_request(&self, line: &str) -> Result<Self::RequestLine>;

    fn load_ref_kv_file(
        &self,
        request_file: &str,
        kv_server: &mut HashMap<String, String>,
        list_kv_server: &mut HashMap<String, Vec<String>>,
    ) -> Result<()> {
        let file = File::open(request_file)?;
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let request = self.read_request(&line?)?;
            self.modify_server_state_ref_kv(&request, kv_server, list_kv_server)?;
        }
        Ok(())
    }

    fn load_file(
        &self,
        request_file: &str,
        kv_server: &mut KVServer<D>,
        list_kv_server: &mut ListKVServer<D>,
        mempool_ids: &mut Vec<MempoolID>,
        datapath: &mut D,
    ) -> Result<()> {
        let file = File::open(request_file)?;
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let request = self.read_request(&line?)?;
            self.modify_server_state(&request, kv_server, list_kv_server, mempool_ids, datapath)?;
        }
        Ok(())
    }

    fn modify_server_state_ref_kv(
        &self,
        request: &Self::RequestLine,
        kv: &mut HashMap<String, String>,
        list_kv: &mut HashMap<String, Vec<String>>,
    ) -> Result<()>;

    fn modify_server_state(
        &self,
        request: &Self::RequestLine,
        kv_server: &mut KVServer<D>,
        list_kv_server: &mut ListKVServer<D>,
        mempool_ids: &mut Vec<MempoolID>,
        datapath: &mut D,
    ) -> Result<()>;
}

pub trait RequestGenerator {
    type RequestLine: Clone + std::fmt::Debug + PartialEq + Eq;
    fn new(
        file: &str,
        client_id: usize,
        thread_id: usize,
        total_num_clients: usize,
        total_num_threads: usize,
    ) -> Result<Self>
    where
        Self: Sized;

    fn next_line(&mut self) -> Result<Option<String>>;

    fn check_get(
        &self,
        request: &Self::RequestLine,
        value: &[u8],
        kv: &HashMap<String, String>,
    ) -> Result<()>;

    fn check_getm(
        &self,
        request: &Self::RequestLine,
        values: Vec<&[u8]>,
        kv: &HashMap<String, String>,
    ) -> Result<()>;

    fn check_get_list(
        &self,
        request: &Self::RequestLine,
        values: Vec<&[u8]>,
        list_kv: &HashMap<String, Vec<String>>,
    ) -> Result<()>;

    fn get_request(&self, line: &str) -> Result<Self::RequestLine>;

    fn message_type(&self, request: &Self::RequestLine) -> Result<MsgType>;

    fn emit_get_data<'a>(&self, req: &'a Self::RequestLine) -> Result<&'a str>;

    fn emit_put_data<'a>(&self, req: &'a Self::RequestLine) -> Result<(&'a str, &'a str)>;

    fn emit_getm_data<'a>(&self, req: &'a Self::RequestLine, size: u16) -> Result<&'a Vec<String>>;

    fn emit_putm_data<'a>(
        &self,
        req: &'a Self::RequestLine,
        size: u16,
    ) -> Result<(&'a Vec<String>, &'a Vec<String>)>;

    fn emit_get_list_data<'a>(&self, req: &'a Self::RequestLine, size: u16) -> Result<&'a str>;

    fn emit_put_list_data<'a>(
        &self,
        req: &'a Self::RequestLine,
        size: u16,
    ) -> Result<(&'a str, &'a Vec<String>)>;

    fn emit_append_data<'a>(
        &self,
        req: &'a Self::RequestLine,
        size: u16,
    ) -> Result<(&'a str, &'a str)>;
}

pub trait ClientSerializer<D>
where
    D: Datapath,
{
    fn new() -> Self
    where
        Self: Sized;

    fn check_recved_msg<L>(
        &self,
        buf: &[u8],
        datapath: &D,
        load_generator: &L,
        request: &L::RequestLine,
        ref_kv: &HashMap<String, String>,
        ref_list_kv: &HashMap<String, Vec<String>>,
    ) -> Result<()>
    where
        L: RequestGenerator;

    fn serialize_get(&self, buf: &mut [u8], key: &str, datapath: &D) -> Result<usize>;

    fn serialize_put(&self, buf: &mut [u8], key: &str, value: &str, datapath: &D) -> Result<usize>;

    fn serialize_getm(&self, buf: &mut [u8], keys: &Vec<String>, datapath: &D) -> Result<usize>;

    fn serialize_putm(
        &self,
        buf: &mut [u8],
        keys: &Vec<String>,
        values: &Vec<String>,
        datapath: &D,
    ) -> Result<usize>;

    fn serialize_get_list(&self, buf: &mut [u8], key: &str, datapath: &D) -> Result<usize>;

    fn serialize_put_list(
        &self,
        buf: &mut [u8],
        key: &str,
        values: &Vec<String>,
        datapath: &D,
    ) -> Result<usize>;

    fn serialize_append(
        &self,
        buf: &mut [u8],
        key: &str,
        value: &str,
        datapath: &D,
    ) -> Result<usize>;
}

pub struct KVClient<R, C, D>
where
    R: RequestGenerator,
    C: ClientSerializer<D>,
    D: Datapath,
{
    request_generator: R,
    serializer: C,
    _datapath: PhantomData<D>,
    last_sent_id: MsgID,
    received: usize,
    num_retried: usize,
    num_timed_out: usize,
    server_addr: AddressInfo,
    rtts: ManualHistogram,
    buf: Vec<u8>,
    outgoing_requests: HashMap<MsgID, String>,
    outgoing_msg_types: HashMap<MsgID, R::RequestLine>,
    using_retries: bool,
    ref_kv: HashMap<String, String>,
    ref_list_kv: HashMap<String, Vec<String>>,
}

impl<R, C, D> KVClient<R, C, D>
where
    R: RequestGenerator,
    C: ClientSerializer<D>,
    D: Datapath,
{
    pub fn new(
        server_addr: AddressInfo,
        request_file: &str,
        max_num_requests: usize,
        client_id: usize,
        thread_id: usize,
        max_clients: usize,
        max_threads: usize,
        using_retries: bool,
        server_trace: Option<(&str, impl ServerLoadGenerator<D>)>,
    ) -> Result<KVClient<R, C, D>> {
        let (ref_kv, ref_list_kv) = match server_trace {
            Some((file, load_gen)) => load_gen.new_ref_kv_state(file)?,
            None => ((HashMap::default(), HashMap::default())),
        };
        Ok(KVClient {
            request_generator: R::new(
                request_file,
                client_id,
                thread_id,
                max_clients,
                max_threads,
            )?,
            serializer: C::new(),
            last_sent_id: 0,
            received: 0,
            num_retried: 0,
            num_timed_out: 0,
            server_addr: server_addr,
            rtts: ManualHistogram::new(max_num_requests),
            _datapath: PhantomData,
            buf: vec![0u8; D::max_packet_size()],
            outgoing_requests: HashMap::default(),
            outgoing_msg_types: HashMap::default(),
            using_retries: using_retries,
            ref_kv: ref_kv,
            ref_list_kv: ref_list_kv,
        })
    }

    pub fn write_request<'a>(&mut self, req: &'a str, datapath: &D) -> Result<usize> {
        let request = self.request_generator.get_request(req)?;
        let msg_type = self.request_generator.message_type(&request)?;
        msg_type.to_buf(&mut self.buf);
        let buf_size = match msg_type {
            MsgType::Get => self.serializer.serialize_get(
                &mut self.buf.as_mut_slice()[REQ_TYPE_SIZE..],
                self.request_generator.emit_get_data(&request)?,
                datapath,
            )?,
            MsgType::Put => {
                let put_data = self.request_generator.emit_put_data(&request)?;
                self.serializer.serialize_put(
                    &mut self.buf.as_mut_slice()[2..],
                    put_data.0,
                    put_data.1,
                    datapath,
                )?
            }
            MsgType::GetM(size) => self.serializer.serialize_getm(
                &mut self.buf.as_mut_slice()[REQ_TYPE_SIZE..],
                self.request_generator.emit_getm_data(&request, size)?,
                datapath,
            )?,
            MsgType::PutM(size) => {
                let put_data = self.request_generator.emit_putm_data(&request, size)?;
                self.serializer.serialize_putm(
                    &mut self.buf.as_mut_slice()[REQ_TYPE_SIZE..],
                    put_data.0,
                    put_data.1,
                    datapath,
                )?
            }
            MsgType::GetList(size) => self.serializer.serialize_get_list(
                &mut self.buf.as_mut_slice()[REQ_TYPE_SIZE..],
                self.request_generator.emit_get_list_data(&request, size)?,
                datapath,
            )?,
            MsgType::PutList(size) => {
                let put_data = self.request_generator.emit_put_list_data(&request, size)?;
                self.serializer.serialize_put_list(
                    &mut self.buf.as_mut_slice()[REQ_TYPE_SIZE..],
                    put_data.0,
                    put_data.1,
                    datapath,
                )?
            }
            MsgType::AppendToList(size) => {
                let append_data = self.request_generator.emit_append_data(&request, size)?;
                self.serializer.serialize_append(
                    &mut self.buf.as_mut_slice()[REQ_TYPE_SIZE..],
                    append_data.0,
                    append_data.1,
                    datapath,
                )?
            }
        };
        Ok(buf_size + REQ_TYPE_SIZE)
    }
}

impl<R, C, D> ClientSM for KVClient<R, C, D>
where
    R: RequestGenerator,
    C: ClientSerializer<D>,
    D: Datapath,
{
    type Datapath = D;

    fn increment_uniq_received(&mut self) {
        self.received += 1;
    }

    fn increment_uniq_sent(&mut self) {
        self.last_sent_id += 1;
    }

    fn increment_num_timed_out(&mut self) {
        self.num_timed_out += 1;
    }

    fn increment_num_retried(&mut self) {
        self.num_retried += 1;
    }

    fn uniq_sent_so_far(&self) -> usize {
        self.last_sent_id as usize
    }

    fn uniq_received_so_far(&self) -> usize {
        self.received
    }

    fn num_retried(&self) -> usize {
        self.num_retried
    }

    fn num_timed_out(&self) -> usize {
        self.num_timed_out
    }

    fn get_mut_rtts(&mut self) -> &mut ManualHistogram {
        &mut self.rtts
    }

    fn server_addr(&self) -> AddressInfo {
        self.server_addr.clone()
    }

    fn get_next_msg(
        &mut self,
        datapath: &<Self as ClientSM>::Datapath,
    ) -> Result<Option<(MsgID, &[u8])>> {
        let next_line = match self.request_generator.next_line()? {
            Some(l) => l,
            None => {
                return Ok(None);
            }
        };
        let buf_size = self.write_request(next_line.as_str(), &datapath)?;
        if cfg!(debug_assertions) {
            let request = self.request_generator.get_request(&next_line.as_str())?;
            self.outgoing_msg_types.insert(self.last_sent_id, request);
        }

        if self.using_retries {
            self.outgoing_requests.insert(self.last_sent_id, next_line);
        }

        Ok(Some((
            self.last_sent_id,
            &mut self.buf.as_mut_slice()[0..buf_size],
        )))
    }

    fn process_received_msg(
        &mut self,
        sga: ReceivedPkt<<Self as ClientSM>::Datapath>,
        datapath: &<Self as ClientSM>::Datapath,
    ) -> Result<()> {
        // if in debug mode, check whether the bytes are what they should be
        tracing::debug!(id = sga.msg_id(), size = sga.data_len(), "Received sga");
        if cfg!(debug_assertions) {
            let request_line = match self.outgoing_msg_types.remove(&sga.msg_id()) {
                Some(m) => m,
                None => {
                    bail!("Received ID not in in flight map: {}", sga.msg_id());
                }
            };
            self.serializer.check_recved_msg(
                sga.seg(0).as_ref(),
                datapath,
                &self.request_generator,
                &request_line,
                &self.ref_kv,
                &self.ref_list_kv,
            )?;
        }
        if self.using_retries {
            if let Some(_) = self.outgoing_requests.remove(&sga.msg_id()) {
            } else {
                bail!("Received ID not in in flight map: {}", sga.msg_id());
            }
        }

        Ok(())
    }

    fn init(&mut self, connection: &mut Self::Datapath) -> Result<()> {
        connection.add_memory_pool(8192, 8192)?;
        Ok(())
    }

    fn cleanup(&mut self, _connection: &mut Self::Datapath) -> Result<()> {
        Ok(())
    }

    fn msg_timeout_cb(&mut self, id: MsgID, datapath: &Self::Datapath) -> Result<&[u8]> {
        let line = match self.outgoing_requests.get(&id) {
            Some(l) => l.to_string(),
            None => {
                bail!("Cannot find data for msg # {} to send retry", id);
            }
        };

        let buf_size = self.write_request(&line, datapath)?;
        Ok(&self.buf.as_slice()[0..buf_size])
    }
}
