pub mod capnproto;
pub mod cornflakes_dynamic;
pub mod flatbuffers;
pub mod protobuf;
pub mod redis;
pub mod retwis;
pub mod retwis_run_datapath;
pub mod ycsb;
pub mod ycsb_run_datapath;

// TODO: though capnpc 0.14^ supports generating nested namespace files
// there seems to be a bug in the code generation, so must include it at crate root
mod kv_capnp {
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(dead_code)]
    #![allow(improper_ctypes)]
    include!(concat!(env!("OUT_DIR"), "/cf_kv_capnp.rs"));
}

use byteorder::{BigEndian, ByteOrder};
use bytes::Bytes;
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

pub static mut MIN_MEMPOOL_SIZE: usize = 262144;

// 8 bytes at front of message for framing
pub const REQ_TYPE_SIZE: usize = 4;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum MsgType {
    Get,                // single get
    Put,                // single put
    GetM(u16),          // multiple key get
    PutM(u16),          // multiple key put
    GetList(u16),       // get list
    PutList(u16),       // put list
    AppendToList(u16),  // append to list
    AddUser,            // add user retwis,
    FollowUnfollow,     // follow unfollow retwis
    PostTweet,          // Post Tweet Retwis,
    GetTimeline(usize), // Get timeline
}

impl MsgType {
    /// Reads first four bytes of packet to determine message type.
    pub fn from_packet<D: Datapath>(packet: &ReceivedPkt<D>) -> Result<Self> {
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
            (7, 0) => Ok(MsgType::AddUser),
            (8, 0) => Ok(MsgType::FollowUnfollow),
            (9, 0) => Ok(MsgType::PostTweet),
            (10, 0) => Ok(MsgType::GetTimeline(0)),
            (x, y) => {
                bail!("unrecognized message type for kv store app: {}, {}", x, y);
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
            MsgType::AddUser => {
                BigEndian::write_u16(&mut buf[0..2], 7);
                BigEndian::write_u16(&mut buf[2..4], 0);
            }
            MsgType::FollowUnfollow => {
                BigEndian::write_u16(&mut buf[0..2], 8);
                BigEndian::write_u16(&mut buf[2..4], 0);
            }
            MsgType::PostTweet => {
                BigEndian::write_u16(&mut buf[0..2], 9);
                BigEndian::write_u16(&mut buf[2..4], 0);
            }
            MsgType::GetTimeline(_size) => {
                BigEndian::write_u16(&mut buf[0..2], 10);
                BigEndian::write_u16(&mut buf[2..4], 0);
            }
        }
    }
}

pub struct KVNode<D>
where
    D: Datapath,
{
    buffer: D::DatapathBuffer,
    next: Option<Box<KVNode<D>>>,
}

/*impl<D> Drop for KVNode<D> where D: Datapath {
    fn drop(&mut self) {
        match self.next {
            Some(x) => {

            }
        }
    }
}*/

impl<D> AsRef<[u8]> for KVNode<D>
where
    D: Datapath,
{
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_ref()
    }
}

impl<D> KVNode<D>
where
    D: Datapath,
{
    pub fn new(buf: D::DatapathBuffer) -> Self {
        KVNode {
            buffer: buf,
            next: None,
        }
    }

    pub fn append(&mut self, buf: D::DatapathBuffer) {
        match &mut self.next {
            Some(ref mut node) => {
                node.append(buf);
            }
            None => {
                self.next = Some(Box::new(KVNode::new(buf)));
            }
        }
    }

    pub fn append_node(&mut self, node: KVNode<D>) {
        match &mut self.next {
            Some(ref mut our_node) => {
                our_node.append_node(node);
            }
            None => {
                self.next = Some(Box::new(node));
            }
        }
    }

    pub fn get_buffer(&self) -> &D::DatapathBuffer {
        &self.buffer
    }

    pub fn get_data(&self) -> &[u8] {
        self.buffer.as_ref()
    }

    pub fn get_next(&self) -> Option<&Box<KVNode<D>>> {
        self.next.as_ref()
    }

    pub fn replace_data(&mut self, elt: D::DatapathBuffer) {
        self.buffer = elt;
    }
}

pub struct LinkedListKVServer<D>
where
    D: Datapath,
{
    map: HashMap<String, Box<KVNode<D>>>,
}

impl<D> LinkedListKVServer<D>
where
    D: Datapath,
{
    pub fn new() -> Self {
        LinkedListKVServer {
            map: HashMap::default(),
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn get_map(&self) -> &HashMap<String, Box<KVNode<D>>> {
        &self.map
    }

    pub fn get_mut_map(&mut self) -> &mut HashMap<String, Box<KVNode<D>>> {
        &mut self.map
    }

    pub fn get(&self, key: &str) -> Option<&Box<KVNode<D>>> {
        self.map.get(key)
    }

    pub fn remove(&mut self, key: &str) -> Option<Box<KVNode<D>>> {
        self.map.remove(key)
    }

    pub fn insert(&mut self, key: String, value: D::DatapathBuffer) {
        match self.map.get_mut(&key) {
            Some(ref mut node) => {
                node.as_mut().append(value);
            }
            None => {
                self.map.insert(key, Box::new(KVNode::new(value)));
            }
        }
    }

    pub fn insert_with_copies(
        &mut self,
        key: &str,
        value: &[u8],
        datapath: &mut D,
        mempool_ids: &mut Vec<MempoolID>,
    ) -> Result<()> {
        let key = key.to_string();
        let mut datapath_buffer = allocate_datapath_buffer(datapath, value.len(), mempool_ids)?;
        let _ = datapath_buffer.write(value)?;
        self.map.insert(key, Box::new(KVNode::new(datapath_buffer)));
        Ok(())
    }

    pub fn insert_list_with_copies<'a>(
        &mut self,
        key: &str,
        mut values: impl Iterator<Item = &'a [u8]>,
        datapath: &mut D,
        mempool_ids: &mut Vec<MempoolID>,
    ) -> Result<()> {
        let first_buffer = {
            let value = values.next().unwrap();
            let mut datapath_buffer = allocate_datapath_buffer(datapath, value.len(), mempool_ids)?;
            let _ = datapath_buffer.write(value)?;
            datapath_buffer
        };

        let mut kv_node = KVNode::new(first_buffer);
        while let Some(value) = values.next() {
            let mut datapath_buffer = allocate_datapath_buffer(datapath, value.len(), mempool_ids)?;
            let _ = datapath_buffer.write(value)?;
            kv_node.append(datapath_buffer);
        }

        self.map.insert(key.to_string(), Box::new(kv_node));
        Ok(())
    }
    pub fn keys(&self) -> Vec<String> {
        self.map.keys().cloned().collect::<Vec<String>>()
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

    pub fn len(&self) -> usize {
        self.map.len()
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

    pub fn remove(&mut self, key: &str) -> Option<D::DatapathBuffer> {
        self.map.remove(key)
    }

    pub fn insert(&mut self, key: String, value: D::DatapathBuffer) {
        self.map.insert(key, value);
    }

    pub fn insert_with_copies(
        &mut self,
        key: &str,
        value: &[u8],
        datapath: &mut D,
        mempool_ids: &mut Vec<MempoolID>,
    ) -> Result<()> {
        let key = key.to_string();
        let mut datapath_buffer = allocate_datapath_buffer(datapath, value.len(), mempool_ids)?;
        let _ = datapath_buffer.write(value)?;
        self.map.insert(key, datapath_buffer);
        Ok(())
    }

    pub fn keys(&self) -> Vec<String> {
        self.map.keys().cloned().collect::<Vec<String>>()
    }
}

pub fn allocate_and_copy_into_datapath_buffer<D>(
    value: &[u8],
    datapath: &mut D,
    mempool_ids: &mut Vec<MempoolID>,
) -> Result<D::DatapathBuffer>
where
    D: Datapath,
{
    let mut datapath_buffer = allocate_datapath_buffer(datapath, value.len(), mempool_ids)?;
    let _ = datapath_buffer.write(value)?;
    Ok(datapath_buffer)
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

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.map.contains_key(key)
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

    pub fn insert_with_copies<'a>(
        &mut self,
        key: &str,
        values: impl Iterator<Item = &'a [u8]>,
        datapath: &mut D,
        mempool_ids: &mut Vec<MempoolID>,
    ) -> Result<()> {
        let datapath_buffers: Result<Vec<D::DatapathBuffer>> = values
            .map(|value| {
                let mut datapath_buffer =
                    allocate_datapath_buffer(datapath, value.len(), mempool_ids)?;
                let _ = datapath_buffer.write(value)?;
                Ok(datapath_buffer)
            })
            .collect();
        self.map.insert(key.to_string(), datapath_buffers?);
        Ok(())
    }

    pub fn keys(&self) -> Vec<String> {
        self.map.keys().cloned().collect::<Vec<String>>()
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
            unsafe{
                mempool_ids.append(&mut datapath.add_memory_pool(size, MIN_MEMPOOL_SIZE)?);
            }
            tracing::info!("Added mempool");
            match datapath.allocate(size)? {
                Some(buf) => Ok(buf),
                None => {
                    unreachable!();
                }
            }
        }
    }
}

pub trait ServerLoadGenerator {
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

    fn new_kv_state<D>(
        &self,
        file: &str,
        datapath: &mut D,
        use_linked_list_kv: bool,
    ) -> Result<(
        KVServer<D>,
        ListKVServer<D>,
        LinkedListKVServer<D>,
        Vec<MempoolID>,
    )>
    where
        D: Datapath,
    {
        let mut kv_server = KVServer::new();
        let mut list_kv_server = ListKVServer::new();
        let mut linked_list_kv_server = LinkedListKVServer::new();
        let mut mempool_ids: Vec<MempoolID> = Vec::default();
        self.load_file(
            file,
            &mut kv_server,
            &mut list_kv_server,
            &mut linked_list_kv_server,
            &mut mempool_ids,
            datapath,
            use_linked_list_kv,
        )?;
        Ok((
            kv_server,
            list_kv_server,
            linked_list_kv_server,
            mempool_ids,
        ))
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

    fn load_file<D>(
        &self,
        request_file: &str,
        kv_server: &mut KVServer<D>,
        list_kv_server: &mut ListKVServer<D>,
        linked_list_kv_server: &mut LinkedListKVServer<D>,
        mempool_ids: &mut Vec<MempoolID>,
        datapath: &mut D,
        use_linked_list_kv_server: bool,
    ) -> Result<()>
    where
        D: Datapath,
    {
        let file = File::open(request_file)?;
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let request = self.read_request(&line?)?;
            self.modify_server_state(
                &request,
                kv_server,
                list_kv_server,
                linked_list_kv_server,
                mempool_ids,
                datapath,
                use_linked_list_kv_server,
            )?;
        }
        tracing::info!(trace = request_file, mempool_ids =? mempool_ids, "Finished loading trace file");
        Ok(())
    }

    fn modify_server_state_ref_kv(
        &self,
        request: &Self::RequestLine,
        kv: &mut HashMap<String, String>,
        list_kv: &mut HashMap<String, Vec<String>>,
    ) -> Result<()>;

    fn modify_server_state<D>(
        &self,
        request: &Self::RequestLine,
        kv_server: &mut KVServer<D>,
        list_kv_server: &mut ListKVServer<D>,
        linked_list_kv_server: &mut LinkedListKVServer<D>,
        mempool_ids: &mut Vec<MempoolID>,
        datapath: &mut D,
        use_linked_list_kv_server: bool,
    ) -> Result<()>
    where
        D: Datapath;
}

pub trait RequestGenerator {
    type RequestLine: Clone + std::fmt::Debug + PartialEq + Eq;
    fn new(
        _file: &str,
        _client_id: usize,
        _thread_id: usize,
        _total_num_clients: usize,
        _total_num_threads: usize,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        unimplemented!();
    }

    fn next_request(&mut self) -> Result<Option<Self::RequestLine>>;

    fn message_type(&self, req: &Self::RequestLine) -> Result<MsgType>;

    fn serialize_request<S, D>(
        &self,
        request: &Self::RequestLine,
        buf: &mut [u8],
        serializer: &S,
        datapath: &D,
    ) -> Result<usize>
    where
        S: ClientSerializer<D>,
        D: Datapath;

    fn check_response<S, D>(
        &self,
        request: &Self::RequestLine,
        buf: &[u8],
        serializer: &S,
        kv: &HashMap<String, String>,
        list_kv: &HashMap<String, Vec<String>>,
    ) -> Result<bool>
    where
        S: ClientSerializer<D>,
        D: Datapath;
}

pub trait ClientSerializer<D>
where
    D: Datapath,
{
    fn new() -> Self
    where
        Self: Sized;

    fn deserialize_get_response(&self, buf: &[u8]) -> Result<Vec<u8>>;

    fn deserialize_getm_response(&self, buf: &[u8]) -> Result<Vec<Vec<u8>>>;

    fn deserialize_getlist_response(&self, buf: &[u8]) -> Result<Vec<Vec<u8>>>;

    fn check_add_user_num_values(&self, buf: &[u8]) -> Result<usize>;

    fn check_follow_unfollow_num_values(&self, buf: &[u8]) -> Result<usize>;

    fn check_post_tweet_num_values(&self, buf: &[u8]) -> Result<usize>;

    fn check_get_timeline_num_values(&self, buf: &[u8]) -> Result<usize>;

    fn check_retwis_response_num_values(&self, buf: &[u8]) -> Result<usize>;

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

    fn serialize_add_user(
        &self,
        buf: &mut [u8],
        keys: &Vec<&str>,
        values: &Vec<String>,
        _datapath: &D,
    ) -> Result<usize>;
    fn serialize_add_follow_unfollow(
        &self,
        buf: &mut [u8],
        keys: &Vec<&str>,
        values: &Vec<String>,
        _datapath: &D,
    ) -> Result<usize>;

    fn serialize_post_tweet(
        &self,
        buf: &mut [u8],
        keys: &Vec<&str>,
        values: &Vec<String>,
        _datapath: &D,
    ) -> Result<usize>;

    fn serialize_get_timeline(
        &self,
        buf: &mut [u8],
        keys: &Vec<&str>,
        _values: &Vec<String>,
        _datapath: &D,
    ) -> Result<usize>;
}

pub struct KVClient<R, C, D>
where
    R: RequestGenerator,
    C: ClientSerializer<D>,
    D: Datapath,
{
    request_generator: R,
    requests: Vec<Bytes>,
    serializer: C,
    _datapath: PhantomData<D>,
    last_sent_id: MsgID,
    received: usize,
    num_retried: usize,
    num_timed_out: usize,
    server_addr: AddressInfo,
    rtts: ManualHistogram,
    buf: Vec<u8>,
    outgoing_requests: HashMap<MsgID, R::RequestLine>,
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
        request_generator: R,
        server_addr: AddressInfo,
        max_num_requests: usize,
        using_retries: bool,
        server_trace: Option<(&str, impl ServerLoadGenerator)>,
    ) -> Result<KVClient<R, C, D>> {
        let (ref_kv, ref_list_kv) = match server_trace {
            Some((file, load_gen)) => load_gen.new_ref_kv_state(file)?,
            None => (HashMap::default(), HashMap::default()),
        };
        Ok(KVClient {
            request_generator: request_generator,
            requests: Vec::default(),
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

    pub fn write_request_into_new_bytes(
        &self,
        request: &R::RequestLine,
        datapath: &D,
    ) -> Result<Bytes> {
        let mut bytes = vec![0u8; 9216];
        let serialized_len = self.request_generator.serialize_request(
            &request,
            &mut bytes.as_mut_slice(),
            &self.serializer,
            datapath,
        )?;
        Ok(Bytes::copy_from_slice(&bytes.as_slice()[0..serialized_len]))
    }

    pub fn write_request<'a>(&mut self, request: &R::RequestLine, datapath: &D) -> Result<usize> {
        self.request_generator.serialize_request(
            &request,
            &mut self.buf,
            &self.serializer,
            datapath,
        )
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

    fn prep_requests(
        &mut self,
        nb_requests: usize,
        datapath: &<Self as ClientSM>::Datapath,
    ) -> Result<usize> {
        self.requests = Vec::with_capacity(nb_requests);
        for _ in 0..nb_requests {
            let next_request = match self.request_generator.next_request()? {
                Some(l) => l,
                None => {
                    return Ok(self.requests.len());
                }
            };
            let bytes = self.write_request_into_new_bytes(&next_request, &datapath)?;
            self.requests.push(bytes);
        }
        Ok(nb_requests)
    }

    fn get_next_msg(
        &mut self,
        datapath: &<Self as ClientSM>::Datapath,
    ) -> Result<Option<(MsgID, &[u8])>> {
        if (self.last_sent_id as usize) < self.requests.len() {
            let bytes = &self.requests[self.last_sent_id as usize];
            Ok(Some((self.last_sent_id, bytes.as_ref())))
        } else {
            let next_request = match self.request_generator.next_request()? {
                Some(l) => l,
                None => {
                    return Ok(None);
                }
            };
            let buf_size = self.write_request(&next_request, &datapath)?;
            tracing::debug!(
                msg_id = self.last_sent_id,
                "Sending msg of type {:?}",
                next_request
            );
            if cfg!(debug_assertions) {
                self.outgoing_msg_types
                    .insert(self.last_sent_id, next_request.clone());
            }

            if self.using_retries {
                self.outgoing_requests
                    .insert(self.last_sent_id, next_request.clone());
            }

            Ok(Some((
                self.last_sent_id,
                &mut self.buf.as_mut_slice()[0..buf_size],
            )))
        }
    }

    fn process_received_msg(
        &mut self,
        sga: ReceivedPkt<<Self as ClientSM>::Datapath>,
        _datapath: &<Self as ClientSM>::Datapath,
    ) -> Result<bool> {
        // if in debug mode, check whether the bytes are what they should be
        tracing::debug!(id = sga.msg_id(), size = sga.data_len(), "Received sga");
        if self.using_retries {
            if let Some(_) = self.outgoing_requests.remove(&sga.msg_id()) {
            } else {
                bail!("Received ID not in in flight map: {}", sga.msg_id());
            }
        }
        if cfg!(debug_assertions) {
            let request_line = match self.outgoing_msg_types.remove(&sga.msg_id()) {
                Some(m) => m,
                None => {
                    bail!("Received ID not in in flight map: {}", sga.msg_id());
                }
            };
            self.request_generator.check_response(
                &request_line,
                sga.seg(0).as_ref(),
                &self.serializer,
                &self.ref_kv,
                &self.ref_list_kv,
            )
        } else {
            return Ok(true);
        }
    }

    fn init(&mut self, _connection: &mut Self::Datapath) -> Result<()> {
        Ok(())
    }

    fn cleanup(&mut self, _connection: &mut Self::Datapath) -> Result<()> {
        Ok(())
    }

    fn msg_timeout_cb(&mut self, id: MsgID, datapath: &Self::Datapath) -> Result<&[u8]> {
        let req = match self.outgoing_requests.get(&id) {
            Some(r) => r.clone(),
            None => {
                bail!("Cannot find data for msg # {} to send retry", id);
            }
        };

        let buf_size = self.write_request(&req, datapath)?;
        Ok(&self.buf.as_slice()[0..buf_size])
    }
}
