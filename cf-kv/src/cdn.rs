//! CDN trace
//! git clone https://github.com/UMass-LIDS/Tragen.git
//! pip install numpy scipy datetime matplotlib
//! python3 tragen_cli.py -c config.json
//!
//! config.json:
//! {
//!     "Trace_length": "1000000",
//!     "Hitrate_type": "bhr",
//!     "Input_unit": "reqs/s",
//!     "Traffic_classes": [
//!         {
//!             "traffic_class": "tc-1",
//!             "traffic_volume": "1"
//!         }
//!     ]
//! }
use super::{
    allocate_datapath_buffer, ClientSerializer, KVServer, LinkedListKVServer, ListKVServer,
    MsgType, RequestGenerator, ServerLoadGenerator, REQ_TYPE_SIZE,
};
use color_eyre::eyre::{bail, ensure, Result, WrapErr};
use cornflakes_libos::{allocator::MempoolID, datapath::Datapath};
use hashbrown::HashMap;
use rand::{
    distributions::{Alphanumeric, Distribution, Uniform, WeightedIndex},
    thread_rng, Rng,
};
use std::io::{self, BufRead, Write};

const DEFAULT_NUM_KEYS: usize = 1_000_000;
const DEFAULT_KEYS_SIZE: usize = 100;
const DEFAULT_VALUE_SIZE_KB: usize = 10;
const MAX_VALUE_SIZE_KB: usize = 160;

fn get_key(idx: usize, key_length: usize) -> String {
    let key_name = format!("key_{}", idx);
    let additional_chars: String = std::iter::repeat("a")
        .take(key_length - key_name.len())
        .collect();
    format!("{}{}", key_name, additional_chars)
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CdnServerLine {
    key: String,
    value_size: usize,
}
impl CdnServerLine {
    fn new(key: String, value_size: usize) -> Self {
        CdnServerLine { key, value_size }
    }

    pub fn value_size(&self) -> usize {
        self.value_size
    }

    pub fn key(&self) -> &str {
        self.key.as_str()
    }
}

#[derive(Debug, Clone)]
pub struct CdnServerLoader {
    num_keys: usize,
    key_length: usize,
}

impl CdnServerLoader {
    pub fn new(
        num_keys: usize,
        key_length: usize,
    ) -> Self {
        CdnServerLoader {
            num_keys,
            key_length,
        }
    }
}

impl ServerLoadGenerator for CdnServerLoader {
    type RequestLine = CdnServerLine;

    fn read_request(&self, line: &str) -> Result<Self::RequestLine> {
        // Tragen generates lines in the form <timestamp>,<obj_id>,<obj_size_kb>
        let idx: Vec<usize> =
            line.split(',').map(|x| x.parse::<usize>().unwrap()).collect();
        let key = get_key(idx[1], self.key_length);

        // Limit the value size
        let value_size_kb = std::cmp::min(idx[2], MAX_VALUE_SIZE_KB);
        Ok(CdnServerLine::new(key, value_size_kb * 1000))
    }

    fn load_ref_kv_file(
        &self,
        _request_file: &str,
        kv_server: &mut HashMap<String, String>,
        list_kv_server: &mut HashMap<String, Vec<String>>,
    ) -> Result<()> {
        for i in 0..self.num_keys {
            let request = self.read_request(&format!("{},0,{}", i, DEFAULT_VALUE_SIZE_KB))?;
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
        let file = std::fs::File::open(request_file).unwrap();
        for line in io::BufReader::new(file).lines() {
            let request = self.read_request(line)?;
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
        tracing::info!(
            "After load, kv_server size: {}, list kv server size: {}, linked list server size {}",
            kv_server.len(),
            list_kv_server.len(),
            linked_list_kv_server.len()
        );
        tracing::info!(trace = request_file, mempool_ids =? mempool_ids, "Finished loading trace file");
        Ok(())
    }

    fn modify_server_state_ref_kv(
        &self,
        _request: &Self::RequestLine,
        _kv: &mut HashMap<String, String>,
        _list_kv: &mut HashMap<String, Vec<String>>,
    ) -> Result<()> {
        Ok(())
    }

    fn modify_server_state<D>(
        &self,
        request: &Self::RequestLine,
        _kv_server: &mut KVServer<D>,
        _list_kv_server: &mut ListKVServer<D>,
        linked_list_kv_server: &mut LinkedListKVServer<D>,
        mempool_ids: &mut Vec<MempoolID>,
        datapath: &mut D,
        _use_linked_list_kv_server: bool,
    ) -> Result<()>
    where
        D: Datapath,
    {
        // for cdn loader, only used linked list kv server
        let char = thread_rng().sample(&Alphanumeric) as char;
        let value: String = std::iter::repeat(char).take(request.value_size).collect();
        // TODO: What if the value length is greater than the max UDP payload?
        // TODO: Does this have an issue if replacing an existing key?
        let mut datapath_buffer = allocate_datapath_buffer(datapath, value.len(), mempool_ids)?;
        let _ = datapath_buffer.write(value.as_bytes())?;
        linked_list_kv_server.insert(request.key().to_string(), datapath_buffer);
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct CdnClient {
    cur_request_index: usize,
    request_keys: Vec<String>,
}

impl CdnClient {
    pub fn new(
        request_file: &str,
        total_keys: usize,
        key_length: usize,
    ) -> Self {
        let request_keys = vec![];
        let file = std::fs::File::open(request_file).unwrap();
        for line in io::BufReader::new(file).lines() {
            let idx: Vec<usize> =
                line.split(',').map(|x| x.parse::<usize>().unwrap()).collect();
            let key = get_key(idx[1], self.key_length);
            request_keys.push(key);
            if request_keys.len() == total_keys {
                break;
            }
        }

        if request_keys.len() != total_keys {
            panic!("not enough keys in trace file");
        }
        CdnClient { cur_request_index: 0, request_keys }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CdnRequest(usize);

impl RequestGenerator for CdnClient {
    type RequestLine = CdnRequest;

    fn new(
        file: &str,
        _client_id: usize,
        _thread_id: usize,
        _total_num_clients: usize,
        _total_num_threads: usize,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(CdnClient::new(file, DEFAULT_NUM_KEYS, DEFAULT_KEYS_SIZE))
    }

    fn next_request(&mut self) -> Result<Option<Self::RequestLine>> {
        Ok(if let Some(key) = self.request_keys.get(self.cur_request_index) {
            tracing::debug!("Sending key {:?}", key);
            Some(CdnRequest(key))
        } else {
            None
        })
    }

    fn message_type(&self, _req: &CdnRequest) -> Result<MsgType> {
        Ok(MsgType::Get)
    }

    fn serialize_request<S, D>(
        &self,
        request: &<Self as RequestGenerator>::RequestLine,
        buf: &mut [u8],
        serializer: &S,
        datapath: &D,
    ) -> Result<usize>
    where
        S: ClientSerializer<D>,
        D: Datapath,
    {
        unimplemented!()
    }

    fn check_response<S, D>(
        &self,
        _request: &<Self as RequestGenerator>::RequestLine,
        buf: &[u8],
        serializer: &S,
        _kv: &HashMap<String, String>,
        _list_kv: &HashMap<String, Vec<String>>,
    ) -> Result<bool>
    where
        S: ClientSerializer<D>,
        D: Datapath,
    {
        unimplemented!()
    }
}
