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

// TODO: Do this in bytes so we're packing e.g. 8192 bytes instead of 8000 bytes
// into a single segment?
const MAX_SUBOBJ_SIZE_KB: usize = 8;

/// * obj_id: The object ID in the trace.
/// * obj_subid: The segment index of the object value.
/// * key_length: The length of the key in characters.
fn get_key(obj_id: usize, obj_subid: usize, key_length: usize) -> String {
    let key_name = format!("key_{}.{}", obj_id, obj_subid);
    let additional_chars: String = std::iter::repeat("a")
        .take(key_length - key_name.len())
        .collect();
    format!("{}{}", key_name, additional_chars)
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CdnServerLine {
    /// The key strings of each object segment.
    keys: Vec<String>,
    /// The value size, in kB, of the last object segment. The value is at most
    /// MAX_SUBOBJ_SIZE_KB. Preceding segments are of the maximum segment size.
    last_value_size_kb: usize,
}
impl CdnServerLine {
    fn new(obj_id: usize, value_size_kb: usize, key_length: usize) -> Self {
        let mut segments = value_size_kb / MAX_SUBOBJ_SIZE_KB;
        let mut last_value_size_kb = value_size_kb - segments * MAX_SUBOBJ_SIZE_KB;
        if last_value_size_kb == 0 {
            last_value_size_kb = MAX_SUBOBJ_SIZE_KB;
        } else {
            segments += 1;
        }
        let keys = (0..segments)
            .map(|i| get_key(obj_id, i, key_length))
            .collect::<Vec<_>>();
        CdnServerLine { keys, last_value_size_kb }
    }

    pub fn take_keys(mut self) -> Vec<String> {
        self.keys.drain(..).collect()
    }

    pub fn num_keys(&self) -> usize {
        self.keys.len()
    }

    /// Panics if the key index does not exist.
    pub fn get_key(&self, index: usize) -> &str {
        self.keys[index].as_str()
    }

    /// Panics if the key index does not exist.
    pub fn get_key_value_size_kb(&self, index: usize) -> (&str, usize) {
        let key = self.get_key(index);
        let value_size_kb = if index + 1 == self.keys.len() {
            self.last_value_size_kb
        } else {
            MAX_SUBOBJ_SIZE_KB
        };
        Some((key, value_size_kb))
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
        let line = line.split(',').collect::<Vec<_>>();
        let obj_id = line[1].parse::<usize>().unwrap();
        let value_size_kb = line[2].parse::<usize>().unwrap();
        Ok(CdnServerLine::new(obj_id, value_size_kb, self.key_length))
    }

    fn load_ref_kv_file(
        &self,
        _request_file: &str,
        kv_server: &mut HashMap<String, String>,
        list_kv_server: &mut HashMap<String, Vec<String>>,
    ) -> Result<()> {
        for i in 0..self.num_keys {
            let request = self.read_request(&format!("0,{},{}", i, DEFAULT_VALUE_SIZE_KB))?;
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
        for i in (0..request.num_keys()) {
            let (key, value_size_kb) = request.get_key_value_size_kb(i);
            let char = thread_rng().sample(&Alphanumeric) as char;
            let value: String = std::iter::repeat(char).take(value_size_kb * 1000).collect();
            let mut datapath_buffer = allocate_datapath_buffer(datapath, value.len(), mempool_ids)?;
            let _ = datapath_buffer.write(value.as_bytes())?;
            linked_list_kv_server.insert(key.to_string(), datapath_buffer);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct CdnClient {
    cur_request_index: usize,
    request_keys: Vec<Vec<String>>,
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
            let line = line.split(',').collect::<Vec<_>>();
            let obj_id = line[1].parse::<usize>().unwrap();
            let value_size_kb = line[2].parse::<usize>().unwrap();
            let mut line = CdnServerLine::new(obj_id, value_size_kb, self.key_length);
            let keys = line.take_keys();
            // Push keys for all object segments of a given object.
            // Initializing a CdnClient is done before the experiment starts.
            request_keys.push(keys);
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
pub struct CdnRequest(Vec<String>);

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
        Ok(if let Some(keys) = self.request_keys.get(self.cur_request_index) {
            tracing::debug!("Sending key {:?}", key);
            self.cur_request_index += 1;
            Some(CdnRequest(keys))
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
