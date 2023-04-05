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
use color_eyre::eyre::{bail, Result};
use cornflakes_libos::{allocator::MempoolID, datapath::Datapath};
use hashbrown::HashMap;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use std::io::{self, BufRead, Write};

// TODO: Do this in bytes so we're packing e.g. 8192 bytes instead of 8000 bytes
// into a single segment?
const MAX_SUBOBJ_SIZE_KB: usize = 8;

/// * obj_id: The object ID in the trace.
/// * obj_subid: The segment index of the object value.
/// * key_length: The length of the key in characters.
fn get_key(obj_id: usize, key_length: usize) -> String {
    let key_name = format!("key_{}", obj_id);
    let additional_chars: String = std::iter::repeat("a")
        .take(key_length - key_name.len())
        .collect();
    format!("{}{}", key_name, additional_chars)
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct KeyMetadata {
    pub obj_id: usize,
    pub obj_subid: usize,
    pub key_length: usize,
    pub val_size: usize,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CdnServerLine {
    /// Total object id.
    obj_id: usize,
    /// The key strings of each object segment.
    keys: Vec<KeyMetadata>,
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
            .map(|i| {
                let val_size = match i == (segments - 1) {
                    true => last_value_size_kb,
                    false => MAX_SUBOBJ_SIZE_KB,
                };
                KeyMetadata {
                    obj_id,
                    obj_subid: i,
                    key_length,
                    val_size,
                }
            })
            .collect::<Vec<_>>();
        CdnServerLine { obj_id, keys }
    }

    fn num_keys(&self) -> usize {
        self.keys.len()
    }

    pub fn get_key_string(&self, key_length: usize) -> String {
        get_key(self.obj_id, key_length)
    }

    pub fn get_key(&self, i: usize) -> &KeyMetadata {
        &self.keys[i]
    }

    pub fn take_keys(self) -> Vec<KeyMetadata> {
        self.keys
    }

    /// Panics if the key index does not exist.
    pub fn get_value_size_kb(&self, index: usize) -> usize {
        let key = self.get_key(index);
        key.val_size
    }
}

pub struct CdnServerLoader {
    key_length: usize,
    max_num_lines: usize,
}

impl CdnServerLoader {
    pub fn new(key_length: usize, max_num_lines: usize) -> Self {
        CdnServerLoader {
            key_length,
            max_num_lines,
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
        _kv_server: &mut HashMap<String, String>,
        _list_kv_server: &mut HashMap<String, Vec<String>>,
    ) -> Result<()> {
        unimplemented!();
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
        for (i, line) in io::BufReader::new(file).lines().enumerate() {
            if i == self.max_num_lines {
                break;
            }
            let request = self.read_request(line?.as_str())?;
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
        list_kv_server: &mut ListKVServer<D>,
        _linked_list_kv_server: &mut LinkedListKVServer<D>,
        mempool_ids: &mut Vec<MempoolID>,
        datapath: &mut D,
        _use_linked_list_kv_server: bool,
    ) -> Result<()>
    where
        D: Datapath,
    {
        // for cdn loader, only used linked list kv server
        let mut buffer_vec: Vec<D::DatapathBuffer> = Vec::with_capacity(request.num_keys());
        let key = request.get_key_string(self.key_length);
        if list_kv_server.contains_key(key.as_str()) {
            return Ok(());
        }
        for i in 0..request.num_keys() {
            let value_size_kb = request.get_value_size_kb(i);
            let char = thread_rng().sample(&Alphanumeric) as char;
            let value: String = std::iter::repeat(char).take(value_size_kb * 1000).collect();
            let mut datapath_buffer = allocate_datapath_buffer(datapath, value.len(), mempool_ids)?;
            let _ = datapath_buffer.write(value.as_bytes())?;
            buffer_vec.push(datapath_buffer);
        }
        list_kv_server.insert(key.to_string(), buffer_vec);
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CdnClient {
    /// Current index into metadata.
    cur_request_index: usize,
    /// All keys.
    all_keys: HashMap<KeyMetadata, String>,
    /// Vector of request IDs to be sent by this client.
    request_ids: Vec<KeyMetadata>,
}

impl CdnClient {
    /// Total keys should be the number of unique object IDs in the trace file.
    pub fn is_responsible_for(
        obj_id: usize,
        our_thread: usize,
        our_client: usize,
        total_num_threads: usize,
        total_num_clients: usize,
    ) -> bool {
        let our_thread_client = total_num_threads * our_client + our_thread;
        let total_thread_clients = total_num_threads * total_num_clients;
        our_thread_client == (obj_id % total_thread_clients)
    }
    pub fn new(
        request_file: &str,
        num_total_requests: usize,
        key_length: usize,
        thread_id: usize,
        client_id: usize,
        total_num_threads: usize,
        total_num_clients: usize,
        max_num_lines: usize,
    ) -> Result<Self> {
        let mut request_ids = Vec::with_capacity(num_total_requests);
        let mut all_keys: HashMap<KeyMetadata, String> = HashMap::default();
        let file = std::fs::File::open(request_file)?;
        for (i, file_line_res) in io::BufReader::new(file).lines().enumerate() {
            if i == max_num_lines {
                break;
            }
            let file_line = file_line_res?;
            let line = file_line.as_str().split(',').collect::<Vec<&str>>();
            let obj_id = line[1].parse::<usize>()?;
            let value_size_kb = line[2].parse::<usize>()?;
            // hash object id to see if we are responsible for this
            if !CdnClient::is_responsible_for(
                obj_id,
                thread_id,
                client_id,
                total_num_threads,
                total_num_clients,
            ) {
                continue;
            }
            let line = CdnServerLine::new(obj_id, value_size_kb, key_length);
            let key = line.get_key_string(key_length);
            let mut key_metadatas = line.take_keys();
            for metadata in key_metadatas.iter() {
                if all_keys.contains_key(&metadata) {
                    break;
                }
                all_keys.insert(metadata.clone(), key.clone());
            }
            request_ids.append(&mut key_metadatas);
        }
        tracing::info!("Done reading file");

        Ok(CdnClient {
            cur_request_index: 0,
            all_keys,
            request_ids,
        })
    }

    fn emit_get_data(&self, req: &CdnRequest) -> Result<(&str, usize)> {
        match self.all_keys.get(&req.0) {
            Some(s) => Ok((s, req.0.obj_subid)),
            None => {
                bail!("Could not find key for req {:?}", req);
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CdnRequest(KeyMetadata);
impl CdnRequest {
    pub fn msg_type(&self) -> MsgType {
        MsgType::GetFromList
    }
}

impl RequestGenerator for CdnClient {
    type RequestLine = CdnRequest;

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
        Ok(CdnClient::default())
    }

    fn next_request(&mut self) -> Result<Option<Self::RequestLine>> {
        Ok(
            if let Some(key) = self.request_ids.get(self.cur_request_index) {
                tracing::debug!("Sending key {:?}", key);
                self.cur_request_index += 1;
                Some(CdnRequest(key.clone()))
            } else {
                None
            },
        )
    }

    fn message_type(&self, _req: &CdnRequest) -> Result<MsgType> {
        Ok(MsgType::GetFromList)
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
        request.msg_type().to_buf(buf);
        let get_data = self.emit_get_data(&request)?;

        let buf_size = serializer.serialize_get_from_list(
            &mut buf[REQ_TYPE_SIZE..],
            get_data.0,
            get_data.1,
            datapath,
        )?;
        Ok(REQ_TYPE_SIZE + buf_size)
    }

    fn check_response<S, D>(
        &self,
        request: &<Self as RequestGenerator>::RequestLine,
        buf: &[u8],
        serializer: &S,
        _kv: &HashMap<String, String>,
        _list_kv: &HashMap<String, Vec<String>>,
    ) -> Result<bool>
    where
        S: ClientSerializer<D>,
        D: Datapath,
    {
        if cfg!(debug_assertions) {
            let val = serializer.deserialize_get_response(buf)?;
            if val.len() != request.0.val_size {
                return Ok(false);
            }
        }
        return Ok(true);
    }
}
