use super::{
    allocate_datapath_buffer, KVServer, ListKVServer, MsgType, RequestGenerator,
    ServerLoadGenerator,
};
use color_eyre::eyre::{bail, ensure, Result};
use cornflakes_libos::{allocator::MempoolID, datapath::Datapath};
use hashbrown::HashMap;
use std::{
    fs::File,
    io::{prelude::*, BufReader, Lines, Write},
};
const MAX_BATCHES: usize = 8;
const DEFAULT_VALUE_SIZE: usize = 4096;
const DEFAULT_NUM_KEYS: usize = 1;
const DEFAULT_NUM_VALUES: usize = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct YCSBLine {
    keys: Vec<String>,
    vals: Vec<String>,
    req_type: MsgType,
}

impl YCSBLine {
    // derive YCSB request from line of the file
    pub fn new(
        line: &str,
        num_keys: usize,
        num_values: usize,
        value_size: usize,
    ) -> Result<YCSBLine> {
        let mut split: std::str::Split<&str> = line.split(" ");
        let _ = match &split.next().unwrap().parse::<usize>() {
            Ok(x) => *x,
            Err(e) => {
                bail!("Could not parse string request: {:?}", e);
            }
        };
        let req = split.next().unwrap();
        let mut keys: Vec<String> = Vec::default();
        for _i in 0..std::cmp::min(num_keys, MAX_BATCHES) {
            let key = &split.next().unwrap();
            keys.push(key.to_string());
        }

        match req {
            // TODO: what about appends? Not in YCSB.
            "GET" => {
                let mut msg_type = MsgType::Get;
                if num_values > 1 && num_keys == num_values {
                    msg_type = MsgType::GetM(num_values as u16);
                } else if num_values > 1 && num_keys == 1 {
                    msg_type = MsgType::GetList(num_values as u16);
                }
                Ok(YCSBLine {
                    keys: keys,
                    vals: Vec::default(),
                    req_type: msg_type,
                })
            }
            "UPDATE" => {
                let mut msg_type = MsgType::Put;
                if num_values > 1 && num_keys == num_values {
                    msg_type = MsgType::PutM(num_values as u16);
                } else if num_values > 1 && num_keys == 1 {
                    msg_type = MsgType::PutList(num_values as u16);
                }
                let val = &split.next().unwrap();
                let sized_value = std::str::from_utf8(&val.as_bytes()[0..value_size])?;
                Ok(YCSBLine {
                    keys: keys,
                    vals: std::iter::repeat(sized_value.to_string())
                        .take(num_values)
                        .collect(),
                    req_type: msg_type,
                })
            }
            x => {
                bail!("Unknown request type: {:?}", x);
            }
        }
    }

    pub fn get_values(&self) -> &Vec<String> {
        &self.vals
    }

    pub fn get_keys(&self) -> &Vec<String> {
        &self.keys
    }

    pub fn msg_type(&self) -> MsgType {
        self.req_type
    }
}

pub struct YCSBServerLoader {
    value_size: usize,
    num_values: usize,
    num_keys: usize,
}

impl YCSBServerLoader {
    pub fn new(value_size: usize, num_values: usize, num_keys: usize) -> Self {
        YCSBServerLoader {
            value_size: value_size,
            num_values: num_values,
            num_keys: num_keys,
        }
    }
}

impl<D> ServerLoadGenerator<D> for YCSBServerLoader
where
    D: Datapath,
{
    type RequestLine = YCSBLine;

    fn read_request(&self, line: &str) -> Result<Self::RequestLine> {
        YCSBLine::new(line, self.num_keys, self.num_values, self.value_size)
    }

    fn modify_server_state_ref_kv(
        &self,
        request: &Self::RequestLine,
        kv: &mut HashMap<String, String>,
        list_kv: &mut HashMap<String, Vec<String>>,
    ) -> Result<()> {
        let msg_type = request.msg_type();
        match msg_type {
            MsgType::Put => {
                let key = request.get_keys()[0].as_str();
                let value = request.get_values()[0].as_str();
                kv.insert(key.to_string(), value.to_string());
            }
            MsgType::PutM(size) => {
                let keys = request.get_keys();
                let values = request.get_values();
                ensure!(
                    keys.len() == size as usize && values.len() == size as usize,
                    format!(
                        "Keys and values expected to have length {}, instead have length {} and {}",
                        size,
                        keys.len(),
                        values.len()
                    )
                );
                for (key, value) in keys.iter().zip(values.iter()) {
                    kv.insert(key.to_string(), value.to_string());
                }
            }
            MsgType::PutList(size) => {
                let key = request.get_keys()[0].as_str();
                let values = request.get_values();
                ensure!(
                    values.len() == size as usize,
                    format!(
                        "values expected to have length {}, instead has length {}",
                        size,
                        values.len()
                    )
                );
                list_kv.insert(key.to_string(), values.clone());
            }
            _ => {
                bail!(
                    "YCSBLine  for server loader should only be parsed as Put, PutM, and PutList"
                );
            }
        }
        Ok(())
    }

    fn modify_server_state(
        &self,
        request: &Self::RequestLine,
        kv_server: &mut KVServer<D>,
        list_kv_server: &mut ListKVServer<D>,
        mempool_ids: &mut Vec<MempoolID>,
        datapath: &mut D,
    ) -> Result<()> {
        let msg_type = request.msg_type();
        match msg_type {
            MsgType::Put => {
                let key = request.get_keys()[0].as_str();
                let value = request.get_values()[0].as_str();
                let mut datapath_buffer =
                    allocate_datapath_buffer(datapath, value.len(), mempool_ids)?;
                let _ = datapath_buffer.write(value.as_bytes())?;
                kv_server.insert(key.to_string(), datapath_buffer);
            }
            MsgType::PutM(size) => {
                let keys = request.get_keys();
                let values = request.get_values();
                ensure!(
                    keys.len() == size as usize && values.len() == size as usize,
                    format!(
                        "Keys and values expected to have length {}, instead have length {} and {}",
                        size,
                        keys.len(),
                        values.len()
                    )
                );
                for (key, value) in keys.iter().zip(values.iter()) {
                    let mut datapath_buffer =
                        allocate_datapath_buffer(datapath, value.len(), mempool_ids)?;
                    let _ = datapath_buffer.write(value.as_str().as_bytes())?;
                    kv_server.insert(key.to_string(), datapath_buffer);
                }
            }
            MsgType::PutList(size) => {
                let key = request.get_keys()[0].as_str();
                let values = request.get_values();
                ensure!(
                    values.len() == size as usize,
                    format!(
                        "values expected to have length {}, instead has length {}",
                        size,
                        values.len()
                    )
                );
                let datapath_buffers: Result<Vec<D::DatapathBuffer>> = values
                    .iter()
                    .map(|value| {
                        let mut datapath_buffer =
                            allocate_datapath_buffer(datapath, value.len(), mempool_ids)?;
                        let _ = datapath_buffer.write(value.as_str().as_bytes())?;
                        Ok(datapath_buffer)
                    })
                    .collect();
                list_kv_server.insert(key.to_string(), datapath_buffers?);
            }
            _ => {
                bail!(
                    "YCSBLine  for server loader should only be parsed as Put, PutM, and PutList"
                );
            }
        }
        Ok(())
    }
}

pub struct YCSBClient {
    client_id: usize,
    thread_id: usize,
    total_num_clients: usize,
    total_num_threads: usize,
    cur_thread_id: usize,
    cur_client_id: usize,
    lines: Lines<BufReader<File>>,
    line_id: usize,
    value_size: usize,
    num_values: usize,
    num_keys: usize,
}

impl YCSBClient {
    pub fn set_num_keys(&mut self, num: usize) {
        self.num_keys = num;
    }

    pub fn set_value_size(&mut self, val: usize) {
        self.value_size = val;
    }

    pub fn set_num_values(&mut self, num: usize) {
        self.num_values = num;
    }

    fn increment(&mut self) {
        self.line_id += 1;
        self.increment_client_id_counter();
    }

    fn increment_client_id_counter(&mut self) {
        if (self.cur_client_id + 1) == self.total_num_clients {
            // increment thread when we reach the next client
            self.increment_thread_id_counter();
        }
        self.cur_client_id = (self.cur_client_id + 1) % self.total_num_clients;
    }

    fn increment_thread_id_counter(&mut self) {
        self.cur_thread_id = (self.cur_thread_id + 1) % self.total_num_threads;
    }
}

impl RequestGenerator for YCSBClient {
    type RequestLine = YCSBLine;

    fn new(
        request_file: &str,
        client_id: usize,
        thread_id: usize,
        max_clients: usize,
        max_threads: usize,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        let file = File::open(request_file)?;
        let reader = BufReader::new(file);
        Ok(YCSBClient {
            client_id: client_id,
            thread_id: thread_id,
            total_num_clients: max_clients,
            total_num_threads: max_threads,
            cur_thread_id: 0,
            cur_client_id: 0,
            lines: reader.lines(),
            line_id: 0,
            value_size: DEFAULT_VALUE_SIZE,
            num_keys: DEFAULT_NUM_KEYS,
            num_values: DEFAULT_NUM_VALUES,
        })
    }

    fn next_line(&mut self) -> Result<Option<String>> {
        loop {
            // find the next request with our client and thread id
            if self.cur_client_id == self.client_id && self.cur_thread_id == self.thread_id {
                if let Some(parsed_line_res) = self.lines.next() {
                    match parsed_line_res {
                        Ok(s) => {
                            tracing::debug!(
                                client_id = self.client_id,
                                thread_id = self.thread_id,
                                "Returning line {}",
                                self.line_id
                            );
                            self.increment();
                            return Ok(Some(s));
                        }
                        Err(e) => {
                            bail!("Could not get next line in iterator: {:?}", e);
                        }
                    }
                } else {
                    return Ok(None);
                }
            } else {
                if let Some(_) = self.lines.next() {
                    self.increment();
                } else {
                    return Ok(None);
                }
            }
        }
    }

    fn get_request(&self, line: &str) -> Result<Self::RequestLine> {
        YCSBLine::new(line, self.num_keys, self.num_values, self.value_size)
    }

    fn message_type(&self, req: &Self::RequestLine) -> Result<MsgType> {
        Ok(req.msg_type())
    }

    fn check_get(
        &self,
        request: &Self::RequestLine,
        value: &[u8],
        kv: &HashMap<String, String>,
    ) -> Result<()> {
        let key = request.get_keys()[0].as_str();
        match kv.get(key) {
            Some(val) => {
                ensure!(
                    val.as_str().as_bytes() == value,
                    format!(
                        "Check get failed. Expected: {:?}, Recved: {:?}",
                        val.as_str().as_bytes(),
                        value
                    )
                );
            }
            None => {
                ensure!(
                    value.len() == self.value_size,
                    format!(
                        "Expected value of length {}, received {}",
                        self.value_size,
                        value.len()
                    )
                );
            }
        }
        Ok(())
    }

    fn check_getm(
        &self,
        request: &Self::RequestLine,
        values: Vec<&[u8]>,
        kv: &HashMap<String, String>,
    ) -> Result<()> {
        ensure!(
            values.len() == self.num_values,
            format!(
                "received values of length {}, expected {}",
                values.len(),
                self.num_values
            )
        );
        for (key, received_value) in request.get_keys().iter().zip(values.iter()) {
            match kv.get(key) {
                Some(expected_value) => {
                    ensure!(
                        &expected_value.as_str().as_bytes() == received_value,
                        format!(
                            "Check get failed. Expected: {:?}, Recved: {:?}",
                            expected_value.as_str().as_bytes(),
                            received_value,
                        )
                    );
                }
                None => {
                    ensure!(
                        received_value.len() == self.value_size,
                        format!(
                            "Expected value of length {}, received {}",
                            self.value_size,
                            received_value.len()
                        )
                    );
                }
            }
        }
        Ok(())
    }

    fn check_get_list(
        &self,
        request: &Self::RequestLine,
        values: Vec<&[u8]>,
        list_kv: &HashMap<String, Vec<String>>,
    ) -> Result<()> {
        let key = request.get_keys()[0].as_str();
        match list_kv.get(key) {
            Some(val_list) => {
                ensure!(
                    values.len() == val_list.len(),
                    format!(
                        "expected values of length {}, received {}",
                        val_list.len(),
                        values.len(),
                    )
                );

                for (expected_value, received_value) in val_list.iter().zip(values.iter()) {
                    ensure!(
                        &expected_value.as_str().as_bytes() == received_value,
                        format!(
                            "Check get failed. Expected: {:?}, Recved: {:?}",
                            expected_value.as_str().as_bytes(),
                            received_value,
                        )
                    );
                }
            }
            None => {
                ensure!(
                    values.len() == self.num_values,
                    format!(
                        "Expected value list of length {}, received {}",
                        self.num_values,
                        values.len()
                    )
                );
                for (i, received_value) in values.iter().enumerate() {
                    ensure!(
                        received_value.len() == self.value_size,
                        format!(
                            "List item {}, Expected value of length {}, received {}",
                            i,
                            self.value_size,
                            received_value.len()
                        )
                    );
                }
            }
        }
        Ok(())
    }

    fn emit_get_data<'a>(&self, req: &'a Self::RequestLine) -> Result<&'a str> {
        Ok(req.get_keys()[0].as_str())
    }

    fn emit_put_data<'a>(&self, req: &'a Self::RequestLine) -> Result<(&'a str, &'a str)> {
        Ok((req.get_keys()[0].as_str(), req.get_values()[0].as_str()))
    }

    fn emit_getm_data<'a>(
        &self,
        req: &'a Self::RequestLine,
        _size: u16,
    ) -> Result<&'a Vec<String>> {
        Ok(&req.get_keys())
    }

    fn emit_putm_data<'a>(
        &self,
        req: &'a Self::RequestLine,
        _size: u16,
    ) -> Result<(&'a Vec<String>, &'a Vec<String>)> {
        Ok((&req.get_keys(), &req.get_values()))
    }

    fn emit_get_list_data<'a>(&self, req: &'a Self::RequestLine, _size: u16) -> Result<&'a str> {
        Ok(req.get_keys()[0].as_str())
    }

    fn emit_put_list_data<'a>(
        &self,
        req: &'a Self::RequestLine,
        _size: u16,
    ) -> Result<(&'a str, &'a Vec<String>)> {
        Ok((req.get_keys()[0].as_str(), &req.get_values()))
    }

    fn emit_append_data<'a>(
        &self,
        req: &'a Self::RequestLine,
        _size: u16,
    ) -> Result<(&'a str, &'a str)> {
        Ok((req.get_keys()[0].as_str(), req.get_values()[0].as_str()))
    }
}
