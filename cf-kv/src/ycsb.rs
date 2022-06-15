use super::{
    allocate_datapath_buffer, ClientSerializer, KVServer, ListKVServer, MsgType, RequestGenerator,
    ServerLoadGenerator, ZeroCopyPutKVServer, REQ_TYPE_SIZE,
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
        for i in 0..MAX_BATCHES {
            let key = &split.next().unwrap();
            if i < num_keys {
                keys.push(key.to_string());
            }
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
    allocate_contiguously: bool,
}

impl YCSBServerLoader {
    pub fn new(
        value_size: usize,
        num_values: usize,
        num_keys: usize,
        allocate_contiguously: bool,
    ) -> Self {
        YCSBServerLoader {
            value_size: value_size,
            num_values: num_values,
            num_keys: num_keys,
            allocate_contiguously: allocate_contiguously,
        }
    }

    fn modify_server_state_round<D>(
        &self,
        request: &<Self as ServerLoadGenerator>::RequestLine,
        kv_server: &mut KVServer<D>,
        list_kv_server: &mut ListKVServer<D>,
        mempool_ids: &mut Vec<MempoolID>,
        datapath: &mut D,
        round: usize,
    ) -> Result<()>
    where
        D: Datapath,
    {
        let msg_type = request.msg_type();
        match msg_type {
            MsgType::Put => {
                assert!(round == 0);
                let key = request.get_keys()[0].as_str();
                let value = request.get_values()[0].as_str();
                let mut datapath_buffer =
                    allocate_datapath_buffer(datapath, value.len(), mempool_ids)?;
                let _ = datapath_buffer.write(value.as_bytes())?;
                kv_server.insert(key.to_string(), datapath_buffer);
            }
            MsgType::PutM(size) => {
                ensure!(
                    request.get_values().len() == size as usize && request.get_keys().len() == size as usize,
                    format!(
                        "values and keys expected to have length {}, instead has length: values {}, keys {}",
                        size,
                        request.get_values().len(),
                        request.get_keys().len(),
                    )
                );
                let key = request.get_keys()[round].as_str();
                let value = request.get_values()[round].as_str();
                let mut datapath_buffer =
                    allocate_datapath_buffer(datapath, value.len(), mempool_ids)?;
                let _ = datapath_buffer.write(value.as_bytes())?;
                kv_server.insert(key.to_string(), datapath_buffer);
            }
            MsgType::PutList(size) => {
                ensure!(
                    request.get_values().len() == size as usize,
                    format!(
                        "values expected to have length {}, instead has length {}",
                        size,
                        request.get_values().len()
                    )
                );
                let key = request.get_keys()[0].as_str();
                let value = request.get_values()[round].as_str();
                let mut datapath_buffer =
                    allocate_datapath_buffer(datapath, value.len(), mempool_ids)?;
                let _ = datapath_buffer.write(value.as_bytes())?;
                if list_kv_server.contains_key(key) {
                    let list = list_kv_server.get_mut_map().get_mut(key).unwrap();
                    list.push(datapath_buffer);
                } else {
                    list_kv_server.insert(key.to_string(), vec![datapath_buffer]);
                }
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

impl ServerLoadGenerator for YCSBServerLoader {
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

    fn load_file<D>(
        &self,
        request_file: &str,
        kv_server: &mut KVServer<D>,
        list_kv_server: &mut ListKVServer<D>,
        _zero_copy_server: &mut ZeroCopyPutKVServer<D>,
        mempool_ids: &mut Vec<MempoolID>,
        datapath: &mut D,
        _use_zero_copy_puts: bool,
    ) -> Result<()>
    where
        D: Datapath,
    {
        if !self.allocate_contiguously {
            for i in 0..self.num_values {
                let file = File::open(request_file)?;
                let reader = BufReader::new(file);
                for line in reader.lines() {
                    let request = self.read_request(&line?)?;
                    self.modify_server_state_round(
                        &request,
                        kv_server,
                        list_kv_server,
                        mempool_ids,
                        datapath,
                        i,
                    )?;
                }
            }
        } else {
            let file = File::open(request_file)?;
            let reader = BufReader::new(file);
            for line in reader.lines() {
                let request = self.read_request(&line?)?;
                for i in 0..self.num_values {
                    self.modify_server_state_round(
                        &request,
                        kv_server,
                        list_kv_server,
                        mempool_ids,
                        datapath,
                        i,
                    )?;
                }
            }
        }
        tracing::info!(trace = request_file, mempool_ids =? mempool_ids, "Finished loading trace file");
        Ok(())
    }

    fn modify_server_state<D>(
        &self,
        request: &Self::RequestLine,
        kv_server: &mut KVServer<D>,
        list_kv_server: &mut ListKVServer<D>,
        _zero_copy_server: &mut ZeroCopyPutKVServer<D>,
        mempool_ids: &mut Vec<MempoolID>,
        datapath: &mut D,
        _use_zero_copy_puts: bool,
    ) -> Result<()>
    where
        D: Datapath,
    {
        for i in 0..self.num_values {
            self.modify_server_state_round(
                request,
                kv_server,
                list_kv_server,
                mempool_ids,
                datapath,
                i,
            )?;
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

    fn emit_get_data<'a>(
        &self,
        req: &'a <Self as RequestGenerator>::RequestLine,
    ) -> Result<&'a str> {
        Ok(req.get_keys()[0].as_str())
    }

    fn emit_put_data<'a>(
        &self,
        req: &'a <Self as RequestGenerator>::RequestLine,
    ) -> Result<(&'a str, &'a str)> {
        Ok((req.get_keys()[0].as_str(), req.get_values()[0].as_str()))
    }

    fn emit_getm_data<'a>(
        &self,
        req: &'a <Self as RequestGenerator>::RequestLine,
        _size: u16,
    ) -> Result<&'a Vec<String>> {
        Ok(&req.get_keys())
    }

    fn emit_putm_data<'a>(
        &self,
        req: &'a <Self as RequestGenerator>::RequestLine,
        _size: u16,
    ) -> Result<(&'a Vec<String>, &'a Vec<String>)> {
        Ok((&req.get_keys(), &req.get_values()))
    }

    fn emit_get_list_data<'a>(
        &self,
        req: &'a <Self as RequestGenerator>::RequestLine,
        _size: u16,
    ) -> Result<&'a str> {
        Ok(req.get_keys()[0].as_str())
    }

    fn emit_put_list_data<'a>(
        &self,
        req: &'a <Self as RequestGenerator>::RequestLine,
        _size: u16,
    ) -> Result<(&'a str, &'a Vec<String>)> {
        Ok((req.get_keys()[0].as_str(), &req.get_values()))
    }

    fn emit_append_data<'a>(
        &self,
        req: &'a <Self as RequestGenerator>::RequestLine,
        _size: u16,
    ) -> Result<(&'a str, &'a str)> {
        Ok((req.get_keys()[0].as_str(), req.get_values()[0].as_str()))
    }

    fn check_get(
        &self,
        request: &<Self as RequestGenerator>::RequestLine,
        value: Vec<u8>,
        kv: &HashMap<String, String>,
    ) -> Result<()> {
        let key = request.get_keys()[0].as_str();
        match kv.get(key) {
            Some(val) => {
                ensure!(
                    val.as_str().as_bytes() == value.as_slice(),
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
        request: &<Self as RequestGenerator>::RequestLine,
        values: Vec<Vec<u8>>,
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
                        &expected_value.as_str().as_bytes() == &received_value.as_slice(),
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
        request: &<Self as RequestGenerator>::RequestLine,
        values: Vec<Vec<u8>>,
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
                        &expected_value.as_str().as_bytes() == &received_value.as_slice(),
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

    fn get_request(&self, line: &str) -> Result<<Self as RequestGenerator>::RequestLine> {
        YCSBLine::new(line, self.num_keys, self.num_values, self.value_size)
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

    fn next_request(&mut self) -> Result<Option<Self::RequestLine>> {
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
                            return Ok(Some(self.get_request(&s)?));
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

    fn message_type(&self, req: &<Self as RequestGenerator>::RequestLine) -> Result<MsgType> {
        Ok(req.msg_type())
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
        let bufsize = match request.msg_type() {
            MsgType::Get => serializer.serialize_get(
                &mut buf[REQ_TYPE_SIZE..],
                self.emit_get_data(&request)?,
                datapath,
            )?,
            MsgType::Put => {
                let put_data = self.emit_put_data(&request)?;
                serializer.serialize_put(&mut buf[2..], put_data.0, put_data.1, datapath)?
            }
            MsgType::GetM(size) => serializer.serialize_getm(
                &mut buf[REQ_TYPE_SIZE..],
                self.emit_getm_data(&request, size)?,
                datapath,
            )?,
            MsgType::PutM(size) => {
                let put_data = self.emit_putm_data(&request, size)?;
                serializer.serialize_putm(
                    &mut buf[REQ_TYPE_SIZE..],
                    put_data.0,
                    put_data.1,
                    datapath,
                )?
            }
            MsgType::GetList(size) => serializer.serialize_get_list(
                &mut buf[REQ_TYPE_SIZE..],
                self.emit_get_list_data(&request, size)?,
                datapath,
            )?,
            MsgType::PutList(size) => {
                let put_data = self.emit_put_list_data(&request, size)?;
                serializer.serialize_put_list(
                    &mut buf[REQ_TYPE_SIZE..],
                    put_data.0,
                    put_data.1,
                    datapath,
                )?
            }
            MsgType::AppendToList(size) => {
                let append_data = self.emit_append_data(&request, size)?;
                serializer.serialize_append(
                    &mut buf[REQ_TYPE_SIZE..],
                    append_data.0,
                    append_data.1,
                    datapath,
                )?
            }
            _ => {
                bail!(
                    "YCSB client does not handle request type: {:?}",
                    request.msg_type()
                );
            }
        };
        Ok(bufsize + REQ_TYPE_SIZE)
    }
    fn check_response<S, D>(
        &self,
        request: &<Self as RequestGenerator>::RequestLine,
        buf: &[u8],
        serializer: &S,
        kv: &HashMap<String, String>,
        list_kv: &HashMap<String, Vec<String>>,
    ) -> Result<bool>
    where
        S: ClientSerializer<D>,
        D: Datapath,
    {
        match request.msg_type() {
            MsgType::Get => {
                if cfg!(debug_assertions) {
                    let val = serializer.deserialize_get_response(buf)?;
                    self.check_get(&request, val, &kv)?;
                }
            }
            MsgType::GetM(_size) => {
                if cfg!(debug_assertions) {
                    let vals = serializer.deserialize_getm_response(buf)?;
                    self.check_getm(&request, vals, &kv)?;
                }
            }
            MsgType::GetList(_size) => {
                if cfg!(debug_assertions) {
                    let vals = serializer.deserialize_getlist_response(buf)?;
                    self.check_get_list(&request, vals, &list_kv)?;
                }
            }
            MsgType::Put => {}
            MsgType::PutM(_size) => {}
            MsgType::PutList(_size) => {}
            _ => {
                bail!("YCSB Generator does not check requests other than get & put");
            }
        }

        return Ok(true);
    }
}
