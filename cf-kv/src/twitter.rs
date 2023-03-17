use super::{
    allocate_datapath_buffer, ClientSerializer, KVServer, LinkedListKVServer, ListKVServer,
    MsgType, RequestGenerator, ServerLoadGenerator, REQ_TYPE_SIZE,
};
use color_eyre::eyre::{bail, Result};
use cornflakes_libos::{
    allocator::MempoolID,
    datapath::Datapath,
    loadgen::request_schedule::{DistributionType, PacketSchedule},
};
use hashbrown::HashMap;
use rand::{distributions::Alphanumeric, seq::SliceRandom, thread_rng, Rng};
use std::{
    fs::File,
    io::{prelude::*, BufReader, Lines, Write},
    time::Duration,
};

pub struct TwitterClient {
    /// view into trace file,
    lines: Lines<BufReader<File>>,
    // our thread id
    thread_id: usize,
    // our client id
    client_id: usize,
    // total number of clients
    total_num_clients: usize,
    // total number of threads
    total_num_threads: usize,
    // end time to run trace until
    twitter_end_time: usize,
}

impl TwitterClient {
    pub fn new_twitter_client(
        request_file: &str,
        client_id: usize,
        thread_id: usize,
        max_clients: usize,
        max_threads: usize,
        twitter_end_time: usize,
    ) -> Result<Self> {
        let file = File::open(request_file)?;
        let reader = BufReader::new(file);
        Ok(TwitterClient {
            client_id,
            thread_id,
            total_num_clients: max_clients,
            total_num_threads: max_threads,
            twitter_end_time,
            lines: reader.lines(),
        })
    }

    pub fn generate_packet_schedule(
        &self,
        request_file: &str,
        speed_factor: f64,
        dist_type: DistributionType,
    ) -> Result<PacketSchedule> {
        let file = File::open(request_file)?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        let modified_time = (self.twitter_end_time + 1) * speed_factor.ceil() as usize;
        let mut pps: Vec<usize> = vec![0usize; modified_time + 1];
        tracing::info!(
            "Generating schedule for {} twitter seconds worth of packets",
            modified_time
        );
        while let Some(parsed_line_res) = lines.next() {
            match parsed_line_res {
                Ok(s) => {
                    let req = self.get_request(s.as_str())?;
                    if self.is_responsible_for(&req) {
                        let time = req.get_time();
                        if time > modified_time {
                            break;
                        }
                        pps[time] += 1;
                    }
                }
                Err(e) => {
                    bail!("Could not get next line in iterator: {:?}", e);
                }
            }
        }
        // create schedule from rates and speed factor
        let mut schedule = PacketSchedule::default();
        let time_unit = Duration::from_nanos((1_000_000_000 as f64 / speed_factor as f64) as u64);
        let mut time_to_add = Duration::from_nanos(0);
        for num_packets_to_generate in pps.iter() {
            let scaled_packet_rate = (*num_packets_to_generate as f64 * speed_factor as f64) as u64;
            let mut sched =
                PacketSchedule::new(*num_packets_to_generate, scaled_packet_rate, dist_type)?;
            schedule.append(&mut sched);
            // if no packets,
            // pad previous intersend with skip time
            if *num_packets_to_generate == 0 {
                time_to_add += time_unit;
                if let Some(dur) = schedule.get_last() {
                    schedule.set_last(dur + time_to_add);
                    time_to_add = Duration::from_nanos(0);
                }
            }
        }
        Ok(schedule)
    }

    fn get_request(&self, line: &str) -> Result<<Self as RequestGenerator>::RequestLine> {
        TwitterLine::new(line)
    }

    fn is_responsible_for(&self, req: &TwitterLine) -> bool {
        let our_thread_client = self.total_num_threads * self.client_id + self.thread_id;
        let total_thread_clients = self.total_num_threads * self.total_num_clients;
        our_thread_client == (req.get_client_id() % total_thread_clients)
    }

    fn emit_get_data<'a>(
        &self,
        req: &'a <Self as RequestGenerator>::RequestLine,
    ) -> Result<&'a str> {
        Ok(req.get_key())
    }

    fn emit_put_data<'a>(
        &self,
        req: &'a <Self as RequestGenerator>::RequestLine,
    ) -> Result<(&'a str, &'a str)> {
        Ok((req.get_key(), req.get_value()))
    }
}

impl RequestGenerator for TwitterClient {
    type RequestLine = TwitterLine;
    fn new(
        _request_file: &str,
        _client_id: usize,
        _thread_id: usize,
        _max_clients: usize,
        _max_threads: usize,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        unimplemented!();
    }

    fn next_request(&mut self) -> Result<Option<Self::RequestLine>> {
        // find the next request with our client and thread ID
        loop {
            if let Some(parsed_line_res) = self.lines.next() {
                match parsed_line_res {
                    Ok(s) => {
                        let req = self.get_request(s.as_str())?;
                        if self.is_responsible_for(&req) {
                            return Ok(Some(req));
                        }
                        // otherwise, continue iterating until the next line
                    }
                    Err(e) => {
                        bail!("Could not get next line in iterator: {:?}", e);
                    }
                }
            } else {
                return Ok(None);
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
            ),
            MsgType::Put => {
                let put_data = self.emit_put_data(&request)?;
                serializer.serialize_put(
                    &mut buf[REQ_TYPE_SIZE..],
                    put_data.0,
                    put_data.1,
                    datapath,
                )
            }
            _ => {
                bail!("Twitter loader does not handle anything other than gets and puts");
            }
        }?;
        Ok(REQ_TYPE_SIZE + bufsize)
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
        match request.msg_type() {
            MsgType::Get => {
                if cfg!(debug_assertions) {
                    let val = serializer.deserialize_get_response(buf)?;
                    if val.len() != request.value_size() {
                        return Ok(false);
                    }
                }
            }
            MsgType::Put => {}
            _ => {
                bail!("Twitter loader does not handle more than get and put");
            }
        }
        return Ok(true);
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct TwitterLine {
    key: String,
    value: String,
    value_size: usize,
    client_id: usize,
    twitter_time: usize,
    msg_type: MsgType,
}

impl TwitterLine {
    pub fn new(line: &str) -> Result<Self> {
        // parse the comma separated line
        let parts = line.split(",").collect::<Vec<&str>>();
        let time = parts[0].parse::<usize>()?;
        let k = parts[1];
        let v_size = parts[3].parse::<usize>()?;
        let client_id = parts[4].parse::<usize>()?;
        let msg_type = match parts[5] {
            "get" => MsgType::Get,
            "set" => MsgType::Put,
            "add" => MsgType::Put,
            x => bail!("Unknown msg type: {}", x),
        };

        Ok(TwitterLine::new_from(k, v_size, client_id, time, msg_type))
    }

    pub fn get_client_id(&self) -> usize {
        self.client_id
    }
    pub fn value_size(&self) -> usize {
        self.value.len()
    }
    pub fn new_from(
        k: &str,
        value_size: usize,
        client_id: usize,
        twitter_time: usize,
        msg_type: MsgType,
    ) -> Self {
        let c = char::from(rand::thread_rng().sample(&Alphanumeric));
        let value = std::iter::repeat(c).take(value_size).collect::<String>();
        TwitterLine {
            key: k.to_string(),
            value_size: value.len(),
            value,
            client_id,
            twitter_time,
            msg_type,
        }
    }
    pub fn get_time(&self) -> usize {
        self.twitter_time
    }

    pub fn msg_type(&self) -> MsgType {
        self.msg_type
    }

    pub fn get_key(&self) -> &str {
        self.key.as_str()
    }

    pub fn get_value(&self) -> &str {
        self.value.as_str()
    }
}

pub struct TwitterServerLoader {
    // server should iterate across trace, adding all requests to kv store that show up in a get to
    // have a default value
    twitter_end_time: usize,
    // make sure minimum number of keys are loaded
    min_keys_to_load: usize,
}

impl TwitterServerLoader {
    pub fn new(end_time: usize, min_num_keys: usize) -> Self {
        TwitterServerLoader {
            twitter_end_time: end_time,
            min_keys_to_load: min_num_keys,
        }
    }
}

impl ServerLoadGenerator for TwitterServerLoader {
    type RequestLine = TwitterLine;

    fn read_request(&self, line: &str) -> Result<Self::RequestLine> {
        TwitterLine::new(line)
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
        let batch_size = 1000;
        let mut reached_end_time = false;
        let mut reached_min_keys = false;
        let file = File::open(request_file)?;
        let reader = BufReader::new(file);
        let mut lines_iterator = reader.lines();
        let mut last_time = 0;
        while !(reached_end_time && reached_min_keys) {
            let mut lines_vec: Vec<Self::RequestLine> = Vec::with_capacity(batch_size);
            for _ in 0..batch_size {
                match lines_iterator.next() {
                    Some(line) => {
                        let request = self.read_request(&line?)?;
                        last_time = request.get_time();
                        if request.get_time() > self.twitter_end_time {
                            reached_end_time = true;
                        }
                        if kv_server.len() >= self.min_keys_to_load
                            || linked_list_kv_server.len() >= self.min_keys_to_load
                            || list_kv_server.len() >= self.min_keys_to_load
                        {
                            reached_min_keys = true;
                        }
                        if reached_end_time && reached_min_keys {
                            break;
                        }
                        lines_vec.push(request)
                    }
                    None => {
                        break;
                    }
                }
            }

            // shuffle the vector to randomize where values are loaded
            let mut vec: Vec<usize> = (0..lines_vec.len()).collect();
            vec.shuffle(&mut thread_rng());
            for idx in vec.iter() {
                let request = &lines_vec[*idx];
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
        }
        tracing::info!(trace = request_file, mempool_ids =? mempool_ids, "Finished loading trace file until about  time {}", last_time);
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
        kv_server: &mut KVServer<D>,
        _list_kv_server: &mut ListKVServer<D>,
        _linked_list_kv_server: &mut LinkedListKVServer<D>,
        mempool_ids: &mut Vec<MempoolID>,
        datapath: &mut D,
        _use_linked_list_kv_server: bool,
    ) -> Result<()>
    where
        D: Datapath,
    {
        let msg_type = request.msg_type();
        match msg_type {
            MsgType::Get => {
                let key = request.get_key();
                let value = request.get_value();
                if kv_server.contains_key(key) {
                    // only allocate for new keys not seen before
                    return Ok(());
                }
                let mut datapath_buffer =
                    allocate_datapath_buffer(datapath, value.len(), mempool_ids)?;
                let _ = datapath_buffer.write(value.as_bytes())?;
                kv_server.insert(key.to_string(), datapath_buffer);
            }
            _ => {}
        }

        Ok(())
    }
}
