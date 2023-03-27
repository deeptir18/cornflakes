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

pub fn generate_ws_accessed(
    request_file: &str,
    speed_factor: f64,
    time: usize,
    value_size: Option<usize>,
) -> Result<()> {
    let file = File::open(request_file)?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    let modified_time = (time + 1) * speed_factor.ceil() as usize;
    let mut map: HashMap<String, usize> = HashMap::default();
    let mut reuse_distance: HashMap<String, Vec<usize>> = HashMap::default();
    let mut i = 0;
    let mut sets = vec![];
    let mut gets = vec![];
    while let Some(parsed_line_res) = lines.next() {
        match parsed_line_res {
            Ok(line) => {
                let req = TwitterLine::new(&line, &value_size)?;
                if req.msg_type() == MsgType::Put {
                    let key = req.get_key();
                    let v_size = req.get_value().len();
                    map.insert(key.to_string(), v_size);
                    sets.push(v_size as u64);
                    continue;
                }
                if req.get_time() > modified_time {
                    break;
                }
                let key = req.get_key();
                let v_size = req.get_value().len();
                gets.push(v_size as u64);
                if !map.contains_key(key) {
                    map.insert(key.to_string(), v_size);
                }

                // reuse distance
                match reuse_distance.get_mut(key) {
                    Some(set) => {
                        set.push(i);
                    }
                    None => {
                        reuse_distance.insert(key.to_string(), vec![i]);
                    }
                }
                i += 1;
            }
            Err(e) => {
                bail!("Could not get next line in iterator: {:?}", e);
            }
        }
    }

    // analyze actual accessed values for sets and gets
    let mut gets_size_hist = cornflakes_libos::timing::ManualHistogram::new_from_vec(gets);
    let mut sets_size_hist = cornflakes_libos::timing::ManualHistogram::new_from_vec(sets);

    // analyze map
    let mut total_value_size = 0;
    let mut total_key_size = 0;
    for (k, v) in map.iter() {
        total_value_size += *v;
        total_key_size += k.len();
    }
    tracing::info!(
        num_keys = map.len(),
        total_key_size = total_key_size,
        total_value_size = total_value_size,
        "Map stats"
    );

    // analyze reuse distance
    let mut ct = 0;
    let mut ct_1 = 0;
    for (_, uses) in reuse_distance.iter() {
        if uses.len() == 1 {
            ct_1 += 1;
        } else {
            ct += uses.len() + 1;
        }
    }
    let mut hist = cornflakes_libos::timing::ManualHistogram::new(ct);
    let mut reuse_count = cornflakes_libos::timing::ManualHistogram::new(map.len());
    for (_, uses) in reuse_distance.iter() {
        reuse_count.record(uses.len() as u64);
        if uses.len() == 1 {
            continue;
        } else {
            for idx in 1..uses.len() {
                let cur = uses[idx];
                let prev = uses[idx - 1];
                hist.record((cur - prev) as u64);
            }
        }
    }

    hist.sort()?;
    reuse_count.sort()?;
    gets_size_hist.sort()?;
    sets_size_hist.sort()?;
    tracing::info!(
        "{} entries only accessed once = {:?} % of {}",
        ct_1,
        ct_1 as f64 / map.len() as f64,
        map.len()
    );
    hist.dump("Reuse distance histogram")?;
    reuse_count.dump("Reuse count histogram")?;
    gets_size_hist.dump("Gets value size histogram")?;
    sets_size_hist.dump("Sets value size histogram")?;
    Ok(())
}

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
    // ignore given value size and use this size
    value_size: Option<usize>,
    // ignore sets
    ignore_sets: bool,
}

impl TwitterClient {
    pub fn new_twitter_client(
        request_file: &str,
        client_id: usize,
        thread_id: usize,
        max_clients: usize,
        max_threads: usize,
        twitter_end_time: usize,
        value_size: Option<usize>,
        ignore_sets: bool,
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
            value_size,
            ignore_sets,
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
                        if req.msg_type() == MsgType::Put && self.ignore_sets {
                            continue;
                        }
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
        TwitterLine::new(line, &self.value_size)
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
                        if req.msg_type() == MsgType::Put && self.ignore_sets {
                            continue;
                        }
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
    pub fn new(line: &str, value_size: &Option<usize>) -> Result<Self> {
        // parse the comma separated line
        let parts = line.split(",").collect::<Vec<&str>>();
        let time = parts[0].parse::<usize>()?;
        let k = parts[1];
        let mut v_size = parts[3].parse::<usize>()?;
        if let Some(x) = value_size {
            v_size = *x;
        }
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
    // optional (override) value size
    value_size: Option<usize>,
}

impl TwitterServerLoader {
    pub fn new(end_time: usize, min_num_keys: usize, value_size: Option<usize>) -> Self {
        TwitterServerLoader {
            twitter_end_time: end_time,
            min_keys_to_load: min_num_keys,
            value_size,
        }
    }
}

impl ServerLoadGenerator for TwitterServerLoader {
    type RequestLine = TwitterLine;

    fn read_request(&self, line: &str) -> Result<Self::RequestLine> {
        TwitterLine::new(line, &self.value_size)
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