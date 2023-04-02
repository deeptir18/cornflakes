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
use std::io::Write;

// taken from: https://sagark.org/assets/pubs/protoacc-micro2021-preprint.pdf
// figure 4c
const DEFAULT_BUCKETS: [(usize, usize, f64); 8] = [
    (1, 9, 33.8),
    (9, 17, 15.3),
    (17, 23, 7.6),
    (23, 33, 13.5),
    (33, 65, 10.6),
    (65, 129, 6.1),
    (129, 513, 8.0),
    (513, usize::MAX, 5.6),
];
const DEFAULT_NUM_KEYS: usize = 1_000_000;
const DEFAULT_KEYS_SIZE: usize = 100;
const DEFAULT_NUM_VALUES: usize = 1;

#[derive(Debug, Clone)]
pub enum NumValuesDistribution {
    UniformOverRange(Uniform<usize>),
    SingleValue(usize),
}
impl Default for NumValuesDistribution {
    fn default() -> Self {
        NumValuesDistribution::SingleValue(DEFAULT_NUM_VALUES)
    }
}

impl NumValuesDistribution {
    pub fn from_range(a: usize, b: usize) -> Self {
        NumValuesDistribution::UniformOverRange(Uniform::from(a..b))
    }

    pub fn from_single_size(a: usize) -> Self {
        NumValuesDistribution::SingleValue(a)
    }

    fn sample(&self) -> usize {
        match self {
            NumValuesDistribution::UniformOverRange(uniform) => {
                let mut rng = thread_rng();
                uniform.sample(&mut rng)
            }
            NumValuesDistribution::SingleValue(value_size) => *value_size,
        }
    }
}

impl std::str::FromStr for NumValuesDistribution {
    type Err = color_eyre::eyre::Error;
    fn from_str(s: &str) -> Result<NumValuesDistribution> {
        let split: Vec<&str> = s.split("-").collect();
        if split.len() < 2 {
            bail!(
                "Request shape pattern needs atleast 2 items, got: {:?}",
                split
            );
        }
        let numbers_result: Result<Vec<usize>> = split
            .iter()
            .skip(1)
            .map(|x| match x.parse::<usize>() {
                Ok(s) => Ok(s),
                Err(e) => {
                    bail!("Failed to parse {}: {:?}", x, e);
                }
            })
            .collect();
        let numbers = numbers_result.wrap_err("Not able to parse numbers result")?;
        if split[0] == "UniformOverRange" {
            if numbers.len() != 2 {
                bail!(
                    "UniformOverRange must provide a start and end range; got: {:?}",
                    numbers
                );
            }

            Ok(NumValuesDistribution::from_range(numbers[0], numbers[1]))
        } else if split[0] == "SingleValue" {
            if numbers.len() != 1 {
                bail!(
                    "Single value must provide a single size, got: {:?}",
                    numbers
                );
            }
            Ok(NumValuesDistribution::from_single_size(numbers[0]))
        } else {
            bail!("First part of value generator must be one of [UniformOverRange, SingleValue], got: {:?}", split[0]);
        }
    }
}

pub fn default_buckets() -> (Vec<(usize, usize)>, Vec<f64>) {
    let probs: Vec<f64> = DEFAULT_BUCKETS.iter().map(|(_, _, p)| *p).collect();
    let buckets: Vec<(usize, usize)> = DEFAULT_BUCKETS.iter().map(|(x, y, _)| (*x, *y)).collect();
    (buckets, probs)
}

#[derive(Debug, Clone)]
pub struct ValueSizeDistribution {
    // get bucket index
    weighted_index: WeightedIndex<f64>,
    // each bucket samples uniformly between two sizes
    buckets: Vec<Uniform<usize>>,
    avg_size: f64,
}

impl ValueSizeDistribution {
    pub fn new(max_size: usize, mut buckets: Vec<(usize, usize)>, probs: Vec<f64>) -> Result<Self> {
        let buckets_len = buckets.len();
        buckets[buckets_len - 1].1 = max_size;
        ensure!(
            probs.len() == buckets.len(),
            "Probability length must match bucket length"
        );

        let weighted_index = WeightedIndex::<f64>::new(probs.clone())?;
        let uniform_distrs: Vec<Uniform<usize>> = buckets
            .iter()
            .map(|(start, end)| Uniform::from(*start..*end))
            .collect();
        let mut avg_size = 0f64;
        for i in 0..buckets.len() {
            avg_size += (((buckets[i].0 + buckets[i].1) as f64) / 2f64) * probs[i];
        }
        Ok(ValueSizeDistribution {
            weighted_index,
            buckets: uniform_distrs,
            avg_size,
        })
    }

    pub fn sample(&self) -> usize {
        let mut rng = thread_rng();
        let bucket_index = self.weighted_index.sample(&mut rng);
        let s = self.buckets[bucket_index].sample(&mut rng);
        return s;
    }

    pub fn avg_size(&self) -> f64 {
        self.avg_size
    }
}

fn get_key(idx: usize, key_length: usize) -> String {
    let key_name = format!("key_{}", idx);
    let additional_chars: String = std::iter::repeat("a")
        .take(key_length - key_name.len())
        .collect();
    format!("{}{}", key_name, additional_chars)
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct GoogleProtobufServerLine {
    key: String,
    value_sizes: Vec<usize>,
}
impl GoogleProtobufServerLine {
    fn new(key: String, value_sizes: Vec<usize>) -> Self {
        GoogleProtobufServerLine { key, value_sizes }
    }

    pub fn value_sizes(&self) -> &Vec<usize> {
        &self.value_sizes
    }

    pub fn key(&self) -> &str {
        self.key.as_str()
    }
}

#[derive(Debug, Clone)]
pub struct GoogleProtobufServerLoader {
    num_keys: usize,
    key_length: usize,
    num_values_distribution: NumValuesDistribution,
    value_size_generator: ValueSizeDistribution,
    max_size: usize,
}

impl GoogleProtobufServerLoader {
    pub fn new(
        num_keys: usize,
        key_length: usize,
        value_size_generator: ValueSizeDistribution,
        num_values_distribution: NumValuesDistribution,
        max_size: usize,
    ) -> Self {
        GoogleProtobufServerLoader {
            num_keys,
            key_length,
            num_values_distribution,
            value_size_generator,
            max_size,
        }
    }
}

impl ServerLoadGenerator for GoogleProtobufServerLoader {
    type RequestLine = GoogleProtobufServerLine;

    fn read_request(&self, line: &str) -> Result<Self::RequestLine> {
        let idx = line.to_string().parse::<usize>().unwrap();
        let key = get_key(idx, self.key_length);
        let num_values = self.num_values_distribution.sample();
        let mut value_sizes: Vec<usize> = (0..num_values)
            .map(|_| self.value_size_generator.sample())
            .collect();
        let mut sum = value_sizes.iter().sum::<usize>();
        // to make sure the entire request won't exceed a jumbo frame
        while sum > self.max_size {
            value_sizes = (0..num_values)
                .map(|_| self.value_size_generator.sample())
                .collect();
            sum = value_sizes.iter().sum::<usize>();
        }

        Ok(GoogleProtobufServerLine::new(key, value_sizes))
    }

    fn load_ref_kv_file(
        &self,
        _request_file: &str,
        kv_server: &mut HashMap<String, String>,
        list_kv_server: &mut HashMap<String, Vec<String>>,
    ) -> Result<()> {
        for i in 0..self.num_keys {
            let request = self.read_request(&format!("{}", i))?;
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
        for i in 0..self.num_keys {
            let request = self.read_request(&format!("{}", i))?;
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
        // for google protobuf loader, only used linked list kv server
        let char = thread_rng().sample(&Alphanumeric) as char;
        for value_size in request.value_sizes().iter() {
            let value: String = std::iter::repeat(char).take(*value_size).collect();
            let mut datapath_buffer = allocate_datapath_buffer(datapath, value.len(), mempool_ids)?;
            let _ = datapath_buffer.write(value.as_bytes())?;
            linked_list_kv_server.insert(request.key().to_string(), datapath_buffer);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct GoogleProtobufClient {
    keys: Vec<String>,
    keys_to_sample: Vec<usize>,
    cur_key: usize,
}

pub fn google_protobuf_keys(total_keys: usize, key_length: usize) -> Vec<String> {
    let keys: Vec<String> = (0..total_keys)
        .map(|idx| get_key(idx, key_length))
        .collect();
    keys
}

impl GoogleProtobufClient {
    pub fn new(total_keys: usize, key_length: usize, total_keys_to_sample: usize) -> Self {
        let keys: Vec<String> = (0..total_keys)
            .map(|idx| get_key(idx, key_length))
            .collect();
        let key_distr = Uniform::from(0..total_keys);
        let mut rng = thread_rng();
        let keys_to_sample: Vec<usize> = (0..total_keys_to_sample)
            .map(|_| key_distr.sample(&mut rng))
            .collect();
        GoogleProtobufClient {
            keys,
            keys_to_sample,
            cur_key: 0,
        }
    }
}

impl Default for GoogleProtobufClient {
    fn default() -> Self {
        GoogleProtobufClient::new(DEFAULT_NUM_KEYS, DEFAULT_KEYS_SIZE, 1000000)
    }
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct GoogleProtobufRequest(usize);

impl RequestGenerator for GoogleProtobufClient {
    type RequestLine = GoogleProtobufRequest;

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
        Ok(GoogleProtobufClient::default())
    }

    fn next_request(&mut self) -> Result<Option<Self::RequestLine>> {
        let idx = self.keys_to_sample[self.cur_key];
        self.cur_key += 1;
        Ok(Some(GoogleProtobufRequest(idx)))
    }

    fn message_type(&self, _req: &GoogleProtobufRequest) -> Result<MsgType> {
        Ok(MsgType::GetList(0))
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
        MsgType::GetList(0).to_buf(buf);
        Ok(serializer.serialize_get_list(
            &mut buf[REQ_TYPE_SIZE..],
            self.keys[request.0].as_str(),
            datapath,
        )? + REQ_TYPE_SIZE)
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
        let vals = serializer.deserialize_getlist_response(buf)?;
        match vals.len() > 0 {
            true => return Ok(true),
            false => return Ok(false),
        }
    }
}
