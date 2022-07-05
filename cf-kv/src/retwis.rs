use super::{
    allocate_datapath_buffer, ClientSerializer, KVServer, ListKVServer, MsgType, RequestGenerator,
    ServerLoadGenerator, ZeroCopyPutKVServer, REQ_TYPE_SIZE,
};
use color_eyre::eyre::{bail, ensure, Result, WrapErr};
use cornflakes_libos::{allocator::MempoolID, datapath::Datapath};
use hashbrown::HashMap;
use rand::{
    distributions::{Alphanumeric, Distribution, Uniform, WeightedIndex},
    thread_rng, Rng,
};
use std::io::Write;
use zipf::ZipfDistribution;

pub const ADD_USER_GETS: usize = 1;
pub const ADD_USER_PUTS: usize = 3;

pub const POST_TWEET_GETS: usize = 3;
pub const POST_TWEET_PUTS: usize = 5;

pub const FOLLOW_UNFOLLOW_GETS: usize = 2;
pub const FOLLOW_UNFOLLOW_PUTS: usize = 2;

const RETWIS_DEFAULT_VALUE_SIZE: usize = 64;
const RETWIS_DEFAULT_NUM_KEYS: usize = 1_000_000;
const RETWIS_DEFAULT_KEY_SIZE: usize = 64;
const RETWIS_DEFAULT_ZIPF: f64 = 0.75;
const ADD_USER_WEIGHT: usize = 5;
const FOLLOW_UNFOLLOW_WEIGHT: usize = 15;
const POST_TWEET_WEIGHT: usize = 30;
const GET_TIMELINE_WEIGHT: usize = 50;
const GET_TIMELINE_MAX_SIZE: usize = 8;
const POSSIBLE_MESSAGE_TYPES: [MsgType; 4] = [
    MsgType::AddUser,
    MsgType::FollowUnfollow,
    MsgType::PostTweet,
    MsgType::GetTimeline(0),
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetwisRequestDistribution([usize; 4]);

impl RetwisRequestDistribution {
    fn to_weighted_distribution(&self) -> WeightedIndex<usize> {
        WeightedIndex::<usize>::new(self.0).unwrap()
    }
}

impl std::str::FromStr for RetwisRequestDistribution {
    type Err = color_eyre::eyre::Error;
    fn from_str(s: &str) -> Result<Self> {
        let items_result: Result<Vec<usize>> = s
            .split("-")
            .map(|x| match x.parse::<usize>() {
                Ok(s) => Ok(s),
                Err(e) => {
                    bail!("Failed to parse {}, {:?}", x, e);
                }
            })
            .collect();
        let items = items_result?;
        ensure!(items.len() == 4, "Request distribution must have 4 items");
        Ok(RetwisRequestDistribution([
            items[0], items[1], items[2], items[3],
        ]))
    }
}

fn get_key(idx: usize, key_length: usize) -> String {
    let key_name = format!("key_{}", idx);
    let additional_chars: String = std::iter::repeat("a")
        .take(key_length - key_name.len())
        .collect();
    format!("{}{}", key_name, additional_chars)
}

#[derive(Debug, Clone)]
pub enum RetwisValueSizeGenerator {
    UniformOverSizes(Vec<usize>, Uniform<usize>, usize),
    UniformOverRange(Uniform<usize>, usize),
    SingleValue(usize),
}

impl std::str::FromStr for RetwisValueSizeGenerator {
    type Err = color_eyre::eyre::Error;
    fn from_str(s: &str) -> Result<RetwisValueSizeGenerator> {
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

            Ok(RetwisValueSizeGenerator::from_range(numbers[0], numbers[1]))
        } else if split[0] == "UniformOverSizes" {
            Ok(RetwisValueSizeGenerator::from_sizes(numbers))
        } else if split[0] == "SingleValue" {
            if numbers.len() != 1 {
                bail!(
                    "Single value must provide a single size, got: {:?}",
                    numbers
                );
            }
            Ok(RetwisValueSizeGenerator::from_single_size(numbers[0]))
        } else {
            bail!("First part of value generator must be one of [UniformOverSizes, UniformOverRange, SingleValue], got: {:?}", split[0]);
        }
    }
}

impl Default for RetwisValueSizeGenerator {
    fn default() -> Self {
        RetwisValueSizeGenerator::SingleValue(RETWIS_DEFAULT_VALUE_SIZE)
    }
}

impl RetwisValueSizeGenerator {
    pub fn from_sizes(sizes: Vec<usize>) -> Self {
        let uniform = Uniform::from(0..sizes.len());
        let average = sizes.iter().sum::<usize>() / sizes.len();
        RetwisValueSizeGenerator::UniformOverSizes(sizes, uniform, average)
    }

    pub fn from_range(a: usize, b: usize) -> Self {
        RetwisValueSizeGenerator::UniformOverRange(Uniform::from(a..b), (a + b) / 2)
    }

    pub fn from_single_size(a: usize) -> Self {
        RetwisValueSizeGenerator::SingleValue(a)
    }

    fn sample(&self) -> usize {
        match self {
            RetwisValueSizeGenerator::UniformOverSizes(sizes, uniform, _) => {
                let mut rng = thread_rng();
                let index = uniform.sample(&mut rng);
                sizes[index]
            }
            RetwisValueSizeGenerator::UniformOverRange(uniform, _) => {
                let mut rng = thread_rng();
                uniform.sample(&mut rng)
            }
            RetwisValueSizeGenerator::SingleValue(value_size) => *value_size,
        }
    }

    pub fn avg_size(&self) -> usize {
        match self {
            RetwisValueSizeGenerator::UniformOverSizes(_, _, x) => *x,
            RetwisValueSizeGenerator::UniformOverRange(_, x) => *x,
            RetwisValueSizeGenerator::SingleValue(value_size) => *value_size,
        }
    }
}

#[derive(Debug, PartialEq, Clone, Eq)]
pub struct RetwisServerLine {
    key: String,
    value_size: usize,
}

impl RetwisServerLine {
    fn new(key: String, value_size: usize) -> Self {
        RetwisServerLine {
            key: key,
            value_size: value_size,
        }
    }

    pub fn value_size(&self) -> usize {
        self.value_size
    }

    pub fn key(&self) -> &str {
        self.key.as_str()
    }
}

#[derive(Debug, Clone)]
pub struct RetwisServerLoader {
    num_keys: usize,
    key_length: usize,
    value_size_generator: RetwisValueSizeGenerator,
}

impl RetwisServerLoader {
    pub fn new(
        num_keys: usize,
        key_length: usize,
        value_size_generator: RetwisValueSizeGenerator,
    ) -> Self {
        RetwisServerLoader {
            num_keys: num_keys,
            key_length: key_length,
            value_size_generator: value_size_generator,
        }
    }
}

impl ServerLoadGenerator for RetwisServerLoader {
    type RequestLine = RetwisServerLine;

    fn read_request(&self, line: &str) -> Result<Self::RequestLine> {
        let idx = line.to_string().parse::<usize>().unwrap();
        let key = get_key(idx, self.key_length);
        let value_size = self.value_size_generator.sample();
        Ok(RetwisServerLine::new(key, value_size))
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
        zero_copy_server: &mut ZeroCopyPutKVServer<D>,
        mempool_ids: &mut Vec<MempoolID>,
        datapath: &mut D,
        use_zero_copy_puts: bool,
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
                zero_copy_server,
                mempool_ids,
                datapath,
                use_zero_copy_puts,
            )?;
        }
        tracing::info!(
            "After load, kv_server size: {}, list kv server size: {}, zero copy server size {}",
            kv_server.len(),
            list_kv_server.len(),
            zero_copy_server.len()
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
        kv_server: &mut KVServer<D>,
        _list_kv_server: &mut ListKVServer<D>,
        zero_copy_server: &mut ZeroCopyPutKVServer<D>,
        mempool_ids: &mut Vec<MempoolID>,
        datapath: &mut D,
        use_zero_copy_puts: bool,
    ) -> Result<()>
    where
        D: Datapath,
    {
        let char = thread_rng().sample(&Alphanumeric) as char;
        let value: String = std::iter::repeat(char).take(request.value_size).collect();
        let mut datapath_buffer = allocate_datapath_buffer(datapath, value.len(), mempool_ids)?;
        let _ = datapath_buffer.write(value.as_bytes())?;
        if use_zero_copy_puts {
            let metadata = datapath.get_metadata(datapath_buffer)?.unwrap();
            zero_copy_server.insert(request.key().to_string(), metadata);
        } else {
            kv_server.insert(request.key().to_string(), datapath_buffer);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct RetwisClient {
    zipf_distribution: ZipfDistribution,
    value_generator: RetwisValueSizeGenerator,
    request_generator: WeightedIndex<usize>,
    get_timeline_size_generator: Uniform<usize>,
    keys: Vec<String>,
}

impl Default for RetwisClient {
    fn default() -> Self {
        let weights = [
            ADD_USER_WEIGHT,
            FOLLOW_UNFOLLOW_WEIGHT,
            POST_TWEET_WEIGHT,
            GET_TIMELINE_WEIGHT,
        ];
        RetwisClient::new(
            retwis_keys(RETWIS_DEFAULT_NUM_KEYS, RETWIS_DEFAULT_KEY_SIZE),
            RETWIS_DEFAULT_ZIPF,
            RetwisValueSizeGenerator::default(),
            RetwisRequestDistribution(weights),
        )
        .unwrap()
    }
}

pub fn retwis_keys(total_keys: usize, key_length: usize) -> Vec<String> {
    tracing::info!("Starting to initialize retwis client");
    let keys: Vec<String> = (0..total_keys)
        .map(|idx| get_key(idx, key_length))
        .collect();
    tracing::info!("Finished initializing retwis keys");
    keys
}

impl RetwisClient {
    pub fn new(
        keys: Vec<String>,
        zipf_coefficient: f64,
        value_generator: RetwisValueSizeGenerator,
        retwis_weights: RetwisRequestDistribution,
    ) -> Result<Self> {
        let zipf = ZipfDistribution::new(keys.len(), zipf_coefficient).unwrap();
        tracing::info!("Finished initializing zipf");
        Ok(RetwisClient {
            zipf_distribution: zipf,
            value_generator: value_generator,
            keys: keys,
            request_generator: retwis_weights.to_weighted_distribution(),
            get_timeline_size_generator: Uniform::from(0..GET_TIMELINE_MAX_SIZE),
        })
    }

    pub fn set_keys(&mut self, total_keys: usize, key_length: usize) -> Result<()> {
        ensure!(key_length >= 32, "Key length must atleast be 32");
        let keys: Vec<String> = (0..total_keys)
            .map(|idx| get_key(idx, key_length))
            .collect();
        self.keys = keys;
        Ok(())
    }

    pub fn set_zipf(&mut self, zipf_coefficient: f64) {
        self.zipf_distribution = ZipfDistribution::new(self.keys.len(), zipf_coefficient).unwrap();
    }

    pub fn set_value_generator(&mut self, gen: RetwisValueSizeGenerator) {
        self.value_generator = gen;
    }

    pub fn get_value(&self) -> String {
        let value_size = self.value_generator.sample();
        let char = thread_rng().sample(&Alphanumeric) as char;
        let ret: String = std::iter::repeat(char).take(value_size).collect();
        ret
    }
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct RetwisRequest {
    msg_type: MsgType,
    get_keys: Vec<usize>,
    put_values: Vec<String>,
}

impl RetwisRequest {
    pub fn new(msg_type: MsgType, keys: Vec<usize>, values: Vec<String>) -> Self {
        RetwisRequest {
            msg_type: msg_type,
            get_keys: keys,
            put_values: values,
        }
    }

    pub fn msg_type(&self) -> MsgType {
        self.msg_type
    }

    pub fn get_keys(&self) -> &Vec<usize> {
        &self.get_keys
    }

    pub fn get_values(&self) -> &Vec<String> {
        &self.put_values
    }
}

impl RequestGenerator for RetwisClient {
    type RequestLine = RetwisRequest;

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
        Ok(RetwisClient::default())
    }

    fn next_request(&mut self) -> Result<Option<Self::RequestLine>> {
        let mut rng = thread_rng();
        let req = match POSSIBLE_MESSAGE_TYPES[self.request_generator.sample(&mut rng)] {
            MsgType::AddUser => {
                let keys: Vec<usize> = (0..std::cmp::max(ADD_USER_GETS, ADD_USER_PUTS))
                    .map(|_i| self.zipf_distribution.sample(&mut rng) - 1)
                    .collect();
                let values: Vec<String> = (0..ADD_USER_PUTS).map(|_i| self.get_value()).collect();
                RetwisRequest::new(MsgType::AddUser, keys, values)
            }
            MsgType::FollowUnfollow => {
                let keys: Vec<usize> =
                    (0..std::cmp::max(FOLLOW_UNFOLLOW_GETS, FOLLOW_UNFOLLOW_PUTS))
                        .map(|_i| self.zipf_distribution.sample(&mut rng) - 1)
                        .collect();
                let values: Vec<String> = (0..FOLLOW_UNFOLLOW_PUTS)
                    .map(|_i| self.get_value())
                    .collect();
                RetwisRequest::new(MsgType::FollowUnfollow, keys, values)
            }
            MsgType::PostTweet => {
                let keys: Vec<usize> = (0..std::cmp::max(POST_TWEET_GETS, POST_TWEET_PUTS))
                    .map(|_i| self.zipf_distribution.sample(&mut rng) - 1)
                    .collect();
                let values: Vec<String> = (0..POST_TWEET_PUTS).map(|_i| self.get_value()).collect();
                RetwisRequest::new(MsgType::PostTweet, keys, values)
            }
            MsgType::GetTimeline(_default) => {
                let keys: Vec<usize> = (0..self.get_timeline_size_generator.sample(&mut rng) + 1)
                    .map(|_i| self.zipf_distribution.sample(&mut rng) - 1)
                    .collect();
                RetwisRequest::new(MsgType::GetTimeline(keys.len()), keys, vec![])
            }
            _ => {
                bail!("Other message types not implemented for retwis");
            }
        };
        Ok(Some(req))
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
        tracing::debug!(
            "Serializing request with msg type {:?} and keys length {}",
            request.msg_type(),
            request.get_keys().len()
        );
        let keys: Vec<&str> = request
            .get_keys()
            .iter()
            .map(|idx| self.keys[*idx].as_str())
            .collect();
        let values = request.get_values();
        let size = match request.msg_type() {
            MsgType::AddUser => serializer.serialize_add_user(
                &mut buf[REQ_TYPE_SIZE..],
                &keys,
                &values,
                &datapath,
            )?,
            MsgType::FollowUnfollow => serializer.serialize_add_follow_unfollow(
                &mut buf[REQ_TYPE_SIZE..],
                &keys,
                &values,
                &datapath,
            )?,
            MsgType::PostTweet => serializer.serialize_post_tweet(
                &mut buf[REQ_TYPE_SIZE..],
                &keys,
                &values,
                &datapath,
            )?,
            MsgType::GetTimeline(_size) => serializer.serialize_get_timeline(
                &mut buf[REQ_TYPE_SIZE..],
                &keys,
                &values,
                &datapath,
            )?,
            _ => {
                bail!("Retwis only serializes add user, follow unfollow, post tweet, and get timeline");
            }
        };
        Ok(REQ_TYPE_SIZE + size)
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
            MsgType::AddUser => {
                let num_values = serializer.check_add_user_num_values(buf)?;
                if num_values != ADD_USER_GETS {
                    tracing::warn!(
                        "Received add user with {} gets instead of {}",
                        num_values,
                        ADD_USER_GETS
                    );
                }
                Ok(num_values == ADD_USER_GETS)
            }
            MsgType::FollowUnfollow => {
                let num_values = serializer.check_follow_unfollow_num_values(buf)?;
                if num_values != FOLLOW_UNFOLLOW_GETS {
                    tracing::warn!(
                        "Received follow unfollow with {} gets instead of {}",
                        num_values,
                        FOLLOW_UNFOLLOW_GETS
                    );
                }
                Ok(num_values == FOLLOW_UNFOLLOW_GETS)
            }
            MsgType::PostTweet => {
                let num_values = serializer.check_post_tweet_num_values(buf)?;
                if num_values != POST_TWEET_GETS {
                    tracing::warn!(
                        "Received post tweet with {} gets instead of {}",
                        num_values,
                        POST_TWEET_GETS
                    );
                }
                Ok(num_values == POST_TWEET_GETS)
            }
            MsgType::GetTimeline(size) => {
                tracing::debug!(
                    "Received get timeline of size {}, should be {}",
                    serializer.check_get_timeline_num_values(buf)?,
                    size
                );
                Ok(serializer.check_get_timeline_num_values(buf)? == size)
            }
            _ => {
                bail!("Retwis only serializes add user, follow unfollow, post tweet, and get timeline");
            }
        }
    }
}