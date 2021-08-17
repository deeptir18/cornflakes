use super::MsgType;
use color_eyre::eyre::{bail, ensure, Result};
use cornflakes_libos::MsgID;

const ALPHABET: &str = "abcdefghijklmnopqrstuvwxyz";

pub fn get_client_id(line: &str) -> Result<usize> {
    let mut split: std::str::Split<&str> = line.split(" ");
    let id = match &split.next().unwrap().parse::<usize>() {
        Ok(x) => *x,
        Err(e) => {
            bail!("Could not parse string request: {:?}", e);
        }
    };
    Ok(id)
}

#[derive(Clone, PartialEq, Eq, Copy, Debug)]
pub struct YCSBRequest<'a> {
    pub key: &'a str,
    pub val: &'a str,
    pub req_type: MsgType,
    pub client_id: usize,
    cur_idx: usize, // in generating the (Get-M) or (Put-M) request from this, where are we ?
    pub num_keys: usize,
    pub req_id: MsgID,
}

impl<'a> YCSBRequest<'a> {
    // derive YCSB request from line of the file
    pub fn new(
        line: &'a str,
        num_keys: usize,
        value_size: usize,
        req_id: MsgID,
    ) -> Result<YCSBRequest<'a>> {
        let mut split: std::str::Split<&'a str> = line.split(" ");
        let id = match &split.next().unwrap().parse::<usize>() {
            Ok(x) => *x,
            Err(e) => {
                bail!("Could not parse string request: {:?}", e);
            }
        };
        let req = split.next().unwrap();
        let key = &split.next().unwrap();

        match req {
            "GET" => Ok(YCSBRequest {
                key: key,
                val: "",
                req_type: MsgType::Get(num_keys),
                client_id: id,
                num_keys: num_keys,
                cur_idx: 0,
                req_id: req_id,
            }),
            "UPDATE" => Ok(YCSBRequest {
                key: key,
                val: &split.next().unwrap()[0..value_size], // assumes the MAX value size we are testing is what is inside generated trace
                req_type: MsgType::Put(num_keys),
                client_id: id,
                num_keys: num_keys,
                cur_idx: 0,
                req_id: req_id,
            }),
            x => {
                bail!("Unknown request time: {:?}", x);
            }
        }
    }

    pub fn get_val(&self) -> &'a str {
        self.val
    }

    pub fn get_type(&self) -> MsgType {
        self.req_type
    }

    pub fn get_id(&self) -> MsgID {
        self.req_id
    }

    pub fn get_next_kv(&mut self) -> Result<(String, &'a str)> {
        ensure!(self.cur_idx < self.num_keys, "No more keys in iterator");
        self.cur_idx += 1;
        Ok((replace_key_field(self.key, self.cur_idx - 1), self.val))
    }
}

fn replace_key_field(key: &str, idx: usize) -> String {
    let mut ret = key.to_string();
    tracing::debug!(
        "RET: {}, idx: {}, alphabet len: {}",
        ret,
        idx,
        ALPHABET.len()
    );
    ret.replace_range(
        (key.len() - 1)..key.len(),
        ALPHABET.get(idx..(idx + 1)).unwrap(),
    );
    ret
}

impl<'a> Iterator for YCSBRequest<'a> {
    type Item = (String, &'a str);

    fn next(&mut self) -> Option<Self::Item> {
        tracing::debug!("cur_idx: {}", self.cur_idx);
        if self.cur_idx == self.num_keys {
            tracing::debug!("Ending iterator");
            return None;
        }

        self.cur_idx += 1;
        return Some((replace_key_field(self.key, self.cur_idx - 1), self.val));
    }
}
