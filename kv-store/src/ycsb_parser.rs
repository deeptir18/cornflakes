use super::MsgType;
use color_eyre::eyre::{bail, ensure, Result};
use cornflakes_libos::MsgID;

const MAX_BATCHES: usize = 8;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct YCSBRequest<'a> {
    pub keys: Vec<&'a str>,
    pub val: &'a str,
    pub req_type: MsgType,
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
        let _ = match &split.next().unwrap().parse::<usize>() {
            Ok(x) => *x,
            Err(e) => {
                bail!("Could not parse string request: {:?}", e);
            }
        };
        let req = split.next().unwrap();
        let mut keys: Vec<&str> = Vec::default();
        for i in 0..MAX_BATCHES {
            let key = &split.next().unwrap();
            if i < num_keys {
                keys.push(key)
            }
        }

        match req {
            "GET" => Ok(YCSBRequest {
                keys: keys,
                val: "",
                req_type: MsgType::Get(num_keys),
                num_keys: num_keys,
                cur_idx: 0,
                req_id: req_id,
            }),
            "UPDATE" => Ok(YCSBRequest {
                keys: keys,
                val: &split.next().unwrap()[0..value_size], // assumes the MAX value size we are testing is what is inside generated trace
                req_type: MsgType::Put(num_keys),
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
        Ok((self.keys[self.cur_idx - 1].to_string(), self.val))
    }
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
        return Some((self.keys[self.cur_idx - 1].to_string(), self.val));
    }
}
