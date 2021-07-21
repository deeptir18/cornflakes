use super::MsgType;
use color_eyre::eyre::{bail, Result};

const ALPHABET: &str = "abcdefghijklmnopqrstuvwxyz";

#[derive(Clone, PartialEq, Eq, Copy, Debug)]
pub struct YCSBRequest<'a> {
    pub key: &'a str,
    pub val: &'a str,
    pub req_type: MsgType,
    pub client_id: usize,
    cur_idx: usize, // in generating the (Get-M) or (Put-M) request from this, where are we ?
    pub num_keys: usize,
}

impl<'a> YCSBRequest<'a> {
    // derive YCSB request from line of the file
    pub fn new(line: &'a str, num_keys: usize, value_size: usize) -> Result<YCSBRequest<'a>> {
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
            }),
            "UPDATE" => Ok(YCSBRequest {
                key: key,
                val: &split.next().unwrap()[0..value_size],
                req_type: MsgType::Put(num_keys),
                client_id: id,
                num_keys: num_keys,
                cur_idx: 0,
            }),
            x => {
                bail!("Unknown request time: {:?}", x);
            }
        }
    }

    pub fn get_type(&self) -> MsgType {
        return self.req_type;
    }
}

fn replace_key_field(key: &str, idx: usize) -> String {
    let mut ret = key.to_string();
    ret.replace_range(
        (key.len() - 1)..key.len(),
        ALPHABET.get(idx..(idx + 1)).unwrap(),
    );
    ret
}

impl<'a> Iterator for YCSBRequest<'a> {
    type Item = (String, &'a str);

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_idx == self.num_keys {
            return None;
        }

        self.cur_idx += 1;
        return Some((replace_key_field(self.key, self.cur_idx - 1), self.val));
    }
}
