use super::MsgType;
use color_eyre::eyre::{bail, Result};
use cornflakes_libos::MsgID;

const MAX_BATCHES: usize = 8;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct YCSBRequest<'a> {
    pub key: &'a str,
    pub val: &'a str,
    pub req_type: MsgType,
    cur_idx: usize, // in erating the (Get-M) or (Put-M) request from this, where are we ?
    pub num_values: usize,
    pub req_id: MsgID,
}

impl<'a> YCSBRequest<'a> {
    // derive YCSB request from line of the file
    pub fn new(
        line: &'a str,
        num_values: usize,
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
        // in this workload: one key is paired with n values
        let key = &split.next().unwrap();
        for _i in 1..MAX_BATCHES {
            let _key = &split.next().unwrap();
        }
        match req {
            "GET" => Ok(YCSBRequest {
                key: key,
                val: "",
                req_type: MsgType::Get(num_values),
                num_values: num_values,
                cur_idx: 0,
                req_id: req_id,
            }),
            "UPDATE" => Ok(YCSBRequest {
                key: key,
                val: &split.next().unwrap()[0..value_size], // assumes the MAX value size we are testing is what is inside generated trace
                req_type: MsgType::Put(num_values),
                num_values: num_values,
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

    pub fn get_kv(&self) -> Result<(String, Vec<String>)> {
        Ok((
            self.key.to_string(),
            vec![self.val.to_string(); self.num_values],
        ))
    }

    pub fn get_key(&self) -> String {
        self.key.to_string()
    }
}
