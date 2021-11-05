use super::MsgType;
use color_eyre::eyre::{bail, ensure, Result};
use cornflakes_libos::MsgID;
use std::fs::File;
use std::io::prelude::*;


#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TwitterGets<'a> {
    pub mut get_key: Vec<&'a str>,
    pub mut get_key_size: Vec<usize>,
    pub mut val_size: Vec<usize>,
    pub mut not_get_key: Vec<&'a str>,
}

impl<'a> TwitterGets<'a> {
    pub fn new(line: &'a str) -> Result<(TwitterGets<'a>)> {
      let mut split: std::str::Split<&'a str> = line.split(",");
      let mut twitter_request = TwitterRequest {};
      split.next(); // Skip timestamp
      debug!("Timestamp: {}", self.timestamp);
      let temp_key = split.next().unwrap();
      self.key_size.push_back(split.next().unwrap().parse()::<usize>());
      self.key.push_back(temp_key[0..self.key_size]);
      self.value_size.push_back(split.next().unwrap().parse()::<usize>());
      self.val.push_back("");
      self.client_id.push_back(split.next().unwrap().parse()::<usize>());
      let op = split.next().unwrap();
      match op {
        "Get" => {
        }
      }
        self.operation.push_back();
        self.ttl.push_back(split.next().unwrap().parse()::<usize>());
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TwitterRequest<'a> {
    pub timestamp: Vec<usize>,
    pub key: Vec<&'a str>,
    pub key_size: Vec<usize>,
    pub val_size: Vec<usize>,
    pub val: Vec<&'a str>,
    pub client_id: Vec<usize>,
    pub operation: Vec<MsgType>,
    pub ttl: Vec<usize>, 
}

impl<'a> TwitterRequest<'a> {
    // derive Twitter request from the file itself
    pub fn new(
        line: &'a str,
    ) -> Result<(Twi)> {
        Ok(())
    }

    pub fn process(&mut self, line: &'a str) -> &'a str {
        let mut split: std::str::Split<&'a str> = line.split(",");
        let mut twitter_request = TwitterRequest {};
        self.timestamp.push_back(split.next().unwrap().parse()::<usize>());
        debug!("Timestamp: {}", self.timestamp);
        let temp_key = split.next().unwrap();
        self.key_size.push_back(split.next().unwrap().parse()::<usize>());
        self.key.push_back(temp_key[0..self.key_size]);
        self.value_size.push_back(split.next().unwrap().parse()::<usize>());
        self.val.push_back("");
        self.client_id.push_back(split.next().unwrap().parse()::<usize>());
        let op = split.next().unwrap();
        match op {
            "Get" => ,
            _ => ,
        };
        self.operation.push_back();
        self.ttl.push_back(split.next().unwrap().parse()::<usize>());
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

impl<'a> Iterator for TwitterRequest<'a> {
    type Item = (String, &'a str);

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_idx == self.num_keys {
            return None;
        }

        self.cur_idx += 1;
        return Some((self.keys[self.cur_idx - 1].to_string(), self.val));
    }
}
