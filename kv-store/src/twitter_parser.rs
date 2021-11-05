use super::MsgType;
use color_eyre::eyre::{bail, ensure, Result};
use std::fs::File;
use std::io::prelude::*;


#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TwitterGets<'a> {
    pub key: &'a str,
    pub key_size: usize,
    pub val_size: usize,
    pub operation: MsgType,
}

impl<'a> TwitterGets<'a> {
    pub fn new(line: &'a str) -> Result<(TwitterGets<'a>)> {
      let mut split: std::str::Split<&'a str> = line.split(",");
      let mut vec_gets : Vec<&str> = split.collect::<Vec<&str>>();
      let op = vec_gets[5];
      match op {
        "get" => Ok(TwitterGets{
            key: vec_gets[1],
            key_size: vec_gets[2].parse::<usize>(),
            val_size: vec_gets[3].parse::<usize>(),
            operation: MsgType::Get(1),
        }),
        _ => Ok(TwitterGets{
            key: vec_gets[1],
            key_size: vec_gets[2].parse()::<usize>(),
            val_size: vec_gets[3].parse()::<usize>(),
            operation: MsgType::Put(1),
        })
      }
    }

    pub fn get_key(&self) -> &'a str {
        self.key
    }

    pub fn get_val_size(&self) -> usize {
        self.val_size
    }

    pub fn get_type(&self) -> MsgType {
        self.operation
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
    ) -> Result<()> {
        Ok(())
    }

    /*pub fn process(&mut self, line: &'a str) -> &'a str {
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
    }*/
}

/*impl<'a> Iterator for TwitterRequest<'a> {
    type Item = (String, &'a str);

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_idx == self.num_keys {
            return None;
        }

        self.cur_idx += 1;
        return Some((self.keys[self.cur_idx - 1].to_string(), self.val));
     }
}*/
