use super::MsgType;
use cornflakes_libos::MsgID;
use color_eyre::eyre::{Result};
use std::str;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TwitterRequest<'a> {
    pub key: &'a str,
    pub val: &'a str,
    pub key_size: usize,
    pub val_size: usize,
    pub second: u64,
    pub client_id: u64,
    pub req_type: MsgType,
}

impl<'a> TwitterRequest<'a> {
    pub fn new(line: &'a str) -> Result<TwitterRequest<'a>> {
      let split: std::str::Split<&'a str> = line.split(",");
      let vec_gets : Vec<&str> = split.collect::<Vec<&str>>();
      let op = vec_gets[5];

      match op {
        "get" => Ok(TwitterRequest{
            key: vec_gets[1],
            val: "",//vec_gets[vec_gets.len() - 1],
            key_size: vec_gets[2].parse::<usize>().unwrap(),
            val_size: vec_gets[3].parse::<usize>().unwrap(),
            second: vec_gets[0].parse::<u64>().unwrap(),
            client_id: vec_gets[4].parse::<u64>().unwrap(),
            req_type: MsgType::Get(1),
        }),
        _ => Ok(TwitterRequest{
            key: vec_gets[1],
            val: "a",//str::from_utf8(raw_bytestring)?,
            key_size: vec_gets[2].parse::<usize>().unwrap(),
            val_size: vec_gets[3].parse::<usize>().unwrap(),
            second: vec_gets[0].parse::<u64>().unwrap(),
            client_id: vec_gets[4].parse::<u64>().unwrap(),
            req_type: MsgType::Put(1),
        })
      }
    }

    pub fn get_next_kv(&mut self) -> Result<(String, &'a str)> {
      /*  ensure!(self.cur_idx < self.num_keys, "No more keys in iterator");
        self.cur_idx += 1;
        Ok((self.keys[self.cur_idx - 1].to_string(), self.val))*/
        Ok((self.key.to_string(), self.get_val()))
    }

    pub fn get_key(&self) -> &'a str {
        self.key
    }

    pub fn get_val(&self) -> &'a str {
        self.val
    }

    pub fn get_client(&self) -> u64 {
        self.client_id
    }

    pub fn get_val_size(&self) -> usize {
        self.val_size
    }

    pub fn get_type(&self) -> MsgType {
        self.req_type
    }

    pub fn get_second(&self) -> u64 {
        self.second
    }
    
    pub fn get_id(&self) -> MsgID {
        MsgID::default()
    }
}

impl<'a> Iterator for TwitterRequest<'a> {
    type Item = (String, &'a str);

    fn next(&mut self) -> Option<Self::Item> {
        return Some((self.key.to_string(), self.get_val()));
     }
}
