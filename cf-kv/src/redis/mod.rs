// pub mod kv_serializer {
//     #![allow(unused_variables)]
//     #![allow(non_camel_case_types)]
//     #![allow(non_upper_case_globals)]
//     #![allow(non_snake_case)]
//     include!(concat!(env!("OUT_DIR"), "/kv_sga_cornflakes.rs"));
// }
use cornflakes_libos::{
    datapath::Datapath,
//     dynamic_sga_hdr::SgaHeaderRepr,
//     dynamic_sga_hdr::*,
};
use hashbrown::HashMap;
// use kv_serializer::*;
use redis;

use super::{ClientSerializer, MsgType, RequestGenerator};
use color_eyre::eyre::Result;
#[cfg(feature = "profiler")]
use perftools;
use std::{marker::PhantomData};

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct RedisClient<D>
where
    D: Datapath,
{
    _datapath: PhantomData<D>,
}

impl<D> ClientSerializer<D> for RedisClient<D>
where
    D: Datapath,
{
    fn new() -> Self
    where
        Self: Sized,
    {
        RedisClient {
            _datapath: PhantomData::default(),
        }
    }

    fn check_recved_msg<L>(
        &self,
        buf: &[u8],
        _datapath: &D,
        request_generator: &L,
        request: &L::RequestLine,
        ref_kv: &HashMap<String, String>,
        ref_list_kv: &HashMap<String, Vec<String>>,
    ) -> Result<()>
    where
        L: RequestGenerator,
    {
        match request_generator.message_type(request)? {
            MsgType::Get => {
                // let mut get_resp = GetResp::new();
                // get_resp.deserialize(buf)?;
                // ensure!(get_resp.has_val(), "Get Response does not have a value");
                // request_generator.check_get(request, get_resp.get_val().get_ptr(), ref_kv)?;
                todo!()
            }
            MsgType::Put => {}
            MsgType::GetM(_size) => {
                unimplemented!()
            }
            MsgType::PutM(_size) => {}
            MsgType::GetList(_size) => {
                unimplemented!()
            }
            MsgType::PutList(_size) => {}
            MsgType::AppendToList(_size) => {}
        }
        Ok(())
    }

    fn serialize_get(&self, buf: &mut [u8], key: &str, datapath: &D) -> Result<usize> {
        let data = redis::cmd("GET").arg(key).get_packed_command();
        buf[0..data.len()].copy_from_slice(&data);
        Ok(data.len())
    }

    fn serialize_put(
        &self,
        buf: &mut [u8],
        key: &str,
        value: &str,
        _datapath: &D,
    ) -> Result<usize> {
        let data = redis::cmd("SET").arg(key).arg(value).get_packed_command();
        buf[0..data.len()].copy_from_slice(&data);
        Ok(data.len())
    }

    fn serialize_getm(
        &self,
        _buf: &mut [u8],
        _keys: &Vec<String>,
        _datapath: &D,
    ) -> Result<usize> {
        unimplemented!()
    }

    fn serialize_putm(
        &self,
        _buf: &mut [u8],
        _keys: &Vec<String>,
        _values: &Vec<String>,
        _datapath: &D,
    ) -> Result<usize> {
        unimplemented!()
    }

    fn serialize_get_list(
        &self,
        _buf: &mut [u8],
        _key: &str,
        _datapath: &D,
    ) -> Result<usize> {
        unimplemented!()
    }

    fn serialize_put_list(
        &self,
        _buf: &mut [u8],
        _key: &str,
        _values: &Vec<String>,
        _datapath: &D,
    ) -> Result<usize> {
        unimplemented!()
    }

    fn serialize_append(
        &self,
        _buf: &mut [u8],
        _key: &str,
        _value: &str,
        _datapath: &D,
    ) -> Result<usize> {
        unimplemented!()
    }
}
