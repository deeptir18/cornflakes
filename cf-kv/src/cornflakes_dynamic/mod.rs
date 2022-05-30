pub mod kv_serializer {
    #![allow(non_camel_case_types)]
    #![allow(non_upper_case_globals)]
    #![allow(non_snake_case)]
    include!(concat!(env!("OUT_DIR"), "/kv_sga_cornflakes.rs"));
}
use cornflakes_libos::{
    datapath::{Datapath, MetadataOps, PushBufType, ReceivedPkt},
    dynamic_sga_hdr::SgaHeaderRepr,
    dynamic_sga_hdr::*,
    state_machine::server::ServerSM,
    ArenaOrderedSga, OrderedSga,
};
use kv_serializer::*;

use super::{ClientSerializer, KVServer, ListKVServer, MsgType};
use color_eyre::eyre::Result;
use std::marker::PhantomData;

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct CornflakesClient<D>
where
    D: Datapath,
{
    _datapath: PhantomData<D>,
}

impl<D> ClientSerializer<D> for CornflakesClient<D>
where
    D: Datapath,
{
    fn new() -> Self
    where
        Self: Sized,
    {
        CornflakesClient {
            _datapath: PhantomData::default(),
        }
    }

    // TODO: should check actually try to check the data in the packet is correct?
    fn check_recved_msg(&self, buf: &[u8], msg_type: MsgType, datapath: &D) -> Result<()> {
        unimplemented!();
    }
    fn serialize_get(&self, buf: &mut [u8], key: &str, datapath: &D) -> Result<usize> {
        let mut get = GetReq::new();
        get.set_key(CFString::new(key));
        get.serialize_into_buf(datapath, buf)
    }

    fn serialize_put(&self, buf: &mut [u8], key: &str, value: &str, datapath: &D) -> Result<usize> {
        let mut put = PutReq::new();
        put.set_key(CFString::new(key));
        put.set_val(CFBytes::new(value.as_bytes()));
        put.serialize_into_buf(datapath, buf)
    }

    fn serialize_getm(&self, buf: &mut [u8], keys: &Vec<String>, datapath: &D) -> Result<usize> {
        let mut getm = GetMReq::new();
        getm.init_keys(keys.len());
        let keys_list = getm.get_mut_keys();
        for key in keys.iter() {
            keys_list.append(CFString::new(key.as_str()));
        }
        getm.serialize_into_buf(datapath, buf)
    }

    fn serialize_putm(
        &self,
        buf: &mut [u8],
        keys: &Vec<String>,
        values: &Vec<String>,
        datapath: &D,
    ) -> Result<usize> {
        let mut putm = PutMReq::new();
        putm.init_keys(keys.len());
        let keys_list = putm.get_mut_keys();
        for key in keys.iter() {
            keys_list.append(CFString::new(key.as_str()));
        }

        putm.init_vals(values.len());
        let vals_list = putm.get_mut_vals();
        for val in values.iter() {
            vals_list.append(CFBytes::new(val.as_str().as_bytes()));
        }
        putm.serialize_into_buf(datapath, buf)
    }

    fn serialize_get_list(&self, buf: &mut [u8], key: &str, datapath: &D) -> Result<usize> {
        let mut get = GetListReq::new();
        get.set_key(CFString::new(key));
        get.serialize_into_buf(datapath, buf)
    }

    fn serialize_put_list(
        &self,
        buf: &mut [u8],
        key: &str,
        values: &Vec<String>,
        datapath: &D,
    ) -> Result<usize> {
        let mut put = PutListReq::new();
        put.set_key(CFString::new(key));
        put.init_vals(values.len());
        let mut vals = put.get_mut_vals();
        for val in values.iter() {
            vals.append(CFBytes::new(val.as_str().as_bytes()));
        }
        put.serialize_into_buf(datapath, buf)
    }

    fn serialize_append(
        &self,
        buf: &mut [u8],
        key: &str,
        value: &str,
        datapath: &D,
    ) -> Result<usize> {
        let mut append = PutReq::new();
        append.set_key(CFString::new(key));
        append.set_val(CFBytes::new(value.as_bytes()));
        append.serialize_into_buf(datapath, buf)
    }
}
