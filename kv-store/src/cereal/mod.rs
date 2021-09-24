use super::{
    ycsb_parser::YCSBRequest, KVSerializer, MsgType, SerializedRequestGenerator, ALIGN_SIZE,
};
use color_eyre::eyre::{bail, ensure, Result, WrapErr};
use cornflakes_libos::{CfBuf, Datapath, RcCornPtr, RcCornflake, ReceivedPkt};
use cxx;
use hashbrown::HashMap;
use std::{io::Write, marker::PhantomData, slice};

#[cxx::bridge]
mod ffi {
    unsafe extern "C++" {
        include!("kv-store/src/cereal/include/cereal_headers.hh");
        type GetRequest;
        type GetResponse;
        /*type GetMRequest;
        type GetMResponse;
        type PutRequest;
        type PutMRequest;
        type PutResponse;*/

        fn set_id(self: &GetRequest, id: u32);
        fn get_id(self: &GetRequest) -> u32;
        fn set_key(self: &GetRequest, key: &[u8]);
        fn get_key(self: &GetRequest) -> &CxxString;
        fn serialized_size(self: &GetRequest) -> usize;
        fn serialize_to_array(self: &GetRequest, buf: &mut [u8]);
        fn new_get_request() -> UniquePtr<GetRequest>;
        fn deserialize_get_request_from_array(buf: &[u8]) -> UniquePtr<GetRequest>;

        fn set_id(self: &GetResponse, id: u32);
        fn get_id(self: &GetResponse) -> u32;
        fn set_value(self: &GetResponse, key: &[u8]);
        fn get_value(self: &GetResponse) -> &CxxString;
        fn serialized_size(self: &GetResponse) -> usize;
        fn serialize_to_array(self: &GetResponse, buf: &mut [u8]);
        fn new_get_response() -> UniquePtr<GetResponse>;
        fn deserialize_get_response_from_array(buf: &[u8]) -> UniquePtr<GetResponse>;
    }
}
