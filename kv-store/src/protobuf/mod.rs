pub mod kv_messages {
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(dead_code)]
    #![allow(improper_ctypes)]
    #![allow(unused_imports)]
    include!(concat!(env!("OUT_DIR"), "/kv_proto.rs"));
}
use super::{
    ycsb_parser::YCSBRequest, twitter_parser::TwitterRequest, KVSerializer, MsgType, SerializedRequestGenerator, ALIGN_SIZE,
};
use color_eyre::eyre::{bail, ensure, Result, WrapErr};
use cornflakes_libos::{CfBuf, Datapath, RcCornPtr, RcCornflake, ReceivedPkt};
use hashbrown::HashMap;
use protobuf::{CodedOutputStream, Message};
use std::{io::Write, marker::PhantomData};

pub struct ProtobufSerializer<D>
where
    D: Datapath,
{
    _marker: PhantomData<D>,
}

impl<D> KVSerializer<D> for ProtobufSerializer<D>
where
    D: Datapath,
{
    type HeaderCtx = Vec<u8>;

    fn new_server(_serialize_to_native_buffers: bool) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(ProtobufSerializer {
            _marker: PhantomData,
        })
    }

    fn handle_get<'a>(
        &mut self,
        pkt: ReceivedPkt<D>,
        map: &HashMap<String, CfBuf<D>>,
        num_values: usize,
        offset: usize,
    ) -> Result<(Self::HeaderCtx, RcCornflake<'a, D>)> {
        let mut output_vec = Vec::default();
        let mut output_stream = CodedOutputStream::vec(&mut output_vec);
        let message = pkt.contiguous_slice(offset, pkt.data_len() - offset)?;
        tracing::debug!("Received message len: {}", message.len());
        match num_values {
            0 => {
                bail!("Cannot have 0 as num_values");
            }
            1 => {
                let get_request = kv_messages::GetReq::parse_from_bytes(message)
                    .wrap_err("Failed to deserialize proto GetReq")?;
                let key = get_request.get_key();
                let value = match map.get(key) {
                    Some(v) => v,
                    None => {
                        bail!("Cannot find value for key in KV store: {:?}", key);
                    }
                };
                let mut get_resp = kv_messages::GetResp::new();
                get_resp.set_val(value.as_ref().to_vec());
                get_resp.set_id(get_request.get_id());
                get_resp
                    .write_to(&mut output_stream)
                    .wrap_err("Failed to write to CodedOutputStream for protobuf GetResp")?;
            }
            _x => {
                let getm_request = kv_messages::GetMReq::parse_from_bytes(message)
                    .wrap_err("Failed to deserialize proto GetMResp")?;
                let keys = getm_request.get_keys();
                let mut getm_resp = kv_messages::GetMResp::new();
                let vals = getm_resp.mut_vals();
                tracing::debug!("Received getm request with {} keys", keys.len());
                for key in keys.iter() {
                    let value = match map.get(key) {
                        Some(v) => v,
                        None => {
                            bail!("Cannot find value for key in KV store: {:?}", key);
                        }
                    };
                    tracing::debug!("Found val for key {}", key);
                    vals.push(value.as_ref().to_vec());
                }
                getm_resp.set_id(getm_request.get_id());
                getm_resp
                    .write_to(&mut output_stream)
                    .wrap_err("Failed to write to CodedOutputStream for protobuf GetMResp")?;
            }
        }
        output_stream
            .flush()
            .wrap_err("Failed to flush output stream.")?;
        Ok((output_vec, RcCornflake::with_capacity(1)))
    }

    fn handle_put<'a>(
        &self,
        pkt: ReceivedPkt<D>,
        map: &mut HashMap<String, CfBuf<D>>,
        num_values: usize,
        offset: usize,
        connection: &mut D,
    ) -> Result<(Self::HeaderCtx, RcCornflake<'a, D>)> {
        let mut output_vec = Vec::default();
        let mut output_stream = CodedOutputStream::vec(&mut output_vec);
        let message = pkt.contiguous_slice(offset, pkt.data_len() - offset)?;
        match num_values {
            0 => {
                bail!("Cannot have 0 as num_values");
            }
            1 => {
                let put_request = kv_messages::PutReq::parse_from_bytes(message)
                    .wrap_err("Could not deserialize PutReq proto")?;
                let key = put_request.get_key();
                let val = put_request.get_val();
                let mut datapath_buffer = CfBuf::allocate(connection, val.len(), ALIGN_SIZE)
                    .wrap_err(format!(
                        "Failed to allocate CfBuf for put req # {}",
                        put_request.get_id()
                    ))?;
                if datapath_buffer
                    .write(val)
                    .wrap_err("Failed to write bytes into CfBuf.")?
                    != val.len()
                {
                    bail!(
                        "Failed to write all of the value bytes into CfBuf. for req {}",
                        put_request.get_id()
                    );
                }
                map.insert(key.to_string(), datapath_buffer);
                let mut put_resp = kv_messages::PutResp::new();
                put_resp.set_id(put_request.get_id());
                put_resp
                    .write_to(&mut output_stream)
                    .wrap_err("Failed to write into CodedOutputStream for put_resp proto")?;
            }
            x => {
                let putm_request = kv_messages::PutMReq::parse_from_bytes(message)
                    .wrap_err("Could not deserialize PutMReq proto")?;
                let keys = putm_request.get_keys();
                let vals = putm_request.get_vals();
                for i in 0..x {
                    let key = &keys[i];
                    let val = &vals[i];
                    let mut datapath_buffer = CfBuf::allocate(connection, val.len(), ALIGN_SIZE)
                        .wrap_err(format!(
                            "Failed to allocate CfBuf for put req # {}",
                            putm_request.get_id()
                        ))?;
                    if datapath_buffer
                        .write(&val.as_slice())
                        .wrap_err("Failed to write bytes into CfBuf.")?
                        != val.len()
                    {
                        bail!(
                            "Failed to write all of the value bytes into CfBuf. for req {}",
                            putm_request.get_id()
                        );
                    }
                    map.insert(key.to_string(), datapath_buffer);
                }
                let mut put_resp = kv_messages::PutResp::new();
                put_resp.set_id(putm_request.get_id());
                tracing::debug!("Set put resp id as {}", put_resp.get_id());
                put_resp
                    .write_to(&mut output_stream)
                    .wrap_err("Failed to write into CodedOutputStream for put_resp proto")?;
            }
        }
        output_stream
            .flush()
            .wrap_err("Failed to flush output stream.")?;
        tracing::debug!("Output bytes: {:?}", output_vec);
        Ok((output_vec, RcCornflake::with_capacity(1)))
    }

    fn process_header<'a>(
        &self,
        ctx: &'a Self::HeaderCtx,
        cornflake: &mut RcCornflake<'a, D>,
    ) -> Result<()> {
        cornflake.add_entry(RcCornPtr::RawRef(&ctx.as_slice()));
        Ok(())
    }
}

impl<D> SerializedRequestGenerator<D> for ProtobufSerializer<D>
where
    D: Datapath,
{
    fn new_request_generator() -> Self {
        ProtobufSerializer {
            _marker: PhantomData,
        }
    }

    fn check_recved_msg(
        &self,
        pkt: &ReceivedPkt<D>,
        msg_type: MsgType,
        value_size: usize,
        _keys: Vec<String>,
        _hashmap: &HashMap<String, String>,
        _check_value: bool,
    ) -> Result<bool> {
        let message = pkt.contiguous_slice(0, pkt.data_len())?;
        let id = pkt.get_id();
        tracing::debug!("Returned bytes: {:?}", message);
        match msg_type {
            MsgType::Get(num_values) => match num_values {
                0 => {
                    bail!("Request cannot have 0 as number of values");
                }
                1 => {
                    let get_resp = kv_messages::GetResp::parse_from_bytes(message)
                        .wrap_err("Could not parse get_resp from message")?;
                    ensure!(
                        get_resp.get_id() == id,
                        "Get Resp id does not match packet id"
                    );
                    let val = get_resp.get_val();
                    ensure!(
                        val.len() == value_size,
                        "Value size in GetResp does not much, expected: {}, actual: {}",
                        value_size,
                        val.len()
                    );
                }
                x => {
                    let getm_resp = kv_messages::GetMResp::parse_from_bytes(message)
                        .wrap_err("Could not parse getm_resp from message")?;
                    ensure!(
                        getm_resp.get_id() == id,
                        "GetM Resp id does not match packet id"
                    );
                    let vals = getm_resp.get_vals();
                    ensure!(vals.len() == x, "GetM Response does not have expected number of values, expected: {}, actual: {}", x, vals.len());
                    for (i, val) in vals.iter().enumerate() {
                        ensure!(
                            val.len() == value_size,
                            "Value {} size in GetResp does not much, expected: {}, actual: {}",
                            i,
                            value_size,
                            val.len()
                        );
                    }
                }
            },
            MsgType::Put(num_values) => match num_values {
                0 => {
                    bail!("Request cannot have 0 as number of values");
                }
                _x => {
                    let put_resp = kv_messages::PutResp::parse_from_bytes(message)
                        .wrap_err("Could not parse put resp from message")?;
                    ensure!(
                        put_resp.get_id() == id,
                        "Put Resp id does not match packet id"
                    );
                }
            },
        }
        Ok(true)
    }

    fn write_next_request<'a>(
        &self,
        mut buf: &mut [u8],
        req: &mut YCSBRequest<'a>,
    ) -> Result<usize> {
        let mut output_vec = Vec::default();
        let mut output_stream = CodedOutputStream::vec(&mut output_vec);
        match req.get_type() {
            MsgType::Get(size) => match size {
                0 => {
                    bail!("Msg size cannot be 0");
                }
                1 => {
                    let (key, _val) = req.get_next_kv()?;
                    let mut get_req = kv_messages::GetReq::new();
                    get_req.set_id(req.get_id());
                    get_req.set_key(key.to_string());
                    get_req
                        .write_to(&mut output_stream)
                        .wrap_err("Failed to write into CodedOutputStream for GetReq proto")?;
                }
                x => {
                    let mut request_keys: Vec<String> = Vec::with_capacity(x);
                    while let Some((key, _val)) = req.next() {
                        request_keys.push(key);
                    }
                    let mut getm_req = kv_messages::GetMReq::new();
                    getm_req.set_id(req.get_id());
                    let keys = getm_req.mut_keys();
                    for i in 0..x {
                        keys.push(request_keys[i].to_string());
                    }
                    tracing::debug!("Constructed get request with length {}", keys.len());
                    tracing::debug!("Proto's keys length: {}", getm_req.get_keys().len());
                    getm_req
                        .write_to(&mut output_stream)
                        .wrap_err("Failed to write into CodedOutputStream for GetMReq proto")?;
                }
            },
            MsgType::Put(size) => match size {
                0 => {
                    bail!("Msg size cannot be 0");
                }
                1 => {
                    let (key, val) = req.get_next_kv()?;
                    let mut put_req = kv_messages::PutReq::new();
                    put_req.set_id(req.get_id());
                    put_req.set_key(key.to_string());
                    put_req.set_val(val.as_bytes().to_vec());
                    put_req
                        .write_to(&mut output_stream)
                        .wrap_err("Failed to write into CodedOutputStream for PutReq proto")?;
                }
                x => {
                    let mut request_keys: Vec<String> = Vec::with_capacity(x);
                    while let Some((key, _val)) = req.next() {
                        request_keys.push(key);
                    }
                    let mut putm_req = kv_messages::PutMReq::new();
                    putm_req.set_id(req.get_id());
                    let keys = putm_req.mut_keys();
                    for i in 0..x {
                        keys.push(request_keys[i].to_string());
                    }
                    let vals = putm_req.mut_vals();
                    for _i in 0..x {
                        vals.push(req.get_val().as_bytes().to_vec());
                    }
                    putm_req
                        .write_to(&mut output_stream)
                        .wrap_err("Failed to write into CodedOutputStream for PutMReq proto")?;
                }
            },
        }

        // copy from output stream into buf
        output_stream
            .flush()
            .wrap_err("Failed to flush output stream.")?;
        tracing::debug!("Output vec len: {}", output_vec.len());
        if buf.write(output_vec.as_slice())? != output_vec.len() {
            bail!("Failed to copy from output vector into buffer");
        }
        Ok(output_vec.len())
    }

    fn write_next_twitter_request<'a>(
        &self,
        mut buf: &mut [u8],
        req: &mut TwitterRequest<'a>,
    ) -> Result<usize> {
        let mut output_vec = Vec::default();
        let mut output_stream = CodedOutputStream::vec(&mut output_vec);
        match req.get_type() {
            MsgType::Get(size) => match size {
                0 => {
                    bail!("Msg size cannot be 0");
                }
                1 => {
                    let (key, _val) = req.get_next_kv()?;
                    let mut get_req = kv_messages::GetReq::new();
                    get_req.set_id(req.get_id());
                    get_req.set_key(key.to_string());
                    get_req
                        .write_to(&mut output_stream)
                        .wrap_err("Failed to write into CodedOutputStream for GetReq proto")?;
                }
                x => {
                    let mut request_keys: Vec<String> = Vec::with_capacity(x);
                    while let Some((key, _val)) = req.next() {
                        request_keys.push(key);
                    }
                    let mut getm_req = kv_messages::GetMReq::new();
                    getm_req.set_id(req.get_id());
                    let keys = getm_req.mut_keys();
                    for i in 0..x {
                        keys.push(request_keys[i].to_string());
                    }
                    tracing::debug!("Constructed get request with length {}", keys.len());
                    tracing::debug!("Proto's keys length: {}", getm_req.get_keys().len());
                    getm_req
                        .write_to(&mut output_stream)
                        .wrap_err("Failed to write into CodedOutputStream for GetMReq proto")?;
                }
            },
            MsgType::Put(size) => match size {
                0 => {
                    bail!("Msg size cannot be 0");
                }
                1 => {
                    let (key, val) = req.get_next_kv()?;
                    let mut put_req = kv_messages::PutReq::new();
                    put_req.set_id(req.get_id());
                    put_req.set_key(key.to_string());
                    put_req.set_val(val.as_bytes().to_vec());
                    put_req
                        .write_to(&mut output_stream)
                        .wrap_err("Failed to write into CodedOutputStream for PutReq proto")?;
                }
                x => {
                    let mut request_keys: Vec<String> = Vec::with_capacity(x);
                    while let Some((key, _val)) = req.next() {
                        request_keys.push(key);
                    }
                    let mut putm_req = kv_messages::PutMReq::new();
                    putm_req.set_id(req.get_id());
                    let keys = putm_req.mut_keys();
                    for i in 0..x {
                        keys.push(request_keys[i].to_string());
                    }
                    let vals = putm_req.mut_vals();
                    for _i in 0..x {
                        vals.push(req.get_val().as_bytes().to_vec());
                    }
                    putm_req
                        .write_to(&mut output_stream)
                        .wrap_err("Failed to write into CodedOutputStream for PutMReq proto")?;
                }
            },
        }

        // copy from output stream into buf
        output_stream
            .flush()
            .wrap_err("Failed to flush output stream.")?;
        tracing::debug!("Output vec len: {}", output_vec.len());
        if buf.write(output_vec.as_slice())? != output_vec.len() {
            bail!("Failed to copy from output vector into buffer");
        }
        Ok(output_vec.len())
    }

}
