pub mod kv_api {
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(dead_code)]
    #![allow(improper_ctypes)]
    #![allow(unused_imports)]
    include!(concat!(env!("OUT_DIR"), "/kv_fb_generated.rs"));
}
use kv_api::kv_fb;

use super::{
    ycsb_parser::YCSBRequest, KVSerializer, MsgType, SerializedRequestGenerator, ALIGN_SIZE,
};
use color_eyre::eyre::{bail, ensure, Result, WrapErr};
use cornflakes_libos::{CfBuf, Datapath, RcCornPtr, RcCornflake, ReceivedPkt};
use flatbuffers::{get_root, FlatBufferBuilder, WIPOffset};
use hashbrown::HashMap;
use std::{io::Write, marker::PhantomData};

// empty object
pub struct FlatBufferSerializer<'fbb, D>
where
    D: Datapath,
{
    _marker: PhantomData<(&'fbb [u8], D)>,
}

impl<'fbb, D> KVSerializer<D> for FlatBufferSerializer<'fbb, D>
where
    D: Datapath,
{
    type HeaderCtx = FlatBufferBuilder<'fbb>;

    fn new_server(_serialie_to_native_buffers: bool) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(FlatBufferSerializer {
            _marker: PhantomData,
        })
    }

    fn handle_get<'a>(
        &self,
        pkt: ReceivedPkt<D>,
        map: &HashMap<String, Vec<CfBuf<D>>>,
        num_values: usize,
        offset: usize,
    ) -> Result<(Self::HeaderCtx, RcCornflake<'a, D>)> {
        let pkt_data = pkt.contiguous_slice(offset, pkt.data_len() - offset)?;
        let get_request = get_root::<kv_fb::GetReq>(&pkt_data);
        let key = match get_request.key() {
            Some(x) => x,
            None => {
                bail!("Key not present in get request");
            }
        };
        let values = match map.get(key) {
            Some(v) => v,
            None => {
                bail!("Cannot find value for key in KV store: {:?}", key);
            }
        };

        let mut builder = FlatBufferBuilder::new();
        let args_vec_res: Result<Vec<kv_fb::ValueArgs>> = (0..num_values)
            .map(|idx| {
                Ok(kv_fb::ValueArgs {
                    data: Some(builder.create_vector_direct::<u8>(values[idx].as_ref())),
                })
            })
            .collect();
        let args_vec: Vec<WIPOffset<kv_fb::Value>> = args_vec_res?
            .iter()
            .map(|args| kv_fb::Value::create(&mut builder, args))
            .collect();
        let get_resp_args = kv_fb::GetRespArgs {
            id: get_request.id(),
            vals: Some(builder.create_vector(args_vec.as_slice())),
        };
        let get_resp = kv_fb::GetResp::create(&mut builder, &get_resp_args);
        builder.finish(get_resp, None);
        Ok((builder, RcCornflake::with_capacity(1)))
    }

    fn handle_put<'a>(
        &self,
        pkt: ReceivedPkt<D>,
        map: &mut HashMap<String, Vec<CfBuf<D>>>,
        num_values: usize,
        offset: usize,
        connection: &mut D,
    ) -> Result<(Self::HeaderCtx, RcCornflake<'a, D>)> {
        let pkt_data = pkt.contiguous_slice(offset, pkt.data_len() - offset)?;
        let put_request = get_root::<kv_fb::PutReq>(&pkt_data);
        let key = match put_request.key() {
            Some(k) => k,
            None => {
                bail!("Key not present in put request.");
            }
        };
        let vals = match put_request.vals() {
            Some(v) => v,
            None => {
                bail!("Value not present in put request.");
            }
        };
        let mut list: Vec<CfBuf<D>> = Vec::default();
        for i in 0..num_values {
            let val = match vals.get(i).data() {
                Some(d) => d,
                None => {
                    bail!(
                        "Val # {} data is none for put req id {}",
                        i,
                        put_request.id()
                    );
                }
            };
            let mut datapath_buffer =
                CfBuf::allocate(connection, val.len(), ALIGN_SIZE).wrap_err(format!(
                    "Failed to allocate CfBuf for put req # {}",
                    put_request.id()
                ))?;
            if datapath_buffer
                .write(val)
                .wrap_err("Failed to write bytes into CfBuf.")?
                != val.len()
            {
                bail!(
                    "Failed to write all of the value bytes into CfBuf. for req {}, key-value # {}",
                    put_request.id(),
                    i
                );
            }
            list.push(datapath_buffer);
        }
        map.insert(key.to_string(), list);
        let mut builder = FlatBufferBuilder::new();
        let args = kv_fb::PutRespArgs {
            id: put_request.id(),
        };

        let put_resp = kv_fb::PutResp::create(&mut builder, &args);
        builder.finish(put_resp, None);
        Ok((builder, RcCornflake::with_capacity(1)))
    }

    fn process_header<'a>(
        &self,
        ctx: &'a Self::HeaderCtx,
        cornflake: &mut RcCornflake<'a, D>,
    ) -> Result<()> {
        cornflake.add_entry(RcCornPtr::RawRef(ctx.finished_data()));
        Ok(())
    }
}

fn copy_into_buf<'fbb>(mut buf: &mut [u8], builder: &FlatBufferBuilder<'fbb>) -> Result<usize> {
    let data = builder.finished_data();
    if buf.write(data)? != data.len() {
        bail!("Failed to copy data into buf");
    }
    Ok(data.len())
}

impl<'fbb, D> SerializedRequestGenerator<D> for FlatBufferSerializer<'fbb, D>
where
    D: Datapath,
{
    fn new_request_generator() -> Self {
        FlatBufferSerializer {
            _marker: PhantomData,
        }
    }

    fn check_recved_msg(
        &self,
        pkt: &ReceivedPkt<D>,
        msg_type: MsgType,
        value_size: usize,
        _keys: String,
        _hashmap: &HashMap<String, Vec<String>>,
        _check_value: bool,
    ) -> Result<bool> {
        let pkt_data = pkt.contiguous_slice(0, pkt.data_len())?;
        let id = pkt.get_id();
        match msg_type {
            MsgType::Get(num_values) => {
                let get_resp = get_root::<kv_fb::GetResp>(&pkt_data);
                ensure!(
                    get_resp.id() == id,
                    format!(
                        "Id in  deserialized message does not match: expected: {}, actual: {}",
                        id,
                        get_resp.id()
                    )
                );
                let vals = match get_resp.vals() {
                    Some(x) => x,
                    None => {
                        bail!("Vals not present in get response");
                    }
                };
                for i in 0..num_values {
                    let val = match vals.get(i).data() {
                        Some(v) => v,
                        None => {
                            bail!(
                                "Val not present in get response for idx # {}, pkt {}",
                                i,
                                id
                            );
                        }
                    };
                    ensure!(
                            val.len() == value_size,
                            format!("Deserialized value does not have the correct size, expected: {}, actual: {}", value_size, val.len())
                            );
                }
            }
            MsgType::Put(_) => {
                let put_resp = get_root::<kv_fb::PutResp>(&pkt_data);
                ensure!(
                    put_resp.id() == id,
                    format!(
                        "Id in  deserialized message does not match: expected: {}, actual: {}",
                        id,
                        put_resp.id()
                    )
                );
            }
        }
        Ok(true)
    }

    fn write_next_request<'a>(&self, buf: &mut [u8], req: &mut YCSBRequest<'a>) -> Result<usize> {
        // TODO: is it more efficient to allocate with some capacity?
        let mut builder = FlatBufferBuilder::new();
        match req.get_type() {
            MsgType::Get(_size) => {
                let key = req.get_key();
                let args = kv_fb::GetReqArgs {
                    id: req.get_id(),
                    key: Some(builder.create_string(key.as_ref())),
                };
                let get_req = kv_fb::GetReq::create(&mut builder, &args);
                builder.finish(get_req, None);
            }
            MsgType::Put(size) => {
                let (key, values) = req.get_kv()?;
                let val_vec_data: Vec<kv_fb::ValueArgs> = (0..size)
                    .map(|i| kv_fb::ValueArgs {
                        data: Some(
                            builder.create_vector_direct::<u8>(&values[i].as_str().as_bytes()),
                        ),
                    })
                    .collect();
                let val_vec: Vec<WIPOffset<kv_fb::Value>> = val_vec_data
                    .iter()
                    .map(|args| kv_fb::Value::create(&mut builder, args))
                    .collect();
                let put_req_args = kv_fb::PutReqArgs {
                    id: req.get_id(),
                    key: Some(builder.create_string(key.as_ref())),
                    vals: Some(builder.create_vector(val_vec.as_slice())),
                };
                let put_req = kv_fb::PutReq::create(&mut builder, &put_req_args);
                builder.finish(put_req, None);
            }
        }
        copy_into_buf(buf, &builder)
    }
}
