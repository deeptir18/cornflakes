pub mod hardcoded_cf;

pub mod kv_messages {
    include!(concat!(env!("OUT_DIR"), "/kv_cf_dynamic.rs"));
}

use super::{ycsb_parser::YCSBRequest, KVSerializer, MsgType, SerializedRequestGenerator};
use color_eyre::eyre::{bail, ensure, Result, WrapErr};
use cornflakes_codegen::utils::rc_dynamic_hdr::{CFBytes, HeaderRepr};
use cornflakes_libos::{
    dpdk_bindings::rte_memcpy_wrapper as rte_memcpy, CfBuf, Datapath, RcCornPtr, RcCornflake,
    ReceivedPkt, ScatterGather, USING_REF_COUNTING,
};
use hashbrown::HashMap;
use std::marker::PhantomData;

// empty object
pub struct CornflakesDynamicSerializer<D>
where
    D: Datapath,
{
    _marker: PhantomData<D>,
}

impl<D> KVSerializer<D> for CornflakesDynamicSerializer<D>
where
    D: Datapath,
{
    type HeaderCtx = Vec<u8>;

    fn new_server(_serialize_to_native_buffers: bool) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(CornflakesDynamicSerializer {
            _marker: PhantomData,
        })
    }

    fn handle_get<'a>(
        &self,
        mut pkt: ReceivedPkt<D>,
        map: &HashMap<String, Vec<CfBuf<D>>>,
        num_values: usize,
        offset: usize,
    ) -> Result<(Self::HeaderCtx, RcCornflake<'a, D>)> {
        let mut get_request = kv_messages::GetReq::<D>::new();
        get_request.deserialize(&pkt, offset)?;
        let key = get_request.get_key();
        tracing::debug!("Handling get request with {} num_values", num_values);

        let mut get_response = kv_messages::GetResp::<D>::new();
        get_response.set_id(get_request.get_id());
        get_response.init_vals(num_values);
        let values = get_response.get_mut_vals();
        let vals = match map.get(key.to_str()?) {
            Some(v) => v,
            None => {
                bail!("Cannot find values for key in KV store: {:?}", key);
            }
        };
        for i in 0..num_values {
            values.append(CFBytes::new_rc(vals[i].clone()));
        }

        let (header_vec, cf) = get_response.serialize(rte_memcpy)?;
        if unsafe { !USING_REF_COUNTING } {
            pkt.free_inner();
        }
        return Ok((header_vec, cf));
    }

    fn handle_put<'a>(
        &self,
        mut pkt: ReceivedPkt<D>,
        map: &mut HashMap<String, Vec<CfBuf<D>>>,
        num_values: usize,
        offset: usize,
        _connection: &mut D,
    ) -> Result<(Self::HeaderCtx, RcCornflake<'a, D>)> {
        let mut put_request = kv_messages::PutReq::new();
        put_request.deserialize(&pkt, offset)?;
        let key = put_request.get_key();
        let vals = put_request.get_vals();
        let mut list: Vec<CfBuf<D>> = Vec::with_capacity(num_values);
        for i in 0..num_values {
            let val = &vals[i];
            list.push(val.get_inner_rc()?);
        }
        map.insert(key.to_string()?, list);

        let mut response = kv_messages::PutResp::new();
        response.set_id(put_request.get_id());

        let (header_vec, cf) = response.serialize(rte_memcpy)?;
        if unsafe { !USING_REF_COUNTING } {
            pkt.free_inner();
        }
        return Ok((header_vec, cf));
    }

    fn process_header<'a>(
        &self,
        ctx: &'a Self::HeaderCtx,
        cornflake: &mut RcCornflake<'a, D>,
    ) -> Result<()> {
        cornflake.replace(0, RcCornPtr::RawRef(ctx.as_slice()));
        Ok(())
    }
}

impl<D> SerializedRequestGenerator<D> for CornflakesDynamicSerializer<D>
where
    D: Datapath,
{
    fn new_request_generator() -> Self {
        CornflakesDynamicSerializer {
            _marker: PhantomData,
        }
    }

    fn check_recved_msg(
        &self,
        pkt: &ReceivedPkt<D>,
        msg_type: MsgType,
        value_size: usize,
        key: String,
        hashmap: &HashMap<String, Vec<String>>,
        check_value: bool,
    ) -> Result<bool> {
        let id = pkt.get_id();
        match msg_type {
            MsgType::Get(num_values) => {
                let mut get_resp = kv_messages::GetResp::<D>::new();
                get_resp.deserialize(&pkt, 0)?;
                ensure!(
                    get_resp.get_id() == id,
                    format!(
                        "Id in  deserialized message does not match: expected: {}, actual: {}",
                        id,
                        get_resp.get_id()
                    )
                );

                ensure!(
                    get_resp.has_vals(),
                    "Deserialized get resp does not have values"
                );

                let vals = get_resp.get_vals();
                ensure!(vals.len() == num_values, format!("Deserialized get response does not have enough values, expected: {}, actual: {}", num_values, vals.len()));

                for i in 0..num_values {
                    let val = &vals[i];
                    ensure!(
                        val.len() == value_size,
                        format!(
                            "Value {} not correct size, expected: {}, actual: {}",
                            i,
                            value_size,
                            val.len()
                        )
                    );
                    if check_value {
                        let values = match hashmap.get(&key) {
                            Some(v) => v,

                            None => {
                                bail!("Request key {:?} does not have a value in the hashmap", key);
                            }
                        };
                        ensure!(
                            vals[i].to_bytes_vec() == values[i].as_str().as_bytes().to_vec(),
                            "Value is not correct"
                        );
                    }
                }
            }
            MsgType::Put(_) => {
                let mut put_resp = kv_messages::PutResp::new();
                put_resp.deserialize(&pkt, 0)?;
                ensure!(put_resp.has_id(), "Received put response doesn't have id");
                ensure!(
                    put_resp.get_id() == id,
                    format!(
                        "Put resp id doesn't match, expected: {}, actual: {}",
                        id,
                        put_resp.get_id()
                    )
                );
            }
        }
        Ok(true)
    }

    fn write_next_request<'a>(&self, buf: &mut [u8], req: &mut YCSBRequest<'a>) -> Result<usize> {
        match req.get_type() {
            MsgType::Get(_) => {
                let mut get_req = kv_messages::GetReq::<D>::new();
                get_req.set_id(req.get_id());
                let key = req.get_key();
                get_req.set_key(&key);

                // serialize the data into the buffer
                let written = get_req
                    .serialize_into_buffer(buf, rte_memcpy)
                    .wrap_err(format!(
                        "Unable to serialize req # {} into buffer",
                        req.get_id()
                    ))?;
                return Ok(written);
            }
            MsgType::Put(size) => {
                let (key, values) = req.get_kv()?;
                let mut put_req = kv_messages::PutReq::<D>::new();
                put_req.set_id(req.get_id());
                put_req.set_key(&key);
                put_req.init_vals(size);
                let vals = put_req.get_mut_vals();
                for i in 0..size {
                    vals.append(CFBytes::new_raw(values[i].as_str().as_ref()));
                }

                // serialize the data into the buffer
                let written = put_req
                    .serialize_into_buffer(buf, rte_memcpy)
                    .wrap_err(format!(
                        "Unable to serialize req # {} into buffer",
                        req.get_id()
                    ))?;
                return Ok(written);
            }
        }
    }
}
