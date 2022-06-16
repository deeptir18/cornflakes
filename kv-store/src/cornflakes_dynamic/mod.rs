pub mod hardcoded_cf;

pub mod kv_messages {
    include!(concat!(env!("OUT_DIR"), "/kv_cf_dynamic.rs"));
}

use cornflakes_libos::allocator::MempoolID;
use super::{ycsb_parser::YCSBRequest, KVSerializer, MsgType, SerializedRequestGenerator};
use color_eyre::eyre::{bail, ensure, Result, WrapErr};
use cornflakes_codegen::utils::rc_dynamic_hdr::{CFBytes, CFString, HeaderRepr};
use cornflakes_libos::{
    CfBuf, Datapath, RcCornPtr, RcCornflake, ReceivedPkt, ScatterGather, USING_REF_COUNTING,
};
use dpdk_datapath::dpdk_bindings::rte_memcpy_wrapper as rte_memcpy;
use hashbrown::HashMap;
use std::marker::PhantomData;

#[cfg(feature = "profiler")]
use perftools;

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
        &mut self,
        mut pkt: ReceivedPkt<D>,
        map: &HashMap<String, CfBuf<D>>,
        num_values: usize,
        offset: usize,
    ) -> Result<(Self::HeaderCtx, RcCornflake<'a, D>, MempoolID)> {
        #[cfg(feature = "profiler")]
        perftools::timer!("Handle get cornflakes");

        match num_values {
            0 => {
                bail!("Number of get values cannot be 0");
            }
            1 => {
                // deserialize request
                let mut get_request = kv_messages::GetReq::<D>::new();
                {
                    #[cfg(feature = "profiler")]
                    perftools::timer!("Deserialize pkt");
                    get_request.deserialize(&pkt, offset)?;
                }
                tracing::debug!(
                    "Received get request for key: {:?}",
                    get_request.get_key().to_str()?
                );
                let key = get_request.get_key();
                let value = {
                    let key_str = {
                        #[cfg(feature = "profiler")]
                        perftools::timer!("re-encode str");
                        key.to_str()?
                    };
                    {
                        #[cfg(feature = "profiler")]
                        perftools::timer!("Query key");
                        match map.get(key_str) {
                            Some(v) => v,
                            None => {
                                bail!("Cannot find value for key in KV store: {:?}", key);
                            }
                        }
                    }
                };
                tracing::info!("Found val for key {:?}: value {:?}", key.to_str()?, value);

                // construct response
                let mut response = kv_messages::GetResp::<D>::new();
                response.set_id(get_request.get_id());
                {
                    #[cfg(feature = "profiler")]
                    perftools::timer!("Set val rc");
                    response.set_val_rc(value.clone());
                }

                // serialize response
                let (header_vec, cf) = {
                    #[cfg(feature = "profiler")]
                    perftools::timer!("Serialize resp");
                    response.serialize(rte_memcpy)?
                };
                return Ok((header_vec, cf, value.get_mempool_id()));
            }
            x => {
                let mut getm_request = kv_messages::GetMReq::<D>::new();
                {
                    #[cfg(feature = "profiler")]
                    perftools::timer!("Deserialize pkt");
                    getm_request.deserialize(&pkt, offset)?;
                }
                let keys = getm_request.get_keys();
                tracing::debug!("Handling getm request with {} values", x);

                let mut getm_response = kv_messages::GetMResp::<D>::new();
                getm_response.set_id(getm_request.get_id());
                getm_response.init_vals(num_values);
                let values = getm_response.get_mut_vals();
                for i in 0..x {
                    let key = &keys[i];
                    let value = {
                        let key_str = {
                            #[cfg(feature = "profiler")]
                            perftools::timer!("re-encode str");
                            key.to_str()?
                        };
                        {
                            #[cfg(feature = "profiler")]
                            perftools::timer!("Query key 1");
                            match map.get(key_str) {
                                Some(v) => v,
                                None => {
                                    bail!("Cannot find value for key in KV store: {:?}", key);
                                }
                            }
                        }
                    };
                    {
                        #[cfg(feature = "profiler")]
                        perftools::timer!("Set val rc");
                        values.append(CFBytes::new_rc(value.clone()));
                    }
                }

                let (header_vec, cf) = {
                    #[cfg(feature = "profiler")]
                    perftools::timer!("Serialize resp");
                    getm_response.serialize(rte_memcpy)?
                };
                if unsafe { !USING_REF_COUNTING } {
                    pkt.free_inner();
                }
                return Ok((header_vec, cf, 0)); // TODO
            }
        }
    }

    fn handle_put<'a>(
        &self,
        mut pkt: ReceivedPkt<D>,
        map: &mut HashMap<String, CfBuf<D>>,
        num_values: usize,
        offset: usize,
        _connection: &mut D,
    ) -> Result<(Self::HeaderCtx, RcCornflake<'a, D>, MempoolID)> {
        match num_values {
            0 => {
                bail!("Cannot have 0 values for a put request.");
            }
            1 => {
                // deserialize request
                let mut put_request = kv_messages::PutReq::<D>::new();
                put_request.deserialize(&pkt, offset)?;
                tracing::debug!(
                    "Put request key: {:?}; val: {:?}",
                    put_request.get_key().to_str()?,
                    put_request.get_val()
                );
                map.insert(
                    put_request.get_key().to_string()?,
                    put_request.get_val().get_inner_rc()?,
                );

                // construct response
                let mut response = kv_messages::PutResp::new();
                response.set_id(put_request.get_id());

                // serialize response
                let (header_vec, cf) = response.serialize(rte_memcpy)?;
                return Ok((header_vec, cf, 0));
            }
            x => {
                let mut putm_request = kv_messages::PutMReq::new();
                putm_request.deserialize(&pkt, offset)?;
                let keys = putm_request.get_keys();
                let vals = putm_request.get_vals();
                for i in 0..x {
                    let key = &keys[i];
                    let val = &vals[i];
                    map.insert(key.to_string()?, val.get_inner_rc()?);
                }

                let mut response = kv_messages::PutResp::new();
                response.set_id(putm_request.get_id());

                let (header_vec, cf) = response.serialize(rte_memcpy)?;
                if unsafe { !USING_REF_COUNTING } {
                    pkt.free_inner();
                }
                return Ok((header_vec, cf, 0));
            }
        }
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
        keys: Vec<String>,
        hashmap: &HashMap<String, String>,
        check_value: bool,
    ) -> Result<bool> {
        let id = pkt.get_id();
        let values = match check_value {
            true => {
                let mut vals: Vec<String> = Vec::default();
                for key in keys.iter() {
                    match hashmap.get(key) {
                        Some(v) => {
                            vals.push(v.to_string());
                        }

                        None => {
                            bail!("Request key {:?} does not have a value in the hashmap", key);
                        }
                    }
                }
                vals
            }
            false => Vec::default(),
        };
        match msg_type {
            MsgType::Get(num_values) => {
                match num_values {
                    0 => {
                        bail!("Msg type cannot have {} values", num_values);
                    }
                    1 => {
                        // deserialize into GetResp
                        let mut get_resp = kv_messages::GetResp::<D>::new();
                        get_resp.deserialize(&pkt, 0)?;
                        ensure!(
                        get_resp.get_id() == id,
                        format!("Id in  deserialized message does not match: expected: {}, actual: {}", id, get_resp.get_id())
                    );
                        ensure!(
                            get_resp.has_val(),
                            "Deserialized get response does not have value"
                        );
                        ensure!(
                        get_resp.get_val().len() == value_size,
                        format!("Deserialized value does not have the correct size, expected: {}, actual: {}", value_size, get_resp.get_val().len())
                    );
                        if check_value {
                            ensure!(
                                get_resp.get_val().to_bytes_vec()
                                    == values[0].as_str().as_bytes().to_vec(),
                                "Value is not correct"
                            );
                        }
                    }
                    _ => {
                        let mut getm_resp = kv_messages::GetMResp::<D>::new();
                        getm_resp.deserialize(&pkt, 0)?;
                        ensure!(
                            getm_resp.get_id() == id,
                        format!("Id in  deserialized message does not match: expected: {}, actual: {}", id, getm_resp.get_id())
                        );

                        ensure!(
                            getm_resp.has_vals(),
                            "Deserialized getm resp does not have values"
                        );

                        let vals = getm_resp.get_vals();
                        ensure!(vals.len() == num_values, format!("Deserialized getm response does not have enough values, expected: {}, actual: {}", num_values, vals.len()));

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
                                ensure!(
                                    vals[i].to_bytes_vec()
                                        == values[i].as_str().as_bytes().to_vec(),
                                    "Value is not correct"
                                );
                            }
                        }
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
            MsgType::Get(size) => match size {
                0 => {
                    bail!("Msg size cannot be 0");
                }
                1 => {
                    let mut get_req = kv_messages::GetReq::<D>::new();
                    get_req.set_id(req.get_id());
                    let (key, _val) = req.get_next_kv()?;
                    get_req.set_key(&key);

                    // serialize the data into the buffer
                    let written =
                        get_req
                            .serialize_into_buffer(buf, rte_memcpy)
                            .wrap_err(format!(
                                "Unable to serialize req # {} into buffer",
                                req.get_id()
                            ))?;
                    return Ok(written);
                }
                x => {
                    let mut request_keys: Vec<String> = Vec::with_capacity(x);
                    while let Some((key, _val)) = req.next() {
                        request_keys.push(key);
                    }

                    let mut getm_req = kv_messages::GetMReq::<D>::new();
                    getm_req.set_id(req.get_id());
                    getm_req.init_keys(x);
                    let keys = getm_req.get_mut_keys();
                    for i in 0..x {
                        keys.append(CFString::new(&request_keys[i]));
                    }

                    // serialize the data into the buffer
                    let written =
                        getm_req
                            .serialize_into_buffer(buf, rte_memcpy)
                            .wrap_err(format!(
                                "Unable to serialize req # {} into buffer",
                                req.get_id()
                            ))?;
                    return Ok(written);
                }
            },
            MsgType::Put(size) => match size {
                0 => {
                    bail!("Msg size cannot be 0");
                }
                1 => {
                    let mut put_req = kv_messages::PutReq::<D>::new();
                    put_req.set_id(req.get_id());
                    let (key, val) = req.get_next_kv()?;
                    put_req.set_key(&key);
                    put_req.set_val(val.as_bytes());
                    // serialize the data into the buffer
                    let written =
                        put_req
                            .serialize_into_buffer(buf, rte_memcpy)
                            .wrap_err(format!(
                                "Unable to serialize req # {} into buffer",
                                req.get_id()
                            ))?;
                    return Ok(written);
                }
                x => {
                    let mut request_keys: Vec<String> = Vec::with_capacity(x);
                    while let Some((key, _val)) = req.next() {
                        request_keys.push(key);
                    }

                    let mut putm_req = kv_messages::PutMReq::<D>::new();
                    putm_req.set_id(req.get_id());
                    putm_req.init_keys(x);
                    putm_req.init_vals(x);
                    let keys = putm_req.get_mut_keys();
                    for i in 0..x {
                        keys.append(CFString::new(&request_keys[i]));
                    }

                    let vals = putm_req.get_mut_vals();
                    for _i in 0..x {
                        vals.append(CFBytes::new_raw(req.get_val().as_bytes()));
                    }

                    // serialize the data into the buffer
                    let written =
                        putm_req
                            .serialize_into_buffer(buf, rte_memcpy)
                            .wrap_err(format!(
                                "Unable to serialize req # {} into buffer",
                                req.get_id()
                            ))?;
                    return Ok(written);
                }
            },
        }
    }
}
