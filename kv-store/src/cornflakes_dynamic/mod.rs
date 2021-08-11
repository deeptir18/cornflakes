pub mod hardcoded_cf;

/*pub mod kv_messages {
    include!(concat!(env!("OUT_DIR"), "/kv_cf_dynamic.rs"));
}*/

use super::{ycsb_parser::YCSBRequest, KVSerializer, MsgType, SerializedRequestGenerator};
use color_eyre::eyre::{bail, Result, WrapErr};
use cornflakes_codegen::utils::rc_dynamic_hdr::{CFBytes, CFString, HeaderRepr};
use cornflakes_libos::{
    dpdk_bindings::rte_memcpy_wrapper as rte_memcpy, CfBuf, Datapath, RcCornPtr, RcCornflake,
    ReceivedPkt, ScatterGather,
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

    fn new(_serialize_to_native_buffers: bool) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(CornflakesDynamicSerializer {
            _marker: PhantomData,
        })
    }

    fn handle_get<'a>(
        &self,
        pkt: ReceivedPkt<D>,
        map: &HashMap<String, CfBuf<D>>,
        num_values: usize,
    ) -> Result<(Self::HeaderCtx, RcCornflake<'a, D>)> {
        match num_values {
            0 => {
                bail!("Number of get values cannot be 0");
            }
            1 => {
                // deserialize request
                let mut get_request = hardcoded_cf::GetReq::<D>::new();
                get_request.deserialize(&pkt)?;
                let key = get_request.get_key();
                let value = match map.get(key.to_str()?) {
                    Some(v) => v,
                    None => {
                        bail!("Cannot find value for key in KV store: {:?}", key);
                    }
                };

                // construct response
                let mut response = hardcoded_cf::GetResp::<D>::new();
                response.set_id(get_request.get_id());
                response.set_val_rc(value.clone());

                // serialize response
                let (header_vec, cf) = response.serialize(rte_memcpy)?;
                return Ok((header_vec, cf));
            }
            x => {
                let mut getm_request = hardcoded_cf::GetMReq::<D>::new();
                getm_request.deserialize(&pkt)?;
                let keys = getm_request.get_keys();

                let mut getm_response = hardcoded_cf::GetMResp::<D>::new();
                getm_response.init_vals(x);
                let values = getm_response.get_mut_vals();
                for i in 0..x {
                    let key = &keys[i];
                    let value = match map.get(key.to_str()?) {
                        Some(v) => v,
                        None => {
                            bail!("Cannot find value for key in KV store: {:?}", key);
                        }
                    };
                    values.append(CFBytes::new_rc(value.clone()));
                }

                let (header_vec, cf) = getm_response.serialize(rte_memcpy)?;
                return Ok((header_vec, cf));
            }
        }
    }

    fn handle_put<'a>(
        &self,
        pkt: ReceivedPkt<D>,
        map: &mut HashMap<String, CfBuf<D>>,
        num_values: usize,
    ) -> Result<(Self::HeaderCtx, RcCornflake<'a, D>)> {
        match num_values {
            0 => {
                bail!("Cannot have 0 values for a put request.");
            }
            1 => {
                // deserialize request
                let mut put_request = hardcoded_cf::PutReq::<D>::new();
                put_request.deserialize(&pkt)?;
                map.insert(
                    put_request.get_key().to_string()?,
                    put_request.get_val().get_inner_rc()?,
                );

                // construct response
                let mut response = hardcoded_cf::PutResp::<D>::new();
                response.set_id(put_request.get_id());

                // serialize response
                let (header_vec, cf) = response.serialize(rte_memcpy)?;
                return Ok((header_vec, cf));
            }
            x => {
                let mut putm_request = hardcoded_cf::PutMReq::<D>::new();
                putm_request.deserialize(&pkt)?;
                let keys = putm_request.get_keys();
                let vals = putm_request.get_vals();
                for i in 0..x {
                    let key = &keys[i];
                    let val = &vals[i];
                    map.insert(key.to_string()?, val.get_inner_rc()?);
                }

                let mut response = hardcoded_cf::PutResp::<D>::new();
                response.set_id(putm_request.get_id());

                let (header_vec, cf) = response.serialize(rte_memcpy)?;
                return Ok((header_vec, cf));
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

impl<D> SerializedRequestGenerator for CornflakesDynamicSerializer<D>
where
    D: Datapath,
{
    fn new() -> Self {
        CornflakesDynamicSerializer {
            _marker: PhantomData,
        }
    }

    fn write_next_request<'a>(&self, buf: &mut [u8], req: &mut YCSBRequest<'a>) -> Result<usize> {
        match req.get_type() {
            MsgType::Get(size) => match size {
                0 => {
                    bail!("Msg size cannot be 0");
                }
                1 => {
                    let mut get_req = hardcoded_cf::GetReq::<D>::new();
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

                    let mut getm_req = hardcoded_cf::GetMReq::<D>::new();
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
                    let mut put_req = hardcoded_cf::PutReq::<D>::new();
                    put_req.set_id(req.get_id());
                    let (key, val) = req.get_next_kv()?;
                    put_req.set_key(&key);
                    put_req.set_val(val.as_bytes());
                }
                x => {
                    let mut request_keys: Vec<String> = Vec::with_capacity(x);
                    while let Some((key, _val)) = req.next() {
                        request_keys.push(key);
                    }

                    let mut putm_req = hardcoded_cf::PutMReq::<D>::new();
                    putm_req.set_id(req.get_id());
                    putm_req.init_keys(x);
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
        Ok(0)
    }
}
