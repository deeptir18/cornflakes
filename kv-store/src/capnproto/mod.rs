use super::{
    kv_capnp, ycsb_parser::YCSBRequest, twitter_parser::TwitterRequest,
    KVSerializer, MsgType, SerializedRequestGenerator,
    ALIGN_SIZE,
};
use byteorder::{ByteOrder, LittleEndian};
use capnp::message::{
    Allocator, Builder, HeapAllocator, Reader, ReaderOptions, ReaderSegments, SegmentArray,
};
use color_eyre::eyre::{bail, ensure, Result, WrapErr};
use cornflakes_libos::{CfBuf, Datapath, RcCornPtr, RcCornflake, ReceivedPkt, ScatterGather};
use hashbrown::HashMap;
use std::{io::Write, marker::PhantomData};
const FRAMING_ENTRY_SIZE: usize = 8;

pub struct CapnprotoSerializer<D>
where
    D: Datapath,
{
    _marker: PhantomData<D>,
}

fn read_context<D>(recved_msg: &ReceivedPkt<D>, offset: usize) -> Result<Vec<&[u8]>>
where
    D: Datapath,
{
    assert!(recved_msg.data_len() >= FRAMING_ENTRY_SIZE);
    let num_segments = LittleEndian::read_u32(recved_msg.contiguous_slice(offset, 4)?) as usize;
    tracing::debug!(
        num_segments = num_segments,
        total_len = recved_msg.data_len(),
        "read_context"
    );
    assert!(
        (recved_msg.data_len() - offset)
            >= (FRAMING_ENTRY_SIZE + num_segments * FRAMING_ENTRY_SIZE)
    );
    let mut size_so_far = FRAMING_ENTRY_SIZE + num_segments * FRAMING_ENTRY_SIZE;
    let mut segments: Vec<&[u8]> = Vec::default();
    for i in 0..num_segments {
        let cur_idx = offset + FRAMING_ENTRY_SIZE + i * FRAMING_ENTRY_SIZE;
        let data_offset = LittleEndian::read_u32(recved_msg.contiguous_slice(cur_idx, 4)?) as usize;
        let size = LittleEndian::read_u32(recved_msg.contiguous_slice(cur_idx + 4, 4)?) as usize;
        tracing::debug!("Segment {} size: {}", i, size);
        assert!(recved_msg.data_len() >= (size_so_far + size));
        segments.push(recved_msg.contiguous_slice(offset + data_offset, size)?);
        size_so_far += size;
    }
    Ok(segments)
}

// TODO: does advancing the buffer here cause problems?
fn copy_into_buf<T>(mut buf: &mut [u8], framing: &Vec<u8>, builder: &Builder<T>) -> Result<usize>
where
    T: Allocator,
{
    // first, copy the framing
    if buf.write(framing.as_slice())? != framing.len() {
        bail!("Failed to write in framing");
    }

    // second, copy in all the segments
    let mut offset = framing.len();
    let segments = builder.get_segments_for_output();
    for seg in segments.iter() {
        // write updates the location of the buffer
        if buf.write(seg.as_ref())? != seg.len() {
            bail!("Failed to copy in data to buff");
        }
        offset += seg.len();
    }
    Ok(offset)
}

fn fill_in_context<T>(builder: &Builder<T>) -> (Vec<u8>, usize)
where
    T: Allocator,
{
    let segments = builder.get_segments_for_output();
    let mut framing: Vec<u8> = vec![0u8; FRAMING_ENTRY_SIZE * (segments.len() + 1)];
    let mut cur_idx = 0;
    LittleEndian::write_u32(&mut framing[cur_idx..(cur_idx + 4)], segments.len() as u32);
    tracing::debug!("Writing in # segments as {}", segments.len());
    cur_idx += 8;
    let mut cur_offset = (segments.len() + 1) * FRAMING_ENTRY_SIZE;
    for seg in segments.iter() {
        tracing::debug!(
            cur_idx = cur_idx,
            pos = cur_offset,
            len = seg.len(),
            "Segment statistics"
        );
        LittleEndian::write_u32(&mut framing[cur_idx..(cur_idx + 4)], cur_offset as u32);
        cur_idx += 4;
        LittleEndian::write_u32(&mut framing[cur_idx..(cur_idx + 4)], seg.len() as u32);
        cur_idx += 4;
        cur_offset += seg.len();
    }
    (framing, segments.len())
}

impl<D> KVSerializer<D> for CapnprotoSerializer<D>
where
    D: Datapath,
{
    type HeaderCtx = (Vec<u8>, Builder<HeapAllocator>);

    fn new_server(_serialize_to_native_buffers: bool) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(CapnprotoSerializer {
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
        let segment_array_vec = read_context(&pkt, offset)?;
        let segment_array = SegmentArray::new(&segment_array_vec.as_slice());
        let message_reader = Reader::new(segment_array, ReaderOptions::default());
        let mut builder = Builder::new_default();
        match num_values {
            0 => {
                bail!("Number of values cannot be 0");
            }
            1 => {
                let get_request = message_reader
                    .get_root::<kv_capnp::get_req::Reader>()
                    .wrap_err("Failed to deserialize GetReq.")?;
                tracing::debug!("Received get request for key: {:?}", get_request.get_key());
                let key = get_request.get_key()?;
                let value = match map.get(key) {
                    Some(v) => v,
                    None => {
                        bail!("Cannot find value for key in KV store: {:?}", key);
                    }
                };

                // construct response
                let mut response = builder.init_root::<kv_capnp::get_resp::Builder>();
                // TODO: value.as_ref() should directly be able to get the underlying data address
                response.set_val(value.as_ref());
                response.set_id(get_request.get_id());
                let (context, num_segments) = fill_in_context(&builder);

                let cf = RcCornflake::with_capacity(num_segments + 1);
                Ok(((context, builder), cf))
            }
            x => {
                // deserialize multi get request
                let getm_request = message_reader
                    .get_root::<kv_capnp::get_m_req::Reader>()
                    .wrap_err("Failed to deserialize GetMReq.")?;
                let keys = getm_request
                    .get_keys()
                    .wrap_err("Failed to run get_keys on deserialized GetMRequest.")?;

                let mut response = builder.init_root::<kv_capnp::get_m_resp::Builder>();
                response.set_id(getm_request.get_id());
                let mut list = response.init_vals(x as u32);
                for i in 0..x {
                    let key = keys.get(i as u32).wrap_err(format!(
                        "Failed to get entry # {} from deserialized GetMRequest.",
                        i
                    ))?;
                    let val = match map.get(key) {
                        Some(v) => v,
                        None => {
                            bail!("Cannot find value for key in KV store: {:?}", key);
                        }
                    };
                    // TODO: val.as_ref() should directly return the mbuf data addr, not need to
                    // make an extra pointer deref to the CfBuf
                    list.set(i as u32, val.as_ref());
                }
                let (context, num_segments) = fill_in_context(&builder);

                let cf = RcCornflake::with_capacity(num_segments + 1);
                Ok(((context, builder), cf))
            }
        }
    }

    fn handle_put<'a>(
        &self,
        pkt: ReceivedPkt<D>,
        map: &mut HashMap<String, CfBuf<D>>,
        num_values: usize,
        offset: usize,
        connection: &mut D,
    ) -> Result<(Self::HeaderCtx, RcCornflake<'a, D>)> {
        let segment_array_vec = read_context(&pkt, offset)?;
        let segment_array = SegmentArray::new(&segment_array_vec.as_slice());
        let message_reader = Reader::new(segment_array, ReaderOptions::default());
        let mut builder = Builder::new_default();
        match num_values {
            0 => {
                bail!("Cannot have 0 values for a put request.");
            }
            1 => {
                // deserialize put request
                let put_request = message_reader
                    .get_root::<kv_capnp::put_req::Reader>()
                    .wrap_err("Failed to deserialize PutReq.")?;
                let key = put_request.get_key()?;
                let val = put_request.get_val()?;
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
                let mut response = builder.init_root::<kv_capnp::put_resp::Builder>();
                response.set_id(put_request.get_id());
                let (context, num_segments) = fill_in_context(&builder);

                let cf = RcCornflake::with_capacity(num_segments + 1);
                Ok(((context, builder), cf))
            }
            x => {
                let putm_request = message_reader
                    .get_root::<kv_capnp::put_m_req::Reader>()
                    .wrap_err("Failed to deserialize PutMReq.")?;
                let keys = putm_request.get_keys()?;
                let vals = putm_request.get_vals()?;
                for i in 0..x {
                    let key = keys.get(i as u32)?;
                    let val = vals.get(i as u32)?;
                    let mut datapath_buffer = CfBuf::allocate(connection, val.len(), ALIGN_SIZE)
                        .wrap_err(format!(
                            "Failed to allocate CfBuf for put req # {}",
                            putm_request.get_id()
                        ))?;
                    if datapath_buffer
                        .write(val)
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

                let mut response = builder.init_root::<kv_capnp::put_resp::Builder>();
                response.set_id(putm_request.get_id());
                let (context, num_segments) = fill_in_context(&builder);

                let cf = RcCornflake::with_capacity(num_segments + 1);
                Ok(((context, builder), cf))
            }
        }
    }

    fn process_header<'a>(
        &self,
        ctx: &'a Self::HeaderCtx,
        cornflake: &mut RcCornflake<'a, D>,
    ) -> Result<()> {
        cornflake.add_entry(RcCornPtr::RawRef(&ctx.0.as_slice()));
        tracing::debug!(len = ctx.0.as_slice().len(), addr=? &ctx.0.as_slice().as_ptr(), "Entry 1");
        let segments = ctx.1.get_segments_for_output();
        for seg in segments.iter() {
            tracing::debug!(len = seg.len(), addr =? &seg.as_ptr(), "Adding segment");
            cornflake.add_entry(RcCornPtr::RawRef(&seg));
        }
        tracing::debug!(
            data_len = cornflake.data_len(),
            num_segments = cornflake.num_segments(),
            "Sending out cf"
        );
        Ok(())
    }
}

impl<D> SerializedRequestGenerator<D> for CapnprotoSerializer<D>
where
    D: Datapath,
{
    fn new_request_generator() -> Self {
        CapnprotoSerializer {
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
        let segment_array_vec = read_context(&pkt, 0)?;
        let segment_array = SegmentArray::new(&segment_array_vec.as_slice());
        let message_reader = Reader::new(segment_array, ReaderOptions::default());
        let id = pkt.get_id();
        match msg_type {
            MsgType::Get(num_values) => {
                match num_values {
                    0 => {
                        bail!("Msg type cannot have {} values", num_values);
                    }
                    1 => {
                        // deserialize into GetResp
                        let get_resp = message_reader
                            .get_root::<kv_capnp::get_resp::Reader>()
                            .wrap_err("Failed to deserialize GetResp.")?;
                        ensure!(
                        get_resp.get_id() == id,
                        format!("Id in  deserialized message does not match: expected: {}, actual: {}", id, get_resp.get_id())
                    );
                        let val = get_resp
                            .get_val()
                            .wrap_err("Deserialized get response does not have value")?;
                        ensure!(
                            val.len() == value_size,
                        format!("Deserialized value does not have the correct size, expected: {}, actual: {}", value_size, val.len())
                    );
                    }
                    _ => {
                        let getm_resp = message_reader
                            .get_root::<kv_capnp::get_m_resp::Reader>()
                            .wrap_err("Failed to deserialize GetReq.")?;
                        ensure!(
                        getm_resp.get_id() == id,
                        format!("Id in  deserialized message does not match: expected: {}, actual: {}", id, getm_resp.get_id())
                    );
                        let vals = getm_resp
                            .get_vals()
                            .wrap_err("Deserialized get response does not have value list.")?;
                        ensure!(vals.len() as usize == num_values, format!("Deserialized getm response does not have enough values, expected: {}, actual: {}", num_values, vals.len()));

                        for i in 0..num_values {
                            let val = vals.get(i as u32)?;
                            ensure!(
                                val.len() == value_size,
                                format!(
                                    "Value {} not correct size, expected: {}, actual: {}",
                                    i,
                                    value_size,
                                    val.len()
                                )
                            );
                        }
                    }
                }
            }
            MsgType::Put(_) => {
                let put_resp = message_reader
                    .get_root::<kv_capnp::put_resp::Reader>()
                    .wrap_err("Failed to deserialize GetReq.")?;
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
        let mut builder = Builder::new_default();
        match req.get_type() {
            MsgType::Get(size) => match size {
                0 => {
                    bail!("Msg size cannot be 0");
                }
                1 => {
                    let (key, _val) = req.get_next_kv()?;
                    let mut get_req = builder.init_root::<kv_capnp::get_req::Builder>();
                    get_req.set_key(&key);
                    get_req.set_id(req.get_id());
                    let (context, _num_segments) = fill_in_context(&builder);
                    let written = copy_into_buf(buf, &context, &builder)?;
                    return Ok(written);
                }
                x => {
                    let mut request_keys: Vec<String> = Vec::with_capacity(x);
                    while let Some((key, _val)) = req.next() {
                        request_keys.push(key);
                    }

                    let mut getm_req = builder.init_root::<kv_capnp::get_m_req::Builder>();
                    getm_req.set_id(req.get_id());
                    let mut keys = getm_req.init_keys(x as u32);
                    for i in 0..x {
                        keys.set(i as u32, &request_keys[i]);
                    }
                    let (context, _num_segments) = fill_in_context(&builder);
                    let written = copy_into_buf(buf, &context, &builder)?;
                    return Ok(written);
                }
            },
            MsgType::Put(size) => match size {
                0 => {
                    bail!("Msg size cannot be 0");
                }
                1 => {
                    let (key, val) = req.get_next_kv()?;
                    let mut put_req = builder.init_root::<kv_capnp::put_req::Builder>();
                    put_req.set_key(&key);
                    put_req.set_val(&val.as_bytes());
                    put_req.set_id(req.get_id());
                    let (context, _num_segments) = fill_in_context(&builder);
                    let written = copy_into_buf(buf, &context, &builder)?;
                    return Ok(written);
                }
                x => {
                    let mut request_keys: Vec<String> = Vec::with_capacity(x);
                    while let Some((key, _val)) = req.next() {
                        request_keys.push(key);
                    }

                    let mut putm_req = builder.init_root::<kv_capnp::put_m_req::Builder>();
                    putm_req.set_id(req.get_id());
                    {
                        let mut keys = putm_req.reborrow().init_keys(x as u32);
                        for i in 0..x {
                            keys.set(i as u32, &request_keys[i]);
                        }
                    }
                    {
                        let mut vals = putm_req.reborrow().init_vals(x as u32);
                        for i in 0..x {
                            vals.set(i as u32, &req.get_val().as_bytes());
                        }
                    }
                    let (context, _num_segments) = fill_in_context(&builder);
                    let written = copy_into_buf(buf, &context, &builder)?;
                    return Ok(written);
                }
            },
        }
    }

    fn write_next_twitter_request<'a>(&self, buf: &mut [u8], req: &mut TwitterRequest<'a>) -> Result<usize> {
        let mut builder = Builder::new_default();
        match req.get_type() {
            MsgType::Get(size) => match size {
                0 => {
                    bail!("Msg size cannot be 0");
                }
                1 => {
                    let (key, _val) = req.get_next_kv()?;
                    let mut get_req = builder.init_root::<kv_capnp::get_req::Builder>();
                    get_req.set_key(&key);
                    get_req.set_id(req.get_id());
                    let (context, _num_segments) = fill_in_context(&builder);
                    let written = copy_into_buf(buf, &context, &builder)?;
                    return Ok(written);
                }
                x => {
                    let mut request_keys: Vec<String> = Vec::with_capacity(x);
                    while let Some((key, _val)) = req.next() {
                        request_keys.push(key);
                    }

                    let mut getm_req = builder.init_root::<kv_capnp::get_m_req::Builder>();
                    getm_req.set_id(req.get_id());
                    let mut keys = getm_req.init_keys(x as u32);
                    for i in 0..x {
                        keys.set(i as u32, &request_keys[i]);
                    }
                    let (context, _num_segments) = fill_in_context(&builder);
                    let written = copy_into_buf(buf, &context, &builder)?;
                    return Ok(written);
                }
            },
            MsgType::Put(size) => match size {
                0 => {
                    bail!("Msg size cannot be 0");
                }
                1 => {
                    let (key, val) = req.get_next_kv()?;
                    let mut put_req = builder.init_root::<kv_capnp::put_req::Builder>();
                    put_req.set_key(&key);
                    put_req.set_val(&val.as_bytes());
                    put_req.set_id(req.get_id());
                    let (context, _num_segments) = fill_in_context(&builder);
                    let written = copy_into_buf(buf, &context, &builder)?;
                    return Ok(written);
                }
                x => {
                    let mut request_keys: Vec<String> = Vec::with_capacity(x);
                    while let Some((key, _val)) = req.next() {
                        request_keys.push(key);
                    }

                    let mut putm_req = builder.init_root::<kv_capnp::put_m_req::Builder>();
                    putm_req.set_id(req.get_id());
                    {
                        let mut keys = putm_req.reborrow().init_keys(x as u32);
                        for i in 0..x {
                            keys.set(i as u32, &request_keys[i]);
                        }
                    }
                    {
                        let mut vals = putm_req.reborrow().init_vals(x as u32);
                        for i in 0..x {
                            vals.set(i as u32, &req.get_val().as_bytes());
                        }
                    }
                    let (context, _num_segments) = fill_in_context(&builder);
                    let written = copy_into_buf(buf, &context, &builder)?;
                    return Ok(written);
                }
            },
        }
    }

}
