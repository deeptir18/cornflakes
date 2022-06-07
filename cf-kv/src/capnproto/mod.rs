use super::{
    allocate_datapath_buffer, kv_capnp, ClientSerializer, KVServer, ListKVServer, MsgType,
    RequestGenerator, ServerLoadGenerator, REQ_TYPE_SIZE,
};
use bumpalo::Bump;
use byteorder::{ByteOrder, LittleEndian};
use capnp::message::{
    Allocator, Builder, HeapAllocator, Reader, ReaderOptions, ReaderSegments, SegmentArray,
};
use color_eyre::eyre::{bail, ensure, Result, WrapErr};
use cornflakes_libos::{
    allocator::MempoolID,
    datapath::{Datapath, PushBufType, ReceivedPkt},
    dynamic_sga_hdr::SgaHeaderRepr,
    dynamic_sga_hdr::*,
    state_machine::server::ServerSM,
    ArenaOrderedSga, Sge,
};
use hashbrown::HashMap;
#[cfg(feature = "profiler")]
use perftools;
use std::{io::Write, marker::PhantomData};
const FRAMING_ENTRY_SIZE: usize = 8;

fn read_context(buf: &[u8]) -> Result<Vec<&[u8]>> {
    let num_segments = LittleEndian::read_u32(&buf[0..4]) as usize;
    tracing::debug!(
        num_segments = num_segments,
        buf_len = buf.len(),
        "read_context"
    );
    let mut size_so_far = FRAMING_ENTRY_SIZE + num_segments * FRAMING_ENTRY_SIZE;
    let mut segments: Vec<&[u8]> = Vec::default();
    for i in 0..num_segments {
        let cur_idx = FRAMING_ENTRY_SIZE + i * FRAMING_ENTRY_SIZE;
        let data_offset = LittleEndian::read_u32(&buf[cur_idx..(cur_idx + 4)]) as usize;
        let size = LittleEndian::read_u32(&buf[(cur_idx + 4)..cur_idx + 8]) as usize;
        tracing::debug!("Segment {} size: {}", i, size);
        segments.push(&buf[data_offset..(data_offset + size)]);
        size_so_far += size;
    }
    Ok(segments)
}
pub struct CapnprotoSerializer<D>
where
    D: Datapath,
{
    _phantom: PhantomData<D>,
}

impl<D> CapnprotoSerializer<D>
where
    D: Datapath,
{
    pub fn new() -> Self {
        CapnprotoSerializer {
            _phantom: PhantomData::default(),
        }
    }

    fn handle_get<T>(
        &mut self,
        kv_server: &KVServer<D>,
        pkt: &ReceivedPkt<D>,
        builder: &mut Builder<T>,
    ) -> Result<()>
    where
        T: Allocator,
    {
        let segment_array_vec = read_context(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
        let segment_array = SegmentArray::new(&segment_array_vec.as_slice());
        let message_reader = Reader::new(segment_array, ReaderOptions::default());
        let get_request = message_reader
            .get_root::<kv_capnp::get_req::Reader>()
            .wrap_err("Failed to deserialize GetReq.")?;
        tracing::debug!("Received get request for key: {:?}", get_request.get_key());
        let key = get_request.get_key()?;
        let value = match kv_server.get(key) {
            Some(v) => v,
            None => {
                bail!("Cannot find value for key in KV store: {:?}", key);
            }
        };

        // construct response
        let mut response = builder.init_root::<kv_capnp::get_resp::Builder>();
        response.set_val(value.as_ref());
        response.set_id(get_request.get_id());
        Ok(())
    }

    /*fn handle_put<'arena>(
        &self,
        kv_server: &mut KVServer<D>,
        mempool_ids: &mut Vec<MempoolID>,
        pkt: &ReceivedPkt<D>,
        datapath: &mut D,
        arena: &'arena bumpalo::Bump,
    ) -> Result<ArenaOrderedSga<'arena>> {
        let mut put_req = PutReq::new();
        put_req.deserialize(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
        let key = put_req.get_key().to_string();
        // allocate space in kv for value
        let mut datapath_buffer =
            allocate_datapath_buffer(datapath, put_req.get_val().len(), mempool_ids)?;
        let _ = datapath_buffer.write(put_req.get_val().get_ptr())?;
        kv_server.insert(key, datapath_buffer);

        let mut put_resp = PutResp::new();
        put_resp.set_id(put_req.get_id());
        let mut arena_sga =
            ArenaOrderedSga::allocate(put_resp.num_scatter_gather_entries(), &arena);
        put_resp.partially_serialize_into_arena_sga(&mut arena_sga, arena)?;
        Ok(arena_sga)
    }

    fn handle_getm<'kv, 'arena>(
        &self,
        kv_server: &'kv KVServer<D>,
        pkt: &ReceivedPkt<D>,
        _datapath: &D,
        arena: &'arena bumpalo::Bump,
    ) -> Result<ArenaOrderedSga<'arena>>
    where
        'kv: 'arena,
    {
        let mut getm_req = GetMReq::new();
        {
            #[cfg(feature = "profiler")]
            perftools::timer!("Deserialize pkt");
            getm_req.deserialize(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
        }
        let mut getm_resp = GetMResp::new();
        getm_resp.init_vals(getm_req.get_keys().len());
        let vals = getm_resp.get_mut_vals();
        for key in getm_req.get_keys().iter() {
            let value = {
                #[cfg(feature = "profiler")]
                perftools::timer!("got value");
                match kv_server.get(key.as_str()) {
                    Some(v) => v,
                    None => {
                        bail!("Could not find value for key: {:?}", key);
                    }
                }
            };
            tracing::debug!(
                "For given key {:?}, found value {:?} with length {}",
                key.as_str(),
                value.as_ref().as_ptr(),
                value.as_ref().len()
            );
            {
                #[cfg(feature = "profiler")]
                perftools::timer!("append value");
                vals.append(CFBytes::new(value.as_ref()));
            }
        }
        getm_resp.set_id(getm_req.get_id());

        let mut arena_sga = {
            #[cfg(feature = "profiler")]
            perftools::timer!("allocate sga");
            ArenaOrderedSga::allocate(getm_resp.num_scatter_gather_entries(), &arena)
        };
        {
            #[cfg(feature = "profiler")]
            perftools::timer!("serialize sga");
            getm_resp.partially_serialize_into_arena_sga(&mut arena_sga, arena)?;
        }
        Ok(arena_sga)
    }

    fn handle_putm<'arena>(
        &self,
        kv_server: &mut KVServer<D>,
        mempool_ids: &mut Vec<MempoolID>,
        pkt: &ReceivedPkt<D>,
        datapath: &mut D,
        arena: &'arena bumpalo::Bump,
    ) -> Result<ArenaOrderedSga<'arena>> {
        let mut putm_req = PutMReq::new();
        putm_req.deserialize(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
        for (key, value) in putm_req.get_keys().iter().zip(putm_req.get_vals().iter()) {
            // allocate space in kv for value
            let mut datapath_buffer = allocate_datapath_buffer(datapath, value.len(), mempool_ids)?;
            let _ = datapath_buffer.write(value.get_ptr())?;
            kv_server.insert(key.to_string(), datapath_buffer);
        }

        let mut put_resp = PutResp::new();
        put_resp.set_id(putm_req.get_id());
        let mut arena_sga =
            ArenaOrderedSga::allocate(put_resp.num_scatter_gather_entries(), &arena);
        put_resp.partially_serialize_into_arena_sga(&mut arena_sga, arena)?;
        Ok(arena_sga)
    }
    fn handle_getlist<'kv, 'arena>(
        &self,
        list_kv_server: &'kv ListKVServer<D>,
        pkt: &ReceivedPkt<D>,
        _datapath: &D,
        arena: &'arena bumpalo::Bump,
    ) -> Result<ArenaOrderedSga<'arena>>
    where
        'kv: 'arena,
    {
        let mut getlist_req = GetListReq::new();
        getlist_req.deserialize(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
        let value_list = match list_kv_server.get(getlist_req.get_key().as_str()) {
            Some(v) => v,
            None => {
                bail!("Could not find value for key: {:?}", getlist_req.get_key());
            }
        };

        let mut getlist_resp = GetListResp::new();
        getlist_resp.set_id(getlist_req.get_id());
        getlist_resp.init_val_list(value_list.len());
        let list = getlist_resp.get_mut_val_list();
        for value in value_list.iter() {
            list.append(CFBytes::new(value.as_ref()));
        }

        let mut arena_sga =
            ArenaOrderedSga::allocate(getlist_resp.num_scatter_gather_entries(), &arena);
        getlist_resp.partially_serialize_into_arena_sga(&mut arena_sga, arena)?;
        Ok(arena_sga)
    }*/
}

pub struct CapnprotoKVServer<D>
where
    D: Datapath,
{
    kv_server: KVServer<D>,
    list_kv_server: ListKVServer<D>,
    mempool_ids: Vec<MempoolID>,
    serializer: CapnprotoSerializer<D>,
    push_buf_type: PushBufType,
    arena: bumpalo::Bump,
}

impl<D> CapnprotoKVServer<D>
where
    D: Datapath,
{
    pub fn new<L>(
        file: &str,
        load_generator: L,
        datapath: &mut D,
        push_buf_type: PushBufType,
    ) -> Result<Self>
    where
        L: ServerLoadGenerator,
    {
        let (kv, list_kv, mempool_ids) = load_generator.new_kv_state(file, datapath)?;
        Ok(CapnprotoKVServer {
            kv_server: kv,
            list_kv_server: list_kv,
            mempool_ids: mempool_ids,
            push_buf_type: push_buf_type,
            serializer: CapnprotoSerializer::new(),
            arena: bumpalo::Bump::with_capacity(
                ArenaOrderedSga::arena_size(
                    D::batch_size(),
                    D::max_packet_size(),
                    D::max_scatter_gather_entries(),
                ) * 100,
            ),
        })
    }
}

impl<D> ServerSM for CapnprotoKVServer<D>
where
    D: Datapath,
{
    type Datapath = D;
    #[inline]
    fn push_buf_type(&self) -> PushBufType {
        self.push_buf_type
    }

    #[inline]
    fn process_requests_single_buf(
        &mut self,
        sga: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        datapath: &mut Self::Datapath,
    ) -> Result<()> {
        let mut builder = Builder::new_default();
        let pkts_len = sga.len();
        for (i, pkt) in sga.into_iter().enumerate() {
            let message_type = MsgType::from_packet(&pkt)?;
            let framing_vec = match message_type {
                MsgType::Get => self
                    .serializer
                    .handle_get(&self.kv_server, &pkt, &mut builder),
                MsgType::GetM(size) => {
                    unimplemented!();
                }
                MsgType::GetList(size) => {
                    unimplemented!();
                }
                MsgType::Put => {
                    unimplemented!();
                }
                MsgType::PutM(size) => {
                    unimplemented!();
                }
                MsgType::PutList(size) => {
                    unimplemented!();
                }
                MsgType::AppendToList(size) => {
                    unimplemented!();
                }
            }?;

            let mut framing_vec = bumpalo::collections::Vec::with_capacity_zeroed_in(
                FRAMING_ENTRY_SIZE * (1 + builder.get_segments_for_output().len()),
                &self.arena,
            );
            let mut sga =
                ArenaOrderedSga::allocate(builder.get_segments_for_output().len() + 1, &self.arena);
            fill_in_context(&builder, &mut framing_vec);
            sga.add_entry(Sge::new(framing_vec.as_slice()));
            for seg in builder.get_segments_for_output().iter() {
                sga.add_entry(Sge::new(&seg));
            }
            datapath
                .queue_sga_with_copy((pkt.msg_id(), pkt.conn_id(), &sga), i == (pkts_len - 1))?;
        }
        self.arena.reset();
        Ok(())
    }
}

fn fill_in_context_without_arena<T>(builder: &Builder<T>, framing: &mut [u8]) -> Result<usize>
where
    T: Allocator,
{
    let mut cur_idx = 0;
    let segments = builder.get_segments_for_output();
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
    tracing::debug!(
        "Written framing: {:?}",
        &framing[0..(FRAMING_ENTRY_SIZE * (segments.len() + 1))]
    );
    Ok(FRAMING_ENTRY_SIZE * (segments.len() + 1))
}

fn fill_in_context<'arena, T>(
    builder: &Builder<T>,
    framing: &mut bumpalo::collections::Vec<'arena, u8>,
) where
    T: Allocator,
{
    let mut cur_idx = 0;
    let segments = builder.get_segments_for_output();
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
}

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct CapnprotoClient<D>
where
    D: Datapath,
{
    _datapath: PhantomData<D>,
}

impl<D> ClientSerializer<D> for CapnprotoClient<D>
where
    D: Datapath,
{
    fn new() -> Self
    where
        Self: Sized,
    {
        CapnprotoClient {
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
        let segment_array_vec = read_context(buf)?;
        let segment_array = SegmentArray::new(&segment_array_vec.as_slice());
        let message_reader = Reader::new(segment_array, ReaderOptions::default());
        match request_generator.message_type(request)? {
            MsgType::Get => {
                let get_resp = message_reader
                    .get_root::<kv_capnp::get_resp::Reader>()
                    .wrap_err("Failed to deserialize GetResp.")?;
                let val = get_resp
                    .get_val()
                    .wrap_err("deserialized get response does not have value")?;
                request_generator.check_get(request, &val, ref_kv)?;
            }
            MsgType::Put => {}
            MsgType::GetM(_size) => {
                /*let mut getm_resp = GetMResp::new();
                getm_resp.deserialize(buf)?;
                ensure!(
                    getm_resp.has_vals(),
                    "GetM Response does not have value list"
                );
                let vec: Vec<&[u8]> = getm_resp
                    .get_vals()
                    .iter()
                    .map(|cf_bytes| cf_bytes.get_ptr())
                    .collect();
                request_generator.check_getm(request, vec, ref_kv)?;*/
            }
            MsgType::PutM(_size) => {}
            MsgType::GetList(_size) => {
                /*let mut getlist_resp = GetListResp::new();
                getlist_resp.deserialize(buf)?;
                ensure!(
                    getlist_resp.has_val_list(),
                    "Get List Response does not have value list"
                );
                let vec: Vec<&[u8]> = getlist_resp
                    .get_val_list()
                    .iter()
                    .map(|cf_bytes| cf_bytes.get_ptr())
                    .collect();
                request_generator.check_get_list(request, vec, ref_list_kv)?;*/
            }
            MsgType::PutList(_size) => {}
            MsgType::AppendToList(_size) => {}
        }
        Ok(())
    }

    fn serialize_get(&self, buf: &mut [u8], key: &str, datapath: &D) -> Result<usize> {
        let mut builder = Builder::new_default();
        let mut get_req = builder.init_root::<kv_capnp::get_req::Builder>();
        get_req.set_key(&key);
        let framing_size = fill_in_context_without_arena(&builder, buf)?;
        let full_size = copy_into_buf(buf, framing_size, &builder)?;
        tracing::debug!(
            "Full buffer: {:?}, full buffer length: {}",
            &buf[0..full_size],
            full_size
        );
        return Ok(full_size);
    }

    fn serialize_put(&self, buf: &mut [u8], key: &str, value: &str, datapath: &D) -> Result<usize> {
        Ok(0)
    }

    fn serialize_getm(&self, buf: &mut [u8], keys: &Vec<String>, datapath: &D) -> Result<usize> {
        Ok(0)
    }

    fn serialize_putm(
        &self,
        buf: &mut [u8],
        keys: &Vec<String>,
        values: &Vec<String>,
        datapath: &D,
    ) -> Result<usize> {
        Ok(0)
    }

    fn serialize_get_list(&self, buf: &mut [u8], key: &str, datapath: &D) -> Result<usize> {
        Ok(0)
    }

    fn serialize_put_list(
        &self,
        buf: &mut [u8],
        key: &str,
        values: &Vec<String>,
        datapath: &D,
    ) -> Result<usize> {
        Ok(0)
    }

    fn serialize_append(
        &self,
        buf: &mut [u8],
        key: &str,
        value: &str,
        datapath: &D,
    ) -> Result<usize> {
        Ok(0)
    }
}

fn copy_into_buf<T>(buf: &mut [u8], framing_size: usize, builder: &Builder<T>) -> Result<usize>
where
    T: Allocator,
{
    let mut offset = framing_size;
    let segments = builder.get_segments_for_output();
    for seg in segments.iter() {
        // write updates the location of the buffer
        let buf_to_copy = &mut buf[offset..(offset + seg.len())];
        buf_to_copy.copy_from_slice(seg.as_ref());
        offset += seg.len();
    }
    Ok(offset)
}
