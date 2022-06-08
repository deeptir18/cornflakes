pub mod kv_messages {
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(dead_code)]
    #![allow(improper_ctypes)]
    #![allow(unused_imports)]
    include!(concat!(env!("OUT_DIR"), "/kv_protobuf.rs"));
}

use super::{
    allocate_datapath_buffer, ClientSerializer, KVServer, ListKVServer, MsgType, RequestGenerator,
    ServerLoadGenerator, REQ_TYPE_SIZE,
};
use bumpalo::Bump;
use color_eyre::eyre::{bail, ensure, Result, WrapErr};
use cornflakes_libos::{
    allocator::MempoolID,
    datapath::{Datapath, PushBufType, ReceivedPkt},
    dynamic_sga_hdr::SgaHeaderRepr,
    dynamic_sga_hdr::*,
    state_machine::server::ServerSM,
    ArenaOrderedSga,
};
use hashbrown::HashMap;
#[cfg(feature = "profiler")]
use perftools;
use protobuf::{CodedOutputStream, Message};
use std::{io::Write, marker::PhantomData};

pub struct ProtobufSerializer<D>
where
    D: Datapath,
{
    _phantom: PhantomData<D>,
}

impl<D> ProtobufSerializer<D>
where
    D: Datapath,
{
    pub fn new() -> Self {
        ProtobufSerializer {
            _phantom: PhantomData::default(),
        }
    }

    fn handle_get(
        &self,
        kv_server: &KVServer<D>,
        pkt: &ReceivedPkt<D>,
    ) -> Result<kv_messages::GetResp> {
        let get_request =
            kv_messages::GetReq::parse_from_bytes(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])
                .wrap_err("Failed to deserialize proto GetReq")?;
        let value = match kv_server.get(&get_request.key) {
            Some(v) => v,
            None => {
                bail!(
                    "Cannot find value for key in KV store: {:?}",
                    get_request.key
                );
            }
        };
        let mut get_resp = kv_messages::GetResp::new();
        get_resp.val = value.as_ref().to_vec();
        Ok(get_resp)
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

pub struct ProtobufKVServer<D>
where
    D: Datapath,
{
    kv_server: KVServer<D>,
    list_kv_server: ListKVServer<D>,
    mempool_ids: Vec<MempoolID>,
    serializer: ProtobufSerializer<D>,
    push_buf_type: PushBufType,
    reusable_vec: Vec<u8>,
    arena: bumpalo::Bump,
}

impl<D> ProtobufKVServer<D>
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
        Ok(ProtobufKVServer {
            kv_server: kv,
            list_kv_server: list_kv,
            mempool_ids: mempool_ids,
            push_buf_type: push_buf_type,
            serializer: ProtobufSerializer::new(),
            reusable_vec: Vec::with_capacity(D::max_packet_size()),
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

impl<D> ServerSM for ProtobufKVServer<D>
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
        let pkts_len = sga.len();
        for (i, pkt) in sga.into_iter().enumerate() {
            self.reusable_vec.clear();
            let message_type = MsgType::from_packet(&pkt)?;
            let response_size = match message_type {
                MsgType::Get => {
                    let response = self.serializer.handle_get(&self.kv_server, &pkt)?;
                    response.write_to_vec(&mut self.reusable_vec)?;
                    response.compute_size() as usize
                }
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
            };
            datapath.queue_single_buffer_with_copy(
                (pkt.msg_id(), pkt.conn_id(), &self.reusable_vec.as_slice()),
                i == (pkts_len - 1),
            )?;
        }
        self.arena.reset();
        Ok(())
    }
}

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct ProtobufClient<D>
where
    D: Datapath,
{
    _datapath: PhantomData<D>,
}

impl<D> ClientSerializer<D> for ProtobufClient<D>
where
    D: Datapath,
{
    fn new() -> Self
    where
        Self: Sized,
    {
        ProtobufClient {
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
                let get_resp = kv_messages::GetResp::parse_from_bytes(buf)
                    .wrap_err("Could not parse get_resp from message")?;
                request_generator.check_get(request, &get_resp.val.as_slice(), ref_kv)?;
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
        let mut output_stream = CodedOutputStream::bytes(buf);
        let mut get_req = kv_messages::GetReq::new();
        get_req.key = key.to_string();
        get_req
            .write_to(&mut output_stream)
            .wrap_err("Failed to write into CodedOutputStream for GetReq proto")?;
        output_stream
            .flush()
            .wrap_err("Failed to flush output stream.")?;

        Ok(output_stream.total_bytes_written() as _)
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
