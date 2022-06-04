pub mod kv_serializer {
    #![allow(unused_variables)]
    #![allow(non_camel_case_types)]
    #![allow(non_upper_case_globals)]
    #![allow(non_snake_case)]
    include!(concat!(env!("OUT_DIR"), "/kv_sga_cornflakes.rs"));
}
use cornflakes_libos::{
    allocator::MempoolID,
    datapath::{Datapath, PushBufType, ReceivedPkt},
    dynamic_sga_hdr::SgaHeaderRepr,
    dynamic_sga_hdr::*,
    state_machine::server::ServerSM,
    ArenaOrderedSga,
};
use hashbrown::HashMap;
use kv_serializer::*;

use super::{
    allocate_datapath_buffer, ClientSerializer, KVServer, ListKVServer, MsgType, RequestGenerator,
    ServerLoadGenerator, REQ_TYPE_SIZE,
};
use color_eyre::eyre::{bail, ensure, Result};
#[cfg(feature = "profiler")]
use perftools;
use std::{io::Write, marker::PhantomData};

pub struct CornflakesSerializer<D>
where
    D: Datapath,
{
    _phantom: PhantomData<D>,
}

impl<D> CornflakesSerializer<D>
where
    D: Datapath,
{
    pub fn new() -> Self {
        CornflakesSerializer {
            _phantom: PhantomData::default(),
        }
    }

    fn handle_get<'kv, 'arena>(
        &self,
        kv_server: &'kv KVServer<D>,
        pkt: &ReceivedPkt<D>,
        _datapath: &D,
        arena: &'arena bumpalo::Bump,
    ) -> Result<ArenaOrderedSga<'arena>>
    where
        'kv: 'arena,
    {
        let mut get_req = GetReq::new();
        get_req.deserialize(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
        let value = match kv_server.get(get_req.get_key().as_str()) {
            Some(v) => v,
            None => {
                bail!("Could not find value for key: {:?}", get_req.get_key());
            }
        };
        tracing::debug!(
            "For given key {:?}, found value {:?} with length {}",
            get_req.get_key().as_str(),
            value.as_ref().as_ptr(),
            value.as_ref().len()
        );
        let mut get_resp = GetResp::new();
        get_resp.set_id(get_req.get_id());
        get_resp.set_val(CFBytes::new(value.as_ref()));
        let mut arena_sga =
            ArenaOrderedSga::allocate(get_resp.num_scatter_gather_entries(), &arena);
        get_resp.partially_serialize_into_arena_sga(&mut arena_sga, arena)?;
        tracing::debug!("Done serializing response");
        Ok(arena_sga)
    }
    fn handle_put<'arena>(
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
    }

    fn handle_putlist<'arena>(
        &self,
        list_kv_server: &mut ListKVServer<D>,
        mempool_ids: &mut Vec<MempoolID>,
        pkt: &ReceivedPkt<D>,
        datapath: &mut D,
        arena: &'arena bumpalo::Bump,
    ) -> Result<ArenaOrderedSga<'arena>> {
        let mut putlist_req = PutListReq::new();
        putlist_req.deserialize(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;

        let key = putlist_req.get_key();
        let values: Result<Vec<D::DatapathBuffer>> = putlist_req
            .get_vals()
            .iter()
            .map(|value| {
                let mut datapath_buffer =
                    allocate_datapath_buffer(datapath, value.len(), mempool_ids)?;
                let _ = datapath_buffer.write(value.get_ptr())?;
                Ok(datapath_buffer)
            })
            .collect();
        list_kv_server.insert(key.to_string(), values?);

        let mut put_resp = PutResp::new();
        put_resp.set_id(putlist_req.get_id());
        let mut arena_sga =
            ArenaOrderedSga::allocate(put_resp.num_scatter_gather_entries(), &arena);
        put_resp.partially_serialize_into_arena_sga(&mut arena_sga, arena)?;
        Ok(arena_sga)
    }
}

pub struct CornflakesKVServer<D>
where
    D: Datapath,
{
    kv_server: KVServer<D>,
    list_kv_server: ListKVServer<D>,
    mempool_ids: Vec<MempoolID>,
    serializer: CornflakesSerializer<D>,
    push_buf_type: PushBufType,
}

impl<D> CornflakesKVServer<D>
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
        Ok(CornflakesKVServer {
            kv_server: kv,
            list_kv_server: list_kv,
            mempool_ids: mempool_ids,
            push_buf_type: push_buf_type,
            serializer: CornflakesSerializer::new(),
        })
    }
}

impl<D> ServerSM for CornflakesKVServer<D>
where
    D: Datapath,
{
    type Datapath = D;
    #[inline]
    fn push_buf_type(&self) -> PushBufType {
        self.push_buf_type
    }

    #[inline]
    fn process_requests_arena_ordered_sga(
        &mut self,
        sga: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        datapath: &mut Self::Datapath,
        arena: &mut bumpalo::Bump,
    ) -> Result<()> {
        let end = sga.len();
        for (i, pkt) in sga.into_iter().enumerate() {
            tracing::debug!(
                "Received packet with data {:?} and length {}",
                pkt.seg(0).as_ref(),
                pkt.data_len()
            );
            let end_batch = i == (end - 1);
            let msg_type = MsgType::from_packet(&pkt)?;
            match msg_type {
                MsgType::Get => {
                    let sga =
                        self.serializer
                            .handle_get(&self.kv_server, &pkt, &datapath, &arena)?;
                    datapath
                        .queue_arena_ordered_sga((pkt.msg_id(), pkt.conn_id(), sga), end_batch)?;
                }
                MsgType::Put => {
                    let sga = self.serializer.handle_put(
                        &mut self.kv_server,
                        &mut self.mempool_ids,
                        &pkt,
                        datapath,
                        &arena,
                    )?;
                    datapath
                        .queue_arena_ordered_sga((pkt.msg_id(), pkt.conn_id(), sga), end_batch)?;
                }
                MsgType::GetM(_size) => {
                    let sga =
                        self.serializer
                            .handle_getm(&self.kv_server, &pkt, datapath, &arena)?;
                    datapath
                        .queue_arena_ordered_sga((pkt.msg_id(), pkt.conn_id(), sga), end_batch)?;
                }
                MsgType::PutM(_size) => {
                    let sga = self.serializer.handle_putm(
                        &mut self.kv_server,
                        &mut self.mempool_ids,
                        &pkt,
                        datapath,
                        &arena,
                    )?;
                    datapath
                        .queue_arena_ordered_sga((pkt.msg_id(), pkt.conn_id(), sga), end_batch)?;
                }
                MsgType::GetList(_size) => {
                    let sga = self.serializer.handle_getlist(
                        &self.list_kv_server,
                        &pkt,
                        datapath,
                        &arena,
                    )?;
                    datapath
                        .queue_arena_ordered_sga((pkt.msg_id(), pkt.conn_id(), sga), end_batch)?;
                }
                MsgType::PutList(_size) => {
                    let sga = self.serializer.handle_putlist(
                        &mut self.list_kv_server,
                        &mut self.mempool_ids,
                        &pkt,
                        datapath,
                        &arena,
                    )?;
                    datapath
                        .queue_arena_ordered_sga((pkt.msg_id(), pkt.conn_id(), sga), end_batch)?;
                }
                MsgType::AppendToList(_) => {
                    unimplemented!();
                }
            }
        }
        arena.reset();
        Ok(())
    }
}

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
                let mut get_resp = GetResp::new();
                get_resp.deserialize(buf)?;
                ensure!(get_resp.has_val(), "Get Response does not have a value");
                request_generator.check_get(request, get_resp.get_val().get_ptr(), ref_kv)?;
            }
            MsgType::Put => {}
            MsgType::GetM(_size) => {
                let mut getm_resp = GetMResp::new();
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
                request_generator.check_getm(request, vec, ref_kv)?;
            }
            MsgType::PutM(_size) => {}
            MsgType::GetList(_size) => {
                let mut getlist_resp = GetListResp::new();
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
                request_generator.check_get_list(request, vec, ref_list_kv)?;
            }
            MsgType::PutList(_size) => {}
            MsgType::AppendToList(_size) => {}
        }
        Ok(())
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
        let vals = put.get_mut_vals();
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
