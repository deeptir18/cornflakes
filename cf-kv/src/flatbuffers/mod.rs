pub mod kv_api {
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(dead_code)]
    #![allow(improper_ctypes)]
    #![allow(unused_imports)]
    include!(concat!(env!("OUT_DIR"), "/cf_kv_fb_generated.rs"));
}
use super::{
    allocate_datapath_buffer, ClientSerializer, KVServer, ListKVServer, MsgType, RequestGenerator,
    ServerLoadGenerator, REQ_TYPE_SIZE,
};
use color_eyre::eyre::{bail, ensure, Result};
use cornflakes_libos::{
    allocator::MempoolID,
    datapath::{Datapath, PushBufType, ReceivedPkt},
    dynamic_sga_hdr::SgaHeaderRepr,
    dynamic_sga_hdr::*,
    state_machine::server::ServerSM,
};
use flatbuffers::{get_root, FlatBufferBuilder, WIPOffset};
use hashbrown::HashMap;
use kv_api::cf_kv_fbs;
#[cfg(feature = "profiler")]
use perftools;
use std::{io::Write, marker::PhantomData};

pub struct FlatbuffersSerializer<D>
where
    D: Datapath,
{
    _phantom: PhantomData<D>,
}

impl<D> FlatbuffersSerializer<D>
where
    D: Datapath,
{
    pub fn new() -> Self {
        FlatbuffersSerializer {
            _phantom: PhantomData::default(),
        }
    }

    fn handle_get(
        &self,
        kv_server: &KVServer<D>,
        pkt: &ReceivedPkt<D>,
        builder: &mut FlatBufferBuilder,
    ) -> Result<()> {
        let get_request = get_root::<cf_kv_fbs::GetReq>(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..]);
        let value = match kv_server.get(get_request.key().unwrap()) {
            Some(v) => v,
            None => {
                bail!("Could not find value for key: {:?}", get_request.key());
            }
        };

        tracing::debug!(
            "For given key {:?}, found value {:?} with length {}",
            get_request.key().unwrap(),
            value.as_ref().as_ptr(),
            value.as_ref().len()
        );
        let args = cf_kv_fbs::GetRespArgs {
            val: Some(builder.create_vector_direct::<u8>(value.as_ref())),
            id: get_request.id(),
        };

        let get_resp = cf_kv_fbs::GetResp::create(builder, &args);
        builder.finish(get_resp, None);
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

pub struct FlatbuffersKVServer<D>
where
    D: Datapath,
{
    kv_server: KVServer<D>,
    list_kv_server: ListKVServer<D>,
    mempool_ids: Vec<MempoolID>,
    serializer: FlatbuffersSerializer<D>,
    push_buf_type: PushBufType,
}

impl<D> FlatbuffersKVServer<D>
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
        Ok(FlatbuffersKVServer {
            kv_server: kv,
            list_kv_server: list_kv,
            mempool_ids: mempool_ids,
            push_buf_type: push_buf_type,
            serializer: FlatbuffersSerializer::new(),
        })
    }
}

impl<D> ServerSM for FlatbuffersKVServer<D>
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
        let mut builder = FlatBufferBuilder::new();
        let pkts_len = sga.len();
        for (i, pkt) in sga.into_iter().enumerate() {
            builder.reset();
            let message_type = MsgType::from_packet(&pkt)?;
            match message_type {
                MsgType::Get => {
                    self.serializer
                        .handle_get(&self.kv_server, &pkt, &mut builder)?;
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
            }
            datapath.queue_single_buffer_with_copy(
                (pkt.msg_id(), pkt.conn_id(), &builder.finished_data()),
                i == (pkts_len - 1),
            )?;
        }
        Ok(())
    }
}

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct FlatbuffersClient<D>
where
    D: Datapath,
{
    _datapath: PhantomData<D>,
}

impl<D> ClientSerializer<D> for FlatbuffersClient<D>
where
    D: Datapath,
{
    fn new() -> Self
    where
        Self: Sized,
    {
        FlatbuffersClient {
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
                let get_resp = get_root::<cf_kv_fbs::GetResp>(buf);
                let val = match get_resp.val() {
                    Some(x) => x,
                    None => {
                        bail!("Key not present in get response");
                    }
                };
                request_generator.check_get(request, val, ref_kv)?;
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
        let mut builder = FlatBufferBuilder::new();
        let args = cf_kv_fbs::GetReqArgs {
            // TODO: actually add in ID
            id: 0,
            key: Some(builder.create_string(key.as_ref())),
        };
        let get_req = cf_kv_fbs::GetReq::create(&mut builder, &args);
        builder.finish(get_req, None);
        Ok(copy_into_buf(buf, &builder))
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

fn copy_into_buf<'fbb>(buf: &mut [u8], builder: &FlatBufferBuilder<'fbb>) -> usize {
    let data = builder.finished_data();
    let mut buf_to_copy = &mut buf[0..data.len()];
    buf_to_copy.copy_from_slice(data);
    data.len()
}
