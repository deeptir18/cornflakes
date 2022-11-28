use super::{
    kv_capnp, ClientSerializer, KVServer, LinkedListKVServer, ListKVServer, MsgType,
    ServerLoadGenerator, REQ_TYPE_SIZE,
};
use byteorder::{ByteOrder, LittleEndian};
use capnp::message::{Allocator, Builder, Reader, ReaderOptions, ReaderSegments, SegmentArray};
use color_eyre::eyre::{bail, ensure, Result, WrapErr};
use cornflakes_libos::{
    allocator::MempoolID,
    datapath::{Datapath, PushBufType, ReceivedPkt},
    state_machine::server::ServerSM,
    ArenaOrderedSga, Sge,
};
use std::marker::PhantomData;
const FRAMING_ENTRY_SIZE: usize = 8;

fn read_context(buf: &[u8]) -> Result<Vec<&[u8]>> {
    let num_segments = LittleEndian::read_u32(&buf[0..4]) as usize;
    tracing::debug!(
        num_segments = num_segments,
        buf_len = buf.len(),
        "read_context"
    );
    let mut segments: Vec<&[u8]> = Vec::default();
    for i in 0..num_segments {
        let cur_idx = FRAMING_ENTRY_SIZE + i * FRAMING_ENTRY_SIZE;
        let data_offset = LittleEndian::read_u32(&buf[cur_idx..(cur_idx + 4)]) as usize;
        let size = LittleEndian::read_u32(&buf[(cur_idx + 4)..cur_idx + 8]) as usize;
        tracing::debug!("Segment {} size: {}", i, size);
        segments.push(&buf[data_offset..(data_offset + size)]);
    }
    Ok(segments)
}
pub struct CapnprotoSerializer<D>
where
    D: Datapath,
{
    _phantom: PhantomData<D>,
    use_linked_list_kv_server: bool,
}

impl<D> CapnprotoSerializer<D>
where
    D: Datapath,
{
    pub fn new(use_list_kv_server: bool) -> Self {
        CapnprotoSerializer {
            _phantom: PhantomData::default(),
            use_linked_list_kv_server: use_list_kv_server,
        }
    }

    pub fn use_linked_list(&self) -> bool {
        self.use_linked_list_kv_server
    }

    fn handle_get<T>(
        &mut self,
        kv_server: &KVServer<D>,
        linked_list_kv_server: &LinkedListKVServer<D>,
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
        let value = match self.use_linked_list() {
            true => match linked_list_kv_server.get(key) {
                Some(v) => v.as_ref().get_buffer(),
                None => {
                    bail!("Could not find value for key: {:?}", key);
                }
            },
            false => match kv_server.get(key) {
                Some(v) => v,
                None => {
                    bail!("Cannot find value for key in KV store: {:?}", key);
                }
            },
        };
        tracing::debug!("Value len: {:?}", value.as_ref().len());

        // construct response
        let mut response = builder.init_root::<kv_capnp::get_resp::Builder>();
        response.set_val(value.as_ref());
        Ok(())
    }

    fn handle_put<T>(
        &self,
        kv_server: &mut KVServer<D>,
        linked_list_kv_server: &mut LinkedListKVServer<D>,
        mempool_ids: &mut Vec<MempoolID>,
        pkt: &ReceivedPkt<D>,
        datapath: &mut D,
        builder: &mut Builder<T>,
    ) -> Result<()>
    where
        T: Allocator,
    {
        let segment_array_vec = read_context(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
        let segment_array = SegmentArray::new(&segment_array_vec.as_slice());
        let message_reader = Reader::new(segment_array, ReaderOptions::default());
        let put_request = message_reader
            .get_root::<kv_capnp::put_req::Reader>()
            .wrap_err("Failed to deserialize PutReq.")?;
        let key = put_request.get_key()?;
        let value = put_request.get_val()?;
        if self.use_linked_list() {
            linked_list_kv_server.insert_with_copies(key, value, datapath, mempool_ids)?;
        } else {
            kv_server.insert_with_copies(key, value, datapath, mempool_ids)?;
        }

        // construct response
        let mut response = builder.init_root::<kv_capnp::put_resp::Builder>();
        response.set_id(put_request.get_id());
        Ok(())
    }

    fn handle_getm<T>(
        &self,
        kv_server: &KVServer<D>,
        linked_list_kv_server: &LinkedListKVServer<D>,
        pkt: &ReceivedPkt<D>,
        builder: &mut Builder<T>,
    ) -> Result<()>
    where
        T: Allocator,
    {
        let segment_array_vec = read_context(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
        let segment_array = SegmentArray::new(&segment_array_vec.as_slice());
        let message_reader = Reader::new(segment_array, ReaderOptions::default());
        let getm_request = message_reader
            .get_root::<kv_capnp::get_m_req::Reader>()
            .wrap_err("Failed to deserialize GetMReq.")?;
        let keys = getm_request.get_keys()?;

        let response = builder.init_root::<kv_capnp::get_m_resp::Builder>();
        let mut list = response.init_vals(keys.len());
        for (i, key_res) in keys.iter().enumerate() {
            let key = key_res?;
            let value = match self.use_linked_list() {
                true => match linked_list_kv_server.get(key) {
                    Some(v) => v.as_ref().as_ref(),
                    None => {
                        bail!("Could not find value for key: {:?}", key);
                    }
                },
                false => match kv_server.get(key) {
                    Some(v) => v.as_ref(),
                    None => {
                        bail!("Cannot find value for key in KV store: {:?}", key);
                    }
                },
            };
            tracing::debug!("Value len: {:?}", value.as_ref().len());
            list.set(i as u32, value.as_ref());
        }
        Ok(())
    }

    fn handle_putm<T>(
        &self,
        kv_server: &mut KVServer<D>,
        linked_list_kv_server: &mut LinkedListKVServer<D>,
        mempool_ids: &mut Vec<MempoolID>,
        pkt: &ReceivedPkt<D>,
        datapath: &mut D,
        builder: &mut Builder<T>,
    ) -> Result<()>
    where
        T: Allocator,
    {
        let segment_array_vec = read_context(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
        let segment_array = SegmentArray::new(&segment_array_vec.as_slice());
        let message_reader = Reader::new(segment_array, ReaderOptions::default());
        let putm_request = message_reader
            .get_root::<kv_capnp::put_m_req::Reader>()
            .wrap_err("Failed to deserialize PutReq.")?;
        let keys = putm_request.get_keys()?;
        let values = putm_request.get_vals()?;
        for (key, value) in keys.iter().zip(values.iter()) {
            if self.use_linked_list() {
                linked_list_kv_server.insert_with_copies(key?, value?, datapath, mempool_ids)?;
            } else {
                kv_server.insert_with_copies(key?, value?, datapath, mempool_ids)?;
            }
        }

        // construct response
        let mut response = builder.init_root::<kv_capnp::put_resp::Builder>();
        response.set_id(putm_request.get_id());
        Ok(())
    }

    fn handle_getlist<T>(
        &self,
        list_kv_server: &ListKVServer<D>,
        linked_list_kv: &LinkedListKVServer<D>,
        pkt: &ReceivedPkt<D>,
        builder: &mut Builder<T>,
    ) -> Result<()>
    where
        T: Allocator,
    {
        let segment_array_vec = read_context(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
        let segment_array = SegmentArray::new(&segment_array_vec.as_slice());
        let message_reader = Reader::new(segment_array, ReaderOptions::default());
        let getlist_request = message_reader
            .get_root::<kv_capnp::get_list_req::Reader>()
            .wrap_err("Failed to deserialize GetList.")?;
        tracing::debug!(
            "Received get list request for key: {:?}",
            getlist_request.get_key()
        );
        let key = getlist_request.get_key()?;
        let response = builder.init_root::<kv_capnp::get_list_resp::Builder>();
        if self.use_linked_list() {
            let range_start = getlist_request.get_rangestart();
            let range_end = getlist_request.get_rangeend();
            let mut node_option = linked_list_kv.get(key);
            let range_len = {
                // TODO: range parsing is buggy?
                if range_end == -1 || range_end == 0 {
                    let mut len = 0;
                    while let Some(node) = node_option {
                        len += 1;
                        node_option = node.get_next();
                    }
                    len - range_start as usize
                } else {
                    ensure!(
                        range_start < range_end,
                        "Cannot process get list with range_end < range_start"
                    );
                    (range_end - range_start) as usize
                }
            };
            let mut list = response.init_vals(range_len as _);
            let mut node_option = linked_list_kv.get(key);

            let mut idx: i32 = 0;
            while let Some(node) = node_option {
                if idx < range_start {
                    node_option = node.get_next();
                    idx += 1;
                    continue;
                }
                if idx == range_len as i32 {
                    break;
                }
                list.set((idx - range_start) as u32, node.get_data());
                node_option = node.get_next();
                idx += 1;
            }
        } else {
            let values = match list_kv_server.get(key) {
                Some(v) => v,
                None => {
                    bail!("Cannot find value for key in KV store: {:?}", key);
                }
            };
            tracing::debug!("Values len: {:?}", values.len());

            let mut list = response.init_vals(values.len() as _);
            for (i, val) in values.iter().enumerate() {
                list.set(i as u32, val.as_ref());
            }
        }
        Ok(())
    }

    fn handle_putlist<T>(
        &self,
        list_kv_server: &mut ListKVServer<D>,
        linked_list_kv_server: &mut LinkedListKVServer<D>,
        mempool_ids: &mut Vec<MempoolID>,
        pkt: &ReceivedPkt<D>,
        datapath: &mut D,
        builder: &mut Builder<T>,
    ) -> Result<()>
    where
        T: Allocator,
    {
        let segment_array_vec = read_context(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
        let segment_array = SegmentArray::new(&segment_array_vec.as_slice());
        let message_reader = Reader::new(segment_array, ReaderOptions::default());
        let putlist_request = message_reader
            .get_root::<kv_capnp::put_list_req::Reader>()
            .wrap_err("Failed to deserialize PutReq.")?;
        let key = putlist_request.get_key()?;
        let values = putlist_request.get_vals()?.iter().map(|val| val.unwrap());
        if self.use_linked_list() {
            linked_list_kv_server.insert_list_with_copies(key, values, datapath, mempool_ids)?;
        } else {
            list_kv_server.insert_with_copies(key, values, datapath, mempool_ids)?;
        }

        // construct response
        let mut response = builder.init_root::<kv_capnp::put_resp::Builder>();
        response.set_id(putlist_request.get_id());
        Ok(())
    }
}

pub struct CapnprotoKVServer<D>
where
    D: Datapath,
{
    kv_server: KVServer<D>,
    list_kv_server: ListKVServer<D>,
    linked_list_kv_server: LinkedListKVServer<D>,
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
        use_linked_list: bool,
    ) -> Result<Self>
    where
        L: ServerLoadGenerator,
    {
        let (kv, list_kv, linked_list_kv, mempool_ids) =
            load_generator.new_kv_state(file, datapath, use_linked_list)?;
        Ok(CapnprotoKVServer {
            kv_server: kv,
            list_kv_server: list_kv,
            linked_list_kv_server: linked_list_kv,
            mempool_ids: mempool_ids,
            push_buf_type: push_buf_type,
            serializer: CapnprotoSerializer::new(use_linked_list),
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
        let pkts_len = sga.len();
        for (i, pkt) in sga.into_iter().enumerate() {
            let mut builder = Builder::new_default();
            let message_type = MsgType::from_packet(&pkt)?;
            match message_type {
                MsgType::Get => {
                    self.serializer.handle_get(
                        &self.kv_server,
                        &self.linked_list_kv_server,
                        &pkt,
                        &mut builder,
                    )?;
                }
                MsgType::GetM(_size) => {
                    self.serializer.handle_getm(
                        &self.kv_server,
                        &self.linked_list_kv_server,
                        &pkt,
                        &mut builder,
                    )?;
                }
                MsgType::GetList(_size) => {
                    self.serializer.handle_getlist(
                        &self.list_kv_server,
                        &self.linked_list_kv_server,
                        &pkt,
                        &mut builder,
                    )?;
                }
                MsgType::Put => {
                    self.serializer.handle_put(
                        &mut self.kv_server,
                        &mut self.linked_list_kv_server,
                        &mut self.mempool_ids,
                        &pkt,
                        datapath,
                        &mut builder,
                    )?;
                }
                MsgType::PutM(_size) => {
                    self.serializer.handle_putm(
                        &mut self.kv_server,
                        &mut self.linked_list_kv_server,
                        &mut self.mempool_ids,
                        &pkt,
                        datapath,
                        &mut builder,
                    )?;
                }
                MsgType::PutList(_size) => {
                    self.serializer.handle_putlist(
                        &mut self.list_kv_server,
                        &mut self.linked_list_kv_server,
                        &mut self.mempool_ids,
                        &pkt,
                        datapath,
                        &mut builder,
                    )?;
                }
                MsgType::AddUser => {
                    let segment_array_vec = read_context(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
                    let segment_array = SegmentArray::new(&segment_array_vec.as_slice());
                    let message_reader = Reader::new(segment_array, ReaderOptions::default());
                    let add_user_request = message_reader
                        .get_root::<kv_capnp::add_user::Reader>()
                        .wrap_err("Failed to deserialize AddUser.")?;
                    let keys = add_user_request.get_keys()?;
                    let values = add_user_request.get_vals()?;

                    let mut response = builder.init_root::<kv_capnp::add_user_response::Builder>();
                    let first_value = match self.kv_server.get(keys.get(0)?) {
                        Some(v) => v,
                        None => {
                            bail!("Cannot find value for key in KV store: {:?}", keys.get(0)?);
                        }
                    };
                    response.set_first_val(first_value.as_ref());
                    for (key_res, val_res) in keys.iter().zip(values.iter()) {
                        let key = key_res?;
                        let val = val_res?;
                        self.kv_server.insert_with_copies(
                            key,
                            val,
                            datapath,
                            &mut self.mempool_ids,
                        )?;
                    }
                }
                MsgType::FollowUnfollow => {
                    let segment_array_vec = read_context(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
                    let segment_array = SegmentArray::new(&segment_array_vec.as_slice());
                    let message_reader = Reader::new(segment_array, ReaderOptions::default());
                    let follow_unfollow_request = message_reader
                        .get_root::<kv_capnp::follow_unfollow::Reader>()
                        .wrap_err("Failed to deserialize FollowUnfollow.")?;
                    let keys = follow_unfollow_request.get_keys()?;
                    let values = follow_unfollow_request.get_vals()?;

                    let response =
                        builder.init_root::<kv_capnp::follow_unfollow_response::Builder>();
                    let mut list = response.init_original_vals(2);

                    for (i, (key_res, new_val_res)) in
                        keys.iter().zip(values.iter()).enumerate().take(2)
                    {
                        let key = key_res?;
                        let new_val = new_val_res?;
                        let old_val = match self.kv_server.get(key) {
                            Some(v) => v,
                            None => {
                                bail!("Cannot find value for key in KV store: {:?}", keys.get(0)?);
                            }
                        };
                        list.set(i as u32, old_val.as_ref());
                        self.kv_server.insert_with_copies(
                            key,
                            new_val,
                            datapath,
                            &mut self.mempool_ids,
                        )?;
                    }
                }
                MsgType::PostTweet => {
                    let segment_array_vec = read_context(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
                    let segment_array = SegmentArray::new(&segment_array_vec.as_slice());
                    let message_reader = Reader::new(segment_array, ReaderOptions::default());
                    let post_tweet_request = message_reader
                        .get_root::<kv_capnp::post_tweet::Reader>()
                        .wrap_err("Failed to deserialize Post Tweet.")?;
                    let keys = post_tweet_request.get_keys()?;
                    let values = post_tweet_request.get_vals()?;

                    let response = builder.init_root::<kv_capnp::post_tweet_response::Builder>();
                    let mut list = response.init_vals(3);

                    for (i, (key_res, new_val_res)) in
                        keys.iter().zip(values.iter()).enumerate().take(3)
                    {
                        let key = key_res?;
                        let new_val = new_val_res?;
                        let old_val = match self.kv_server.get(key) {
                            Some(v) => v,
                            None => {
                                bail!("Cannot find value for key in KV store: {:?}", keys.get(0)?);
                            }
                        };
                        list.set(i as u32, old_val.as_ref());
                        self.kv_server.insert_with_copies(
                            key,
                            new_val,
                            datapath,
                            &mut self.mempool_ids,
                        )?;
                    }
                    for (key_res, new_val_res) in keys.iter().zip(values.iter()).skip(3).take(2) {
                        let key = key_res?;
                        let new_val = new_val_res?;
                        self.kv_server.insert_with_copies(
                            key,
                            new_val,
                            datapath,
                            &mut self.mempool_ids,
                        )?;
                    }
                }
                MsgType::GetTimeline(_) => {
                    let segment_array_vec = read_context(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
                    let segment_array = SegmentArray::new(&segment_array_vec.as_slice());
                    let message_reader = Reader::new(segment_array, ReaderOptions::default());
                    let get_timeline_request = message_reader
                        .get_root::<kv_capnp::get_timeline::Reader>()
                        .wrap_err("Failed to deserialize Get Timeline.")?;
                    let keys = get_timeline_request.get_keys()?;

                    let response = builder.init_root::<kv_capnp::get_timeline_response::Builder>();
                    let mut list = response.init_vals(keys.len());

                    for (i, key_res) in keys.iter().enumerate() {
                        let key = key_res?;
                        let old_val = match self.kv_server.get(key) {
                            Some(v) => v,
                            None => {
                                bail!("Cannot find value for key in KV store: {:?}", keys.get(0)?);
                            }
                        };
                        list.set(i as u32, old_val.as_ref());
                    }
                }
                _ => {
                    unimplemented!();
                }
            }

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

    fn deserialize_get_response(&self, buf: &[u8]) -> Result<Vec<u8>> {
        let segment_array_vec = read_context(buf)?;
        let segment_array = SegmentArray::new(&segment_array_vec.as_slice());
        let message_reader = Reader::new(segment_array, ReaderOptions::default());
        let get_resp = message_reader
            .get_root::<kv_capnp::get_resp::Reader>()
            .wrap_err("Failed to deserialize GetResp.")?;
        if get_resp.has_val() {
            let val = get_resp
                .get_val()
                .wrap_err("deserialized get response does not have value")?;
            return Ok(val.to_vec());
        } else {
            return Ok(vec![]);
        }
    }

    fn deserialize_getm_response(&self, buf: &[u8]) -> Result<Vec<Vec<u8>>> {
        let segment_array_vec = read_context(buf)?;
        let segment_array = SegmentArray::new(&segment_array_vec.as_slice());
        let message_reader = Reader::new(segment_array, ReaderOptions::default());
        let getm_resp = message_reader
            .get_root::<kv_capnp::get_m_resp::Reader>()
            .wrap_err("Failed to deserialize GetMResp.")?;
        if getm_resp.has_vals() {
            let vals = getm_resp
                .get_vals()
                .wrap_err("deserialized get response does not have value")?;
            vals.iter()
                .map(|val| Ok(val?.to_vec()))
                .collect::<Result<Vec<Vec<u8>>>>()
        } else {
            return Ok(vec![]);
        }
    }

    fn deserialize_getlist_response(&self, buf: &[u8]) -> Result<Vec<Vec<u8>>> {
        let segment_array_vec = read_context(buf)?;
        let segment_array = SegmentArray::new(&segment_array_vec.as_slice());
        let message_reader = Reader::new(segment_array, ReaderOptions::default());
        let getlist_resp = message_reader
            .get_root::<kv_capnp::get_list_resp::Reader>()
            .wrap_err("Failed to deserialize GetListResp.")?;
        if getlist_resp.has_vals() {
            let vals = getlist_resp
                .get_vals()
                .wrap_err("deserialized get response does not have value")?;
            vals.iter()
                .map(|val| Ok(val?.to_vec()))
                .collect::<Result<Vec<Vec<u8>>>>()
        } else {
            return Ok(vec![]);
        }
    }

    fn check_add_user_num_values(&self, buf: &[u8]) -> Result<usize> {
        let segment_array_vec = read_context(buf)?;
        let segment_array = SegmentArray::new(&segment_array_vec.as_slice());
        let message_reader = Reader::new(segment_array, ReaderOptions::default());
        let add_user_resp = message_reader
            .get_root::<kv_capnp::add_user_response::Reader>()
            .wrap_err("Failed to deserialize AddUser Response.")?;
        if add_user_resp.has_first_val() {
            return Ok(1);
        } else {
            return Ok(0);
        }
    }

    fn check_follow_unfollow_num_values(&self, buf: &[u8]) -> Result<usize> {
        let segment_array_vec = read_context(buf)?;
        let segment_array = SegmentArray::new(&segment_array_vec.as_slice());
        let message_reader = Reader::new(segment_array, ReaderOptions::default());
        let follow_unfollow_resp = message_reader
            .get_root::<kv_capnp::follow_unfollow_response::Reader>()
            .wrap_err("Failed to deserialize FollowUnfollow Response.")?;
        if follow_unfollow_resp.has_original_vals() {
            return Ok(follow_unfollow_resp.get_original_vals()?.len() as _);
        } else {
            return Ok(0);
        }
    }

    fn check_post_tweet_num_values(&self, buf: &[u8]) -> Result<usize> {
        let segment_array_vec = read_context(buf)?;
        let segment_array = SegmentArray::new(&segment_array_vec.as_slice());
        let message_reader = Reader::new(segment_array, ReaderOptions::default());
        let post_tweet_resp = message_reader
            .get_root::<kv_capnp::post_tweet_response::Reader>()
            .wrap_err("Failed to deserialize PostTweet Response.")?;
        if post_tweet_resp.has_vals() {
            return Ok(post_tweet_resp.get_vals()?.len() as _);
        } else {
            return Ok(0);
        }
    }

    fn check_get_timeline_num_values(&self, buf: &[u8]) -> Result<usize> {
        let segment_array_vec = read_context(buf)?;
        let segment_array = SegmentArray::new(&segment_array_vec.as_slice());
        let message_reader = Reader::new(segment_array, ReaderOptions::default());
        let get_timeline_resp = message_reader
            .get_root::<kv_capnp::get_timeline_response::Reader>()
            .wrap_err("Failed to deserialize GetTimeline Response.")?;
        if get_timeline_resp.has_vals() {
            return Ok(get_timeline_resp.get_vals()?.len() as _);
        } else {
            return Ok(0);
        }
    }

    fn check_retwis_response_num_values(&self, _buf: &[u8]) -> Result<usize> {
        unimplemented!();
    }

    fn serialize_get(&self, buf: &mut [u8], key: &str, _datapath: &D) -> Result<usize> {
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

    fn serialize_put(
        &self,
        buf: &mut [u8],
        key: &str,
        value: &str,
        _datapath: &D,
    ) -> Result<usize> {
        let mut builder = Builder::new_default();
        let mut put_req = builder.init_root::<kv_capnp::put_req::Builder>();
        put_req.set_key(&key);
        put_req.set_val(&value.as_bytes());
        let framing_size = fill_in_context_without_arena(&builder, buf)?;
        let full_size = copy_into_buf(buf, framing_size, &builder)?;
        tracing::debug!(
            "Full buffer: {:?}, full buffer length: {}",
            &buf[0..full_size],
            full_size
        );
        return Ok(full_size);
    }

    fn serialize_append(
        &self,
        _buf: &mut [u8],
        _key: &str,
        _value: &str,
        _datapath: &D,
    ) -> Result<usize> {
        unimplemented!();
    }

    fn serialize_getm(&self, buf: &mut [u8], keys: &Vec<String>, _datapath: &D) -> Result<usize> {
        let mut builder = Builder::new_default();
        let getm_req = builder.init_root::<kv_capnp::get_m_req::Builder>();
        let mut keys_list = getm_req.init_keys(keys.len() as _);
        for (i, key) in keys.iter().enumerate() {
            keys_list.set(i as u32, key);
        }
        let framing_size = fill_in_context_without_arena(&builder, buf)?;
        let full_size = copy_into_buf(buf, framing_size, &builder)?;
        tracing::debug!(
            "Full buffer: {:?}, full buffer length: {}",
            &buf[0..full_size],
            full_size
        );
        return Ok(full_size);
    }

    fn serialize_putm(
        &self,
        buf: &mut [u8],
        keys: &Vec<String>,
        values: &Vec<String>,
        _datapath: &D,
    ) -> Result<usize> {
        let mut builder = Builder::new_default();
        let mut putm_req = builder.init_root::<kv_capnp::put_m_req::Builder>();
        {
            let mut keys_list = putm_req.reborrow().init_keys(keys.len() as _);
            for (i, key) in keys.iter().enumerate() {
                keys_list.set(i as u32, key);
            }
        }
        let mut vals_list = putm_req.init_vals(values.len() as _);
        for (i, val) in values.iter().enumerate() {
            vals_list.set(i as u32, val.as_ref());
        }
        let framing_size = fill_in_context_without_arena(&builder, buf)?;
        let full_size = copy_into_buf(buf, framing_size, &builder)?;
        tracing::debug!(
            "Full buffer: {:?}, full buffer length: {}",
            &buf[0..full_size],
            full_size
        );
        return Ok(full_size);
    }

    fn serialize_get_list(&self, buf: &mut [u8], key: &str, _datapath: &D) -> Result<usize> {
        let mut builder = Builder::new_default();
        let mut getlist_req = builder.init_root::<kv_capnp::get_list_req::Builder>();
        getlist_req.set_rangestart(0);
        getlist_req.set_rangeend(-1);
        getlist_req.set_key(&key);
        let framing_size = fill_in_context_without_arena(&builder, buf)?;
        let full_size = copy_into_buf(buf, framing_size, &builder)?;
        tracing::debug!(
            "Full buffer: {:?}, full buffer length: {}",
            &buf[0..full_size],
            full_size
        );
        return Ok(full_size);
    }

    fn serialize_put_list(
        &self,
        buf: &mut [u8],
        key: &str,
        values: &Vec<String>,
        _datapath: &D,
    ) -> Result<usize> {
        let mut builder = Builder::new_default();
        let mut putlist_req = builder.init_root::<kv_capnp::put_list_req::Builder>();
        putlist_req.set_key(&key);
        let mut vals_list = putlist_req.init_vals(values.len() as _);
        for (i, val) in values.iter().enumerate() {
            vals_list.set(i as u32, val.as_ref());
        }
        let framing_size = fill_in_context_without_arena(&builder, buf)?;
        let full_size = copy_into_buf(buf, framing_size, &builder)?;
        tracing::debug!(
            "Full buffer: {:?}, full buffer length: {}",
            &buf[0..full_size],
            full_size
        );
        return Ok(full_size);
    }

    fn serialize_add_user(
        &self,
        buf: &mut [u8],
        keys: &Vec<&str>,
        values: &Vec<String>,
        _datapath: &D,
    ) -> Result<usize> {
        let mut builder = Builder::new_default();
        let mut add_user = builder.init_root::<kv_capnp::add_user::Builder>();
        {
            let mut keys_list = add_user.reborrow().init_keys(keys.len() as _);
            for (i, key) in keys.iter().enumerate() {
                keys_list.set(i as u32, key);
            }
        }
        let mut vals_list = add_user.init_vals(values.len() as _);
        for (i, val) in values.iter().enumerate() {
            vals_list.set(i as u32, val.as_str().as_bytes());
        }
        let framing_size = fill_in_context_without_arena(&builder, buf)?;
        let full_size = copy_into_buf(buf, framing_size, &builder)?;
        tracing::debug!(
            "Full buffer: {:?}, full buffer length: {}",
            &buf[0..full_size],
            full_size
        );
        return Ok(full_size);
    }

    fn serialize_add_follow_unfollow(
        &self,
        buf: &mut [u8],
        keys: &Vec<&str>,
        values: &Vec<String>,
        _datapath: &D,
    ) -> Result<usize> {
        let mut builder = Builder::new_default();
        let mut follow_unfollow = builder.init_root::<kv_capnp::follow_unfollow::Builder>();
        {
            let mut keys_list = follow_unfollow.reborrow().init_keys(keys.len() as _);
            for (i, key) in keys.iter().enumerate() {
                keys_list.set(i as u32, key);
            }
        }
        let mut vals_list = follow_unfollow.init_vals(values.len() as _);
        for (i, val) in values.iter().enumerate() {
            vals_list.set(i as u32, val.as_str().as_bytes());
        }
        let framing_size = fill_in_context_without_arena(&builder, buf)?;
        let full_size = copy_into_buf(buf, framing_size, &builder)?;
        tracing::debug!(
            "Full buffer: {:?}, full buffer length: {}",
            &buf[0..full_size],
            full_size
        );
        return Ok(full_size);
    }

    fn serialize_post_tweet(
        &self,
        buf: &mut [u8],
        keys: &Vec<&str>,
        values: &Vec<String>,
        _datapath: &D,
    ) -> Result<usize> {
        let mut builder = Builder::new_default();
        let mut post_tweet = builder.init_root::<kv_capnp::post_tweet::Builder>();
        {
            let mut keys_list = post_tweet.reborrow().init_keys(keys.len() as _);
            for (i, key) in keys.iter().enumerate() {
                keys_list.set(i as u32, key);
            }
        }
        let mut vals_list = post_tweet.init_vals(values.len() as _);
        for (i, val) in values.iter().enumerate() {
            vals_list.set(i as u32, val.as_str().as_bytes());
        }
        let framing_size = fill_in_context_without_arena(&builder, buf)?;
        let full_size = copy_into_buf(buf, framing_size, &builder)?;
        tracing::debug!(
            "Full buffer: {:?}, full buffer length: {}",
            &buf[0..full_size],
            full_size
        );
        return Ok(full_size);
    }

    fn serialize_get_timeline(
        &self,
        buf: &mut [u8],
        keys: &Vec<&str>,
        _values: &Vec<String>,
        _datapath: &D,
    ) -> Result<usize> {
        let mut builder = Builder::new_default();
        let get_timeline_req = builder.init_root::<kv_capnp::get_timeline::Builder>();
        let mut keys_list = get_timeline_req.init_keys(keys.len() as _);
        for (i, key) in keys.iter().enumerate() {
            keys_list.set(i as u32, key);
        }
        let framing_size = fill_in_context_without_arena(&builder, buf)?;
        let full_size = copy_into_buf(buf, framing_size, &builder)?;
        tracing::debug!(
            "Full buffer: {:?}, full buffer length: {}",
            &buf[0..full_size],
            full_size
        );
        return Ok(full_size);
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
