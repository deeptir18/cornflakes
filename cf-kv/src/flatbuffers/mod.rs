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
    ClientSerializer, KVServer, LinkedListKVServer, ListKVServer, MsgType, ServerLoadGenerator,
    REQ_TYPE_SIZE,
};
use color_eyre::eyre::{bail, ensure, Result};
use cornflakes_libos::{
    allocator::MempoolID,
    datapath::{Datapath, PushBufType, ReceivedPkt},
    state_machine::server::ServerSM,
};
use flatbuffers::{root, FlatBufferBuilder, WIPOffset};
use kv_api::cf_kv_fbs;
use std::marker::PhantomData;

pub struct FlatbuffersSerializer<D>
where
    D: Datapath,
{
    _phantom: PhantomData<D>,
    use_linked_list_kv: bool,
}

impl<D> FlatbuffersSerializer<D>
where
    D: Datapath,
{
    pub fn new(use_linked_list_kv: bool) -> Self {
        FlatbuffersSerializer {
            _phantom: PhantomData::default(),
            use_linked_list_kv: use_linked_list_kv,
        }
    }

    pub fn use_linked_list(&self) -> bool {
        self.use_linked_list_kv
    }

    fn handle_get(
        &self,
        kv_server: &KVServer<D>,
        linked_list_kv_server: &LinkedListKVServer<D>,
        pkt: &ReceivedPkt<D>,
        builder: &mut FlatBufferBuilder,
    ) -> Result<()> {
        let get_request = root::<cf_kv_fbs::GetReq>(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
        let value = match self.use_linked_list() {
            true => match linked_list_kv_server.get(get_request.key().unwrap()) {
                Some(v) => v.as_ref().as_ref(),
                None => {
                    bail!("Could not find value for key: {:?}", get_request.key());
                }
            },
            false => match kv_server.get(get_request.key().unwrap()) {
                Some(v) => v.as_ref(),
                None => {
                    bail!("Could not find value for key: {:?}", get_request.key());
                }
            },
        };

        tracing::debug!(
            "For given key {:?}, found value {:?} with length {}",
            get_request.key().unwrap(),
            value.as_ptr(),
            value.len()
        );
        let args = cf_kv_fbs::GetRespArgs {
            val: Some(builder.create_vector_direct::<u8>(value)),
            id: get_request.id(),
        };

        let get_resp = cf_kv_fbs::GetResp::create(builder, &args);
        builder.finish(get_resp, None);
        Ok(())
    }

    fn handle_get_from_list(
        &self,
        list_kv_server: &ListKVServer<D>,
        pkt: &ReceivedPkt<D>,
        builder: &mut FlatBufferBuilder,
    ) -> Result<()> {
        let get_request = {
            #[cfg(feature = "profiler")]
            demikernel::timer!("Deserialize pkt");
            root::<cf_kv_fbs::GetFromListReq>(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?
        };
        let value = {
            #[cfg(feature = "profiler")]
            demikernel::timer!("Retrieve value");
            match list_kv_server.get(get_request.key().unwrap()) {
                Some(list) => match list.get(get_request.idx() as usize) {
                    Some(v) => v,
                    None => {
                        bail!(
                            "Could not find idx {} for key {} in list kv server",
                            get_request.idx(),
                            get_request.key().unwrap(),
                        );
                    }
                },
                None => {
                    bail!(
                        "Could not find value for key: {:?}",
                        get_request.key().unwrap()
                    );
                }
            }
        };

        tracing::debug!(
            "For given key {:?}, found value {:?} with length {}",
            get_request.key().unwrap(),
            value.as_ref(),
            value.as_ref().len()
        );
        let args = {
            cf_kv_fbs::GetRespArgs {
                val: {
                    #[cfg(feature = "profiler")]
                    demikernel::timer!("Generate response args");
                    Some(builder.create_vector_direct::<u8>(value.as_ref()))
                },
                id: get_request.id(),
            }
        };

        let get_resp = cf_kv_fbs::GetResp::create(builder, &args);
        {
            #[cfg(feature = "profiler")]
            demikernel::timer!("finish serialize");
            builder.finish(get_resp, None);
        }
        Ok(())
    }

    fn handle_put(
        &self,
        kv_server: &mut KVServer<D>,
        mempool_ids: &mut Vec<MempoolID>,
        pkt: &ReceivedPkt<D>,
        datapath: &mut D,
        builder: &mut FlatBufferBuilder,
    ) -> Result<()> {
        let put_req = root::<cf_kv_fbs::PutReq>(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
        kv_server.insert_with_copies(
            put_req.key().unwrap(),
            put_req.val().unwrap(),
            datapath,
            mempool_ids,
        )?;
        let args = cf_kv_fbs::PutRespArgs { id: put_req.id() };
        let get_resp = cf_kv_fbs::PutResp::create(builder, &args);
        builder.finish(get_resp, None);
        Ok(())
    }

    fn handle_getm(
        &self,
        kv_server: &KVServer<D>,
        linked_list_kv_server: &LinkedListKVServer<D>,
        pkt: &ReceivedPkt<D>,
        builder: &mut FlatBufferBuilder,
    ) -> Result<()> {
        let getm_request = {
            #[cfg(feature = "profiler")]
            demikernel::timer!("deserialize");
            root::<cf_kv_fbs::GetMReq>(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])
        }?;
        let keys = getm_request.keys().unwrap();
        let args_vec_res: Result<Vec<cf_kv_fbs::ValueArgs>> = keys
            .iter()
            .map(|key| {
                #[cfg(feature = "profiler")]
                demikernel::timer!("got value");
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
                            bail!("Could not find value for key: {:?}", key);
                        }
                    },
                };
                {
                    #[cfg(feature = "profiler")]
                    demikernel::timer!("append value");
                    Ok(cf_kv_fbs::ValueArgs {
                        data: Some(builder.create_vector_direct::<u8>(value)),
                    })
                }
            })
            .collect();
        let args_vec: Vec<WIPOffset<cf_kv_fbs::Value>> = args_vec_res?
            .iter()
            .map(|args| cf_kv_fbs::Value::create(builder, args))
            .collect();
        let getm_resp_args = cf_kv_fbs::GetMRespArgs {
            id: getm_request.id(),
            vals: Some(builder.create_vector(args_vec.as_slice())),
        };
        let getm_resp = cf_kv_fbs::GetMResp::create(builder, &getm_resp_args);
        builder.finish(getm_resp, None);
        Ok(())
    }

    fn handle_putm(
        &self,
        kv_server: &mut KVServer<D>,
        mempool_ids: &mut Vec<MempoolID>,
        pkt: &ReceivedPkt<D>,
        datapath: &mut D,
        builder: &mut FlatBufferBuilder,
    ) -> Result<()> {
        let putm_request = root::<cf_kv_fbs::PutMReq>(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
        let keys = putm_request.keys().unwrap();
        let vals = putm_request.vals().unwrap();
        for (key, value) in keys.iter().zip(vals.iter()) {
            let val = value.data().unwrap();
            kv_server.insert_with_copies(key, val, datapath, mempool_ids)?;
        }
        let args = cf_kv_fbs::PutRespArgs {
            id: putm_request.id(),
        };

        let put_resp = cf_kv_fbs::PutResp::create(builder, &args);
        builder.finish(put_resp, None);
        Ok(())
    }

    fn handle_getlist(
        &self,
        list_kv_server: &ListKVServer<D>,
        linked_list_kv_server: &LinkedListKVServer<D>,
        pkt: &ReceivedPkt<D>,
        builder: &mut FlatBufferBuilder,
    ) -> Result<()> {
        let getlist_request = {
            #[cfg(feature = "profiler")]
            demikernel::timer!("deserialize");
            root::<cf_kv_fbs::GetListReq>(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?
        };
        let key = getlist_request.key().unwrap();
        if self.use_linked_list() {
            let range_start = getlist_request.rangestart();
            let range_end = getlist_request.rangeend();

            let mut node_option = {
                #[cfg(feature = "profiler")]
                demikernel::timer!("do get on key");
                linked_list_kv_server.get(key)
            };

            let range_len = {
                // TODO: hack: flatbuffers doesn't seem to be recognizing -1
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
                        format!(
                            "Cannot process get list with range_end {}< range_start: {}",
                            range_start, range_end
                        )
                    );
                    (range_end - range_start) as usize
                }
            };

            let mut node_option = {
                #[cfg(feature = "profiler")]
                demikernel::timer!("do get on key 2nd time");
                linked_list_kv_server.get(key)
            };
            let mut idx = 0;
            let mut args_vec: Vec<cf_kv_fbs::ValueArgs> = Vec::with_capacity(range_len);
            while let Some(node) = node_option {
                if idx < range_start {
                    node_option = node.get_next();
                    idx += 1;
                    continue;
                } else if idx as usize == range_len {
                    break;
                }

                #[cfg(feature = "profiler")]
                demikernel::timer!("append node to list");
                args_vec.push(cf_kv_fbs::ValueArgs {
                    data: Some(builder.create_vector_direct::<u8>(node.as_ref().get_data())),
                });

                node_option = node.get_next();
                idx += 1;
            }
            {
                #[cfg(feature = "profiler")]
                demikernel::timer!("finish serializing builder");
                let args_vec: Vec<WIPOffset<cf_kv_fbs::Value>> = args_vec
                    .iter()
                    .map(|args| cf_kv_fbs::Value::create(builder, args))
                    .collect();
                let getlist_resp_args = cf_kv_fbs::GetListRespArgs {
                    id: getlist_request.id(),
                    vals: Some(builder.create_vector(args_vec.as_slice())),
                };
                let getlist_resp = cf_kv_fbs::GetListResp::create(builder, &getlist_resp_args);
                builder.finish(getlist_resp, None)
            };
        } else {
            let vals = match list_kv_server.get(key) {
                Some(v) => v,
                None => {
                    bail!("Cannot find value for key in KV store: {:?}", key);
                }
            };
            let args_vec: Vec<cf_kv_fbs::ValueArgs> = vals
                .iter()
                .map(|v| cf_kv_fbs::ValueArgs {
                    data: Some(builder.create_vector_direct::<u8>(v.as_ref())),
                })
                .collect();
            let args_vec: Vec<WIPOffset<cf_kv_fbs::Value>> = args_vec
                .iter()
                .map(|args| cf_kv_fbs::Value::create(builder, args))
                .collect();
            let getlist_resp_args = cf_kv_fbs::GetListRespArgs {
                id: getlist_request.id(),
                vals: Some(builder.create_vector(args_vec.as_slice())),
            };
            {
                #[cfg(feature = "profiler")]
                demikernel::timer!("finish serializing builder");
                let getlist_resp = cf_kv_fbs::GetListResp::create(builder, &getlist_resp_args);
                builder.finish(getlist_resp, None)
            };
        }
        Ok(())
    }

    fn handle_putlist(
        &self,
        list_kv_server: &mut ListKVServer<D>,
        mempool_ids: &mut Vec<MempoolID>,
        pkt: &ReceivedPkt<D>,
        datapath: &mut D,
        builder: &mut FlatBufferBuilder,
    ) -> Result<()> {
        let putlist_request = root::<cf_kv_fbs::PutListReq>(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
        let key = putlist_request.key().unwrap();
        let values = putlist_request
            .vals()
            .unwrap()
            .iter()
            .map(|value| value.data().unwrap());
        list_kv_server.insert_with_copies(key, values, datapath, mempool_ids)?;
        let args = cf_kv_fbs::PutRespArgs {
            id: putlist_request.id(),
        };

        let put_resp = cf_kv_fbs::PutResp::create(builder, &args);
        builder.finish(put_resp, None);
        Ok(())
    }
}

pub struct FlatbuffersKVServer<'fbb, D>
where
    D: Datapath,
{
    kv_server: KVServer<D>,
    list_kv_server: ListKVServer<D>,
    linked_list_kv_server: LinkedListKVServer<D>,
    mempool_ids: Vec<MempoolID>,
    serializer: FlatbuffersSerializer<D>,
    push_buf_type: PushBufType,
    builder: FlatBufferBuilder<'fbb>,
}

impl<'fbb, D> FlatbuffersKVServer<'fbb, D>
where
    D: Datapath,
{
    pub fn new<L>(
        file: &str,
        load_generator: L,
        datapath: &mut D,
        push_buf_type: PushBufType,
        use_linked_list_kv: bool,
    ) -> Result<Self>
    where
        L: ServerLoadGenerator,
    {
        let (kv, list_kv, linked_list_kv, mempool_ids) =
            load_generator.new_kv_state(file, datapath, use_linked_list_kv)?;
        Ok(FlatbuffersKVServer {
            kv_server: kv,
            list_kv_server: list_kv,
            linked_list_kv_server: linked_list_kv,
            mempool_ids: mempool_ids,
            push_buf_type: push_buf_type,
            serializer: FlatbuffersSerializer::new(use_linked_list_kv),
            builder: FlatBufferBuilder::new(),
        })
    }
}

impl<'fbb, D> ServerSM for FlatbuffersKVServer<'fbb, D>
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
            self.builder.reset();
            let message_type = MsgType::from_packet(&pkt)?;
            match message_type {
                MsgType::Get => {
                    self.serializer.handle_get(
                        &self.kv_server,
                        &self.linked_list_kv_server,
                        &pkt,
                        &mut self.builder,
                    )?;
                }
                MsgType::GetFromList => {
                    self.serializer.handle_get_from_list(
                        &self.list_kv_server,
                        &pkt,
                        &mut self.builder,
                    )?;
                }
                MsgType::GetM(_size) => {
                    self.serializer.handle_getm(
                        &self.kv_server,
                        &self.linked_list_kv_server,
                        &pkt,
                        &mut self.builder,
                    )?;
                }
                MsgType::GetList(_size) => {
                    #[cfg(feature = "profiler")]
                    demikernel::timer!("handle getlist flatbuffers");
                    self.serializer.handle_getlist(
                        &self.list_kv_server,
                        &self.linked_list_kv_server,
                        &pkt,
                        &mut self.builder,
                    )?;
                }
                MsgType::Put => {
                    self.serializer.handle_put(
                        &mut self.kv_server,
                        &mut self.mempool_ids,
                        &pkt,
                        datapath,
                        &mut self.builder,
                    )?;
                }
                MsgType::PutM(_size) => {
                    self.serializer.handle_putm(
                        &mut self.kv_server,
                        &mut self.mempool_ids,
                        &pkt,
                        datapath,
                        &mut self.builder,
                    )?;
                }
                MsgType::PutList(_size) => {
                    self.serializer.handle_putlist(
                        &mut self.list_kv_server,
                        &mut self.mempool_ids,
                        &pkt,
                        datapath,
                        &mut self.builder,
                    )?;
                }
                MsgType::AddUser => {
                    let add_user =
                        root::<cf_kv_fbs::AddUser>(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
                    let keys = add_user.keys().unwrap();
                    let vals = add_user.vals().unwrap();
                    let value = self.kv_server.get(keys.get(0)).unwrap();
                    let args = cf_kv_fbs::AddUserResponseArgs {
                        first_value: Some(self.builder.create_vector_direct::<u8>(value.as_ref())),
                    };
                    for (key, val) in keys.iter().zip(vals.iter()) {
                        self.kv_server.insert_with_copies(
                            key,
                            val.data().unwrap(),
                            datapath,
                            &mut self.mempool_ids,
                        )?;
                    }

                    let add_user_response =
                        cf_kv_fbs::AddUserResponse::create(&mut self.builder, &args);
                    self.builder.finish(add_user_response, None);
                }
                MsgType::FollowUnfollow => {
                    let follow_unfollow =
                        root::<cf_kv_fbs::FollowUnfollow>(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
                    let keys = follow_unfollow.keys().unwrap();
                    let args_vec_res: Result<Vec<cf_kv_fbs::ValueArgs>> = keys
                        .iter()
                        .map(|key| {
                            let v = match self.kv_server.get(key) {
                                Some(v) => v,
                                None => {
                                    bail!("Cannot find value for key in KV store: {:?}", key);
                                }
                            };
                            Ok(cf_kv_fbs::ValueArgs {
                                data: Some(self.builder.create_vector_direct::<u8>(v.as_ref())),
                            })
                        })
                        .collect();
                    let args_vec: Vec<WIPOffset<cf_kv_fbs::Value>> = args_vec_res?
                        .iter()
                        .map(|args| cf_kv_fbs::Value::create(&mut self.builder, args))
                        .collect();

                    let args = cf_kv_fbs::FollowUnfollowResponseArgs {
                        original_vals: Some(self.builder.create_vector(args_vec.as_slice())),
                    };
                    let vals = follow_unfollow.vals().unwrap();
                    for (key, val) in keys.iter().zip(vals.iter()) {
                        self.kv_server.insert_with_copies(
                            key,
                            val.data().unwrap(),
                            datapath,
                            &mut self.mempool_ids,
                        )?;
                    }

                    let follow_unfollow_response =
                        cf_kv_fbs::FollowUnfollowResponse::create(&mut self.builder, &args);
                    self.builder.finish(follow_unfollow_response, None);
                }
                MsgType::PostTweet => {
                    // 3 gets, 5 puts
                    let post_tweet =
                        root::<cf_kv_fbs::PostTweet>(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
                    let keys = post_tweet.keys().unwrap();
                    let args_vec_res: Result<Vec<cf_kv_fbs::ValueArgs>> = keys
                        .iter()
                        .take(3)
                        .map(|key| {
                            let v = match self.kv_server.get(key) {
                                Some(v) => v,
                                None => {
                                    bail!(
                                        "Cannot find value for key in KV store: {:?}; len: {}; keys: {:?}",
                                        key,
                                        self.kv_server.len(),keys
                                    );
                                }
                            };
                            Ok(cf_kv_fbs::ValueArgs {
                                data: Some(self.builder.create_vector_direct::<u8>(v.as_ref())),
                            })
                        })
                        .collect();
                    let args_vec: Vec<WIPOffset<cf_kv_fbs::Value>> = args_vec_res?
                        .iter()
                        .map(|args| cf_kv_fbs::Value::create(&mut self.builder, args))
                        .collect();

                    let args = cf_kv_fbs::PostTweetResponseArgs {
                        vals: Some(self.builder.create_vector(args_vec.as_slice())),
                    };
                    let vals = post_tweet.vals().unwrap();
                    for (key, val) in keys.iter().zip(vals.iter()) {
                        self.kv_server.insert_with_copies(
                            key,
                            val.data().unwrap(),
                            datapath,
                            &mut self.mempool_ids,
                        )?;
                    }

                    let post_tweet_response =
                        cf_kv_fbs::PostTweetResponse::create(&mut self.builder, &args);
                    self.builder.finish(post_tweet_response, None);
                }
                MsgType::GetTimeline(_) => {
                    let get_timeline_request =
                        root::<cf_kv_fbs::GetTimeline>(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
                    let keys = get_timeline_request.keys().unwrap();
                    let args_vec_res: Result<Vec<cf_kv_fbs::ValueArgs>> = keys
                        .iter()
                        .map(|key| {
                            let v = match self.kv_server.get(key) {
                                Some(v) => v,
                                None => {
                                    bail!("Cannot find value for key in KV store: {:?}", key);
                                }
                            };
                            Ok(cf_kv_fbs::ValueArgs {
                                data: Some(self.builder.create_vector_direct::<u8>(v.as_ref())),
                            })
                        })
                        .collect();
                    let args_vec: Vec<WIPOffset<cf_kv_fbs::Value>> = args_vec_res?
                        .iter()
                        .map(|args| cf_kv_fbs::Value::create(&mut self.builder, args))
                        .collect();
                    let get_timeline_resp_args = cf_kv_fbs::GetTimelineResponseArgs {
                        vals: Some(self.builder.create_vector(args_vec.as_slice())),
                    };
                    let get_timeline_resp = cf_kv_fbs::GetTimelineResponse::create(
                        &mut self.builder,
                        &get_timeline_resp_args,
                    );
                    self.builder.finish(get_timeline_resp, None);
                }
                _ => {
                    unimplemented!();
                }
            }
            datapath.queue_single_buffer_with_copy(
                (pkt.msg_id(), pkt.conn_id(), &self.builder.finished_data()),
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

    fn deserialize_get_response(&self, buf: &[u8]) -> Result<Vec<u8>> {
        let get_resp = root::<cf_kv_fbs::GetResp>(buf)?;
        match get_resp.val() {
            Some(x) => {
                return Ok(x.to_vec());
            }
            None => {
                return Ok(vec![]);
            }
        };
    }

    fn deserialize_getm_response(&self, buf: &[u8]) -> Result<Vec<Vec<u8>>> {
        let getm_resp = root::<cf_kv_fbs::GetMResp>(buf)?;
        match getm_resp.vals() {
            Some(x) => {
                return Ok(x
                    .iter()
                    .map(|val| val.data().unwrap().to_vec())
                    .collect::<Vec<Vec<u8>>>());
            }
            None => {
                return Ok(vec![]);
            }
        };
    }

    fn deserialize_getlist_response(&self, buf: &[u8]) -> Result<Vec<Vec<u8>>> {
        let getlist_resp = root::<cf_kv_fbs::GetListResp>(buf)?;
        match getlist_resp.vals() {
            Some(x) => {
                return Ok(x
                    .iter()
                    .map(|val| val.data().unwrap().to_vec())
                    .collect::<Vec<Vec<u8>>>());
            }
            None => {
                return Ok(vec![]);
            }
        };
    }

    fn check_add_user_num_values(&self, buf: &[u8]) -> Result<usize> {
        let add_user_resp = root::<cf_kv_fbs::AddUserResponse>(buf)?;
        match add_user_resp.first_value() {
            Some(_) => return Ok(1),
            None => return Ok(0),
        }
    }

    fn check_follow_unfollow_num_values(&self, buf: &[u8]) -> Result<usize> {
        let follow_unfollow_resp = root::<cf_kv_fbs::FollowUnfollowResponse>(buf)?;
        match follow_unfollow_resp.original_vals() {
            Some(x) => return Ok(x.len()),
            None => return Ok(0),
        }
    }

    fn check_post_tweet_num_values(&self, buf: &[u8]) -> Result<usize> {
        let post_tweet_resp = root::<cf_kv_fbs::PostTweetResponse>(buf)?;
        match post_tweet_resp.vals() {
            Some(x) => return Ok(x.len()),
            None => return Ok(0),
        }
    }

    fn check_get_timeline_num_values(&self, buf: &[u8]) -> Result<usize> {
        let get_timeline_resp = root::<cf_kv_fbs::GetTimelineResponse>(buf)?;
        match get_timeline_resp.vals() {
            Some(x) => return Ok(x.len()),
            None => return Ok(0),
        }
    }

    fn check_retwis_response_num_values(&self, _buf: &[u8]) -> Result<usize> {
        unimplemented!();
    }

    fn serialize_get(&self, buf: &mut [u8], key: &str, _datapath: &D) -> Result<usize> {
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

    fn serialize_get_from_list(
        &self,
        buf: &mut [u8],
        key: &str,
        idx: usize,
        _datapath: &D,
    ) -> Result<usize> {
        let mut builder = FlatBufferBuilder::new();
        let args = cf_kv_fbs::GetFromListReqArgs {
            // TODO: actually add in ID
            id: 0,
            idx: idx as u32,
            key: Some(builder.create_string(key.as_ref())),
        };
        let get_req = cf_kv_fbs::GetFromListReq::create(&mut builder, &args);
        builder.finish(get_req, None);
        Ok(copy_into_buf(buf, &builder))
    }

    fn serialize_put(
        &self,
        buf: &mut [u8],
        key: &str,
        value: &str,
        _datapath: &D,
    ) -> Result<usize> {
        let mut builder = FlatBufferBuilder::new();
        let args = cf_kv_fbs::PutReqArgs {
            id: 0,
            key: Some(builder.create_string(key.as_ref())),
            val: Some(builder.create_vector_direct::<u8>(value.as_bytes())),
        };
        let put_req = cf_kv_fbs::PutReq::create(&mut builder, &args);
        builder.finish(put_req, None);
        Ok(copy_into_buf(buf, &builder))
    }

    fn serialize_getm(&self, buf: &mut [u8], keys: &Vec<String>, _datapath: &D) -> Result<usize> {
        let mut builder = FlatBufferBuilder::new();
        let args_vec: Vec<WIPOffset<&str>> = keys
            .iter()
            .map(|key| builder.create_string(key.as_str()))
            .collect();
        let getm_req_args = cf_kv_fbs::GetMReqArgs {
            id: 0,
            keys: Some(builder.create_vector(args_vec.as_slice())),
        };
        let getm_req = cf_kv_fbs::GetMReq::create(&mut builder, &getm_req_args);
        builder.finish(getm_req, None);
        Ok(copy_into_buf(buf, &builder))
    }

    fn serialize_putm(
        &self,
        buf: &mut [u8],
        keys: &Vec<String>,
        values: &Vec<String>,
        _datapath: &D,
    ) -> Result<usize> {
        let mut builder = FlatBufferBuilder::new();
        let keys_vec: Vec<WIPOffset<&str>> = keys
            .iter()
            .map(|key| builder.create_string(key.as_str()))
            .collect();

        let val_vec_data: Vec<cf_kv_fbs::ValueArgs> = values
            .iter()
            .map(|val| cf_kv_fbs::ValueArgs {
                data: Some(builder.create_vector_direct::<u8>(&val.as_str().as_bytes())),
            })
            .collect();
        let val_vec: Vec<WIPOffset<cf_kv_fbs::Value>> = val_vec_data
            .iter()
            .map(|args| cf_kv_fbs::Value::create(&mut builder, args))
            .collect();
        let putm_req_args = cf_kv_fbs::PutMReqArgs {
            id: 0,
            keys: Some(builder.create_vector(keys_vec.as_slice())),
            vals: Some(builder.create_vector(val_vec.as_slice())),
        };
        let putm_req = cf_kv_fbs::PutMReq::create(&mut builder, &putm_req_args);
        builder.finish(putm_req, None);
        Ok(copy_into_buf(buf, &builder))
    }

    fn serialize_get_list(&self, buf: &mut [u8], key: &str, _datapath: &D) -> Result<usize> {
        let mut builder = FlatBufferBuilder::new();
        let args = cf_kv_fbs::GetListReqArgs {
            // TODO: actually add in ID
            id: 0,
            key: Some(builder.create_string(key.as_ref())),
            rangestart: 0,
            rangeend: -1,
        };
        let get_list = cf_kv_fbs::GetListReq::create(&mut builder, &args);
        builder.finish(get_list, None);
        Ok(copy_into_buf(buf, &builder))
    }

    fn serialize_put_list(
        &self,
        buf: &mut [u8],
        key: &str,
        values: &Vec<String>,
        _datapath: &D,
    ) -> Result<usize> {
        let mut builder = FlatBufferBuilder::new();
        let val_vec_data: Vec<cf_kv_fbs::ValueArgs> = values
            .iter()
            .map(|val| cf_kv_fbs::ValueArgs {
                data: Some(builder.create_vector_direct::<u8>(&val.as_str().as_bytes())),
            })
            .collect();
        let val_vec: Vec<WIPOffset<cf_kv_fbs::Value>> = val_vec_data
            .iter()
            .map(|args| cf_kv_fbs::Value::create(&mut builder, args))
            .collect();
        let putlist_req_args = cf_kv_fbs::PutListReqArgs {
            id: 0,
            key: Some(builder.create_string(key.as_ref())),
            vals: Some(builder.create_vector(val_vec.as_slice())),
        };
        let putlist_req = cf_kv_fbs::PutListReq::create(&mut builder, &putlist_req_args);
        builder.finish(putlist_req, None);
        Ok(copy_into_buf(buf, &builder))
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

    fn serialize_add_user(
        &self,
        buf: &mut [u8],
        keys: &Vec<&str>,
        values: &Vec<String>,
        _datapath: &D,
    ) -> Result<usize> {
        let mut builder = FlatBufferBuilder::new();
        let keys_vec: Vec<WIPOffset<&str>> =
            keys.iter().map(|key| builder.create_string(key)).collect();

        let val_vec_data: Vec<cf_kv_fbs::ValueArgs> = values
            .iter()
            .map(|val| cf_kv_fbs::ValueArgs {
                data: Some(builder.create_vector_direct::<u8>(&val.as_str().as_bytes())),
            })
            .collect();
        let val_vec: Vec<WIPOffset<cf_kv_fbs::Value>> = val_vec_data
            .iter()
            .map(|args| cf_kv_fbs::Value::create(&mut builder, args))
            .collect();
        let add_user_args = cf_kv_fbs::AddUserArgs {
            keys: Some(builder.create_vector(keys_vec.as_slice())),
            vals: Some(builder.create_vector(val_vec.as_slice())),
        };
        let add_user = cf_kv_fbs::AddUser::create(&mut builder, &add_user_args);
        builder.finish(add_user, None);
        Ok(copy_into_buf(buf, &builder))
    }

    fn serialize_add_follow_unfollow(
        &self,
        buf: &mut [u8],
        keys: &Vec<&str>,
        values: &Vec<String>,
        _datapath: &D,
    ) -> Result<usize> {
        let mut builder = FlatBufferBuilder::new();
        let keys_vec: Vec<WIPOffset<&str>> =
            keys.iter().map(|key| builder.create_string(key)).collect();

        let val_vec_data: Vec<cf_kv_fbs::ValueArgs> = values
            .iter()
            .map(|val| cf_kv_fbs::ValueArgs {
                data: Some(builder.create_vector_direct::<u8>(&val.as_str().as_bytes())),
            })
            .collect();
        let val_vec: Vec<WIPOffset<cf_kv_fbs::Value>> = val_vec_data
            .iter()
            .map(|args| cf_kv_fbs::Value::create(&mut builder, args))
            .collect();
        let follow_unfollow_args = cf_kv_fbs::FollowUnfollowArgs {
            keys: Some(builder.create_vector(keys_vec.as_slice())),
            vals: Some(builder.create_vector(val_vec.as_slice())),
        };
        let follow_unfollow =
            cf_kv_fbs::FollowUnfollow::create(&mut builder, &follow_unfollow_args);
        builder.finish(follow_unfollow, None);
        Ok(copy_into_buf(buf, &builder))
    }

    fn serialize_post_tweet(
        &self,
        buf: &mut [u8],
        keys: &Vec<&str>,
        values: &Vec<String>,
        _datapath: &D,
    ) -> Result<usize> {
        let mut builder = FlatBufferBuilder::new();
        let keys_vec: Vec<WIPOffset<&str>> =
            keys.iter().map(|key| builder.create_string(key)).collect();

        let val_vec_data: Vec<cf_kv_fbs::ValueArgs> = values
            .iter()
            .map(|val| cf_kv_fbs::ValueArgs {
                data: Some(builder.create_vector_direct::<u8>(&val.as_str().as_bytes())),
            })
            .collect();
        let val_vec: Vec<WIPOffset<cf_kv_fbs::Value>> = val_vec_data
            .iter()
            .map(|args| cf_kv_fbs::Value::create(&mut builder, args))
            .collect();
        let post_tweet_args = cf_kv_fbs::PostTweetArgs {
            keys: Some(builder.create_vector(keys_vec.as_slice())),
            vals: Some(builder.create_vector(val_vec.as_slice())),
        };
        let post_tweet = cf_kv_fbs::PostTweet::create(&mut builder, &post_tweet_args);
        builder.finish(post_tweet, None);
        Ok(copy_into_buf(buf, &builder))
    }

    fn serialize_get_timeline(
        &self,
        buf: &mut [u8],
        keys: &Vec<&str>,
        _values: &Vec<String>,
        _datapath: &D,
    ) -> Result<usize> {
        let mut builder = FlatBufferBuilder::new();
        let keys_vec: Vec<WIPOffset<&str>> =
            keys.iter().map(|key| builder.create_string(key)).collect();

        let get_timeline_args = cf_kv_fbs::GetTimelineArgs {
            keys: Some(builder.create_vector(keys_vec.as_slice())),
        };
        let get_timeline = cf_kv_fbs::GetTimeline::create(&mut builder, &get_timeline_args);
        builder.finish(get_timeline, None);
        Ok(copy_into_buf(buf, &builder))
    }
}

fn copy_into_buf<'fbb>(buf: &mut [u8], builder: &FlatBufferBuilder<'fbb>) -> usize {
    let data = builder.finished_data();
    let buf_to_copy = &mut buf[0..data.len()];
    buf_to_copy.copy_from_slice(data);
    data.len()
}
