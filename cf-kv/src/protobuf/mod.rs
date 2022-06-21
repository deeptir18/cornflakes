pub mod kv_messages {
    #![allow(non_upper_case_globals)]
    #![allow(unused_mut)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(dead_code)]
    #![allow(improper_ctypes)]
    #![allow(unused_imports)]
    include!(concat!(env!("OUT_DIR"), "/kv_protobuf.rs"));
}

use super::{
    ClientSerializer, KVServer, ListKVServer, MsgType, ServerLoadGenerator, ZeroCopyPutKVServer,
    REQ_TYPE_SIZE,
};
use color_eyre::eyre::{bail, Result, WrapErr};
use cornflakes_libos::{
    allocator::MempoolID,
    datapath::{Datapath, PushBufType, ReceivedPkt},
    state_machine::server::ServerSM,
};
#[cfg(feature = "profiler")]
use perftools;
use protobuf::{CodedOutputStream, Message};
use std::marker::PhantomData;

pub struct ProtobufSerializer<D>
where
    D: Datapath,
{
    _phantom: PhantomData<D>,
    _zero_copy_puts: bool,
}

impl<D> ProtobufSerializer<D>
where
    D: Datapath,
{
    pub fn new(zero_copy_puts: bool) -> Self {
        ProtobufSerializer {
            _phantom: PhantomData::default(),
            _zero_copy_puts: zero_copy_puts,
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

    fn handle_put(
        &self,
        kv_server: &mut KVServer<D>,
        mempool_ids: &mut Vec<MempoolID>,
        pkt: &ReceivedPkt<D>,
        datapath: &mut D,
    ) -> Result<kv_messages::PutResp> {
        let put_request =
            kv_messages::PutReq::parse_from_bytes(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])
                .wrap_err("Failed to deserialize proto PutReq")?;
        kv_server.insert_with_copies(
            &put_request.key.as_str(),
            &put_request.val.as_slice(),
            datapath,
            mempool_ids,
        )?;
        let mut put_resp = kv_messages::PutResp::new();
        put_resp.id = put_request.id;
        Ok(put_resp)
    }

    fn handle_getm(
        &self,
        kv_server: &KVServer<D>,
        pkt: &ReceivedPkt<D>,
    ) -> Result<kv_messages::GetMResp> {
        let getm_request =
            kv_messages::GetMReq::parse_from_bytes(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])
                .wrap_err("Failed to deserialize proto GetMReq")?;
        let mut vals: Vec<Vec<u8>> = Vec::with_capacity(getm_request.keys.len());
        for key in getm_request.keys.iter() {
            let value = match kv_server.get(&key.as_str()) {
                Some(v) => v,
                None => {
                    bail!("Cannot find value for key in KV store: {:?}", &key.as_str(),);
                }
            };
            vals.push(value.as_ref().to_vec());
        }
        let mut getm_resp = kv_messages::GetMResp::new();
        getm_resp.vals = vals;
        Ok(getm_resp)
    }

    fn handle_putm(
        &self,
        kv_server: &mut KVServer<D>,
        mempool_ids: &mut Vec<MempoolID>,
        pkt: &ReceivedPkt<D>,
        datapath: &mut D,
    ) -> Result<kv_messages::PutResp> {
        let putm_request =
            kv_messages::PutMReq::parse_from_bytes(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])
                .wrap_err("Failed to deserialize proto PutMReq")?;
        for (key, value) in putm_request.keys.iter().zip(putm_request.vals.iter()) {
            kv_server.insert_with_copies(key.as_str(), value.as_slice(), datapath, mempool_ids)?;
        }
        let mut put_resp = kv_messages::PutResp::new();
        put_resp.id = putm_request.id;
        Ok(put_resp)
    }

    fn handle_getlist(
        &self,
        list_kv_server: &ListKVServer<D>,
        pkt: &ReceivedPkt<D>,
    ) -> Result<kv_messages::GetListResp> {
        let getlist_request =
            kv_messages::GetListReq::parse_from_bytes(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])
                .wrap_err("Failed to deserialize proto GetListReq")?;
        let values = match list_kv_server.get(&getlist_request.key) {
            Some(v) => v,
            None => {
                bail!(
                    "Cannot find values for key in KV store: {:?}",
                    getlist_request.key
                );
            }
        };
        let mut values_list: Vec<Vec<u8>> = Vec::with_capacity(values.len());
        for val in values.iter() {
            values_list.push(val.as_ref().to_vec());
        }
        let mut getlist_resp = kv_messages::GetListResp::new();
        getlist_resp.val_list = values_list;
        Ok(getlist_resp)
    }

    fn handle_putlist(
        &self,
        list_kv_server: &mut ListKVServer<D>,
        mempool_ids: &mut Vec<MempoolID>,
        pkt: &ReceivedPkt<D>,
        datapath: &mut D,
    ) -> Result<kv_messages::PutResp> {
        let putlist_request =
            kv_messages::PutListReq::parse_from_bytes(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])
                .wrap_err("Failed to deserialize proto PutListReq")?;
        let values_iter = putlist_request.vals.iter().map(|val| val.as_slice());
        list_kv_server.insert_with_copies(
            putlist_request.key.as_str(),
            values_iter,
            datapath,
            mempool_ids,
        )?;
        let mut put_resp = kv_messages::PutResp::new();
        put_resp.id = putlist_request.id;
        Ok(put_resp)
    }
}

pub struct ProtobufKVServer<D>
where
    D: Datapath,
{
    kv_server: KVServer<D>,
    list_kv_server: ListKVServer<D>,
    _zero_copy_put_kv_server: ZeroCopyPutKVServer<D>,

    mempool_ids: Vec<MempoolID>,
    serializer: ProtobufSerializer<D>,
    push_buf_type: PushBufType,
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
        zero_copy_puts: bool,
    ) -> Result<Self>
    where
        L: ServerLoadGenerator,
    {
        let (kv, list_kv, zero_copy_server, mempool_ids) =
            load_generator.new_kv_state(file, datapath, zero_copy_puts)?;
        Ok(ProtobufKVServer {
            kv_server: kv,
            list_kv_server: list_kv,
            _zero_copy_put_kv_server: zero_copy_server,
            mempool_ids: mempool_ids,
            push_buf_type: push_buf_type,
            serializer: ProtobufSerializer::new(zero_copy_puts),
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
            let message_type = MsgType::from_packet(&pkt)?;
            match message_type {
                MsgType::Get => {
                    let response = self.serializer.handle_get(&self.kv_server, &pkt)?;
                    datapath.queue_protobuf_message(
                        (pkt.msg_id(), pkt.conn_id(), &response),
                        i == (pkts_len - 1),
                    )?;
                }
                MsgType::GetM(_size) => {
                    let response = self.serializer.handle_getm(&self.kv_server, &pkt)?;
                    datapath.queue_protobuf_message(
                        (pkt.msg_id(), pkt.conn_id(), &response),
                        i == (pkts_len - 1),
                    )?;
                }
                MsgType::GetList(_size) => {
                    let response = self.serializer.handle_getlist(&self.list_kv_server, &pkt)?;
                    datapath.queue_protobuf_message(
                        (pkt.msg_id(), pkt.conn_id(), &response),
                        i == (pkts_len - 1),
                    )?;
                }
                MsgType::Put => {
                    let response = self.serializer.handle_put(
                        &mut self.kv_server,
                        &mut self.mempool_ids,
                        &pkt,
                        datapath,
                    )?;
                    datapath.queue_protobuf_message(
                        (pkt.msg_id(), pkt.conn_id(), &response),
                        i == (pkts_len - 1),
                    )?;
                }
                MsgType::PutM(_size) => {
                    let response = self.serializer.handle_putm(
                        &mut self.kv_server,
                        &mut self.mempool_ids,
                        &pkt,
                        datapath,
                    )?;
                    datapath.queue_protobuf_message(
                        (pkt.msg_id(), pkt.conn_id(), &response),
                        i == (pkts_len - 1),
                    )?;
                }
                MsgType::PutList(_size) => {
                    let response = self.serializer.handle_putlist(
                        &mut self.list_kv_server,
                        &mut self.mempool_ids,
                        &pkt,
                        datapath,
                    )?;
                    datapath.queue_protobuf_message(
                        (pkt.msg_id(), pkt.conn_id(), &response),
                        i == (pkts_len - 1),
                    )?;
                }
                MsgType::AddUser => {
                    let request = kv_messages::AddUser::parse_from_bytes(
                        &pkt.seg(0).as_ref()[REQ_TYPE_SIZE..],
                    )
                    .wrap_err("Failed to deserialize proto PutMReq")?;
                    let mut response = kv_messages::AddUserResponse::new();
                    let old_value = self.kv_server.get(&request.keys[0].as_str()).unwrap();
                    response.first_val = old_value.as_ref().to_vec();
                    for (key, value) in request.keys.iter().zip(request.vals.iter()) {
                        self.kv_server.insert_with_copies(
                            key.as_str(),
                            value.as_slice(),
                            datapath,
                            &mut self.mempool_ids,
                        )?;
                    }
                    datapath.queue_protobuf_message(
                        (pkt.msg_id(), pkt.conn_id(), &response),
                        i == (pkts_len - 1),
                    )?;
                }
                MsgType::FollowUnfollow => {
                    let request = kv_messages::FollowUnfollow::parse_from_bytes(
                        &pkt.seg(0).as_ref()[REQ_TYPE_SIZE..],
                    )
                    .wrap_err("Failed to deserialize proto PutMReq")?;
                    let mut response = kv_messages::FollowUnfollowResponse::new();
                    let mut list: Vec<Vec<u8>> = Vec::with_capacity(2);
                    for (key, value) in request.keys.iter().zip(request.vals.iter()) {
                        let old_value = self.kv_server.remove(&key.as_str()).unwrap();
                        self.kv_server.insert_with_copies(
                            key.as_str(),
                            value.as_slice(),
                            datapath,
                            &mut self.mempool_ids,
                        )?;
                        list.push(old_value.as_ref().to_vec());
                    }
                    response.original_vals = list;
                    datapath.queue_protobuf_message(
                        (pkt.msg_id(), pkt.conn_id(), &response),
                        i == (pkts_len - 1),
                    )?;
                }
                MsgType::PostTweet => {
                    let request = kv_messages::PostTweet::parse_from_bytes(
                        &pkt.seg(0).as_ref()[REQ_TYPE_SIZE..],
                    )
                    .wrap_err("Failed to deserialize proto PutMReq")?;
                    let mut response = kv_messages::PostTweetResponse::new();
                    let mut list: Vec<Vec<u8>> = Vec::with_capacity(2);
                    for (key, value) in request.keys.iter().zip(request.vals.iter()).take(3) {
                        let old_value = self.kv_server.remove(&key.as_str()).unwrap();
                        self.kv_server.insert_with_copies(
                            key.as_str(),
                            value.as_slice(),
                            datapath,
                            &mut self.mempool_ids,
                        )?;
                        list.push(old_value.as_ref().to_vec());
                    }
                    response.vals = list;
                    for (key, value) in request.keys.iter().zip(request.vals.iter()).skip(3).take(2)
                    {
                        self.kv_server.insert_with_copies(
                            key.as_str(),
                            value.as_slice(),
                            datapath,
                            &mut self.mempool_ids,
                        )?;
                    }
                    datapath.queue_protobuf_message(
                        (pkt.msg_id(), pkt.conn_id(), &response),
                        i == (pkts_len - 1),
                    )?;
                }
                MsgType::GetTimeline(_) => {
                    let request = kv_messages::GetTimeline::parse_from_bytes(
                        &pkt.seg(0).as_ref()[REQ_TYPE_SIZE..],
                    )
                    .wrap_err("Failed to deserialize proto PutMReq")?;
                    let mut response = kv_messages::GetTimelineResponse::new();
                    let mut list: Vec<Vec<u8>> = Vec::with_capacity(request.keys.len());
                    for key in request.keys.iter() {
                        let old_value = self.kv_server.get(&key.as_str()).unwrap();
                        list.push(old_value.as_ref().to_vec());
                    }
                    response.vals = list;
                    datapath.queue_protobuf_message(
                        (pkt.msg_id(), pkt.conn_id(), &response),
                        i == (pkts_len - 1),
                    )?;
                }
                MsgType::AppendToList(_size) => {
                    unimplemented!();
                }
            }
        }
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

    fn deserialize_get_response(&self, buf: &[u8]) -> Result<Vec<u8>> {
        let get_resp = kv_messages::GetResp::parse_from_bytes(buf)
            .wrap_err("Could not parse get_resp from message")?;
        return Ok(get_resp.val.clone());
    }

    fn deserialize_getm_response(&self, buf: &[u8]) -> Result<Vec<Vec<u8>>> {
        let getm_resp = kv_messages::GetMResp::parse_from_bytes(buf)
            .wrap_err("Could not parse get_resp from message")?;
        return Ok(getm_resp.vals.clone());
    }

    fn deserialize_getlist_response<'a>(&self, buf: &[u8]) -> Result<Vec<Vec<u8>>> {
        let getlist_resp = kv_messages::GetListResp::parse_from_bytes(buf)
            .wrap_err("Could not parse get_resp from message")?;
        return Ok(getlist_resp.val_list.clone());
    }

    fn check_add_user_num_values(&self, buf: &[u8]) -> Result<usize> {
        let add_user_resp = kv_messages::AddUserResponse::parse_from_bytes(buf)?;
        return Ok((add_user_resp.first_val.len() != 0) as usize);
    }

    fn check_follow_unfollow_num_values(&self, buf: &[u8]) -> Result<usize> {
        let follow_unfollow_response = kv_messages::FollowUnfollowResponse::parse_from_bytes(buf)?;
        return Ok(follow_unfollow_response.original_vals.len());
    }

    fn check_post_tweet_num_values(&self, buf: &[u8]) -> Result<usize> {
        let post_tweet_response = kv_messages::PostTweetResponse::parse_from_bytes(buf)?;
        return Ok(post_tweet_response.vals.len());
    }

    fn check_get_timeline_num_values(&self, buf: &[u8]) -> Result<usize> {
        let response = kv_messages::GetTimelineResponse::parse_from_bytes(buf)?;
        return Ok(response.vals.len());
    }

    fn check_retwis_response_num_values(&self, _buf: &[u8]) -> Result<usize> {
        unimplemented!();
    }

    fn serialize_get(&self, buf: &mut [u8], key: &str, _datapath: &D) -> Result<usize> {
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

    fn serialize_put(
        &self,
        buf: &mut [u8],
        key: &str,
        value: &str,
        _datapath: &D,
    ) -> Result<usize> {
        let mut output_stream = CodedOutputStream::bytes(buf);
        let mut put_req = kv_messages::PutReq::new();
        put_req.key = key.to_string();
        put_req.val = value.as_bytes().to_vec();
        put_req
            .write_to(&mut output_stream)
            .wrap_err("Failed to write into CodedOutputStream for PutReq proto")?;
        output_stream
            .flush()
            .wrap_err("Failed to flush output stream.")?;

        Ok(output_stream.total_bytes_written() as _)
    }

    fn serialize_getm(&self, buf: &mut [u8], keys: &Vec<String>, _datapath: &D) -> Result<usize> {
        let mut output_stream = CodedOutputStream::bytes(buf);
        let mut getm_req = kv_messages::GetMReq::new();
        getm_req.keys = keys.clone();
        getm_req
            .write_to(&mut output_stream)
            .wrap_err("Failed to write into CodedOutputStream for GetMReq proto")?;
        output_stream
            .flush()
            .wrap_err("Failed to flush output stream.")?;

        Ok(output_stream.total_bytes_written() as _)
    }

    fn serialize_putm(
        &self,
        buf: &mut [u8],
        keys: &Vec<String>,
        values: &Vec<String>,
        _datapath: &D,
    ) -> Result<usize> {
        let mut output_stream = CodedOutputStream::bytes(buf);
        let mut put_req = kv_messages::PutMReq::new();
        put_req.keys = keys.clone();
        put_req.vals = values
            .iter()
            .map(|x| x.as_str().as_bytes().to_vec())
            .collect::<Vec<Vec<u8>>>();
        put_req
            .write_to(&mut output_stream)
            .wrap_err("Failed to write into CodedOutputStream for PutReq proto")?;
        output_stream
            .flush()
            .wrap_err("Failed to flush output stream.")?;

        Ok(output_stream.total_bytes_written() as _)
    }

    fn serialize_get_list(&self, buf: &mut [u8], key: &str, _datapath: &D) -> Result<usize> {
        let mut output_stream = CodedOutputStream::bytes(buf);
        let mut get_req = kv_messages::GetListReq::new();
        get_req.key = key.to_string();
        get_req
            .write_to(&mut output_stream)
            .wrap_err("Failed to write into CodedOutputStream for GetListReq proto")?;
        output_stream
            .flush()
            .wrap_err("Failed to flush output stream.")?;

        Ok(output_stream.total_bytes_written() as _)
    }

    fn serialize_put_list(
        &self,
        buf: &mut [u8],
        key: &str,
        values: &Vec<String>,
        _datapath: &D,
    ) -> Result<usize> {
        let mut output_stream = CodedOutputStream::bytes(buf);
        let mut putlist_req = kv_messages::PutListReq::new();
        putlist_req.key = key.to_string();
        putlist_req.vals = values
            .iter()
            .map(|x| x.as_str().as_bytes().to_vec())
            .collect::<Vec<Vec<u8>>>();
        putlist_req
            .write_to(&mut output_stream)
            .wrap_err("Failed to write into CodedOutputStream for PutReq proto")?;
        output_stream
            .flush()
            .wrap_err("Failed to flush output stream.")?;

        Ok(output_stream.total_bytes_written() as _)
    }

    fn serialize_append(
        &self,
        _buf: &mut [u8],
        _key: &str,
        _value: &str,
        _datapath: &D,
    ) -> Result<usize> {
        Ok(0)
    }

    fn serialize_add_user(
        &self,
        buf: &mut [u8],
        keys: &Vec<&str>,
        values: &Vec<String>,
        _datapath: &D,
    ) -> Result<usize> {
        let mut output_stream = CodedOutputStream::bytes(buf);
        let mut add_user = kv_messages::AddUser::new();
        add_user.keys = keys
            .iter()
            .map(|key| key.to_string())
            .collect::<Vec<String>>();
        add_user.vals = values
            .iter()
            .map(|val| val.as_str().as_bytes().to_vec())
            .collect::<Vec<Vec<u8>>>();
        add_user
            .write_to(&mut output_stream)
            .wrap_err("Failed to write into CodedOutputStream for AddUser proto")?;
        output_stream
            .flush()
            .wrap_err("Failed to flush output stream.")?;

        Ok(output_stream.total_bytes_written() as _)
    }

    fn serialize_add_follow_unfollow(
        &self,
        buf: &mut [u8],
        keys: &Vec<&str>,
        values: &Vec<String>,
        _datapath: &D,
    ) -> Result<usize> {
        let mut output_stream = CodedOutputStream::bytes(buf);
        let mut follow_unfollow = kv_messages::FollowUnfollow::new();
        follow_unfollow.keys = keys
            .iter()
            .map(|key| key.to_string())
            .collect::<Vec<String>>();
        follow_unfollow.vals = values
            .iter()
            .map(|val| val.as_str().as_bytes().to_vec())
            .collect::<Vec<Vec<u8>>>();
        follow_unfollow
            .write_to(&mut output_stream)
            .wrap_err("Failed to write into CodedOutputStream for FollowUnfollow proto")?;
        output_stream
            .flush()
            .wrap_err("Failed to flush output stream.")?;

        Ok(output_stream.total_bytes_written() as _)
    }

    fn serialize_post_tweet(
        &self,
        buf: &mut [u8],
        keys: &Vec<&str>,
        values: &Vec<String>,
        _datapath: &D,
    ) -> Result<usize> {
        let mut output_stream = CodedOutputStream::bytes(buf);
        let mut post_tweet = kv_messages::PostTweet::new();
        post_tweet.keys = keys
            .iter()
            .map(|key| key.to_string())
            .collect::<Vec<String>>();

        post_tweet.vals = values
            .iter()
            .map(|val| val.as_str().as_bytes().to_vec())
            .collect::<Vec<Vec<u8>>>();
        post_tweet
            .write_to(&mut output_stream)
            .wrap_err("Failed to write into CodedOutputStream for PostTweet proto")?;
        output_stream
            .flush()
            .wrap_err("Failed to flush output stream.")?;

        Ok(output_stream.total_bytes_written() as _)
    }

    fn serialize_get_timeline(
        &self,
        buf: &mut [u8],
        keys: &Vec<&str>,
        _values: &Vec<String>,
        _datapath: &D,
    ) -> Result<usize> {
        let mut output_stream = CodedOutputStream::bytes(buf);
        let mut get_timeline = kv_messages::GetTimeline::new();
        get_timeline.keys = keys
            .iter()
            .map(|key| key.to_string())
            .collect::<Vec<String>>();
        get_timeline
            .write_to(&mut output_stream)
            .wrap_err("Failed to write into CodedOutputStream for GetTimeline proto")?;
        output_stream
            .flush()
            .wrap_err("Failed to flush output stream.")?;

        Ok(output_stream.total_bytes_written() as _)
    }
}
