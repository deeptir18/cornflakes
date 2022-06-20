pub mod echo_messages {
    #![allow(non_upper_case_globals)]
    #![allow(unused_mut)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(dead_code)]
    #![allow(improper_ctypes)]
    #![allow(unused_imports)]
    include!(concat!(env!("OUT_DIR"), "/echo_proto.rs"));
}
use super::{read_message_type, ClientCerealizeMessage, REQ_TYPE_SIZE};
use color_eyre::eyre::{ensure, Result, WrapErr};
use cornflakes_libos::{
    datapath::{Datapath, PushBufType, ReceivedPkt},
    state_machine::server::ServerSM,
};
use cornflakes_utils::{SimpleMessageType, TreeDepth};
use protobuf::{Message, MessageField};
use std::marker::PhantomData;

pub struct ProtobufSerializer<D>
where
    D: Datapath,
{
    push_buf_type: PushBufType,
    _phantom_data: PhantomData<D>,
}

impl<D> ProtobufSerializer<D>
where
    D: Datapath,
{
    pub fn new(push_buf_type: PushBufType) -> Self {
        ProtobufSerializer {
            push_buf_type: push_buf_type,
            _phantom_data: PhantomData::default(),
        }
    }

    pub fn set_with_copy(&mut self) {}
}

impl<D> ServerSM for ProtobufSerializer<D>
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
            let msg_type = read_message_type(&pkt)?;
            match msg_type {
                SimpleMessageType::Single => {
                    let object_deser = echo_messages::SingleBufferProto::parse_from_bytes(
                        &pkt.seg(0).as_ref()[REQ_TYPE_SIZE..],
                    )
                    .wrap_err("Failed to deserialize single buffer proto.")?;
                    let mut object_ser = echo_messages::SingleBufferProto::new();
                    object_ser.message = object_deser.message.clone();
                    datapath.queue_protobuf_message(
                        (pkt.msg_id(), pkt.conn_id(), &object_ser),
                        i == (pkts_len - 1),
                    )?;
                }
                SimpleMessageType::List(_list_size) => {
                    let object_deser = echo_messages::ListProto::parse_from_bytes(
                        &pkt.seg(0).as_ref()[REQ_TYPE_SIZE..],
                    )
                    .wrap_err("Failed to deserialize list proto.")?;
                    let mut object_ser = echo_messages::ListProto::new();
                    let mut list: Vec<Vec<u8>> = Vec::with_capacity(object_deser.messages.len());
                    for message in object_deser.messages.iter() {
                        list.push(message.clone());
                    }
                    object_ser.messages = list;
                    datapath.queue_protobuf_message(
                        (pkt.msg_id(), pkt.conn_id(), &object_ser),
                        i == (pkts_len - 1),
                    )?;
                }
                SimpleMessageType::Tree(tree_depth) => match tree_depth {
                    TreeDepth::One => {
                        let object_deser = echo_messages::Tree1LProto::parse_from_bytes(
                            &pkt.seg(0).as_ref()[REQ_TYPE_SIZE..],
                        )
                        .wrap_err("Failed to deserialize Tree1LProto.")?;
                        let tree = deserialize_tree1l(&object_deser);
                        datapath.queue_protobuf_message(
                            (pkt.msg_id(), pkt.conn_id(), &tree),
                            i == (pkts_len - 1),
                        )?;
                    }
                    TreeDepth::Two => {
                        let object_deser = echo_messages::Tree2LProto::parse_from_bytes(
                            &pkt.seg(0).as_ref()[REQ_TYPE_SIZE..],
                        )
                        .wrap_err("Failed to deserialize Tree1LProto.")?;
                        let tree = deserialize_tree2l(&object_deser);
                        datapath.queue_protobuf_message(
                            (pkt.msg_id(), pkt.conn_id(), &tree),
                            i == (pkts_len - 1),
                        )?;
                    }
                    TreeDepth::Three => {
                        let object_deser = echo_messages::Tree3LProto::parse_from_bytes(
                            &pkt.seg(0).as_ref()[REQ_TYPE_SIZE..],
                        )
                        .wrap_err("Failed to deserialize Tree1LProto.")?;
                        let tree = deserialize_tree3l(&object_deser);
                        datapath.queue_protobuf_message(
                            (pkt.msg_id(), pkt.conn_id(), &tree),
                            i == (pkts_len - 1),
                        )?;
                    }
                    TreeDepth::Four => {
                        let object_deser = echo_messages::Tree4LProto::parse_from_bytes(
                            &pkt.seg(0).as_ref()[REQ_TYPE_SIZE..],
                        )
                        .wrap_err("Failed to deserialize Tree1LProto.")?;
                        let tree = deserialize_tree4l(&object_deser);
                        datapath.queue_protobuf_message(
                            (pkt.msg_id(), pkt.conn_id(), &tree),
                            i == (pkts_len - 1),
                        )?;
                    }
                    TreeDepth::Five => {
                        let object_deser = echo_messages::Tree5LProto::parse_from_bytes(
                            &pkt.seg(0).as_ref()[REQ_TYPE_SIZE..],
                        )
                        .wrap_err("Failed to deserialize Tree1LProto.")?;
                        let tree = deserialize_tree5l(&object_deser);
                        datapath.queue_protobuf_message(
                            (pkt.msg_id(), pkt.conn_id(), &tree),
                            i == (pkts_len - 1),
                        )?;
                    }
                },
            }
        }
        Ok(())
    }
}

pub struct ProtobufEchoClient {}

impl<D> ClientCerealizeMessage<D> for ProtobufEchoClient
where
    D: Datapath,
{
    fn new() -> Self
    where
        Self: Sized,
    {
        ProtobufEchoClient {}
    }

    fn check_echoed_payload(
        &self,
        pkt: &ReceivedPkt<D>,
        bytes_to_check: (SimpleMessageType, &Vec<Vec<u8>>),
    ) -> Result<bool> {
        let (ty, our_payloads) = bytes_to_check;
        match ty {
            SimpleMessageType::Single => {
                assert!(our_payloads.len() == 1);
                let object_deser =
                    echo_messages::SingleBufferProto::parse_from_bytes(&pkt.seg(0).as_ref())
                        .wrap_err(
                            "Failed to deserialize received packet into SingleBufferProto.",
                        )?;
                let bytes_vec = object_deser.message;
                assert!(bytes_vec.len() == our_payloads[0].len());
                assert!(bytes_vec == *our_payloads[0]);
            }
            SimpleMessageType::List(list_size) => {
                assert!(our_payloads.len() == list_size);
                let object_deser = echo_messages::ListProto::parse_from_bytes(pkt.seg(0).as_ref())
                    .wrap_err("Failed to deserialize received packet into ListProto.")?;
                assert!(object_deser.messages.len() == list_size);
                for (i, payload) in our_payloads.iter().enumerate() {
                    let bytes_vec = &object_deser.messages[i];
                    assert!(bytes_vec == payload);
                }
            }
            SimpleMessageType::Tree(depth) => {
                ensure!(
                    our_payloads.len() == u64::pow(2, depth.to_u32()) as usize,
                    format!(
                        "Expected bytes vec length {} for tree of depth {:?}",
                        u64::pow(2, depth.to_u32()),
                        depth
                    )
                );
                match depth {
                    TreeDepth::One => {
                        let object_deser =
                            echo_messages::Tree1LProto::parse_from_bytes(pkt.seg(0).as_ref())
                                .wrap_err(
                                    "Failed to deserialize received packet into Tree1LProto.",
                                )?;
                        check_tree1l(&[0, 1], &our_payloads, &object_deser);
                    }
                    TreeDepth::Two => {
                        let object_deser =
                            echo_messages::Tree2LProto::parse_from_bytes(pkt.seg(0).as_ref())
                                .wrap_err(
                                    "Failed to deserialize received packet into Tree2LProto.",
                                )?;
                        check_tree2l(&[0, 1, 2, 3], &our_payloads, &object_deser);
                    }
                    TreeDepth::Three => {
                        let indices: Vec<usize> = (0usize..8usize).collect();
                        let object_deser =
                            echo_messages::Tree3LProto::parse_from_bytes(pkt.seg(0).as_ref())
                                .wrap_err(
                                    "Failed to deserialize received packet into Tree3LProto.",
                                )?;
                        check_tree3l(indices.as_slice(), &our_payloads, &object_deser);
                    }
                    TreeDepth::Four => {
                        let indices: Vec<usize> = (0usize..16usize).collect();
                        let object_deser =
                            echo_messages::Tree4LProto::parse_from_bytes(pkt.seg(0).as_ref())
                                .wrap_err(
                                    "Failed to deserialize received packet into Tree4LProto.",
                                )?;
                        check_tree4l(indices.as_slice(), &our_payloads, &object_deser);
                    }
                    TreeDepth::Five => {
                        let indices: Vec<usize> = (0usize..32usize).collect();
                        let object_deser =
                            echo_messages::Tree5LProto::parse_from_bytes(pkt.seg(0).as_ref())
                                .wrap_err(
                                    "Failed to deserialize received packet into Tree5LProto.",
                                )?;
                        check_tree5l(indices.as_slice(), &our_payloads, &object_deser);
                    }
                }
            }
        }
        Ok(true)
    }

    fn get_serialized_bytes(
        ty: SimpleMessageType,
        payloads: &Vec<Vec<u8>>,
        _datapath: &D,
    ) -> Result<Vec<u8>> {
        let vec = match ty {
            SimpleMessageType::Single => {
                assert!(payloads.len() == 1);
                let mut object = echo_messages::SingleBufferProto::new();
                object.message = payloads[0].clone();
                object.write_to_bytes()
            }
            SimpleMessageType::List(list_size) => {
                assert!(payloads.len() == list_size);
                let mut object = echo_messages::ListProto::new();
                object.messages = payloads.clone();
                object.write_to_bytes()
            }
            SimpleMessageType::Tree(depth) => {
                ensure!(
                    payloads.len() == u64::pow(2, depth.to_u32()) as usize,
                    format!(
                        "Expected bytes vec length {} for tree of depth {:?}",
                        u64::pow(2, depth.to_u32()),
                        depth
                    )
                );
                match depth {
                    TreeDepth::One => {
                        assert!(payloads.len() == 2);
                        let tree = get_tree_1l_message(&[0, 1], &payloads);
                        tree.write_to_bytes()
                    }
                    TreeDepth::Two => {
                        assert!(payloads.len() == 4);
                        let indices: Vec<usize> = (0usize..4usize).collect();
                        let tree = get_tree_2l_message(&indices.as_slice(), &payloads);
                        tree.write_to_bytes()
                    }
                    TreeDepth::Three => {
                        assert!(payloads.len() == 8);
                        let indices: Vec<usize> = (0usize..8usize).collect();
                        let tree = get_tree_3l_message(&indices.as_slice(), &payloads);
                        tree.write_to_bytes()
                    }
                    TreeDepth::Four => {
                        assert!(payloads.len() == 16);
                        let indices: Vec<usize> = (0usize..16usize).collect();
                        let tree = get_tree_4l_message(&indices.as_slice(), &payloads);
                        tree.write_to_bytes()
                    }
                    TreeDepth::Five => {
                        assert!(payloads.len() == 32);
                        let indices: Vec<usize> = (0usize..32usize).collect();
                        let tree = get_tree_5l_message(&indices.as_slice(), &payloads);
                        tree.write_to_bytes()
                    }
                }
            }
        }?;
        Ok(vec)
    }
}

fn check_tree5l(indices: &[usize], payloads: &Vec<Vec<u8>>, object: &echo_messages::Tree5LProto) {
    check_tree4l(&indices[0..16], payloads, &object.left);
    check_tree4l(&indices[16..32], payloads, &object.right);
}

fn check_tree4l(indices: &[usize], payloads: &Vec<Vec<u8>>, object: &echo_messages::Tree4LProto) {
    check_tree3l(&indices[0..8], payloads, &object.left);
    check_tree3l(&indices[8..16], payloads, &object.right);
}

fn check_tree3l(indices: &[usize], payloads: &Vec<Vec<u8>>, object: &echo_messages::Tree3LProto) {
    check_tree2l(&indices[0..4], payloads, &object.left);
    check_tree2l(&indices[4..8], payloads, &object.right);
}

fn check_tree2l(indices: &[usize], payloads: &Vec<Vec<u8>>, object: &echo_messages::Tree2LProto) {
    check_tree1l(&indices[0..2], payloads, &object.left);
    check_tree1l(&indices[2..4], payloads, &object.right);
}

fn check_tree1l(indices: &[usize], payloads: &Vec<Vec<u8>>, object: &echo_messages::Tree1LProto) {
    check_single_buffer(indices[0], payloads, &object.left);
    check_single_buffer(indices[1], payloads, &object.right);
}

fn check_single_buffer(
    idx: usize,
    payloads: &Vec<Vec<u8>>,
    object: &echo_messages::SingleBufferProto,
) {
    assert!(object.message.len() == payloads[idx].len());
    assert!(object.message.to_vec() == payloads[idx].clone());
}

fn get_tree_5l_message<'a>(
    indices: &[usize],
    payloads: &Vec<Vec<u8>>,
) -> echo_messages::Tree5LProto {
    let mut tree_5l = echo_messages::Tree5LProto::new();
    tree_5l.left = MessageField::some(get_tree_4l_message(&indices[0..16], payloads));
    tree_5l.right = MessageField::some(get_tree_4l_message(&indices[16..32], payloads));
    tree_5l
}

fn get_tree_4l_message<'a>(
    indices: &[usize],
    payloads: &Vec<Vec<u8>>,
) -> echo_messages::Tree4LProto {
    let mut tree_4l = echo_messages::Tree4LProto::new();
    tree_4l.left = MessageField::some(get_tree_3l_message(&indices[0..8], payloads));
    tree_4l.right = MessageField::some(get_tree_3l_message(&indices[8..16], payloads));
    tree_4l
}

fn get_tree_3l_message<'a>(
    indices: &[usize],
    payloads: &Vec<Vec<u8>>,
) -> echo_messages::Tree3LProto {
    let mut tree_3l = echo_messages::Tree3LProto::new();
    tree_3l.left = MessageField::some(get_tree_2l_message(&indices[0..4], payloads));
    tree_3l.right = MessageField::some(get_tree_2l_message(&indices[4..8], payloads));
    tree_3l
}

fn get_tree_2l_message<'a>(
    indices: &[usize],
    payloads: &Vec<Vec<u8>>,
) -> echo_messages::Tree2LProto {
    let mut tree_2l = echo_messages::Tree2LProto::new();
    tree_2l.left = MessageField::some(get_tree_1l_message(&indices[0..2], payloads));
    tree_2l.right = MessageField::some(get_tree_1l_message(&indices[2..4], payloads));
    tree_2l
}

fn get_tree_1l_message<'a>(
    indices: &[usize],
    payloads: &Vec<Vec<u8>>,
) -> echo_messages::Tree1LProto {
    let mut tree_1l = echo_messages::Tree1LProto::new();
    tree_1l.left = MessageField::some(get_single_buffer_message(indices[0], payloads));
    tree_1l.right = MessageField::some(get_single_buffer_message(indices[1], payloads));
    tree_1l
}

fn get_single_buffer_message<'a>(
    idx: usize,
    payloads: &Vec<Vec<u8>>,
) -> echo_messages::SingleBufferProto {
    let mut single_buffer_proto = echo_messages::SingleBufferProto::new();
    single_buffer_proto.message = payloads[idx].clone();
    single_buffer_proto
}

fn deserialize_tree5l(input: &echo_messages::Tree5LProto) -> echo_messages::Tree5LProto {
    let mut output = echo_messages::Tree5LProto::new();
    output.left = MessageField::some(deserialize_tree4l(&input.left));
    output.right = MessageField::some(deserialize_tree4l(&input.right));
    output
}

fn deserialize_tree4l(input: &echo_messages::Tree4LProto) -> echo_messages::Tree4LProto {
    let mut output = echo_messages::Tree4LProto::new();
    output.left = MessageField::some(deserialize_tree3l(&input.left));
    output.right = MessageField::some(deserialize_tree3l(&input.right));
    output
}

fn deserialize_tree3l(input: &echo_messages::Tree3LProto) -> echo_messages::Tree3LProto {
    let mut output = echo_messages::Tree3LProto::new();
    output.left = MessageField::some(deserialize_tree2l(&input.left));
    output.right = MessageField::some(deserialize_tree2l(&input.right));
    output
}

fn deserialize_tree2l(input: &echo_messages::Tree2LProto) -> echo_messages::Tree2LProto {
    let mut output = echo_messages::Tree2LProto::new();
    output.left = MessageField::some(deserialize_tree1l(&input.left));
    output.right = MessageField::some(deserialize_tree1l(&input.right));
    output
}

fn deserialize_tree1l(input: &echo_messages::Tree1LProto) -> echo_messages::Tree1LProto {
    let mut output = echo_messages::Tree1LProto::new();
    output.left = MessageField::some(deserialize_single_buffer(&input.left));
    output.right = MessageField::some(deserialize_single_buffer(&input.right));
    output
}

fn deserialize_single_buffer(
    input: &echo_messages::SingleBufferProto,
) -> echo_messages::SingleBufferProto {
    let mut single_buffer_proto = echo_messages::SingleBufferProto::new();
    single_buffer_proto.message = input.message.clone();
    single_buffer_proto
}
