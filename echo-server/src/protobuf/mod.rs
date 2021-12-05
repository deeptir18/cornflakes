pub mod echo_messages {
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(dead_code)]
    #![allow(improper_ctypes)]
    #![allow(unused_imports)]
    include!(concat!(env!("OUT_DIR"), "/echo_proto.rs"));
}
use super::{get_payloads_as_vec, init_payloads, CerealizeClient, CerealizeMessage};
use color_eyre::eyre::{Result, WrapErr};
use cornflakes_libos::{
    mem::MmapMetadata, CornPtr, Cornflake, Datapath, RcCornPtr, RcCornflake, ReceivedPkt,
    ScatterGather,
};
use cornflakes_utils::{SimpleMessageType, TreeDepth};
use protobuf::{CodedOutputStream, Message};
use std::slice;

fn check_tree5l(indices: &[usize], payloads: &Vec<Vec<u8>>, object: &echo_messages::Tree5LProto) {
    check_tree4l(&indices[0..16], payloads, &object.get_left());
    check_tree4l(&indices[16..32], payloads, &object.get_right());
}

fn check_tree4l(indices: &[usize], payloads: &Vec<Vec<u8>>, object: &echo_messages::Tree4LProto) {
    check_tree3l(&indices[0..8], payloads, &object.get_left());
    check_tree3l(&indices[8..16], payloads, &object.get_right());
}

fn check_tree3l(indices: &[usize], payloads: &Vec<Vec<u8>>, object: &echo_messages::Tree3LProto) {
    check_tree2l(&indices[0..4], payloads, &object.get_left());
    check_tree2l(&indices[4..8], payloads, &object.get_right());
}

fn check_tree2l(indices: &[usize], payloads: &Vec<Vec<u8>>, object: &echo_messages::Tree2LProto) {
    check_tree1l(&indices[0..2], payloads, &object.get_left());
    check_tree1l(&indices[2..4], payloads, &object.get_right());
}

fn check_tree1l(indices: &[usize], payloads: &Vec<Vec<u8>>, object: &echo_messages::Tree1LProto) {
    check_single_buffer(indices[0], payloads, &object.get_left());
    check_single_buffer(indices[1], payloads, &object.get_right());
}

fn check_single_buffer(
    idx: usize,
    payloads: &Vec<Vec<u8>>,
    object: &echo_messages::SingleBufferProto,
) {
    assert!(object.get_message().len() == payloads[idx].len());
    assert!(object.get_message().to_vec() == payloads[idx].clone());
}

fn get_tree_5l_message<'a>(
    indices: &[usize],
    payloads: &Vec<&'a [u8]>,
) -> echo_messages::Tree5LProto {
    let mut tree_5l = echo_messages::Tree5LProto::new();
    tree_5l.set_left(get_tree_4l_message(&indices[0..16], payloads));
    tree_5l.set_right(get_tree_4l_message(&indices[16..32], payloads));
    tree_5l
}

fn get_tree_4l_message<'a>(
    indices: &[usize],
    payloads: &Vec<&'a [u8]>,
) -> echo_messages::Tree4LProto {
    let mut tree_4l = echo_messages::Tree4LProto::new();
    tree_4l.set_left(get_tree_3l_message(&indices[0..8], payloads));
    tree_4l.set_right(get_tree_3l_message(&indices[8..16], payloads));
    tree_4l
}

fn get_tree_3l_message<'a>(
    indices: &[usize],
    payloads: &Vec<&'a [u8]>,
) -> echo_messages::Tree3LProto {
    let mut tree_3l = echo_messages::Tree3LProto::new();
    tree_3l.set_left(get_tree_2l_message(&indices[0..4], payloads));
    tree_3l.set_right(get_tree_2l_message(&indices[4..8], payloads));
    tree_3l
}

fn get_tree_2l_message<'a>(
    indices: &[usize],
    payloads: &Vec<&'a [u8]>,
) -> echo_messages::Tree2LProto {
    let mut tree_2l = echo_messages::Tree2LProto::new();
    tree_2l.set_left(get_tree_1l_message(&indices[0..2], payloads));
    tree_2l.set_right(get_tree_1l_message(&indices[2..4], payloads));
    tree_2l
}

fn get_tree_1l_message<'a>(
    indices: &[usize],
    payloads: &Vec<&'a [u8]>,
) -> echo_messages::Tree1LProto {
    let mut tree_1l = echo_messages::Tree1LProto::new();
    tree_1l.set_left(get_single_buffer_message(indices[0], payloads));
    tree_1l.set_right(get_single_buffer_message(indices[1], payloads));
    tree_1l
}

fn get_single_buffer_message<'a>(
    idx: usize,
    payloads: &Vec<&'a [u8]>,
) -> echo_messages::SingleBufferProto {
    let mut single_buffer_proto = echo_messages::SingleBufferProto::new();
    single_buffer_proto.set_message(payloads[idx].to_vec());
    single_buffer_proto
}

fn deserialize_tree5l(input: &echo_messages::Tree5LProto) -> echo_messages::Tree5LProto {
    let mut output = echo_messages::Tree5LProto::new();
    output.set_left(deserialize_tree4l(&input.get_left()));
    output.set_right(deserialize_tree4l(&input.get_right()));
    output
}

fn deserialize_tree4l(input: &echo_messages::Tree4LProto) -> echo_messages::Tree4LProto {
    let mut output = echo_messages::Tree4LProto::new();
    output.set_left(deserialize_tree3l(&input.get_left()));
    output.set_right(deserialize_tree3l(&input.get_right()));
    output
}

fn deserialize_tree3l(input: &echo_messages::Tree3LProto) -> echo_messages::Tree3LProto {
    let mut output = echo_messages::Tree3LProto::new();
    output.set_left(deserialize_tree2l(&input.get_left()));
    output.set_right(deserialize_tree2l(&input.get_right()));
    output
}

fn deserialize_tree2l(input: &echo_messages::Tree2LProto) -> echo_messages::Tree2LProto {
    let mut output = echo_messages::Tree2LProto::new();
    output.set_left(deserialize_tree1l(&input.get_left()));
    output.set_right(deserialize_tree1l(&input.get_right()));
    output
}

fn deserialize_tree1l(input: &echo_messages::Tree1LProto) -> echo_messages::Tree1LProto {
    let mut output = echo_messages::Tree1LProto::new();
    output.set_left(deserialize_single_buffer(&input.get_left()));
    output.set_right(deserialize_single_buffer(&input.get_right()));
    output
}

fn deserialize_single_buffer(
    input: &echo_messages::SingleBufferProto,
) -> echo_messages::SingleBufferProto {
    let mut single_buffer_proto = echo_messages::SingleBufferProto::new();
    single_buffer_proto.set_message(input.get_message().to_vec());
    single_buffer_proto
}

pub struct ProtobufSerializer {
    message_type: SimpleMessageType,
}

impl ProtobufSerializer {
    pub fn new(message_type: SimpleMessageType, _size: usize) -> ProtobufSerializer {
        ProtobufSerializer {
            message_type: message_type,
        }
    }
}

impl<D> CerealizeMessage<D> for ProtobufSerializer
where
    D: Datapath,
{
    type Ctx = Vec<u8>;

    fn message_type(&self) -> SimpleMessageType {
        self.message_type
    }

    fn process_msg<'registered>(
        &self,
        recved_msg: &'registered ReceivedPkt<D>,
        _conn: &mut D,
    ) -> Result<(Self::Ctx, RcCornflake<'registered, D>)> {
        let mut ctx = Vec::default();
        let mut output_stream = CodedOutputStream::vec(&mut ctx);
        match self.message_type {
            SimpleMessageType::Single => {
                let object_deser = echo_messages::SingleBufferProto::parse_from_bytes(
                    recved_msg.index(0).as_ref(),
                )
                .wrap_err("Failed to deserialize single buffer proto.")?;
                let mut object_ser = echo_messages::SingleBufferProto::new();
                object_ser.set_message(object_deser.get_message().to_vec());
                object_ser.write_to(&mut output_stream).wrap_err(
                    "Failed to write to context for single buffer proto serialization.",
                )?;
            }
            SimpleMessageType::List(_list_elts) => {
                let object_deser =
                    echo_messages::ListProto::parse_from_bytes(recved_msg.index(0).as_ref())
                        .wrap_err("Failed to deserialize list proto.")?;
                let mut object_ser = echo_messages::ListProto::new();
                let list = object_ser.mut_messages();
                for message in object_deser.get_messages().iter() {
                    list.push(message.to_vec());
                }
                object_ser
                    .write_to(&mut output_stream)
                    .wrap_err("Failed to write to context for list proto serialization.")?;
            }
            SimpleMessageType::Tree(depth) => match depth {
                TreeDepth::One => {
                    let object_deser =
                        echo_messages::Tree1LProto::parse_from_bytes(recved_msg.index(0).as_ref())
                            .wrap_err("Failed to deserialize Tree1LProto.")?;
                    deserialize_tree1l(&object_deser)
                        .write_to(&mut output_stream)
                        .wrap_err("Failed to write context for Tree1L serialization.")?;
                }
                TreeDepth::Two => {
                    let object_deser =
                        echo_messages::Tree2LProto::parse_from_bytes(recved_msg.index(0).as_ref())
                            .wrap_err("Failed to deserialize Tree1LProto.")?;
                    deserialize_tree2l(&object_deser)
                        .write_to(&mut output_stream)
                        .wrap_err("Failed to write context for Tree1L serialization.")?;
                }
                TreeDepth::Three => {
                    let object_deser =
                        echo_messages::Tree3LProto::parse_from_bytes(recved_msg.index(0).as_ref())
                            .wrap_err("Failed to deserialize Tree1LProto.")?;
                    deserialize_tree3l(&object_deser)
                        .write_to(&mut output_stream)
                        .wrap_err("Failed to write context for Tree1L serialization.")?;
                }
                TreeDepth::Four => {
                    let object_deser =
                        echo_messages::Tree4LProto::parse_from_bytes(recved_msg.index(0).as_ref())
                            .wrap_err("Failed to deserialize Tree1LProto.")?;
                    deserialize_tree4l(&object_deser)
                        .write_to(&mut output_stream)
                        .wrap_err("Failed to write context for Tree1L serialization.")?;
                }
                TreeDepth::Five => {
                    let object_deser =
                        echo_messages::Tree5LProto::parse_from_bytes(recved_msg.index(0).as_ref())
                            .wrap_err("Failed to deserialize Tree1LProto.")?;
                    deserialize_tree5l(&object_deser)
                        .write_to(&mut output_stream)
                        .wrap_err("Failed to write context for Tree1L serialization.")?;
                }
            },
        }
        output_stream
            .flush()
            .wrap_err("Failed to flush output stream.")?;
        Ok((ctx, RcCornflake::with_capacity(1)))
    }

    fn process_header<'registered>(
        &self,
        ctx: &'registered Self::Ctx,
        cornflake: &mut RcCornflake<'registered, D>,
    ) -> Result<()> {
        cornflake.add_entry(RcCornPtr::RawRef(ctx.as_slice()));
        Ok(())
    }

    fn new_context(&self) -> Self::Ctx {
        // TODO: could also try to directly allocate a packet buffer here, so a second copy into
        // registered memory isn't required
        Vec::default()
        //vec![0u8; self.context_size]
    }
}

pub struct ProtobufEchoClient<'registered, 'normal> {
    message_type: SimpleMessageType,
    payload_ptrs: Vec<(*const u8, usize)>,
    sga: Cornflake<'registered, 'normal>,
}

impl<'registered, 'normal, D> CerealizeClient<'normal, D>
    for ProtobufEchoClient<'registered, 'normal>
where
    D: Datapath,
{
    type Ctx = Vec<u8>;

    fn new(
        message_type: SimpleMessageType,
        field_sizes: Vec<usize>,
        mmap_metadata: MmapMetadata,
    ) -> Result<Self> {
        let payload_ptrs = init_payloads(&field_sizes, &mmap_metadata)?;
        let sga = Cornflake::default();

        Ok(ProtobufEchoClient {
            message_type: message_type,
            payload_ptrs: payload_ptrs,
            sga: sga,
        })
    }

    fn init(&mut self, ctx: &'normal mut Self::Ctx) -> Result<()> {
        let mut output_stream = CodedOutputStream::vec(ctx);
        self.sga = Cornflake::with_capacity(1);
        let payloads: Vec<&[u8]> = self
            .payload_ptrs
            .clone()
            .iter()
            .map(|(ptr, size)| unsafe { slice::from_raw_parts(*ptr, *size) })
            .collect();
        match self.message_type {
            SimpleMessageType::Single => {
                assert!(payloads.len() == 1);
                let mut single_buffer_proto = echo_messages::SingleBufferProto::new();
                single_buffer_proto.set_message(payloads[0].to_vec());
                single_buffer_proto
                    .write_to(&mut output_stream)
                    .wrap_err("Failed to serialize single buffer proto.")?;
            }
            SimpleMessageType::List(list_elts) => {
                assert!(payloads.len() == list_elts);
                let mut list_proto = echo_messages::ListProto::new();
                let list = list_proto.mut_messages();
                for payload in payloads.iter() {
                    list.push(payload.to_vec());
                }
                list_proto
                    .write_to(&mut output_stream)
                    .wrap_err("Failed to serialize list proto.")?;
            }
            SimpleMessageType::Tree(depth) => match depth {
                TreeDepth::One => {
                    assert!(payloads.len() == 2);
                    let tree_cf = get_tree_1l_message(&[0, 1], &payloads);
                    tree_cf
                        .write_to(&mut output_stream)
                        .wrap_err("Failed to serialize tree1l proto.")?;
                }
                TreeDepth::Two => {
                    assert!(payloads.len() == 4);
                    let tree_cf = get_tree_2l_message(&[0, 1, 2, 3, 4], &payloads);
                    tree_cf
                        .write_to(&mut output_stream)
                        .wrap_err("Failed to serialize tree2l proto.")?;
                }
                TreeDepth::Three => {
                    assert!(payloads.len() == 8);
                    let indices: Vec<usize> = (0usize..8usize).collect();
                    let tree_cf = get_tree_3l_message(indices.as_slice(), &payloads);
                    tree_cf
                        .write_to(&mut output_stream)
                        .wrap_err("Failed to serialize tree3l proto.")?;
                }
                TreeDepth::Four => {
                    assert!(payloads.len() == 16);
                    let indices: Vec<usize> = (0usize..16usize).collect();
                    let tree_cf = get_tree_4l_message(indices.as_slice(), &payloads);
                    tree_cf
                        .write_to(&mut output_stream)
                        .wrap_err("Failed to serialize tree4l proto.")?;
                }
                TreeDepth::Five => {
                    assert!(payloads.len() == 32);
                    let indices: Vec<usize> = (0usize..32usize).collect();
                    let tree_cf = get_tree_5l_message(indices.as_slice(), &payloads);
                    tree_cf
                        .write_to(&mut output_stream)
                        .wrap_err("Failed to serialize tree5l proto.")?;
                }
            },
        }
        output_stream
            .flush()
            .wrap_err("Failed to flush output stream.")?;
        self.sga.add_entry(CornPtr::Normal(ctx));
        Ok(())
    }

    fn message_type(&self) -> SimpleMessageType {
        self.message_type
    }

    fn payload_sizes(&self) -> Vec<usize> {
        self.payload_ptrs.iter().map(|(_ptr, len)| *len).collect()
    }

    fn get_msg(&self) -> Result<Vec<u8>> {
        Ok(self.sga.contiguous_repr())
    }

    fn check_echoed_payload(&self, recved_msg: &ReceivedPkt<D>) -> Result<()> {
        let our_payloads = get_payloads_as_vec(&self.payload_ptrs);
        match self.message_type {
            SimpleMessageType::Single => {
                let object_deser = echo_messages::SingleBufferProto::parse_from_bytes(
                    recved_msg.index(0).as_ref(),
                )
                .wrap_err("Failed to deserialize received packet into SingleBufferProto.")?;
                let bytes_vec = object_deser.get_message().to_vec();
                assert!(bytes_vec.len() == our_payloads[0].len());
                assert!(bytes_vec == our_payloads[0]);
            }
            SimpleMessageType::List(list_size) => {
                assert!(our_payloads.len() == list_size);
                let object_deser =
                    echo_messages::ListProto::parse_from_bytes(recved_msg.index(0).as_ref())
                        .wrap_err("Failed to deserialize received packet into ListProto.")?;
                assert!(object_deser.get_messages().len() == list_size);
                for (i, payload) in our_payloads.iter().enumerate() {
                    let bytes_vec = object_deser.get_messages()[i].to_vec();
                    assert!(bytes_vec == payload.clone());
                }
            }
            SimpleMessageType::Tree(depth) => match depth {
                TreeDepth::One => {
                    let object_deser =
                        echo_messages::Tree1LProto::parse_from_bytes(recved_msg.index(0).as_ref())
                            .wrap_err("Failed to deserialize received packet into Tree1LProto.")?;
                    check_tree1l(&[0, 1], &our_payloads, &object_deser);
                }
                TreeDepth::Two => {
                    let object_deser =
                        echo_messages::Tree2LProto::parse_from_bytes(recved_msg.index(0).as_ref())
                            .wrap_err("Failed to deserialize received packet into Tree2LProto.")?;
                    check_tree2l(&[0, 1, 2, 3], &our_payloads, &object_deser);
                }
                TreeDepth::Three => {
                    let indices: Vec<usize> = (0usize..8usize).collect();
                    let object_deser =
                        echo_messages::Tree3LProto::parse_from_bytes(recved_msg.index(0).as_ref())
                            .wrap_err("Failed to deserialize received packet into Tree3LProto.")?;
                    check_tree3l(indices.as_slice(), &our_payloads, &object_deser);
                }
                TreeDepth::Four => {
                    let indices: Vec<usize> = (0usize..16usize).collect();
                    let object_deser =
                        echo_messages::Tree4LProto::parse_from_bytes(recved_msg.index(0).as_ref())
                            .wrap_err("Failed to deserialize received packet into Tree4LProto.")?;
                    check_tree4l(indices.as_slice(), &our_payloads, &object_deser);
                }
                TreeDepth::Five => {
                    let indices: Vec<usize> = (0usize..32usize).collect();
                    let object_deser =
                        echo_messages::Tree5LProto::parse_from_bytes(recved_msg.index(0).as_ref())
                            .wrap_err("Failed to deserialize received packet into Tree5LProto.")?;
                    check_tree5l(indices.as_slice(), &our_payloads, &object_deser);
                }
            },
        }

        Ok(())
    }

    fn new_context(&self) -> Self::Ctx {
        Vec::default()
    }
}
