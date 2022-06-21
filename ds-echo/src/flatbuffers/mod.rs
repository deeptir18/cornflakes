pub mod echo_api {
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(dead_code)]
    #![allow(improper_ctypes)]
    #![allow(unused_imports)]
    include!(concat!(env!("OUT_DIR"), "/echo_fb_generated.rs"));
}

use super::{read_message_type, ClientCerealizeMessage, REQ_TYPE_SIZE};
use color_eyre::eyre::{ensure, Result};
use cornflakes_libos::{
    datapath::{Datapath, PushBufType, ReceivedPkt},
    state_machine::server::ServerSM,
};
use cornflakes_utils::{SimpleMessageType, TreeDepth};
use echo_api::*;
use flatbuffers::{get_root, FlatBufferBuilder, WIPOffset};

#[cfg(feature = "profiler")]
use perftools;
use std::marker::PhantomData;

pub struct FlatbuffersSerializer<'fbb, D>
where
    D: Datapath,
{
    push_buf_type: PushBufType,
    _phantom_data: PhantomData<D>,
    _with_copy: bool,
    builder: FlatBufferBuilder<'fbb>,
}

impl<'fbb, D> FlatbuffersSerializer<'fbb, D>
where
    D: Datapath,
{
    pub fn new(push_buf_type: PushBufType) -> Self {
        FlatbuffersSerializer {
            push_buf_type: push_buf_type,
            _phantom_data: PhantomData::default(),
            _with_copy: false,
            builder: FlatBufferBuilder::new(),
        }
    }

    pub fn set_with_copy(&mut self) {}
}

impl<'fbb, D> ServerSM for FlatbuffersSerializer<'fbb, D>
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
                    let object_deser =
                        get_root::<echo_fb::SingleBufferFB>(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..]);
                    let args = echo_fb::SingleBufferFBArgs {
                        message: Some(
                            self.builder
                                .create_vector_direct::<u8>(object_deser.message().unwrap()),
                        ),
                    };
                    let single_buffer_fb =
                        echo_fb::SingleBufferFB::create(&mut self.builder, &args);
                    self.builder.finish(single_buffer_fb, None);
                }
                SimpleMessageType::List(list_elts) => {
                    let object_deser =
                        get_root::<echo_fb::ListFB>(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..]);
                    let args_vec: Vec<echo_fb::SingleBufferFBArgs> = (0..list_elts)
                        .map(|idx| echo_fb::SingleBufferFBArgs {
                            message: Some(self.builder.create_vector_direct::<u8>(
                                object_deser.messages().unwrap().get(idx).message().unwrap(),
                            )),
                        })
                        .collect();
                    let vec: Vec<WIPOffset<echo_fb::SingleBufferFB>> = args_vec
                        .iter()
                        .map(|args| echo_fb::SingleBufferFB::create(&mut self.builder, args))
                        .collect();
                    let list_args = echo_fb::ListFBArgs {
                        messages: Some(self.builder.create_vector(vec.as_slice())),
                    };
                    let list_fb = echo_fb::ListFB::create(&mut self.builder, &list_args);
                    self.builder.finish(list_fb, None);
                }
                SimpleMessageType::Tree(tree_depth) => match tree_depth {
                    TreeDepth::One => {
                        let object_deser =
                            get_root::<echo_fb::Tree1LFB>(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..]);
                        let args =
                            get_tree1l_args_from_tree1l(&mut self.builder, vec![object_deser]);
                        let tree1l = echo_fb::Tree1LFB::create(&mut self.builder, &args[0]);
                        self.builder.finish(tree1l, None);
                    }
                    TreeDepth::Two => {
                        let object_deser =
                            get_root::<echo_fb::Tree2LFB>(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..]);
                        let args =
                            get_tree2l_args_from_tree2l(&mut self.builder, vec![object_deser]);
                        let tree2l = echo_fb::Tree2LFB::create(&mut self.builder, &args[0]);
                        self.builder.finish(tree2l, None);
                    }
                    TreeDepth::Three => {
                        let object_deser =
                            get_root::<echo_fb::Tree3LFB>(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..]);
                        let args =
                            get_tree3l_args_from_tree3l(&mut self.builder, vec![object_deser]);
                        let tree3l = echo_fb::Tree3LFB::create(&mut self.builder, &args[0]);
                        self.builder.finish(tree3l, None);
                    }
                    TreeDepth::Four => {
                        let object_deser =
                            get_root::<echo_fb::Tree4LFB>(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..]);
                        let args =
                            get_tree4l_args_from_tree4l(&mut self.builder, vec![object_deser]);
                        let tree4l = echo_fb::Tree4LFB::create(&mut self.builder, &args[0]);
                        self.builder.finish(tree4l, None);
                    }
                    TreeDepth::Five => {
                        let object_deser =
                            get_root::<echo_fb::Tree5LFB>(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..]);
                        let args =
                            get_tree5l_args_from_tree5l(&mut self.builder, vec![object_deser]);
                        let tree5l = echo_fb::Tree5LFB::create(&mut self.builder, &args[0]);
                        self.builder.finish(tree5l, None);
                    }
                },
            }
            datapath.queue_single_buffer_with_copy(
                (pkt.msg_id(), pkt.conn_id(), &self.builder.finished_data()),
                i == pkts_len,
            )?;
        }
        Ok(())
    }
}

pub struct FlatbuffersEchoClient {}

impl<D> ClientCerealizeMessage<D> for FlatbuffersEchoClient
where
    D: Datapath,
{
    fn new() -> Self
    where
        Self: Sized,
    {
        FlatbuffersEchoClient {}
    }

    fn check_echoed_payload(
        &self,
        pkt: &ReceivedPkt<D>,
        bytes_to_check: (SimpleMessageType, &Vec<Vec<u8>>),
        _datapath: &D,
    ) -> Result<bool> {
        let (ty, our_payloads) = bytes_to_check;
        match ty {
            SimpleMessageType::Single => {
                assert!(our_payloads.len() == 1);
                let object_deser =
                    get_root::<echo_fb::SingleBufferFB>(&pkt.seg(0).as_ref().as_ref());
                let bytes_vec = object_deser.message().unwrap().to_vec();
                assert!(bytes_vec.len() == our_payloads[0].len());
                assert!(bytes_vec == our_payloads[0]);
            }
            SimpleMessageType::List(list_size) => {
                assert!(our_payloads.len() == list_size);
                let object_deser = get_root::<echo_fb::ListFB>(&pkt.seg(0).as_ref().as_ref());
                assert!(object_deser.messages().unwrap().len() == our_payloads.len());
                for (i, payload) in our_payloads.iter().enumerate() {
                    let bytes_vec = object_deser
                        .messages()
                        .unwrap()
                        .get(i)
                        .message()
                        .unwrap()
                        .to_vec();
                    assert!(bytes_vec.len() == payload.len());
                    assert!(bytes_vec == payload.clone());
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
                        assert!(our_payloads.len() == 2);
                        let object_deser =
                            get_root::<echo_fb::Tree1LFB>(&pkt.seg(0).as_ref().as_ref());
                        let indices: Vec<usize> = (0usize..2usize).collect();
                        check_tree1l(indices.as_slice(), &our_payloads, &object_deser);
                    }
                    TreeDepth::Two => {
                        assert!(our_payloads.len() == 4);
                        let object_deser =
                            get_root::<echo_fb::Tree2LFB>(&pkt.seg(0).as_ref().as_ref());
                        let indices: Vec<usize> = (0usize..4usize).collect();
                        check_tree2l(indices.as_slice(), &our_payloads, &object_deser);
                    }
                    TreeDepth::Three => {
                        assert!(our_payloads.len() == 8);
                        let object_deser =
                            get_root::<echo_fb::Tree3LFB>(&pkt.seg(0).as_ref().as_ref());
                        let indices: Vec<usize> = (0usize..8usize).collect();
                        check_tree3l(indices.as_slice(), &our_payloads, &object_deser);
                    }
                    TreeDepth::Four => {
                        assert!(our_payloads.len() == 16);
                        let object_deser =
                            get_root::<echo_fb::Tree4LFB>(&pkt.seg(0).as_ref().as_ref());
                        let indices: Vec<usize> = (0usize..16usize).collect();
                        check_tree4l(indices.as_slice(), &our_payloads, &object_deser);
                    }
                    TreeDepth::Five => {
                        assert!(our_payloads.len() == 32);
                        let object_deser =
                            get_root::<echo_fb::Tree5LFB>(&pkt.seg(0).as_ref().as_ref());
                        let indices: Vec<usize> = (0usize..32usize).collect();
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
        let mut ctx = FlatBufferBuilder::new();
        match ty {
            SimpleMessageType::Single => {
                assert!(payloads.len() == 1);
                let args = echo_fb::SingleBufferFBArgs {
                    message: Some(ctx.create_vector_direct::<u8>(&payloads[0].as_slice())),
                };
                let single_buffer_fb = echo_fb::SingleBufferFB::create(&mut ctx, &args);
                ctx.finish(single_buffer_fb, None);
            }
            SimpleMessageType::List(list_size) => {
                assert!(payloads.len() == list_size);
                let mut vec: Vec<WIPOffset<echo_fb::SingleBufferFB>> =
                    Vec::with_capacity(list_size);
                for payload in payloads.iter() {
                    let args = echo_fb::SingleBufferFBArgs {
                        message: Some(ctx.create_vector_direct::<u8>(&payload)),
                    };
                    vec.push(echo_fb::SingleBufferFB::create(&mut ctx, &args));
                }
                let args = echo_fb::ListFBArgs {
                    messages: Some(ctx.create_vector(vec.as_slice())),
                };
                let list_fb = echo_fb::ListFB::create(&mut ctx, &args);
                ctx.finish(list_fb, None);
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
                        build_tree1l(&mut ctx, &payloads);
                    }
                    TreeDepth::Two => {
                        build_tree2l(&mut ctx, &payloads);
                    }
                    TreeDepth::Three => {
                        build_tree3l(&mut ctx, &payloads);
                    }
                    TreeDepth::Four => {
                        build_tree4l(&mut ctx, &payloads);
                    }
                    TreeDepth::Five => {
                        build_tree5l(&mut ctx, &payloads);
                    }
                }
            }
        }
        let ret = ctx.finished_data().to_vec();
        Ok(ret)
    }
}

fn check_tree5l<'a>(indices: &[usize], payloads: &Vec<Vec<u8>>, object: &echo_fb::Tree5LFB<'a>) {
    check_tree4l(&indices[0..16], payloads, &object.left().unwrap());
    check_tree4l(&indices[16..32], payloads, &object.right().unwrap());
}

fn build_tree5l<'a>(builder: &mut FlatBufferBuilder<'a>, payloads: &Vec<Vec<u8>>) {
    let level5_args = get_tree5l_level_args_from_payloads_vec(5, builder, payloads);
    let tree5lfb = echo_fb::Tree5LFB::create(builder, &level5_args[0]);
    builder.finish(tree5lfb, None);
}
fn get_tree5l_args_from_tree5l<'a, 'b>(
    builder: &mut FlatBufferBuilder<'a>,
    input: Vec<echo_fb::Tree5LFB<'b>>,
) -> Vec<echo_fb::Tree5LFBArgs<'a>>
where
    'a: 'b,
{
    let input_tree4l_vec: Vec<echo_fb::Tree4LFB> = input
        .iter()
        .map(|tree4l| vec![tree4l.left().unwrap(), tree4l.right().unwrap()])
        .flatten()
        .collect();
    let tree4l_args = get_tree4l_args_from_tree4l(builder, input_tree4l_vec);
    let tree4l_offsets: Vec<WIPOffset<echo_fb::Tree4LFB>> = tree4l_args
        .iter()
        .map(|arg| echo_fb::Tree4LFB::create(builder, arg))
        .collect();
    let level5_args: Vec<echo_fb::Tree5LFBArgs> = (0..input.len())
        .map(|idx| echo_fb::Tree5LFBArgs {
            left: Some(tree4l_offsets[idx * 2]),
            right: Some(tree4l_offsets[idx * 2 + 1]),
        })
        .collect();
    level5_args
}

fn get_tree5l_level_args_from_payloads_vec<'a>(
    depth: usize,
    builder: &mut FlatBufferBuilder<'a>,
    payloads: &Vec<Vec<u8>>,
) -> Vec<echo_fb::Tree5LFBArgs<'a>> {
    let vec_size = u32::pow(2, depth as u32 - 5) as usize;
    let level4 = get_tree4l_level_args_from_payloads_vec(depth, builder, payloads);
    let level4_args: Vec<WIPOffset<echo_fb::Tree4LFB>> = level4
        .iter()
        .map(|arg| echo_fb::Tree4LFB::create(builder, arg))
        .collect();

    let level5_args: Vec<echo_fb::Tree5LFBArgs> = (0..vec_size)
        .map(|idx| echo_fb::Tree5LFBArgs {
            left: Some(level4_args[idx * 2]),
            right: Some(level4_args[idx * 2 + 1]),
        })
        .collect();
    level5_args
}

fn check_tree4l<'a>(indices: &[usize], payloads: &Vec<Vec<u8>>, object: &echo_fb::Tree4LFB<'a>) {
    check_tree3l(&indices[0..8], payloads, &object.left().unwrap());
    check_tree3l(&indices[8..16], payloads, &object.right().unwrap());
}

fn build_tree4l<'a>(builder: &mut FlatBufferBuilder<'a>, payloads: &Vec<Vec<u8>>) {
    let level4_args = get_tree4l_level_args_from_payloads_vec(4, builder, payloads);
    let tree4lfb = echo_fb::Tree4LFB::create(builder, &level4_args[0]);
    builder.finish(tree4lfb, None);
}

fn get_tree4l_args_from_tree4l<'a, 'b>(
    builder: &mut FlatBufferBuilder<'a>,
    input: Vec<echo_fb::Tree4LFB<'b>>,
) -> Vec<echo_fb::Tree4LFBArgs<'a>>
where
    'a: 'b,
{
    let input_tree3l_vec: Vec<echo_fb::Tree3LFB> = input
        .iter()
        .map(|tree3l| vec![tree3l.left().unwrap(), tree3l.right().unwrap()])
        .flatten()
        .collect();
    let tree3l_args = get_tree3l_args_from_tree3l(builder, input_tree3l_vec);
    let tree3l_offsets: Vec<WIPOffset<echo_fb::Tree3LFB>> = tree3l_args
        .iter()
        .map(|arg| echo_fb::Tree3LFB::create(builder, arg))
        .collect();
    let level4_args: Vec<echo_fb::Tree4LFBArgs> = (0..input.len())
        .map(|idx| echo_fb::Tree4LFBArgs {
            left: Some(tree3l_offsets[idx * 2]),
            right: Some(tree3l_offsets[idx * 2 + 1]),
        })
        .collect();
    level4_args
}

fn get_tree4l_level_args_from_payloads_vec<'a>(
    depth: usize,
    builder: &mut FlatBufferBuilder<'a>,
    payloads: &Vec<Vec<u8>>,
) -> Vec<echo_fb::Tree4LFBArgs<'a>> {
    let vec_size = u32::pow(2, depth as u32 - 4) as usize;
    let level3 = get_tree3l_level_args_from_payloads_vec(depth, builder, payloads);
    let level3_args: Vec<WIPOffset<echo_fb::Tree3LFB>> = level3
        .iter()
        .map(|arg| echo_fb::Tree3LFB::create(builder, arg))
        .collect();

    let level4_args: Vec<echo_fb::Tree4LFBArgs> = (0..vec_size)
        .map(|idx| echo_fb::Tree4LFBArgs {
            left: Some(level3_args[idx * 2]),
            right: Some(level3_args[idx * 2 + 1]),
        })
        .collect();
    level4_args
}

fn check_tree3l<'a>(indices: &[usize], payloads: &Vec<Vec<u8>>, object: &echo_fb::Tree3LFB<'a>) {
    check_tree2l(&indices[0..4], payloads, &object.left().unwrap());
    check_tree2l(&indices[4..8], payloads, &object.right().unwrap());
}

fn build_tree3l<'a>(builder: &mut FlatBufferBuilder<'a>, payloads: &Vec<Vec<u8>>) {
    let level3_args = get_tree3l_level_args_from_payloads_vec(3, builder, payloads);
    let tree3lfb = echo_fb::Tree3LFB::create(builder, &level3_args[0]);
    builder.finish(tree3lfb, None);
}

fn get_tree3l_args_from_tree3l<'a, 'b>(
    builder: &mut FlatBufferBuilder<'a>,
    input: Vec<echo_fb::Tree3LFB<'b>>,
) -> Vec<echo_fb::Tree3LFBArgs<'a>>
where
    'a: 'b,
{
    let input_tree2l_vec: Vec<echo_fb::Tree2LFB> = input
        .iter()
        .map(|tree3l| vec![tree3l.left().unwrap(), tree3l.right().unwrap()])
        .flatten()
        .collect();
    let tree2l_args = get_tree2l_args_from_tree2l(builder, input_tree2l_vec);
    let tree2l_offsets: Vec<WIPOffset<echo_fb::Tree2LFB>> = tree2l_args
        .iter()
        .map(|arg| echo_fb::Tree2LFB::create(builder, arg))
        .collect();
    let level3_args: Vec<echo_fb::Tree3LFBArgs> = (0..input.len())
        .map(|idx| echo_fb::Tree3LFBArgs {
            left: Some(tree2l_offsets[idx * 2]),
            right: Some(tree2l_offsets[idx * 2 + 1]),
        })
        .collect();
    level3_args
}

fn get_tree3l_level_args_from_payloads_vec<'a>(
    depth: usize,
    builder: &mut FlatBufferBuilder<'a>,
    payloads: &Vec<Vec<u8>>,
) -> Vec<echo_fb::Tree3LFBArgs<'a>> {
    let vec_size = u32::pow(2, depth as u32 - 3) as usize;
    let level2 = get_tree2l_level_args_from_payloads_vec(depth, builder, payloads);
    let level2_args: Vec<WIPOffset<echo_fb::Tree2LFB>> = level2
        .iter()
        .map(|arg| echo_fb::Tree2LFB::create(builder, arg))
        .collect();

    let level3_args: Vec<echo_fb::Tree3LFBArgs> = (0..vec_size)
        .map(|idx| echo_fb::Tree3LFBArgs {
            left: Some(level2_args[idx * 2]),
            right: Some(level2_args[idx * 2 + 1]),
        })
        .collect();
    level3_args
}

fn check_tree2l<'a>(indices: &[usize], payloads: &Vec<Vec<u8>>, object: &echo_fb::Tree2LFB<'a>) {
    check_tree1l(&indices[0..2], payloads, &object.left().unwrap());
    check_tree1l(&indices[2..4], payloads, &object.right().unwrap());
}

fn build_tree2l<'a>(builder: &mut FlatBufferBuilder<'a>, payloads: &Vec<Vec<u8>>) {
    let level2_args = get_tree2l_level_args_from_payloads_vec(2, builder, payloads);
    let tree2lfb = echo_fb::Tree2LFB::create(builder, &level2_args[0]);
    builder.finish(tree2lfb, None);
}

fn get_tree2l_args_from_tree2l<'a, 'b>(
    builder: &mut FlatBufferBuilder<'a>,
    input: Vec<echo_fb::Tree2LFB<'b>>,
) -> Vec<echo_fb::Tree2LFBArgs<'a>>
where
    'a: 'b,
{
    let input_tree1l_vec: Vec<echo_fb::Tree1LFB> = input
        .iter()
        .map(|tree2l| vec![tree2l.left().unwrap(), tree2l.right().unwrap()])
        .flatten()
        .collect();
    let tree1l_args = get_tree1l_args_from_tree1l(builder, input_tree1l_vec);
    let tree1l_offsets: Vec<WIPOffset<echo_fb::Tree1LFB>> = tree1l_args
        .iter()
        .map(|arg| echo_fb::Tree1LFB::create(builder, arg))
        .collect();
    let level2_args: Vec<echo_fb::Tree2LFBArgs> = (0..input.len())
        .map(|idx| echo_fb::Tree2LFBArgs {
            left: Some(tree1l_offsets[idx * 2]),
            right: Some(tree1l_offsets[idx * 2 + 1]),
        })
        .collect();
    level2_args
}

fn get_tree2l_level_args_from_payloads_vec<'a>(
    depth: usize,
    builder: &mut FlatBufferBuilder<'a>,
    payloads: &Vec<Vec<u8>>,
) -> Vec<echo_fb::Tree2LFBArgs<'a>> {
    let vec_size = u32::pow(2, depth as u32 - 2) as usize;
    let level1 = get_tree1l_level_args_from_payloads_vec(depth, builder, payloads);
    let level1_args: Vec<WIPOffset<echo_fb::Tree1LFB>> = level1
        .iter()
        .map(|arg| echo_fb::Tree1LFB::create(builder, arg))
        .collect();

    let level2_args: Vec<echo_fb::Tree2LFBArgs> = (0..vec_size)
        .map(|idx| echo_fb::Tree2LFBArgs {
            left: Some(level1_args[idx * 2]),
            right: Some(level1_args[idx * 2 + 1]),
        })
        .collect();
    level2_args
}

fn check_tree1l<'a>(indices: &[usize], payloads: &Vec<Vec<u8>>, object: &echo_fb::Tree1LFB<'a>) {
    check_single_buffer(indices[0], payloads, &object.left().unwrap());
    check_single_buffer(indices[1], payloads, &object.right().unwrap());
}

fn build_tree1l<'a>(builder: &mut FlatBufferBuilder<'a>, payloads: &Vec<Vec<u8>>) {
    let level1_args = get_tree1l_level_args_from_payloads_vec(1, builder, payloads);
    let tree1lfb = echo_fb::Tree1LFB::create(builder, &level1_args[0]);
    builder.finish(tree1lfb, None);
}

fn get_tree1l_args_from_tree1l<'a, 'b>(
    builder: &mut FlatBufferBuilder<'a>,
    input: Vec<echo_fb::Tree1LFB<'b>>,
) -> Vec<echo_fb::Tree1LFBArgs<'a>>
where
    'a: 'b,
{
    // for each tree1l, need to get the leaves
    let input_single_buffer_vec: Vec<echo_fb::SingleBufferFB> = input
        .iter()
        .map(|tree1l| vec![tree1l.left().unwrap(), tree1l.right().unwrap()])
        .flatten()
        .collect();
    let leaves = get_leaves_from_single_buffer_fb(builder, input_single_buffer_vec);
    let leaf_args: Vec<WIPOffset<echo_fb::SingleBufferFB>> = leaves
        .iter()
        .map(|leaf| echo_fb::SingleBufferFB::create(builder, leaf))
        .collect();
    let level1_args: Vec<echo_fb::Tree1LFBArgs> = (0..input.len())
        .map(|idx| echo_fb::Tree1LFBArgs {
            left: Some(leaf_args[idx * 2]),
            right: Some(leaf_args[idx * 2 + 1]),
        })
        .collect();
    level1_args
}

fn get_tree1l_level_args_from_payloads_vec<'a>(
    depth: usize,
    builder: &mut FlatBufferBuilder<'a>,
    payloads: &Vec<Vec<u8>>,
) -> Vec<echo_fb::Tree1LFBArgs<'a>> {
    let vec_size = u32::pow(2, depth as u32 - 1) as usize;
    let leaves = get_leaves_from_payloads_vec(depth, builder, payloads);
    let leaf_args: Vec<WIPOffset<echo_fb::SingleBufferFB>> = leaves
        .iter()
        .map(|leaf| echo_fb::SingleBufferFB::create(builder, leaf))
        .collect();

    let level1_args: Vec<echo_fb::Tree1LFBArgs> = (0..vec_size)
        .map(|idx| echo_fb::Tree1LFBArgs {
            left: Some(leaf_args[idx * 2]),
            right: Some(leaf_args[idx * 2 + 1]),
        })
        .collect();
    level1_args
}

fn check_single_buffer<'a>(
    index: usize,
    payloads: &Vec<Vec<u8>>,
    object: &echo_fb::SingleBufferFB<'a>,
) {
    assert!(object.message().unwrap().len() == payloads[index].len());
    assert!(object.message().unwrap().to_vec() == payloads[index].clone());
}

fn get_leaves_from_single_buffer_fb<'a, 'b>(
    builder: &mut FlatBufferBuilder<'a>,
    input: Vec<echo_fb::SingleBufferFB<'b>>,
) -> Vec<echo_fb::SingleBufferFBArgs<'a>>
where
    'a: 'b,
{
    let leaves: Vec<echo_fb::SingleBufferFBArgs> = input
        .iter()
        .map(|single_buffer_fb| echo_fb::SingleBufferFBArgs {
            message: Some(builder.create_vector_direct::<u8>(single_buffer_fb.message().unwrap())),
        })
        .collect();
    leaves
}

fn get_leaves_from_payloads_vec<'a>(
    depth: usize,
    builder: &mut FlatBufferBuilder<'a>,
    payloads: &Vec<Vec<u8>>,
) -> Vec<echo_fb::SingleBufferFBArgs<'a>> {
    assert!(payloads.len() == u32::pow(2, depth as u32) as usize);
    let leaves: Vec<echo_fb::SingleBufferFBArgs> = payloads
        .iter()
        .map(|payload| echo_fb::SingleBufferFBArgs {
            message: Some(builder.create_vector_direct::<u8>(payload.as_ref())),
        })
        .collect();
    leaves
}
