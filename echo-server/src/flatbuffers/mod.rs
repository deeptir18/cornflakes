pub mod echo_messages {
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(dead_code)]
    #![allow(improper_ctypes)]
    #![allow(unused_imports)]
    include!(concat!(env!("OUT_DIR"), "/echo_fb_generated.rs"));
}
use super::{
    get_equal_fields, get_payloads_as_vec, init_payloads, init_payloads_as_vec, CerealizeClient,
    CerealizeMessage,
};
use color_eyre::eyre::Result;
use cornflakes_libos::{
    mem::MmapMetadata, CornPtr, Cornflake, Datapath, ReceivedPacket, ScatterGather,
};
use cornflakes_utils::{SimpleMessageType, TreeDepth};
use echo_messages::echo_fb;
use flatbuffers::{get_root, FlatBufferBuilder, WIPOffset};
use std::{marker::PhantomData, slice};

fn check_tree5l<'a>(indices: &[usize], payloads: &Vec<Vec<u8>>, object: &echo_fb::Tree5LFB<'a>) {
    check_tree4l(&indices[0..16], payloads, &object.left().unwrap());
    check_tree4l(&indices[16..32], payloads, &object.right().unwrap());
}

fn build_tree5l<'a>(builder: &mut FlatBufferBuilder<'a>, payloads: &Vec<&'a [u8]>) {
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
    payloads: &Vec<&'a [u8]>,
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

fn build_tree4l<'a>(builder: &mut FlatBufferBuilder<'a>, payloads: &Vec<&'a [u8]>) {
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
    payloads: &Vec<&'a [u8]>,
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

fn build_tree3l<'a>(builder: &mut FlatBufferBuilder<'a>, payloads: &Vec<&'a [u8]>) {
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
    payloads: &Vec<&'a [u8]>,
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

fn build_tree2l<'a>(builder: &mut FlatBufferBuilder<'a>, payloads: &Vec<&'a [u8]>) {
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
    payloads: &Vec<&'a [u8]>,
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

fn build_tree1l<'a>(builder: &mut FlatBufferBuilder<'a>, payloads: &Vec<&'a [u8]>) {
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
    payloads: &Vec<&'a [u8]>,
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
    payloads: &Vec<&'a [u8]>,
) -> Vec<echo_fb::SingleBufferFBArgs<'a>> {
    assert!(payloads.len() == u32::pow(2, depth as u32) as usize);
    let leaves: Vec<echo_fb::SingleBufferFBArgs> = payloads
        .iter()
        .map(|payload| echo_fb::SingleBufferFBArgs {
            message: Some(builder.create_vector_direct::<u8>(payload)),
        })
        .collect();
    leaves
}

fn context_size(message_type: SimpleMessageType, size: usize) -> usize {
    let payloads_vec = init_payloads_as_vec(&get_equal_fields(message_type, size));
    let payloads: Vec<&[u8]> = payloads_vec.iter().map(|vec| vec.as_slice()).collect();
    let mut builder = FlatBufferBuilder::new();

    match message_type {
        SimpleMessageType::Single => {
            assert!(payloads.len() == 1);
            let args = echo_fb::SingleBufferFBArgs {
                message: Some(builder.create_vector_direct::<u8>(&payloads[0])),
            };
            let single_buffer_fb = echo_fb::SingleBufferFB::create(&mut builder, &args);
            builder.finish(single_buffer_fb, None);
            builder.finished_data().len()
        }
        SimpleMessageType::List(list_size) => {
            assert!(payloads.len() == list_size);
            let mut vec: Vec<WIPOffset<echo_fb::SingleBufferFB>> = Vec::with_capacity(list_size);
            for payload in payloads.iter() {
                let args = echo_fb::SingleBufferFBArgs {
                    message: Some(builder.create_vector_direct::<u8>(&payload)),
                };
                vec.push(echo_fb::SingleBufferFB::create(&mut builder, &args));
            }
            let args = echo_fb::ListFBArgs {
                messages: Some(builder.create_vector(vec.as_slice())),
            };
            let list_fb = echo_fb::ListFB::create(&mut builder, &args);
            builder.finish(list_fb, None);
            builder.finished_data().len()
        }
        SimpleMessageType::Tree(depth) => match depth {
            TreeDepth::One => {
                build_tree1l(&mut builder, &payloads);
                builder.finished_data().len()
            }
            TreeDepth::Two => {
                build_tree2l(&mut builder, &payloads);
                builder.finished_data().len()
            }
            TreeDepth::Three => {
                build_tree3l(&mut builder, &payloads);
                builder.finished_data().len()
            }
            TreeDepth::Four => {
                build_tree4l(&mut builder, &payloads);
                builder.finished_data().len()
            }
            TreeDepth::Five => {
                build_tree5l(&mut builder, &payloads);
                builder.finished_data().len()
            }
        },
    }
}

pub struct FlatbuffersSerializer<'fbb> {
    message_type: SimpleMessageType,
    context_size: usize,
    _phantom_data: PhantomData<&'fbb [u8]>,
}

impl<'fbb> FlatbuffersSerializer<'fbb> {
    pub fn new(message_type: SimpleMessageType, size: usize) -> FlatbuffersSerializer<'fbb> {
        FlatbuffersSerializer {
            message_type: message_type,
            context_size: context_size(message_type, size),
            _phantom_data: PhantomData,
        }
    }
}

impl<'fbb, D> CerealizeMessage<D> for FlatbuffersSerializer<'fbb>
where
    D: Datapath,
{
    type Ctx = FlatBufferBuilder<'fbb>;

    fn message_type(&self) -> SimpleMessageType {
        self.message_type
    }

    fn process_msg<'registered, 'normal: 'registered>(
        &self,
        recved_msg: &'registered D::ReceivedPkt,
        ctx: &'normal mut Self::Ctx,
    ) -> Result<Cornflake<'registered, 'normal>> {
        let mut cf = Cornflake::with_capacity(1);
        match self.message_type {
            SimpleMessageType::Single => {
                let object_deser =
                    get_root::<echo_fb::SingleBufferFB>(&recved_msg.get_pkt_buffer());
                let args = echo_fb::SingleBufferFBArgs {
                    message: Some(ctx.create_vector_direct::<u8>(object_deser.message().unwrap())),
                };
                let single_buffer_fb = echo_fb::SingleBufferFB::create(ctx, &args);
                ctx.finish(single_buffer_fb, None);
                cf.add_entry(CornPtr::Normal(ctx.finished_data()));
            }
            SimpleMessageType::List(list_elts) => {
                let object_deser = get_root::<echo_fb::ListFB>(&recved_msg.get_pkt_buffer());
                let args_vec: Vec<echo_fb::SingleBufferFBArgs> = (0..list_elts)
                    .map(|idx| echo_fb::SingleBufferFBArgs {
                        message: Some(ctx.create_vector_direct::<u8>(
                            object_deser.messages().unwrap().get(idx).message().unwrap(),
                        )),
                    })
                    .collect();
                let vec: Vec<WIPOffset<echo_fb::SingleBufferFB>> = args_vec
                    .iter()
                    .map(|args| echo_fb::SingleBufferFB::create(ctx, args))
                    .collect();
                let list_args = echo_fb::ListFBArgs {
                    messages: Some(ctx.create_vector(vec.as_slice())),
                };
                let list_fb = echo_fb::ListFB::create(ctx, &list_args);
                ctx.finish(list_fb, None);
                cf.add_entry(CornPtr::Normal(ctx.finished_data()));
            }
            SimpleMessageType::Tree(depth) => match depth {
                TreeDepth::One => {
                    let object_deser = get_root::<echo_fb::Tree1LFB>(&recved_msg.get_pkt_buffer());
                    let args = get_tree1l_args_from_tree1l(ctx, vec![object_deser]);
                    let tree1l = echo_fb::Tree1LFB::create(ctx, &args[0]);
                    ctx.finish(tree1l, None);
                    cf.add_entry(CornPtr::Normal(ctx.finished_data()));
                }
                TreeDepth::Two => {
                    let object_deser = get_root::<echo_fb::Tree2LFB>(&recved_msg.get_pkt_buffer());
                    let args = get_tree2l_args_from_tree2l(ctx, vec![object_deser]);
                    let tree2l = echo_fb::Tree2LFB::create(ctx, &args[0]);
                    ctx.finish(tree2l, None);
                    cf.add_entry(CornPtr::Normal(ctx.finished_data()));
                }
                TreeDepth::Three => {
                    let object_deser = get_root::<echo_fb::Tree3LFB>(&recved_msg.get_pkt_buffer());
                    let args = get_tree3l_args_from_tree3l(ctx, vec![object_deser]);
                    let tree3l = echo_fb::Tree3LFB::create(ctx, &args[0]);
                    ctx.finish(tree3l, None);
                    cf.add_entry(CornPtr::Normal(ctx.finished_data()));
                }
                TreeDepth::Four => {
                    let object_deser = get_root::<echo_fb::Tree4LFB>(&recved_msg.get_pkt_buffer());
                    let args = get_tree4l_args_from_tree4l(ctx, vec![object_deser]);
                    let tree4l = echo_fb::Tree4LFB::create(ctx, &args[0]);
                    ctx.finish(tree4l, None);
                    cf.add_entry(CornPtr::Normal(ctx.finished_data()));
                }
                TreeDepth::Five => {
                    let object_deser = get_root::<echo_fb::Tree5LFB>(&recved_msg.get_pkt_buffer());
                    let args = get_tree5l_args_from_tree5l(ctx, vec![object_deser]);
                    let tree5l = echo_fb::Tree5LFB::create(ctx, &args[0]);
                    ctx.finish(tree5l, None);
                    cf.add_entry(CornPtr::Normal(ctx.finished_data()));
                }
            },
        }
        Ok(cf)
    }

    fn new_context(&self) -> Self::Ctx {
        FlatBufferBuilder::new_with_capacity(self.context_size)
    }
}

pub struct FlatbuffersEchoClient<'registered, 'normal> {
    message_type: SimpleMessageType,
    payload_ptrs: Vec<(*const u8, usize)>,
    sga: Cornflake<'registered, 'normal>,
    total_size: usize,
}

impl<'registered, 'normal, D> CerealizeClient<'normal, D>
    for FlatbuffersEchoClient<'registered, 'normal>
where
    D: Datapath,
{
    type Ctx = FlatBufferBuilder<'registered>;

    fn new(
        message_type: SimpleMessageType,
        field_sizes: Vec<usize>,
        mmap_metadata: MmapMetadata,
    ) -> Result<Self> {
        let payload_ptrs = init_payloads(&field_sizes, &mmap_metadata)?;

        let total_size: usize = payload_ptrs.iter().map(|(_, size)| *size).sum();
        Ok(FlatbuffersEchoClient {
            message_type: message_type,
            payload_ptrs: payload_ptrs,
            sga: Cornflake::default(),
            total_size: total_size,
        })
    }

    fn init(&mut self, ctx: &'normal mut Self::Ctx) -> Result<()> {
        let payloads: Vec<&'registered [u8]> = self
            .payload_ptrs
            .clone()
            .iter()
            .map(|(ptr, size)| unsafe { slice::from_raw_parts(*ptr, *size) })
            .collect();
        self.sga = Cornflake::with_capacity(1);

        match self.message_type {
            SimpleMessageType::Single => {
                assert!(payloads.len() == 1);
                let args = echo_fb::SingleBufferFBArgs {
                    message: Some(ctx.create_vector_direct::<u8>(&payloads[0])),
                };
                let single_buffer_fb = echo_fb::SingleBufferFB::create(ctx, &args);
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
                    vec.push(echo_fb::SingleBufferFB::create(ctx, &args));
                }
                let args = echo_fb::ListFBArgs {
                    messages: Some(ctx.create_vector(vec.as_slice())),
                };
                let list_fb = echo_fb::ListFB::create(ctx, &args);
                ctx.finish(list_fb, None);
            }
            SimpleMessageType::Tree(depth) => match depth {
                TreeDepth::One => {
                    build_tree1l(ctx, &payloads);
                }
                TreeDepth::Two => {
                    build_tree2l(ctx, &payloads);
                }
                TreeDepth::Three => {
                    build_tree3l(ctx, &payloads);
                }
                TreeDepth::Four => {
                    build_tree4l(ctx, &payloads);
                }
                TreeDepth::Five => {
                    build_tree5l(ctx, &payloads);
                }
            },
        }
        self.sga.add_entry(CornPtr::Normal(ctx.finished_data()));
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

    fn check_echoed_payload(&self, recved_msg: &D::ReceivedPkt) -> Result<()> {
        let our_payloads = get_payloads_as_vec(&self.payload_ptrs);

        match self.message_type {
            SimpleMessageType::Single => {
                assert!(our_payloads.len() == 1);
                let object_deser =
                    get_root::<echo_fb::SingleBufferFB>(&recved_msg.get_pkt_buffer());
                let bytes_vec = object_deser.message().unwrap().to_vec();
                assert!(bytes_vec.len() == our_payloads[0].len());
                assert!(bytes_vec == our_payloads[0]);
            }
            SimpleMessageType::List(list_size) => {
                assert!(our_payloads.len() == list_size);
                let object_deser = get_root::<echo_fb::ListFB>(&recved_msg.get_pkt_buffer());
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
            SimpleMessageType::Tree(depth) => match depth {
                TreeDepth::One => {
                    assert!(our_payloads.len() == 2);
                    let object_deser = get_root::<echo_fb::Tree1LFB>(&recved_msg.get_pkt_buffer());
                    let indices: Vec<usize> = (0usize..2usize).collect();
                    check_tree1l(indices.as_slice(), &our_payloads, &object_deser);
                }
                TreeDepth::Two => {
                    assert!(our_payloads.len() == 4);
                    let object_deser = get_root::<echo_fb::Tree2LFB>(&recved_msg.get_pkt_buffer());
                    let indices: Vec<usize> = (0usize..4usize).collect();
                    check_tree2l(indices.as_slice(), &our_payloads, &object_deser);
                }
                TreeDepth::Three => {
                    assert!(our_payloads.len() == 8);
                    let object_deser = get_root::<echo_fb::Tree3LFB>(&recved_msg.get_pkt_buffer());
                    let indices: Vec<usize> = (0usize..8usize).collect();
                    check_tree3l(indices.as_slice(), &our_payloads, &object_deser);
                }
                TreeDepth::Four => {
                    assert!(our_payloads.len() == 16);
                    let object_deser = get_root::<echo_fb::Tree4LFB>(&recved_msg.get_pkt_buffer());
                    let indices: Vec<usize> = (0usize..16usize).collect();
                    check_tree4l(indices.as_slice(), &our_payloads, &object_deser);
                }
                TreeDepth::Five => {
                    assert!(our_payloads.len() == 32);
                    let object_deser = get_root::<echo_fb::Tree5LFB>(&recved_msg.get_pkt_buffer());
                    let indices: Vec<usize> = (0usize..32usize).collect();
                    check_tree5l(indices.as_slice(), &our_payloads, &object_deser);
                }
            },
        }
        Ok(())
    }

    fn new_context(&self) -> Self::Ctx {
        FlatBufferBuilder::new_with_capacity(context_size(self.message_type, self.total_size))
    }
}
