use super::{
    ds_query_capnp, get_equal_fields, get_payloads_as_vec, init_payloads, init_payloads_as_vec,
    CerealizeClient, CerealizeMessage,
};
use byteorder::{ByteOrder, LittleEndian};

use capnp::message::{
    Allocator, Builder, HeapAllocator, Reader, ReaderOptions, ReaderSegments, SegmentArray,
};
use color_eyre::eyre::{Result, WrapErr};
use cornflakes_libos::{mem::MmapMetadata, CornPtr, Cornflake, Datapath, ReceivedPacket};
use cornflakes_utils::{SimpleMessageType, TreeDepth};
use std::slice;

const FRAMING_ENTRY_SIZE: usize = 8;

fn check_tree5l(
    indices: &[usize],
    payloads: &Vec<Vec<u8>>,
    tree5l: ds_query_capnp::tree5_l_c_p::Reader,
) -> Result<()> {
    check_tree4l(
        &indices[0..16],
        payloads,
        tree5l
            .get_left()
            .wrap_err("Not able to call get_left() on tree5l.")?,
    )?;
    check_tree4l(
        &indices[16..32],
        payloads,
        tree5l
            .get_right()
            .wrap_err("Not able to call get_right() on tree5l.")?,
    )?;
    Ok(())
}

fn deserialize_tree5l(
    input: ds_query_capnp::tree5_l_c_p::Reader,
    output: &mut ds_query_capnp::tree5_l_c_p::Builder,
) -> Result<()> {
    {
        let mut left = output.reborrow().init_left();
        deserialize_tree4l(
            input
                .get_left()
                .wrap_err("Unable to run get_left() on input tree5l")?,
            &mut left,
        )?;
    }
    let mut right = output.reborrow().init_right();
    deserialize_tree4l(
        input
            .get_right()
            .wrap_err("Unable to run get_left() on input tree5l")?,
        &mut right,
    )?;
    Ok(())
}

fn build_tree5l<'a>(
    tree5l: &mut ds_query_capnp::tree5_l_c_p::Builder,
    indices: &[usize],
    payloads: &Vec<&'a [u8]>,
) {
    {
        let mut left = tree5l.reborrow().init_left();
        build_tree4l(&mut left, &indices[0..16], payloads);
    }
    let mut right = tree5l.reborrow().init_right();
    build_tree4l(&mut right, &indices[16..32], payloads);
}

fn check_tree4l(
    indices: &[usize],
    payloads: &Vec<Vec<u8>>,
    tree4l: ds_query_capnp::tree4_l_c_p::Reader,
) -> Result<()> {
    check_tree3l(
        &indices[0..8],
        payloads,
        tree4l
            .get_left()
            .wrap_err("Not able to call get_left() on tree4l.")?,
    )?;
    check_tree3l(
        &indices[8..16],
        payloads,
        tree4l
            .get_right()
            .wrap_err("Not able to call get_right() on tree4l.")?,
    )?;
    Ok(())
}

fn deserialize_tree4l(
    input: ds_query_capnp::tree4_l_c_p::Reader,
    output: &mut ds_query_capnp::tree4_l_c_p::Builder,
) -> Result<()> {
    {
        let mut left = output.reborrow().init_left();
        deserialize_tree3l(
            input
                .get_left()
                .wrap_err("Unable to run get_left() on input tree4l")?,
            &mut left,
        )?;
    }
    let mut right = output.reborrow().init_right();
    deserialize_tree3l(
        input
            .get_right()
            .wrap_err("Unable to run get_left() on input tree4l")?,
        &mut right,
    )?;
    Ok(())
}

fn build_tree4l<'a>(
    tree4l: &mut ds_query_capnp::tree4_l_c_p::Builder,
    indices: &[usize],
    payloads: &Vec<&'a [u8]>,
) {
    {
        let mut left = tree4l.reborrow().init_left();
        build_tree3l(&mut left, &indices[0..8], payloads);
    }
    let mut right = tree4l.reborrow().init_right();
    build_tree3l(&mut right, &indices[8..16], payloads);
}

fn check_tree3l(
    indices: &[usize],
    payloads: &Vec<Vec<u8>>,
    tree3l: ds_query_capnp::tree3_l_c_p::Reader,
) -> Result<()> {
    check_tree2l(
        &indices[0..4],
        payloads,
        tree3l
            .get_left()
            .wrap_err("Not able to call get_left() on tree3l.")?,
    )?;
    check_tree2l(
        &indices[4..8],
        payloads,
        tree3l
            .get_right()
            .wrap_err("Not able to call get_right() on tree3l.")?,
    )?;
    Ok(())
}

fn deserialize_tree3l(
    input: ds_query_capnp::tree3_l_c_p::Reader,
    output: &mut ds_query_capnp::tree3_l_c_p::Builder,
) -> Result<()> {
    {
        let mut left = output.reborrow().init_left();
        deserialize_tree2l(
            input
                .get_left()
                .wrap_err("Unable to run get_left() on input tree3l")?,
            &mut left,
        )?;
    }
    let mut right = output.reborrow().init_right();
    deserialize_tree2l(
        input
            .get_right()
            .wrap_err("Unable to run get_left() on input tree3l")?,
        &mut right,
    )?;
    Ok(())
}

fn build_tree3l<'a>(
    tree3l: &mut ds_query_capnp::tree3_l_c_p::Builder,
    indices: &[usize],
    payloads: &Vec<&'a [u8]>,
) {
    {
        let mut left = tree3l.reborrow().init_left();
        build_tree2l(&mut left, &indices[0..4], payloads);
    }
    let mut right = tree3l.reborrow().init_right();
    build_tree2l(&mut right, &indices[4..8], payloads);
}

fn check_tree2l(
    indices: &[usize],
    payloads: &Vec<Vec<u8>>,
    tree2l: ds_query_capnp::tree2_l_c_p::Reader,
) -> Result<()> {
    check_tree1l(
        &indices[0..2],
        payloads,
        tree2l
            .get_left()
            .wrap_err("Not able to call get_left() on tree2l.")?,
    )?;
    check_tree1l(
        &indices[2..4],
        payloads,
        tree2l
            .get_right()
            .wrap_err("Not able to call get_right() on tree2l.")?,
    )?;
    Ok(())
}

fn deserialize_tree2l(
    input: ds_query_capnp::tree2_l_c_p::Reader,
    output: &mut ds_query_capnp::tree2_l_c_p::Builder,
) -> Result<()> {
    {
        let mut left = output.reborrow().init_left();
        deserialize_tree1l(
            input
                .get_left()
                .wrap_err("Unable to run get_left() on input tree2l")?,
            &mut left,
        )?;
    }
    let mut right = output.reborrow().init_right();
    deserialize_tree1l(
        input
            .get_right()
            .wrap_err("Unable to run get_left() on input tree2l")?,
        &mut right,
    )?;
    Ok(())
}

fn build_tree2l<'a>(
    tree2l: &mut ds_query_capnp::tree2_l_c_p::Builder,
    indices: &[usize],
    payloads: &Vec<&'a [u8]>,
) {
    {
        let mut left = tree2l.reborrow().init_left();
        build_tree1l(&mut left, &indices[0..2], payloads);
    }
    let mut right = tree2l.reborrow().init_right();
    build_tree1l(&mut right, &indices[2..4], payloads);
}

fn check_tree1l(
    indices: &[usize],
    payloads: &Vec<Vec<u8>>,
    tree1l: ds_query_capnp::tree1_l_c_p::Reader,
) -> Result<()> {
    check_single_buffer(
        indices[0],
        payloads,
        tree1l
            .get_left()
            .wrap_err("Not able to call get_left() on tree1l.")?,
    )?;
    check_single_buffer(
        indices[1],
        payloads,
        tree1l
            .get_right()
            .wrap_err("Not able to call get_right() on tree1l.")?,
    )?;
    Ok(())
}

fn deserialize_tree1l(
    input: ds_query_capnp::tree1_l_c_p::Reader,
    output: &mut ds_query_capnp::tree1_l_c_p::Builder,
) -> Result<()> {
    {
        let mut left = output.reborrow().init_left();
        deserialize_single_buffer(
            input
                .get_left()
                .wrap_err("Unable to run get_left() on input tree1l")?,
            &mut left,
        )?;
    }
    let mut right = output.reborrow().init_right();
    deserialize_single_buffer(
        input
            .get_right()
            .wrap_err("Unable to run get_left() on input tree1l")?,
        &mut right,
    )?;
    Ok(())
}

fn build_tree1l<'a>(
    tree1l: &mut ds_query_capnp::tree1_l_c_p::Builder,
    indices: &[usize],
    payloads: &Vec<&'a [u8]>,
) {
    {
        let mut left = tree1l.reborrow().init_left();
        build_single_buffer_message(&mut left, indices[0], payloads);
    }
    let mut right = tree1l.reborrow().init_right();
    build_single_buffer_message(&mut right, indices[1], payloads);
}

fn check_single_buffer(
    index: usize,
    payloads: &Vec<Vec<u8>>,
    single_buffer_cp: ds_query_capnp::single_buffer_c_p::Reader,
) -> Result<()> {
    let buf = single_buffer_cp
        .get_message()
        .wrap_err("Unable to run get_message() on single_buffer_c_p")?;
    assert!(buf.len() == payloads[index].len());
    assert!(buf.to_vec() == payloads[index]);
    Ok(())
}

fn deserialize_single_buffer(
    input: ds_query_capnp::single_buffer_c_p::Reader,
    output: &mut ds_query_capnp::single_buffer_c_p::Builder,
) -> Result<()> {
    output.set_message(
        input
            .get_message()
            .wrap_err("Failed to run get_message() on input SingleBufferCP")?,
    );
    Ok(())
}

fn read_context<'registered, T>(recved_msg: &'registered T) -> Vec<&'registered [u8]>
where
    T: ReceivedPacket,
{
    assert!(recved_msg.len() >= FRAMING_ENTRY_SIZE);
    let num_segments = LittleEndian::read_u32(&recved_msg.get_pkt_buffer()[0..4]) as usize;
    assert!(recved_msg.len() >= FRAMING_ENTRY_SIZE + num_segments * FRAMING_ENTRY_SIZE);
    let mut size_so_far = FRAMING_ENTRY_SIZE + num_segments * FRAMING_ENTRY_SIZE;
    let mut segments: Vec<&'registered [u8]> = Vec::default();
    for i in 0..num_segments {
        let cur_idx = FRAMING_ENTRY_SIZE + i * FRAMING_ENTRY_SIZE;
        let offset =
            LittleEndian::read_u32(&recved_msg.get_pkt_buffer()[cur_idx..(cur_idx + 4)]) as usize;
        let size =
            LittleEndian::read_u32(&recved_msg.get_pkt_buffer()[(cur_idx + 4)..(cur_idx + 8)])
                as usize;
        assert!(recved_msg.len() >= (size_so_far + size));
        segments.push(&recved_msg.get_pkt_buffer()[offset..(offset + size)]);
        size_so_far += size;
    }
    segments
}

fn fill_in_context<T>(framing: &mut Vec<u8>, builder: &Builder<T>)
where
    T: Allocator,
{
    let segments = builder.get_segments_for_output();
    // write number of segments into the head
    let mut cur_idx = 0;
    LittleEndian::write_u32(&mut framing[cur_idx..(cur_idx + 4)], segments.len() as u32);
    cur_idx += 8;
    let mut cur_offset = (segments.len() + 1) * FRAMING_ENTRY_SIZE;
    for seg in segments.iter() {
        LittleEndian::write_u32(&mut framing[cur_idx..(cur_idx + 4)], cur_offset as u32);
        cur_idx += 4;
        LittleEndian::write_u32(&mut framing[cur_idx..(cur_idx + 4)], seg.len() as u32);
        cur_idx += 4;
        cur_offset += seg.len();
    }
}

fn context_framing_size(message_type: SimpleMessageType, size: usize) -> usize {
    let payloads_vec = init_payloads_as_vec(&get_equal_fields(message_type, size));
    let payloads: Vec<&[u8]> = payloads_vec.iter().map(|vec| vec.as_slice()).collect();
    let mut builder = Builder::new_default();
    match message_type {
        SimpleMessageType::Single => {
            assert!(payloads.len() == 1);
            let mut object = builder.init_root::<ds_query_capnp::single_buffer_c_p::Builder>();
            object.set_message(payloads[0]);
        }
        SimpleMessageType::List(list_size) => {
            assert!(payloads.len() == list_size);
            let object = builder.init_root::<ds_query_capnp::list_c_p::Builder>();
            let mut list = object.init_messages(list_size as u32);
            for (i, payload) in payloads.iter().enumerate() {
                list.set(i as u32, payload);
            }
        }
        SimpleMessageType::Tree(depth) => match depth {
            TreeDepth::One => {
                assert!(payloads.len() == 2);
                let mut tree1l = builder.init_root::<ds_query_capnp::tree1_l_c_p::Builder>();
                build_tree1l(&mut tree1l, &[0, 1], &payloads);
            }
            TreeDepth::Two => {
                assert!(payloads.len() == 4);
                let mut tree2l = builder.init_root::<ds_query_capnp::tree2_l_c_p::Builder>();
                let indices: Vec<usize> = (0usize..4usize).collect();
                build_tree2l(&mut tree2l, indices.as_slice(), &payloads);
            }
            TreeDepth::Three => {
                assert!(payloads.len() == 8);
                let mut tree3l = builder.init_root::<ds_query_capnp::tree3_l_c_p::Builder>();
                let indices: Vec<usize> = (0usize..8usize).collect();
                build_tree3l(&mut tree3l, indices.as_slice(), &payloads);
            }
            TreeDepth::Four => {
                assert!(payloads.len() == 16);
                let mut tree4l = builder.init_root::<ds_query_capnp::tree4_l_c_p::Builder>();
                let indices: Vec<usize> = (0usize..16usize).collect();
                build_tree4l(&mut tree4l, indices.as_slice(), &payloads);
            }
            TreeDepth::Five => {
                assert!(payloads.len() == 32);
                let mut tree5l = builder.init_root::<ds_query_capnp::tree5_l_c_p::Builder>();
                let indices: Vec<usize> = (0usize..32usize).collect();
                build_tree5l(&mut tree5l, indices.as_slice(), &payloads);
            }
        },
    }
    // initialize sga from builder
    let segments = builder.get_segments_for_output();
    (segments.len() + 1) * FRAMING_ENTRY_SIZE
}

fn build_single_buffer_message<'a>(
    single_buffer_cp: &mut ds_query_capnp::single_buffer_c_p::Builder,
    index: usize,
    payloads: &Vec<&'a [u8]>,
) {
    single_buffer_cp.set_message(payloads[index])
}

pub struct CapnprotoSerializer {
    message_type: SimpleMessageType,
    framing_size: usize,
}

impl CapnprotoSerializer {
    pub fn new(message_type: SimpleMessageType, size: usize) -> CapnprotoSerializer {
        CapnprotoSerializer {
            message_type: message_type,
            framing_size: context_framing_size(message_type, size),
        }
    }
}
impl<D> CerealizeMessage<D> for CapnprotoSerializer
where
    D: Datapath,
{
    // it seems like because initLeft() and initRight() take self and &self
    // we need to have different contexts for each nested object
    type Ctx = (Vec<u8>, Builder<HeapAllocator>);

    fn message_type(&self) -> SimpleMessageType {
        self.message_type
    }

    fn process_msg<'registered, 'normal: 'registered>(
        &self,
        recved_msg: &'registered D::ReceivedPkt,
        ctx: &'normal mut Self::Ctx,
    ) -> Result<Cornflake<'registered, 'normal>> {
        let (ref mut framing_vec, ref mut builder) = ctx;
        let segment_array_vec = read_context(recved_msg);
        let segment_array = SegmentArray::new(&segment_array_vec.as_slice());
        let message_reader = Reader::new(segment_array, ReaderOptions::default());
        match self.message_type {
            SimpleMessageType::Single => {
                let object_deser = message_reader
                    .get_root::<ds_query_capnp::single_buffer_c_p::Reader>()
                    .wrap_err("Failed to deserialize SingleBufferCP message.")?;
                let mut object = builder.init_root::<ds_query_capnp::single_buffer_c_p::Builder>();
                object.set_message(
                    object_deser
                        .get_message()
                        .wrap_err("Failed to run get_message on deserialized SingleBufferCP.")?,
                );
            }
            SimpleMessageType::List(list_size) => {
                let object_deser = message_reader
                    .get_root::<ds_query_capnp::list_c_p::Reader>()
                    .wrap_err("Failed to deserialze ListCP message.")?;
                let list_deser = object_deser
                    .get_messages()
                    .wrap_err("Failed to run get_messages on deserialized ListCP.")?;
                let object = builder.init_root::<ds_query_capnp::list_c_p::Builder>();
                let mut list = object.init_messages(list_size as u32);
                for i in 0..list_size {
                    list.set(
                        i as u32,
                        list_deser.get(i as u32).wrap_err(format!(
                            "Failed to get entry # {} from deserialized ListCP",
                            i
                        ))?,
                    );
                }
            }
            SimpleMessageType::Tree(depth) => match depth {
                TreeDepth::One => {
                    let object_deser = message_reader
                        .get_root::<ds_query_capnp::tree1_l_c_p::Reader>()
                        .wrap_err("Failed to deserialize Tree1L message.")?;
                    let mut object_ser =
                        builder.init_root::<ds_query_capnp::tree1_l_c_p::Builder>();
                    deserialize_tree1l(object_deser, &mut object_ser)?;
                }
                TreeDepth::Two => {
                    let object_deser = message_reader
                        .get_root::<ds_query_capnp::tree2_l_c_p::Reader>()
                        .wrap_err("Failed to deserialize Tree1L message.")?;
                    let mut object_ser =
                        builder.init_root::<ds_query_capnp::tree2_l_c_p::Builder>();
                    deserialize_tree2l(object_deser, &mut object_ser)?;
                }
                TreeDepth::Three => {
                    let object_deser = message_reader
                        .get_root::<ds_query_capnp::tree3_l_c_p::Reader>()
                        .wrap_err("Failed to deserialize Tree1L message.")?;
                    let mut object_ser =
                        builder.init_root::<ds_query_capnp::tree3_l_c_p::Builder>();
                    deserialize_tree3l(object_deser, &mut object_ser)?;
                }
                TreeDepth::Four => {
                    let object_deser = message_reader
                        .get_root::<ds_query_capnp::tree4_l_c_p::Reader>()
                        .wrap_err("Failed to deserialize Tree1L message.")?;
                    let mut object_ser =
                        builder.init_root::<ds_query_capnp::tree4_l_c_p::Builder>();
                    deserialize_tree4l(object_deser, &mut object_ser)?;
                }
                TreeDepth::Five => {
                    let object_deser = message_reader
                        .get_root::<ds_query_capnp::tree5_l_c_p::Reader>()
                        .wrap_err("Failed to deserialize Tree1L message.")?;
                    let mut object_ser =
                        builder.init_root::<ds_query_capnp::tree5_l_c_p::Builder>();
                    deserialize_tree5l(object_deser, &mut object_ser)?;
                }
            },
        }

        fill_in_context(framing_vec, &builder);
        let segments = builder.get_segments_for_output();
        let mut cf = Cornflake::with_capacity(segments.len() + 1);

        cf.add_entry(CornPtr::Normal(framing_vec.as_slice()));

        for seg in segments.iter() {
            cf.add_entry(CornPtr::Normal(seg));
        }
        Ok(cf)
    }

    fn new_context(&self) -> Self::Ctx {
        (vec![0u8; self.framing_size], Builder::new_default())
    }
}

pub struct CapnprotoEchoClient<'registered, 'normal> {
    message_type: SimpleMessageType,
    payload_ptrs: Vec<(*const u8, usize)>,
    sga: Cornflake<'registered, 'normal>,
    total_size: usize,
}

impl<'registered, 'normal, D> CerealizeClient<'normal, D>
    for CapnprotoEchoClient<'registered, 'normal>
where
    D: Datapath,
{
    type Ctx = (Vec<u8>, Builder<HeapAllocator>);
    type OutgoingMsg = Cornflake<'registered, 'normal>;

    fn new(
        message_type: SimpleMessageType,
        field_sizes: Vec<usize>,
        mmap_metadata: MmapMetadata,
    ) -> Result<Self> {
        let payload_ptrs = init_payloads(&field_sizes, &mmap_metadata)?;
        let sga = Cornflake::default();
        let total_size: usize = payload_ptrs.iter().map(|(_, size)| *size).sum();

        Ok(CapnprotoEchoClient {
            message_type: message_type,
            payload_ptrs: payload_ptrs,
            sga: sga,
            total_size: total_size,
        })
    }

    fn init(&mut self, ctx: &'normal mut Self::Ctx) -> Result<()> {
        let (ref mut framing_vec, ref mut builder) = ctx;
        let payloads: Vec<&[u8]> = self
            .payload_ptrs
            .clone()
            .iter()
            .map(|(ptr, size)| unsafe { slice::from_raw_parts(*ptr, *size) })
            .collect();
        match self.message_type {
            SimpleMessageType::Single => {
                assert!(payloads.len() == 1);
                let mut object = builder.init_root::<ds_query_capnp::single_buffer_c_p::Builder>();
                object.set_message(payloads[0]);
            }
            SimpleMessageType::List(list_size) => {
                assert!(payloads.len() == list_size);
                let object = builder.init_root::<ds_query_capnp::list_c_p::Builder>();
                let mut list = object.init_messages(list_size as u32);
                for (i, payload) in payloads.iter().enumerate() {
                    list.set(i as u32, payload);
                }
            }
            SimpleMessageType::Tree(depth) => match depth {
                TreeDepth::One => {
                    assert!(payloads.len() == 2);
                    let mut tree1l = builder.init_root::<ds_query_capnp::tree1_l_c_p::Builder>();
                    build_tree1l(&mut tree1l, &[0, 1], &payloads);
                }
                TreeDepth::Two => {
                    assert!(payloads.len() == 4);
                    let mut tree2l = builder.init_root::<ds_query_capnp::tree2_l_c_p::Builder>();
                    let indices: Vec<usize> = (0usize..4usize).collect();
                    build_tree2l(&mut tree2l, indices.as_slice(), &payloads);
                }
                TreeDepth::Three => {
                    assert!(payloads.len() == 8);
                    let mut tree3l = builder.init_root::<ds_query_capnp::tree3_l_c_p::Builder>();
                    let indices: Vec<usize> = (0usize..8usize).collect();
                    build_tree3l(&mut tree3l, indices.as_slice(), &payloads);
                }
                TreeDepth::Four => {
                    assert!(payloads.len() == 16);
                    let mut tree4l = builder.init_root::<ds_query_capnp::tree4_l_c_p::Builder>();
                    let indices: Vec<usize> = (0usize..16usize).collect();
                    build_tree4l(&mut tree4l, indices.as_slice(), &payloads);
                }
                TreeDepth::Five => {
                    assert!(payloads.len() == 32);
                    let mut tree5l = builder.init_root::<ds_query_capnp::tree5_l_c_p::Builder>();
                    let indices: Vec<usize> = (0usize..32usize).collect();
                    build_tree5l(&mut tree5l, indices.as_slice(), &payloads);
                }
            },
        }

        // initialize sga from builder
        fill_in_context(framing_vec, &builder);
        let segments = builder.get_segments_for_output();
        self.sga = Cornflake::with_capacity(segments.len() + 1);

        self.sga.add_entry(CornPtr::Normal(framing_vec.as_slice()));

        for seg in segments.iter() {
            tracing::debug!("seg: {:?}", seg);
            self.sga.add_entry(CornPtr::Normal(seg));
        }

        Ok(())
    }

    fn message_type(&self) -> SimpleMessageType {
        self.message_type
    }

    fn payload_sizes(&self) -> Vec<usize> {
        self.payload_ptrs.iter().map(|(_ptr, len)| *len).collect()
    }

    fn get_sga(&self) -> Result<Self::OutgoingMsg> {
        Ok(self.sga.clone())
    }

    fn check_echoed_payload(&self, recved_msg: &D::ReceivedPkt) -> Result<()> {
        let our_payloads = get_payloads_as_vec(&self.payload_ptrs);
        let segment_array_vec = read_context(recved_msg);
        let segment_array = SegmentArray::new(&segment_array_vec.as_slice());
        let message_reader = Reader::new(segment_array, ReaderOptions::default());
        match self.message_type {
            SimpleMessageType::Single => {
                let object_deser = message_reader
                    .get_root::<ds_query_capnp::single_buffer_c_p::Reader>()
                    .wrap_err("Failed to deserialize SingleBufferCP message.")?;
                let payload = object_deser
                    .get_message()
                    .wrap_err("Unable to run get_message on SingleBufferCP.")?;
                assert!(payload.len() == our_payloads[0].len());
                assert!(payload.to_vec() == our_payloads[0].to_vec());
            }
            SimpleMessageType::List(list_elts) => {
                assert!(our_payloads.len() == list_elts);
                let object_deser = message_reader
                    .get_root::<ds_query_capnp::list_c_p::Reader>()
                    .wrap_err("Failed to deserialize ListCP message.")?;
                let list_deser = object_deser
                    .get_messages()
                    .wrap_err("Unable to run get_messages() on ListCP.")?;
                for (i, our_payload) in our_payloads.iter().enumerate() {
                    let payload_deser = list_deser
                        .get(i as u32)
                        .wrap_err("Not able to get elt {:?} i in list_deser for ListCP message.")?;
                    assert!(payload_deser.len() == our_payload.len());
                    assert!(payload_deser.to_vec() == our_payload.to_vec());
                }
            }
            SimpleMessageType::Tree(depth) => match depth {
                TreeDepth::One => {
                    let object_deser = message_reader
                        .get_root::<ds_query_capnp::tree1_l_c_p::Reader>()
                        .wrap_err("Failed to deserialize Tree1L message.")?;
                    let indices: Vec<usize> = (0usize..2usize).collect();
                    check_tree1l(indices.as_slice(), &our_payloads, object_deser)?;
                }
                TreeDepth::Two => {
                    let object_deser = message_reader
                        .get_root::<ds_query_capnp::tree2_l_c_p::Reader>()
                        .wrap_err("Failed to deserialize Tree2L message.")?;
                    let indices: Vec<usize> = (0usize..4usize).collect();
                    check_tree2l(indices.as_slice(), &our_payloads, object_deser)?;
                }
                TreeDepth::Three => {
                    let object_deser = message_reader
                        .get_root::<ds_query_capnp::tree3_l_c_p::Reader>()
                        .wrap_err("Failed to deserialize Tree3L message.")?;
                    let indices: Vec<usize> = (0usize..8usize).collect();
                    check_tree3l(indices.as_slice(), &our_payloads, object_deser)?;
                }
                TreeDepth::Four => {
                    let object_deser = message_reader
                        .get_root::<ds_query_capnp::tree4_l_c_p::Reader>()
                        .wrap_err("Failed to deserialize Tree4L message.")?;
                    let indices: Vec<usize> = (0usize..16usize).collect();
                    check_tree4l(indices.as_slice(), &our_payloads, object_deser)?;
                }
                TreeDepth::Five => {
                    let object_deser = message_reader
                        .get_root::<ds_query_capnp::tree5_l_c_p::Reader>()
                        .wrap_err("Failed to deserialize Tree5L message.")?;
                    let indices: Vec<usize> = (0usize..32usize).collect();
                    check_tree5l(indices.as_slice(), &our_payloads, object_deser)?;
                }
            },
        }

        Ok(())
    }

    fn new_context(&self) -> Self::Ctx {
        let framing_size = context_framing_size(self.message_type, self.total_size);
        (vec![0u8; framing_size], Builder::new_default())
    }
}
