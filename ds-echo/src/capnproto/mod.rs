use super::echo_capnp;
use super::{read_message_type, ClientCerealizeMessage, REQ_TYPE_SIZE};
use byteorder::{ByteOrder, LittleEndian};
use capnp::message::{Allocator, Builder, Reader, ReaderOptions, ReaderSegments, SegmentArray};
use color_eyre::eyre::{ensure, Result, WrapErr};
use cornflakes_libos::{
    datapath::{Datapath, PushBufType, ReceivedPkt},
    state_machine::server::ServerSM,
    ArenaOrderedSga, Sge,
};
use cornflakes_utils::{SimpleMessageType, TreeDepth};

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

fn allocate_vec<T>(builder: &Builder<T>) -> Vec<u8>
where
    T: Allocator,
{
    let segments = builder.get_segments_for_output();
    let size = FRAMING_ENTRY_SIZE
        + segments
            .iter()
            .map(|seg| seg.len() + FRAMING_ENTRY_SIZE)
            .sum::<usize>();
    vec![0u8; size]
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

pub struct CapnprotoSerializer<D>
where
    D: Datapath,
{
    push_buf_type: PushBufType,
    _phantom_data: PhantomData<D>,
    arena: bumpalo::Bump,
}

impl<D> CapnprotoSerializer<D>
where
    D: Datapath,
{
    pub fn new(push_buf_type: PushBufType) -> Self {
        CapnprotoSerializer {
            push_buf_type: push_buf_type,
            _phantom_data: PhantomData::default(),
            arena: bumpalo::Bump::with_capacity(
                ArenaOrderedSga::arena_size(
                    D::batch_size(),
                    D::max_packet_size(),
                    D::max_scatter_gather_entries(),
                ) * 100,
            ),
        }
    }

    pub fn set_with_copy(&mut self) {}
}

impl<D> ServerSM for CapnprotoSerializer<D>
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
            let msg_type = read_message_type(&pkt)?;
            let segment_array_vec = read_context(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
            let segment_array = SegmentArray::new(&segment_array_vec.as_slice());
            let message_reader = Reader::new(segment_array, ReaderOptions::default());

            match msg_type {
                SimpleMessageType::Single => {
                    let object_deser = message_reader
                        .get_root::<echo_capnp::single_buffer_c_p::Reader>()
                        .wrap_err("Failed to deserialize SingleBufferCP message.")?;
                    let mut object = builder.init_root::<echo_capnp::single_buffer_c_p::Builder>();
                    object.set_message(
                        object_deser.get_message().wrap_err(
                            "Failed to run get_message on deserialized SingleBufferCP.",
                        )?,
                    );
                }
                SimpleMessageType::List(list_size) => {
                    let object_deser = message_reader
                        .get_root::<echo_capnp::list_c_p::Reader>()
                        .wrap_err("Failed to deserialze ListCP message.")?;
                    let list_deser = object_deser
                        .get_messages()
                        .wrap_err("Failed to run get_messages on deserialized ListCP.")?;
                    let object = builder.init_root::<echo_capnp::list_c_p::Builder>();
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
                SimpleMessageType::Tree(tree_depth) => match tree_depth {
                    TreeDepth::One => {
                        let object_deser = message_reader
                            .get_root::<echo_capnp::tree1_l_c_p::Reader>()
                            .wrap_err("Failed to deserialize Tree1L message.")?;
                        let mut object_ser =
                            builder.init_root::<echo_capnp::tree1_l_c_p::Builder>();
                        deserialize_tree1l(object_deser, &mut object_ser)?;
                    }
                    TreeDepth::Two => {
                        let object_deser = message_reader
                            .get_root::<echo_capnp::tree2_l_c_p::Reader>()
                            .wrap_err("Failed to deserialize Tree1L message.")?;
                        let mut object_ser =
                            builder.init_root::<echo_capnp::tree2_l_c_p::Builder>();
                        deserialize_tree2l(object_deser, &mut object_ser)?;
                    }
                    TreeDepth::Three => {
                        let object_deser = message_reader
                            .get_root::<echo_capnp::tree3_l_c_p::Reader>()
                            .wrap_err("Failed to deserialize Tree1L message.")?;
                        let mut object_ser =
                            builder.init_root::<echo_capnp::tree3_l_c_p::Builder>();
                        deserialize_tree3l(object_deser, &mut object_ser)?;
                    }
                    TreeDepth::Four => {
                        let object_deser = message_reader
                            .get_root::<echo_capnp::tree4_l_c_p::Reader>()
                            .wrap_err("Failed to deserialize Tree1L message.")?;
                        let mut object_ser =
                            builder.init_root::<echo_capnp::tree4_l_c_p::Builder>();
                        deserialize_tree4l(object_deser, &mut object_ser)?;
                    }
                    TreeDepth::Five => {
                        let object_deser = message_reader
                            .get_root::<echo_capnp::tree5_l_c_p::Reader>()
                            .wrap_err("Failed to deserialize Tree1L message.")?;
                        let mut object_ser =
                            builder.init_root::<echo_capnp::tree5_l_c_p::Builder>();
                        deserialize_tree5l(object_deser, &mut object_ser)?;
                    }
                },
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

pub struct CapnprotoEchoClient {}

impl<D> ClientCerealizeMessage<D> for CapnprotoEchoClient
where
    D: Datapath,
{
    fn new() -> Self
    where
        Self: Sized,
    {
        CapnprotoEchoClient {}
    }

    fn check_echoed_payload(
        &self,
        pkt: &ReceivedPkt<D>,
        bytes_to_check: (SimpleMessageType, &Vec<Vec<u8>>),
        _datapath: &D,
    ) -> Result<bool> {
        let (ty, our_payloads) = bytes_to_check;
        let recved_msg = pkt.seg(0).as_ref();
        let segment_array_vec = read_context(recved_msg)?;
        let segment_array = SegmentArray::new(&segment_array_vec.as_slice());
        let message_reader = Reader::new(segment_array, ReaderOptions::default());
        match ty {
            SimpleMessageType::Single => {
                assert!(our_payloads.len() == 1);
                let object_deser = message_reader
                    .get_root::<echo_capnp::single_buffer_c_p::Reader>()
                    .wrap_err("Failed to deserialize SingleBufferCP message.")?;
                let payload = object_deser
                    .get_message()
                    .wrap_err("Unable to run get_message on SingleBufferCP.")?;
                assert!(payload.len() == our_payloads[0].len());
                assert!(payload.to_vec() == our_payloads[0]);
            }
            SimpleMessageType::List(list_size) => {
                assert!(our_payloads.len() == list_size);
                let object_deser = message_reader
                    .get_root::<echo_capnp::list_c_p::Reader>()
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
                        let object_deser = message_reader
                            .get_root::<echo_capnp::tree1_l_c_p::Reader>()
                            .wrap_err("Failed to deserialize Tree1L message.")?;
                        let indices: Vec<usize> = (0usize..2usize).collect();
                        check_tree1l(indices.as_slice(), &our_payloads, object_deser)?;
                    }
                    TreeDepth::Two => {
                        let object_deser = message_reader
                            .get_root::<echo_capnp::tree2_l_c_p::Reader>()
                            .wrap_err("Failed to deserialize Tree2L message.")?;
                        let indices: Vec<usize> = (0usize..4usize).collect();
                        check_tree2l(indices.as_slice(), &our_payloads, object_deser)?;
                    }
                    TreeDepth::Three => {
                        let object_deser = message_reader
                            .get_root::<echo_capnp::tree3_l_c_p::Reader>()
                            .wrap_err("Failed to deserialize Tree3L message.")?;
                        let indices: Vec<usize> = (0usize..8usize).collect();
                        check_tree3l(indices.as_slice(), &our_payloads, object_deser)?;
                    }
                    TreeDepth::Four => {
                        let object_deser = message_reader
                            .get_root::<echo_capnp::tree4_l_c_p::Reader>()
                            .wrap_err("Failed to deserialize Tree4L message.")?;
                        let indices: Vec<usize> = (0usize..16usize).collect();
                        check_tree4l(indices.as_slice(), &our_payloads, object_deser)?;
                    }
                    TreeDepth::Five => {
                        let object_deser = message_reader
                            .get_root::<echo_capnp::tree5_l_c_p::Reader>()
                            .wrap_err("Failed to deserialize Tree5L message.")?;
                        let indices: Vec<usize> = (0usize..32usize).collect();
                        check_tree5l(indices.as_slice(), &our_payloads, object_deser)?;
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
        let mut builder = Builder::new_default();
        match ty {
            SimpleMessageType::Single => {
                assert!(payloads.len() == 1);
                let mut object = builder.init_root::<echo_capnp::single_buffer_c_p::Builder>();
                object.set_message(payloads[0].as_slice());
            }
            SimpleMessageType::List(list_size) => {
                assert!(payloads.len() == list_size);
                let object = builder.init_root::<echo_capnp::list_c_p::Builder>();
                let mut list = object.init_messages(list_size as u32);
                for (i, payload) in payloads.iter().enumerate() {
                    list.set(i as u32, payload.as_slice());
                }
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
                        let mut tree1l = builder.init_root::<echo_capnp::tree1_l_c_p::Builder>();
                        build_tree1l(&mut tree1l, &[0, 1], &payloads);
                    }
                    TreeDepth::Two => {
                        assert!(payloads.len() == 4);
                        let mut tree2l = builder.init_root::<echo_capnp::tree2_l_c_p::Builder>();
                        let indices: Vec<usize> = (0usize..4usize).collect();
                        build_tree2l(&mut tree2l, indices.as_slice(), &payloads);
                    }
                    TreeDepth::Three => {
                        assert!(payloads.len() == 8);
                        let mut tree3l = builder.init_root::<echo_capnp::tree3_l_c_p::Builder>();
                        let indices: Vec<usize> = (0usize..8usize).collect();
                        build_tree3l(&mut tree3l, indices.as_slice(), &payloads);
                    }
                    TreeDepth::Four => {
                        assert!(payloads.len() == 16);
                        let mut tree4l = builder.init_root::<echo_capnp::tree4_l_c_p::Builder>();
                        let indices: Vec<usize> = (0usize..16usize).collect();
                        build_tree4l(&mut tree4l, indices.as_slice(), &payloads);
                    }
                    TreeDepth::Five => {
                        assert!(payloads.len() == 32);
                        let mut tree5l = builder.init_root::<echo_capnp::tree5_l_c_p::Builder>();
                        let indices: Vec<usize> = (0usize..32usize).collect();
                        build_tree5l(&mut tree5l, indices.as_slice(), &payloads);
                    }
                }
            }
        }
        let mut vec = allocate_vec(&builder);
        let framing_size = fill_in_context_without_arena(&builder, vec.as_mut_slice())?;
        let _ = copy_into_buf(vec.as_mut_slice(), framing_size, &builder)?;
        Ok(vec)
    }
}

fn check_tree5l(
    indices: &[usize],
    payloads: &Vec<Vec<u8>>,
    tree5l: echo_capnp::tree5_l_c_p::Reader,
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
    input: echo_capnp::tree5_l_c_p::Reader,
    output: &mut echo_capnp::tree5_l_c_p::Builder,
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
    tree5l: &mut echo_capnp::tree5_l_c_p::Builder,
    indices: &[usize],
    payloads: &Vec<Vec<u8>>,
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
    tree4l: echo_capnp::tree4_l_c_p::Reader,
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
    input: echo_capnp::tree4_l_c_p::Reader,
    output: &mut echo_capnp::tree4_l_c_p::Builder,
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
    tree4l: &mut echo_capnp::tree4_l_c_p::Builder,
    indices: &[usize],
    payloads: &Vec<Vec<u8>>,
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
    tree3l: echo_capnp::tree3_l_c_p::Reader,
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
    input: echo_capnp::tree3_l_c_p::Reader,
    output: &mut echo_capnp::tree3_l_c_p::Builder,
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
    tree3l: &mut echo_capnp::tree3_l_c_p::Builder,
    indices: &[usize],
    payloads: &Vec<Vec<u8>>,
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
    tree2l: echo_capnp::tree2_l_c_p::Reader,
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
    input: echo_capnp::tree2_l_c_p::Reader,
    output: &mut echo_capnp::tree2_l_c_p::Builder,
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
    tree2l: &mut echo_capnp::tree2_l_c_p::Builder,
    indices: &[usize],
    payloads: &Vec<Vec<u8>>,
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
    tree1l: echo_capnp::tree1_l_c_p::Reader,
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
    input: echo_capnp::tree1_l_c_p::Reader,
    output: &mut echo_capnp::tree1_l_c_p::Builder,
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
    tree1l: &mut echo_capnp::tree1_l_c_p::Builder,
    indices: &[usize],
    payloads: &Vec<Vec<u8>>,
) {
    {
        let mut left = tree1l.reborrow().init_left();
        build_single_buffer_message(&mut left, indices[0], payloads);
    }
    let mut right = tree1l.reborrow().init_right();
    build_single_buffer_message(&mut right, indices[1], payloads);
}

fn build_single_buffer_message(
    single_buffer_cp: &mut echo_capnp::single_buffer_c_p::Builder,
    index: usize,
    payloads: &Vec<Vec<u8>>,
) {
    single_buffer_cp.set_message(payloads[index].as_slice())
}

fn check_single_buffer(
    index: usize,
    payloads: &Vec<Vec<u8>>,
    single_buffer_cp: echo_capnp::single_buffer_c_p::Reader,
) -> Result<()> {
    let buf = single_buffer_cp
        .get_message()
        .wrap_err("Unable to run get_message() on single_buffer_c_p")?;
    assert!(buf.len() == payloads[index].len());
    assert!(buf.to_vec() == payloads[index]);
    Ok(())
}

fn deserialize_single_buffer(
    input: echo_capnp::single_buffer_c_p::Reader,
    output: &mut echo_capnp::single_buffer_c_p::Builder,
) -> Result<()> {
    output.set_message(
        input
            .get_message()
            .wrap_err("Failed to run get_message() on input SingleBufferCP")?,
    );
    Ok(())
}
