pub mod echo_messages_hybrid {
    #![allow(unused_variables)]
    #![allow(non_camel_case_types)]
    #![allow(non_upper_case_globals)]
    #![allow(non_snake_case)]
    include!(concat!(env!("OUT_DIR"), "/echo_dynamic_hybrid.rs"));
}
pub mod echo_messages_sga {
    #![allow(non_camel_case_types)]
    #![allow(non_upper_case_globals)]
    #![allow(non_snake_case)]
    include!(concat!(env!("OUT_DIR"), "/echo_dynamic_sga.rs"));
}

pub mod echo_messages_rcsga {
    #![allow(unused_variables)]
    #![allow(non_camel_case_types)]
    #![allow(non_upper_case_globals)]
    #![allow(non_snake_case)]
    include!(concat!(env!("OUT_DIR"), "/echo_dynamic_rcsga.rs"));
}
use super::{read_message_type, ClientCerealizeMessage, REQ_TYPE_SIZE};
use color_eyre::eyre::{ensure, Result};
use cornflakes_libos::{
    datapath::{Datapath, PushBufType, ReceivedPkt},
    dynamic_rcsga_hdr,
    dynamic_rcsga_hdr::RcSgaHeaderRepr,
    dynamic_rcsga_hybrid_hdr,
    dynamic_rcsga_hybrid_hdr::HybridArenaRcSgaHdr,
    state_machine::server::ServerSM,
    ArenaOrderedRcSga, CopyContext,
};
use cornflakes_utils::{SimpleMessageType, TreeDepth};
use std::marker::PhantomData;

pub struct CornflakesSerializer<D>
where
    D: Datapath,
{
    push_buf_type: PushBufType,
    _phantom_data: PhantomData<D>,
    with_copy: bool,
}

impl<D> CornflakesSerializer<D>
where
    D: Datapath,
{
    pub fn new(push_buf_type: PushBufType) -> Self {
        CornflakesSerializer {
            push_buf_type: push_buf_type,
            _phantom_data: PhantomData::default(),
            with_copy: false,
        }
    }

    pub fn set_with_copy(&mut self) {
        self.with_copy = true;
    }
}

impl<D> ServerSM for CornflakesSerializer<D>
where
    D: Datapath,
{
    type Datapath = D;
    fn push_buf_type(&self) -> PushBufType {
        self.push_buf_type
    }

    #[inline]
    fn process_requests_object(
        &mut self,
        sga: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        datapath: &mut Self::Datapath,
        arena: &mut bumpalo::Bump,
    ) -> Result<()> {
        let end = sga.len();
        let mut copy_context = CopyContext::new(arena, datapath)?;
        for (i, pkt) in sga.into_iter().enumerate() {
            let end_batch = i == (end - 1);
            let message_type = read_message_type(&pkt)?;
            match message_type {
                SimpleMessageType::Single => {
                    let mut single_deser = echo_messages_hybrid::SingleBufferCF::new_in(arena);
                    let mut single_ser = echo_messages_hybrid::SingleBufferCF::new_in(arena);
                    {
                        #[cfg(feature = "profiler")]
                        demikernel::timer!("Deserialize pkt");
                        single_deser.deserialize(&pkt, REQ_TYPE_SIZE, arena)?;
                    }
                    {
                        #[cfg(feature = "profiler")]
                        demikernel::timer!("Set message");
                        single_ser.set_message(dynamic_rcsga_hybrid_hdr::CFBytes::new(
                            single_deser.get_message().as_ref(),
                            datapath,
                            &mut copy_context,
                        )?);
                        tracing::debug!(set_msg =? single_ser.get_message().as_ref());
                    }
                    datapath.queue_cornflakes_obj(
                        pkt.msg_id(),
                        pkt.conn_id(),
                        &mut copy_context,
                        single_ser,
                        end_batch,
                    )?;
                }
                SimpleMessageType::List(_list_elts) => {
                    let mut list_deser =
                        echo_messages_hybrid::ListCF::<Self::Datapath>::new_in(arena);
                    let mut list_ser =
                        echo_messages_hybrid::ListCF::<Self::Datapath>::new_in(arena);
                    list_deser.deserialize(&pkt, REQ_TYPE_SIZE, arena)?;

                    list_ser.init_messages(list_deser.get_messages().len(), arena);
                    let messages = list_ser.get_mut_messages();
                    for elt in list_deser.get_messages().iter() {
                        messages.append(dynamic_rcsga_hybrid_hdr::CFBytes::new(
                            elt.as_ref(),
                            datapath,
                            &mut copy_context,
                        )?);
                    }

                    datapath.queue_cornflakes_obj(
                        pkt.msg_id(),
                        pkt.conn_id(),
                        &mut copy_context,
                        list_ser,
                        end_batch,
                    )?;
                }
                SimpleMessageType::Tree(tree_depth) => match tree_depth {
                    TreeDepth::One => {
                        let mut tree_deser = echo_messages_hybrid::Tree1LCF::new_in(arena);
                        tree_deser.deserialize(&pkt, REQ_TYPE_SIZE, arena)?;
                        let tree_ser = deserialize_from_tree1l(
                            &tree_deser,
                            datapath,
                            &mut copy_context,
                            arena,
                        )?;
                        datapath.queue_cornflakes_obj(
                            pkt.msg_id(),
                            pkt.conn_id(),
                            &mut copy_context,
                            tree_ser,
                            end_batch,
                        )?;
                    }
                    TreeDepth::Two => {
                        let mut tree_deser = echo_messages_hybrid::Tree2LCF::new_in(arena);
                        tree_deser.deserialize(&pkt, REQ_TYPE_SIZE, arena)?;
                        let tree_ser = deserialize_from_tree2l(
                            &tree_deser,
                            datapath,
                            &mut copy_context,
                            arena,
                        )?;
                        datapath.queue_cornflakes_obj(
                            pkt.msg_id(),
                            pkt.conn_id(),
                            &mut copy_context,
                            tree_ser,
                            end_batch,
                        )?;
                    }
                    TreeDepth::Three => {
                        let mut tree_deser = echo_messages_hybrid::Tree3LCF::new_in(arena);
                        tree_deser.deserialize(&pkt, REQ_TYPE_SIZE, arena)?;
                        let tree_ser = deserialize_from_tree3l(
                            &tree_deser,
                            datapath,
                            &mut copy_context,
                            arena,
                        )?;
                        datapath.queue_cornflakes_obj(
                            pkt.msg_id(),
                            pkt.conn_id(),
                            &mut copy_context,
                            tree_ser,
                            end_batch,
                        )?;
                    }
                    TreeDepth::Four => {
                        let mut tree_deser = echo_messages_hybrid::Tree4LCF::new_in(arena);
                        tree_deser.deserialize(&pkt, REQ_TYPE_SIZE, arena)?;
                        let tree_ser = deserialize_from_tree4l(
                            &tree_deser,
                            datapath,
                            &mut copy_context,
                            arena,
                        )?;
                        datapath.queue_cornflakes_obj(
                            pkt.msg_id(),
                            pkt.conn_id(),
                            &mut copy_context,
                            tree_ser,
                            end_batch,
                        )?;
                    }
                    TreeDepth::Five => {
                        let mut tree_deser = echo_messages_hybrid::Tree5LCF::new_in(arena);
                        tree_deser.deserialize(&pkt, REQ_TYPE_SIZE, arena)?;
                        let tree_ser = deserialize_from_tree5l(
                            &tree_deser,
                            datapath,
                            &mut copy_context,
                            arena,
                        )?;
                        datapath.queue_cornflakes_obj(
                            pkt.msg_id(),
                            pkt.conn_id(),
                            &mut copy_context,
                            tree_ser,
                            end_batch,
                        )?;
                    }
                },
            }
        }
        Ok(())
    }

    fn process_requests_arena_ordered_sga(
        &mut self,
        pkts: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        datapath: &mut Self::Datapath,
        arena: &mut bumpalo::Bump,
    ) -> Result<()> {
        let pkts_len = pkts.len();
        for (i, pkt) in pkts.into_iter().enumerate() {
            let end_batch = i == (pkts_len - 1);
            let message_type = read_message_type(&pkt)?;
            match message_type {
                SimpleMessageType::Single => {
                    let mut single_deser =
                        echo_messages_rcsga::SingleBufferCF::<Self::Datapath>::new();
                    let mut single_ser =
                        echo_messages_rcsga::SingleBufferCF::<Self::Datapath>::new();
                    tracing::debug!(pkt_data =? &pkt.seg(0).as_ref()[REQ_TYPE_SIZE..], "Incoming packet data");
                    tracing::debug!(len = pkt.data_len(), "Incoming packet length");
                    {
                        #[cfg(feature = "profiler")]
                        demikernel::timer!("Deserialize pkt");
                        single_deser.deserialize_from_pkt(&pkt, REQ_TYPE_SIZE)?;
                    }
                    {
                        #[cfg(feature = "profiler")]
                        demikernel::timer!("Set message");
                        tracing::debug!(get_msg =? single_deser.get_message().as_bytes());
                        single_ser.set_message(dynamic_rcsga_hdr::CFBytes::new(
                            single_deser.get_message().as_bytes(),
                            datapath,
                        )?);
                        tracing::debug!(set_msg =? single_ser.get_message().as_bytes());
                    }
                    let mut ordered_sga = {
                        #[cfg(feature = "profiler")]
                        demikernel::timer!("Allocate sga");
                        ArenaOrderedRcSga::allocate(single_ser.num_scatter_gather_entries(), &arena)
                    };
                    {
                        #[cfg(feature = "profiler")]
                        demikernel::timer!("Serialize into sga");
                        single_ser.serialize_into_arena_sga(
                            &mut ordered_sga,
                            &arena,
                            datapath,
                            self.with_copy,
                        )?;
                    }
                    datapath.queue_arena_ordered_rcsga(
                        (pkt.msg_id(), pkt.conn_id(), ordered_sga),
                        end_batch,
                    )?;
                }
                SimpleMessageType::List(_list_elts) => {
                    let mut list_deser = echo_messages_rcsga::ListCF::<Self::Datapath>::new();
                    let mut list_ser = echo_messages_rcsga::ListCF::<Self::Datapath>::new();
                    list_deser.deserialize_from_pkt(&pkt, REQ_TYPE_SIZE)?;

                    list_ser.init_messages(list_deser.get_messages().len());
                    let messages = list_ser.get_mut_messages();
                    for elt in list_deser.get_messages().iter() {
                        messages.append(dynamic_rcsga_hdr::CFBytes::new(elt.as_bytes(), datapath)?);
                    }

                    let mut ordered_sga = {
                        #[cfg(feature = "profiler")]
                        demikernel::timer!("Allocate arena sga");
                        ArenaOrderedRcSga::allocate(list_ser.num_scatter_gather_entries(), &arena)
                    };
                    list_ser.serialize_into_arena_sga(
                        &mut ordered_sga,
                        &arena,
                        &datapath,
                        self.with_copy,
                    )?;
                    datapath.queue_arena_ordered_rcsga(
                        (pkt.msg_id(), pkt.conn_id(), ordered_sga),
                        end_batch,
                    )?;
                }
                SimpleMessageType::Tree(depth) => match depth {
                    TreeDepth::One => {
                        let mut tree_deser =
                            echo_messages_rcsga::Tree1LCF::<<Self as ServerSM>::Datapath>::new();
                        tree_deser.deserialize_from_pkt(&pkt, REQ_TYPE_SIZE)?;
                        let tree_ser = deserialize_from_pkt_tree1l_sga(&tree_deser, datapath)?;
                        let mut ordered_sga = ArenaOrderedRcSga::allocate(
                            tree_ser.num_scatter_gather_entries(),
                            &arena,
                        );
                        tree_ser.serialize_into_arena_sga(
                            &mut ordered_sga,
                            &arena,
                            &datapath,
                            self.with_copy,
                        )?;
                        datapath.queue_arena_ordered_rcsga(
                            (pkt.msg_id(), pkt.conn_id(), ordered_sga),
                            end_batch,
                        )?;
                    }
                    TreeDepth::Two => {
                        let mut tree_deser =
                            echo_messages_rcsga::Tree2LCF::<<Self as ServerSM>::Datapath>::new();
                        tree_deser.deserialize_from_pkt(&pkt, REQ_TYPE_SIZE)?;
                        let tree_ser = deserialize_from_pkt_tree2l_sga(&tree_deser, datapath)?;
                        let mut ordered_sga = ArenaOrderedRcSga::allocate(
                            tree_ser.num_scatter_gather_entries(),
                            &arena,
                        );
                        tree_ser.serialize_into_arena_sga(
                            &mut ordered_sga,
                            &arena,
                            &datapath,
                            self.with_copy,
                        )?;
                        datapath.queue_arena_ordered_rcsga(
                            (pkt.msg_id(), pkt.conn_id(), ordered_sga),
                            end_batch,
                        )?;
                    }
                    TreeDepth::Three => {
                        let mut tree_deser =
                            echo_messages_rcsga::Tree3LCF::<<Self as ServerSM>::Datapath>::new();
                        tree_deser.deserialize_from_pkt(&pkt, REQ_TYPE_SIZE)?;
                        let tree_ser = deserialize_from_pkt_tree3l_sga(&tree_deser, datapath)?;
                        let mut ordered_sga = ArenaOrderedRcSga::allocate(
                            tree_ser.num_scatter_gather_entries(),
                            &arena,
                        );
                        tree_ser.serialize_into_arena_sga(
                            &mut ordered_sga,
                            &arena,
                            &datapath,
                            self.with_copy,
                        )?;
                        datapath.queue_arena_ordered_rcsga(
                            (pkt.msg_id(), pkt.conn_id(), ordered_sga),
                            end_batch,
                        )?;
                    }
                    TreeDepth::Four => {
                        let mut tree_deser =
                            echo_messages_rcsga::Tree4LCF::<<Self as ServerSM>::Datapath>::new();
                        tree_deser.deserialize_from_pkt(&pkt, REQ_TYPE_SIZE)?;
                        let tree_ser = deserialize_from_pkt_tree4l_sga(&tree_deser, datapath)?;
                        let mut ordered_sga = ArenaOrderedRcSga::allocate(
                            tree_ser.num_scatter_gather_entries(),
                            &arena,
                        );
                        tree_ser.serialize_into_arena_sga(
                            &mut ordered_sga,
                            &arena,
                            &datapath,
                            self.with_copy,
                        )?;
                        datapath.queue_arena_ordered_rcsga(
                            (pkt.msg_id(), pkt.conn_id(), ordered_sga),
                            end_batch,
                        )?;
                    }
                    TreeDepth::Five => {
                        let mut tree_deser =
                            echo_messages_rcsga::Tree5LCF::<<Self as ServerSM>::Datapath>::new();
                        tree_deser.deserialize_from_pkt(&pkt, REQ_TYPE_SIZE)?;
                        let tree_ser = deserialize_from_pkt_tree5l_sga(&tree_deser, datapath)?;
                        let mut ordered_sga = ArenaOrderedRcSga::allocate(
                            tree_ser.num_scatter_gather_entries(),
                            &arena,
                        );
                        tree_ser.serialize_into_arena_sga(
                            &mut ordered_sga,
                            &arena,
                            &datapath,
                            self.with_copy,
                        )?;
                        datapath.queue_arena_ordered_rcsga(
                            (pkt.msg_id(), pkt.conn_id(), ordered_sga),
                            end_batch,
                        )?;
                    }
                },
            }
        }
        arena.reset();
        Ok(())
    }

    fn process_requests_sga(
        &mut self,
        sga: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        datapath: &mut Self::Datapath,
    ) -> Result<()> {
        // TODO(ygina): is it ok to wrap this?
        self.process_requests_ordered_sga(sga, datapath)
    }

    fn process_requests_rc_sga(
        &mut self,
        _sga: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        _datapath: &mut Self::Datapath,
    ) -> Result<()> {
        Ok(())
    }

    fn process_requests_single_buf(
        &mut self,
        _sga: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        _datapath: &mut Self::Datapath,
    ) -> Result<()> {
        Ok(())
    }
}

fn deserialize_from_tree5l<'arena, 'obj, D>(
    input: &'obj echo_messages_hybrid::Tree5LCF<'obj, D>,
    datapath: &mut D,
    copy_context: &mut CopyContext<'arena, D>,
    arena: &'arena bumpalo::Bump,
) -> Result<echo_messages_hybrid::Tree5LCF<'obj, D>>
where
    D: Datapath,
{
    let mut output = echo_messages_hybrid::Tree5LCF::new_in(arena);
    output.set_left(deserialize_from_tree4l(
        input.get_left(),
        datapath,
        copy_context,
        arena,
    )?);
    output.set_right(deserialize_from_tree4l(
        input.get_right(),
        datapath,
        copy_context,
        arena,
    )?);
    Ok(output)
}

fn deserialize_from_tree4l<'arena, 'obj, D>(
    input: &'obj echo_messages_hybrid::Tree4LCF<'obj, D>,
    datapath: &mut D,
    copy_context: &mut CopyContext<'arena, D>,
    arena: &'arena bumpalo::Bump,
) -> Result<echo_messages_hybrid::Tree4LCF<'obj, D>>
where
    D: Datapath,
{
    let mut output = echo_messages_hybrid::Tree4LCF::new_in(arena);
    output.set_left(deserialize_from_tree3l(
        input.get_left(),
        datapath,
        copy_context,
        arena,
    )?);
    output.set_right(deserialize_from_tree3l(
        input.get_right(),
        datapath,
        copy_context,
        arena,
    )?);
    Ok(output)
}

fn deserialize_from_tree3l<'arena, 'obj, D>(
    input: &'obj echo_messages_hybrid::Tree3LCF<'obj, D>,
    datapath: &mut D,
    copy_context: &mut CopyContext<'arena, D>,
    arena: &'arena bumpalo::Bump,
) -> Result<echo_messages_hybrid::Tree3LCF<'obj, D>>
where
    D: Datapath,
{
    let mut output = echo_messages_hybrid::Tree3LCF::new_in(arena);
    output.set_left(deserialize_from_tree2l(
        input.get_left(),
        datapath,
        copy_context,
        arena,
    )?);
    output.set_right(deserialize_from_tree2l(
        input.get_right(),
        datapath,
        copy_context,
        arena,
    )?);
    Ok(output)
}

fn deserialize_from_tree2l<'arena, 'obj, D>(
    input: &'obj echo_messages_hybrid::Tree2LCF<'obj, D>,
    datapath: &mut D,
    copy_context: &mut CopyContext<'arena, D>,
    arena: &'arena bumpalo::Bump,
) -> Result<echo_messages_hybrid::Tree2LCF<'obj, D>>
where
    D: Datapath,
{
    let mut output = echo_messages_hybrid::Tree2LCF::new_in(arena);
    output.set_left(deserialize_from_tree1l(
        input.get_left(),
        datapath,
        copy_context,
        arena,
    )?);
    output.set_right(deserialize_from_tree1l(
        input.get_right(),
        datapath,
        copy_context,
        arena,
    )?);
    Ok(output)
}

fn deserialize_from_tree1l<'arena, 'obj, D>(
    input: &'obj echo_messages_hybrid::Tree1LCF<'obj, D>,
    datapath: &mut D,
    copy_context: &mut CopyContext<'arena, D>,
    arena: &'arena bumpalo::Bump,
) -> Result<echo_messages_hybrid::Tree1LCF<'obj, D>>
where
    D: Datapath,
{
    let mut output = echo_messages_hybrid::Tree1LCF::new_in(arena);
    output.set_left(deserialize_from_single(
        input.get_left(),
        datapath,
        copy_context,
        arena,
    )?);
    output.set_right(deserialize_from_single(
        input.get_right(),
        datapath,
        copy_context,
        arena,
    )?);
    Ok(output)
}

fn deserialize_from_single<'arena, 'obj, D>(
    input: &echo_messages_hybrid::SingleBufferCF<'obj, D>,
    datapath: &mut D,
    copy_context: &mut CopyContext<'arena, D>,
    arena: &'arena bumpalo::Bump,
) -> Result<echo_messages_hybrid::SingleBufferCF<'obj, D>>
where
    D: Datapath,
{
    let mut output = echo_messages_hybrid::SingleBufferCF::new_in(arena);
    output.set_message(dynamic_rcsga_hybrid_hdr::CFBytes::new(
        input.get_message().as_ref(),
        datapath,
        copy_context,
    )?);
    Ok(output)
}
fn deserialize_from_pkt_tree5l_sga<'obj, D>(
    input: &'obj echo_messages_rcsga::Tree5LCF<'obj, D>,
    datapath: &D,
) -> Result<echo_messages_rcsga::Tree5LCF<'obj, D>>
where
    D: Datapath,
{
    let mut output = echo_messages_rcsga::Tree5LCF::new();
    output.set_left(deserialize_from_pkt_tree4l_sga(input.get_left(), datapath)?);
    output.set_right(deserialize_from_pkt_tree4l_sga(
        input.get_right(),
        datapath,
    )?);
    Ok(output)
}

fn deserialize_from_pkt_tree4l_sga<'obj, D>(
    input: &'obj echo_messages_rcsga::Tree4LCF<'obj, D>,
    datapath: &D,
) -> Result<echo_messages_rcsga::Tree4LCF<'obj, D>>
where
    D: Datapath,
{
    let mut output = echo_messages_rcsga::Tree4LCF::new();
    output.set_left(deserialize_from_pkt_tree3l_sga(input.get_left(), datapath)?);
    output.set_right(deserialize_from_pkt_tree3l_sga(
        input.get_right(),
        datapath,
    )?);
    Ok(output)
}

fn deserialize_from_pkt_tree3l_sga<'obj, D>(
    input: &'obj echo_messages_rcsga::Tree3LCF<'obj, D>,
    datapath: &D,
) -> Result<echo_messages_rcsga::Tree3LCF<'obj, D>>
where
    D: Datapath,
{
    let mut output = echo_messages_rcsga::Tree3LCF::new();
    output.set_left(deserialize_from_pkt_tree2l_sga(input.get_left(), datapath)?);
    output.set_right(deserialize_from_pkt_tree2l_sga(
        input.get_right(),
        datapath,
    )?);
    Ok(output)
}

fn deserialize_from_pkt_tree2l_sga<'obj, D>(
    input: &'obj echo_messages_rcsga::Tree2LCF<'obj, D>,
    datapath: &D,
) -> Result<echo_messages_rcsga::Tree2LCF<'obj, D>>
where
    D: Datapath,
{
    let mut output = echo_messages_rcsga::Tree2LCF::new();
    output.set_left(deserialize_from_pkt_tree1l_sga(input.get_left(), datapath)?);
    output.set_right(deserialize_from_pkt_tree1l_sga(
        input.get_right(),
        datapath,
    )?);
    Ok(output)
}

fn deserialize_from_pkt_tree1l_sga<'obj, D>(
    input: &'obj echo_messages_rcsga::Tree1LCF<'obj, D>,
    datapath: &D,
) -> Result<echo_messages_rcsga::Tree1LCF<'obj, D>>
where
    D: Datapath,
{
    let mut output = echo_messages_rcsga::Tree1LCF::new();
    output.set_left(deserialize_from_pkt_single_buffer_sga(
        input.get_left(),
        datapath,
    )?);
    output.set_right(deserialize_from_pkt_single_buffer_sga(
        input.get_right(),
        datapath,
    )?);
    Ok(output)
}

fn deserialize_from_pkt_single_buffer_sga<'obj, D>(
    input: &'obj echo_messages_rcsga::SingleBufferCF<'obj, D>,
    datapath: &D,
) -> Result<echo_messages_rcsga::SingleBufferCF<'obj, D>>
where
    D: Datapath,
{
    let mut output = echo_messages_rcsga::SingleBufferCF::<'obj, D>::new();
    output.set_message(dynamic_rcsga_hdr::CFBytes::new(
        input.get_message().as_bytes(),
        datapath,
    )?);
    Ok(output)
}

pub struct CornflakesEchoClient {}

impl<D> ClientCerealizeMessage<D> for CornflakesEchoClient
where
    D: Datapath,
{
    fn new() -> Self
    where
        Self: Sized,
    {
        CornflakesEchoClient {}
    }

    fn check_echoed_payload(
        &self,
        pkt: &ReceivedPkt<D>,
        bytes_to_check: (SimpleMessageType, &Vec<Vec<u8>>),
        datapath: &D,
    ) -> Result<bool> {
        let (ty, input) = bytes_to_check;
        match ty {
            SimpleMessageType::Single => {
                let obj = get_singlebuf_sga(&input[0], datapath)?;
                let mut obj_deser = echo_messages_rcsga::SingleBufferCF::new();
                obj_deser.deserialize_from_buf(pkt.seg(0).as_ref())?;
                Ok(obj.check_deep_equality(&obj_deser))
            }
            SimpleMessageType::List(_list_size) => {
                let obj = get_list_sga(&input, datapath)?;
                let mut obj_deser = echo_messages_rcsga::ListCF::new();
                obj_deser.deserialize_from_buf(pkt.seg(0).as_ref())?;
                Ok(obj.check_deep_equality(&obj_deser))
            }
            SimpleMessageType::Tree(depth) => {
                ensure!(
                    input.len() == u64::pow(2, depth.to_u32()) as usize,
                    format!(
                        "Expected bytes vec length {} for tree of depth {:?}",
                        u64::pow(2, depth.to_u32()),
                        depth
                    )
                );
                match depth {
                    TreeDepth::One => {
                        tracing::debug!("Received bytes: {:?}", pkt.seg(0).as_ref());
                        tracing::debug!("Expected bytes: {:?}", &input.as_slice());
                        let tree_ser = get_tree1l_sga(&input.as_slice(), datapath)?;
                        let mut tree_deser = echo_messages_rcsga::Tree1LCF::new();
                        tree_deser.deserialize_from_buf(pkt.seg(0).as_ref())?;
                        if tree_deser.get_bitmap_field(
                            echo_messages_rcsga::Tree1LCF::<D>::LEFT_BITMAP_IDX,
                            echo_messages_rcsga::Tree1LCF::<D>::LEFT_BITMAP_OFFSET,
                        ) != tree_ser.get_bitmap_field(
                            echo_messages_rcsga::Tree1LCF::<D>::LEFT_BITMAP_IDX,
                            echo_messages_rcsga::Tree1LCF::<D>::LEFT_BITMAP_OFFSET,
                        ) {
                            tracing::debug!(
                                "Bitmap for left doesn't match, arrived: {:?}, expected: {:?}",
                                tree_deser.get_bitmap_field(
                                    echo_messages_rcsga::Tree1LCF::<D>::LEFT_BITMAP_IDX,
                                    echo_messages_rcsga::Tree1LCF::<D>::LEFT_BITMAP_OFFSET
                                ),
                                tree_ser.get_bitmap_field(
                                    echo_messages_rcsga::Tree1LCF::<D>::LEFT_BITMAP_IDX,
                                    echo_messages_rcsga::Tree1LCF::<D>::LEFT_BITMAP_OFFSET
                                )
                            );
                            return Ok(false);
                        } else if tree_deser.get_bitmap_field(
                            echo_messages_rcsga::Tree1LCF::<D>::LEFT_BITMAP_IDX,
                            echo_messages_rcsga::Tree1LCF::<D>::LEFT_BITMAP_OFFSET,
                        ) && tree_ser.get_bitmap_field(
                            echo_messages_rcsga::Tree1LCF::<D>::LEFT_BITMAP_IDX,
                            echo_messages_rcsga::Tree1LCF::<D>::LEFT_BITMAP_OFFSET,
                        ) {
                            if !tree_deser
                                .get_left()
                                .check_deep_equality(&tree_ser.get_left())
                            {
                                tracing::debug!(
                                    "Paylod for left doesn't match: deser: {:?}, expected: {:?}",
                                    tree_deser.get_left().get_message().as_bytes(),
                                    tree_ser.get_left().get_message().as_bytes()
                                );
                                return Ok(false);
                            }
                        }

                        if tree_deser.get_bitmap_field(
                            echo_messages_rcsga::Tree1LCF::<D>::RIGHT_BITMAP_IDX,
                            echo_messages_rcsga::Tree1LCF::<D>::RIGHT_BITMAP_OFFSET,
                        ) != tree_ser.get_bitmap_field(
                            echo_messages_rcsga::Tree1LCF::<D>::RIGHT_BITMAP_IDX,
                            echo_messages_rcsga::Tree1LCF::<D>::RIGHT_BITMAP_OFFSET,
                        ) {
                            tracing::debug!(
                                "Bitmap for right doesn't match, arrived: {:?}, expected: {:?}",
                                tree_deser.get_bitmap_field(
                                    echo_messages_rcsga::Tree1LCF::<D>::RIGHT_BITMAP_IDX,
                                    echo_messages_rcsga::Tree1LCF::<D>::RIGHT_BITMAP_OFFSET
                                ),
                                tree_ser.get_bitmap_field(
                                    echo_messages_rcsga::Tree1LCF::<D>::RIGHT_BITMAP_IDX,
                                    echo_messages_rcsga::Tree1LCF::<D>::RIGHT_BITMAP_OFFSET
                                )
                            );
                            return Ok(false);
                        } else if tree_deser.get_bitmap_field(
                            echo_messages_rcsga::Tree1LCF::<D>::RIGHT_BITMAP_IDX,
                            echo_messages_rcsga::Tree1LCF::<D>::RIGHT_BITMAP_OFFSET,
                        ) && tree_ser.get_bitmap_field(
                            echo_messages_rcsga::Tree1LCF::<D>::RIGHT_BITMAP_IDX,
                            echo_messages_rcsga::Tree1LCF::<D>::RIGHT_BITMAP_OFFSET,
                        ) {
                            if !tree_deser
                                .get_right()
                                .check_deep_equality(&tree_ser.get_right())
                            {
                                tracing::debug!(
                                    "Paylod for right doesn't match: deser: {:?}, expected: {:?}",
                                    tree_deser.get_right().get_message().as_bytes(),
                                    tree_ser.get_right().get_message().as_bytes()
                                );
                                return Ok(false);
                            }
                        }

                        Ok(tree_ser.check_deep_equality(&tree_deser))
                    }
                    TreeDepth::Two => {
                        let tree_ser = get_tree2l_sga(&input.as_slice(), datapath)?;
                        let mut tree_deser = echo_messages_rcsga::Tree2LCF::new();
                        tree_deser.deserialize_from_buf(pkt.seg(0).as_ref())?;
                        Ok(tree_ser.check_deep_equality(&tree_deser))
                    }
                    TreeDepth::Three => {
                        let tree_ser = get_tree3l_sga(&input.as_slice(), datapath)?;
                        let mut tree_deser = echo_messages_rcsga::Tree3LCF::new();
                        tree_deser.deserialize_from_buf(pkt.seg(0).as_ref())?;
                        Ok(tree_ser.check_deep_equality(&tree_deser))
                    }
                    TreeDepth::Four => {
                        let tree_ser = get_tree4l_sga(&input.as_slice(), datapath)?;
                        let mut tree_deser = echo_messages_rcsga::Tree4LCF::new();
                        tree_deser.deserialize_from_buf(pkt.seg(0).as_ref())?;
                        Ok(tree_ser.check_deep_equality(&tree_deser))
                    }
                    TreeDepth::Five => {
                        let tree_ser = get_tree5l_sga(&input.as_slice(), datapath)?;
                        let mut tree_deser = echo_messages_rcsga::Tree5LCF::new();
                        tree_deser.deserialize_from_buf(pkt.seg(0).as_ref())?;
                        Ok(tree_ser.check_deep_equality(&tree_deser))
                    }
                }
            }
        }
    }

    fn get_serialized_bytes(
        ty: SimpleMessageType,
        input: &Vec<Vec<u8>>,
        datapath: &D,
    ) -> Result<Vec<u8>> {
        match ty {
            SimpleMessageType::Single => {
                let obj = get_singlebuf_sga(&input[0], datapath)?;
                obj.serialize_to_owned(datapath)
            }
            SimpleMessageType::List(_list_size) => {
                let obj = get_list_sga(&input, datapath)?;
                obj.serialize_to_owned(datapath)
            }
            SimpleMessageType::Tree(depth) => {
                ensure!(
                    input.len() == u64::pow(2, depth.to_u32()) as usize,
                    format!(
                        "Expected bytes vec length {} for tree of depth {:?}",
                        u64::pow(2, depth.to_u32()),
                        depth
                    )
                );
                match depth {
                    TreeDepth::One => {
                        let tree_ser = get_tree1l_sga(&input.as_slice(), datapath)?;
                        tree_ser.serialize_to_owned(datapath)
                    }
                    TreeDepth::Two => {
                        let tree_ser = get_tree2l_sga(&input.as_slice(), datapath)?;
                        tree_ser.serialize_to_owned(datapath)
                    }
                    TreeDepth::Three => {
                        let tree_ser = get_tree3l_sga(&input.as_slice(), datapath)?;
                        tree_ser.serialize_to_owned(datapath)
                    }
                    TreeDepth::Four => {
                        let tree_ser = get_tree4l_sga(&input.as_slice(), datapath)?;
                        tree_ser.serialize_to_owned(datapath)
                    }
                    TreeDepth::Five => {
                        let tree_ser = get_tree5l_sga(&input.as_slice(), datapath)?;
                        tree_ser.serialize_to_owned(datapath)
                    }
                }
            }
        }
    }
}

fn get_list_sga<'obj, D>(
    bytes_vec: &'obj [Vec<u8>],
    datapath: &D,
) -> Result<echo_messages_rcsga::ListCF<'obj, D>>
where
    D: Datapath,
{
    let mut obj = echo_messages_rcsga::ListCF::new();
    obj.init_messages(bytes_vec.len());
    let list = obj.get_mut_messages();
    for payload in bytes_vec.iter() {
        list.append(dynamic_rcsga_hdr::CFBytes::new(
            &payload.as_slice(),
            datapath,
        )?);
    }
    Ok(obj)
}

fn get_tree5l_sga<'obj, D>(
    bytes_vec: &'obj [Vec<u8>],
    datapath: &D,
) -> Result<echo_messages_rcsga::Tree5LCF<'obj, D>>
where
    D: Datapath,
{
    let mut tree = echo_messages_rcsga::Tree5LCF::new();
    tree.set_left(get_tree4l_sga(&bytes_vec[0..16], datapath)?);
    tree.set_right(get_tree4l_sga(&bytes_vec[16..32], datapath)?);
    Ok(tree)
}

fn get_tree4l_sga<'obj, D>(
    bytes_vec: &'obj [Vec<u8>],
    datapath: &D,
) -> Result<echo_messages_rcsga::Tree4LCF<'obj, D>>
where
    D: Datapath,
{
    let mut tree = echo_messages_rcsga::Tree4LCF::new();
    tree.set_left(get_tree3l_sga(&bytes_vec[0..8], datapath)?);
    tree.set_right(get_tree3l_sga(&bytes_vec[8..16], datapath)?);
    Ok(tree)
}

fn get_tree3l_sga<'obj, D>(
    bytes_vec: &'obj [Vec<u8>],
    datapath: &D,
) -> Result<echo_messages_rcsga::Tree3LCF<'obj, D>>
where
    D: Datapath,
{
    let mut tree = echo_messages_rcsga::Tree3LCF::new();
    tree.set_left(get_tree2l_sga(&bytes_vec[0..4], datapath)?);
    tree.set_right(get_tree2l_sga(&bytes_vec[4..8], datapath)?);
    Ok(tree)
}

fn get_tree2l_sga<'obj, D>(
    bytes_vec: &'obj [Vec<u8>],
    datapath: &D,
) -> Result<echo_messages_rcsga::Tree2LCF<'obj, D>>
where
    D: Datapath,
{
    let mut tree = echo_messages_rcsga::Tree2LCF::new();
    tree.set_left(get_tree1l_sga(&bytes_vec[0..2], datapath)?);
    tree.set_right(get_tree1l_sga(&bytes_vec[2..4], datapath)?);
    Ok(tree)
}

fn get_tree1l_sga<'obj, D>(
    bytes_vec: &'obj [Vec<u8>],
    datapath: &D,
) -> Result<echo_messages_rcsga::Tree1LCF<'obj, D>>
where
    D: Datapath,
{
    let mut tree = echo_messages_rcsga::Tree1LCF::new();
    tree.set_left(get_singlebuf_sga(&bytes_vec[0], datapath)?);
    tree.set_right(get_singlebuf_sga(&bytes_vec[1], datapath)?);
    Ok(tree)
}

fn get_singlebuf_sga<'obj, D>(
    bytes_vec: &'obj Vec<u8>,
    datapath: &D,
) -> Result<echo_messages_rcsga::SingleBufferCF<'obj, D>>
where
    D: Datapath,
{
    let mut obj = echo_messages_rcsga::SingleBufferCF::new();
    obj.set_message(dynamic_rcsga_hdr::CFBytes::new(
        &bytes_vec.as_slice(),
        datapath,
    )?);
    Ok(obj)
}
