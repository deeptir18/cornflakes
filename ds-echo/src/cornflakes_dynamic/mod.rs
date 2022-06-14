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
    dynamic_sga_hdr,
    dynamic_sga_hdr::SgaHeaderRepr,
    state_machine::server::ServerSM,
    ArenaOrderedSga, OrderedSga,
};
use cornflakes_utils::{SimpleMessageType, TreeDepth};
#[cfg(feature = "profiler")]
use perftools;
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
    D:,
{
    type Datapath = D;
    fn push_buf_type(&self) -> PushBufType {
        self.push_buf_type
    }

    fn process_requests_object(
        &mut self,
        pkts: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        datapath: &mut Self::Datapath,
    ) -> Result<()> {
        let objects_iter = pkts.iter().map(|pkt| {
            let msg_type = read_message_type(&pkt)?;
            match msg_type {
            SimpleMessageType::Single => {
                let mut single_deser = echo_messages_sga::SingleBufferCF::new();
                let mut single_ser = echo_messages_sga::SingleBufferCF::new();
                tracing::debug!(pkt_data =? &pkt.seg(0).as_ref()[REQ_TYPE_SIZE..], "Incoming packet data");
                tracing::debug!(len = pkt.data_len(), "Incoming packet length");
                {
                    #[cfg(feature = "profiler")]
                    perftools::timer!("Deserialize pkt");
                    single_deser.deserialize(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
                }
                {
                    #[cfg(feature = "profiler")]
                    perftools::timer!("Set message");
                    tracing::debug!(get_msg =? single_deser.get_message().get_ptr());
                    single_ser.set_message(dynamic_sga_hdr::CFBytes::new(
                        single_deser.get_message().get_ptr(),
                    ));
                    tracing::debug!(set_msg =? single_ser.get_message().get_ptr());
                }
                Ok((pkt.msg_id(), pkt.conn_id(), single_ser))
            }
            _ => {
                unimplemented!();
            }
            }
        });

        datapath.serialize_and_send(objects_iter)?;
        Ok(())
    }

    fn process_requests_ordered_sga(
        &mut self,
        pkts: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        datapath: &mut Self::Datapath,
    ) -> Result<()> {
        tracing::debug!("Processing packet");
        let mut single_deser = echo_messages_sga::SingleBufferCF::new();
        let mut single_ser = echo_messages_sga::SingleBufferCF::new();
        let mut list_deser = echo_messages_sga::ListCF::new();
        let mut list_ser = echo_messages_sga::ListCF::new();
        let sga_results_iter = pkts.iter().map(|pkt| {
            let message_type = read_message_type(pkt)?;
            match message_type {
            SimpleMessageType::Single => {
                tracing::debug!(pkt_data =? &pkt.seg(0).as_ref()[REQ_TYPE_SIZE..], "Incoming packet data");
                tracing::debug!(len = pkt.data_len(), "Incoming packet length");
                {
                    #[cfg(feature = "profiler")]
                    perftools::timer!("Deserialize pkt");
                    single_deser.deserialize(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
                }
                {
                    #[cfg(feature = "profiler")]
                    perftools::timer!("Set message");
                    tracing::debug!(get_msg =? single_deser.get_message().get_ptr());
                    single_ser.set_message(dynamic_sga_hdr::CFBytes::new(
                        single_deser.get_message().get_ptr(),
                    ));
                    tracing::debug!(set_msg =? single_ser.get_message().get_ptr());
                }
                let mut ordered_sga = {
                    #[cfg(feature = "profiler")]
                    perftools::timer!("Allocate sga");
                    OrderedSga::allocate(single_ser.num_scatter_gather_entries())
                };
                {
                    #[cfg(feature = "profiler")]
                    perftools::timer!("Serialize into sga");
                    single_ser.serialize_into_sga(&mut ordered_sga, datapath)?;
                }
                {
                    #[cfg(feature = "profiler")]
                    perftools::timer!("Clear bitmap");
                    single_deser.clear_bitmap();
                    single_ser.clear_bitmap();
                }
                Ok((pkt.msg_id(), pkt.conn_id(), ordered_sga))
            }
            SimpleMessageType::List(_list_elts) => {
                list_deser.deserialize(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;

                list_ser.init_messages(list_deser.get_messages().len());
                let messages = list_ser.get_mut_messages();
                for elt in list_deser.get_messages().iter() {
                    messages.append(dynamic_sga_hdr::CFBytes::new(elt.get_ptr()));
                }

                let mut ordered_sga = {
                    #[cfg(feature = "profiler")]
                    perftools::timer!("Allocate normal sga");
                    OrderedSga::allocate(list_ser.num_scatter_gather_entries())
                };
                list_ser.serialize_into_sga(&mut ordered_sga, datapath)?;
                list_ser.clear_bitmap();
                list_deser.clear_bitmap();
                Ok((pkt.msg_id(), pkt.conn_id(), ordered_sga))
            }

            SimpleMessageType::Tree(depth) => match depth {
                TreeDepth::One => {
                    let mut tree_deser = echo_messages_sga::Tree1LCF::new();
                    tree_deser.deserialize(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
                    let tree_ser = deserialize_tree1l_sga(&tree_deser)?;
                    let mut ordered_sga =
                        OrderedSga::allocate(tree_ser.num_scatter_gather_entries());
                    tree_ser.serialize_into_sga(&mut ordered_sga, datapath)?;
                    Ok((pkt.msg_id(), pkt.conn_id(), ordered_sga))
                }
                TreeDepth::Two => {
                    let mut tree_deser = echo_messages_sga::Tree2LCF::new();
                    tree_deser.deserialize(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
                    let tree_ser = deserialize_tree2l_sga(&tree_deser)?;
                    let mut ordered_sga =
                        OrderedSga::allocate(tree_ser.num_scatter_gather_entries());
                    tree_ser.serialize_into_sga(&mut ordered_sga, datapath)?;
                    Ok((pkt.msg_id(), pkt.conn_id(), ordered_sga))
                }
                TreeDepth::Three => {
                    let mut tree_deser = echo_messages_sga::Tree3LCF::new();
                    tree_deser.deserialize(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
                    let tree_ser = deserialize_tree3l_sga(&tree_deser)?;
                    let mut ordered_sga =
                        OrderedSga::allocate(tree_ser.num_scatter_gather_entries());
                    tree_ser.serialize_into_sga(&mut ordered_sga, datapath)?;
                    Ok((pkt.msg_id(), pkt.conn_id(), ordered_sga))
                }
                TreeDepth::Four => {
                    let mut tree_deser = echo_messages_sga::Tree4LCF::new();
                    tree_deser.deserialize(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
                    let tree_ser = deserialize_tree4l_sga(&tree_deser)?;
                    let mut ordered_sga =
                        OrderedSga::allocate(tree_ser.num_scatter_gather_entries());
                    tree_ser.serialize_into_sga(&mut ordered_sga, datapath)?;
                    Ok((pkt.msg_id(), pkt.conn_id(), ordered_sga))
                }
                TreeDepth::Five => {
                    let mut tree_deser = echo_messages_sga::Tree5LCF::new();
                    tree_deser.deserialize(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
                    let tree_ser = deserialize_tree5l_sga(&tree_deser)?;
                    let mut ordered_sga =
                        OrderedSga::allocate(tree_ser.num_scatter_gather_entries());
                    tree_ser.serialize_into_sga(&mut ordered_sga, datapath)?;
                    Ok((pkt.msg_id(), pkt.conn_id(), ordered_sga))
                }
            },
            }
        });
        {
            #[cfg(feature = "profiler")]
            perftools::timer!("push iterator");
            datapath.push_ordered_sgas_iterator(sga_results_iter)?;
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
                    let mut single_deser = echo_messages_sga::SingleBufferCF::new();
                    let mut single_ser = echo_messages_sga::SingleBufferCF::new();
                    tracing::debug!(pkt_data =? &pkt.seg(0).as_ref()[REQ_TYPE_SIZE..], "Incoming packet data");
                    tracing::debug!(len = pkt.data_len(), "Incoming packet length");
                    {
                        #[cfg(feature = "profiler")]
                        perftools::timer!("Deserialize pkt");
                        single_deser.deserialize(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
                    }
                    {
                        #[cfg(feature = "profiler")]
                        perftools::timer!("Set message");
                        tracing::debug!(get_msg =? single_deser.get_message().get_ptr());
                        single_ser.set_message(dynamic_sga_hdr::CFBytes::new(
                            single_deser.get_message().get_ptr(),
                        ));
                        tracing::debug!(set_msg =? single_ser.get_message().get_ptr());
                    }
                    let mut ordered_sga = {
                        #[cfg(feature = "profiler")]
                        perftools::timer!("Allocate sga");
                        ArenaOrderedSga::allocate(single_ser.num_scatter_gather_entries(), &arena)
                    };
                    {
                        #[cfg(feature = "profiler")]
                        perftools::timer!("Serialize into sga");
                        single_ser.partially_serialize_into_arena_sga(&mut ordered_sga, &arena)?;
                    }
                    {
                        #[cfg(feature = "profiler")]
                        perftools::timer!("Clear bitmap");
                        single_deser.clear_bitmap();
                        single_ser.clear_bitmap();
                    }
                    datapath.queue_arena_ordered_sga(
                        (pkt.msg_id(), pkt.conn_id(), ordered_sga),
                        end_batch,
                    )?;
                }
                SimpleMessageType::List(_list_elts) => {
                    let mut list_deser = echo_messages_sga::ListCF::new();
                    let mut list_ser = echo_messages_sga::ListCF::new();
                    list_deser.deserialize(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;

                    list_ser.init_messages(list_deser.get_messages().len());
                    let messages = list_ser.get_mut_messages();
                    for elt in list_deser.get_messages().iter() {
                        messages.append(dynamic_sga_hdr::CFBytes::new(elt.get_ptr()));
                    }

                    let mut ordered_sga = {
                        #[cfg(feature = "profiler")]
                        perftools::timer!("Allocate arena sga");
                        ArenaOrderedSga::allocate(list_ser.num_scatter_gather_entries(), &arena)
                    };
                    list_ser.partially_serialize_into_arena_sga(&mut ordered_sga, &arena)?;
                    list_ser.clear_bitmap();
                    list_deser.clear_bitmap();
                    datapath.queue_arena_ordered_sga(
                        (pkt.msg_id(), pkt.conn_id(), ordered_sga),
                        end_batch,
                    )?;
                }
                SimpleMessageType::Tree(depth) => match depth {
                    TreeDepth::One => {
                        let mut tree_deser = echo_messages_sga::Tree1LCF::new();
                        tree_deser.deserialize(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
                        let tree_ser = deserialize_tree1l_sga(&tree_deser)?;
                        let mut ordered_sga = ArenaOrderedSga::allocate(
                            tree_ser.num_scatter_gather_entries(),
                            &arena,
                        );
                        tree_ser.partially_serialize_into_arena_sga(&mut ordered_sga, &arena)?;
                        datapath.queue_arena_ordered_sga(
                            (pkt.msg_id(), pkt.conn_id(), ordered_sga),
                            end_batch,
                        )?;
                    }
                    TreeDepth::Two => {
                        let mut tree_deser = echo_messages_sga::Tree2LCF::new();
                        tree_deser.deserialize(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
                        let tree_ser = deserialize_tree2l_sga(&tree_deser)?;
                        let mut ordered_sga = ArenaOrderedSga::allocate(
                            tree_ser.num_scatter_gather_entries(),
                            &arena,
                        );
                        tree_ser.partially_serialize_into_arena_sga(&mut ordered_sga, &arena)?;
                        datapath.queue_arena_ordered_sga(
                            (pkt.msg_id(), pkt.conn_id(), ordered_sga),
                            end_batch,
                        )?;
                    }
                    TreeDepth::Three => {
                        let mut tree_deser = echo_messages_sga::Tree3LCF::new();
                        tree_deser.deserialize(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
                        let tree_ser = deserialize_tree3l_sga(&tree_deser)?;
                        let mut ordered_sga = ArenaOrderedSga::allocate(
                            tree_ser.num_scatter_gather_entries(),
                            &arena,
                        );
                        tree_ser.partially_serialize_into_arena_sga(&mut ordered_sga, &arena)?;
                        datapath.queue_arena_ordered_sga(
                            (pkt.msg_id(), pkt.conn_id(), ordered_sga),
                            end_batch,
                        )?;
                    }
                    TreeDepth::Four => {
                        let mut tree_deser = echo_messages_sga::Tree4LCF::new();
                        tree_deser.deserialize(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
                        let tree_ser = deserialize_tree4l_sga(&tree_deser)?;
                        let mut ordered_sga = ArenaOrderedSga::allocate(
                            tree_ser.num_scatter_gather_entries(),
                            &arena,
                        );
                        tree_ser.partially_serialize_into_arena_sga(&mut ordered_sga, &arena)?;
                        datapath.queue_arena_ordered_sga(
                            (pkt.msg_id(), pkt.conn_id(), ordered_sga),
                            end_batch,
                        )?;
                    }
                    TreeDepth::Five => {
                        let mut tree_deser = echo_messages_sga::Tree5LCF::new();
                        tree_deser.deserialize(&pkt.seg(0).as_ref()[REQ_TYPE_SIZE..])?;
                        let tree_ser = deserialize_tree5l_sga(&tree_deser)?;
                        let mut ordered_sga = ArenaOrderedSga::allocate(
                            tree_ser.num_scatter_gather_entries(),
                            &arena,
                        );
                        tree_ser.partially_serialize_into_arena_sga(&mut ordered_sga, &arena)?;
                        datapath.queue_arena_ordered_sga(
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
        _sga: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        _datapath: &mut Self::Datapath,
    ) -> Result<()> {
        unimplemented!();
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

fn deserialize_tree5l_sga<'obj>(
    input: &echo_messages_sga::Tree5LCF<'obj>,
) -> Result<echo_messages_sga::Tree5LCF<'obj>> {
    let mut output = echo_messages_sga::Tree5LCF::new();
    output.set_left(deserialize_tree4l_sga(input.get_left())?);
    output.set_right(deserialize_tree4l_sga(input.get_right())?);
    Ok(output)
}

fn deserialize_tree4l_sga<'obj>(
    input: &echo_messages_sga::Tree4LCF<'obj>,
) -> Result<echo_messages_sga::Tree4LCF<'obj>> {
    let mut output = echo_messages_sga::Tree4LCF::new();
    output.set_left(deserialize_tree3l_sga(input.get_left())?);
    output.set_right(deserialize_tree3l_sga(input.get_right())?);
    Ok(output)
}

fn deserialize_tree3l_sga<'obj>(
    input: &echo_messages_sga::Tree3LCF<'obj>,
) -> Result<echo_messages_sga::Tree3LCF<'obj>> {
    let mut output = echo_messages_sga::Tree3LCF::new();
    output.set_left(deserialize_tree2l_sga(input.get_left())?);
    output.set_right(deserialize_tree2l_sga(input.get_right())?);
    Ok(output)
}

fn deserialize_tree2l_sga<'obj>(
    input: &echo_messages_sga::Tree2LCF<'obj>,
) -> Result<echo_messages_sga::Tree2LCF<'obj>> {
    let mut output = echo_messages_sga::Tree2LCF::new();
    output.set_left(deserialize_tree1l_sga(input.get_left())?);
    output.set_right(deserialize_tree1l_sga(input.get_right())?);
    Ok(output)
}

fn deserialize_tree1l_sga<'obj>(
    input: &echo_messages_sga::Tree1LCF<'obj>,
) -> Result<echo_messages_sga::Tree1LCF<'obj>> {
    let mut output = echo_messages_sga::Tree1LCF::new();
    output.set_left(deserialize_single_buffer_sga(input.get_left())?);
    output.set_right(deserialize_single_buffer_sga(input.get_right())?);
    Ok(output)
}

fn deserialize_single_buffer_sga<'obj>(
    input: &echo_messages_sga::SingleBufferCF<'obj>,
) -> Result<echo_messages_sga::SingleBufferCF<'obj>> {
    let mut output = echo_messages_sga::SingleBufferCF::<'obj>::new();
    output.set_message(dynamic_sga_hdr::CFBytes::new(input.get_message().get_ptr()));
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
    ) -> Result<bool> {
        let (ty, input) = bytes_to_check;
        match ty {
            SimpleMessageType::Single => {
                let obj = get_singlebuf_sga(&input[0])?;
                let mut obj_deser = echo_messages_sga::SingleBufferCF::new();
                obj_deser.deserialize(pkt.seg(0).as_ref())?;
                Ok(obj.check_deep_equality(&obj_deser))
            }
            SimpleMessageType::List(_list_size) => {
                let obj = get_list_sga(&input)?;
                let mut obj_deser = echo_messages_sga::ListCF::new();
                obj_deser.deserialize(pkt.seg(0).as_ref())?;
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
                        let tree_ser = get_tree1l_sga(&input.as_slice())?;
                        let mut tree_deser = echo_messages_sga::Tree1LCF::new();
                        tree_deser.deserialize(pkt.seg(0).as_ref())?;
                        Ok(tree_ser.check_deep_equality(&tree_deser))
                    }
                    TreeDepth::Two => {
                        let tree_ser = get_tree2l_sga(&input.as_slice())?;
                        let mut tree_deser = echo_messages_sga::Tree2LCF::new();
                        tree_deser.deserialize(pkt.seg(0).as_ref())?;
                        Ok(tree_ser.check_deep_equality(&tree_deser))
                    }
                    TreeDepth::Three => {
                        let tree_ser = get_tree3l_sga(&input.as_slice())?;
                        let mut tree_deser = echo_messages_sga::Tree3LCF::new();
                        tree_deser.deserialize(pkt.seg(0).as_ref())?;
                        Ok(tree_ser.check_deep_equality(&tree_deser))
                    }
                    TreeDepth::Four => {
                        let tree_ser = get_tree4l_sga(&input.as_slice())?;
                        let mut tree_deser = echo_messages_sga::Tree4LCF::new();
                        tree_deser.deserialize(pkt.seg(0).as_ref())?;
                        Ok(tree_ser.check_deep_equality(&tree_deser))
                    }
                    TreeDepth::Five => {
                        let tree_ser = get_tree5l_sga(&input.as_slice())?;
                        let mut tree_deser = echo_messages_sga::Tree5LCF::new();
                        tree_deser.deserialize(pkt.seg(0).as_ref())?;
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
                let obj = get_singlebuf_sga(&input[0])?;
                obj.serialize_to_owned(datapath)
            }
            SimpleMessageType::List(_list_size) => {
                let obj = get_list_sga(&input)?;
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
                        let tree_ser = get_tree1l_sga(&input.as_slice())?;
                        tree_ser.serialize_to_owned(datapath)
                    }
                    TreeDepth::Two => {
                        let tree_ser = get_tree2l_sga(&input.as_slice())?;
                        tree_ser.serialize_to_owned(datapath)
                    }
                    TreeDepth::Three => {
                        let tree_ser = get_tree3l_sga(&input.as_slice())?;
                        tree_ser.serialize_to_owned(datapath)
                    }
                    TreeDepth::Four => {
                        let tree_ser = get_tree4l_sga(&input.as_slice())?;
                        tree_ser.serialize_to_owned(datapath)
                    }
                    TreeDepth::Five => {
                        let tree_ser = get_tree5l_sga(&input.as_slice())?;
                        tree_ser.serialize_to_owned(datapath)
                    }
                }
            }
        }
    }
}

fn get_list_sga<'obj>(bytes_vec: &'obj [Vec<u8>]) -> Result<echo_messages_sga::ListCF<'obj>> {
    let mut obj = echo_messages_sga::ListCF::new();
    obj.init_messages(bytes_vec.len());
    let list = obj.get_mut_messages();
    for payload in bytes_vec.iter() {
        list.append(dynamic_sga_hdr::CFBytes::new(&payload.as_slice()));
    }
    Ok(obj)
}

fn get_tree5l_sga<'obj>(bytes_vec: &'obj [Vec<u8>]) -> Result<echo_messages_sga::Tree5LCF<'obj>> {
    let mut tree = echo_messages_sga::Tree5LCF::new();
    tree.set_left(get_tree4l_sga(&bytes_vec[0..16])?);
    tree.set_right(get_tree4l_sga(&bytes_vec[16..32])?);
    Ok(tree)
}

fn get_tree4l_sga<'obj>(bytes_vec: &'obj [Vec<u8>]) -> Result<echo_messages_sga::Tree4LCF<'obj>> {
    let mut tree = echo_messages_sga::Tree4LCF::new();
    tree.set_left(get_tree3l_sga(&bytes_vec[0..8])?);
    tree.set_right(get_tree3l_sga(&bytes_vec[8..16])?);
    Ok(tree)
}

fn get_tree3l_sga<'obj>(bytes_vec: &'obj [Vec<u8>]) -> Result<echo_messages_sga::Tree3LCF<'obj>> {
    let mut tree = echo_messages_sga::Tree3LCF::new();
    tree.set_left(get_tree2l_sga(&bytes_vec[0..4])?);
    tree.set_right(get_tree2l_sga(&bytes_vec[4..8])?);
    Ok(tree)
}

fn get_tree2l_sga<'obj>(bytes_vec: &'obj [Vec<u8>]) -> Result<echo_messages_sga::Tree2LCF<'obj>> {
    let mut tree = echo_messages_sga::Tree2LCF::new();
    tree.set_left(get_tree1l_sga(&bytes_vec[0..2])?);
    tree.set_right(get_tree1l_sga(&bytes_vec[2..4])?);
    Ok(tree)
}

fn get_tree1l_sga<'obj>(bytes_vec: &'obj [Vec<u8>]) -> Result<echo_messages_sga::Tree1LCF<'obj>> {
    let mut tree = echo_messages_sga::Tree1LCF::new();
    tree.set_left(get_singlebuf_sga(&bytes_vec[0])?);
    tree.set_right(get_singlebuf_sga(&bytes_vec[1])?);
    Ok(tree)
}

fn get_singlebuf_sga<'obj>(
    bytes_vec: &'obj Vec<u8>,
) -> Result<echo_messages_sga::SingleBufferCF<'obj>> {
    let mut obj = echo_messages_sga::SingleBufferCF::new();
    obj.set_message(dynamic_sga_hdr::CFBytes::new(&bytes_vec.as_slice()));
    Ok(obj)
}
