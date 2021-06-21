pub mod ds_query_messages {
    include!(concat!(env!("OUT_DIR"), "/ds_query_cf_fixed.rs"));
}
use super::{
    get_equal_fields, get_payloads_as_vec, init_payloads, init_payloads_as_vec, CerealizeClient,
    CerealizeMessage,
};
use color_eyre::eyre::Result;
use cornflakes_codegen::utils::fixed_hdr::*;
use cornflakes_libos::{
    dpdk_bindings::rte_memcpy_wrapper as rte_memcpy, mem::MmapMetadata, Cornflake, Datapath,
    PtrAttributes, ReceivedPacket, ScatterGather,
};
use cornflakes_utils::{SimpleMessageType, TreeDepth};
use std::slice;

pub struct CornflakesFixedSerializer {
    message_type: SimpleMessageType,
    context_size: usize,
    payload_ptrs: Vec<(*const u8, usize)>,
    deserialize_received: bool,
    use_native_buffers: bool,
    prepend_header: bool,
}
fn check_tree5l<'a>(
    indices: &[usize],
    payloads: &Vec<Vec<u8>>,
    object: &ds_query_messages::Tree5LCF<'a>,
) {
    check_tree4l(&indices[0..16], payloads, &object.get_left());
    check_tree4l(&indices[16..32], payloads, &object.get_right());
}

fn check_tree4l<'a>(
    indices: &[usize],
    payloads: &Vec<Vec<u8>>,
    object: &ds_query_messages::Tree4LCF<'a>,
) {
    check_tree3l(&indices[0..8], payloads, &object.get_left());
    check_tree3l(&indices[8..16], payloads, &object.get_right());
}

fn check_tree3l<'a>(
    indices: &[usize],
    payloads: &Vec<Vec<u8>>,
    object: &ds_query_messages::Tree3LCF<'a>,
) {
    check_tree2l(&indices[0..4], payloads, &object.get_left());
    check_tree2l(&indices[4..8], payloads, &object.get_right());
}

fn check_tree2l<'a>(
    indices: &[usize],
    payloads: &Vec<Vec<u8>>,
    object: &ds_query_messages::Tree2LCF<'a>,
) {
    check_tree1l(&indices[0..2], payloads, &object.get_left());
    check_tree1l(&indices[2..4], payloads, &object.get_right());
}

fn check_tree1l<'a>(
    indices: &[usize],
    payloads: &Vec<Vec<u8>>,
    object: &ds_query_messages::Tree1LCF<'a>,
) {
    check_single_buffer(indices[0], payloads, &object.get_left());
    check_single_buffer(indices[1], payloads, &object.get_right());
}

fn check_single_buffer<'a>(
    idx: usize,
    payloads: &Vec<Vec<u8>>,
    object: &ds_query_messages::SingleBufferCF<'a>,
) {
    assert!(object.get_message().len() == payloads[idx].len());
    assert!(object.get_message().to_bytes_vec() == payloads[idx].clone())
}

fn get_tree5l_message<'a>(
    indices: &[usize],
    payloads: &Vec<&'a [u8]>,
) -> ds_query_messages::Tree5LCF<'a> {
    let mut tree_5l = ds_query_messages::Tree5LCF::new();
    tree_5l.set_left(get_tree4l_message(&indices[0..16], payloads));
    tree_5l.set_right(get_tree4l_message(&indices[16..32], payloads));
    tree_5l
}

fn get_tree4l_message<'a>(
    indices: &[usize],
    payloads: &Vec<&'a [u8]>,
) -> ds_query_messages::Tree4LCF<'a> {
    let mut tree_4l = ds_query_messages::Tree4LCF::new();
    tree_4l.set_left(get_tree3l_message(&indices[0..8], payloads));
    tree_4l.set_right(get_tree3l_message(&indices[8..16], payloads));
    tree_4l
}

fn get_tree3l_message<'a>(
    indices: &[usize],
    payloads: &Vec<&'a [u8]>,
) -> ds_query_messages::Tree3LCF<'a> {
    let mut tree_3l = ds_query_messages::Tree3LCF::new();
    tree_3l.set_left(get_tree2l_message(&indices[0..4], payloads));
    tree_3l.set_right(get_tree2l_message(&indices[4..8], payloads));
    tree_3l
}

fn get_tree2l_message<'a>(
    indices: &[usize],
    payloads: &Vec<&'a [u8]>,
) -> ds_query_messages::Tree2LCF<'a> {
    let mut tree_2l = ds_query_messages::Tree2LCF::new();
    tree_2l.set_left(get_tree1l_message(&indices[0..2], payloads));
    tree_2l.set_right(get_tree1l_message(&indices[2..4], payloads));
    tree_2l
}

fn get_tree1l_message<'a>(
    indices: &[usize],
    payloads: &Vec<&'a [u8]>,
) -> ds_query_messages::Tree1LCF<'a> {
    let mut tree_1l = ds_query_messages::Tree1LCF::new();
    tree_1l.set_left(get_single_buffer_message(indices[0], payloads));
    tree_1l.set_right(get_single_buffer_message(indices[1], payloads));
    tree_1l
}

fn get_single_buffer_message<'a>(
    idx: usize,
    payloads: &Vec<&'a [u8]>,
) -> ds_query_messages::SingleBufferCF<'a> {
    let mut single_buffer_cf = ds_query_messages::SingleBufferCF::new();
    single_buffer_cf.set_message(CFBytes::new(&payloads[idx]));
    single_buffer_cf
}

fn context_size(message_type: SimpleMessageType, size: usize) -> usize {
    let payloads_vec = init_payloads_as_vec(&get_equal_fields(message_type, size));
    let payloads: Vec<&[u8]> = payloads_vec.iter().map(|vec| vec.as_slice()).collect();

    match message_type {
        SimpleMessageType::Single => {
            assert!(payloads.len() == 1);
            let mut single_buffer_cf = ds_query_messages::SingleBufferCF::new();
            single_buffer_cf.set_message(CFBytes::new(&payloads[0]));
            tracing::debug!(
                "Context size: {}",
                single_buffer_cf.init_header_buffer().len()
            );
            single_buffer_cf.init_header_buffer().len()
        }
        SimpleMessageType::List(list_size) => {
            assert!(payloads.len() == list_size);
            let mut list_cf = ds_query_messages::ListCF::new();
            list_cf.init_messages(list_size);
            let list_ptr = list_cf.get_mut_messages();
            for payload in payloads.iter() {
                list_ptr.append(CFBytes::new(&payload));
            }
            tracing::debug!("Context size: {}", list_cf.init_header_buffer().len());
            list_cf.init_header_buffer().len()
        }
        SimpleMessageType::Tree(depth) => match depth {
            TreeDepth::One => {
                assert!(payloads.len() == 2);
                let tree_cf = get_tree1l_message(&[0, 1], &payloads);
                tracing::debug!("Context size: {}", tree_cf.init_header_buffer().len());
                tree_cf.init_header_buffer().len()
            }
            TreeDepth::Two => {
                assert!(payloads.len() == 4);
                let tree_cf = get_tree2l_message(&[0, 1, 2, 3], &payloads);
                tracing::debug!("Context size: {}", tree_cf.init_header_buffer().len());
                tree_cf.init_header_buffer().len()
            }
            TreeDepth::Three => {
                assert!(payloads.len() == 8);
                let indices: Vec<usize> = (0usize..8usize).collect();
                let tree_cf = get_tree3l_message(indices.as_slice(), &payloads);
                tracing::debug!("Context size: {}", tree_cf.init_header_buffer().len());
                tree_cf.init_header_buffer().len()
            }
            TreeDepth::Four => {
                assert!(payloads.len() == 16);
                let indices: Vec<usize> = (0usize..16usize).collect();
                let tree_cf = get_tree4l_message(indices.as_slice(), &payloads);
                tracing::debug!("Context size: {}", tree_cf.init_header_buffer().len());
                tree_cf.init_header_buffer().len()
            }
            TreeDepth::Five => {
                assert!(payloads.len() == 32);
                let indices: Vec<usize> = (0usize..32usize).collect();
                let tree_cf = get_tree5l_message(indices.as_slice(), &payloads);
                tracing::debug!("Context size: {}", tree_cf.init_header_buffer().len());
                tree_cf.init_header_buffer().len()
            }
        },
    }
}

impl<D> CerealizeMessage<D> for CornflakesFixedSerializer
where
    D: Datapath,
{
    type Ctx = (Vec<u8>, Vec<D::DatapathPkt>);

    fn new(
        message_type: SimpleMessageType,
        field_sizes: Vec<usize>,
        mmap_metadata: MmapMetadata,
        deserialize_received: bool,
        use_native_buffers: bool,
        prepend_header: bool,
    ) -> Result<CornflakesFixedSerializer> {
        let payload_ptrs = init_payloads(&field_sizes, &mmap_metadata)?;
        let total_size = field_sizes.iter().sum();
        Ok(CornflakesFixedSerializer {
            message_type: message_type,
            context_size: context_size(message_type, total_size),
            payload_ptrs: payload_ptrs,
            deserialize_received: deserialize_received,
            use_native_buffers: use_native_buffers,
            prepend_header: prepend_header,
        })
    }

    fn message_type(&self) -> SimpleMessageType {
        self.message_type
    }

    fn process_msg<'registered, 'normal: 'registered>(
        &self,
        recved_msg: &'registered D::ReceivedPkt,
        ctx: &'normal mut Self::Ctx,
        transport_header: usize,
    ) -> Result<Cornflake<'registered, 'normal>> {
        match self.message_type {
            SimpleMessageType::Single => {
                let mut object_deser = ds_query_messages::SingleBufferCF::new();
                object_deser.deserialize(recved_msg.get_pkt_buffer());

                let mut object_ser = ds_query_messages::SingleBufferCF::new();
                object_ser.set_message(object_deser.get_message());

                Ok(object_ser.serialize(ctx, rte_memcpy))
            }
            SimpleMessageType::List(_list_elts) => {
                let mut object_deser = ds_query_messages::ListCF::new();
                object_deser.deserialize(recved_msg.get_pkt_buffer());
                let mut list_field_deser = object_deser.get_messages();
                tracing::debug!("list field deser length: {}", list_field_deser.len());
                let mut object_ser = ds_query_messages::ListCF::new();
                object_ser.init_messages(list_field_deser.len());
                let list_field_ser = object_ser.get_mut_messages();
                for i in 0..list_field_deser.len() {
                    list_field_ser.append(list_field_deser.get(i).clone());
                }

                Ok(object_ser.serialize(ctx, rte_memcpy))
            }
            SimpleMessageType::Tree(depth) => match depth {
                TreeDepth::One => {
                    let mut tree_deser = ds_query_messages::Tree1LCF::new();
                    tree_deser.deserialize(recved_msg.get_pkt_buffer());
                    Ok(deserialize_tree1l(&tree_deser).serialize(ctx, rte_memcpy))
                }
                TreeDepth::Two => {
                    let mut tree_deser = ds_query_messages::Tree2LCF::new();
                    tree_deser.deserialize(recved_msg.get_pkt_buffer());
                    Ok(deserialize_tree2l(&tree_deser).serialize(ctx, rte_memcpy))
                }
                TreeDepth::Three => {
                    let mut tree_deser = ds_query_messages::Tree3LCF::new();
                    tree_deser.deserialize(recved_msg.get_pkt_buffer());
                    Ok(deserialize_tree3l(&tree_deser).serialize(ctx, rte_memcpy))
                }
                TreeDepth::Four => {
                    let mut tree_deser = ds_query_messages::Tree4LCF::new();
                    tree_deser.deserialize(recved_msg.get_pkt_buffer());
                    Ok(deserialize_tree4l(&tree_deser).serialize(ctx, rte_memcpy))
                }
                TreeDepth::Five => {
                    let mut tree_deser = ds_query_messages::Tree5LCF::new();
                    tree_deser.deserialize(recved_msg.get_pkt_buffer());
                    Ok(deserialize_tree5l(&tree_deser).serialize(ctx, rte_memcpy))
                }
            },
        }
    }

    fn new_context(&self) -> Self::Ctx {
        // TODO: optimization to try: directly allocate a packet buffer here, to write in the
        // object header
        // This way no extra copies are required at all.
        vec![0u8; self.context_size]
    }
}

pub struct CornflakesFixedEchoClient<'registered, 'normal> {
    message_type: SimpleMessageType,
    payload_ptrs: Vec<(*const u8, usize)>,
    sga: Cornflake<'registered, 'normal>,
}

impl<'registered, 'normal, D> CerealizeClient<'normal, D>
    for CornflakesFixedEchoClient<'registered, 'normal>
where
    D: Datapath,
{
    type Ctx = Vec<u8>;
    type OutgoingMsg = Cornflake<'registered, 'normal>;

    fn new(
        message_type: SimpleMessageType,
        field_sizes: Vec<usize>,
        mmap_metadata: MmapMetadata,
    ) -> Result<Self> {
        let payload_ptrs = init_payloads(&field_sizes, &mmap_metadata)?;
        let sga = Cornflake::default();
        Ok(CornflakesFixedEchoClient {
            message_type: message_type,
            payload_ptrs: payload_ptrs,
            sga: sga,
        })
    }

    fn init(&mut self, ctx: &'normal mut Self::Ctx) -> Result<()> {
        // initialize the scatter-gather array
        let payloads: Vec<&[u8]> = self
            .payload_ptrs
            .clone()
            .iter()
            .map(|(ptr, size)| unsafe { slice::from_raw_parts(*ptr, *size) })
            .collect();

        match self.message_type {
            SimpleMessageType::Single => {
                assert!(payloads.len() == 1);
                let mut single_buffer_cf = ds_query_messages::SingleBufferCF::new();
                single_buffer_cf.set_message(CFBytes::new(&payloads[0]));
                self.sga = single_buffer_cf.serialize(ctx.as_mut_slice(), rte_memcpy);
            }
            SimpleMessageType::List(list_size) => {
                assert!(payloads.len() == list_size);
                let mut list_cf = ds_query_messages::ListCF::new();
                list_cf.init_messages(list_size);
                let list_ptr = list_cf.get_mut_messages();
                for payload in payloads.iter() {
                    list_ptr.append(CFBytes::new(payload));
                }
                self.sga = list_cf.serialize(ctx.as_mut_slice(), rte_memcpy);
                for i in 0..self.sga.num_segments() {
                    let entry = self.sga.index(i);
                    tracing::debug!(entry_type =? entry.buf_type(), entry_len = entry.buf_size(), i = i, "Metadata about each cornptr in the sga");
                }
            }
            SimpleMessageType::Tree(depth) => match depth {
                TreeDepth::One => {
                    assert!(payloads.len() == 2);
                    let tree_cf = get_tree1l_message(&[0, 1], &payloads);
                    self.sga = tree_cf.serialize(ctx.as_mut_slice(), rte_memcpy);
                }
                TreeDepth::Two => {
                    assert!(payloads.len() == 4);
                    let tree_cf = get_tree2l_message(&[0, 1, 2, 3], &payloads);
                    self.sga = tree_cf.serialize(ctx.as_mut_slice(), rte_memcpy);
                }
                TreeDepth::Three => {
                    assert!(payloads.len() == 8);
                    let indices: Vec<usize> = (0usize..8usize).collect();
                    let tree_cf = get_tree3l_message(indices.as_slice(), &payloads);
                    self.sga = tree_cf.serialize(ctx.as_mut_slice(), rte_memcpy);
                }
                TreeDepth::Four => {
                    assert!(payloads.len() == 16);
                    let indices: Vec<usize> = (0usize..16usize).collect();
                    let tree_cf = get_tree4l_message(indices.as_slice(), &payloads);
                    self.sga = tree_cf.serialize(ctx.as_mut_slice(), rte_memcpy);
                }
                TreeDepth::Five => {
                    assert!(payloads.len() == 32);
                    let indices: Vec<usize> = (0usize..32usize).collect();
                    let tree_cf = get_tree5l_message(indices.as_slice(), &payloads);
                    self.sga = tree_cf.serialize(ctx.as_mut_slice(), rte_memcpy);
                }
            },
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
        match self.message_type {
            SimpleMessageType::Single => {
                let mut object_deser = ds_query_messages::SingleBufferCF::new();
                object_deser.deserialize(recved_msg.get_pkt_buffer());
                let bytes_vec = object_deser.get_message().to_bytes_vec();
                assert!(bytes_vec.len() == our_payloads[0].len());
                if bytes_vec != our_payloads[0].clone() {
                    tracing::debug!(
                        "Payload not equal: ours: {:?}, theirs: {:?}",
                        our_payloads[0],
                        bytes_vec,
                    );
                }
                assert!(bytes_vec == our_payloads[0]);
            }
            SimpleMessageType::List(list_size) => {
                assert!(our_payloads.len() == list_size);
                let mut object_deser = ds_query_messages::ListCF::new();
                object_deser.deserialize(recved_msg.get_pkt_buffer());
                assert!(object_deser.get_messages().len() == our_payloads.len());
                for (i, payload) in our_payloads.iter().enumerate() {
                    let bytes_vec = object_deser.get_messages().get(i).to_bytes_vec();
                    if bytes_vec != payload.clone() {
                        tracing::debug!(
                            "Payload not equal for # i: {}, ours: {:?}, theirs: {:?}",
                            i,
                            payload,
                            bytes_vec
                        );
                    }
                    assert!(bytes_vec == payload.clone());
                }
            }
            SimpleMessageType::Tree(depth) => match depth {
                TreeDepth::One => {
                    let mut object_deser = ds_query_messages::Tree1LCF::new();
                    object_deser.deserialize(recved_msg.get_pkt_buffer());
                    check_tree1l(&[0usize, 1usize], &our_payloads, &object_deser);
                }
                TreeDepth::Two => {
                    let indices: Vec<usize> = (0usize..4usize).collect();
                    let mut object_deser = ds_query_messages::Tree2LCF::new();
                    object_deser.deserialize(recved_msg.get_pkt_buffer());
                    check_tree2l(indices.as_slice(), &our_payloads, &object_deser);
                }
                TreeDepth::Three => {
                    let indices: Vec<usize> = (0usize..8usize).collect();
                    let mut object_deser = ds_query_messages::Tree3LCF::new();
                    object_deser.deserialize(recved_msg.get_pkt_buffer());
                    check_tree3l(indices.as_slice(), &our_payloads, &object_deser);
                }
                TreeDepth::Four => {
                    let indices: Vec<usize> = (0usize..16usize).collect();
                    let mut object_deser = ds_query_messages::Tree4LCF::new();
                    object_deser.deserialize(recved_msg.get_pkt_buffer());
                    check_tree4l(indices.as_slice(), &our_payloads, &object_deser);
                }
                TreeDepth::Five => {
                    let indices: Vec<usize> = (0usize..32usize).collect();
                    let mut object_deser = ds_query_messages::Tree5LCF::new();
                    object_deser.deserialize(recved_msg.get_pkt_buffer());
                    check_tree5l(indices.as_slice(), &our_payloads, &object_deser);
                }
            },
        }
        Ok(())
    }

    fn new_context(&self) -> Self::Ctx {
        let total_size: usize = self.payload_ptrs.iter().map(|(_, size)| *size).sum();
        vec![0u8; context_size(self.message_type, total_size)]
    }
}
