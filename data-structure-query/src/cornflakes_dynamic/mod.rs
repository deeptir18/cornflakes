pub mod ds_query_messages {
    include!(concat!(env!("OUT_DIR"), "/ds_query_cf_dynamic.rs"));
}
use super::{
    get_equal_fields, init_payload, init_payloads, init_payloads_as_vec, CerealizeClient,
    CerealizeMessage,
};
use color_eyre::eyre::{Result, WrapErr};
use cornflakes_codegen::utils::dynamic_hdr::*;
use cornflakes_libos::{
    dpdk_bindings::rte_memcpy_wrapper as rte_memcpy, mem::MmapMetadata, Cornflake, Datapath,
    DatapathMempoolOptions, PtrAttributes, ReceivedPacket, ScatterGather,
};
use cornflakes_utils::{SimpleMessageType, TreeDepth};
use std::slice;

pub struct CornflakesDynamicSerializer {
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
    check_tree4l(&indices[0..16], payloads, object.get_left());
    check_tree4l(&indices[16..32], payloads, object.get_right());
}

fn check_tree4l<'a>(
    indices: &[usize],
    payloads: &Vec<Vec<u8>>,
    object: &ds_query_messages::Tree4LCF<'a>,
) {
    check_tree3l(&indices[0..8], payloads, object.get_left());
    check_tree3l(&indices[8..16], payloads, object.get_right());
}

fn check_tree3l<'a>(
    indices: &[usize],
    payloads: &Vec<Vec<u8>>,
    object: &ds_query_messages::Tree3LCF<'a>,
) {
    check_tree2l(&indices[0..4], payloads, object.get_left());
    check_tree2l(&indices[4..8], payloads, object.get_right());
}

fn check_tree2l<'a>(
    indices: &[usize],
    payloads: &Vec<Vec<u8>>,
    object: &ds_query_messages::Tree2LCF<'a>,
) {
    check_tree1l(&indices[0..2], payloads, object.get_left());
    check_tree1l(&indices[2..4], payloads, object.get_right());
}

fn check_tree1l<'a>(
    indices: &[usize],
    payloads: &Vec<Vec<u8>>,
    object: &ds_query_messages::Tree1LCF<'a>,
) {
    check_single_buffer(indices[0], payloads, object.get_left());
    check_single_buffer(indices[1], payloads, object.get_right());
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

impl<D> CerealizeMessage<D> for CornflakesDynamicSerializer
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
    ) -> Result<CornflakesDynamicSerializer> {
        let payload_ptrs = init_payloads(&field_sizes, &mmap_metadata)?;
        let total_size = field_sizes.iter().sum();
        Ok(CornflakesDynamicSerializer {
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
        // first problem: the context pointer, which we derive in this function (not passed into this
        // function) gets referenced in the resulting cornflake
        // We need to somehow pass the explicit mutable buffer pointer for serialization
        // Second problem: immutable borrow (of the payload) and mutable borrow (of the context,
        // which is earlier in the same buffer) at the same time
        // Cornflakes will
        let payloads: Vec<&[u8]> = match (self.use_native_buffers, self.prepend_header) {
            (true, true) => {
                tracing::debug!("Use native buffers true, self prepend header true");
                // all payload in datapath pkts, context vector empty
                // header coalesced into first payload vector
                assert!(ctx.1.len() == (self.payload_ptrs.len()));
                let ptrs: Vec<&[u8]> = ctx
                    .1
                    .iter()
                    .enumerate()
                    .map(|(idx, buf)| {
                        let size = self.payload_ptrs[idx].1;
                        match idx == 0 {
                            true => &buf.as_ref()[(transport_header + self.context_size)
                                ..(transport_header + self.context_size + size)],
                            false => &buf.as_ref()[0..size],
                        }
                    })
                    .collect();
                ptrs
            }
            (true, false) => {
                // all payloads in datapath pkts
                // context vector still holds header
                tracing::debug!("Use native buffers true, prepend_headers false");
                assert!(ctx.1.len() == self.payload_ptrs.len() && ctx.0.len() == self.context_size);
                let ptrs: Vec<&[u8]> = ctx
                    .1
                    .iter()
                    .enumerate()
                    .map(|(idx, buf)| {
                        let size = self.payload_ptrs[idx].1;
                        &buf.as_ref()[0..size]
                    })
                    .collect();
                ptrs
            }
            (false, true) => {
                // first payload comes from datapath pkt
                // further payloads comes from externally registered data
                tracing::debug!("Not using native buffers, prepend header true");
                assert!(ctx.1.len() == 1);
                let first_payload_ptr = &ctx.1[0].as_ref()[(transport_header + self.context_size)
                    ..(transport_header + self.context_size + self.payload_ptrs[0].1)];
                let mut ptrs: Vec<&[u8]> = self
                    .payload_ptrs
                    .clone()
                    .iter()
                    .enumerate()
                    .map(|(_, (ptr, size))| unsafe { slice::from_raw_parts(*ptr, *size) })
                    .collect();
                ptrs[0] = first_payload_ptr;
                ptrs
            }
            (false, false) => {
                tracing::debug!("Not using native buffers, not prepending header");
                // allocate from externally registered data
                assert!(ctx.1.len() == 0 && ctx.0.len() == self.context_size);
                self.payload_ptrs
                    .clone()
                    .iter()
                    .map(|(ptr, size)| unsafe { slice::from_raw_parts(*ptr, *size) })
                    .collect()
            }
        };
        let ctx_ptr = ctx.0.as_mut_slice();
        match self.message_type {
            SimpleMessageType::Single => {
                if self.deserialize_received {
                    let mut object_deser = ds_query_messages::SingleBufferCF::new();
                    object_deser.deserialize(recved_msg.get_pkt_buffer());
                }
                let mut single_buffer_cf = ds_query_messages::SingleBufferCF::new();
                single_buffer_cf.set_message(CFBytes::new(&payloads[0]));
                Ok(single_buffer_cf.serialize(ctx_ptr, rte_memcpy))
            }
            SimpleMessageType::List(list_size) => {
                if self.deserialize_received {
                    let mut object_deser = ds_query_messages::ListCF::new();
                    object_deser.deserialize(recved_msg.get_pkt_buffer());
                    let list_field_deser = object_deser.get_messages();
                    tracing::debug!("list field deser length: {}", list_field_deser.len());
                }
                assert!(payloads.len() == list_size);
                let mut list_cf = ds_query_messages::ListCF::new();
                list_cf.init_messages(list_size);
                let list_ptr = list_cf.get_mut_messages();
                //let mut i = 0;
                for payload in payloads.iter() {
                    //tracing::debug!("Setting {:?} as {}th list entry.", payload, i);
                    list_ptr.append(CFBytes::new(payload));
                    //i += 1;
                }
                Ok(list_cf.serialize(ctx_ptr, rte_memcpy))
            }
            SimpleMessageType::Tree(depth) => match depth {
                TreeDepth::One => {
                    if self.deserialize_received {
                        let mut tree_deser = ds_query_messages::Tree1LCF::new();
                        tree_deser.deserialize(recved_msg.get_pkt_buffer());
                    }
                    assert!(payloads.len() == 2);
                    let tree_cf = get_tree1l_message(&[0, 1], &payloads);
                    Ok(tree_cf.serialize(ctx_ptr, rte_memcpy))
                }
                TreeDepth::Two => {
                    if self.deserialize_received {
                        let mut tree_deser = ds_query_messages::Tree2LCF::new();
                        tree_deser.deserialize(recved_msg.get_pkt_buffer());
                    }
                    let tree_cf = get_tree2l_message(&[0, 1, 2, 3], &payloads);
                    Ok(tree_cf.serialize(ctx_ptr, rte_memcpy))
                }
                TreeDepth::Three => {
                    if self.deserialize_received {
                        let mut tree_deser = ds_query_messages::Tree3LCF::new();
                        tree_deser.deserialize(recved_msg.get_pkt_buffer());
                    }
                    let indices: Vec<usize> = (0usize..8usize).collect();
                    let tree_cf = get_tree3l_message(indices.as_slice(), &payloads);
                    Ok(tree_cf.serialize(ctx_ptr, rte_memcpy))
                }
                TreeDepth::Four => {
                    if self.deserialize_received {
                        let mut tree_deser = ds_query_messages::Tree4LCF::new();
                        tree_deser.deserialize(recved_msg.get_pkt_buffer());
                    }
                    let indices: Vec<usize> = (0usize..16usize).collect();
                    let tree_cf = get_tree4l_message(indices.as_slice(), &payloads);
                    Ok(tree_cf.serialize(ctx_ptr, rte_memcpy))
                }
                TreeDepth::Five => {
                    if self.deserialize_received {
                        let mut tree_deser = ds_query_messages::Tree5LCF::new();
                        tree_deser.deserialize(recved_msg.get_pkt_buffer());
                    }
                    let indices: Vec<usize> = (0usize..32usize).collect();
                    let tree_cf = get_tree5l_message(indices.as_slice(), &payloads);
                    Ok(tree_cf.serialize(ctx_ptr, rte_memcpy))
                }
            },
        }
    }

    /// If using native buffers or prepending header,
    /// need to allocate packets from the datapath to serve as the serialization context.
    /// Once these packets are sent, they will be freed.
    fn new_context(&self, conn: &D) -> Result<Self::Ctx> {
        match (self.use_native_buffers, self.prepend_header) {
            (true, true) => {
                let pkts_res: Result<Vec<D::DatapathPkt>> = (0..self.payload_ptrs.len())
                    .map(|idx| {
                        let mempool = match idx == 0 {
                            true => 1,
                            false => 2,
                        };
                        let pkt = conn.alloc_datapath_pkt(mempool).wrap_err(
                            "Failed to initialize context: failed to init datapath pkt.",
                        )?;
                        Ok(pkt)
                    })
                    .collect();
                let pkts = pkts_res?;
                Ok((vec![0u8; self.context_size], pkts))
            }
            (true, false) => {
                let pkts_res: Result<Vec<D::DatapathPkt>> = (0..self.payload_ptrs.len())
                    .map(|_idx| {
                        let pkt = conn.alloc_datapath_pkt(1).wrap_err(
                            "Failed to initialize context: failed to init datapath pkt.",
                        )?;
                        Ok(pkt)
                    })
                    .collect();
                let pkts = pkts_res?;
                Ok((vec![0u8; self.context_size], pkts))
            }
            (false, true) => {
                // allocate 1 buffer from the 1st mempool to put the first payload
                let datapath_buf = conn
                    .alloc_datapath_pkt(1)
                    .wrap_err("Failed to initialize context: failed to init datapath pkt.")?;
                Ok((vec![0u8; self.context_size], vec![datapath_buf]))
            }
            (false, false) => Ok((vec![0u8; self.context_size], Vec::default())),
        }
    }

    fn init_datapath(&self, conn: &mut D) -> Result<()> {
        let native_buf_size = conn.native_buf_size();
        let mut payload_with_header = vec![0u8; self.context_size + conn.get_header_size()];
        payload_with_header.append(&mut init_payload(
            native_buf_size - (self.context_size + conn.get_header_size()),
        ));
        let payload_without_header = init_payload(native_buf_size);
        let mempool_ids = match (self.use_native_buffers, self.prepend_header) {
            (true, true) => {
                // initialize two new mempools: one where the header will go,
                // and one with the payload (in all other fields)
                let options = vec![
                    DatapathMempoolOptions::new(payload_with_header.clone(), 1),
                    DatapathMempoolOptions::new(payload_without_header.clone(), 2),
                ];
                conn.init_native_mempools(&options)
                    .wrap_err("Not able to init native mempools")?;
                vec![1, 1]
            }
            (true, false) => {
                let options = vec![DatapathMempoolOptions::new(
                    payload_without_header.clone(),
                    1,
                )];
                conn.init_native_mempools(&options)
                    .wrap_err("Not able to init native mempools.")?;
                vec![1]
            }
            (false, true) => {
                let options = vec![DatapathMempoolOptions::new(payload_with_header.clone(), 1)];
                conn.init_native_mempools(&options)
                    .wrap_err("Not able to init native mempools.")?;
                vec![1]
            }
            (false, false) => Vec::default(),
        };

        // allocate and free some packets from these mempools
        for mempool_id in mempool_ids.iter() {
            let pkts: Result<Vec<D::DatapathPkt>> = (0..250)
                .map(|_| {
                    Ok(conn.alloc_datapath_pkt(*mempool_id).wrap_err(format!(
                        "Not able to allocate datapath packet from mempool {}",
                        *mempool_id
                    ))?)
                })
                .collect();
            for pkt in pkts?.into_iter() {
                conn.free_datapath_pkt(pkt)?;
            }
        }
        Ok(())
    }
}

pub struct CornflakesDynamicEchoClient<'registered, 'normal> {
    our_message_type: SimpleMessageType,
    server_message_type: SimpleMessageType,
    server_field_sizes: Vec<usize>,
    payload_ptrs: Vec<(*const u8, usize)>,
    sga: Cornflake<'registered, 'normal>,
    total_size: usize,
}

impl<'registered, 'normal, D> CerealizeClient<'normal, D>
    for CornflakesDynamicEchoClient<'registered, 'normal>
where
    D: Datapath,
{
    type Ctx = Vec<u8>;
    type OutgoingMsg = Cornflake<'registered, 'normal>;

    fn new(
        our_message_type: SimpleMessageType,
        server_message_type: SimpleMessageType,
        our_field_sizes: Vec<usize>,
        server_field_sizes: Vec<usize>,
        mmap_metadata: MmapMetadata,
    ) -> Result<Self> {
        let payload_ptrs = init_payloads(&our_field_sizes, &mmap_metadata)?;
        let sga = Cornflake::default();
        let total_size: usize = payload_ptrs.iter().map(|(_, size)| *size).sum();
        Ok(CornflakesDynamicEchoClient {
            our_message_type: our_message_type,
            server_message_type: server_message_type,
            server_field_sizes: server_field_sizes,
            payload_ptrs: payload_ptrs,
            sga: sga,
            total_size: total_size,
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

        match self.our_message_type {
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

    fn our_message_type(&self) -> SimpleMessageType {
        self.our_message_type
    }

    fn server_message_type(&self) -> SimpleMessageType {
        self.server_message_type
    }

    fn get_sga(&self) -> Result<Self::OutgoingMsg> {
        Ok(self.sga.clone())
    }

    fn check_echoed_payload(&self, recved_msg: &D::ReceivedPkt) -> Result<()> {
        let our_payloads = init_payloads_as_vec(&self.server_field_sizes);
        match self.server_message_type {
            SimpleMessageType::Single => {
                let mut object_deser = ds_query_messages::SingleBufferCF::new();
                object_deser.deserialize(recved_msg.get_pkt_buffer());
                let bytes_vec = object_deser.get_message().to_bytes_vec();
                assert!(bytes_vec.len() == our_payloads[0].len());
                assert!(bytes_vec == our_payloads[0]);
            }
            SimpleMessageType::List(list_size) => {
                assert!(our_payloads.len() == list_size);
                let mut object_deser = ds_query_messages::ListCF::new();
                object_deser.deserialize(recved_msg.get_pkt_buffer());
                assert!(object_deser.get_messages().len() == our_payloads.len());
                for (i, payload) in our_payloads.iter().enumerate() {
                    let bytes_vec = object_deser.get_messages()[i].to_bytes_vec();
                    if bytes_vec != payload.clone() {
                        tracing::debug!(i = i, "index");
                        tracing::debug!("What should be received:{:?}", payload);
                        tracing::debug!("Deser message: {:?}", bytes_vec,);
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
        let context_size = context_size(self.our_message_type, self.total_size);
        vec![0u8; context_size]
    }
}
