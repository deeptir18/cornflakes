pub mod kv_serializer_hybrid_object {
    #![allow(unused_variables)]
    #![allow(non_camel_case_types)]
    #![allow(non_upper_case_globals)]
    #![allow(non_snake_case)]
    include!(concat!(env!("OUT_DIR"), "/kv_hybrid_object_cornflakes.rs"));
}
pub mod kv_serializer_hybrid_arena_object {
    #![allow(unused_variables)]
    #![allow(non_camel_case_types)]
    #![allow(non_upper_case_globals)]
    #![allow(non_snake_case)]
    include!(concat!(
        env!("OUT_DIR"),
        "/kv_hybrid_arena_object_cornflakes.rs"
    ));
}
pub mod kv_serializer_hybrid {
    #![allow(unused_variables)]
    #![allow(non_camel_case_types)]
    #![allow(non_upper_case_globals)]
    #![allow(non_snake_case)]
    include!(concat!(env!("OUT_DIR"), "/kv_rcsga_hybrid_cornflakes.rs"));
}
pub mod kv_serializer_sga {
    #![allow(unused_variables)]
    #![allow(non_camel_case_types)]
    #![allow(non_upper_case_globals)]
    #![allow(non_snake_case)]
    include!(concat!(env!("OUT_DIR"), "/kv_sga_cornflakes.rs"));
}
pub mod kv_serializer {
    #![allow(unused_variables)]
    #![allow(non_camel_case_types)]
    #![allow(non_upper_case_globals)]
    #![allow(non_snake_case)]
    include!(concat!(env!("OUT_DIR"), "/kv_rcsga_cornflakes.rs"));
}
use cornflakes_libos::{
    allocator::MempoolID,
    datapath::{Datapath, PushBufType, ReceivedPkt},
    dynamic_object_arena_hdr,
    dynamic_object_arena_hdr::CornflakesArenaObject,
    dynamic_object_hdr,
    dynamic_object_hdr::CornflakesObject,
    dynamic_rcsga_hdr::RcSgaHeaderRepr,
    dynamic_rcsga_hdr::*,
    dynamic_rcsga_hybrid_hdr,
    dynamic_rcsga_hybrid_hdr::HybridArenaRcSgaHdr,
    state_machine::server::ServerSM,
    ArenaDatapathSga, ArenaOrderedRcSga, ConnID, CopyContext, MsgID,
};
use kv_serializer::*;

use super::{
    ClientSerializer, KVServer, LinkedListKVServer, ListKVServer, MsgType, ServerLoadGenerator,
    REQ_TYPE_SIZE,
};
use color_eyre::eyre::{bail, ensure, Result};
use std::marker::PhantomData;

pub struct CornflakesSerializer<D>
where
    D: Datapath,
{
    _phantom: PhantomData<D>,
    with_copies: bool,
    use_linked_list: bool,
}

impl<D> CornflakesSerializer<D>
where
    D: Datapath,
{
    pub fn new(use_linked_list: bool) -> Self {
        CornflakesSerializer {
            _phantom: PhantomData::default(),
            with_copies: false,
            use_linked_list: use_linked_list,
        }
    }

    pub fn use_linked_list(&self) -> bool {
        self.use_linked_list
    }
    pub fn set_with_copies(&mut self) {
        self.with_copies = true;
    }

    pub fn with_copies(&self) -> bool {
        self.with_copies
    }

    fn handle_put_serialize_and_send<'kv, 'arena>(
        &self,
        msg_id: MsgID,
        conn_id: ConnID,
        end_batch: bool,
        kv_server: &'kv mut KVServer<D>,
        linked_list_kv_server: &'kv mut LinkedListKVServer<D>,
        mempool_ids: &mut Vec<MempoolID>,
        pkt: &ReceivedPkt<D>,
        datapath: &mut D,
        arena: &'arena bumpalo::Bump,
    ) -> Result<()>
    where
        'kv: 'arena,
    {
        let mut put_req = kv_serializer_hybrid::PutReq::new_in(arena);
        put_req.deserialize(pkt, REQ_TYPE_SIZE, arena)?;
        if self.use_linked_list() {
            linked_list_kv_server.insert_with_copies(
                put_req.get_key().to_str()?,
                put_req.get_val().as_ref(),
                datapath,
                mempool_ids,
            )?;
        } else {
            kv_server.insert_with_copies(
                put_req.get_key().to_str()?,
                put_req.get_val().as_ref(),
                datapath,
                mempool_ids,
            )?;
        }
        let mut put_resp = kv_serializer_hybrid::PutResp::new_in(arena);
        let mut copy_context = CopyContext::new(arena, datapath)?;
        put_resp.set_id(put_req.get_id());

        // now serialize and send object
        datapath.queue_cornflakes_obj(msg_id, conn_id, &mut copy_context, put_resp, end_batch)?;
        Ok(())
    }

    fn handle_get_serialize_and_send<'kv, 'arena>(
        &self,
        msg_id: MsgID,
        conn_id: ConnID,
        end_batch: bool,
        kv_server: &'kv KVServer<D>,
        linked_list_kv_server: &'kv LinkedListKVServer<D>,
        pkt: &ReceivedPkt<D>,
        datapath: &mut D,
        arena: &'arena bumpalo::Bump,
    ) -> Result<()>
    where
        'kv: 'arena,
    {
        let mut get_req = kv_serializer_hybrid::GetReq::new_in(arena);
        {
            //#[cfg(feature = "profiler")]
            //demikernel::timer!("Deserialize pkt");
            get_req.deserialize(pkt, REQ_TYPE_SIZE, arena)?;
        }
        let value = {
            //#[cfg(feature = "profiler")]
            //demikernel::timer!("Get value from kv");
            match self.use_linked_list() {
                true => match linked_list_kv_server.get(get_req.get_key().to_str()?) {
                    Some(v) => v.as_ref().get_buffer(),
                    None => {
                        bail!(
                            "Could not find value for key: {:?}",
                            get_req.get_key().to_str()
                        );
                    }
                },
                false => match kv_server.get(get_req.get_key().to_str()?) {
                    Some(v) => v,
                    None => {
                        bail!(
                            "Could not find value for key: {:?}",
                            get_req.get_key().to_str()
                        );
                    }
                },
            }
        };

        tracing::debug!(
            "For given key {:?}, found value {:?} with length {}",
            get_req.get_key().to_str()?,
            value.as_ref().as_ptr(),
            value.as_ref().len()
        );
        let mut get_resp = kv_serializer_hybrid::GetResp::new_in(arena);
        let mut copy_context = {
            //#[cfg(feature = "profiler")]
            //demikernel::timer!("Allocate cc");
            CopyContext::new(arena, datapath)?
        };

        get_resp.set_id(get_req.get_id());

        {
            //#[cfg(feature = "profiler")]
            //demikernel::timer!("Set val inside cornflakes");
            get_resp.set_val(dynamic_rcsga_hybrid_hdr::CFBytes::new(
                value.as_ref(),
                datapath,
                &mut copy_context,
            )?);
        }

        // now serialize and send object
        datapath.queue_cornflakes_obj(msg_id, conn_id, &mut copy_context, get_resp, end_batch)?;
        Ok(())
    }

    fn handle_get<'kv, 'arena>(
        &self,
        kv_server: &'kv KVServer<D>,
        linked_list_kv_server: &'kv LinkedListKVServer<D>,
        pkt: &ReceivedPkt<D>,
        datapath: &mut D,
        arena: &'arena bumpalo::Bump,
    ) -> Result<ArenaDatapathSga<'arena, D>>
    where
        'kv: 'arena,
    {
        let mut get_req = kv_serializer_hybrid::GetReq::new_in(arena);
        get_req.deserialize(pkt, REQ_TYPE_SIZE, arena)?;
        let value = match self.use_linked_list() {
            true => match linked_list_kv_server.get(get_req.get_key().to_str()?) {
                Some(v) => v.as_ref().get_buffer(),
                None => {
                    bail!(
                        "Could not find value for key: {:?}",
                        get_req.get_key().to_str()
                    );
                }
            },
            false => match kv_server.get(get_req.get_key().to_str()?) {
                Some(v) => v,
                None => {
                    bail!(
                        "Could not find value for key: {:?}",
                        get_req.get_key().to_str()
                    );
                }
            },
        };
        tracing::debug!(
            "For given key {:?}, found value {:?} with length {}",
            get_req.get_key().to_str()?,
            value.as_ref().as_ptr(),
            value.as_ref().len()
        );
        let mut get_resp = kv_serializer_hybrid::GetResp::new_in(arena);
        get_resp.set_id(get_req.get_id());

        // initialize copy context
        let mut copy_context = CopyContext::new(arena, datapath)?;
        get_resp.set_val(dynamic_rcsga_hybrid_hdr::CFBytes::new(
            value.as_ref(),
            datapath,
            &mut copy_context,
        )?);

        let datapath_sga =
            get_resp.serialize_into_arena_datapath_sga(datapath, copy_context, arena)?;
        Ok(datapath_sga)
    }

    fn handle_put<'arena>(
        &self,
        kv_server: &mut KVServer<D>,
        linked_list_kv_server: &mut LinkedListKVServer<D>,
        mempool_ids: &mut Vec<MempoolID>,
        pkt: &ReceivedPkt<D>,
        datapath: &mut D,
        arena: &'arena bumpalo::Bump,
    ) -> Result<ArenaOrderedRcSga<'arena, D>> {
        let mut put_req = PutReq::new();
        put_req.deserialize_from_pkt(&pkt, REQ_TYPE_SIZE)?;
        if self.use_linked_list() {
            linked_list_kv_server.insert_with_copies(
                put_req.get_key().to_str()?,
                put_req.get_val().as_bytes(),
                datapath,
                mempool_ids,
            )?;
        } else {
            kv_server.insert_with_copies(
                put_req.get_key().to_str()?,
                put_req.get_val().as_bytes(),
                datapath,
                mempool_ids,
            )?;
        }

        let mut put_resp = PutResp::new();
        put_resp.set_id(put_req.get_id());
        let mut arena_sga =
            ArenaOrderedRcSga::allocate(put_resp.num_scatter_gather_entries(), &arena);
        put_resp.serialize_into_arena_sga(&mut arena_sga, arena, datapath, self.with_copies)?;
        Ok(arena_sga)
    }

    fn handle_getm_serialize_and_send<'kv, 'arena>(
        &self,
        msg_id: MsgID,
        conn_id: ConnID,
        end_batch: bool,
        kv_server: &'kv KVServer<D>,
        linked_list_kv_server: &'kv LinkedListKVServer<D>,
        pkt: &ReceivedPkt<D>,
        datapath: &mut D,
        arena: &'arena bumpalo::Bump,
    ) -> Result<()>
    where
        'kv: 'arena,
    {
        let mut getm_req = kv_serializer_hybrid::GetMReq::new_in(arena);
        {
            #[cfg(feature = "profiler")]
            demikernel::timer!("Deserialize pkt");
            getm_req.deserialize(&pkt, REQ_TYPE_SIZE, arena)?;
        }
        let mut getm_resp = kv_serializer_hybrid::GetMResp::new_in(arena);
        let mut copy_context = CopyContext::new(arena, datapath)?;
        getm_resp.init_vals(getm_req.get_keys().len(), arena);
        let vals = getm_resp.get_mut_vals();
        for key in getm_req.get_keys().iter() {
            let value = {
                tracing::debug!("Key bytes: {:?}", key);
                #[cfg(feature = "profiler")]
                demikernel::timer!("got value");
                match self.use_linked_list() {
                    true => match linked_list_kv_server.get(key.to_str()?) {
                        Some(v) => v.as_ref().get_buffer(),
                        None => {
                            bail!("Could not find value for key: {:?}", key.to_str());
                        }
                    },
                    false => match kv_server.get(key.to_str()?) {
                        Some(v) => v,
                        None => {
                            bail!("Could not find value for key: {:?}", key.to_str());
                        }
                    },
                }
            };
            tracing::debug!(
                "For given key {:?}, found value {:?} with length {}",
                key.to_str()?,
                value.as_ref().as_ptr(),
                value.as_ref().len()
            );
            {
                #[cfg(feature = "profiler")]
                demikernel::timer!("append value");
                vals.append(dynamic_rcsga_hybrid_hdr::CFBytes::new(
                    value.as_ref(),
                    datapath,
                    &mut copy_context,
                )?);
            }
        }
        getm_resp.set_id(getm_req.get_id());
        tracing::debug!("Finished setting all pointers");
        tracing::debug!("Sending back {:?}", getm_resp);

        datapath.queue_cornflakes_obj(msg_id, conn_id, &mut copy_context, getm_resp, end_batch)?;
        Ok(())
    }

    fn handle_getm<'kv, 'arena>(
        &self,
        kv_server: &'kv KVServer<D>,
        linked_list_kv_server: &'kv LinkedListKVServer<D>,
        pkt: &ReceivedPkt<D>,
        datapath: &mut D,
        arena: &'arena bumpalo::Bump,
    ) -> Result<ArenaDatapathSga<'arena, D>>
    where
        'kv: 'arena,
    {
        let mut getm_req = kv_serializer_hybrid::GetMReq::new_in(arena);
        {
            #[cfg(feature = "profiler")]
            demikernel::timer!("Deserialize pkt");
            getm_req.deserialize(&pkt, REQ_TYPE_SIZE, arena)?;
        }
        let mut getm_resp = kv_serializer_hybrid::GetMResp::new_in(arena);
        getm_resp.init_vals(getm_req.get_keys().len(), arena);
        let vals = getm_resp.get_mut_vals();
        let mut copy_context = CopyContext::new(arena, datapath)?;
        for key in getm_req.get_keys().iter() {
            let value = {
                tracing::debug!("Key bytes: {:?}", key);
                #[cfg(feature = "profiler")]
                demikernel::timer!("got value");
                match self.use_linked_list() {
                    true => match linked_list_kv_server.get(key.to_str()?) {
                        Some(v) => v.get_buffer(),
                        None => {
                            bail!("Could not find value for key: {:?}", key.to_str());
                        }
                    },
                    false => match kv_server.get(key.to_str()?) {
                        Some(v) => v,
                        None => {
                            bail!("Could not find value for key: {:?}", key.to_str());
                        }
                    },
                }
            };
            tracing::debug!(
                "For given key {:?}, found value {:?} with length {}",
                key.to_str()?,
                value.as_ref().as_ptr(),
                value.as_ref().len()
            );
            {
                #[cfg(feature = "profiler")]
                demikernel::timer!("append value");
                vals.append(dynamic_rcsga_hybrid_hdr::CFBytes::new(
                    value.as_ref(),
                    datapath,
                    &mut copy_context,
                )?);
            }
        }
        getm_resp.set_id(getm_req.get_id());
        tracing::debug!("Sending back {:?}", getm_resp);

        let datapath_sga = {
            #[cfg(feature = "profiler")]
            demikernel::timer!("serialize sga");
            getm_resp.serialize_into_arena_datapath_sga(datapath, copy_context, arena)
        }?;
        Ok(datapath_sga)
    }

    fn handle_putm<'arena>(
        &self,
        kv_server: &mut KVServer<D>,
        linked_list_kv_server: &mut LinkedListKVServer<D>,
        mempool_ids: &mut Vec<MempoolID>,
        pkt: &ReceivedPkt<D>,
        datapath: &mut D,
        arena: &'arena bumpalo::Bump,
    ) -> Result<ArenaOrderedRcSga<'arena, D>> {
        let mut putm_req = PutMReq::new();
        putm_req.deserialize_from_pkt(&pkt, REQ_TYPE_SIZE)?;
        for (key, value) in putm_req.get_keys().iter().zip(putm_req.get_vals().iter()) {
            if self.use_linked_list() {
                linked_list_kv_server.insert_with_copies(
                    key.to_str()?,
                    value.as_bytes(),
                    datapath,
                    mempool_ids,
                )?;
            } else {
                kv_server.insert_with_copies(
                    key.to_str()?,
                    value.as_bytes(),
                    datapath,
                    mempool_ids,
                )?;
            }
        }
        let mut put_resp = PutResp::new();
        put_resp.set_id(putm_req.get_id());
        let mut arena_sga =
            ArenaOrderedRcSga::allocate(put_resp.num_scatter_gather_entries(), &arena);
        put_resp.serialize_into_arena_sga(&mut arena_sga, arena, datapath, self.with_copies)?;
        Ok(arena_sga)
    }

    fn handle_getlist_serialize_and_send<'kv, 'arena>(
        &self,
        msg_id: MsgID,
        conn_id: ConnID,
        end_batch: bool,
        list_kv_server: &'kv ListKVServer<D>,
        linked_list_kv: &'kv LinkedListKVServer<D>,
        pkt: &ReceivedPkt<D>,
        datapath: &mut D,
        arena: &'arena bumpalo::Bump,
    ) -> Result<()>
    where
        'kv: 'arena,
    {
        let mut getlist_req = kv_serializer_hybrid::GetListReq::new_in(arena);
        getlist_req.deserialize(&pkt, REQ_TYPE_SIZE, arena)?;
        let mut getlist_resp = kv_serializer_hybrid::GetListResp::new_in(arena);
        let mut copy_context = CopyContext::new(arena, datapath)?;
        getlist_resp.set_id(getlist_req.get_id());

        if self.use_linked_list() {
            let range_start = getlist_req.get_range_start();
            let range_end = getlist_req.get_range_end();
            tracing::debug!("Linked list kv length: {}", linked_list_kv.len());
            for key in linked_list_kv.get_map().iter() {
                tracing::debug!("k: {:?}", key.0);
                break;
            }
            let mut node_option = linked_list_kv.get(getlist_req.get_key().to_str()?);
            // TODO: likely possible to inline this with iterating over the nodes below
            let range_len = {
                if range_end == -1 {
                    let mut len = 0;
                    while let Some(node) = node_option {
                        tracing::debug!(
                            key = getlist_req.get_key().to_str()?,
                            "But HERE, node is something"
                        );
                        len += 1;
                        node_option = node.get_next();
                    }
                    len - range_start as usize
                } else {
                    ensure!(
                        range_start < range_end,
                        "Cannot process get list with range_end < range_start"
                    );
                    (range_end - range_start) as usize
                }
            };

            getlist_resp.init_val_list(range_len, arena);
            let list = getlist_resp.get_mut_val_list();
            let mut node_option = linked_list_kv.get(getlist_req.get_key().to_str()?);

            let mut idx = 0;
            while let Some(node) = node_option {
                if idx < range_start {
                    node_option = node.get_next();
                    idx += 1;
                    continue;
                } else if idx as usize == range_len {
                    tracing::debug!("Got to idx = range len");
                    break;
                }
                tracing::debug!(
                    "Appending value to linked list with size {}",
                    node.get_data().len()
                );
                list.append(dynamic_rcsga_hybrid_hdr::CFBytes::new(
                    node.get_data(),
                    datapath,
                    &mut copy_context,
                )?);
                node_option = node.get_next();
                idx += 1;
            }
            tracing::debug!("Done iterating over linked list");
        } else {
            let value_list = match list_kv_server.get(getlist_req.get_key().to_str()?) {
                Some(v) => v,
                None => {
                    bail!(
                        "Could not find value for key: {:?}",
                        getlist_req.get_key().to_str()
                    );
                }
            };

            getlist_resp.init_val_list(value_list.len(), arena);
            let list = getlist_resp.get_mut_val_list();
            for value in value_list.iter() {
                list.append(dynamic_rcsga_hybrid_hdr::CFBytes::new(
                    value.as_ref(),
                    datapath,
                    &mut copy_context,
                )?);
            }
        }

        datapath.queue_cornflakes_obj(
            msg_id,
            conn_id,
            &mut copy_context,
            getlist_resp,
            end_batch,
        )?;
        Ok(())
    }

    fn handle_getlist<'kv, 'arena>(
        &self,
        list_kv_server: &'kv ListKVServer<D>,
        linked_list_kv_server: &'kv LinkedListKVServer<D>,
        pkt: &ReceivedPkt<D>,
        datapath: &mut D,
        arena: &'arena bumpalo::Bump,
    ) -> Result<ArenaDatapathSga<'arena, D>>
    where
        'kv: 'arena,
    {
        let mut getlist_req = kv_serializer_hybrid::GetListReq::new_in(arena);
        getlist_req.deserialize(&pkt, REQ_TYPE_SIZE, arena)?;
        let mut getlist_resp = kv_serializer_hybrid::GetListResp::new_in(arena);
        let mut copy_context = CopyContext::new(arena, datapath)?;
        getlist_resp.set_id(getlist_req.get_id());

        if self.use_linked_list() {
            let range_start = getlist_req.get_range_start();
            let range_end = getlist_req.get_range_end();
            let mut node_option = linked_list_kv_server.get(getlist_req.get_key().to_str()?);

            let range_len = {
                if range_end == -1 {
                    let mut len = 0;
                    while let Some(node) = node_option {
                        len += 1;
                        node_option = node.get_next();
                    }
                    len - range_start as usize
                } else {
                    ensure!(
                        range_start < range_end,
                        "Cannot process get list with range_end < range_start"
                    );
                    (range_end - range_start) as usize
                }
            };

            let mut node_option = linked_list_kv_server.get(getlist_req.get_key().to_str()?);
            getlist_resp.init_val_list(range_len, arena);
            let list = getlist_resp.get_mut_val_list();
            let mut idx = 0;
            while let Some(node) = node_option {
                if idx < range_start {
                    node_option = node.get_next();
                    idx += 1;
                    continue;
                } else if idx as usize == range_len {
                    break;
                }

                list.append(dynamic_rcsga_hybrid_hdr::CFBytes::new(
                    node.as_ref().get_data(),
                    datapath,
                    &mut copy_context,
                )?);
                node_option = node.get_next();
                idx += 1;
            }
        } else {
            let value_list = match list_kv_server.get(getlist_req.get_key().to_str()?) {
                Some(v) => v,
                None => {
                    bail!(
                        "Could not find value for key: {:?}",
                        getlist_req.get_key().to_str()
                    );
                }
            };

            getlist_resp.init_val_list(value_list.len(), arena);
            let list = getlist_resp.get_mut_val_list();
            for value in value_list.iter() {
                list.append(dynamic_rcsga_hybrid_hdr::CFBytes::new(
                    value.as_ref(),
                    datapath,
                    &mut copy_context,
                )?);
            }
        }

        let datapath_sga =
            getlist_resp.serialize_into_arena_datapath_sga(datapath, copy_context, arena)?;
        Ok(datapath_sga)
    }

    fn handle_putlist<'arena>(
        &self,
        list_kv_server: &mut ListKVServer<D>,
        linked_list_kv_server: &mut LinkedListKVServer<D>,
        mempool_ids: &mut Vec<MempoolID>,
        pkt: &ReceivedPkt<D>,
        datapath: &mut D,
        arena: &'arena bumpalo::Bump,
    ) -> Result<ArenaOrderedRcSga<'arena, D>> {
        let mut putlist_req = PutListReq::new();
        putlist_req.deserialize_from_pkt(&pkt, REQ_TYPE_SIZE)?;
        let key = putlist_req.get_key();
        let values_iterator = putlist_req.get_vals().iter().map(|value| value.as_bytes());

        if self.use_linked_list() {
            linked_list_kv_server.insert_list_with_copies(
                key.to_str()?,
                values_iterator,
                datapath,
                mempool_ids,
            )?;
        } else {
            list_kv_server.insert_with_copies(
                key.to_str()?,
                values_iterator,
                datapath,
                mempool_ids,
            )?;
        }
        let mut put_resp = PutResp::new();
        put_resp.set_id(putlist_req.get_id());
        let mut arena_sga =
            ArenaOrderedRcSga::allocate(put_resp.num_scatter_gather_entries(), &arena);
        put_resp.serialize_into_arena_sga(&mut arena_sga, arena, datapath, self.with_copies)?;
        Ok(arena_sga)
    }
}

pub struct CornflakesKVServer<D>
where
    D: Datapath,
{
    kv_server: KVServer<D>,
    list_kv_server: ListKVServer<D>,
    linked_list_kv_server: LinkedListKVServer<D>,
    mempool_ids: Vec<MempoolID>,
    serializer: CornflakesSerializer<D>,
    push_buf_type: PushBufType,
}

impl<D> CornflakesKVServer<D>
where
    D: Datapath,
{
    pub fn new<L>(
        file: &str,
        load_generator: L,
        datapath: &mut D,
        push_buf_type: PushBufType,
        use_linked_list: bool,
    ) -> Result<Self>
    where
        L: ServerLoadGenerator,
    {
        let (kv, list_kv, linked_list_kv, mempool_ids) =
            load_generator.new_kv_state(file, datapath, use_linked_list)?;
        let mut serializer = CornflakesSerializer::<D>::new(use_linked_list);
        if datapath.get_copying_threshold() == usize::MAX {
            tracing::info!("For serialization cornflakes 1c, setting with copies");
            serializer.set_with_copies();
        }
        Ok(CornflakesKVServer {
            kv_server: kv,
            list_kv_server: list_kv,
            linked_list_kv_server: linked_list_kv,
            mempool_ids,
            push_buf_type,
            serializer,
        })
    }
}

impl<D> ServerSM for CornflakesKVServer<D>
where
    D: Datapath,
{
    type Datapath = D;
    #[inline]
    fn push_buf_type(&self) -> PushBufType {
        self.push_buf_type
    }

    #[inline]
    fn process_requests_hybrid_object(
        &mut self,
        pkts: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        datapath: &mut Self::Datapath,
    ) -> Result<()> {
        let end = pkts.len();
        for (i, pkt) in pkts.into_iter().enumerate() {
            let end_batch = i == (end - 1);
            let msg_type = MsgType::from_packet(&pkt)?;
            tracing::debug!(
                msg_type =? msg_type,
                "Received packet with data {:?} ptr and length {}",
                pkt.seg(0).as_ref().as_ptr(),
                pkt.data_len()
            );
            match msg_type {
                MsgType::GetList(_) => {
                    let mut getlist_req = kv_serializer_hybrid_object::GetListReq::new();
                    getlist_req.deserialize(&pkt, REQ_TYPE_SIZE)?;
                    let mut getlist_resp = kv_serializer_hybrid_object::GetListResp::new();
                    getlist_resp.set_id(getlist_req.get_id());

                    if self.serializer.use_linked_list() {
                        let range_start = getlist_req.get_range_start();
                        let range_end = getlist_req.get_range_end();
                        let mut node_option = self
                            .linked_list_kv_server
                            .get(getlist_req.get_key().to_str()?);
                        let range_len = {
                            if range_end == -1 {
                                let mut len = 0;
                                while let Some(node) = node_option {
                                    len += 1;
                                    node_option = node.get_next();
                                }
                                len - range_start as usize
                            } else {
                                ensure!(
                                    range_start < range_end,
                                    "Cannot process get list with range_end < range_start"
                                );
                                (range_end - range_start) as usize
                            }
                        };

                        getlist_resp.init_val_list(range_len);
                        let list = getlist_resp.get_mut_val_list();
                        let mut node_option = self
                            .linked_list_kv_server
                            .get(getlist_req.get_key().to_str()?);

                        let mut idx = 0;
                        while let Some(node) = node_option {
                            if idx < range_start {
                                node_option = node.get_next();
                                idx += 1;
                                continue;
                            } else if idx as usize == range_len {
                                tracing::debug!("Got to idx = range len");
                                break;
                            }
                            tracing::debug!(
                                "Appending value to linked list with size {}",
                                node.get_data().len()
                            );
                            list.append(dynamic_object_hdr::CFBytes::new(
                                node.get_data(),
                                datapath,
                            )?);
                            node_option = node.get_next();
                            idx += 1;
                        }
                    } else {
                        let value_list =
                            match self.list_kv_server.get(getlist_req.get_key().to_str()?) {
                                Some(v) => v,
                                None => {
                                    bail!(
                                        "Could not find value for key: {:?}",
                                        getlist_req.get_key().to_str()
                                    );
                                }
                            };

                        getlist_resp.init_val_list(value_list.len());
                        let list = getlist_resp.get_mut_val_list();
                        for value in value_list.iter() {
                            list.append(dynamic_object_hdr::CFBytes::new(
                                value.as_ref(),
                                datapath,
                            )?);
                        }
                    }

                    datapath.queue_cornflakes_hybrid_object(
                        pkt.msg_id(),
                        pkt.conn_id(),
                        getlist_resp,
                        end_batch,
                    )?;
                }
                _ => {
                    unimplemented!();
                }
            }
        }
        Ok(())
    }

    #[inline]
    fn process_requests_hybrid_arena_object(
        &mut self,
        pkts: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        datapath: &mut Self::Datapath,
        arena: &mut bumpalo::Bump,
    ) -> Result<()> {
        let end = pkts.len();
        for (i, pkt) in pkts.into_iter().enumerate() {
            let end_batch = i == (end - 1);
            let msg_type = MsgType::from_packet(&pkt)?;
            tracing::debug!(
                msg_type =? msg_type,
                "Received packet with data {:?} ptr and length {}",
                pkt.seg(0).as_ref().as_ptr(),
                pkt.data_len()
            );
            match msg_type {
                MsgType::GetList(_) => {
                    let mut getlist_req =
                        kv_serializer_hybrid_arena_object::GetListReq::new_in(arena);
                    getlist_req.deserialize(&pkt, REQ_TYPE_SIZE, arena)?;
                    let mut getlist_resp =
                        kv_serializer_hybrid_arena_object::GetListResp::new_in(arena);
                    getlist_resp.set_id(getlist_req.get_id());

                    if self.serializer.use_linked_list() {
                        let range_start = getlist_req.get_range_start();
                        let range_end = getlist_req.get_range_end();
                        let mut node_option = self
                            .linked_list_kv_server
                            .get(getlist_req.get_key().to_str()?);
                        let range_len = {
                            if range_end == -1 {
                                let mut len = 0;
                                while let Some(node) = node_option {
                                    len += 1;
                                    node_option = node.get_next();
                                }
                                len - range_start as usize
                            } else {
                                ensure!(
                                    range_start < range_end,
                                    "Cannot process get list with range_end < range_start"
                                );
                                (range_end - range_start) as usize
                            }
                        };

                        getlist_resp.init_val_list(range_len, arena);
                        let list = getlist_resp.get_mut_val_list();
                        let mut node_option = self
                            .linked_list_kv_server
                            .get(getlist_req.get_key().to_str()?);

                        let mut idx = 0;
                        while let Some(node) = node_option {
                            if idx < range_start {
                                node_option = node.get_next();
                                idx += 1;
                                continue;
                            } else if idx as usize == range_len {
                                tracing::debug!("Got to idx = range len");
                                break;
                            }
                            tracing::debug!(
                                "Appending value to linked list with size {}",
                                node.get_data().len()
                            );
                            list.append(dynamic_object_arena_hdr::CFBytes::new(
                                node.get_data(),
                                datapath,
                                arena,
                            )?);
                            node_option = node.get_next();
                            idx += 1;
                        }
                    } else {
                        let value_list =
                            match self.list_kv_server.get(getlist_req.get_key().to_str()?) {
                                Some(v) => v,
                                None => {
                                    bail!(
                                        "Could not find value for key: {:?}",
                                        getlist_req.get_key().to_str()
                                    );
                                }
                            };

                        getlist_resp.init_val_list(value_list.len(), arena);
                        let list = getlist_resp.get_mut_val_list();
                        for value in value_list.iter() {
                            list.append(dynamic_object_arena_hdr::CFBytes::new(
                                value.as_ref(),
                                datapath,
                                arena,
                            )?);
                        }
                    }

                    datapath.queue_cornflakes_arena_object(
                        pkt.msg_id(),
                        pkt.conn_id(),
                        getlist_resp,
                        end_batch,
                    )?;
                }
                _ => {
                    unimplemented!();
                }
            }
        }
        Ok(())
    }

    #[inline]
    fn process_requests_object(
        &mut self,
        sga: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        datapath: &mut Self::Datapath,
        arena: &mut bumpalo::Bump,
    ) -> Result<()> {
        let end = sga.len();
        for (i, pkt) in sga.into_iter().enumerate() {
            let end_batch = i == (end - 1);
            let msg_type = MsgType::from_packet(&pkt)?;
            tracing::debug!(
                msg_type =? msg_type,
                "Received packet with data {:?} ptr and length {}",
                pkt.seg(0).as_ref().as_ptr(),
                pkt.data_len()
            );
            match msg_type {
                MsgType::Get => {
                    #[cfg(feature = "profiler")]
                    demikernel::timer!("handle get serialize and send");
                    self.serializer.handle_get_serialize_and_send(
                        pkt.msg_id(),
                        pkt.conn_id(),
                        end_batch,
                        &self.kv_server,
                        &self.linked_list_kv_server,
                        &pkt,
                        datapath,
                        &arena,
                    )?;
                }
                MsgType::Put => {
                    #[cfg(feature = "profiler")]
                    demikernel::timer!("handle put serialize and send");
                    self.serializer.handle_put_serialize_and_send(
                        pkt.msg_id(),
                        pkt.conn_id(),
                        end_batch,
                        &mut self.kv_server,
                        &mut self.linked_list_kv_server,
                        &mut self.mempool_ids,
                        &pkt,
                        datapath,
                        &arena,
                    )?;
                }
                MsgType::GetM(_size) => {
                    self.serializer.handle_getm_serialize_and_send(
                        pkt.msg_id(),
                        pkt.conn_id(),
                        end_batch,
                        &self.kv_server,
                        &self.linked_list_kv_server,
                        &pkt,
                        datapath,
                        arena,
                    )?;
                }
                MsgType::PutM(_size) => {
                    unimplemented!();
                }
                MsgType::GetList(_size) => {
                    self.serializer.handle_getlist_serialize_and_send(
                        pkt.msg_id(),
                        pkt.conn_id(),
                        end_batch,
                        &self.list_kv_server,
                        &self.linked_list_kv_server,
                        &pkt,
                        datapath,
                        &arena,
                    )?;
                }
                MsgType::PutList(_size) => {
                    unimplemented!();
                }
                MsgType::AddUser => {
                    #[cfg(feature = "profiler")]
                    demikernel::timer!("Handle add user");
                    let mut add_user = kv_serializer_hybrid::AddUser::<D>::new_in(arena);
                    add_user.deserialize(&pkt, REQ_TYPE_SIZE, arena)?;

                    let mut add_user_response =
                        kv_serializer_hybrid::AddUserResponse::<D>::new_in(arena);
                    let mut copy_context = CopyContext::new(arena, datapath)?;
                    match self.serializer.use_linked_list() {
                        true => {
                            let value = self
                                .linked_list_kv_server
                                .remove(add_user.get_keys()[0].to_str()?)
                                .unwrap();
                            // will increment the reference count of value, or copy into the copy
                            // context
                            add_user_response.set_first_value(
                                dynamic_rcsga_hybrid_hdr::CFBytes::new(
                                    value.as_ref().as_ref(),
                                    datapath,
                                    &mut copy_context,
                                )?,
                            );
                            for (key, value) in
                                add_user.get_keys().iter().zip(add_user.get_values().iter())
                            {
                                self.linked_list_kv_server.insert_with_copies(
                                    key.to_str()?,
                                    value.as_ref(),
                                    datapath,
                                    &mut self.mempool_ids,
                                )?;
                            }
                            datapath.queue_cornflakes_obj(
                                pkt.msg_id(),
                                pkt.conn_id(),
                                &mut copy_context,
                                add_user_response,
                                end_batch,
                            )?;
                        }
                        false => {
                            let value = self
                                .kv_server
                                .remove(add_user.get_keys()[0].to_str()?)
                                .unwrap();
                            add_user_response.set_first_value(
                                dynamic_rcsga_hybrid_hdr::CFBytes::new(
                                    value.as_ref(),
                                    datapath,
                                    &mut copy_context,
                                )?,
                            );
                            for (key, value) in
                                add_user.get_keys().iter().zip(add_user.get_values().iter())
                            {
                                self.kv_server.insert_with_copies(
                                    key.to_str()?,
                                    value.as_ref(),
                                    datapath,
                                    &mut self.mempool_ids,
                                )?;
                            }
                            datapath.queue_cornflakes_obj(
                                pkt.msg_id(),
                                pkt.conn_id(),
                                &mut copy_context,
                                add_user_response,
                                end_batch,
                            )?;
                        }
                    }
                }
                MsgType::FollowUnfollow => {
                    #[cfg(feature = "profiler")]
                    demikernel::timer!("Handle follow unfollow");
                    let mut follow_unfollow =
                        kv_serializer_hybrid::FollowUnfollow::<D>::new_in(arena);
                    follow_unfollow.deserialize(&pkt, REQ_TYPE_SIZE, arena)?;
                    tracing::debug!("Deserialized follow unfollow: {:?}", follow_unfollow);
                    let mut follow_unfollow_response =
                        kv_serializer_hybrid::FollowUnfollowResponse::<D>::new_in(arena);
                    let mut copy_context = CopyContext::new(arena, datapath)?;
                    follow_unfollow_response.init_original_values(2, arena);
                    let response_vals = follow_unfollow_response.get_mut_original_values();
                    match self.serializer.use_linked_list() {
                        true => {
                            for (cf_key, value) in follow_unfollow
                                .get_keys()
                                .iter()
                                .zip(follow_unfollow.get_values().iter())
                                .take(2)
                            {
                                let key = cf_key.to_str()?;
                                let old_value = self.linked_list_kv_server.remove(key).unwrap();
                                self.linked_list_kv_server.insert_with_copies(
                                    key,
                                    value.as_ref(),
                                    datapath,
                                    &mut self.mempool_ids,
                                )?;
                                response_vals.append(dynamic_rcsga_hybrid_hdr::CFBytes::new(
                                    old_value.as_ref().as_ref(),
                                    datapath,
                                    &mut copy_context,
                                )?);
                            }
                            datapath.queue_cornflakes_obj(
                                pkt.msg_id(),
                                pkt.conn_id(),
                                &mut copy_context,
                                follow_unfollow_response,
                                end_batch,
                            )?;
                        }
                        false => {
                            for (cf_key, value) in follow_unfollow
                                .get_keys()
                                .iter()
                                .zip(follow_unfollow.get_values().iter())
                                .take(2)
                            {
                                let key = cf_key.to_str()?;
                                let old_value = self.kv_server.remove(key).unwrap();
                                self.kv_server.insert_with_copies(
                                    key,
                                    value.as_ref(),
                                    datapath,
                                    &mut self.mempool_ids,
                                )?;
                                response_vals.append(dynamic_rcsga_hybrid_hdr::CFBytes::new(
                                    old_value.as_ref(),
                                    datapath,
                                    &mut copy_context,
                                )?);
                            }
                            datapath.queue_cornflakes_obj(
                                pkt.msg_id(),
                                pkt.conn_id(),
                                &mut copy_context,
                                follow_unfollow_response,
                                end_batch,
                            )?;
                        }
                    }
                }
                MsgType::PostTweet => {
                    #[cfg(feature = "profiler")]
                    demikernel::timer!("Handle post tweet");
                    let mut post_tweet = kv_serializer_hybrid::PostTweet::<D>::new_in(arena);
                    post_tweet.deserialize(&pkt, REQ_TYPE_SIZE, arena)?;

                    let mut post_tweet_response =
                        kv_serializer_hybrid::PostTweetResponse::<D>::new_in(arena);
                    let mut copy_context = CopyContext::new(arena, datapath)?;
                    post_tweet_response.init_values(3, arena);
                    let response_vals = post_tweet_response.get_mut_values();
                    match self.serializer.use_linked_list() {
                        true => {
                            for (cf_key, value) in post_tweet
                                .get_keys()
                                .iter()
                                .zip(post_tweet.get_values().iter())
                                .take(3)
                            {
                                let key = cf_key.to_str()?;
                                let old_value = self.linked_list_kv_server.remove(key).unwrap();
                                self.linked_list_kv_server.insert_with_copies(
                                    key,
                                    value.as_ref(),
                                    datapath,
                                    &mut self.mempool_ids,
                                )?;
                                response_vals.append(dynamic_rcsga_hybrid_hdr::CFBytes::new(
                                    old_value.as_ref().as_ref(),
                                    datapath,
                                    &mut copy_context,
                                )?);
                            }
                            for (cf_key, value) in post_tweet
                                .get_keys()
                                .iter()
                                .zip(post_tweet.get_values().iter())
                                .skip(3)
                                .take(2)
                            {
                                self.linked_list_kv_server.insert_with_copies(
                                    cf_key.to_str()?,
                                    value.as_ref(),
                                    datapath,
                                    &mut self.mempool_ids,
                                )?;
                            }
                            datapath.queue_cornflakes_obj(
                                pkt.msg_id(),
                                pkt.conn_id(),
                                &mut copy_context,
                                post_tweet_response,
                                end_batch,
                            )?;
                        }
                        false => {
                            for (cf_key, value) in post_tweet
                                .get_keys()
                                .iter()
                                .zip(post_tweet.get_values().iter())
                                .take(3)
                            {
                                let key = cf_key.to_str()?;
                                let old_value = self.kv_server.remove(key).unwrap();
                                self.kv_server.insert_with_copies(
                                    key,
                                    value.as_ref(),
                                    datapath,
                                    &mut self.mempool_ids,
                                )?;
                                response_vals.append(dynamic_rcsga_hybrid_hdr::CFBytes::new(
                                    old_value.as_ref(),
                                    datapath,
                                    &mut copy_context,
                                )?);
                            }
                            for (cf_key, value) in post_tweet
                                .get_keys()
                                .iter()
                                .zip(post_tweet.get_values().iter())
                                .skip(3)
                                .take(2)
                            {
                                self.kv_server.insert_with_copies(
                                    cf_key.to_str()?,
                                    value.as_ref(),
                                    datapath,
                                    &mut self.mempool_ids,
                                )?;
                            }

                            datapath.queue_cornflakes_obj(
                                pkt.msg_id(),
                                pkt.conn_id(),
                                &mut copy_context,
                                post_tweet_response,
                                end_batch,
                            )?;
                        }
                    }
                }
                MsgType::GetTimeline(_) => {
                    #[cfg(feature = "profiler")]
                    demikernel::timer!("Handle get timeline");
                    let mut get_timeline = kv_serializer_hybrid::GetTimeline::<D>::new_in(arena);
                    get_timeline.deserialize(&pkt, REQ_TYPE_SIZE, arena)?;

                    let mut get_timeline_response =
                        kv_serializer_hybrid::GetTimelineResponse::<D>::new_in(arena);
                    let mut copy_context = CopyContext::new(arena, datapath)?;
                    get_timeline_response.init_values(get_timeline.get_keys().len(), arena);
                    let response_vals = get_timeline_response.get_mut_values();
                    for (_i, key) in get_timeline.get_keys().iter().enumerate() {
                        let cf_bytes = match self.serializer.use_linked_list() {
                            true => {
                                let val = self.linked_list_kv_server.get(key.to_str()?).unwrap();
                                tracing::debug!(
                                    msg_id = pkt.msg_id(),
                                    conn_id = pkt.conn_id(),
                                    key_idx = _i,
                                    "Get timeline val size {}",
                                    val.as_ref().as_ref().len()
                                );
                                dynamic_rcsga_hybrid_hdr::CFBytes::new(
                                    val.as_ref().as_ref(),
                                    datapath,
                                    &mut copy_context,
                                )?
                            }
                            false => {
                                let val = self.kv_server.get(key.to_str()?).unwrap();
                                tracing::debug!(
                                    msg_id = pkt.msg_id(),
                                    conn_id = pkt.conn_id(),
                                    key_idx = _i,
                                    "Get timeline val size {}",
                                    val.as_ref().len()
                                );
                                dynamic_rcsga_hybrid_hdr::CFBytes::new(
                                    val.as_ref(),
                                    datapath,
                                    &mut copy_context,
                                )?
                            }
                        };
                        response_vals.append(cf_bytes);
                    }
                    tracing::debug!(
                        "Sending back get timeline response with msg id {}",
                        pkt.msg_id()
                    );
                    datapath.queue_cornflakes_obj(
                        pkt.msg_id(),
                        pkt.conn_id(),
                        &mut copy_context,
                        get_timeline_response,
                        end_batch,
                    )?;
                }
                _ => {
                    unimplemented!();
                }
            }
        }

        //arena.reset();
        Ok(())
    }

    #[inline]
    fn process_requests_arena_ordered_sga(
        &mut self,
        sga: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        datapath: &mut Self::Datapath,
        arena: &mut bumpalo::Bump,
    ) -> Result<()> {
        let end = sga.len();
        for (i, pkt) in sga.into_iter().enumerate() {
            let end_batch = i == (end - 1);
            let msg_type = MsgType::from_packet(&pkt)?;
            tracing::debug!(
                msg_type =? msg_type,
                "Received packet with data {:?} ptr and length {}",
                pkt.seg(0).as_ref().as_ptr(),
                pkt.data_len()
            );
            match msg_type {
                MsgType::Get => {
                    let sga = self.serializer.handle_get(
                        &self.kv_server,
                        &self.linked_list_kv_server,
                        &pkt,
                        datapath,
                        &arena,
                    )?;
                    datapath
                        .queue_arena_datapath_sga((pkt.msg_id(), pkt.conn_id(), sga), end_batch)?;
                }
                MsgType::Put => {
                    let sga = self.serializer.handle_put(
                        &mut self.kv_server,
                        &mut self.linked_list_kv_server,
                        &mut self.mempool_ids,
                        &pkt,
                        datapath,
                        &arena,
                    )?;
                    datapath
                        .queue_arena_ordered_rcsga((pkt.msg_id(), pkt.conn_id(), sga), end_batch)?;
                }
                MsgType::GetM(_size) => {
                    let sga = self.serializer.handle_getm(
                        &self.kv_server,
                        &self.linked_list_kv_server,
                        &pkt,
                        datapath,
                        &arena,
                    )?;
                    datapath
                        .queue_arena_datapath_sga((pkt.msg_id(), pkt.conn_id(), sga), end_batch)?;
                }
                MsgType::PutM(_size) => {
                    let sga = self.serializer.handle_putm(
                        &mut self.kv_server,
                        &mut self.linked_list_kv_server,
                        &mut self.mempool_ids,
                        &pkt,
                        datapath,
                        &arena,
                    )?;
                    datapath
                        .queue_arena_ordered_rcsga((pkt.msg_id(), pkt.conn_id(), sga), end_batch)?;
                }
                MsgType::GetList(_size) => {
                    let sga = self.serializer.handle_getlist(
                        &self.list_kv_server,
                        &self.linked_list_kv_server,
                        &pkt,
                        datapath,
                        &arena,
                    )?;
                    datapath
                        .queue_arena_datapath_sga((pkt.msg_id(), pkt.conn_id(), sga), end_batch)?;
                }
                MsgType::PutList(_size) => {
                    let sga = self.serializer.handle_putlist(
                        &mut self.list_kv_server,
                        &mut self.linked_list_kv_server,
                        &mut self.mempool_ids,
                        &pkt,
                        datapath,
                        &arena,
                    )?;
                    datapath
                        .queue_arena_ordered_rcsga((pkt.msg_id(), pkt.conn_id(), sga), end_batch)?;
                }
                MsgType::AddUser => {
                    #[cfg(feature = "profiler")]
                    demikernel::timer!("Handle add user");
                    let mut add_user = AddUser::<D>::new();
                    add_user.deserialize_from_pkt(&pkt, REQ_TYPE_SIZE)?;

                    let mut add_user_response = AddUserResponse::<D>::new();
                    match self.serializer.use_linked_list() {
                        true => {
                            /*let value = self
                                .linked_list_kv_server
                                .remove(add_user.get_keys()[0].to_str()?)
                                .unwrap();
                            add_user_response
                                .set_first_value(CFBytes::new(value.as_ref().as_ref(), datapath)?);
                            for (key, value) in
                                add_user.get_keys().iter().zip(add_user.get_values().iter())
                            {
                                self.linked_list_kv_server.insert_with_copies(
                                    key.to_str()?,
                                    value.as_bytes(),
                                    datapath,
                                    &mut self.mempool_ids,
                                )?;
                            }
                            let mut arena_sga = ArenaOrderedRcSga::allocate(
                                add_user_response.num_scatter_gather_entries(),
                                &arena,
                            );
                            add_user_response.serialize_into_arena_sga(
                                &mut arena_sga,
                                arena,
                                datapath,
                                self.serializer.with_copies(),
                            )?;
                            datapath.queue_arena_ordered_rcsga(
                                (pkt.msg_id(), pkt.conn_id(), arena_sga),
                                end_batch,
                            )?*/
                            unimplemented!();
                        }
                        false => {
                            let value = self
                                .kv_server
                                .remove(add_user.get_keys()[0].to_str()?)
                                .unwrap();
                            add_user_response
                                .set_first_value(CFBytes::new(value.as_ref(), datapath)?);
                            for (key, value) in
                                add_user.get_keys().iter().zip(add_user.get_values().iter())
                            {
                                self.kv_server.insert_with_copies(
                                    key.to_str()?,
                                    value.as_bytes(),
                                    datapath,
                                    &mut self.mempool_ids,
                                )?;
                            }
                            let mut arena_sga = ArenaOrderedRcSga::allocate(
                                add_user_response.num_scatter_gather_entries(),
                                &arena,
                            );
                            add_user_response.serialize_into_arena_sga(
                                &mut arena_sga,
                                arena,
                                datapath,
                                self.serializer.with_copies(),
                            )?;

                            datapath.queue_arena_ordered_rcsga(
                                (pkt.msg_id(), pkt.conn_id(), arena_sga),
                                end_batch,
                            )?;
                        }
                    }
                }
                MsgType::FollowUnfollow => {
                    #[cfg(feature = "profiler")]
                    demikernel::timer!("Handle follow unfollow");
                    let mut follow_unfollow = FollowUnfollow::<D>::new();
                    follow_unfollow.deserialize_from_pkt(&pkt, REQ_TYPE_SIZE)?;

                    tracing::debug!("Deserialized follow unfollow: {:?}", follow_unfollow);
                    let mut follow_unfollow_response = FollowUnfollowResponse::<D>::new();
                    follow_unfollow_response.init_original_values(2);
                    let response_vals = follow_unfollow_response.get_mut_original_values();
                    match self.serializer.use_linked_list() {
                        true => {
                            /*for (i, (cf_key, value)) in follow_unfollow
                                .get_keys()
                                .iter()
                                .zip(follow_unfollow.get_values().iter())
                                .take(2)
                                .enumerate()
                            {
                                let key = cf_key.to_str()?;
                                let old_value = self.linked_list_kv_server.remove(key).unwrap();
                                response_vals
                                    .append(CFBytes::new(old_value.as_ref().as_ref(), datapath)?);
                                self.linked_list_kv_server.insert_with_copies(
                                    key,
                                    value.as_bytes(),
                                    datapath,
                                    &mut self.mempool_ids,
                                )?;
                            }
                            let mut arena_sga = ArenaOrderedRcSga::allocate(
                                follow_unfollow_response.num_scatter_gather_entries(),
                                &arena,
                            );
                            follow_unfollow_response.serialize_into_arena_sga(
                                &mut arena_sga,
                                arena,
                                datapath,
                                self.serializer.with_copies(),
                            )?;

                            datapath.queue_arena_ordered_rcsga(
                                (pkt.msg_id(), pkt.conn_id(), arena_sga),
                                end_batch,
                            )?;*/
                            unimplemented!();
                        }
                        false => {
                            let mut old_values: [D::DatapathBuffer; 2] =
                                [D::DatapathBuffer::default(), D::DatapathBuffer::default()];
                            for (i, (cf_key, value)) in follow_unfollow
                                .get_keys()
                                .iter()
                                .zip(follow_unfollow.get_values().iter())
                                .take(2)
                                .enumerate()
                            {
                                let key = cf_key.to_str()?;
                                let old_value = self.kv_server.remove(key).unwrap();
                                old_values[i] = old_value;
                                self.kv_server.insert_with_copies(
                                    key,
                                    value.as_bytes(),
                                    datapath,
                                    &mut self.mempool_ids,
                                )?;
                            }
                            response_vals.append(CFBytes::new(old_values[0].as_ref(), datapath)?);
                            response_vals.append(CFBytes::new(old_values[1].as_ref(), datapath)?);
                            let mut arena_sga = ArenaOrderedRcSga::allocate(
                                follow_unfollow_response.num_scatter_gather_entries(),
                                &arena,
                            );
                            follow_unfollow_response.serialize_into_arena_sga(
                                &mut arena_sga,
                                arena,
                                datapath,
                                self.serializer.with_copies(),
                            )?;

                            datapath.queue_arena_ordered_rcsga(
                                (pkt.msg_id(), pkt.conn_id(), arena_sga),
                                end_batch,
                            )?;
                        }
                    }
                }
                MsgType::PostTweet => {
                    #[cfg(feature = "profiler")]
                    demikernel::timer!("Handle post tweet");
                    let mut post_tweet = PostTweet::<D>::new();
                    post_tweet.deserialize_from_pkt(&pkt, REQ_TYPE_SIZE)?;

                    let mut post_tweet_response = PostTweetResponse::<D>::new();
                    post_tweet_response.init_values(3);
                    let response_vals = post_tweet_response.get_mut_values();
                    match self.serializer.use_linked_list() {
                        true => {
                            /*for (i, (cf_key, value)) in post_tweet
                                .get_keys()
                                .iter()
                                .zip(post_tweet.get_values().iter())
                                .take(3)
                                .enumerate()
                            {
                                let key = cf_key.to_str()?;
                                let old_value = self.linked_list_kv_server.remove(key).unwrap();
                                response_vals
                                    .append(CFBytes::new(old_value.as_ref().as_ref(), datapath)?);
                                self.linked_list_kv_server.insert_with_copies(
                                    key,
                                    value.as_bytes(),
                                    datapath,
                                    &mut self.mempool_ids,
                                )?;
                            }
                            for (cf_key, value) in post_tweet
                                .get_keys()
                                .iter()
                                .zip(post_tweet.get_values().iter())
                                .skip(3)
                                .take(2)
                            {
                                self.linked_list_kv_server.insert_with_copies(
                                    cf_key.to_str()?,
                                    value.as_bytes(),
                                    datapath,
                                    &mut self.mempool_ids,
                                )?;
                            }
                            let mut arena_sga = ArenaOrderedRcSga::allocate(
                                post_tweet_response.num_scatter_gather_entries(),
                                &arena,
                            );
                            post_tweet_response.serialize_into_arena_sga(
                                &mut arena_sga,
                                arena,
                                datapath,
                                self.serializer.with_copies(),
                            )?;

                            datapath.queue_arena_ordered_rcsga(
                                (pkt.msg_id(), pkt.conn_id(), arena_sga),
                                end_batch,
                            )?;*/
                            unimplemented!();
                        }
                        false => {
                            let mut old_values: [D::DatapathBuffer; 3] = [
                                D::DatapathBuffer::default(),
                                D::DatapathBuffer::default(),
                                D::DatapathBuffer::default(),
                            ];
                            for (i, (cf_key, value)) in post_tweet
                                .get_keys()
                                .iter()
                                .zip(post_tweet.get_values().iter())
                                .take(3)
                                .enumerate()
                            {
                                let key = cf_key.to_str()?;
                                let old_value = self.kv_server.remove(key).unwrap();
                                old_values[i] = old_value;
                                self.kv_server.insert_with_copies(
                                    key,
                                    value.as_bytes(),
                                    datapath,
                                    &mut self.mempool_ids,
                                )?;
                            }
                            response_vals.append(CFBytes::new(old_values[0].as_ref(), datapath)?);
                            response_vals.append(CFBytes::new(old_values[1].as_ref(), datapath)?);
                            response_vals.append(CFBytes::new(old_values[2].as_ref(), datapath)?);
                            for (cf_key, value) in post_tweet
                                .get_keys()
                                .iter()
                                .zip(post_tweet.get_values().iter())
                                .skip(3)
                                .take(2)
                            {
                                self.kv_server.insert_with_copies(
                                    cf_key.to_str()?,
                                    value.as_bytes(),
                                    datapath,
                                    &mut self.mempool_ids,
                                )?;
                            }
                            let mut arena_sga = ArenaOrderedRcSga::allocate(
                                post_tweet_response.num_scatter_gather_entries(),
                                &arena,
                            );
                            post_tweet_response.serialize_into_arena_sga(
                                &mut arena_sga,
                                arena,
                                datapath,
                                self.serializer.with_copies(),
                            )?;

                            datapath.queue_arena_ordered_rcsga(
                                (pkt.msg_id(), pkt.conn_id(), arena_sga),
                                end_batch,
                            )?;
                        }
                    }
                }
                MsgType::GetTimeline(_) => {
                    #[cfg(feature = "profiler")]
                    demikernel::timer!("Handle get timeline");
                    let mut get_timeline = GetTimeline::<D>::new();
                    get_timeline.deserialize_from_pkt(&pkt, REQ_TYPE_SIZE)?;

                    let mut get_timeline_response = GetTimelineResponse::<D>::new();
                    get_timeline_response.init_values(get_timeline.get_keys().len());
                    let response_vals = get_timeline_response.get_mut_values();
                    for (_i, key) in get_timeline.get_keys().iter().enumerate() {
                        let cf_bytes = match self.serializer.use_linked_list() {
                            true => {
                                /*let val = self.linked_list_kv_server.get(key.to_str()?).unwrap();
                                tracing::debug!(
                                    msg_id = pkt.msg_id(),
                                    conn_id = pkt.conn_id(),
                                    key_idx = _i,
                                    "Get timeline val size {}",
                                    val.as_ref().as_ref().len()
                                );
                                CFBytes::new(val.as_ref().as_ref(), datapath)?*/
                                unimplemented!();
                            }
                            false => {
                                let val = self.kv_server.get(key.to_str()?).unwrap();
                                tracing::debug!(
                                    msg_id = pkt.msg_id(),
                                    conn_id = pkt.conn_id(),
                                    key_idx = _i,
                                    "Get timeline val size {}",
                                    val.as_ref().len()
                                );
                                CFBytes::new(val.as_ref(), datapath)?
                            }
                        };
                        response_vals.append(cf_bytes);
                    }
                    let mut arena_sga = ArenaOrderedRcSga::allocate(
                        get_timeline_response.num_scatter_gather_entries(),
                        &arena,
                    );
                    get_timeline_response.serialize_into_arena_sga(
                        &mut arena_sga,
                        arena,
                        datapath,
                        self.serializer.with_copies(),
                    )?;

                    tracing::debug!(
                        "Sending back get timeline response with msg id {}",
                        pkt.msg_id()
                    );
                    datapath.queue_arena_ordered_rcsga(
                        (pkt.msg_id(), pkt.conn_id(), arena_sga),
                        end_batch,
                    )?;
                }
                _ => {
                    unimplemented!();
                }
            }
        }
        arena.reset();
        Ok(())
    }
}

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct CornflakesClient<D>
where
    D: Datapath,
{
    datapath: PhantomData<D>,
}

impl<D> ClientSerializer<D> for CornflakesClient<D>
where
    D: Datapath,
{
    fn new() -> Self
    where
        Self: Sized,
    {
        CornflakesClient {
            datapath: PhantomData::default(),
        }
    }

    fn deserialize_get_response(&self, buf: &[u8]) -> Result<Vec<u8>> {
        let mut get_resp = GetResp::<D>::new();
        get_resp.deserialize_from_buf(buf)?;
        if get_resp.has_val() {
            return Ok(get_resp.get_val().as_bytes().to_vec());
        } else {
            return Ok(vec![]);
        }
    }

    fn deserialize_getm_response(&self, buf: &[u8]) -> Result<Vec<Vec<u8>>> {
        let mut getm_resp = GetMResp::<D>::new();
        getm_resp.deserialize_from_buf(buf)?;
        if getm_resp.has_vals() {
            let vec: Vec<Vec<u8>> = getm_resp
                .get_vals()
                .iter()
                .map(|cf_bytes| cf_bytes.as_bytes().to_vec())
                .collect();
            return Ok(vec);
        } else {
            return Ok(vec![]);
        }
    }

    fn deserialize_getlist_response(&self, buf: &[u8]) -> Result<Vec<Vec<u8>>> {
        let mut getlist_resp = GetListResp::<D>::new();
        getlist_resp.deserialize_from_buf(buf)?;
        if getlist_resp.has_val_list() {
            let vec: Vec<Vec<u8>> = getlist_resp
                .get_val_list()
                .iter()
                .map(|cf_bytes| cf_bytes.as_bytes().to_vec())
                .collect();
            return Ok(vec);
        } else {
            return Ok(vec![]);
        }
    }

    fn check_add_user_num_values(&self, buf: &[u8]) -> Result<usize> {
        let mut add_user = AddUserResponse::<D>::new();
        add_user.deserialize_from_buf(buf)?;
        return Ok(add_user.has_first_value() as usize);
    }

    fn check_follow_unfollow_num_values(&self, buf: &[u8]) -> Result<usize> {
        let mut follow_unfollow = FollowUnfollowResponse::<D>::new();
        follow_unfollow.deserialize_from_buf(buf)?;
        if !follow_unfollow.has_original_values() {
            return Ok(0);
        } else {
            return Ok(follow_unfollow.get_original_values().len());
        }
    }

    fn check_post_tweet_num_values(&self, buf: &[u8]) -> Result<usize> {
        let mut post_tweet = PostTweetResponse::<D>::new();
        post_tweet.deserialize_from_buf(buf)?;
        if !post_tweet.has_values() {
            return Ok(0);
        } else {
            return Ok(post_tweet.get_values().len());
        }
    }

    fn check_get_timeline_num_values(&self, buf: &[u8]) -> Result<usize> {
        let mut get_timeline = GetTimelineResponse::<D>::new();
        get_timeline.deserialize_from_buf(buf)?;
        if !get_timeline.has_values() {
            return Ok(0);
        } else {
            return Ok(get_timeline.get_values().len());
        }
    }

    fn check_retwis_response_num_values(&self, buf: &[u8]) -> Result<usize> {
        let mut retwis_response = RetwisResponse::<D>::new();
        retwis_response.deserialize_from_buf(buf)?;
        if !retwis_response.has_get_responses() {
            return Ok(0);
        } else {
            return Ok(retwis_response.get_get_responses().len());
        }
    }

    fn serialize_get(&self, buf: &mut [u8], key: &str, datapath: &D) -> Result<usize> {
        let mut get = GetReq::<D>::new();
        get.set_key(CFString::new_from_str(key));
        get.serialize_into_buf(datapath, buf)
    }

    fn serialize_put(&self, buf: &mut [u8], key: &str, value: &str, datapath: &D) -> Result<usize> {
        let mut put = PutReq::new();
        put.set_key(CFString::new_from_str(key));
        put.set_val(CFBytes::new_from_bytes(value.as_bytes()));
        put.serialize_into_buf(datapath, buf)
    }

    fn serialize_getm(&self, buf: &mut [u8], keys: &Vec<String>, datapath: &D) -> Result<usize> {
        let mut getm = GetMReq::<D>::new();
        getm.init_keys(keys.len());
        let keys_list = getm.get_mut_keys();
        for key in keys.iter() {
            keys_list.append(CFString::new_from_str(key.as_str()));
        }
        tracing::debug!(
            "Serializing {:?} GetM into buffer of length {}",
            getm,
            buf.len()
        );
        getm.serialize_into_buf(datapath, buf)
    }

    fn serialize_putm(
        &self,
        buf: &mut [u8],
        keys: &Vec<String>,
        values: &Vec<String>,
        datapath: &D,
    ) -> Result<usize> {
        let mut putm = PutMReq::<D>::new();
        putm.init_keys(keys.len());
        let keys_list = putm.get_mut_keys();
        for key in keys.iter() {
            keys_list.append(CFString::new_from_str(key.as_str()));
        }

        putm.init_vals(values.len());
        let vals_list = putm.get_mut_vals();
        for val in values.iter() {
            vals_list.append(CFBytes::new_from_bytes(val.as_str().as_bytes()));
        }
        putm.serialize_into_buf(datapath, buf)
    }

    fn serialize_get_list(&self, buf: &mut [u8], key: &str, datapath: &D) -> Result<usize> {
        let mut get = GetListReq::<D>::new();
        get.set_key(CFString::new_from_str(key));
        get.set_range_start(0);
        get.set_range_end(-1);
        tracing::debug!(
            "Serializing get list command: {:?}; total header size is: {}, dynamic header start is {}, has range start: {}, has range end: {}",
            get,
            get.total_header_size(false, false),
            get.dynamic_header_start(),
            get.has_range_start(), get.has_range_end(),
        );
        let size = get.serialize_into_buf(datapath, buf)?;
        tracing::debug!("Serialized buffer: {:?}", &buf[0..size]);
        Ok(size)
    }

    fn serialize_put_list(
        &self,
        buf: &mut [u8],
        key: &str,
        values: &Vec<String>,
        datapath: &D,
    ) -> Result<usize> {
        let mut put = PutListReq::<D>::new();
        put.set_key(CFString::new_from_str(key));
        put.init_vals(values.len());
        let vals = put.get_mut_vals();
        for val in values.iter() {
            vals.append(CFBytes::new_from_bytes(val.as_str().as_bytes()));
        }
        put.serialize_into_buf(datapath, buf)
    }

    fn serialize_append(
        &self,
        buf: &mut [u8],
        key: &str,
        value: &str,
        datapath: &D,
    ) -> Result<usize> {
        let mut append = PutReq::new();
        append.set_key(CFString::new_from_str(key));
        append.set_val(CFBytes::new_from_bytes(value.as_bytes()));
        append.serialize_into_buf(datapath, buf)
    }

    fn serialize_add_user(
        &self,
        buf: &mut [u8],
        keys: &Vec<&str>,
        values: &Vec<String>,
        datapath: &D,
    ) -> Result<usize> {
        let mut add_user = AddUser::new();
        add_user.init_keys(keys.len());
        let skeys = add_user.get_mut_keys();
        for key in keys.iter() {
            skeys.append(CFString::new_from_str(key));
        }
        add_user.init_values(values.len());
        let svalues = add_user.get_mut_values();
        for value in values.iter() {
            svalues.append(CFBytes::new_from_bytes(value.as_str().as_bytes()));
        }
        add_user.serialize_into_buf(datapath, buf)
    }

    fn serialize_add_follow_unfollow(
        &self,
        buf: &mut [u8],
        keys: &Vec<&str>,
        values: &Vec<String>,
        datapath: &D,
    ) -> Result<usize> {
        let mut follow_unfollow = FollowUnfollow::new();
        follow_unfollow.init_keys(keys.len());
        let skeys = follow_unfollow.get_mut_keys();
        for key in keys.iter() {
            skeys.append(CFString::new_from_str(key));
        }
        follow_unfollow.init_values(values.len());
        let svalues = follow_unfollow.get_mut_values();
        for value in values.iter() {
            svalues.append(CFBytes::new_from_bytes(value.as_str().as_bytes()));
        }
        follow_unfollow.serialize_into_buf(datapath, buf)
    }

    fn serialize_post_tweet(
        &self,
        buf: &mut [u8],
        keys: &Vec<&str>,
        values: &Vec<String>,
        datapath: &D,
    ) -> Result<usize> {
        let mut post_tweet = PostTweet::new();
        post_tweet.init_keys(keys.len());
        let skeys = post_tweet.get_mut_keys();
        for key in keys.iter() {
            skeys.append(CFString::new_from_str(key));
        }
        post_tweet.init_values(values.len());
        let svalues = post_tweet.get_mut_values();
        for value in values.iter() {
            svalues.append(CFBytes::new_from_bytes(value.as_str().as_bytes()));
        }
        post_tweet.serialize_into_buf(datapath, buf)
    }

    fn serialize_get_timeline(
        &self,
        buf: &mut [u8],
        keys: &Vec<&str>,
        _values: &Vec<String>,
        datapath: &D,
    ) -> Result<usize> {
        tracing::debug!(keys_len = keys.len(), "Serializing get timeline");
        let mut get_timeline = GetTimeline::new();
        get_timeline.init_keys(keys.len());
        let skeys = get_timeline.get_mut_keys();
        for key in keys.iter() {
            skeys.append(CFString::new_from_str(key));
        }
        get_timeline.serialize_into_buf(datapath, buf)
    }
}
