use super::{
    get_equal_fields, init_payloads, init_payloads_as_vec, CerealizeClient, CerealizeMessage,
};
use color_eyre::eyre::Result;
use cornflakes_libos::{
    mem::MmapMetadata, CornPtr, Cornflake, Datapath, ReceivedPkt, ScatterGather,
};
use cornflakes_utils::{SimpleMessageType, TreeDepth};
use cxx;
use std::slice;
#[cxx::bridge]
mod ffi {
    unsafe extern "C++" {
        include!("echo-server/src/cereal/include/cereal_headers.hh");
        type SingleCereal;
        type ListCereal;
        type Tree1Cereal;
        type Tree2Cereal;
        type Tree3Cereal;
        type Tree4Cereal;
        type Tree5Cereal;

        fn set_data(self: &SingleCereal, value: &[u8]);
        fn get_data(self: &SingleCereal) -> &CxxString;
        fn serialized_size(self: &SingleCereal) -> usize;
        fn serialize_to_array(self: &SingleCereal, buf: &mut [u8]);
        fn _equals(self: &SingleCereal, other: UniquePtr<SingleCereal>) -> bool;
        fn new_single_cereal() -> UniquePtr<SingleCereal>;
        fn deserialize_single_cereal_from_array(buf: &[u8]) -> UniquePtr<SingleCereal>;

        fn append(self: &ListCereal, value: &[u8]);
        fn get(self: &ListCereal, idx: usize) -> &CxxString;
        fn _set(self: &ListCereal, idx: usize, value: &[u8]);
        fn serialized_size(self: &ListCereal) -> usize;
        fn serialize_to_array(self: &ListCereal, buf: &mut [u8]);
        fn new_list_cereal() -> UniquePtr<ListCereal>;
        fn deserialize_list_cereal_from_array(buf: &[u8]) -> UniquePtr<ListCereal>;

        fn set_left(self: &Tree1Cereal, left: UniquePtr<SingleCereal>);
        fn set_right(self: &Tree1Cereal, left: UniquePtr<SingleCereal>);
        fn serialized_size(self: &Tree1Cereal) -> usize;
        fn serialize_to_array(self: &Tree1Cereal, buf: &mut [u8]);
        fn equals(self: &Tree1Cereal, other: UniquePtr<Tree1Cereal>) -> bool;
        fn new_tree1_cereal() -> UniquePtr<Tree1Cereal>;
        fn deserialize_tree1_cereal_from_array(buf: &[u8]) -> UniquePtr<Tree1Cereal>;
        fn reserialize_tree1(input: UniquePtr<Tree1Cereal>) -> UniquePtr<Tree1Cereal>;

        fn set_left(self: &Tree2Cereal, left: UniquePtr<Tree1Cereal>);
        fn set_right(self: &Tree2Cereal, left: UniquePtr<Tree1Cereal>);
        fn serialized_size(self: &Tree2Cereal) -> usize;
        fn serialize_to_array(self: &Tree2Cereal, buf: &mut [u8]);
        fn equals(self: &Tree2Cereal, other: UniquePtr<Tree2Cereal>) -> bool;
        fn new_tree2_cereal() -> UniquePtr<Tree2Cereal>;
        fn deserialize_tree2_cereal_from_array(buf: &[u8]) -> UniquePtr<Tree2Cereal>;
        fn reserialize_tree2(input: UniquePtr<Tree2Cereal>) -> UniquePtr<Tree2Cereal>;

        fn set_left(self: &Tree3Cereal, left: UniquePtr<Tree2Cereal>);
        fn set_right(self: &Tree3Cereal, left: UniquePtr<Tree2Cereal>);
        fn serialized_size(self: &Tree3Cereal) -> usize;
        fn serialize_to_array(self: &Tree3Cereal, buf: &mut [u8]);
        fn equals(self: &Tree3Cereal, other: UniquePtr<Tree3Cereal>) -> bool;
        fn new_tree3_cereal() -> UniquePtr<Tree3Cereal>;
        fn deserialize_tree3_cereal_from_array(buf: &[u8]) -> UniquePtr<Tree3Cereal>;
        fn reserialize_tree3(input: UniquePtr<Tree3Cereal>) -> UniquePtr<Tree3Cereal>;

        fn set_left(self: &Tree4Cereal, left: UniquePtr<Tree3Cereal>);
        fn set_right(self: &Tree4Cereal, left: UniquePtr<Tree3Cereal>);
        fn serialized_size(self: &Tree4Cereal) -> usize;
        fn serialize_to_array(self: &Tree4Cereal, buf: &mut [u8]);
        fn equals(self: &Tree4Cereal, other: UniquePtr<Tree4Cereal>) -> bool;
        fn new_tree4_cereal() -> UniquePtr<Tree4Cereal>;
        fn deserialize_tree4_cereal_from_array(buf: &[u8]) -> UniquePtr<Tree4Cereal>;
        fn reserialize_tree4(input: UniquePtr<Tree4Cereal>) -> UniquePtr<Tree4Cereal>;

        fn set_left(self: &Tree5Cereal, left: UniquePtr<Tree4Cereal>);
        fn set_right(self: &Tree5Cereal, left: UniquePtr<Tree4Cereal>);
        fn serialized_size(self: &Tree5Cereal) -> usize;
        fn serialize_to_array(self: &Tree5Cereal, buf: &mut [u8]);
        fn equals(self: &Tree5Cereal, other: UniquePtr<Tree5Cereal>) -> bool;
        fn new_tree5_cereal() -> UniquePtr<Tree5Cereal>;
        fn deserialize_tree5_cereal_from_array(buf: &[u8]) -> UniquePtr<Tree5Cereal>;
        fn reserialize_tree5(input: UniquePtr<Tree5Cereal>) -> UniquePtr<Tree5Cereal>;
    }
}

fn get_tree5_message<'a>(
    indices: &[usize],
    payloads: &Vec<&'a [u8]>,
) -> cxx::UniquePtr<ffi::Tree5Cereal> {
    let tree5 = ffi::new_tree5_cereal();
    tree5.set_left(get_tree4_message(&indices[0..16], payloads));
    tree5.set_right(get_tree4_message(&indices[16..32], payloads));
    tree5
}

fn get_tree4_message<'a>(
    indices: &[usize],
    payloads: &Vec<&'a [u8]>,
) -> cxx::UniquePtr<ffi::Tree4Cereal> {
    let tree4 = ffi::new_tree4_cereal();
    tree4.set_left(get_tree3_message(&indices[0..8], payloads));
    tree4.set_right(get_tree3_message(&indices[8..16], payloads));
    tree4
}

fn get_tree3_message<'a>(
    indices: &[usize],
    payloads: &Vec<&'a [u8]>,
) -> cxx::UniquePtr<ffi::Tree3Cereal> {
    let tree3 = ffi::new_tree3_cereal();
    tree3.set_left(get_tree2_message(&indices[0..4], payloads));
    tree3.set_right(get_tree2_message(&indices[4..8], payloads));
    tree3
}

fn get_tree2_message<'a>(
    indices: &[usize],
    payloads: &Vec<&'a [u8]>,
) -> cxx::UniquePtr<ffi::Tree2Cereal> {
    let tree2 = ffi::new_tree2_cereal();
    tree2.set_left(get_tree1_message(&indices[0..2], payloads));
    tree2.set_right(get_tree1_message(&indices[2..4], payloads));
    tree2
}

fn get_tree1_message<'a>(
    indices: &[usize],
    payloads: &Vec<&'a [u8]>,
) -> cxx::UniquePtr<ffi::Tree1Cereal> {
    let tree1 = ffi::new_tree1_cereal();
    tree1.set_left(get_single_buffer_message(payloads[indices[0]]));
    tree1.set_right(get_single_buffer_message(payloads[indices[1]]));
    tree1
}

fn get_single_buffer_message<'a>(payload: &'a [u8]) -> cxx::UniquePtr<ffi::SingleCereal> {
    let single_cereal = ffi::new_single_cereal();
    single_cereal.set_data(payload);
    single_cereal
}

fn context_size(message_type: SimpleMessageType, size: usize) -> usize {
    let payloads_vec = init_payloads_as_vec(&get_equal_fields(message_type, size));
    let payloads: Vec<&[u8]> = payloads_vec.iter().map(|vec| vec.as_slice()).collect();

    match message_type {
        SimpleMessageType::Single => {
            assert!(payloads.len() == 1);
            let single_cereal = ffi::new_single_cereal();
            single_cereal.set_data(&payloads[0]);
            single_cereal.serialized_size()
        }
        SimpleMessageType::List(list_size) => {
            assert!(payloads.len() == list_size);
            let list_cereal = ffi::new_list_cereal();
            for payload in payloads.iter() {
                tracing::debug!("Append payload of size {:?}", payload.len());
                list_cereal.append(payload);
            }
            list_cereal.serialized_size()
        }
        SimpleMessageType::Tree(depth) => match depth {
            TreeDepth::One => {
                assert!(payloads.len() == 2);
                let tree1_cereal = get_tree1_message(&[0, 1], &payloads);
                tree1_cereal.serialized_size()
            }
            TreeDepth::Two => {
                assert!(payloads.len() == 4);
                let tree2_cereal = get_tree2_message(&[0, 1, 2, 3], &payloads);
                tree2_cereal.serialized_size()
            }
            TreeDepth::Three => {
                let indices: Vec<usize> = (0usize..8usize).collect();
                let tree3_cereal = get_tree3_message(&indices, &payloads);
                tree3_cereal.serialized_size()
            }
            TreeDepth::Four => {
                let indices: Vec<usize> = (0usize..16usize).collect();
                let tree4_cereal = get_tree4_message(&indices, &payloads);
                tree4_cereal.serialized_size()
            }
            TreeDepth::Five => {
                let indices: Vec<usize> = (0usize..32usize).collect();
                let tree5_cereal = get_tree5_message(&indices, &payloads);
                tree5_cereal.serialized_size()
            }
        },
    }
}

pub struct CerealSerializer {
    message_type: SimpleMessageType,
    context_size: usize,
}

impl CerealSerializer {
    pub fn new(message_type: SimpleMessageType, size: usize) -> CerealSerializer {
        let ctx = context_size(message_type, size);
        tracing::debug!(size = ctx, "Calculated context size.");
        CerealSerializer {
            message_type: message_type,
            context_size: ctx,
        }
    }
}

impl<D> CerealizeMessage<D> for CerealSerializer
where
    D: Datapath,
{
    type Ctx = Vec<u8>;

    fn message_type(&self) -> SimpleMessageType {
        self.message_type
    }

    fn process_msg<'registered, 'normal: 'registered>(
        &self,
        recved_message: &'registered ReceivedPkt<D>,
        ctx: &'normal mut Self::Ctx,
    ) -> Result<Cornflake<'registered, 'normal>> {
        let mut cf = Cornflake::with_capacity(1);
        match self.message_type {
            SimpleMessageType::Single => {
                tracing::debug!(buf=?recved_message.index(0).as_ref().as_ptr(), "In process msg for cereal");
                let object_deser =
                    ffi::deserialize_single_cereal_from_array(recved_message.index(0).as_ref());
                let object_ser = ffi::new_single_cereal();
                object_ser.set_data(object_deser.get_data().as_bytes());
                object_ser.serialize_to_array(ctx.as_mut_slice());
            }
            SimpleMessageType::List(list_length) => {
                let object_deser =
                    ffi::deserialize_list_cereal_from_array(recved_message.index(0).as_ref());
                let object_ser = ffi::new_list_cereal();
                for i in 0..list_length {
                    object_ser.append(object_deser.get(i).as_bytes());
                }
                object_ser.serialize_to_array(ctx.as_mut_slice());
            }
            SimpleMessageType::Tree(depth) => match depth {
                TreeDepth::One => {
                    let object_deser =
                        ffi::deserialize_tree1_cereal_from_array(recved_message.index(0).as_ref());
                    let object_ser = ffi::reserialize_tree1(object_deser);
                    object_ser.serialize_to_array(ctx.as_mut_slice());
                }
                TreeDepth::Two => {
                    let object_deser =
                        ffi::deserialize_tree2_cereal_from_array(recved_message.index(0).as_ref());
                    let object_ser = ffi::reserialize_tree2(object_deser);
                    object_ser.serialize_to_array(ctx.as_mut_slice());
                }
                TreeDepth::Three => {
                    let object_deser =
                        ffi::deserialize_tree3_cereal_from_array(recved_message.index(0).as_ref());
                    let object_ser = ffi::reserialize_tree3(object_deser);
                    object_ser.serialize_to_array(ctx.as_mut_slice());
                }
                TreeDepth::Four => {
                    let object_deser =
                        ffi::deserialize_tree4_cereal_from_array(recved_message.index(0).as_ref());
                    let object_ser = ffi::reserialize_tree4(object_deser);
                    object_ser.serialize_to_array(ctx.as_mut_slice());
                }

                TreeDepth::Five => {
                    let object_deser =
                        ffi::deserialize_tree5_cereal_from_array(recved_message.index(0).as_ref());
                    let object_ser = ffi::reserialize_tree5(object_deser);
                    object_ser.serialize_to_array(ctx.as_mut_slice());
                }
            },
        }
        cf.add_entry(CornPtr::Normal(ctx));
        Ok(cf)
    }

    fn new_context(&self) -> Self::Ctx {
        vec![0u8; self.context_size]
    }
}

pub struct CerealEchoClient<'registered, 'normal> {
    message_type: SimpleMessageType,
    payload_ptrs: Vec<(*const u8, usize)>,
    sga: Cornflake<'registered, 'normal>,
}

impl<'registered, 'normal, D> CerealizeClient<'normal, D> for CerealEchoClient<'registered, 'normal>
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
        Ok(CerealEchoClient {
            message_type: message_type,
            payload_ptrs: payload_ptrs,
            sga: sga,
        })
    }

    fn init(&mut self, ctx: &'normal mut Self::Ctx) -> Result<()> {
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
                let single_cereal = ffi::new_single_cereal();
                single_cereal.set_data(&payloads[0]);
                single_cereal.serialize_to_array(ctx.as_mut_slice())
            }
            SimpleMessageType::List(list_size) => {
                assert!(payloads.len() == list_size);
                let list_cereal = ffi::new_list_cereal();
                for payload in payloads.iter() {
                    list_cereal.append(payload);
                }
                list_cereal.serialize_to_array(ctx.as_mut_slice())
            }
            SimpleMessageType::Tree(depth) => match depth {
                TreeDepth::One => {
                    assert!(payloads.len() == 2);
                    let tree1_cereal = get_tree1_message(&[0, 1], &payloads);
                    tree1_cereal.serialize_to_array(ctx.as_mut_slice())
                }
                TreeDepth::Two => {
                    assert!(payloads.len() == 4);
                    let tree2_cereal = get_tree2_message(&[0, 1, 2, 3], &payloads);
                    tree2_cereal.serialize_to_array(ctx.as_mut_slice())
                }
                TreeDepth::Three => {
                    let indices: Vec<usize> = (0usize..8usize).collect();
                    let tree3_cereal = get_tree3_message(&indices, &payloads);
                    tree3_cereal.serialize_to_array(ctx.as_mut_slice())
                }
                TreeDepth::Four => {
                    let indices: Vec<usize> = (0usize..16usize).collect();
                    let tree4_cereal = get_tree4_message(&indices, &payloads);
                    tree4_cereal.serialize_to_array(ctx.as_mut_slice())
                }
                TreeDepth::Five => {
                    let indices: Vec<usize> = (0usize..32usize).collect();
                    let tree5_cereal = get_tree5_message(&indices, &payloads);
                    tree5_cereal.serialize_to_array(ctx.as_mut_slice())
                }
            },
        }
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
        let payloads: Vec<&[u8]> = self
            .payload_ptrs
            .clone()
            .iter()
            .map(|(ptr, size)| unsafe { slice::from_raw_parts(*ptr, *size) })
            .collect();
        match self.message_type {
            SimpleMessageType::Single => {
                let object_deser =
                    ffi::deserialize_single_cereal_from_array(recved_msg.index(0).as_ref());
                let bytes_vec = object_deser.get_data().as_bytes().to_vec();
                assert!(bytes_vec.len() == payloads[0].len());
                assert!(bytes_vec == payloads[0].to_vec());
            }
            SimpleMessageType::List(_list_size) => {
                let object_deser =
                    ffi::deserialize_list_cereal_from_array(recved_msg.index(0).as_ref());
                for (i, payload) in payloads.iter().enumerate() {
                    let bytes_vec = object_deser.get(i).as_bytes().to_vec();
                    assert!(bytes_vec == payload.to_vec());
                }
            }
            SimpleMessageType::Tree(depth) => match depth {
                TreeDepth::One => {
                    let our_tree1_cereal = get_tree1_message(&[0, 1], &payloads);
                    let object_deser =
                        ffi::deserialize_tree1_cereal_from_array(recved_msg.index(0).as_ref());
                    assert!(our_tree1_cereal.equals(object_deser));
                }
                TreeDepth::Two => {
                    let indices: Vec<usize> = (0usize..4usize).collect();
                    let ours = get_tree2_message(&indices, &payloads);
                    let object_deser =
                        ffi::deserialize_tree2_cereal_from_array(recved_msg.index(0).as_ref());
                    assert!(ours.equals(object_deser));
                }
                TreeDepth::Three => {
                    let indices: Vec<usize> = (0usize..8usize).collect();
                    let ours = get_tree3_message(&indices, &payloads);
                    let object_deser =
                        ffi::deserialize_tree3_cereal_from_array(recved_msg.index(0).as_ref());
                    assert!(ours.equals(object_deser));
                }
                TreeDepth::Four => {
                    let indices: Vec<usize> = (0usize..16usize).collect();
                    let ours = get_tree4_message(&indices, &payloads);
                    let object_deser =
                        ffi::deserialize_tree4_cereal_from_array(recved_msg.index(0).as_ref());
                    assert!(ours.equals(object_deser));
                }
                TreeDepth::Five => {
                    let indices: Vec<usize> = (0usize..32usize).collect();
                    let ours = get_tree5_message(&indices, &payloads);
                    let object_deser =
                        ffi::deserialize_tree5_cereal_from_array(recved_msg.index(0).as_ref());
                    assert!(ours.equals(object_deser));
                }
            },
        }
        Ok(())
    }

    fn new_context(&self) -> Self::Ctx {
        let total_size: usize = self.payload_ptrs.iter().map(|(_, size)| *size).sum();
        let ctx_size = context_size(self.message_type, total_size);
        tracing::debug!(msg=?self.message_type, total_size=total_size, ctx_size=ctx_size,"Trying to find context size in client");
        vec![0u8; ctx_size]
    }
}
