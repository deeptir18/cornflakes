pub mod cf_testobject1;
pub mod cf_testobject2;
pub mod cf_utils1;
pub mod cf_utils2;
use super::{
    get_equal_fields, init_payloads, init_payloads_as_vec, CerealizeClient, CerealizeMessage,
};
use cf_testobject1::*;
use cf_utils1::*;
use color_eyre::eyre::Result;
use cornflakes_libos::{mem::MmapMetadata, Cornflake, Datapath, ReceivedPacket};
use cornflakes_utils::SimpleMessageType;
use memmap::MmapMut;
use std::slice;

pub struct CornflakesSerializer {
    message_type: SimpleMessageType,
    context_size: usize,
}

impl CornflakesSerializer {
    pub fn new(message_type: SimpleMessageType, size: usize) -> CornflakesSerializer {
        // calculate size given message type and total message size
        let payloads = init_payloads_as_vec(&get_equal_fields(message_type, size));
        let context_size = match message_type {
            SimpleMessageType::Single => {
                assert!(payloads.len() == 1);
                let mut test_object = TestObject::new();
                test_object.set_string_field(CFString::new_from_bytes(&payloads[0]));
                test_object.init_header_buffer().len()
            }
            SimpleMessageType::List(list_size) => {
                assert!(payloads.len() == list_size);
                let mut test_object = TestObject::new();
                test_object.init_bytes_list_field(list_size);
                let bytes_list_field = test_object.get_mut_bytes_list_field();
                for payload in payloads.iter() {
                    bytes_list_field.append(CFBytes::new(payload.as_slice()));
                }
                test_object.init_header_buffer().len()
            }
            SimpleMessageType::Tree(depth) => {
                // TODO: Implement the correct logic here for tree
                depth
            }
        };
        CornflakesSerializer {
            message_type: message_type,
            context_size: context_size,
        }
    }
}
impl<D> CerealizeMessage<D> for CornflakesSerializer
where
    D: Datapath,
{
    type Ctx = Vec<u8>;

    fn message_type(&self) -> SimpleMessageType {
        self.message_type
    }

    fn process_msg<'registered, 'normal: 'registered>(
        &self,
        recved_msg: &'registered D::ReceivedPkt,
        ctx: &'normal mut Self::Ctx,
    ) -> Result<Cornflake<'registered, 'normal>> {
        match self.message_type {
            SimpleMessageType::Single => {
                let mut test_object_deser = TestObject::new();
                test_object_deser.deserialize(recved_msg.get_pkt_buffer());
                assert!(test_object_deser.has_string_field());
                let mut test_object_ser = TestObject::new();
                test_object_ser.set_string_field(test_object_deser.get_string_field());
                Ok(test_object_ser.serialize(ctx, copy_func))
            }
            SimpleMessageType::List(_list_elts) => {
                let mut test_object_deser = TestObject::new();
                test_object_deser.deserialize(recved_msg.get_pkt_buffer());
                assert!(test_object_deser.has_bytes_list_field());
                let mut test_object_ser = TestObject::new();
                test_object_ser
                    .init_bytes_list_field(test_object_deser.get_bytes_list_field().len());
                let bytes_list_field_deser = test_object_deser.get_bytes_list_field();
                let bytes_list_field_ser = test_object_ser.get_mut_bytes_list_field();
                for i in 0..bytes_list_field_deser.len() {
                    bytes_list_field_ser.append(bytes_list_field_deser[i]);
                }
                Ok(test_object_ser.serialize(ctx, copy_func))
            }
            SimpleMessageType::Tree(_depth) => Ok(Cornflake::default()),
        }
        // TODO: deserialize the given buffer and spit it back out in the cornflake
    }

    fn new_context(&self) -> Self::Ctx {
        vec![0u8; self.context_size]
    }
}

pub struct CornflakesEchoClient<'registered, 'normal> {
    message_type: SimpleMessageType,
    payload_ptrs: Vec<(*const u8, usize)>,
    sga: Cornflake<'registered, 'normal>,
}

impl<'registered, 'normal, D> CerealizeClient<'normal, D>
    for CornflakesEchoClient<'registered, 'normal>
where
    D: Datapath,
{
    type Ctx = Vec<u8>;
    type OutgoingMsg = Cornflake<'registered, 'normal>;

    fn new(
        message_type: SimpleMessageType,
        field_sizes: Vec<usize>,
        mmap_metadata: MmapMetadata,
        mmap_mut: &mut MmapMut,
    ) -> Result<Self> {
        let payload_ptrs = init_payloads(&field_sizes, &mmap_metadata, mmap_mut)?;
        let sga = Cornflake::default();
        Ok(CornflakesEchoClient {
            message_type: message_type,
            payload_ptrs: payload_ptrs,
            sga: sga,
        })
    }

    fn init(&mut self, ctx: &'normal mut Self::Ctx) {
        // initialize the scatter-gather array
        let payloads: Vec<&[u8]> = self
            .payload_ptrs
            .clone()
            .iter()
            .map(|(ptr, size)| unsafe { slice::from_raw_parts(*ptr, *size) })
            .collect();

        // TODO: based on the message type
        match self.message_type {
            SimpleMessageType::Single => {
                let mut object = TestObject::default();
                object.set_string_field(CFString::new_from_bytes(payloads[0]));
                self.sga = object.serialize(ctx.as_mut_slice(), copy_func);
            }
            SimpleMessageType::List(_list_elts) => {
                let mut object = TestObject::default();
                object.init_bytes_list_field(payloads.len());
                let bytes_list_field = object.get_mut_bytes_list_field();
                for payload in payloads.iter() {
                    bytes_list_field.append(CFBytes::new(payload));
                }
                self.sga = object.serialize(ctx.as_mut_slice(), copy_func);
            }
            SimpleMessageType::Tree(_depth) => {
                unimplemented!();
            }
        }
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

    fn check_echoed_payload(&self, _recved_msg: &D::ReceivedPkt) -> Result<()> {
        // check that the payload follows cornflakes serialization format.
        // Only used for debug mode.
        Ok(())
    }

    fn new_context(&self) -> Self::Ctx {
        // initialize the scatter-gather array
        // TODO: is there a better way then `re-creating` the object here?
        let payloads: Vec<&[u8]> = self
            .payload_ptrs
            .clone()
            .iter()
            .map(|(ptr, size)| unsafe { slice::from_raw_parts(*ptr, *size) })
            .collect();

        let mut object = TestObject::default();
        object.init_bytes_list_field(payloads.len());
        let bytes_list_field = object.get_mut_bytes_list_field();
        for payload in payloads.iter() {
            bytes_list_field.append(CFBytes::new(payload));
        }
        object.init_header_buffer()
    }
}
