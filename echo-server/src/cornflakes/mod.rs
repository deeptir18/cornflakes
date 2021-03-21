pub mod cf_utils;
pub mod serialize_test;
use super::{init_payloads, CerealizeClient, CerealizeMessage};
use cf_utils::*;
use color_eyre::eyre::Result;
use cornflakes_libos::{mem::MmapMetadata, Cornflake, Datapath};
use cornflakes_utils::SimpleMessageType;
use memmap::MmapMut;
use serialize_test::*;
use std::slice;

pub struct CornflakesSerializer {
    message_type: SimpleMessageType,
}

impl CornflakesSerializer {
    pub fn new(message_type: SimpleMessageType) -> CornflakesSerializer {
        CornflakesSerializer {
            message_type: message_type,
        }
    }
}
impl<D> CerealizeMessage<D> for CornflakesSerializer
where
    D: Datapath,
{
    type Ctx = ();

    fn message_type(&self) -> SimpleMessageType {
        self.message_type
    }

    fn process_msg(&self, _recved_msg: &D::ReceivedPkt, _ctx: &mut Self::Ctx) -> Result<Cornflake> {
        let cf = Cornflake::default();
        // TODO: deserialize the given buffer and spit it back out in the cornflake
        Ok(cf)
    }

    fn new_context(&self) -> Self::Ctx {
        ()
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

        let mut object = TestObject::default();
        object.init_bytes_list_field(payloads.len());
        let bytes_list_field = object.get_mut_bytes_list_field();
        for payload in payloads.iter() {
            bytes_list_field.append(CFBytes::new(payload));
        }
        // serialize the cornflake
        self.sga = object.serialize(ctx.as_mut_slice(), copy_func);
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
