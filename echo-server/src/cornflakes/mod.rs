use super::{init_payloads, CerealizeClient, CerealizeMessage};
use color_eyre::eyre::Result;
use cornflakes_libos::{mem::MmapMetadata, CornPtr, Cornflake, Datapath};
use cornflakes_utils::SimpleMessageType;
use memmap::MmapMut;
use std::slice;
pub mod cf_utils;
pub mod serialize_test;

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
    //payloads: Vec<&'a [u8]>,
    sga: Cornflake<'registered, 'normal>,
}

impl<'registered, 'normal, D> CerealizeClient<D> for CornflakesEchoClient<'registered, 'normal>
where
    D: Datapath,
{
    type Ctx = ();
    type OutgoingMsg = Cornflake<'registered, 'normal>;

    fn new(
        message_type: SimpleMessageType,
        field_sizes: Vec<usize>,
        mmap_metadata: MmapMetadata,
        mmap_mut: &mut MmapMut,
    ) -> Result<Self> {
        let payload_ptrs = init_payloads(&field_sizes, &mmap_metadata, mmap_mut)?;
        let payloads: Vec<&[u8]> = payload_ptrs
            .clone()
            .iter()
            .map(|(ptr, size)| unsafe { slice::from_raw_parts(*ptr, *size) })
            .collect();

        // TODO: actually initialize the cornflake for real
        let mut sga = Cornflake::default();
        for payload in payloads.iter() {
            sga.add_entry(CornPtr::Registered(payload));
        }

        Ok(CornflakesEchoClient {
            message_type: message_type,
            payload_ptrs: payload_ptrs,
            //payloads: payloads,
            sga: sga,
        })
    }

    fn message_type(&self) -> SimpleMessageType {
        self.message_type
    }

    fn payload_sizes(&self) -> Vec<usize> {
        self.payload_ptrs.iter().map(|(_ptr, len)| *len).collect()
    }

    fn get_sga(&self, _ctx: &mut Self::Ctx) -> Result<Self::OutgoingMsg> {
        Ok(self.sga.clone())
    }

    fn check_echoed_payload(&self, _recved_msg: &D::ReceivedPkt) -> Result<()> {
        // check that the payload follows cornflakes serialization format.
        // Only used for debug mode.
        Ok(())
    }

    fn new_context(&self) -> Self::Ctx {
        ()
    }
}
