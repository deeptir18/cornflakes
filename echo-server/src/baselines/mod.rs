use super::{init_payloads, CerealizeClient, CerealizeMessage};
use color_eyre::eyre::Result;
use cornflakes_libos::{
    mem::MmapMetadata, Datapath, RcCornPtr, RcCornflake, ReceivedPkt, ScatterGather,
};
use cornflakes_utils::SimpleMessageType;
use dpdk_datapath::dpdk_bindings::rte_memcpy_wrapper as rte_memcpy;
use std::slice;

pub struct IdealSerializer {
    message_type: SimpleMessageType,
}

impl IdealSerializer {
    pub fn new(message_type: SimpleMessageType, _size: usize) -> IdealSerializer {
        IdealSerializer {
            message_type: message_type,
        }
    }
}

impl<D> CerealizeMessage<D> for IdealSerializer
where
    D: Datapath,
{
    type Ctx = ();

    fn new_context(&self) -> Self::Ctx {
        ()
    }

    fn message_type(&self) -> SimpleMessageType {
        self.message_type
    }

    fn process_header<'registered>(
        &self,
        _ctx: &'registered Self::Ctx,
        _cornflake: &mut RcCornflake<'registered, D>,
    ) -> Result<()> {
        Ok(())
    }

    fn process_msg<'registered>(
        &self,
        _recved_msg: &'registered ReceivedPkt<D>,
        _conn: &mut D,
    ) -> Result<(Self::Ctx, RcCornflake<'registered, D>)> {
        Ok(((), RcCornflake::default()))
    }
}

pub struct OneCopySerializer {
    message_type: SimpleMessageType,
    chunk_size: usize,
}

impl OneCopySerializer {
    pub fn new(message_type: SimpleMessageType, size: usize) -> OneCopySerializer {
        let chunk_size = match message_type {
            SimpleMessageType::List(list_elts) => size / list_elts,
            _ => {
                unimplemented!();
            }
        };
        OneCopySerializer {
            message_type: message_type,
            chunk_size: chunk_size,
        }
    }
}

impl<D> CerealizeMessage<D> for OneCopySerializer
where
    D: Datapath,
{
    type Ctx = ();

    fn new_context(&self) -> Self::Ctx {
        ()
    }

    fn message_type(&self) -> SimpleMessageType {
        self.message_type
    }

    fn process_header<'registered>(
        &self,
        _ctx: &'registered Self::Ctx,
        _cornflake: &mut RcCornflake<'registered, D>,
    ) -> Result<()> {
        Ok(())
    }

    fn process_msg<'registered>(
        &self,
        recved_msg: &'registered ReceivedPkt<D>,
        _conn: &mut D,
    ) -> Result<(Self::Ctx, RcCornflake<'registered, D>)> {
        match self.message_type {
            SimpleMessageType::List(list_elts) => {
                let mut rc_cornflake = RcCornflake::with_capacity(list_elts);
                for i in 0..list_elts {
                    let contiguous_slice =
                        recved_msg.contiguous_slice(i * self.chunk_size, self.chunk_size)?;
                    rc_cornflake.add_entry(RcCornPtr::RawRef(contiguous_slice));
                }
                Ok(((), rc_cornflake))
            }
            _ => {
                unimplemented!();
            }
        }
    }
}

pub struct TwoCopySerializer {
    message_type: SimpleMessageType,
    context_size: usize,
    chunk_size: usize,
}

impl TwoCopySerializer {
    pub fn new(message_type: SimpleMessageType, size: usize) -> TwoCopySerializer {
        let chunk_size = match message_type {
            SimpleMessageType::List(list_elts) => size / list_elts,
            _ => {
                unimplemented!();
            }
        };
        TwoCopySerializer {
            message_type: message_type,
            context_size: size,
            chunk_size: chunk_size,
        }
    }
}

impl<D> CerealizeMessage<D> for TwoCopySerializer
where
    D: Datapath,
{
    type Ctx = Vec<u8>;

    fn new_context(&self) -> Self::Ctx {
        unimplemented!();
    }

    fn message_type(&self) -> SimpleMessageType {
        self.message_type
    }

    fn process_header<'registered>(
        &self,
        ctx: &'registered Self::Ctx,
        cornflake: &mut RcCornflake<'registered, D>,
    ) -> Result<()> {
        cornflake.add_entry(RcCornPtr::RawRef(ctx.as_slice()));
        Ok(())
    }

    fn process_msg<'registered>(
        &self,
        recved_msg: &'registered ReceivedPkt<D>,
        _conn: &mut D,
    ) -> Result<(Self::Ctx, RcCornflake<'registered, D>)> {
        match self.message_type {
            SimpleMessageType::List(list_elts) => {
                tracing::debug!("Processing list {} twocopy", list_elts);
                let mut ctx: Vec<u8> = Vec::with_capacity(self.context_size);
                let rc_cornflake = RcCornflake::<D>::with_capacity(1);
                for i in 0..list_elts {
                    let contiguous_slice =
                        recved_msg.contiguous_slice(i * self.chunk_size, self.chunk_size)?;
                    tracing::debug!("Copying {} at off {}", self.chunk_size, i * self.chunk_size);
                    ctx.extend_from_slice(contiguous_slice);
                    /*unsafe {
                        rte_memcpy(
                            ctx.as_mut()
                                .as_mut_ptr()
                                .offset((i * self.chunk_size) as isize)
                                as _,
                            contiguous_slice.as_ptr() as _,
                            self.chunk_size,
                        );
                    }*/
                }
                Ok((ctx, rc_cornflake))
            }
            _ => Ok((Vec::default(), RcCornflake::default())),
        }
    }
}

pub struct BaselineClient<'registered, D>
where
    D: Datapath,
{
    message_type: SimpleMessageType,
    payload_ptrs: Vec<(*const u8, usize)>,
    sga: RcCornflake<'registered, D>,
    total_size: usize,
    chunk_size: usize,
}

impl<'registered, D> CerealizeClient<'registered, D> for BaselineClient<'registered, D>
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
        let sga = RcCornflake::default();
        let total_size: usize = payload_ptrs.iter().map(|(_, size)| *size).sum();
        Ok(BaselineClient {
            message_type: message_type,
            payload_ptrs: payload_ptrs,
            sga: sga,
            total_size: total_size,
            // assumes field sizes are equal
            chunk_size: field_sizes[0],
        })
    }

    fn init(&mut self, ctx: &'registered mut Self::Ctx) -> Result<()> {
        // initialize the scatter-gather array
        let payloads: Vec<&[u8]> = self
            .payload_ptrs
            .clone()
            .iter()
            .map(|(ptr, size)| unsafe { slice::from_raw_parts(*ptr, *size) })
            .collect();

        match self.message_type {
            SimpleMessageType::List(list_size) => {
                assert!(payloads.len() == list_size);
                for i in 0..list_size {
                    unsafe {
                        rte_memcpy(
                            (ctx.as_mut_slice().as_mut_ptr()).offset((self.chunk_size * i) as isize)
                                as _,
                            payloads[i].as_ptr() as _,
                            self.chunk_size,
                        );
                    }
                }
            }
            _ => {
                unimplemented!();
            }
        }
        self.sga.add_entry(RcCornPtr::RawRef(ctx.as_slice()));
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
        for (i, (ptr, len)) in self.payload_ptrs.iter().enumerate() {
            let slice = recved_msg.contiguous_slice(i * self.chunk_size, self.chunk_size)?;
            let payload = unsafe { slice::from_raw_parts(*ptr, *len) };
            assert!(slice.to_vec() == payload.to_vec());
        }
        Ok(())
    }

    fn new_context(&self) -> Self::Ctx {
        vec![0u8; self.total_size]
    }
}
