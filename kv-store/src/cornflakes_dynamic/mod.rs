use color_eyre::eyre::{bail, Result, WrapErr};

pub mod kv_messages {
    include!(concat!(env!("OUT_DIR"), "/kv_cf_dynamic.rs"));
}

use super::{ycsb_parser::YCSBRequest, KVSerializer, SerializedRequestGenerator};
use cornflakes_libos::{
    dpdk_bindings::rte_memcpy_wrapper as rte_memcpy, Cornflake, ReceivedPacket, ScatterGather,
};

// empty object
pub struct CornflakesDynamicSerializer;

impl KVSerializer<D> for CornflakesDynamicSerializer
where
    D: Datapath,
{
    type HeaderCtx = Vec<u8>;

    fn new(_serialize_to_native_buffers: bool) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(CornflakesDynamicSerializer {})
    }

    fn handle_get(
        &self,
        pkt: D::ReceivedPkt,
        map: &HashMap<String, CfBuf<D>>,
        num_values: usize,
    ) -> Result<(Self::HeaderCtx, Cornflake)> {
        match num_values {
            0 => {
                bail!("Number of get values cannot be 0");
            }
            1 => {
                let mut get_request = kv_messages: GetReq::new();
                get_request.deserialize(&pkt.get_pkt_buffer());

                let mut response = kv_messages::GetResp::new();
                response.set_id(get_request.get_id());
            }
            x => {}
        }

        Ok((Vec::default(), Cornflake::default()))
    }

    fn handle_put(
        &self,
        pkt: D::ReceivedPkt,
        map: &mut HashMap<String, CfBuf<D>>,
        num_values: usize,
    ) -> Result<(Self::HeaderCtx, Cornflake)> {
        // for the "copy-out" deserialization/insert:
        //      - just need to allocate the buffer from the networking stack
        //      - and copy the get_value() value into this buffer
        // for ref counting deserialization / insert: where we insert reference to the entire
        // packet
        //      - serialization should be ref counting aware
        //      - getter: needs to generate an CfBuf<D> to insert into kv store -- but how????
        //      - get_value() -> needs to return a buffer that *references* a datapath packet
        //      - the deserialized value should keep a reference to the buffer???
        //      - but also to the datapath packet???
        //      - for the split receive: the received thing could *also* be a scatter-gather array
        //      e.g. GetResponse<D> where D: Datapath {
        //          buf: CfBuf<D>,
        //          has_header_ptr: bool,
        //          bitmap: Vec<u8>,
        //          id: u32,
        //          bytes_field:
        //      }
        //      but then how is an individual element in that struct also a CfBuf???? not clear
        //
        Ok((Vec::default(), Cornflake::default()))
    }
}
