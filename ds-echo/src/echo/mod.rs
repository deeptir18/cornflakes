use super::{read_message_type, ClientCerealizeMessage, REQ_TYPE_SIZE};
use color_eyre::eyre::{bail, Result};
use cornflakes_libos::{
    datapath::{Datapath, DatapathBufferOps, MetadataOps, PushBufType, ReceivedPkt},
    state_machine::server::ServerSM,
};
use cornflakes_utils::SimpleMessageType;
use std::{io::Write, marker::PhantomData};

// Two-copy packet echo
pub struct TwoCopyEcho<D>
where
    D: Datapath,
{
    _phantom_data: PhantomData<D>,
}

impl<D> TwoCopyEcho<D>
where
    D: Datapath,
{
    pub fn new(_push_buf_type: PushBufType) -> Self {
        TwoCopyEcho {
            _phantom_data: PhantomData::default(),
        }
    }

    pub fn set_with_copy(&mut self) {}
}

impl<D> ServerSM for TwoCopyEcho<D>
where
    D: Datapath,
{
    type Datapath = D;
    fn push_buf_type(&self) -> PushBufType {
        PushBufType::Echo
    }

    #[inline]
    fn process_requests_echo(
        &mut self,
        pkts: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        datapath: &mut Self::Datapath,
    ) -> Result<()> {
        let pkts_len = pkts.len();
        for (i, pkt) in pkts.into_iter().enumerate() {
            let message_type = read_message_type(&pkt)?;
            let buf = &pkt.seg(0).as_ref()[REQ_TYPE_SIZE..];
            match message_type {
                SimpleMessageType::Single => {
                    let buf2 = buf.clone();
                    datapath.queue_single_buffer_with_copy(
                        (pkt.msg_id(), pkt.conn_id(), &buf2),
                        i == pkts_len - 1,
                    )?;
                }
                SimpleMessageType::List(list_elts) => {
                    let chunk_size = buf.len() / list_elts;
                    let mut buf2 = Vec::with_capacity(buf.len());
                    for idx in 0..list_elts {
                        let contiguous_slice = &buf[(chunk_size * idx)..(chunk_size * (idx + 1))];
                        buf2.extend_from_slice(contiguous_slice);
                    }
                    datapath.queue_single_buffer_with_copy(
                        (pkt.msg_id(), pkt.conn_id(), &buf2),
                        i == pkts_len - 1,
                    )?;
                }
                _ => {
                    unimplemented!();
                }
            }
        }
        Ok(())
    }
}

// One-copy packet echo
pub struct OneCopyEcho<D>
where
    D: Datapath,
{
    _phantom_data: PhantomData<D>,
}

impl<D> OneCopyEcho<D>
where
    D: Datapath,
{
    pub fn new(_push_buf_type: PushBufType) -> Self {
        OneCopyEcho {
            _phantom_data: PhantomData::default(),
        }
    }

    pub fn set_with_copy(&mut self) {}
}

impl<D> ServerSM for OneCopyEcho<D>
where
    D: Datapath,
{
    type Datapath = D;
    fn push_buf_type(&self) -> PushBufType {
        PushBufType::Echo
    }

    #[inline]
    fn process_requests_echo(
        &mut self,
        pkts: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        datapath: &mut Self::Datapath,
    ) -> Result<()> {
        let pkts_len = pkts.len();
        for (i, pkt) in pkts.into_iter().enumerate() {
            let message_type = read_message_type(&pkt)?;
            let buf = &pkt.seg(0).as_ref()[REQ_TYPE_SIZE..];
            // allocate a datapath buffer
            let mut datapath_buffer = match datapath.allocate_tx_buffer()?.0 {
                Some(buf) => buf,
                None => {
                    bail!("Failed to allocate tx buffer to respond to echo.");
                }
            };

            match message_type {
                SimpleMessageType::Single => {
                    let mut mutable_buffer = datapath_buffer
                        .get_mutable_slice(cornflakes_libos::utils::TOTAL_HEADER_SIZE, buf.len())?;
                    mutable_buffer.write(buf)?;
                    datapath_buffer.set_len(buf.len() + cornflakes_libos::utils::TOTAL_HEADER_SIZE);
                    datapath.queue_datapath_buffer(
                        pkt.msg_id(),
                        pkt.conn_id(),
                        datapath_buffer,
                        i == pkts_len - 1,
                    )?;
                }
                SimpleMessageType::List(size) => {
                    let chunk_size = buf.len() / size;
                    for idx in 0..size {
                        let mut mutable_buffer = datapath_buffer.get_mutable_slice(
                            cornflakes_libos::utils::TOTAL_HEADER_SIZE + chunk_size * idx,
                            chunk_size,
                        )?;
                        mutable_buffer.write(&buf[(chunk_size * idx)..(chunk_size * (idx + 1))])?;
                    }
                    datapath_buffer.set_len(buf.len() + cornflakes_libos::utils::TOTAL_HEADER_SIZE);
                    datapath.queue_datapath_buffer(
                        pkt.msg_id(),
                        pkt.conn_id(),
                        datapath_buffer,
                        i == pkts_len - 1,
                    )?;
                }
                _ => {
                    unimplemented!();
                }
            }
        }
        Ok(())
    }
}

// Manual Zero-copy packet echo
pub struct ManualZeroCopyEcho<D>
where
    D: Datapath,
{
    _phantom_data: PhantomData<D>,
}

impl<D> ManualZeroCopyEcho<D>
where
    D: Datapath,
{
    pub fn new(_push_buf_type: PushBufType) -> Self {
        ManualZeroCopyEcho {
            _phantom_data: PhantomData::default(),
        }
    }

    pub fn set_with_copy(&mut self) {}
}

impl<D> ServerSM for ManualZeroCopyEcho<D>
where
    D: Datapath,
{
    type Datapath = D;
    fn push_buf_type(&self) -> PushBufType {
        PushBufType::Echo
    }

    #[inline]
    fn process_requests_echo(
        &mut self,
        pkts: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        datapath: &mut Self::Datapath,
    ) -> Result<()> {
        let pkts_len = pkts.len();
        for (i, pkt) in pkts.into_iter().enumerate() {
            let message_type = read_message_type(&pkt)?;
            match message_type {
                SimpleMessageType::Single => {
                    // get a datapath metadata
                    let len = pkt.seg(0).as_ref().len() - REQ_TYPE_SIZE;
                    let mut metadata = pkt.seg(0).clone();
                    let original_offset = metadata.offset();
                    metadata.set_data_len_and_offset(len, original_offset + REQ_TYPE_SIZE)?;
                    datapath.queue_metadata_vec(
                        pkt.msg_id(),
                        pkt.conn_id(),
                        vec![metadata],
                        i == pkts_len - 1,
                    )?;
                }
                SimpleMessageType::List(size) => {
                    let len = pkt.seg(0).as_ref().len() - REQ_TYPE_SIZE;
                    let chunk_size = len / size;
                    let metadata_vec_res: Result<Vec<D::DatapathMetadata>> = (0..size)
                        .map(|idx| {
                            let mut metadata = pkt.seg(0).clone();
                            let original_offset = metadata.offset();

                            let new_offset = original_offset + REQ_TYPE_SIZE + idx * chunk_size;
                            metadata.set_data_len_and_offset(chunk_size, new_offset)?;
                            Ok(metadata)
                        })
                        .collect();
                    datapath.queue_metadata_vec(
                        pkt.msg_id(),
                        pkt.conn_id(),
                        metadata_vec_res?,
                        i == pkts_len - 1,
                    )?;
                }
                _ => {
                    unimplemented!();
                }
            }
        }
        Ok(())
    }
}

// Zero-copy packet echo
pub struct RawEcho<D>
where
    D: Datapath,
{
    _phantom_data: PhantomData<D>,
}

impl<D> RawEcho<D>
where
    D: Datapath,
{
    pub fn new(_push_buf_type: PushBufType) -> Self {
        RawEcho {
            _phantom_data: PhantomData::default(),
        }
    }

    pub fn set_with_copy(&mut self) {}
}

impl<D> ServerSM for RawEcho<D>
where
    D: Datapath,
{
    type Datapath = D;
    fn push_buf_type(&self) -> PushBufType {
        PushBufType::Echo
    }

    #[inline]
    fn process_requests_echo(
        &mut self,
        pkts: Vec<ReceivedPkt<<Self as ServerSM>::Datapath>>,
        datapath: &mut Self::Datapath,
    ) -> Result<()> {
        datapath.echo(pkts)?;
        Ok(())
    }
}

pub struct RawEchoClient {}
impl<D> ClientCerealizeMessage<D> for RawEchoClient
where
    D: Datapath,
{
    fn new() -> Self
    where
        Self: Sized,
    {
        RawEchoClient {}
    }

    fn check_echoed_payload(
        &self,
        pkt: &ReceivedPkt<D>,
        bytes_to_check: (SimpleMessageType, &Vec<Vec<u8>>),
        _datapath: &D,
    ) -> Result<bool> {
        let (_ty, input) = bytes_to_check;
        let total_bytes = input.clone().into_iter().flatten().collect::<Vec<u8>>();
        let received_bytes = pkt.seg(0).as_ref()[REQ_TYPE_SIZE..].to_vec();
        if received_bytes != total_bytes {
            return Ok(false);
        }
        return Ok(true);
    }

    fn get_serialized_bytes(
        _ty: SimpleMessageType,
        input: &Vec<Vec<u8>>,
        _datapath: &D,
    ) -> Result<Vec<u8>> {
        let total_bytes = input.clone().into_iter().flatten().collect::<Vec<u8>>();
        Ok(total_bytes)
    }
}
