pub mod run_datapath;
use bytes::{BufMut, Bytes, BytesMut};
use color_eyre::eyre::{ensure, Result};
use cornflakes_libos::{
    datapath::{Datapath, ReceivedPkt},
    state_machine::client::ClientSM,
    timing::ManualHistogram,
    utils::AddressInfo,
    MsgID,
};
use std::marker::PhantomData;
const DEFAULT_CHAR: u8 = 'c' as u8;
const RESPONSE_DATA_OFF: usize = 12;
const REQUEST_SEGLIST_OFFSET_PADDING_SIZE: usize = 4;
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SgBenchClient<D>
where
    D: Datapath,
{
    /// How server initializes memory on other sizes (for checking)
    server_payload_regions: Vec<Bytes>,
    /// requests: actual bytes to send
    requests: Vec<(Bytes, Vec<usize>)>,
    /// Echo mode on server
    echo_mode: bool,
    /// number of mbufs
    num_mbufs: usize,
    /// segment size
    segment_size: usize,
    /// packets received so far
    received: usize,
    /// packets retried so far
    num_retried: usize,
    /// packets timed out so far
    num_timed_out: usize,
    /// last sent
    last_sent_id: usize,
    /// server address
    server_addr: AddressInfo,
    /// round trips
    rtts: ManualHistogram,
    /// phantom data
    _datapath: PhantomData<D>,
}

fn get_starting_position(
    client_id: usize,
    thread_id: usize,
    total_clients: usize,
    total_threads: usize,
    num_regions: usize,
) -> Result<usize> {
    // divide regions into client_id * thread_id groups
    // if there are 4 clients and 8 threads each: 32 starting positions
    let total_client_threads = total_clients * total_threads;
    let this_position = client_id * total_threads + thread_id;
    let region_size = num_regions / total_client_threads;
    return Ok(this_position * region_size);
}

impl<D> SgBenchClient<D>
where
    D: Datapath,
{
    pub fn new(
        server_addr: AddressInfo,
        segment_size: usize,
        echo_mode: bool,
        num_segments: usize,
        array_size: usize,
        send_packet_size: usize,
        max_num_requests: usize,
        thread_id: usize,
        client_id: usize,
        total_threads: usize,
        total_clients: usize,
        region_order: Vec<usize>,
    ) -> Result<Self> {
        let mut server_payload_regions: Vec<Bytes> =
            vec![Bytes::default(); array_size / segment_size];
        let alphabet = [
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q',
            'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
        ];
        for seg in 0..(array_size / segment_size) {
            let letter = alphabet[seg % alphabet.len()];
            let chars: Vec<u8> = std::iter::repeat(letter as u8).take(segment_size).collect();
            let bytes = Bytes::copy_from_slice(chars.as_slice());
            server_payload_regions[seg] = bytes;
        }
        // store bytes in each region for checking
        let mut requests: Vec<(Bytes, Vec<usize>)> = Vec::with_capacity(max_num_requests);

        // 2) based on this thread's thread id and client id, get "starting position" into the
        //    list
        let starting_offset = get_starting_position(
            client_id,
            thread_id,
            total_clients,
            total_threads,
            array_size / segment_size,
        )?;
        let mut cur_region_idx = region_order[0];
        for _ in 0..starting_offset {
            cur_region_idx = region_order[cur_region_idx];
        }

        // (3) with starting position, num segments, construct the packet
        let min_send_size = REQUEST_SEGLIST_OFFSET_PADDING_SIZE + 8 * num_segments;
        ensure!(
            send_packet_size >= min_send_size,
            "Provided send packet size must be atleast segment length"
        );
        let padding: Vec<u8> = match send_packet_size > min_send_size {
            true => std::iter::repeat(0u8)
                .take(send_packet_size - min_send_size)
                .collect(),
            false => Vec::default(),
        };
        for _ in 0..max_num_requests {
            if send_packet_size != 0 {}
            let mut bytes = match send_packet_size <= min_send_size {
                true => BytesMut::with_capacity(min_send_size),
                false => BytesMut::with_capacity(send_packet_size),
            };
            // write four bytes of 0 as the padding
            for _i in 0..4 {
                bytes.put_u8(0);
            }
            let mut segment_indices = Vec::with_capacity(num_segments);
            for _ in 0..num_segments {
                cur_region_idx = region_order[cur_region_idx];
                bytes.put_u64_le(cur_region_idx as u64);
                segment_indices.push(cur_region_idx);
            }
            bytes.put(padding.as_slice());
            requests.push((bytes.freeze(), segment_indices));
        }

        Ok(SgBenchClient {
            server_payload_regions: server_payload_regions,
            requests: requests,
            num_mbufs: num_segments,
            segment_size: segment_size,
            echo_mode: echo_mode,
            received: 0,
            num_retried: 0,
            num_timed_out: 0,
            last_sent_id: 0,
            server_addr: server_addr,
            rtts: ManualHistogram::new(max_num_requests),
            _datapath: PhantomData::default(),
        })
    }
}

impl<D> ClientSM for SgBenchClient<D>
where
    D: Datapath,
{
    type Datapath = D;

    fn increment_uniq_received(&mut self) {
        self.received += 1;
    }

    fn increment_uniq_sent(&mut self) {
        self.last_sent_id += 1;
    }

    fn increment_num_timed_out(&mut self) {
        self.num_timed_out += 1;
    }

    fn increment_num_retried(&mut self) {
        self.num_retried += 1;
    }

    fn uniq_sent_so_far(&self) -> usize {
        self.last_sent_id as usize
    }

    fn uniq_received_so_far(&self) -> usize {
        self.received
    }

    fn num_retried(&self) -> usize {
        self.num_retried
    }

    fn num_timed_out(&self) -> usize {
        self.num_timed_out
    }

    fn get_mut_rtts(&mut self) -> &mut ManualHistogram {
        &mut self.rtts
    }

    fn server_addr(&self) -> AddressInfo {
        self.server_addr.clone()
    }

    fn get_next_msg(
        &mut self,
        _datapath: &<Self as ClientSM>::Datapath,
    ) -> Result<Option<(MsgID, &[u8])>> {
        let id = self.last_sent_id;
        ensure!(
            (id as usize) < self.requests.len(),
            format!("Requests array doesn't have msg id # {}", id)
        );
        tracing::debug!("Seg sequence: {:?}", &self.requests[id as usize].1);
        let bytes_to_send = &self.requests[id as usize].0;
        Ok(Some((id as u32, bytes_to_send)))
    }

    fn process_received_msg(
        &mut self,
        sga: ReceivedPkt<<Self as ClientSM>::Datapath>,
        _datapath: &<Self as ClientSM>::Datapath,
    ) -> Result<bool> {
        if cfg!(debug_assertions) {
            let expected_size = self.num_mbufs * self.segment_size + RESPONSE_DATA_OFF;
            ensure!(
                (sga.data_len() == expected_size),
                format!(
                    "Received sga id {} has length {}, expected {}",
                    sga.msg_id(),
                    sga.data_len(),
                    expected_size,
                )
            );
            match self.echo_mode {
                true => {
                    let chars: Vec<u8> = std::iter::repeat(DEFAULT_CHAR)
                        .take(expected_size - RESPONSE_DATA_OFF)
                        .collect();
                    let msg_to_check = &sga.seg(0).as_ref()[RESPONSE_DATA_OFF..];
                    ensure!(
                        chars.as_slice() == msg_to_check,
                        format!("Expected {:?}, got {:?}", chars.as_slice(), msg_to_check)
                    );
                    return Ok(true);
                }
                false => {
                    let seg_sequence = &self.requests[sga.msg_id() as usize].1;
                    for (idx, seg) in seg_sequence.iter().enumerate() {
                        let msg_to_check = &sga.seg(0).as_ref()[(RESPONSE_DATA_OFF
                            + idx * self.segment_size)
                            ..(RESPONSE_DATA_OFF + (idx + 1) * self.segment_size)];
                        let expected = &self.server_payload_regions[*seg];
                        ensure!(
                            msg_to_check == expected,
                            format!("Expected {:?}, got {:?}", expected, msg_to_check)
                        );
                    }
                }
            }
        }
        return Ok(true);
    }

    fn init(&mut self, _connection: &mut Self::Datapath) -> Result<()> {
        Ok(())
    }

    fn cleanup(&mut self, _connection: &mut Self::Datapath) -> Result<()> {
        Ok(())
    }

    fn msg_timeout_cb(&mut self, id: MsgID, _datapath: &Self::Datapath) -> Result<&[u8]> {
        ensure!(
            (id as usize) < self.requests.len(),
            format!("Requests array doesn't have msg id # {}", id)
        );
        let bytes_to_send = &self.requests[id as usize].0;
        Ok(bytes_to_send)
    }
}
