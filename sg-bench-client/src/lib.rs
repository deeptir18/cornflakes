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
pub static mut REGION_ORDER: &[(usize, usize)] = &[];
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
    /// Only initialized in debug mode.
    server_payload_regions: Vec<Bytes>,
    /// request segments to check
    /// Only initialized in debug mode.
    request_indices: Vec<Vec<usize>>,
    /// number of regions
    num_regions: usize,
    /// Echo mode on server
    echo_mode: bool,
    /// number of mbufs
    num_segments: usize,
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
    /// Send packet size
    send_packet_size: usize,
    /// Min send size
    min_send_size: usize,
    /// cur region idx
    cur_region_idx: usize,
    /// Request padding:
    padding: Vec<u8>,
    /// Last sent bytes
    last_sent_bytes: Bytes,
}

fn get_starting_position(
    client_id: usize,
    thread_id: usize,
    total_clients: usize,
    total_threads: usize,
    num_regions: usize,
    num_refcnt_arrays: usize,
) -> Result<usize> {
    // divide regions into client_id * thread_id groups
    // if there are 4 clients and 8 threads each: 32 starting positions
    let total_client_threads = total_clients * total_threads;
    let this_position = client_id * total_threads + thread_id;
    return Ok(this_position * (num_refcnt_arrays / total_client_threads)
        + this_position * (num_regions / total_client_threads));
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
        num_refcnt_arrays: usize,
    ) -> Result<Self> {
        let num_regions = array_size / segment_size;
        let server_payload_regions: Vec<Bytes> = match cfg!(debug_assertions) {
            true => {
                let mut server_payload_regions = vec![Bytes::default(); array_size / segment_size];
                let alphabet = [
                    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p',
                    'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
                ];
                for physical_seg in 0..(array_size / segment_size) {
                    let letter = alphabet[physical_seg % alphabet.len()];
                    let chars: Vec<u8> =
                        std::iter::repeat(letter as u8).take(segment_size).collect();
                    let bytes = Bytes::copy_from_slice(chars.as_slice());
                    server_payload_regions[physical_seg] = bytes;
                }
                server_payload_regions
            }
            false => Vec::default(),
        };

        // store the segment array if debug mode
        let mut request_indices: Vec<Vec<usize>> = Vec::default();

        // 2) based on this thread's thread id and client id, get "starting position" into the array
        let starting_offset = get_starting_position(
            client_id,
            thread_id,
            total_clients,
            total_threads,
            array_size / segment_size,
            num_refcnt_arrays,
        )?;
        let cur_region_idx = starting_offset;
        let mut cur_region_idx_local = starting_offset;

        // (3) with starting position, num segments, construct the packet
        let min_send_size = REQUEST_SEGLIST_OFFSET_PADDING_SIZE + 8 * num_segments;
        ensure!(
            send_packet_size >= min_send_size,
            format!("Provided send packet size must be atleast segment length: provided {}, expected atleast {}",
            send_packet_size, min_send_size)
        );
        let padding: Vec<u8> = match send_packet_size > min_send_size {
            true => std::iter::repeat(0u8)
                .take(send_packet_size - min_send_size)
                .collect(),
            false => Vec::default(),
        };
        let num_virtual_segments = unsafe { REGION_ORDER.len() };
        for _ in 0..max_num_requests {
            if cfg!(debug_assertions) {
                let mut segment_indices = Vec::with_capacity(num_segments);
                for _ in 0..num_segments {
                    let (phys_region, refcnt_multiplier) =
                        unsafe { REGION_ORDER[cur_region_idx_local] };
                    let virtual_index = num_regions * refcnt_multiplier + phys_region;
                    segment_indices.push(virtual_index);
                    cur_region_idx_local += 1;
                    if cur_region_idx_local == num_virtual_segments {
                        cur_region_idx_local = 0;
                    }
                }
                request_indices.push(segment_indices);
            }
        }

        Ok(SgBenchClient {
            server_payload_regions,
            request_indices,
            num_regions,
            num_segments,
            segment_size,
            echo_mode,
            received: 0,
            num_retried: 0,
            num_timed_out: 0,
            last_sent_id: 0,
            server_addr,
            rtts: ManualHistogram::new(max_num_requests),
            _datapath: PhantomData::default(),
            send_packet_size,
            min_send_size,
            cur_region_idx,
            padding,
            last_sent_bytes: Bytes::default(),
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
        let mut bytes = match self.send_packet_size <= self.min_send_size {
            true => BytesMut::with_capacity(self.min_send_size),
            false => BytesMut::with_capacity(self.send_packet_size),
        };
        // write four bytes of 0 as the padding (for this server)
        for _i in 0..4 {
            bytes.put_u8(0);
        }
        for _i in 0..self.num_segments {
            let (phys_region, refcnt_multiplier) = unsafe { REGION_ORDER[self.cur_region_idx] };
            let virtual_segment = self.num_regions * refcnt_multiplier + phys_region;
            self.cur_region_idx += 1;
            if self.cur_region_idx == unsafe { REGION_ORDER.len() } {
                self.cur_region_idx = 0;
            }
            bytes.put_u64_le(virtual_segment as u64);
        }
        bytes.put(self.padding.as_slice());
        self.last_sent_bytes = bytes.freeze();
        Ok(Some((id as u32, &self.last_sent_bytes.as_ref())))
    }

    fn process_received_msg(
        &mut self,
        sga: ReceivedPkt<<Self as ClientSM>::Datapath>,
        _datapath: &<Self as ClientSM>::Datapath,
    ) -> Result<bool> {
        if cfg!(debug_assertions) {
            let expected_size = self.num_segments * self.segment_size + RESPONSE_DATA_OFF;
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
                    let seg_sequence = &self.request_indices[sga.msg_id() as usize];
                    for (idx, virtual_seg) in seg_sequence.iter().enumerate() {
                        let seg = virtual_seg % self.server_payload_regions.len();
                        let msg_to_check = &sga.seg(0).as_ref()[(RESPONSE_DATA_OFF
                            + idx * self.segment_size)
                            ..(RESPONSE_DATA_OFF + (idx + 1) * self.segment_size)];
                        let expected = &self.server_payload_regions[seg];
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

    fn msg_timeout_cb(&mut self, _id: MsgID, _datapath: &Self::Datapath) -> Result<&[u8]> {
        // Not implemented so we can delete bytes from request array as we send
        unimplemented!();
    }
}
