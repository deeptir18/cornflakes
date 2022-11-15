use byteorder::{ByteOrder, NetworkEndian};
use bytes::{BufMut, Bytes, BytesMut};
use color_eyre::eyre::WrapErr;
use color_eyre::eyre::{bail, ensure, Result};
use cornflakes_libos::{
    allocator::MempoolID,
    datapath::{Datapath, DatapathBufferOps, InlineMode, MetadataOps, ReceivedPkt},
    utils::{AddressInfo, HEADER_ID_SIZE},
    ConnID, MsgID, OrderedSga, RcSga, Sga,
};
use cornflakes_utils::{parse_yaml_map, AppMode};
use eui48::MacAddress;
use hashbrown::HashMap;
use std::{
    io::{self, Write},
    net::{IpAddr, Ipv4Addr, UdpSocket},
    time::{Duration, Instant},
};

const FILLER_MAC: &str = "ff:ff:ff:ff:ff:ff";
const MAX_CONCURRENT_CONNECTIONS: usize = 128;
// TOOD(ygina): careful with fixed max buffer size...
const RECEIVE_BUFFER_SIZE: usize = 2048;
const RECEIVE_BURST_SIZE: usize = 32;

#[derive(PartialEq, Eq, Debug, Clone, Default)]
pub struct MutableByteBuffer {
    buf: BytesMut,
}

impl DatapathBufferOps for MutableByteBuffer {
    fn set_mempool_id(&mut self, _id: MempoolID) {}

    fn get_mempool_id(&self) -> MempoolID {
        0
    }

    fn set_len(&mut self, _len: usize) {
        unimplemented!();
    }

    fn get_mutable_slice(&mut self, _start: usize, _len: usize) -> Result<&mut [u8]> {
        unimplemented!();
    }
}

impl MutableByteBuffer {
    pub fn new(buf: &[u8]) -> Self {
        MutableByteBuffer {
            buf: BytesMut::from(buf),
        }
    }

    pub fn new_with_capacity(data_len: usize) -> Self {
        // TODO: bad because this is a stack allocation?
        MutableByteBuffer {
            buf: BytesMut::from(vec![0u8; data_len].as_slice()),
        }
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn get_inner(self) -> BytesMut {
        self.buf
    }

    pub fn mutable_slice(&mut self, start: usize, end: usize) -> Result<&mut [u8]> {
        if start > self.buf.len() || end > self.buf.len() || end > start {
            bail!("Invalid bounds for MutableByteBuffer");
        }
        Ok(&mut self.buf.as_mut()[start..end])
    }
}

impl AsRef<[u8]> for MutableByteBuffer {
    fn as_ref(&self) -> &[u8] {
        self.buf.as_ref()
    }
}

impl Write for MutableByteBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let to_write = std::cmp::min(buf.len(), self.buf.len());
        self.buf.as_mut().write(&buf[0..to_write])
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(PartialEq, Eq, Clone, Debug, Default)]
pub struct ByteBuffer {
    /// Actual data: not write-able.
    pub bytes: Bytes,
    /// Application data offset
    pub offset: usize,
    /// Application data length
    pub len: usize,
}

impl ByteBuffer {
    pub fn from_buf(buffer: MutableByteBuffer) -> Self {
        let len = buffer.len();
        let buf = buffer.get_inner().freeze();
        ByteBuffer {
            bytes: buf,
            offset: 0,
            len: len,
        }
    }

    pub fn from_raw_buf(buffer: &[u8]) -> Self {
        let mut bytes = BytesMut::with_capacity(buffer.len());
        bytes.copy_from_slice(buffer);
        let buf = bytes.freeze();
        ByteBuffer {
            bytes: buf,
            offset: 0,
            len: buffer.len(),
        }
    }

    pub fn new(buf: &[u8], data_offset: usize, data_len: Option<usize>) -> Result<Self> {
        ensure!(data_offset <= buf.len(), "Provided data offset too large");
        let len = match data_len {
            Some(x) => {
                ensure!(x <= buf.len() - data_offset, "Data len to large");
                x
            }
            None => buf.len() - data_offset,
        };
        let mut bytes_mut = BytesMut::with_capacity(buf.len());
        bytes_mut.put(buf);
        Ok(ByteBuffer {
            bytes: bytes_mut.freeze(),
            offset: data_offset,
            len: len,
        })
    }
}

impl AsRef<[u8]> for ByteBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.bytes.as_ref()[self.offset..(self.offset + self.len)]
    }
}
impl MetadataOps for ByteBuffer {
    fn offset(&self) -> usize {
        self.offset
    }

    fn data_len(&self) -> usize {
        self.len
    }

    fn set_data_len_and_offset(&mut self, len: usize, offset: usize) -> Result<()> {
        ensure!(offset <= self.bytes.len(), "Offset too large");
        ensure!(
            len <= self.bytes.len() - offset,
            "Provided data len too large"
        );
        self.offset = offset;
        self.len = len;
        Ok(())
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct LinuxPerThreadContext {
    /// Queue id
    queue_id: u16,
    /// Source address info
    address_info: AddressInfo,
}

#[derive(Debug, Clone)]
pub struct LinuxDatapathSpecificParams {
    // TODO: insert datapath specific params:
    // Server UDP port, Server IP address, potentially server interface name
    our_ip: Ipv4Addr,
    our_eth: MacAddress,
    client_port: u16,
    server_port: u16,
}

impl LinuxDatapathSpecificParams {
    pub fn get_ipv4(&self) -> Ipv4Addr {
        self.our_ip.clone()
    }

    pub fn get_mac(&self) -> MacAddress {
        self.our_eth.clone()
    }

    pub fn get_client_port(&self) -> u16 {
        self.client_port
    }

    pub fn get_server_port(&self) -> u16 {
        self.server_port
    }
}

pub struct LinuxConnection {
    /// Start time.
    start: Instant,
    /// Server or client mode
    mode: AppMode,
    /// Current window of outstanding packets (used for keeping track of RTTs).
    outgoing_window: HashMap<(MsgID, ConnID), Instant>,
    /// UDP socket
    socket: UdpSocket,
    /// Map from AddressInfo to connection id
    address_to_conn_id: HashMap<AddressInfo, ConnID>,
    /// Addresses of active connections, indexed by connection id
    active_connections: [Option<AddressInfo>; MAX_CONCURRENT_CONNECTIONS],
}

impl LinuxConnection {
    fn insert_into_outgoing_map(&mut self, msg_id: MsgID, conn_id: ConnID) {
        if self.mode == AppMode::Client {
            if !self.outgoing_window.contains_key(&(msg_id, conn_id)) {
                self.outgoing_window
                    .insert((msg_id, conn_id), Instant::now());
            }
        }
    }

    fn check_received_pkt(&mut self) -> Result<Option<ReceivedPkt<Self>>> {
        let mut buf = [0; RECEIVE_BUFFER_SIZE];
        let addr = match self.socket.recv_from(&mut buf) {
            Ok((n, addr)) => {
                if n == 0 {
                    tracing::debug!("Received {} bytes from {:?}", n, addr);
                    return Ok(None);
                } else {
                    assert!(n > HEADER_ID_SIZE);
                    tracing::debug!("Received {} bytes from {:?}", n, addr);
                    addr
                }
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                return Ok(None);
            }
            Err(e) => panic!("encountered IO error: {}", e),
        };
        let msg_id = NetworkEndian::read_u32(&buf[0..4]);
        let conn_id = {
            let ipv4 = match addr.ip() {
                IpAddr::V4(ip) => ip,
                _ => {
                    panic!("expected ipv4 address")
                }
            };
            let src_addr = cornflakes_libos::utils::AddressInfo::new(
                addr.port(),
                ipv4,
                MacAddress::parse_str(FILLER_MAC).unwrap(),
            );
            self.connect(src_addr)
                .wrap_err("TOO MANY CONCURRENT CONNECTIONS")?
        };
        let bytes = ByteBuffer::new(&buf, HEADER_ID_SIZE, None)?;
        let received_pkt = ReceivedPkt::new(vec![bytes], msg_id, conn_id);
        Ok(Some(received_pkt))
    }
}

impl Datapath for LinuxConnection {
    type DatapathBuffer = MutableByteBuffer;

    type DatapathMetadata = ByteBuffer;

    type CallbackEntryState = ();

    type PerThreadContext = LinuxPerThreadContext;

    type DatapathSpecificParams = LinuxDatapathSpecificParams;

    fn parse_config_file(
        config_file: &str,
        our_ip: &Ipv4Addr,
    ) -> Result<Self::DatapathSpecificParams> {
        let (_ip_to_mac, _mac_to_ip, udp_port, client_port) =
            parse_yaml_map(config_file).wrap_err("Failed to parse yaml mapping")?;
        Ok(LinuxDatapathSpecificParams {
            our_ip: our_ip.clone(),
            our_eth: MacAddress::parse_str(FILLER_MAC).unwrap(),
            client_port: client_port,
            server_port: udp_port,
        })
    }

    fn compute_affinity(
        datapath_params: &Self::DatapathSpecificParams,
        num_queues: usize,
        _remote_ip: Option<Ipv4Addr>,
        app_mode: cornflakes_utils::AppMode,
    ) -> Result<Vec<AddressInfo>> {
        if num_queues > 1 {
            bail!(
                "Currently, linux datapath does not support more than one
               queue"
            );
        }
        match app_mode {
            AppMode::Client => Ok(vec![AddressInfo::new(
                datapath_params.get_client_port(),
                datapath_params.get_ipv4(),
                datapath_params.get_mac(),
            )]),

            AppMode::Server => Ok(vec![AddressInfo::new(
                datapath_params.get_server_port(),
                datapath_params.get_ipv4(),
                datapath_params.get_mac(),
            )]),
        }
    }

    fn global_init(
        num_queues: usize,
        _datapath_params: &mut Self::DatapathSpecificParams,
        addresses: Vec<AddressInfo>,
    ) -> Result<Vec<Self::PerThreadContext>> {
        assert_eq!(num_queues, 1);
        assert_eq!(addresses.len(), num_queues);
        let mut ret: Vec<Self::PerThreadContext> = Vec::with_capacity(num_queues);
        for (i, addr) in addresses.into_iter().enumerate() {
            ret.push(LinuxPerThreadContext {
                queue_id: i as _,
                address_info: addr,
            });
        }
        Ok(ret)
    }

    fn per_thread_init(
        _datapath_params: Self::DatapathSpecificParams,
        context: Self::PerThreadContext,
        mode: cornflakes_utils::AppMode,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        let addr = format!(
            "{}:{}",
            context.address_info.ipv4_addr, context.address_info.udp_port,
        );
        tracing::info!("Binding to {}", addr);
        let socket = UdpSocket::bind(addr).unwrap();
        socket.set_nonblocking(true)?;
        Ok(LinuxConnection {
            start: Instant::now(),
            mode,
            outgoing_window: HashMap::default(),
            socket,
            address_to_conn_id: HashMap::default(),
            active_connections: [None; MAX_CONCURRENT_CONNECTIONS],
        })
    }

    fn connect(&mut self, addr: AddressInfo) -> Result<ConnID> {
        if self.address_to_conn_id.contains_key(&addr) {
            return Ok(*self.address_to_conn_id.get(&addr).unwrap());
        } else {
            if self.address_to_conn_id.len() >= MAX_CONCURRENT_CONNECTIONS {
                bail!("too many concurrent connections; cannot connect to more");
            }
            let mut idx: Option<usize> = None;
            for (i, addr_option) in self.active_connections.iter().enumerate() {
                match addr_option {
                    Some(_) => {}
                    None => {
                        self.address_to_conn_id.insert(addr.clone(), i);
                        idx = Some(i);
                        break;
                    }
                }
            }
            match idx {
                Some(i) => {
                    self.active_connections[i] = Some(addr);
                    return Ok(i);
                }
                None => {
                    bail!("too many concurrent connections; cannot connect to more");
                }
            }
        }
    }

    fn push_buffers_with_copy(&mut self, pkts: &[(MsgID, ConnID, &[u8])]) -> Result<()> {
        tracing::debug!("Pushing batch of pkts of length {}", pkts.len());
        for (msg_id, conn_id, data) in pkts.iter() {
            self.insert_into_outgoing_map(*msg_id, *conn_id);
            let mut buf = vec![0, 0, 0, 0];
            NetworkEndian::write_u32(&mut buf, *msg_id);
            buf.extend_from_slice(data);
            let addr = {
                let address_info = self.active_connections[*conn_id].unwrap();
                format!("{}:{}", address_info.ipv4_addr, address_info.udp_port)
            };
            tracing::debug!("Sending {} bytes to {}", buf.len(), addr);
            let n = self.socket.send_to(&buf, &addr).expect(&format!(
                "Failed to send data (len {}) to {}",
                buf.len(),
                addr
            ));
            assert_eq!(n, buf.len());
        }
        Ok(())
    }

    fn echo(&mut self, mut _pkts: Vec<ReceivedPkt<Self>>) -> Result<()>
    where
        Self: Sized,
    {
        unimplemented!();
    }

    fn push_rc_sgas(&mut self, _rc_sgas: &mut [(MsgID, ConnID, RcSga<Self>)]) -> Result<()>
    where
        Self: Sized,
    {
        unimplemented!();
    }

    fn push_ordered_sgas(&mut self, ordered_sgas: &[(MsgID, ConnID, OrderedSga)]) -> Result<()> {
        let bufs = ordered_sgas
            .iter()
            .map(|(_, _, ordered_sga)| {
                let mut buf = vec![];
                buf.extend_from_slice(ordered_sga.get_hdr());
                for sge in ordered_sga.sga().iter() {
                    buf.extend_from_slice(sge.addr());
                }
                buf
            })
            .collect::<Vec<_>>();
        let pkts = ordered_sgas
            .iter()
            .enumerate()
            .map(|(i, (msg_id, conn_id, _))| (*msg_id, *conn_id, &bufs[i][..]))
            .collect::<Vec<_>>();
        self.push_buffers_with_copy(&pkts)
    }

    fn push_sgas(&mut self, sgas: &[(MsgID, ConnID, Sga)]) -> Result<()> {
        let bufs = sgas
            .iter()
            .map(|(_, _, sga)| {
                let mut buf = vec![];
                for sge in sga.iter() {
                    buf.extend_from_slice(sge.addr());
                }
                buf
            })
            .collect::<Vec<_>>();
        let pkts = sgas
            .iter()
            .enumerate()
            .map(|(i, (msg_id, conn_id, _))| (*msg_id, *conn_id, &bufs[i][..]))
            .collect::<Vec<_>>();
        self.push_buffers_with_copy(&pkts)
    }

    fn pop_with_durations(&mut self) -> Result<Vec<(ReceivedPkt<Self>, Duration)>>
    where
        Self: Sized,
    {
        let mut ret: Vec<(ReceivedPkt<Self>, Duration)> = Vec::with_capacity(RECEIVE_BURST_SIZE);
        for _ in 0..RECEIVE_BURST_SIZE {
            let received_pkt = match self.check_received_pkt()? {
                Some(received_pkt) => received_pkt,
                None => {
                    break;
                }
            };
            let dur = match self
                .outgoing_window
                .remove(&(received_pkt.msg_id(), received_pkt.conn_id()))
            {
                Some(start_time) => start_time.elapsed(),
                None => {
                    bail!(
                        "Cannot find msg id {} and conn id {} in outgoing window",
                        received_pkt.msg_id(),
                        received_pkt.conn_id()
                    );
                }
            };
            ret.push((received_pkt, dur));
        }
        if !ret.is_empty() {
            tracing::debug!("Received {} packets", ret.len());
        }
        Ok(ret)
    }

    fn pop(&mut self) -> Result<Vec<ReceivedPkt<Self>>>
    where
        Self: Sized,
    {
        let mut ret: Vec<ReceivedPkt<Self>> = Vec::with_capacity(RECEIVE_BURST_SIZE);
        for _ in 0..RECEIVE_BURST_SIZE {
            let received_pkt = match self.check_received_pkt()? {
                Some(received_pkt) => received_pkt,
                None => {
                    break;
                }
            };
            ret.push(received_pkt);
        }
        if !ret.is_empty() {
            tracing::debug!("Received {} packets", ret.len());
        }
        Ok(ret)
    }

    fn timed_out(&self, _time_out: Duration) -> Result<Vec<(MsgID, ConnID)>> {
        unimplemented!();
    }

    fn is_registered(&self, _buf: &[u8]) -> bool {
        false
    }

    fn allocate(&mut self, _size: usize) -> Result<Option<Self::DatapathBuffer>> {
        unimplemented!();
    }

    fn allocate_tx_buffer(&mut self) -> Result<(Option<Self::DatapathBuffer>, usize)> {
        unimplemented!();
    }

    fn get_metadata(&self, buf: Self::DatapathBuffer) -> Result<Option<Self::DatapathMetadata>> {
        Ok(Some(ByteBuffer::from_buf(buf)))
    }

    fn recover_metadata(&self, buf: &[u8]) -> Result<Option<Self::DatapathMetadata>> {
        Ok(Some(ByteBuffer::from_raw_buf(buf)))
    }

    fn add_memory_pool(&mut self, _size: usize, _min_elts: usize) -> Result<Vec<MempoolID>> {
        Ok(vec![])
    }

    fn register_mempool(&mut self, _id: MempoolID) -> Result<()> {
        unimplemented!();
    }

    fn unregister_mempool(&mut self, _id: MempoolID) -> Result<()> {
        unimplemented!();
    }

    fn header_size(&self) -> usize {
        unimplemented!();
    }

    fn timer_hz(&self) -> u64 {
        // cycles in a second
        // (arbitrary constant)
        1_000_000
    }

    fn cycles_to_ns(&self, t: u64) -> u64 {
        // 1mil cycles/s
        // 1k   cycles/ms
        // 1    cycle/us
        // 1000 ns/cycle
        t * 1_000
    }

    fn current_cycles(&self) -> u64 {
        self.start.elapsed().as_micros() as _
    }

    fn set_copying_threshold(&mut self, _threshold: usize) {}

    fn get_copying_threshold(&self) -> usize {
        std::usize::MAX
    }

    fn set_max_segments(&mut self, _segs: usize) {}

    fn get_max_segments(&self) -> usize {
        std::usize::MAX
    }

    #[inline]
    fn has_mempool(&self, _size: usize) -> bool {
        true
    }

    fn set_inline_mode(&mut self, _mode: InlineMode) {}

    fn max_packet_size() -> usize {
        1500
    }
}
