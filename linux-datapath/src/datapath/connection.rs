use bytes::{BufMut, Bytes, BytesMut};
use color_eyre::eyre::{bail, ensure, Result};
use cornflakes_libos::{
    allocator::MempoolID,
    datapath::{Datapath, InlineMode, MetadataOps, ReceivedPkt},
    serialize::Serializable,
    utils::AddressInfo,
    ConnID, MsgID, RcSga, Sga,
};
use color_eyre::eyre::WrapErr;
use cornflakes_utils::{parse_yaml_map, AppMode};
use eui48::MacAddress;
use hashbrown::HashMap;
use std::{
    io::Write,
    net::{Ipv4Addr, UdpSocket},
    time::{Duration, Instant},
};

const MAX_CONCURRENT_CONNECTIONS: usize = 128;

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct MutableByteBuffer {
    buf: BytesMut,
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

#[derive(PartialEq, Eq, Clone, Debug)]
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
    /// Per thread context.
    thread_context: LinuxPerThreadContext,
    /// Server or client mode
    mode: AppMode,
    /// UDP socket
    socket: UdpSocket,
    /// Map from AddressInfo to connection id
    address_to_conn_id: HashMap<AddressInfo, ConnID>,
    /// Addresses of active connections, indexed by connection id
    active_connections: [Option<AddressInfo>; MAX_CONCURRENT_CONNECTIONS],
}

impl LinuxConnection {
    fn _insert_into_outgoing_map(&mut self, _msg_id: MsgID, _conn_id: ConnID) {
        unimplemented!();
    }

    fn _check_received_pkt(&self) -> Result<Option<ReceivedPkt<Self>>> {
        unimplemented!();
    }
}

impl Datapath for LinuxConnection {
    type DatapathBuffer = MutableByteBuffer;

    type DatapathMetadata = ByteBuffer;

    type PerThreadContext = LinuxPerThreadContext;

    type DatapathSpecificParams = LinuxDatapathSpecificParams;

    fn parse_config_file(
        config_file: &str,
        our_ip: &Ipv4Addr,
    ) -> Result<Self::DatapathSpecificParams> {
        let (ip_to_mac, _mac_to_ip, udp_port, client_port) =
            parse_yaml_map(config_file).wrap_err("Failed to parse yaml mapping")?;

        let eth_addr = match ip_to_mac.get(our_ip) {
            Some(e) => e.clone(),
            None => {
                bail!("Could not find eth addr for passed in ipv4 addr {:?} in config_file ip_to_mac map: {:?}", our_ip, ip_to_mac);
            }
        };

        Ok(LinuxDatapathSpecificParams{
            our_ip: our_ip.clone(),
            our_eth: eth_addr,
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
            bail!("Currently, linux datapath does not support more than one
               queue");
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
            context.address_info.ipv4_addr,
            context.address_info.udp_port,
        );
        tracing::debug!("Binding to {}", addr);
        let socket = UdpSocket::bind(addr).unwrap();
        Ok(LinuxConnection {
            start: Instant::now(),
            thread_context: context,
            mode,
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

    fn push_buffers_with_copy(&mut self, _pkts: Vec<(MsgID, ConnID, &[u8])>) -> Result<()> {
        unimplemented!();
    }

    fn serialize_and_send(
        &mut self,
        _objects: &Vec<(MsgID, ConnID, impl Serializable<Self>)>,
    ) -> Result<()>
    where
        Self: Sized,
    {
        unimplemented!();
    }

    fn echo(&mut self, mut _pkts: Vec<ReceivedPkt<Self>>) -> Result<()>
    where
        Self: Sized,
    {
        unimplemented!();
    }

    fn push_rc_sgas(&mut self, _rc_sgas: &mut Vec<(MsgID, ConnID, RcSga<Self>)>) -> Result<()>
    where
        Self: Sized,
    {
        unimplemented!();
    }

    fn push_sgas(&mut self, _sgas: &Vec<(MsgID, ConnID, Sga)>) -> Result<()> {
        unimplemented!();
    }

    fn pop_with_durations(&mut self) -> Result<Vec<(ReceivedPkt<Self>, Duration)>>
    where
        Self: Sized,
    {
        unimplemented!();
    }

    fn pop(&mut self) -> Result<Vec<ReceivedPkt<Self>>>
    where
        Self: Sized,
    {
        unimplemented!();
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

    fn get_metadata(&self, buf: Self::DatapathBuffer) -> Result<Option<Self::DatapathMetadata>> {
        Ok(Some(ByteBuffer::from_buf(buf)))
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

    fn set_inline_mode(&mut self, _mode: InlineMode) {}
}
