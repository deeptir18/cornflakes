use super::{
    super::dpdk_bindings::*, allocator::MemoryAllocator, dpdk_check, dpdk_error, dpdk_utils::*,
    wrapper::*,
};
use cornflakes_libos::{
    datapath::{Datapath, MetadataOps, ReceivedPkt},
    mem::{PGSIZE_2MB, PGSIZE_4KB},
    serialize::Serializable,
    utils::AddressInfo,
    ConnID, MsgID, RcSga, RcSge, Sga, USING_REF_COUNTING,
};

use byteorder::{ByteOrder, NetworkEndian};
use color_eyre::eyre::{bail, ensure, Result, WrapErr};
use cornflakes_utils::{parse_yaml_map, AppMode};
use eui48::MacAddress;
use hashbrown::HashMap;
use std::{
    ffi::CString,
    fs::read_to_string,
    io::Write,
    mem::MaybeUninit,
    net::Ipv4Addr,
    path::Path,
    ptr,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

#[cfg(feature = "profiler")]
use perftools;

const MAX_CONCURRENT_CONNECTIONS: usize = 128;

const COMPLETION_BUDGET: usize = 32;
const RECEIVE_BURST_SIZE: usize = 32;
const SEND_BURST_SIZE: usize = 32;
const MEMPOOL_MAX_SIZE: usize = 65536;

/// RX and TX Prefetch, Host, and Write-back threshold values should be
/// carefully set for optimal performance. Consult the network
/// controller's datasheet and supporting DPDK documentation for guidance
/// on how these parameters should be set.
const RX_PTHRESH: u8 = 8;
const RX_HTHRESH: u8 = 8;
const RX_WTHRESH: u8 = 0;

/// These default values are optimized for use with the Intel(R) 82599 10 GbE
/// Controller and the DPDK ixgbe PMD. Consider using other values for other
/// network controllers and/or network drivers.
const TX_PTHRESH: u8 = 0;
const TX_HTHRESH: u8 = 0;
const TX_WTHRESH: u8 = 0;

const RX_RING_SIZE: u16 = 2048;
const TX_RING_SIZE: u16 = 2048;

#[derive(PartialEq, Eq)]
pub struct DpdkBuffer {
    /// Underlying allocated mbuf
    mbuf: *mut rte_mbuf,
}

impl DpdkBuffer {
    pub fn new(mbuf: *mut rte_mbuf) -> Self {
        DpdkBuffer { mbuf: mbuf }
    }

    pub fn get_inner(self) -> *mut rte_mbuf {
        self.mbuf
    }

    unsafe fn effective_buf_len(&self) -> usize {
        access!(self.mbuf, buf_len, usize) - access!(self.mbuf, data_off, usize)
    }
}

impl std::fmt::Debug for DpdkBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Mbuf addr: {:?}, data_len: {}", self.mbuf, unsafe {
            access!(self.mbuf, data_len, usize)
        })
    }
}

impl AsRef<[u8]> for DpdkBuffer {
    fn as_ref(&self) -> &[u8] {
        unsafe { mbuf_slice!(self.mbuf, 0, access!(self.mbuf, data_len, usize)) }
    }
}

impl Write for DpdkBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let to_write = std::cmp::min(unsafe { self.effective_buf_len() }, buf.len());
        let mut mut_slice = unsafe { mbuf_mut_slice!(self.mbuf, 0, to_write) };
        let written = mut_slice.write(&buf[0..to_write])?;
        unsafe { write_struct_field!(self.mbuf, data_len, written) };
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(PartialEq, Eq)]
pub struct RteMbufMetadata {
    /// Underlying mbuf
    mbuf: *mut rte_mbuf,
    /// Data offset for application
    offset: usize,
    /// Application data length
    data_len: usize,
}

impl RteMbufMetadata {
    pub fn from_dpdk_buf(dpdk_buffer: DpdkBuffer) -> Self {
        // data_len already set in this mbuf
        let mbuf = dpdk_buffer.get_inner();

        // increment the reference count of the underlying mbuf
        unsafe { rte_pktmbuf_refcnt_set(mbuf, 1) };
        let data_len = unsafe { access!(mbuf, data_len, usize) };

        RteMbufMetadata {
            mbuf: mbuf,
            offset: 0,
            data_len: data_len,
        }
    }

    pub fn new(mbuf: *mut rte_mbuf, data_offset: usize, data_len: Option<usize>) -> Result<Self> {
        // check whether: data_len <= (effective buf_len - new data_offset)
        // data_offset >= 0 and data_offset <= (effective_buf_len())
        let effective_buf_len =
            unsafe { access!(mbuf, buf_len, usize) - access!(mbuf, data_off, usize) };
        ensure!(data_offset <= effective_buf_len, "Data offset too large");

        unsafe { rte_pktmbuf_refcnt_set(mbuf, 1) };

        let len = match data_len {
            Some(x) => {
                ensure!(x <= (effective_buf_len - data_offset), "Data len too large");
                x
            }
            None => unsafe {
                // set to current data length - provided offset
                access!(mbuf, data_len, usize) - data_offset
            },
        };

        Ok(RteMbufMetadata {
            mbuf: mbuf,
            offset: data_offset,
            data_len: len,
        })
    }

    pub fn mbuf(&self) -> *mut rte_mbuf {
        self.mbuf
    }

    unsafe fn effective_buf_len(&self) -> usize {
        access!(self.mbuf, buf_len, usize) - access!(self.mbuf, data_off, usize)
    }
}

impl MetadataOps for RteMbufMetadata {
    fn offset(&self) -> usize {
        self.offset
    }

    fn data_len(&self) -> usize {
        unsafe { access!(self.mbuf, data_len, usize) }
    }

    fn set_data_len_and_offset(&mut self, len: usize, offset: usize) -> Result<()> {
        ensure!(
            offset <= unsafe { self.effective_buf_len() },
            "Provided offset too large"
        );
        ensure!(
            len <= unsafe { self.effective_buf_len() - offset },
            "Provided data len too large"
        );
        self.offset = offset;
        self.data_len = len;
        Ok(())
    }
}

impl Default for RteMbufMetadata {
    fn default() -> Self {
        RteMbufMetadata {
            mbuf: ptr::null_mut(),
            offset: 0,
            data_len: 0,
        }
    }
}

impl Drop for RteMbufMetadata {
    fn drop(&mut self) {
        unsafe {
            if USING_REF_COUNTING {
                rte_pktmbuf_refcnt_update_or_free(self.mbuf, -1);
            } else {
                rte_pktmbuf_free(self.mbuf);
            }
        }
    }
}

impl Clone for RteMbufMetadata {
    fn clone(&self) -> RteMbufMetadata {
        unsafe {
            if USING_REF_COUNTING {
                rte_pktmbuf_refcnt_update_or_free(self.mbuf, 1);
            }
        }
        RteMbufMetadata {
            mbuf: self.mbuf,
            offset: self.offset,
            data_len: self.data_len,
        }
    }
}

impl std::fmt::Debug for RteMbufMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "Mbuf addr: {:?}, data addr: {:?}, data_len: {}",
            self.mbuf,
            self.as_ref().as_ptr(),
            unsafe { access!(self.mbuf, data_len, usize) }
        )?;
        Ok(())
    }
}

impl AsRef<[u8]> for RteMbufMetadata {
    fn as_ref(&self) -> &[u8] {
        unsafe { mbuf_slice!(self.mbuf, self.offset, self.data_len) }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct DpdkPerThreadContext {
    /// Queue id
    queue_id: u16,
    /// Source address info
    address_info: AddressInfo,
    /// Basic mempool for receiving packets
    recv_mempool: *mut rte_mempool,
    /// Empty mempool for allocating external buffers
    extbuf_mempool: *mut rte_mempool,
}

unsafe impl Send for DpdkPerThreadContext {}
unsafe impl Sync for DpdkPerThreadContext {}

impl DpdkPerThreadContext {
    pub fn get_recv_mempool(&self) -> *mut rte_mempool {
        self.recv_mempool
    }

    pub fn get_extbuf_mempool(&self) -> *mut rte_mempool {
        self.extbuf_mempool
    }

    pub fn get_address_info(&self) -> &AddressInfo {
        &self.address_info
    }

    pub fn get_queue_id(&self) -> u16 {
        self.queue_id
    }
}

impl Drop for DpdkPerThreadContext {
    fn drop(&mut self) {
        unsafe {
            rte_mempool_free(self.recv_mempool);
        }
        unsafe {
            rte_mempool_free(self.extbuf_mempool);
        };
    }
}

#[derive(Debug, Clone)]
pub struct DpdkDatapathSpecificParams {
    eal_init: Vec<String>,
    dpdk_port: i16,
    our_ip: Ipv4Addr,
    our_eth: MacAddress,
    starting_client_port: u16,
    server_port: u16,
}

impl DpdkDatapathSpecificParams {
    fn get_eal_params(&self) -> &Vec<String> {
        &self.eal_init
    }

    fn set_physical_port(&mut self, port: u16) {
        self.dpdk_port = port as i16;
    }

    fn get_physical_port(&self) -> Result<u16> {
        if self.dpdk_port < 0 {
            bail!("DPDK port not set");
        }
        Ok(self.dpdk_port as u16)
    }

    fn get_ipv4(&self) -> Ipv4Addr {
        self.our_ip.clone()
    }

    pub fn get_mac(&self) -> MacAddress {
        self.our_eth.clone()
    }

    pub fn get_client_port(&self) -> u16 {
        self.starting_client_port
    }

    pub fn get_server_port(&self) -> u16 {
        self.server_port
    }
}

pub struct DpdkConnection {
    /// Per thread context
    thread_context: DpdkPerThreadContext,
    /// Server or client mode
    mode: AppMode,
    /// Current window of outstanding packets (used to keep track of rtts)
    outgoing_window: HashMap<(MsgID, ConnID), Instant>,
    /// Active connections: current connection IDs mapped to addresses.
    active_connections: [Option<(
        AddressInfo,
        [u8; cornflakes_libos::utils::TOTAL_UDP_HEADER_SIZE],
    )>; MAX_CONCURRENT_CONNECTIONS],
    /// Map from address info to connection id
    address_to_conn_id: HashMap<AddressInfo, ConnID>,
    /// Allocator for outgoing packets
    allocator: MemoryAllocator,
    /// Threshold for copying a segment or leaving as a separate scatter-gather entry
    copying_threshold: usize,
    /// Array of mbuf pointers used to receive packets
    recv_mbufs: [*mut rte_mbuf; RECEIVE_BURST_SIZE],
    /// Array of mbuf pointers used to send packets
    send_mbufs: [*mut rte_mbuf; SEND_BURST_SIZE],
}

impl DpdkConnection {}

impl Datapath for DpdkConnection {
    type DatapathBuffer = DpdkBuffer;

    type DatapathMetadata = RteMbufMetadata;

    type PerThreadContext = DpdkPerThreadContext;

    type DatapathSpecificParams = DpdkDatapathSpecificParams;

    /// IP not required as dpdk has ability to auto-detect the ethernet address
    fn parse_config_file(
        config_file: &str,
        our_ip: &Ipv4Addr,
    ) -> Result<Self::DatapathSpecificParams> {
        let (ip_to_mac, _mac_to_ip, udp_port, client_port) =
            parse_yaml_map(config_file).wrap_err("Failed to parse yaml mapping")?;

        let eal_init = parse_eal_init(config_file)?;

        // since eal init has not been run yet, we cannot run dpdk get macaddr
        let eth_addr = match ip_to_mac.get(our_ip) {
            Some(e) => e.clone(),
            None => {
                bail!("Could not find eth addr for passed in ipv4 addr {:?} in config_file ip_to_mac map: {:?}", our_ip, ip_to_mac);
            }
        };

        Ok(DpdkDatapathSpecificParams {
            eal_init: eal_init,
            dpdk_port: -1,
            our_ip: our_ip.clone(),
            our_eth: eth_addr,
            starting_client_port: client_port,
            server_port: udp_port,
        })
    }

    fn compute_affinity(
        datapath_params: &Self::DatapathSpecificParams,
        num_queues: usize,
        remote_ip: Option<Ipv4Addr>,
        app_mode: AppMode,
    ) -> Result<Vec<AddressInfo>> {
        let my_eth = datapath_params.get_mac();
        let my_starting_ip = datapath_params.get_ipv4();
        let my_port = match app_mode {
            AppMode::Server => datapath_params.get_server_port(),
            AppMode::Client => datapath_params.get_client_port(),
        };

        if app_mode == AppMode::Server {
            // TODO: implement more than one queue on the server side
            if num_queues > 1 {
                bail!("Currently, only 1 queue supported on server side");
            }

            let addr_info = AddressInfo::new(my_port, my_starting_ip, my_eth);
            return Ok(vec![addr_info]);
        }

        let server_ip = match remote_ip {
            Some(x) => x,
            None => {
                bail!("For client mode, must specify server ip to compute affinity");
            }
        };

        let server_port = datapath_params.get_server_port();

        let mut recv_addrs: Vec<AddressInfo> = Vec::with_capacity(num_queues);
        let in_recv_addrs = |ip: &[u8; 4], port: u16, ref_addrs: &Vec<AddressInfo>| -> bool {
            for current_addr in ref_addrs.iter() {
                if current_addr.ipv4_addr.octets() == *ip && current_addr.udp_port == port {
                    return true;
                }
            }
            return false;
        };

        for queue_id in 0..num_queues as u16 {
            let mut cur_octets = my_starting_ip.octets();
            let cur_port = my_port;
            while unsafe {
                compute_flow_affinity(
                    ip_from_octets(&cur_octets),
                    ip_from_octets(&server_ip.octets()),
                    cur_port,
                    server_port,
                    num_queues,
                )
            } != queue_id as u32
                || in_recv_addrs(&cur_octets, cur_port, &recv_addrs)
            {
                cur_octets[3] += 1;
            }
            tracing::info!(queue_id = queue_id, octets = ?cur_octets, port = cur_port, "Chosen addr pair");
            let queue_addr_info = AddressInfo::new(
                cur_port,
                Ipv4Addr::new(cur_octets[0], cur_octets[1], cur_octets[2], cur_octets[3]),
                my_eth,
            );
            recv_addrs.push(queue_addr_info);
        }

        Ok(recv_addrs)
    }

    fn global_init(
        num_queues: usize,
        datapath_params: &mut Self::DatapathSpecificParams,
        addresses: Vec<AddressInfo>,
    ) -> Result<Vec<Self::PerThreadContext>> {
        // run eal init
        let eal_args = datapath_params.get_eal_params();
        let mut args = vec![];
        let mut ptrs = vec![];
        for entry in eal_args.iter() {
            let s = CString::new(entry.as_str()).unwrap();
            ptrs.push(s.as_ptr() as *mut u8);
            args.push(s);
        }

        tracing::debug!("DPDK init args: {:?}", args);
        unsafe {
            dpdk_check_not_failed!(rte_eal_init(ptrs.len() as i32, ptrs.as_ptr() as *mut _));
        }

        // Find and set physical port
        let nb_ports = unsafe { rte_eth_dev_count_avail() };
        if nb_ports <= 0 {
            bail!("DPDK GLOBAL INIT: No physical ports available.");
        }
        tracing::info!(
            "DPDK reports that {} ports (interfaces) are available",
            nb_ports
        );
        datapath_params.set_physical_port(nb_ports - 1);

        // for each core, initialize a native memory pool and external buffer memory pool
        let mut ret: Vec<Self::PerThreadContext> = Vec::with_capacity(num_queues);
        for (i, addr) in addresses.into_iter().enumerate() {
            let recv_mempool = create_recv_mempool(
                &format!("recv_mbuf_pool_{}", i),
                datapath_params.get_physical_port()?,
            )
            .wrap_err(format!(
                "Not able create recv mbuf pool {} in global init",
                i
            ))?;

            let extbuf_mempool = create_extbuf_pool(
                &format!("extbuf_mempool_{}", i),
                datapath_params.get_physical_port()?,
            )
            .wrap_err(format!(
                "Not able to create extbuf mempool {} in global init",
                i
            ))?;

            ret.push(DpdkPerThreadContext {
                queue_id: i as _,
                address_info: addr,
                recv_mempool: recv_mempool,
                extbuf_mempool: extbuf_mempool,
            });
        }

        assert!(unsafe { rte_eth_dev_is_valid_port(datapath_params.get_physical_port()?) == 1 });

        let mut rx_conf: MaybeUninit<rte_eth_rxconf> = MaybeUninit::zeroed();
        unsafe {
            (*rx_conf.as_mut_ptr()).rx_thresh.pthresh = RX_PTHRESH;
            (*rx_conf.as_mut_ptr()).rx_thresh.hthresh = RX_HTHRESH;
            (*rx_conf.as_mut_ptr()).rx_thresh.wthresh = RX_WTHRESH;
            (*rx_conf.as_mut_ptr()).rx_free_thresh = 32;
        }

        let mut tx_conf: MaybeUninit<rte_eth_txconf> = MaybeUninit::zeroed();
        unsafe {
            (*tx_conf.as_mut_ptr()).tx_thresh.pthresh = TX_PTHRESH;
            (*tx_conf.as_mut_ptr()).tx_thresh.hthresh = TX_HTHRESH;
            (*tx_conf.as_mut_ptr()).tx_thresh.wthresh = TX_WTHRESH;
        }

        unsafe {
            eth_dev_configure(
                datapath_params.get_physical_port()?,
                num_queues as _,
                num_queues as _,
            )
        };

        let socket_id = unsafe {
            dpdk_check_not_failed!(
                rte_eth_dev_socket_id(datapath_params.get_physical_port()?),
                "Port id is out of range"
            )
        } as u32;

        for per_thread_context in ret.iter() {
            unsafe {
                dpdk_check_not_errored!(rte_eth_rx_queue_setup(
                    datapath_params.get_physical_port()?,
                    per_thread_context.get_queue_id(),
                    RX_RING_SIZE,
                    socket_id,
                    rx_conf.as_mut_ptr(),
                    per_thread_context.get_recv_mempool()
                ));
            }

            unsafe {
                dpdk_check_not_errored!(rte_eth_tx_queue_setup(
                    datapath_params.get_physical_port()?,
                    per_thread_context.get_queue_id(),
                    TX_RING_SIZE,
                    socket_id,
                    tx_conf.as_mut_ptr()
                ));
            }
        }

        // start ethernet port
        dpdk_check_not_errored!(rte_eth_dev_start(datapath_params.get_physical_port()?));

        // disable rx/tx flow control
        // TODO: why?

        let mut fc_conf: MaybeUninit<rte_eth_fc_conf> = MaybeUninit::zeroed();
        dpdk_check_not_errored!(rte_eth_dev_flow_ctrl_get(
            datapath_params.get_physical_port()?,
            fc_conf.as_mut_ptr()
        ));
        unsafe {
            (*fc_conf.as_mut_ptr()).mode = rte_eth_fc_mode_RTE_FC_NONE;
        }
        dpdk_check_not_errored!(rte_eth_dev_flow_ctrl_set(
            datapath_params.get_physical_port()?,
            fc_conf.as_mut_ptr()
        ));

        wait_for_link_status_up(datapath_params.get_physical_port()?)?;

        Ok(ret)
    }

    fn per_thread_init(
        datapath_params: Self::DatapathSpecificParams,
        context: Self::PerThreadContext,
        mode: AppMode,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(DpdkConnection {
            thread_context: context,
            mode: mode,
            outgoing_window: HashMap::default(),
            active_connections: [None; MAX_CONCURRENT_CONNECTIONS],
            address_to_conn_id: HashMap::default(),
            allocator: MemoryAllocator::default(),
            copying_threshold: 256,
            recv_mbufs: [ptr::null_mut(); RECEIVE_BURST_SIZE],
            send_mbufs: [ptr::null_mut(); SEND_BURST_SIZE],
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
                    let mut bytes: [u8; cornflakes_libos::utils::TOTAL_UDP_HEADER_SIZE] =
                        [0u8; cornflakes_libos::utils::TOTAL_UDP_HEADER_SIZE];
                    let header_info = cornflakes_libos::utils::HeaderInfo::new(
                        self.thread_context.get_address_info().clone(),
                        addr.clone(),
                    );
                    // write in the header to these bytes, assuming data length of 0
                    // data length is updated at runtime and checksums are updated on specific
                    // transmissions
                    cornflakes_libos::utils::write_eth_hdr(
                        &header_info,
                        &mut bytes[0..cornflakes_libos::utils::ETHERNET2_HEADER2_SIZE],
                    )?;
                    cornflakes_libos::utils::write_ipv4_hdr(
                        &header_info,
                        &mut bytes[cornflakes_libos::utils::ETHERNET2_HEADER2_SIZE
                            ..(cornflakes_libos::utils::ETHERNET2_HEADER2_SIZE
                                + cornflakes_libos::utils::IPV4_HEADER2_SIZE)],
                        42,
                    )?;
                    cornflakes_libos::utils::write_udp_hdr(
                        &header_info,
                        &mut bytes[(cornflakes_libos::utils::ETHERNET2_HEADER2_SIZE
                            + cornflakes_libos::utils::IPV4_HEADER2_SIZE)
                            ..(cornflakes_libos::utils::ETHERNET2_HEADER2_SIZE
                                + cornflakes_libos::utils::IPV4_HEADER2_SIZE
                                + cornflakes_libos::utils::UDP_HEADER2_SIZE)],
                        42,
                    )?;
                    self.active_connections[i] = Some((addr, bytes));
                    return Ok(i);
                }
                None => {
                    bail!("too many concurrent connections; cannot connect to more");
                }
            }
        }
    }

    fn push_buffers_with_copy(&mut self, pkts: Vec<(MsgID, ConnID, &[u8])>) -> Result<()> {
        unimplemented!();
    }

    fn echo(&mut self, pkts: Vec<ReceivedPkt<Self>>) -> Result<()>
    where
        Self: Sized,
    {
        unimplemented!();
    }

    fn serialize_and_send(
        &mut self,
        _objects: &Vec<(MsgID, ConnID, impl Serializable<Self>)>,
    ) -> Result<()>
    where
        Self: Sized,
    {
        Ok(())
    }

    fn push_rc_sgas(&mut self, rc_sgas: &Vec<(MsgID, ConnID, RcSga<Self>)>) -> Result<()>
    where
        Self: Sized,
    {
        unimplemented!();
    }

    fn push_sgas(&mut self, sgas: &Vec<(MsgID, ConnID, Sga)>) -> Result<()> {
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

    fn timed_out(&self, time_out: Duration) -> Result<Vec<(MsgID, ConnID)>> {
        let mut timed_out: Vec<(MsgID, ConnID)> = Vec::default();
        for ((id, conn_id), start) in self.outgoing_window.iter() {
            if start.elapsed().as_nanos() > time_out.as_nanos() {
                tracing::debug!(elapsed = ?start.elapsed().as_nanos(), id = *id, "Timing out");
                timed_out.push((*id, *conn_id));
            }
        }
        Ok(timed_out)
    }

    fn is_registered(&self, buf: &[u8]) -> bool {
        false
    }

    fn allocate(&mut self, size: usize, alignment: usize) -> Result<Option<Self::DatapathBuffer>> {
        unimplemented!();
    }

    fn get_metadata(&self, buf: Self::DatapathBuffer) -> Result<Option<Self::DatapathMetadata>> {
        Ok(Some(RteMbufMetadata::from_dpdk_buf(buf)))
    }

    fn add_memory_pool(&mut self, value_size: usize, min_elts: usize) -> Result<()> {
        //let num_values = (min_num_values as f64 * 1.20) as usize;
        // find optimal number of values above this
        let name = format!(
            "thread_{}_mempool_id_{}",
            self.thread_context.get_queue_id(),
            self.allocator.num_mempools_so_far()
        );
        tracing::info!(name = name.as_str(), value_size, min_elts, "Adding mempool");
        let (num_values, num_mempools) = {
            let log2 = (min_elts as f64).log2().ceil() as u32;
            let num_elts = usize::pow(2, log2) as usize;
            let num_mempools = {
                if num_elts < MEMPOOL_MAX_SIZE {
                    1
                } else {
                    num_elts / MEMPOOL_MAX_SIZE
                }
            };
            (num_elts / num_mempools, num_mempools)
        };
        tracing::info!("Creating {} mempools of size {}", num_mempools, num_values);
        for i in 0..num_mempools {
            let mempool_name = format!("{}_{}", name, i);
            let mempool = create_mempool(
                &mempool_name,
                1,
                value_size + RTE_PKTMBUF_HEADROOM as usize,
                num_values - 1,
            )
            .wrap_err(format!(
                "Unable to add mempool {:?} to mempool allocator; value_size {}, num_values {}",
                name, value_size, num_values
            ))?;
            tracing::debug!(
                "Created mempool avail count: {}, in_use count: {}",
                unsafe { rte_mempool_avail_count(mempool) },
                unsafe { rte_mempool_in_use_count(mempool) },
            );
            self.allocator
                .add_mempool(mempool, value_size)
                .wrap_err(format!(
                    "Unable to add mempool {:?} to mempool allocator; value_size {}, num_values {}",
                    name, value_size, num_values
                ))?;
        }
        Ok(())
    }

    fn header_size(&self) -> usize {
        cornflakes_libos::utils::TOTAL_HEADER_SIZE
    }

    fn timer_hz(&self) -> u64 {
        unsafe { rte_get_timer_hz() }
    }

    fn cycles_to_ns(&self, t: u64) -> u64 {
        unimplemented!();
    }

    fn current_cycles(&self) -> u64 {
        unsafe { rte_get_timer_cycles() }
    }
}
