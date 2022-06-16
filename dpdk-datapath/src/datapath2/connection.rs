use super::{
    super::{dpdk_bindings::*, dpdk_call, mbuf_slice2},
    allocator, wrapper,
};
use color_eyre::eyre::{bail, ensure, Result, WrapErr};
use cornflakes_libos::{
    mem,
    timing::{record, timefunc, HistogramWrapper},
    utils, CornType, Datapath, MsgID, PtrAttributes, RcCornflake, ReceivedPkt, RefCnt,
    ScatterGather, USING_REF_COUNTING, allocator::MempoolID,
};
use cornflakes_utils::{parse_yaml_map, AppMode};
use eui48::MacAddress;
use hashbrown::HashMap;
use std::{
    mem::MaybeUninit,
    net::Ipv4Addr,
    ptr, slice,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
    convert::TryInto,
};
use tracing::warn;

#[cfg(feature = "profiler")]
use perftools;

const MAX_ENTRIES: usize = 60;
const PROCESSING_TIMER: &str = "E2E_PROCESSING_TIME";
const RX_BURST_TIMER: &str = "RX_BURST_TIMER";
const TX_BURST_TIMER: &str = "TX_BURST_TIMER";
const PKT_CONSTRUCT_TIMER: &str = "PKT_CONSTRUCT_TIMER";
const POP_PROCESSING_TIMER: &str = "POP_PROCESSING_TIMER";
const PUSH_PROCESSING_TIMER: &str = "PUSH_PROCESSING_TIMER";

#[derive(PartialEq, Eq)]
pub struct DPDKBuffer {
    /// Pointer to allocated mbuf.
    pub mbuf: *mut rte_mbuf,
    /// Actual application data offset (header could be in front)
    pub offset: usize,
    /// Mempool ID that the Buffer is part of
    pub id: MempoolID,
}

impl DPDKBuffer {
    fn new(mbuf: *mut rte_mbuf, data_offset: usize, data_len: Option<usize>, id: MempoolID) -> Self {
        match data_len {
            Some(x) => unsafe {
                (*mbuf).data_len = x as u16;
            },
            None => unsafe {
                (*mbuf).data_len = (*mbuf).buf_len - (*mbuf).data_off;
            },
        }
        dpdk_call!(rte_pktmbuf_refcnt_set(mbuf, 1));
        DPDKBuffer {
            mbuf: mbuf,
            offset: data_offset,
            id: id,
        }
    }

    fn get_mbuf(&self) -> *mut rte_mbuf {
        self.mbuf
    }
}

impl Default for DPDKBuffer {
    fn default() -> Self {
        DPDKBuffer {
            // TODO: might be safest to NOT have this function
            mbuf: ptr::null_mut(),
            offset: 0,
        }
    }
}

impl Drop for DPDKBuffer {
    fn drop(&mut self) {
        // decrement the reference count of the mbuf, or if at 1 or 0, free it
        if unsafe { USING_REF_COUNTING } {
            tracing::debug!(pkt =? self.mbuf, cur_refcnt = dpdk_call!(rte_pktmbuf_refcnt_read(self.mbuf)), "Calling drop on dpdk buffer object");
            wrapper::free_mbuf(self.mbuf);
        }
    }
}

impl Clone for DPDKBuffer {
    fn clone(&self) -> DPDKBuffer {
        if unsafe { USING_REF_COUNTING } {
            tracing::debug!(
                "Cloning mbuf {:?} and increasing ref count to {}",
                self.mbuf,
                wrapper::refcnt(self.mbuf) + 1
            );
            dpdk_call!(rte_pktmbuf_refcnt_update_or_free(self.mbuf, 1));
        }
        DPDKBuffer {
            mbuf: self.mbuf,
            offset: self.offset,
        }
    }
}

impl std::fmt::Debug for DPDKBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Mbuf addr: {:?}, off: {}", self.mbuf, self.offset)
    }
}

impl RefCnt for DPDKBuffer {
    fn inner_offset(&self) -> usize {
        self.offset
    }
    fn free_inner(&mut self) {
        // drop the underlying packet
        dpdk_call!(rte_pktmbuf_free(self.mbuf));
    }
    fn change_rc(&mut self, amt: isize) {
        dpdk_call!(rte_pktmbuf_refcnt_update_or_free(self.mbuf, amt as i16));
    }

    fn count_rc(&self) -> usize {
        dpdk_call!(rte_pktmbuf_refcnt_read(self.mbuf)) as usize
    }
}

impl AsRef<[u8]> for DPDKBuffer {
    fn as_ref(&self) -> &[u8] {
        let data_len = unsafe { (*self.mbuf).data_len } as usize;
        let slice = mbuf_slice2!(self.mbuf, self.offset, data_len - self.offset);
        /*tracing::debug!(
            "Mbuf address: {:?}, slice address: {:?}, data off: {:?}, buf_addr: {:?}",
            self.mbuf,
            slice.as_ptr(),
            unsafe { (*self.mbuf).data_off },
            unsafe { (*self.mbuf).buf_addr }
        );*/
        slice
    }
}

impl AsMut<[u8]> for DPDKBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        let data_len = unsafe { (*self.mbuf).data_len } as usize;
        let slice = mbuf_slice2!(self.mbuf, self.offset, data_len - self.offset);
        /*tracing::debug!(
            "Mbuf address: {:?}, slice address: {:?}, data off: {:?}, buf_addr: {:?}",
            self.mbuf,
            slice.as_ptr(),
            unsafe { (*self.mbuf).data_off },
            unsafe { (*self.mbuf).buf_addr }
        );*/
        slice
    }
}

impl PtrAttributes for DPDKBuffer {
    fn buf_size(&self) -> usize {
        unsafe { (*self.mbuf).data_len as usize }
    }

    fn buf_type(&self) -> CornType {
        CornType::Registered
    }
}

pub struct DPDKConnection {
    /// Queue_id
    queue_id: u16,
    /// Whether to use scatter-gather on send.
    use_scatter_gather: bool,
    /// Server or client mode.
    mode: AppMode,
    /// dpdk_port
    dpdk_port: u16,
    /// Maps ip addresses to corresponding mac addresses.
    ip_to_mac: HashMap<Ipv4Addr, MacAddress>,
    /// Current window of outgoing packets mapped to start time.
    outgoing_window: HashMap<MsgID, Instant>,
    /// Empty mempool for allocating external buffers.
    extbuf_mempool: *mut rte_mempool,
    /// Mempool to copy packets into.
    default_mempool: *mut rte_mempool,
    /// Index of information about mempools
    mempool_allocator: allocator::MempoolAllocator,
    /// Header information
    addr_info: utils::AddressInfo,
    /// Registered memory regions for externally allocated memory
    external_memory_regions: Vec<mem::MmapMetadata>,
    /// Debugging timers.
    timers: HashMap<String, Arc<Mutex<HistogramWrapper>>>,
    /// Mbufs used tx_burst.
    send_mbufs: [[*mut rte_mbuf; wrapper::RECEIVE_BURST_SIZE as usize]; wrapper::MAX_SCATTERS],
    /// Mbufs used for rx_burst.
    recv_mbufs: [*mut rte_mbuf; wrapper::RECEIVE_BURST_SIZE as usize],
    /// For scatter-gather mode debugging: splits per chunk of data
    splits_per_chunk: usize,
    /// Number of mempools
    pub num_mempool_ids: u32,
}

impl DPDKConnection {
    /// Returns a new DPDK connection, or error if there was any problem in initializing and
    /// configuring DPDK.
    /// Also initializes a stub rte_mbuf_ext_shared_info for any external buffers which will be
    /// used to send packets.
    ///
    /// Arguments:
    /// * config_file: String slice representing a path to a config file, in yaml format, that
    /// contains:
    /// (1) A list of mac address and IP addresses in the network.
    /// (2) DPDK rte_eal_init information.
    /// (3) UDP port information for UDP packet headers.
    pub fn new(
        config_file: &str,
        mode: AppMode,
        use_scatter_gather: bool,
    ) -> Result<DPDKConnection> {
        let (ip_to_mac, mac_to_ip, udp_port, _client_port) = parse_yaml_map(config_file).wrap_err(
            "Failed to get ip to mac address mapping, or udp port information from yaml config.",
        )?;
        let outgoing_window: HashMap<MsgID, Instant> = HashMap::new();
        let (mempools, nb_ports) =
            wrapper::dpdk_init(config_file, 1).wrap_err("Failed to dpdk initialization.")?;

        let ext_mempool = wrapper::init_extbuf_mempool("extbuf_mempool", nb_ports)
            .wrap_err("Failed to intialize extbuf mempool")?;
        // TODO: figure out a way to have a "proper" port_id arg
        let my_ether_addr =
            wrapper::get_my_macaddr(nb_ports - 1).wrap_err("Failed to get my own mac address")?;
        let my_mac_addr = MacAddress::from_bytes(&my_ether_addr.addr_bytes)?;
        let my_ip_addr = match mac_to_ip.get(&my_mac_addr) {
            Some(ip) => ip,
            None => {
                bail!(
                    "Not able to find ip addr for my mac addr {:?} in map",
                    my_mac_addr.to_hex_string()
                );
            }
        };

        let addr_info = utils::AddressInfo::new(udp_port, *my_ip_addr, my_mac_addr);

        // initialize any debugging histograms
        // process::exit,
        let mut timers: HashMap<String, Arc<Mutex<HistogramWrapper>>> = HashMap::default();
        if cfg!(feature = "timers") {
            if mode == AppMode::Server {
                timers.insert(
                    PROCESSING_TIMER.to_string(),
                    Arc::new(Mutex::new(HistogramWrapper::new(PROCESSING_TIMER)?)),
                );
                timers.insert(
                    POP_PROCESSING_TIMER.to_string(),
                    Arc::new(Mutex::new(HistogramWrapper::new(POP_PROCESSING_TIMER)?)),
                );
            }
            timers.insert(
                RX_BURST_TIMER.to_string(),
                Arc::new(Mutex::new(HistogramWrapper::new(RX_BURST_TIMER)?)),
            );
            timers.insert(
                PKT_CONSTRUCT_TIMER.to_string(),
                Arc::new(Mutex::new(HistogramWrapper::new(PKT_CONSTRUCT_TIMER)?)),
            );
            timers.insert(
                TX_BURST_TIMER.to_string(),
                Arc::new(Mutex::new(HistogramWrapper::new(TX_BURST_TIMER)?)),
            );
            timers.insert(
                PUSH_PROCESSING_TIMER.to_string(),
                Arc::new(Mutex::new(HistogramWrapper::new(PUSH_PROCESSING_TIMER)?)),
            );
        }

        let mut mempool_allocator = allocator::MempoolAllocator::default();
        let mut num_mempools : u32 = 0;
        for mempool in mempools.iter() {
            mempool_allocator
                .add_mempool(*mempool, wrapper::MBUF_BUF_SIZE as _, num_mempools.clone())
                .wrap_err("Failed to add DPDK default mempool to mempool allocator")?;
            num_mempools += 1;
            tracing::info!("Number of mempools before connection finalizes: {:?}", num_mempools);
        }
        tracing::info!("Number of mempools before connection finalizes: {:?}", num_mempools);
        tracing::debug!("Use scatter_gather: {}", use_scatter_gather);
        Ok(DPDKConnection {
            queue_id: 0,
            use_scatter_gather: use_scatter_gather,
            mode: mode,
            dpdk_port: nb_ports - 1,
            ip_to_mac: ip_to_mac,
            outgoing_window: outgoing_window,
            external_memory_regions: Vec::default(),
            extbuf_mempool: ext_mempool,
            default_mempool: mempools[0],
            mempool_allocator: mempool_allocator,
            addr_info: addr_info,
            timers: timers,
            send_mbufs: [[ptr::null_mut(); wrapper::RECEIVE_BURST_SIZE as usize];
                wrapper::MAX_SCATTERS],
            recv_mbufs: [ptr::null_mut(); wrapper::RECEIVE_BURST_SIZE as usize],
            splits_per_chunk: 1,
            num_mempool_ids: num_mempools,
        })
    }

    pub fn set_splits_per_chunk(&mut self, size: usize) -> Result<()> {
        ensure!(
            size <= wrapper::MAX_SCATTERS,
            format!("Splits per chunk cannot exceed {}", wrapper::MAX_SCATTERS)
        );
        self.splits_per_chunk = size;
        Ok(())
    }

    fn get_timer(
        &self,
        timer_name: &str,
        cond: bool,
    ) -> Result<Option<Arc<Mutex<HistogramWrapper>>>> {
        if !cond {
            return Ok(None);
        }
        match self.timers.get(timer_name) {
            Some(h) => Ok(Some(h.clone())),
            None => bail!("Failed to find timer {}", timer_name),
        }
    }

    fn start_entry(&mut self, timer_name: &str, id: MsgID, src: Ipv4Addr) -> Result<()> {
        let mut hist = match self.timers.contains_key(timer_name) {
            true => match self.timers.get(timer_name).unwrap().lock() {
                Ok(h) => h,
                Err(e) => bail!("Failed to unlock hist: {}", e),
            },
            false => {
                bail!("Entry not in timer map: {}", timer_name);
            }
        };
        hist.start_entry(src, id)?;
        Ok(())
    }

    fn end_entry(&mut self, timer_name: &str, id: MsgID, dst: Ipv4Addr) -> Result<()> {
        let mut hist = match self.timers.contains_key(timer_name) {
            true => match self.timers.get(timer_name).unwrap().lock() {
                Ok(h) => h,
                Err(e) => bail!("Failed to unlock hist: {}", e),
            },
            false => {
                bail!("Entry not in timer map: {}", timer_name);
            }
        };
        hist.end_entry(dst, id)?;
        Ok(())
    }

    fn get_outgoing_header(&self, dst_addr: &utils::AddressInfo) -> utils::HeaderInfo {
        self.addr_info.get_outgoing(dst_addr)
    }

    pub fn add_mempool(
        &mut self,
        name: &str,
        value_size: usize,
        min_num_values: usize,
    ) -> Result<()> {
        //let num_values = (min_num_values as f64 * 1.20) as usize;
        // find optimal number of values above this
        tracing::info!(name, value_size, min_num_values, "Adding mempool");
        let (num_values, num_mempools) = {
            let log2 = (min_num_values as f64).log2().ceil() as u32;
            let num_elts = usize::pow(2, log2) as usize;
            let num_mempools = {
                if num_elts < wrapper::MEMPOOL_MAX_SIZE {
                    1
                } else {
                    num_elts / wrapper::MEMPOOL_MAX_SIZE
                }
            };
            (num_elts / num_mempools, num_mempools)
        };
        tracing::info!("Number of mempools: {:?}", num_mempools);
        tracing::info!("Creating {} mempools of size {}", num_mempools, num_values);
        for i in 0..num_mempools {
            let mempool_name = format!("{}_{}", name, i);
            tracing::info!("Trying to add mempool {} to thingy", mempool_name);
            let mempool = wrapper::create_mempool(
                &mempool_name,
                1,
                value_size + super::dpdk_bindings::RTE_PKTMBUF_HEADROOM as usize,
                num_values - 1,
            )
            .wrap_err(format!(
                "Unable to add mempool {:?} to mempool allocator; value_size {}, num_values {}",
                name, value_size, num_values
            ))?;
            tracing::debug!(
                "Created mempool avail count: {}, in_use count: {}",
                dpdk_call!(rte_mempool_avail_count(mempool)),
                dpdk_call!(rte_mempool_in_use_count(mempool)),
            );
            self.mempool_allocator
                .add_mempool(mempool, value_size, i.clone().try_into().unwrap())
                .wrap_err(format!(
                    "Unable to add mempool {:?} to mempool allocator; value_size {}, num_values {}",
                    name, value_size, num_values
                ))?;
        }
        Ok(())
    } 
}

fn init_timers(mode: AppMode) -> Result<HashMap<String, Arc<Mutex<HistogramWrapper>>>> {
    let mut timers: HashMap<String, Arc<Mutex<HistogramWrapper>>> = HashMap::default();
    if cfg!(feature = "timers") {
        if mode == AppMode::Server {
            timers.insert(
                PROCESSING_TIMER.to_string(),
                Arc::new(Mutex::new(HistogramWrapper::new(PROCESSING_TIMER)?)),
            );
            timers.insert(
                POP_PROCESSING_TIMER.to_string(),
                Arc::new(Mutex::new(HistogramWrapper::new(POP_PROCESSING_TIMER)?)),
            );
        }
        timers.insert(
            RX_BURST_TIMER.to_string(),
            Arc::new(Mutex::new(HistogramWrapper::new(RX_BURST_TIMER)?)),
        );
        timers.insert(
            PKT_CONSTRUCT_TIMER.to_string(),
            Arc::new(Mutex::new(HistogramWrapper::new(PKT_CONSTRUCT_TIMER)?)),
        );
        timers.insert(
            TX_BURST_TIMER.to_string(),
            Arc::new(Mutex::new(HistogramWrapper::new(TX_BURST_TIMER)?)),
        );
        timers.insert(
            PUSH_PROCESSING_TIMER.to_string(),
            Arc::new(Mutex::new(HistogramWrapper::new(PUSH_PROCESSING_TIMER)?)),
        );
    }

    Ok(timers)
}

fn _get_ether_addr(mac: &MacAddress) -> MaybeUninit<rte_ether_addr> {
    let eth_array = mac.to_array();
    let mut server_eth_uninit: MaybeUninit<rte_ether_addr> = MaybeUninit::zeroed();
    unsafe {
        for i in 0..eth_array.len() {
            (*server_eth_uninit.as_mut_ptr()).addr_bytes[i] = eth_array[i];
        }
    }
    server_eth_uninit
}

impl Datapath for DPDKConnection {
    type DatapathPkt = DPDKBuffer;
    type RxPacketAllocator = wrapper::MempoolPtr;

    fn global_init(
        config_path: &str,
        num_cores: usize,
        app_mode: AppMode,
        remote_ip: Option<Ipv4Addr>,
    ) -> Result<(u16, Vec<(Self::RxPacketAllocator, utils::AddressInfo)>)> {
        let (mempools, nb_ports) =
            wrapper::dpdk_init(config_path, num_cores).wrap_err("Failure in dpdk init.")?;
        // TODO: only works when there is one physical queue
        let dpdk_port = nb_ports - 1;

        let (_ip_to_mac, mac_to_ip, server_port, client_port) = parse_yaml_map(config_path)
            .wrap_err(
            "Failed to get ip to mac address mapping, or udp port information from yaml config.",
        )?;

        // retrieve our own base IP address

        // what is my ethernet address (rte_ether_addr struct)
        let my_eth = wrapper::get_my_macaddr(dpdk_port)?;
        let my_mac = MacAddress::from_bytes(&my_eth.addr_bytes)?;
        let my_ip_addr = mac_to_ip.get(&my_mac).unwrap();

        // on server side, just return single mempool and address info
        if app_mode == AppMode::Server {
            ensure!(
                mempools.len() == 1,
                "Currently cannot spawn server with more than one core"
            );

            let addr_info = utils::AddressInfo::new(server_port, *my_ip_addr, my_mac);

            // add a flow control rule on the server side
            // what is their ethernet_addr (should be an rte_ether_addr struct)
            /*let mut server_eth = get_ether_addr(ip_to_mac.get(my_ip_addr).unwrap());
            let server_ip = dpdk_call!(make_ip(
                my_ip_addr.octets()[0],
                my_ip_addr.octets()[1],
                my_ip_addr.octets()[2],
                my_ip_addr.octets()[3]
            ));
            dpdk_call!(add_flow_rule(
                dpdk_port,
                server_eth.as_mut_ptr(),
                server_ip,
                server_port,
                0
            ));*/

            return Ok((
                dpdk_port,
                vec![(wrapper::MempoolPtr(mempools[0]), addr_info)],
            ));
        }

        // on client side, the server ip MUST be specified
        let server_ip = match remote_ip {
            Some(s) => s,
            None => {
                bail!("For client app mode, remote server IP must be specified.");
            }
        };

        let mut recv_addrs: Vec<utils::AddressInfo> = Vec::with_capacity(num_cores);
        let in_recv_addrs = |ip: &[u8; 4], ref_addrs: &Vec<utils::AddressInfo>| -> bool {
            for current_addr in ref_addrs.iter() {
                if current_addr.ipv4_addr.octets() == *ip {
                    return true;
                }
            }
            return false;
        };

        for queue_id in 0..num_cores as u16 {
            let mut cur_octets = my_ip_addr.octets();
            let cur_port = client_port;
            while dpdk_call!(compute_flow_affinity(
                ip_from_octets(&cur_octets),
                ip_from_octets(&server_ip.octets()),
                cur_port,
                server_port,
                num_cores
            )) != queue_id as u32
                || (in_recv_addrs(&cur_octets, &recv_addrs))
            {
                cur_octets[3] += 1;
            }
            tracing::info!(queue_id = queue_id, octets = ?cur_octets, port = cur_port, "Chosen addr pair");
            let queue_addr_info = utils::AddressInfo::new(
                cur_port,
                Ipv4Addr::new(cur_octets[0], cur_octets[1], cur_octets[2], cur_octets[3]),
                my_mac,
            );
            recv_addrs.push(queue_addr_info);
        }

        let mut recv_pairs: Vec<(Self::RxPacketAllocator, utils::AddressInfo)> = Vec::default();
        for i in 0..num_cores {
            recv_pairs.push((wrapper::MempoolPtr(mempools[i]), recv_addrs[i].clone()));
        }

        Ok((dpdk_port, recv_pairs))
    }

    fn per_thread_init(
        physical_port: u16,
        config_file: &str,
        app_mode: AppMode,
        use_scatter_gather: bool,
        queue_id: usize,
        rx_allocator: Self::RxPacketAllocator,
        addr_info: utils::AddressInfo,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        let (ip_to_mac, _mac_to_ip, _server_port, _client_port) = parse_yaml_map(config_file)
            .wrap_err(
            "Failed to get ip to mac address mapping, or udp port information from yaml config.",
        )?;

        let ext_mempool = match use_scatter_gather {
            true => wrapper::init_extbuf_mempool(&format!("extbuf_mempool_{}", queue_id), 1)
                .wrap_err("Failed to initialize extbuf mempool")?,
            false => ptr::null_mut(),
        };

        let timers = init_timers(app_mode).wrap_err("Failed to initialize application timers")?;

        let mut mempool_allocator = allocator::MempoolAllocator::default();
        mempool_allocator
            .add_mempool(rx_allocator.0, wrapper::MBUF_BUF_SIZE as _, 0)
            .wrap_err("Failed to add default mempool to mempool allocator.")?;

        Ok(DPDKConnection {
            queue_id: queue_id as u16,
            use_scatter_gather: use_scatter_gather,
            mode: app_mode,
            dpdk_port: physical_port,
            ip_to_mac: ip_to_mac,
            outgoing_window: HashMap::default(),
            external_memory_regions: Vec::default(),
            extbuf_mempool: ext_mempool,
            default_mempool: rx_allocator.0,
            mempool_allocator: mempool_allocator,
            addr_info: addr_info,
            timers: timers,
            send_mbufs: [[ptr::null_mut(); wrapper::RECEIVE_BURST_SIZE as usize];
                wrapper::MAX_SCATTERS],
            recv_mbufs: [ptr::null_mut(); wrapper::RECEIVE_BURST_SIZE as usize],
            splits_per_chunk: 1,
            num_mempool_ids: 1,
        })
    }

    /// Sends a single buffer to the given address.
    fn push_buf(&mut self, buf: (MsgID, &[u8]), addr: utils::AddressInfo) -> Result<()> {
        let header = self.get_outgoing_header(&addr);
        self.send_mbufs[0][0] =
            wrapper::get_mbuf_with_memcpy(self.default_mempool, &header, buf.1, buf.0)?;

        // if client, add start time for packet
        // if server, end packet processing counter
        match self.mode {
            AppMode::Server => {
                if cfg!(feature = "timers") {
                    self.end_entry(PROCESSING_TIMER, buf.0, addr.ipv4_addr)?;
                }
            }
            AppMode::Client => {
                // only insert new time IF this packet has not already been sent
                if !self.outgoing_window.contains_key(&buf.0) {
                    let _ = self.outgoing_window.insert(buf.0, Instant::now());
                }
            }
        }

        // send out the scatter-gather array
        let mbuf_ptr = &mut self.send_mbufs[0][0] as _;
        timefunc(
            &mut || {
                wrapper::tx_burst(self.dpdk_port, self.queue_id, mbuf_ptr, 1)
                    .wrap_err(format!("Failed to send SGAs."))
            },
            cfg!(feature = "timers"),
            self.get_timer(TX_BURST_TIMER, cfg!(feature = "timers"))?,
        )?;

        Ok(())
    }

    fn echo(&mut self, pkts: Vec<ReceivedPkt<Self>>) -> Result<()>
    where
        Self: Sized,
    {
        let ct = pkts.len();
        for (i, pkt) in pkts.iter().enumerate() {
            // temporary: just free pkt
            // need to obtain ptr to original mbuf
            // TODO: assumes that received packet only has one pointer
            let dpdk_buffer = pkt.index(0);
            let mbuf = dpdk_buffer.get_mbuf();
            tracing::debug!(
                "Echoing packet with id {}, mbuf addr {:?}, refcnt {}",
                pkt.get_id(),
                mbuf,
                wrapper::refcnt(mbuf)
            );
            // increment ref count so that it does not get dropped here:
            // OTHERWISE THERE IS A RACE
            // CONDITION
            dpdk_call!(flip_headers(mbuf, pkt.get_id()));
            dpdk_call!(rte_pktmbuf_refcnt_update_or_free(mbuf, 1));
            self.send_mbufs[0][i] = mbuf;
        }
        // send out the scatter-gather array
        let mbuf_ptr = &mut self.send_mbufs[0][0] as _;
        timefunc(
            &mut || {
                wrapper::tx_burst(self.dpdk_port, self.queue_id, mbuf_ptr, ct as u16)
                    .wrap_err(format!("Failed to send SGAs."))
            },
            cfg!(feature = "timers"),
            self.get_timer(TX_BURST_TIMER, cfg!(feature = "timers"))?,
        )?;
        Ok(())
    }

    fn push_rc_sgas(&mut self, sgas: &Vec<(RcCornflake<Self>, utils::AddressInfo)>) -> Result<()>
    where
        Self: Sized,
    {
        let push_processing_start = Instant::now();
        let push_processing_timer =
            self.get_timer(PUSH_PROCESSING_TIMER, cfg!(feature = "timers"))?;
        record(
            push_processing_timer,
            push_processing_start.elapsed().as_nanos() as u64,
        )?;
        let mut pkts: Vec<wrapper::Pkt> =
            sgas.iter().map(|(_sga, _)| wrapper::Pkt::init()).collect();

        let headers: Vec<utils::HeaderInfo> = sgas
            .iter()
            .map(|(_, addr)| self.get_outgoing_header(addr))
            .collect();
        let pkt_construct_timer = self.get_timer(PKT_CONSTRUCT_TIMER, cfg!(feature = "timers"))?;
        let use_scatter_gather = self.use_scatter_gather;
        timefunc(
            &mut || {
                for (i, (((ref sga, _), ref header), ref mut pkt)) in sgas
                    .iter()
                    .zip(headers.iter())
                    .zip(pkts.iter_mut())
                    .enumerate()
                {
                    if use_scatter_gather {
                        pkt.construct_from_sga(
                            &mut self.send_mbufs,
                            i,
                            sga,
                            self.default_mempool,
                            self.extbuf_mempool,
                            header,
                            &self.mempool_allocator,
                            &self.external_memory_regions,
                            self.splits_per_chunk,
                        )
                        .wrap_err(format!(
                            "Unable to construct pkt from sga with scatter-gather, sga idx: {}",
                            sga.get_id()
                        ))?;
                    } else {
                        pkt.construct_from_sga_without_scatter_gather(
                            &mut self.send_mbufs,
                            i,
                            sga,
                            self.default_mempool,
                            header,
                        )
                        .wrap_err(format!(
                            "Unable to construct pkt from sga without scatter-gather, sga idx: {}",
                            sga.get_id()
                        ))?;
                    }
                }
                Ok(())
            },
            cfg!(feature = "timers"),
            pkt_construct_timer,
        )?;
        tracing::debug!("Constructed packet.");

        // if client, add start time for packet
        // if server, end packet processing counter
        match self.mode {
            AppMode::Server => {
                if cfg!(feature = "timers") {
                    for (sga, addr) in sgas.iter() {
                        self.end_entry(PROCESSING_TIMER, sga.get_id(), addr.ipv4_addr)?;
                    }
                }
            }
            AppMode::Client => {
                for (sga, _) in sgas.iter() {
                    // only insert new time IF this packet has not already been sent
                    if !self.outgoing_window.contains_key(&sga.get_id()) {
                        let _ = self.outgoing_window.insert(sga.get_id(), Instant::now());
                    }
                }
            }
        }

        // send out the scatter-gather array
        let mbuf_ptr = &mut self.send_mbufs[0][0] as _;
        timefunc(
            &mut || {
                wrapper::tx_burst(self.dpdk_port, self.queue_id, mbuf_ptr, sgas.len() as u16)
                    .wrap_err(format!("Failed to send SGAs."))
            },
            cfg!(feature = "timers"),
            self.get_timer(TX_BURST_TIMER, cfg!(feature = "timers"))?,
        )?;

        Ok(())
    }
    /// Sends out a cornflake to the given Ipv4Addr.
    /// Returns an error if the address is not present in the ip_to_mac table,
    /// or if there is a problem constructing a linked list of mbufs to copy/attach the cornflake
    /// data to.
    ///
    /// Arguments:
    /// * sga - reference to a cornflake which contains the scatter-gather array to send
    /// out.
    /// * addr - Ipv4Addr to send the given scatter-gather array to.
    fn push_sgas(&mut self, sgas: &Vec<(impl ScatterGather, utils::AddressInfo)>) -> Result<()> {
        #[cfg(feature = "profiler")]
        perftools::timer!("Push sgas func");
        let push_processing_start = Instant::now();
        let push_processing_timer =
            self.get_timer(PUSH_PROCESSING_TIMER, cfg!(feature = "timers"))?;
        record(
            push_processing_timer,
            push_processing_start.elapsed().as_nanos() as u64,
        )?;
        let mut pkts: Vec<wrapper::Pkt> =
            sgas.iter().map(|(_sga, _)| wrapper::Pkt::init()).collect();

        let headers: Vec<utils::HeaderInfo> = sgas
            .iter()
            .map(|(_, addr)| self.get_outgoing_header(addr))
            .collect();
        let pkt_construct_timer = self.get_timer(PKT_CONSTRUCT_TIMER, cfg!(feature = "timers"))?;
        let use_scatter_gather = self.use_scatter_gather;
        timefunc(
            &mut || {
                for (i, (((ref sga, _), ref header), ref mut pkt)) in sgas
                    .iter()
                    .zip(headers.iter())
                    .zip(pkts.iter_mut())
                    .enumerate()
                {
                    #[cfg(feature = "profiler")]
                    perftools::timer!("Construct from sga");
                    if use_scatter_gather {
                        pkt.construct_from_sga(
                            &mut self.send_mbufs,
                            i,
                            sga,
                            self.default_mempool,
                            self.extbuf_mempool,
                            header,
                            &self.mempool_allocator,
                            &self.external_memory_regions,
                            self.splits_per_chunk,
                        )
                        .wrap_err(format!(
                            "Unable to construct pkt from sga with scatter-gather, sga idx: {}",
                            sga.get_id()
                        ))?;
                    } else {
                        #[cfg(feature = "profiler")]
                        perftools::timer!("Construct from sga without scatter-gather");
                        pkt.construct_from_sga_without_scatter_gather(
                            &mut self.send_mbufs,
                            i,
                            sga,
                            self.default_mempool,
                            header,
                        )
                        .wrap_err(format!(
                            "Unable to construct pkt from sga without scatter-gather, sga idx: {}",
                            sga.get_id()
                        ))?;
                    }
                }
                Ok(())
            },
            cfg!(feature = "timers"),
            pkt_construct_timer,
        )?;
        tracing::debug!("Constructed packet.");

        // if client, add start time for packet
        // if server, end packet processing counter
        match self.mode {
            AppMode::Server => {
                if cfg!(feature = "timers") {
                    for (sga, addr) in sgas.iter() {
                        self.end_entry(PROCESSING_TIMER, sga.get_id(), addr.ipv4_addr)?;
                    }
                }
            }
            AppMode::Client => {
                for (sga, _) in sgas.iter() {
                    // only insert new time IF this packet has not already been sent
                    if !self.outgoing_window.contains_key(&sga.get_id()) {
                        let _ = self.outgoing_window.insert(sga.get_id(), Instant::now());
                    }
                }
            }
        }

        // send out the scatter-gather array
        {
            #[cfg(feature = "profiler")]
            perftools::timer!("dpdk processing");
            let mbuf_ptr = &mut self.send_mbufs[0][0] as _;
            timefunc(
                &mut || {
                    wrapper::tx_burst(self.dpdk_port, self.queue_id, mbuf_ptr, sgas.len() as u16)
                        .wrap_err(format!("Failed to send SGAs."))
                },
                cfg!(feature = "timers"),
                self.get_timer(TX_BURST_TIMER, cfg!(feature = "timers"))?,
            )?;
        }

        Ok(())
    }

    /// Checks to see if any packet has arrived, if any packet is valid.
    /// Feturns a Vec<(DPDKReceivedPkt, Duration)> for each valid packet.
    /// For client mode, provides duration since sending sga with this id.
    /// FOr server mode, returns 0 duration.
    fn pop(&mut self) -> Result<Vec<(ReceivedPkt<Self>, Duration)>> {
        let received = wrapper::rx_burst(
            self.dpdk_port,
            self.queue_id,
            self.recv_mbufs.as_mut_ptr(),
            wrapper::RECEIVE_BURST_SIZE,
            &self.addr_info,
        )
        .wrap_err("Error on calling rte_eth_rx_burst.")?;
        let mut ret: Vec<(ReceivedPkt<Self>, Duration)> = Vec::new();

        // Debugging end to end processing time
        if cfg!(feature = "timers") && self.mode == AppMode::Server {
            for (_, (msg_id, addr_info, _data_len)) in received.iter() {
                self.start_entry(PROCESSING_TIMER, *msg_id, addr_info.ipv4_addr.clone())?;
            }
        }

        if received.len() > 0 {
            tracing::debug!(queue_id = self.queue_id, "Received some packs");
            let pop_processing_timer = self.get_timer(
                POP_PROCESSING_TIMER,
                cfg!(feature = "timers") && self.mode == AppMode::Server,
            )?;

            let start = Instant::now();
            for (idx, (msg_id, addr_info, _data_len)) in received.iter() {
                let mbuf = self.recv_mbufs[*idx];
                if mbuf.is_null() {
                    bail!("Mbuf for index {} in returned array is null.", idx);
                }
                let received_buffer = vec![DPDKBuffer::new(
                    mbuf,
                    utils::TOTAL_HEADER_SIZE,
                    Some(unsafe { (*mbuf).data_len as usize }),
                )];

                let received_pkt = ReceivedPkt::new(received_buffer, *msg_id, addr_info.clone());

                let duration = match self.mode {
                    AppMode::Client => match self.outgoing_window.remove(msg_id) {
                        Some(start) => start.elapsed(),
                        None => {
                            warn!("Received packet for an old msg_id: {}", *msg_id);
                            continue;
                        }
                    },
                    AppMode::Server => Duration::new(0, 0),
                };
                ret.push((received_pkt, duration));
            }
            record(pop_processing_timer, start.elapsed().as_nanos() as u64)?;
        }
        Ok(ret)
    }

    /// Checks if any outstanding Cornflake has timed out.
    /// Returns a vector with the IDs of any timed-out Cornflakes.
    ///
    /// Arguments:
    /// * time_out - std::time::Duration that represents the timeout period to check for.
    fn timed_out(&self, time_out: Duration) -> Result<Vec<MsgID>> {
        let mut timed_out: Vec<MsgID> = Vec::new();
        for (id, start) in self.outgoing_window.iter() {
            if start.elapsed().as_nanos() > time_out.as_nanos() {
                tracing::debug!(elapsed = ?start.elapsed().as_nanos(), id = *id, "Timing out");
                timed_out.push(*id);
            }
        }
        Ok(timed_out)
    }

    /// Returns the current cycles since boot.
    /// Use rte_get_timer_hz() to know the number of cycles per second.
    fn current_cycles(&self) -> u64 {
        dpdk_call!(rte_get_timer_cycles())
    }

    /// Number of cycles per second.
    /// Can ve used in conjunction with `current_cycles` for time.
    fn timer_hz(&self) -> u64 {
        dpdk_call!(rte_get_timer_hz())
    }

    /// The maximum number of scattered segments that this datapath supports.
    fn max_scatter_entries(&self) -> usize {
        return MAX_ENTRIES;
    }

    /// Maxmimum packet length this datapath supports.
    /// We do not yet support sending payloads larger than an MTU.
    fn max_packet_len(&self) -> usize {
        return wrapper::RX_PACKET_LEN as usize;
    }

    /// Registers this external piece of memory with DPDK,
    /// so regions of this memory can be used while sending external mbufs.
    fn register_external_region(&mut self, metadata: &mut mem::MmapMetadata) -> Result<()> {
        let mut lkey_out: u32 = 0;
        let ibv_mr = wrapper::dpdk_register_extmem(&metadata, &mut lkey_out as *mut u32)?;
        metadata.set_lkey(lkey_out);
        metadata.set_ibv_mr(ibv_mr);
        self.external_memory_regions.push(metadata.clone());
        Ok(())
    }

    fn unregister_external_region(&mut self, metadata: &mem::MmapMetadata) -> Result<()> {
        let mut idx_to_remove = 0;
        let mut found = false;
        for (idx, meta) in self.external_memory_regions.iter().enumerate() {
            if meta.ptr == metadata.ptr && meta.length == metadata.length {
                idx_to_remove = idx;
                found = true;
                break;
            }
        }
        if !found {
            bail!("Could not find external memory region to remove.");
        }
        let metadata = self.external_memory_regions.remove(idx_to_remove);
        wrapper::dpdk_unregister_extmem(&metadata)?;
        Ok(())
    }

    fn get_timers(&self) -> Vec<Arc<Mutex<HistogramWrapper>>> {
        self.timers.iter().map(|(_, hist)| hist.clone()).collect()
    }

    /// Returns a HeaderInfo struct with udp, ethernet and ipv4 header information.
    ///
    /// Arguments:
    /// * dst_addr - Ipv4Addr that is the destination.
    ///
    /// Returns:
    ///  * AddressInfo - struct with destination mac, ip address and udp port
    fn get_outgoing_addr_from_ip(
        &self,
        dst_addr: &Ipv4Addr,
        port: u16,
    ) -> Result<utils::AddressInfo> {
        match self.ip_to_mac.get(dst_addr) {
            Some(mac) => Ok(utils::AddressInfo::new(port, dst_addr.clone(), *mac)),
            None => {
                bail!("Don't know ethernet address for Ip address: {:?}", dst_addr);
            }
        }
    }

    fn get_header_size(&self) -> usize {
        utils::TOTAL_HEADER_SIZE
    }

    fn allocate_tx(&self, len: usize) -> Result<Self::DatapathPkt> {
        let mbuf = wrapper::alloc_mbuf(self.default_mempool)
            .wrap_err("Not able to allocate tx mbuf from default mempool")?;
        return Ok(DPDKBuffer::new(mbuf, 0, Some(len)));
    }

    fn allocate(&self, size: usize, align: usize) -> Result<Self::DatapathPkt> {
        let mbuf = self
            .mempool_allocator
            .allocate(size, align)
            .wrap_err(format!(
                "Not able to allocate packet of size {:?} and alignment {:?} from allocator",
                size, align
            ))?;
        return Ok(DPDKBuffer::new(mbuf, 0, None));
    }
}

/// When the DPDKConnection goes out of scope,
/// we make sure that the underlying mempools are freed as well.
impl Drop for DPDKConnection {
    fn drop(&mut self) {
        tracing::debug!("DPDK connection is being dropped");
        for metadata in self.external_memory_regions.iter() {
            match wrapper::dpdk_unregister_extmem(metadata) {
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(metadata = ?metadata, e = ?e, "Error from calling unregister extmem");
                }
            }
        }
        // rest of mempools are freed when the Mempool allocator is dropped
        wrapper::free_mempool(self.extbuf_mempool);
    }
}
