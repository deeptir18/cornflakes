use super::{
    super::{
        dpdk_bindings::*,
        dpdk_call, mbuf_slice, mem,
        timing::{record, timefunc, HistogramWrapper},
        utils, CornType, Datapath, MsgID, PtrAttributes, ReceivedPacket, ScatterGather,
    },
    dpdk_utils, wrapper,
};
use color_eyre::eyre::{bail, Result, WrapErr};
use eui48::MacAddress;
use hashbrown::HashMap;
use std::{
    mem::zeroed,
    mem::MaybeUninit,
    net::Ipv4Addr,
    ptr, slice,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tracing::warn;

const MAX_ENTRIES: usize = 60;
const RECEIVE_BURST_SIZE: u16 = 32;
const PROCESSING_TIMER: &str = "E2E_PROCESSING_TIME";
const RX_BURST_TIMER: &str = "RX_BURST_TIMER";
const TX_BURST_TIMER: &str = "TX_BURST_TIMER";
const PKT_CONSTRUCT_TIMER: &str = "PKT_CONSTRUCT_TIMER";
const POP_PROCESSING_TIMER: &str = "POP_PROCESSING_TIMER";
const PUSH_PROCESSING_TIMER: &str = "PUSH_PROCESSING_TIMER";

/// Wrapper around rte_mbuf.
#[derive(Copy, Clone)]
pub struct MbufWrapper {
    /// Mbuf this wraps around.
    pub mbuf: *mut rte_mbuf,
    /// Any networking header size.
    /// For example, could be udp + ethernet + ip header.
    pub header_size: usize,
}

impl AsRef<[u8]> for MbufWrapper {
    fn as_ref(&self) -> &[u8] {
        // length of the payload ends up being pkt_len - header_size
        let payload_len = unsafe { (*self.mbuf).pkt_len as usize - self.header_size as usize };
        mbuf_slice!(self.mbuf, self.header_size, payload_len)
    }
}

impl PtrAttributes for MbufWrapper {
    fn buf_type(&self) -> CornType {
        CornType::Borrowed
    }

    fn buf_size(&self) -> usize {
        unsafe { (*self.mbuf).pkt_len as usize - self.header_size }
    }
}

/// The DPDK datapath returns this on the datapath pop function.
/// Exposes methods around the mbuf.
pub struct DPDKReceivedPkt {
    /// ID of received message.
    id: MsgID,
    // pointer to underlying rte_mbuf
    mbuf_wrapper: MbufWrapper,
    // the address information about the received packet
    addr_info: utils::AddressInfo,
}

impl DPDKReceivedPkt {
    fn new(
        id: MsgID,
        mbuf: *mut rte_mbuf,
        header_size: usize,
        addr_info: utils::AddressInfo,
    ) -> DPDKReceivedPkt {
        DPDKReceivedPkt {
            id: id,
            mbuf_wrapper: MbufWrapper {
                mbuf: mbuf,
                header_size: header_size,
            },
            addr_info: addr_info,
        }
    }
}

/// Implementing drop for DPDKReceivedPkt ensures that the underlying mbuf is freed,
/// once all references to this struct are out of scope.
impl Drop for DPDKReceivedPkt {
    fn drop(&mut self) {
        tracing::debug!("Dropping the mbuf!");
        wrapper::free_mbuf(self.mbuf_wrapper.mbuf);
    }
}

impl ReceivedPacket for DPDKReceivedPkt {
    fn get_addr(&self) -> utils::AddressInfo {
        self.addr_info.clone()
    }
}

/// DPDKReceivedPkt implements ScatterGather so it can be returned by the pop function in the
/// Datapath trait.
impl ScatterGather for DPDKReceivedPkt {
    type Ptr = MbufWrapper;
    type Collection = Option<Self::Ptr>;

    fn get_id(&self) -> MsgID {
        self.id
    }

    fn set_id(&mut self, id: MsgID) {
        self.id = id;
    }

    fn num_segments(&self) -> usize {
        1
    }

    fn num_borrowed_segments(&self) -> usize {
        0
    }

    fn data_len(&self) -> usize {
        self.mbuf_wrapper.buf_size()
    }

    fn collection(&self) -> Self::Collection {
        Some(self.mbuf_wrapper)
    }

    fn iter_apply(&self, mut consume_element: impl FnMut(&Self::Ptr) -> Result<()>) -> Result<()> {
        consume_element(&self.mbuf_wrapper)
    }

    fn contiguous_repr(&self) -> Vec<u8> {
        let mut ret: Vec<u8> = Vec::new();
        ret.extend_from_slice(self.mbuf_wrapper.as_ref());
        ret
    }
}

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum DPDKMode {
    Server,
    Client,
}

impl std::str::FromStr for DPDKMode {
    type Err = color_eyre::eyre::Error;
    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "client" => DPDKMode::Client,
            "server" => DPDKMode::Server,
            x => bail!("Unknown DPDKMode: {:?}", x),
        })
    }
}

pub struct DPDKConnection {
    /// Server or client mode.
    mode: DPDKMode,
    /// dpdk_port
    dpdk_port: u16,
    /// Maps ip addresses to corresponding mac addresses.
    ip_to_mac: HashMap<Ipv4Addr, MacAddress>,
    /// Maps mac addresses to corresponding ip address.
    //mac_to_ip: HashMap<MacAddress, Ipv4Addr>,
    /// Current window of outgoing packets mapped to start time.
    outgoing_window: HashMap<MsgID, Instant>,
    /// Default mempool for allocating mbufs to copy into.
    default_mempool: *mut rte_mempool,
    /// Empty mempool for allocating external buffers.
    extbuf_mempool: *mut rte_mempool,
    /// Header information
    addr_info: utils::AddressInfo,
    /// shinfo: TODO: it is unclear how to ``properly'' use the shinfo.
    /// There might be one shinfo per external memory region.
    /// Here, so far, we're assuming one memory region.
    /// Theoretically should be like HashMap<metadata, shinfo>
    /// And whenever we have a reference -- check which shinfo is the relevant one.
    shared_info: HashMap<mem::MmapMetadata, MaybeUninit<rte_mbuf_ext_shared_info>>,
    /// Debugging timers.
    timers: HashMap<String, Arc<Mutex<HistogramWrapper>>>,
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
    pub fn new(config_file: &str, mode: DPDKMode) -> Result<DPDKConnection> {
        let (ip_to_mac, mac_to_ip, udp_port) = dpdk_utils::parse_yaml_map(config_file).wrap_err(
            "Failed to get ip to mac address mapping, or udp port information from yaml config.",
        )?;
        let outgoing_window: HashMap<MsgID, Instant> = HashMap::new();
        let (mempool, ext_mempool, nb_ports) =
            wrapper::dpdk_init(config_file).wrap_err("Failed to dpdk initialization.")?;

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
            if mode == DPDKMode::Server {
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

        Ok(DPDKConnection {
            mode: mode,
            dpdk_port: nb_ports - 1,
            ip_to_mac: ip_to_mac,
            //mac_to_ip: mac_to_ip,
            outgoing_window: outgoing_window,
            default_mempool: mempool,
            extbuf_mempool: ext_mempool,
            addr_info: addr_info,
            shared_info: HashMap::new(),
            timers: timers,
        })
    }

    pub fn get_timers(&self) -> Vec<Arc<Mutex<HistogramWrapper>>> {
        self.timers.iter().map(|(_, hist)| hist.clone()).collect()
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

    /*fn add_entry(&mut self, timer_name: &str, val: u64) -> Result<()> {
        let mut hist = match self.timers.contains_key(timer_name) {
            true => match self.timers.get(timer_name).unwrap().lock() {
                Ok(h) => h,
                Err(e) => bail!("Failed to unlock hist: {}", e),
            },
            false => {
                bail!("Entry not in timer map: {}", timer_name);
            }
        };
        hist.record(val)?;
        Ok(())
    }*/

    /// Returns a HeaderInfo struct with udp, ethernet and ipv4 header information.
    ///
    /// Arguments:
    /// * dst_addr - Ipv4Addr that is the destination.
    fn get_outgoing_header(&self, dst_addr: Ipv4Addr) -> Result<utils::HeaderInfo> {
        match self.ip_to_mac.get(&dst_addr) {
            Some(mac) => Ok(self.addr_info.get_outgoing(dst_addr, mac.clone())),
            None => {
                bail!("Don't know ethernet address for Ip address: {:?}", dst_addr);
            }
        }
    }
}

impl Datapath for DPDKConnection {
    type ReceivedPkt = DPDKReceivedPkt;
    /// Sends out a cornflake to the given Ipv4Addr.
    /// Returns an error if the address is not present in the ip_to_mac table,
    /// or if there is a problem constructing a linked list of mbufs to copy/attach the cornflake
    /// data to.
    /// Will prepend UDP headers in the first mbuf.
    ///
    /// Arguments:
    /// * sga - reference to a cornflake which contains the scatter-gather array to send
    /// out.
    /// * addr - Ipv4Addr to send the given scatter-gather array to.
    fn push_sga(&mut self, sga: impl ScatterGather, addr: Ipv4Addr) -> Result<()> {
        // check the SGA meets this datapath's allowed sending criteria
        let push_processing_start = Instant::now();
        let push_processing_timer =
            self.get_timer(PUSH_PROCESSING_TIMER, cfg!(feature = "timers"))?;
        if sga.num_borrowed_segments() + 1 > self.max_scatter_entries()
            || sga.data_len() > self.max_packet_len()
        {
            bail!("Sga either has too many scatter-gather entries ( > {} ) or the packet data is too large ( < {} bytes ).", self.max_scatter_entries(), self.max_packet_len());
        }

        // initialize a linked list of mbufs to represent the sga
        let mut pkt = wrapper::Pkt::init(sga.num_borrowed_segments() + 1);
        record(
            push_processing_timer,
            push_processing_start.elapsed().as_nanos() as u64,
        )?;

        let pkt_construct_timer = self.get_timer(PKT_CONSTRUCT_TIMER, cfg!(feature = "timers"))?;
        timefunc(
            &mut || {
                pkt.construct_from_sga(
                    &sga,
                    self.default_mempool,
                    self.extbuf_mempool,
                    &self.get_outgoing_header(addr)?,
                    &mut self.shared_info,
                )
                .wrap_err("Unable to construct pkt from sga.")
            },
            cfg!(feature = "timers"),
            pkt_construct_timer,
        )?;

        // if client, add start time for packet
        match self.mode {
            DPDKMode::Server => {}
            DPDKMode::Client => {
                let _ = self.outgoing_window.insert(sga.get_id(), Instant::now());
            }
        }

        if cfg!(feature = "timers") && self.mode == DPDKMode::Server {
            self.end_entry(PROCESSING_TIMER, sga.get_id(), addr.clone())?;
        }

        // send out the scatter-gather array
        timefunc(
            &mut || {
                wrapper::tx_burst(self.dpdk_port, 0, pkt.mbuf_list_ptr(), 1)
                    .wrap_err(format!("Failed to send SGA with id {}.", sga.get_id()))
            },
            cfg!(feature = "timers"),
            self.get_timer(TX_BURST_TIMER, cfg!(feature = "timers"))?,
        )?;

        Ok(())
    }

    /// Checks to see if any packet has arrived, if any packet is valid.
    /// Feturns a Vec<(DPDKReceivedPkt, Duration)> for each valid packet.
    /// For client mode, provides duration since sending sga with this id.
    /// FOr server mode, returns 0 duration.
    fn pop(&mut self) -> Result<Vec<(Self::ReceivedPkt, Duration)>> {
        let mut mbuf_array: [*mut rte_mbuf; RECEIVE_BURST_SIZE as usize] = unsafe { zeroed() };
        let received = wrapper::rx_burst(
            self.dpdk_port,
            0,
            mbuf_array.as_mut_ptr(),
            RECEIVE_BURST_SIZE,
            &self.addr_info,
        )
        .wrap_err("Error on calling rte_eth_rx_burst.")?;
        let mut ret: Vec<(DPDKReceivedPkt, Duration)> = Vec::new();

        // Debugging end to end processing time
        if cfg!(feature = "timers") && self.mode == DPDKMode::Server {
            for (_, (msg_id, addr_info)) in received.iter() {
                self.start_entry(PROCESSING_TIMER, *msg_id, addr_info.ipv4_addr.clone())?;
            }
        }

        if received.len() > 0 {
            let pop_processing_timer = self.get_timer(
                POP_PROCESSING_TIMER,
                cfg!(feature = "timers") && self.mode == DPDKMode::Server,
            )?;
            let start = Instant::now();
            for (idx, (msg_id, addr_info)) in received.into_iter() {
                let mbuf = mbuf_array[idx];
                if mbuf.is_null() {
                    bail!("Mbuf for index {} in returned array is null.", idx);
                }
                let received_pkt = DPDKReceivedPkt::new(
                    msg_id,
                    mbuf_array[idx],
                    utils::TOTAL_HEADER_SIZE,
                    addr_info,
                );
                let duration = match self.mode {
                    DPDKMode::Client => match self.outgoing_window.remove(&msg_id) {
                        Some(start) => start.elapsed(),
                        None => {
                            warn!("Received packet for an old msg_id: {}", msg_id);
                            continue;
                        }
                    },
                    DPDKMode::Server => Duration::new(0, 0),
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
    fn register_external_region(&mut self, metadata: mem::MmapMetadata) -> Result<()> {
        wrapper::dpdk_register_extmem(&metadata)?;
        let mut shared_info: MaybeUninit<rte_mbuf_ext_shared_info> = MaybeUninit::zeroed();
        unsafe {
            (*shared_info.as_mut_ptr()).refcnt = 1;
            (*shared_info.as_mut_ptr()).fcb_opaque = ptr::null_mut();
            (*shared_info.as_mut_ptr()).free_cb = Some(general_free_cb_);
        }

        self.shared_info.insert(metadata, shared_info);

        Ok(())
    }

    fn unregister_external_region(&mut self, metadata: mem::MmapMetadata) -> Result<()> {
        wrapper::dpdk_unregister_extmem(&metadata)?;
        match self.shared_info.remove(&metadata) {
            Some(_) => Ok(()),
            None => {
                bail!(
                "DPDK datapath doesn't have reference to particular external memory region: {:?}",
                metadata
            );
            }
        }
    }
}

/// When the DPDKConnection goes out of scope,
/// we make sure that the underlying mempools are freed as well.
impl Drop for DPDKConnection {
    fn drop(&mut self) {
        tracing::debug!("DPDK connection is being dropped");
        for (metadata, _) in self.shared_info.iter() {
            match wrapper::dpdk_unregister_extmem(metadata) {
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(metadata = ?metadata, e = ?e, "Error from calling unregister extmem");
                }
            }
        }
        wrapper::free_mempool(self.default_mempool);
        wrapper::free_mempool(self.extbuf_mempool);
    }
}
