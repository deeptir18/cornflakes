use super::{
    super::{
        dpdk_bindings::*, dpdk_call, mbuf_slice, utils, CornType, Datapath, MsgID, PtrAttributes,
        ReceivedPacket, ScatterGather,
    },
    dpdk_utils, wrapper,
};
use color_eyre::eyre::{bail, Result, WrapErr};
use eui48::MacAddress;
use hashbrown::HashMap;
use std::{
    mem::MaybeUninit,
    net::Ipv4Addr,
    ptr, slice,
    time::{Duration, Instant},
};
use tracing::warn;

const MAX_ENTRIES: usize = 60;
const RECEIVE_BURST_SIZE: u16 = 32;

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
        let payload_len = unsafe { (*self.mbuf).pkt_len as usize - self.header_size };
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
}

#[derive(Debug, Clone, PartialEq)]
pub enum DPDKMode {
    Server,
    Client,
}

pub struct DPDKConnection {
    /// Server or client mode.
    mode: DPDKMode,
    /// dpdk_port
    dpdk_port: u16,
    /// Maps ip addresses to corresponding mac addresses.
    ip_to_mac: HashMap<Ipv4Addr, MacAddress>,
    /// Maps mac addresses to corresponding ip address.
    mac_to_ip: HashMap<MacAddress, Ipv4Addr>,
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
    shared_info: *mut rte_mbuf_ext_shared_info,
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
        let mut shared_info: MaybeUninit<rte_mbuf_ext_shared_info> = MaybeUninit::zeroed();
        unsafe {
            (*shared_info.as_mut_ptr()).refcnt = 1;
            (*shared_info.as_mut_ptr()).fcb_opaque = ptr::null_mut();
            (*shared_info.as_mut_ptr()).free_cb = Some(general_free_cb_);
        }

        Ok(DPDKConnection {
            mode: mode,
            dpdk_port: nb_ports - 1,
            ip_to_mac: ip_to_mac,
            mac_to_ip: mac_to_ip,
            outgoing_window: outgoing_window,
            default_mempool: mempool,
            extbuf_mempool: ext_mempool,
            addr_info: addr_info,
            shared_info: shared_info.as_mut_ptr(),
        })
    }

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
        if sga.num_borrowed_segments() + 1 > self.max_scatter_entries()
            || sga.data_len() > self.max_packet_len()
        {
            bail!("Sga either has too many scatter-gather entries ( > {} ) or the packet data is too large ( < {} bytes ).", self.max_scatter_entries(), self.max_packet_len());
        }

        // initialize a linked list of mbufs to represent the sga
        let mut pkt = wrapper::Pkt::init(sga.num_borrowed_segments() + 1);
        pkt.construct_from_sga(
            &sga,
            self.default_mempool,
            self.extbuf_mempool,
            &self.get_outgoing_header(addr)?,
            self.shared_info,
        )
        .wrap_err("Unable to construct pkt from sga.")?;

        // if client, add start time for packet
        match self.mode {
            DPDKMode::Server => {}
            DPDKMode::Client => {
                let _ = self.outgoing_window.insert(sga.get_id(), Instant::now());
            }
        }

        // send out the scatter-gather array
        wrapper::tx_burst(self.dpdk_port, 0, pkt.mbuf_list_ptr(), 1)
            .wrap_err(format!("Failed to send SGA with id {}.", sga.get_id()))?;

        Ok(())
    }

    /// Checks to see if any packet has arrived, if any packet is valid.
    /// Feturns a Vec<(DPDKReceivedPkt, Duration)> for each valid packet.
    /// For client mode, provides duration since sending sga with this id.
    /// FOr server mode, returns 0 duration.
    fn pop(&mut self) -> Result<Vec<(Self::ReceivedPkt, Duration)>> {
        let mut mbuf_array: [*mut rte_mbuf; RECEIVE_BURST_SIZE as usize] =
            [ptr::null_mut(); RECEIVE_BURST_SIZE as usize];
        let received = wrapper::rx_burst(
            self.dpdk_port,
            0,
            mbuf_array.as_mut_ptr(),
            RECEIVE_BURST_SIZE,
            &self.addr_info,
        )
        .wrap_err("Error on calling rte_eth_rx_burst.")?;
        let mut ret: Vec<(DPDKReceivedPkt, Duration)> = Vec::new();
        for (idx, (msg_id, addr_info)) in received.into_iter() {
            // also check if the mac address / ip address ifnromation matches with our expectation
            match self.mac_to_ip.get(&addr_info.ether_addr) {
                Some(ip) => {
                    if *ip != addr_info.ipv4_addr {
                        bail!("Ipv4 addr {:?} does not map with mac addr {:?} in our map, our map has {:?}", addr_info.ipv4_addr, addr_info.ether_addr, ip);
                    }
                }
                None => {
                    bail!(
                        "We don't have information about this ether addr: {:?}",
                        addr_info.ether_addr
                    );
                }
            }
            let mbuf = mbuf_array[idx];
            if mbuf.is_null() {
                bail!("Mbuf for index {} in returned array is null.", idx);
            }
            let received_pkt =
                DPDKReceivedPkt::new(msg_id, mbuf_array[idx], utils::TOTAL_HEADER_SIZE, addr_info);
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
}

/// When the DPDKConnection goes out of scope,
/// we make sure that the underlying mempools are freed as well.
impl Drop for DPDKConnection {
    fn drop(&mut self) {
        wrapper::free_mempool(self.default_mempool);
        wrapper::free_mempool(self.extbuf_mempool);
    }
}
