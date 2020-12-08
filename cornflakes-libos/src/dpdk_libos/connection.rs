use super::{
    super::{dpdk_bindings::*, dpdk_call, utils, CornPtr, Cornflake, Datapath, MsgID},
    wrapper,
};
use color_eyre::eyre::{bail, Result, WrapErr};
use eui48::MacAddress;
use hashbrown::HashMap;
use std::{
    fs::read_to_string,
    mem::MaybeUninit,
    net::Ipv4Addr,
    path::Path,
    ptr,
    str::FromStr,
    time::{Duration, Instant},
};
use tracing::debug;
use yaml_rust::{Yaml, YamlLoader};

const MAX_ENTRIES: usize = 60;
const RECEIVE_BURST_SIZE: u16 = 32;

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
    header_info: utils::HeaderInfo,
    /// shinfo: TODO: it is unclear how to ``properly'' use the shinfo.
    /// There might be one shinfo per external memory region.
    /// Here, so far, we're assuming one memory region.
    shared_info: *mut rte_mbuf_ext_shared_info,
}

fn parse_yaml_map(
    config_file: &str,
) -> Result<(
    HashMap<Ipv4Addr, MacAddress>,
    HashMap<MacAddress, Ipv4Addr>,
    u16,
)> {
    let file_str = read_to_string(Path::new(&config_file))?;
    let yamls = match YamlLoader::load_from_str(&file_str) {
        Ok(docs) => docs,
        Err(e) => {
            bail!("Could not parse config yaml: {:?}", e);
        }
    };

    let yaml = &yamls[0];
    debug!("Yaml: {:?}", yaml);
    let mut ip_to_mac: HashMap<Ipv4Addr, MacAddress> = HashMap::new();
    let mut mac_to_ip: HashMap<MacAddress, Ipv4Addr> = HashMap::new();
    match yaml["lwip"].as_hash() {
        Some(lwip_map) => {
            let known_hosts = match lwip_map.get(&Yaml::from_str("known_hosts")) {
                Some(map) => map.as_hash().unwrap(),
                None => {
                    bail!("Yaml config dpdk has no known_hosts entry");
                }
            };
            for (key, value) in known_hosts.iter() {
                let mac_addr = MacAddress::from_str(key.as_str().unwrap())?;
                let ip_addr = Ipv4Addr::from_str(value.as_str().unwrap())?;
                ip_to_mac.insert(ip_addr, mac_addr);
                mac_to_ip.insert(mac_addr, ip_addr);
            }
        }
        None => {
            bail!("Yaml config dpdk has no lwip entry");
        }
    }

    let udp_port = match yaml.as_hash().unwrap().get(&Yaml::from_str("port")) {
        Some(port_str) => {
            let val = port_str.as_i64().unwrap() as u16;
            val
        }
        None => {
            bail!("Yaml config has no port entry.");
        }
    };

    Ok((ip_to_mac, mac_to_ip, udp_port))
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
        let (ip_to_mac, mac_to_ip, udp_port) = parse_yaml_map(config_file).wrap_err(
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

        let header_info = utils::HeaderInfo::new(udp_port, my_ip_addr, &my_mac_addr);
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
            header_info: header_info,
            shared_info: shared_info.as_mut_ptr(),
        })
    }

    fn get_outgoing_header(&self, dst_addr: Ipv4Addr) -> Result<utils::HeaderInfo> {
        match self.ip_to_mac.get(&dst_addr) {
            Some(mac) => Ok(self.header_info.get_outgoing(dst_addr, mac.clone())),
            None => {
                bail!("Don't know ethernet address for Ip address: {:?}", dst_addr);
            }
        }
    }
}

impl Datapath for DPDKConnection {
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
    fn push_sga(&mut self, sga: &Cornflake, addr: Ipv4Addr) -> Result<()> {
        let pkt_id = sga.get_id();
        // check the SGA meets this datapath's allowed sending criteria
        if sga.num_scattered_entries() > self.max_scatter_entries()
            || sga.data_len() > self.max_packet_len()
        {
            bail!("Sga either has too many scatter-gather entries ( > {} ) or the packet data is too large ( < {} bytes ).", self.max_scatter_entries(), self.max_packet_len());
        }

        // allocate header mbuf and fill in header information
        let mut head_pkt = wrapper::Pkt::new(self.default_mempool)?;
        head_pkt
            .set_header(&self.get_outgoing_header(addr)?, sga)
            .wrap_err("Unable to set header on packet.")?;

        let mut pkts: Vec<wrapper::Pkt> = vec![head_pkt];
        let mut finished_copied_buffers = false;

        // copy in all owned memory regions into the pkt
        for (i, (cornptr, len)) in sga.iter().enumerate() {
            match cornptr {
                CornPtr::Borrowed(buf) => {
                    finished_copied_buffers = true;
                    // need to allocate a new pkt and attach externally
                    let mut pkt = wrapper::Pkt::new(self.extbuf_mempool).wrap_err(format!(
                        "Failed to allocate mbuf for external buffer for sga {}, entry {}.",
                        pkt_id, i
                    ))?;
                    pkt.set_external_payload(buf, *len as u16, self.shared_info)
                        .wrap_err(format!(
                            "Failed to set external payload for sga {}, entry {}.",
                            pkt_id, i
                        ))?;
                    let current_length = pkts.len();
                    let last_packet = &mut pkts[current_length];
                    last_packet.set_next(&pkt);
                    pkts.push(pkt);
                }
                CornPtr::Owned(buf) => {
                    if finished_copied_buffers {
                        bail!("Sga cannot have owned buffers after borrowed buffers.");
                    }
                    let head_pkt = &mut pkts[0];
                    head_pkt.copy_payload(buf.as_ref()).wrap_err(format!(
                        "Failed to copy payload into header mbuf for sga {}, entry {}.",
                        pkt_id, i
                    ))?;
                }
            }
        }

        // set scatter-gather relevant fields in the head packet.
        let pkt = &mut pkts[0];
        pkt.add_pkt_len(sga.data_len() as u32);
        pkt.set_nb_segs(sga.num_scattered_entries() as u16);

        // if client, add start time for packet
        match self.mode {
            DPDKMode::Server => {}
            DPDKMode::Client => {
                let _ = self.outgoing_window.insert(sga.get_id(), Instant::now());
            }
        }

        // send out the scatter-gather array
        let mut head_mbuf = pkt.get_mbuf();
        let mbuf_list = &mut head_mbuf;
        wrapper::tx_burst(self.dpdk_port, 0, mbuf_list, 1)
            .wrap_err(format!("Failed to send SGA with id {}.", sga.get_id()))?;

        Ok(())
    }

    /// Checks to see if any packet has arrived, if any packet is valid.
    /// If anything is valid, returns the received data in a new Cornflake.
    /// For client mode, provides duration since sending sga with this id.
    /// FOr server mode, returns 0 duration.
    fn pop(&mut self) -> Result<Vec<(Cornflake, Duration)>> {
        let mut mbuf_array_raw: [*mut rte_mbuf; RECEIVE_BURST_SIZE as usize] =
            [ptr::null_mut(); RECEIVE_BURST_SIZE as usize];
        let num_received = wrapper::rx_burst(
            self.dpdk_port,
            0,
            mbuf_array_raw.as_mut_ptr(),
            RECEIVE_BURST_SIZE,
        )
        .wrap_err("Error on calling rte_eth_rx_burst.")?;
        let mut ret: Vec<(Cornflake, Duration)> = Vec::new();
        for _i in num_received {}

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

    fn timer_hz(&self) -> u64 {
        dpdk_call!(rte_get_timer_hz())
    }

    fn max_scatter_entries(&self) -> usize {
        return MAX_ENTRIES;
    }

    fn max_packet_len(&self) -> usize {
        return wrapper::RX_PACKET_LEN as usize;
    }
}

impl Drop for DPDKConnection {
    fn drop(&mut self) {
        wrapper::free_mempool(self.default_mempool);
        wrapper::free_mempool(self.extbuf_mempool);
    }
}
