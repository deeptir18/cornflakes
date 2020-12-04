use super::{
    super::{dpdk_bindings::*, dpdk_call, utils, Cornflake, Datapath, MsgID},
    wrapper,
};
use color_eyre::eyre::{bail, Result, WrapErr};
use eui48::MacAddress;
use hashbrown::HashMap;
use std::{
    fs::read_to_string,
    net::Ipv4Addr,
    path::Path,
    str::FromStr,
    time::{Duration, Instant},
};
use tracing::debug;
use yaml_rust::{Yaml, YamlLoader};

#[derive(Debug, Clone, PartialEq)]
pub enum DPDKMode {
    Server,
    Client,
}

pub struct DPDKConnection {
    /// Server or client mode.
    mode: DPDKMode,
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
    pub fn new(config_file: &str, mode: DPDKMode) -> Result<DPDKConnection> {
        let (ip_to_mac, mac_to_ip, udp_port) = parse_yaml_map(config_file).wrap_err(
            "Failed to get ip to mac address mapping, or udp port information from yaml config.",
        )?;
        let outgoing_window: HashMap<MsgID, Instant> = HashMap::new();
        let (mempool, ext_mempool) =
            wrapper::dpdk_init(config_file).wrap_err("Failed to dpdk initialization.")?;

        // TODO: figure out a way to have a "proper" port_id arg
        let my_ether_addr =
            wrapper::get_my_macaddr(0).wrap_err("Failed to get my own mac address")?;
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

        Ok(DPDKConnection {
            mode: mode,
            ip_to_mac: ip_to_mac,
            mac_to_ip: mac_to_ip,
            outgoing_window: outgoing_window,
            default_mempool: mempool,
            extbuf_mempool: ext_mempool,
            header_info: header_info,
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
    fn push_sga(&mut self, sga: &Cornflake, addr: Ipv4Addr) -> Result<()> {
        // if client, add start time for packet
        match self.mode {
            DPDKMode::Server => {}
            DPDKMode::Client => {
                let _ = self.outgoing_window.insert(sga.get_id(), Instant::now());
            }
        }

        // allocate header mbuf and fill in header information
        let pkt = wrapper::alloc_mbuf(self.default_mempool)
            .wrap_err("Unable to allocate mbuf to store header for outgoing packet.")?;
        let header_info = self.get_outgoing_header(addr)?;
        wrapper::fill_in_header(pkt, &header_info, sga.data_len(), sga.get_id())
            .wrap_err("Unable to fill header into mbuf.")?;

        // now copy or externally attach memory regions referenced by the SGA
        // TODO: create the linked list of mbufs represented by this cornflake

        Ok(())
    }

    fn pop(&mut self) -> Result<Option<(Cornflake, Duration)>> {
        unimplemented!();
    }

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
}
