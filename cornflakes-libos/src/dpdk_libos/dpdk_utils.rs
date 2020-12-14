use color_eyre::eyre::{bail, Result};
use eui48::MacAddress;
use hashbrown::HashMap;
use std::{fs::read_to_string, net::Ipv4Addr, path::Path, str::FromStr};
use tracing::debug;
use yaml_rust::{Yaml, YamlLoader};

pub fn parse_yaml_map(
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

pub fn parse_eal_init(config_path: &str) -> Result<Vec<String>> {
    let file_str = read_to_string(Path::new(&config_path))?;
    let yamls = match YamlLoader::load_from_str(&file_str) {
        Ok(docs) => docs,
        Err(e) => {
            bail!("Could not parse config yaml: {:?}", e);
        }
    };

    let mut args = vec![];
    let yaml = &yamls[0];
    match yaml["dpdk"].as_hash() {
        Some(map) => {
            let eal_init = match map.get(&Yaml::from_str("eal_init")) {
                Some(list) => list,
                None => {
                    bail!("Yaml config dpdk has no eal_init entry");
                }
            };
            for entry in eal_init.as_vec().unwrap() {
                //let entry_str = std::str::from_utf8(entry).unwrap();
                args.push(entry.as_str().unwrap().to_string());
            }
        }

        None => {
            bail!("Yaml config file has no entry dpdk");
        }
    }
    Ok(args)
}
