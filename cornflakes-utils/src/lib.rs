use color_eyre::eyre::{bail, Result};
use eui48::MacAddress;
use hashbrown::HashMap;
use std::{fs::read_to_string, net::Ipv4Addr, path::Path, str::FromStr};
use tracing::Level;
use tracing_subscriber;
use tracing_subscriber::{filter::LevelFilter, FmtSubscriber};
use yaml_rust::{Yaml, YamlLoader};

pub fn parse_server_port(config_file: &str) -> Result<u16> {
    let file_str = read_to_string(Path::new(&config_file))?;
    let yamls = match YamlLoader::load_from_str(&file_str) {
        Ok(docs) => docs,
        Err(e) => {
            bail!("Could not parse config yaml: {:?}", e);
        }
    };

    let yaml = &yamls[0];
    let udp_port = match yaml.as_hash().unwrap().get(&Yaml::from_str("port")) {
        Some(port_str) => {
            let val = port_str.as_i64().unwrap() as u16;
            val
        }
        None => {
            bail!("Yaml config has no port entry.");
        }
    };

    Ok(udp_port)
}

pub fn parse_yaml_map(
    config_file: &str,
) -> Result<(
    HashMap<Ipv4Addr, MacAddress>,
    HashMap<MacAddress, Ipv4Addr>,
    u16,
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

    let client_port = match yaml.as_hash().unwrap().get(&Yaml::from_str("client_port")) {
        Some(port_str) => {
            let val = port_str.as_i64().unwrap() as u16;
            val
        }
        None => {
            bail!("Yaml config has no port entry.");
        }
    };

    Ok((ip_to_mac, mac_to_ip, udp_port, client_port))
}

#[macro_export]
macro_rules! test_init(
        () => {
            let subscriber = tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer())
                .with(tracing_subscriber::EnvFilter::from_default_env())
                .with(ErrorLayer::default());
            let _guard = subscriber.set_default();
            color_eyre::install().unwrap_or_else(|_| ());
        }
    );

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum AppMode {
    Client,
    Server,
}

impl std::str::FromStr for AppMode {
    type Err = color_eyre::eyre::Error;
    fn from_str(s: &str) -> Result<AppMode> {
        Ok(match s {
            "server" | "Server" | "SERVER" => AppMode::Server,
            "client" | "Client" | "CLIENT" => AppMode::Client,
            x => bail!("{} app mode unknown.", x),
        })
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum NetworkDatapath {
    DPDK,
    OFED,
}

impl std::str::FromStr for NetworkDatapath {
    type Err = color_eyre::eyre::Error;
    fn from_str(s: &str) -> Result<NetworkDatapath> {
        Ok(match s {
            "dpdk" | "DPDK" => NetworkDatapath::DPDK,
            "ofed" | "Ofed" | "OFED" => NetworkDatapath::OFED,
            x => bail!("{} datapath unknown.", x),
        })
    }
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum TreeDepth {
    One,
    Two,
    Three,
    Four,
    Five,
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum SimpleMessageType {
    /// Message with a single field
    Single,
    /// List with a variable number of elements
    List(usize),
    /// Tree with a variable depth.
    Tree(TreeDepth),
}

impl std::str::FromStr for SimpleMessageType {
    type Err = color_eyre::eyre::Error;
    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "single" | "SINGLE" | "Single" => SimpleMessageType::Single,
            x => {
                if x.starts_with("List-") || x.starts_with("LIST-") || x.starts_with("list-") {
                    let list_size: usize = x[5..].to_string().parse()?;
                    SimpleMessageType::List(list_size)
                } else if x.starts_with("Tree-") || x.starts_with("TREE-") || x.starts_with("tree-")
                {
                    let tree_size: usize = x[5..].to_string().parse()?;
                    let depth = match tree_size {
                        1 => TreeDepth::One,
                        2 => TreeDepth::Two,
                        3 => TreeDepth::Three,
                        4 => TreeDepth::Four,
                        5 => TreeDepth::Five,
                        x => {
                            bail!("Provided tree depth not supported: {:?}", x);
                        }
                    };
                    SimpleMessageType::Tree(depth)
                } else {
                    bail!("{}: unknown message type", x)
                }
            }
        })
    }
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum SerializationType {
    /// Protobuf baseline.
    Protobuf,
    /// Flatbuffers baseline.
    Flatbuffers,
    /// Capnproto baseline.
    Capnproto,
    /// Normal, zero-copy cornflakes with a dynamically sized header.
    CornflakesDynamic,
    /// Cornflakes with a fixed size header.
    CornflakesFixed,
    /// Copied into 1 buffer, but with dynamically sized header.
    CornflakesOneCopyDynamic,
    /// Cornflakes where everything is copied into 1 packet buffer, with fix sized header.
    CornflakesOneCopyFixed,
    /// Cereal serialization library.
    Cereal,
}

impl std::str::FromStr for SerializationType {
    type Err = color_eyre::eyre::Error;
    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "protobuf" | "PROTOBUF" | "Protobuf" => SerializationType::Protobuf,
            "flatbuffers" | "FLATBUFFERS" | "Flatbuffers" => SerializationType::Flatbuffers,
            "capnproto" | "CAPNPROTO" | "Capnproto" => SerializationType::Capnproto,
            "cereal" | "CEREAL" | "Cereal" => SerializationType::Cereal,
            "cornflakes-dynamic" | "CORNFLAKES-DYNAMIC" | "Cornflakes-Dynamic"
            | "CornflakesDynamic" => SerializationType::CornflakesDynamic,
            "cornflakes-fixed" | "CORNFLAKES-FIXED" | "Cornflakes-Fixed" | "CornflakesFixed" => {
                SerializationType::CornflakesFixed
            }
            "cornflakes1c-fixed" | "CORNFLAKES1C-FIXED" | "Cornflakes1C-Fixed"
            | "Cornflakes1c-Fixed" => SerializationType::CornflakesOneCopyFixed,
            "cornflakes1c-dynamic"
            | "CORNFLAKES1C-DYNAMIC"
            | "Cornflakes1C-Dynamic"
            | "Cornflakes1c-Dynamic" => SerializationType::CornflakesOneCopyDynamic,
            x => {
                bail!("{} serialization type unknown.", x);
            }
        })
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum TraceLevel {
    Debug,
    Info,
    Warn,
    Error,
    Off,
}

impl std::str::FromStr for TraceLevel {
    type Err = color_eyre::eyre::Error;
    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "debug" => TraceLevel::Debug,
            "info" => TraceLevel::Info,
            "warn" => TraceLevel::Warn,
            "error" => TraceLevel::Error,
            "off" => TraceLevel::Off,
            x => bail!("unknown TRACE level {:?}", x),
        })
    }
}

pub fn global_debug_init(trace_level: TraceLevel) -> Result<()> {
    color_eyre::install()?;
    let subscriber = match trace_level {
        TraceLevel::Debug => FmtSubscriber::builder()
            .with_max_level(Level::DEBUG)
            .finish(),
        TraceLevel::Info => FmtSubscriber::builder()
            .with_max_level(Level::INFO)
            .finish(),
        TraceLevel::Warn => FmtSubscriber::builder()
            .with_max_level(Level::WARN)
            .finish(),
        TraceLevel::Error => FmtSubscriber::builder()
            .with_max_level(Level::ERROR)
            .finish(),
        TraceLevel::Off => FmtSubscriber::builder()
            .with_max_level(LevelFilter::OFF)
            .finish(),
    };
    tracing::subscriber::set_global_default(subscriber).expect("setting defualt subscriber failed");
    Ok(())
}
