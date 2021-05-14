use color_eyre::eyre::{bail, Result};
use tracing::Level;
use tracing_subscriber;
use tracing_subscriber::{filter::LevelFilter, FmtSubscriber};

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
pub enum NetworkDatapath {
    DPDK,
}

impl std::str::FromStr for NetworkDatapath {
    type Err = color_eyre::eyre::Error;
    fn from_str(s: &str) -> Result<NetworkDatapath> {
        Ok(match s {
            "dpdk" | "DPDK" => NetworkDatapath::DPDK,
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
}

impl std::str::FromStr for SerializationType {
    type Err = color_eyre::eyre::Error;
    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "protobuf" | "PROTOBUF" | "Protobuf" => SerializationType::Protobuf,
            "flatbuffers" | "FLATBUFFERS" | "Flatbuffers" => SerializationType::Flatbuffers,
            "capnproto" | "CAPNPROTO" | "Capnproto" => SerializationType::Capnproto,
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
