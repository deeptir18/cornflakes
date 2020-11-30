use color_eyre::eyre::Result;
use cornflakes_libos::dpdk_bindings;
use cornflakes_libos::dpdk_libos::wrapper;
use cornflakes_libos::utils::TraceLevel;
use structopt::StructOpt;
use tracing::Level;
use tracing_subscriber::{filter::LevelFilter, FmtSubscriber};

#[derive(Debug, StructOpt)]
#[structopt(name = "DPDK server", about = "DPDK server program")]

struct Opt {
    #[structopt(
        short = "trace",
        long = "tracing_level",
        help = "Configure tracing settings.",
        default_value = "warn"
    )]
    trace_level: TraceLevel,
    #[structopt(
        short = "f",
        long = "config_file",
        help = "Folder containing shared config information."
    )]
    config_file: String,
}
fn main() -> Result<()> {
    color_eyre::install()?;
    dpdk_bindings::load_mlx5_driver();
    let opt = Opt::from_args();
    let trace_level = opt.trace_level;
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
    wrapper::dpdk_init(&opt.config_file)?;
    Ok(())
}
