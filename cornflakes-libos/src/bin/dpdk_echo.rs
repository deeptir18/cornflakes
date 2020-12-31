use color_eyre::eyre::{Result, WrapErr};
use cornflakes_libos::{
    dpdk_bindings,
    dpdk_libos::{
        connection::{DPDKConnection, DPDKMode},
        echo::{EchoClient, EchoServer},
    },
    utils::TraceLevel,
    ClientSM, ServerSM,
};
use std::{net::Ipv4Addr, time::Duration};
use structopt::StructOpt;
use tracing::Level;
use tracing_subscriber::{filter::LevelFilter, FmtSubscriber};

#[derive(Debug, StructOpt)]
#[structopt(name = "DPDK server", about = "DPDK server program")]

struct Opt {
    #[structopt(short = "z", long = "zero_copy", help = "Zero-copy mode.")]
    zero_copy: bool,
    #[structopt(
        short = "debug",
        long = "debug_level",
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
    #[structopt(short = "cl", long = "closed_loop", help = "Run in closed loop mode.")]
    closed_loop: bool,
    #[structopt(
        short = "n",
        long = "iterations",
        help = "Number of packets for closed loop mode",
        default_value = "100"
    )]
    iterations: u64,
    #[structopt(short = "m", long = "mode", help = "DPDK server or client mode.")]
    mode: DPDKMode,
    #[structopt(
        short = "s",
        long = "size",
        help = "Size of message to be sent.",
        default_value = "1000"
    )]
    size: usize,
    #[structopt(
        short = "t",
        long = "time",
        help = "Time to run the benchmark for in seconds.",
        default_value = "1"
    )]
    total_time: u64,
    #[structopt(
        short = "r",
        long = "rate",
        help = "Rate of client (in pkts/sec)",
        default_value = "2000"
    )]
    rate: u64,
    #[structopt(
        short = "ip",
        long = "server_ip",
        help = "Server ip address",
        default_value = "127.0.0.1"
    )]
    server_ip: Ipv4Addr,
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

    let mut connection = DPDKConnection::new(&opt.config_file, opt.mode)
        .wrap_err("Failed to initialize DPDK server connection.")?;
    match opt.mode {
        DPDKMode::Server => {
            let mut server = EchoServer::new(opt.size, opt.zero_copy)?;
            server.init(&mut connection)?;
            server.run_state_machine(&mut connection)?;
        }
        DPDKMode::Client => {
            let mut client = EchoClient::new(opt.size, opt.server_ip, opt.zero_copy)?;
            client.init(&mut connection)?;
            if opt.closed_loop {
                client.run_closed_loop(
                    &mut connection,
                    opt.iterations,
                    Duration::new(0, 1000000),
                )?;
            } else {
                client.run_open_loop(
                    &mut connection,
                    (1e9 / opt.rate as f64) as u64,
                    opt.total_time,
                    Duration::new(0, 1000000),
                )?;
            }
            client.dump_stats();
        }
    }
    Ok(())
}
