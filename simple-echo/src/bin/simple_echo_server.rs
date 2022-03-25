use color_eyre::eyre::Result;
use cornflakes_libos::{
    datapath::{Datapath, PushBufType},
    loadgen::request_schedule::DistributionType,
    state_machine::server::ServerSM,
};
use cornflakes_utils::{AppMode, TraceLevel};
use mlx5_datapath::datapath::connection::Mlx5Connection;
use simple_echo::server::SimpleEchoServer;
use std::net::Ipv4Addr;
use structopt::StructOpt;

#[derive(Debug, StructOpt, Clone)]
#[structopt(
    name = "Mlx5 test echo",
    about = "Binary to test and debug Mellanox Datapath"
)]
struct Opt {
    #[structopt(
        short = "debug",
        long = "debug_level",
        help = "Configure tracing settings.",
        default_value = "warn"
    )]
    trace_level: TraceLevel,
    #[structopt(
        short = "cf",
        long = "config_file",
        help = "Folder containing shared config information."
    )]
    config_file: String,
    #[structopt(long = "mode", help = "Echo server or client mode.")]
    mode: AppMode,
    #[structopt(
        short = "t",
        long = "time",
        help = "Time to run the benchmark for in seconds.",
        default_value = "1"
    )]
    total_time: u64,
    #[structopt(
        short = "ip",
        long = "server_ip",
        help = "Server IP Address",
        default_value = "127.0.0.1"
    )]
    server_ip: Ipv4Addr,
    #[structopt(
        short = "r",
        long = "rate",
        help = "Rate of client (in pkts/sec)",
        default_value = "2000"
    )]
    rate: u64,
    #[structopt(long = "retries", help = "Enable client retries.")]
    retries: bool,
    #[structopt(long = "logfile", help = "Logfile to log all client RTTs.")]
    logfile: Option<String>,
    #[structopt(long = "threadlog", help = "Logfile to log per thread statistics")]
    thread_log: Option<String>,
    #[structopt(
        long = "distribution",
        help = "Arrival distribution",
        default_value = "exponential"
    )]
    distribution: DistributionType,
    #[structopt(
        long = "num_threads",
        help = "Total number of threads",
        default_value = "1"
    )]
    num_threads: usize,
    #[structopt(
        long = "num_clients",
        help = "Total number of clients",
        default_value = "1"
    )]
    _num_clients: usize,
    #[structopt(long = "client_id", help = "ID of this client", default_value = "1")]
    _client_id: usize,
}

fn main() -> Result<()> {
    Ok(())
}
