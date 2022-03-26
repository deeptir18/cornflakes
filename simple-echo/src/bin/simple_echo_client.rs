use color_eyre::eyre::Result;
use cornflakes_libos::{
    datapath::{Datapath, PushBufType},
    dpdk_bindings,
    loadgen::request_schedule::DistributionType,
    state_machine::client::ClientSM,
};
use cornflakes_utils::{global_debug_init, AppMode, NetworkDatapath, TraceLevel};
//use mlx5_datapath::datapath::connection::Mlx5Connection;
use simple_echo::{client::SimpleEchoClient, RequestShape};
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
    #[structopt(
        short = "dp",
        long = "datapath",
        help = "Datapath to use",
        default_value = "dpdk"
    )]
    datapath: NetworkDatapath,
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
    #[structopt(
        short = "t",
        long = "time",
        help = "Time to run the benchmark for in seconds.",
        default_value = "1"
    )]
    total_time: u64,
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
    #[structopt(
        long = "request_shape",
        help = "Request Shape Pattern; 1-4-256 = pattern of length 1 of [256], repeated four times.",
        default_value = "1-4-256"
    )]
    request_shape: RequestShape,
}

fn main() -> Result<()> {
    let opt = Opt::from_args();
    global_debug_init(opt.trace_level)?;
    if opt.datapath == NetworkDatapath::DPDK {
        dpdk_bindings::load_mlx5_driver();
    }
    Ok(())
}
