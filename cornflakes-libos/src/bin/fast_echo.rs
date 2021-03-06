use color_eyre::eyre::Result;
use cornflakes_libos::{
    dpdk_bindings,
    dpdk_libos::{
        connection::DPDKMode,
        fast_echo::{do_client, do_server, MemoryMode},
    },
};
use cornflakes_utils::{global_debug_init, TraceLevel};
use std::net::Ipv4Addr;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "DPDK Netperf server",
    about = "Barebones netperf server written in Rust"
)]
struct Opt {
    #[structopt(short = "z", long = "zero_copy", help = "zero-copy mode.")]
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
    #[structopt(
        short = "mem",
        long = "memory",
        help = "Memory Mode",
        default_value = "DPDK"
    )]
    memory_mode: MemoryMode,
    #[structopt(
        short = "mbuf",
        long = "num_mbufs",
        help = "Number of mbufs",
        default_value = "1"
    )]
    num_mbufs: usize,
    #[structopt(
        short = "split_payload",
        long = "split_payload",
        help = "Amount of payload to put behind header in first mbuf.",
        default_value = "0"
    )]
    split_payload: usize,
    #[structopt(
        short = "use_c",
        long = "use_c",
        help = "do everything in c for debugging"
    )]
    use_c: bool,
}

fn main() -> Result<()> {
    let opt = Opt::from_args();
    global_debug_init(opt.trace_level)?;
    dpdk_bindings::load_mlx5_driver();

    match opt.mode {
        DPDKMode::Server => {
            do_server(
                &opt.config_file,
                opt.zero_copy,
                opt.memory_mode,
                opt.num_mbufs,
                opt.split_payload,
                opt.use_c,
            )?;
        }
        DPDKMode::Client => {
            do_client(
                opt.rate,
                opt.total_time,
                opt.size,
                &opt.config_file,
                &opt.server_ip,
            )?;
        }
    }

    Ok(())
}
