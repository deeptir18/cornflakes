use color_eyre::eyre::Result;
use cornflakes_libos::{
    dpdk_bindings,
    dpdk_libos::fast_echo::{do_client, do_server, MemoryMode},
};
use cornflakes_utils::{global_debug_init, AppMode, TraceLevel};
use std::net::Ipv4Addr;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "DPDK Netperf server",
    about = "Barebones netperf server written in Rust"
)]
struct Opt {
    #[structopt(long = "zero_copy", help = "zero-copy mode.")]
    zero_copy: bool,
    #[structopt(
        long = "debug_level",
        help = "Configure tracing settings.",
        default_value = "warn"
    )]
    trace_level: TraceLevel,
    #[structopt(
        long = "config_file",
        help = "Folder containing shared config information."
    )]
    config_file: String,
    #[structopt(long = "mode", help = "DPDK server or client mode.")]
    mode: AppMode,
    #[structopt(
        long = "size",
        help = "Size of message to be sent.",
        default_value = "1000"
    )]
    size: usize,
    #[structopt(
        long = "time",
        help = "Time to run the benchmark for in seconds.",
        default_value = "1"
    )]
    total_time: u64,
    #[structopt(
        long = "rate",
        help = "Rate of client (in pkts/sec)",
        default_value = "2000"
    )]
    rate: u64,
    #[structopt(
        long = "server_ip",
        help = "Server ip address",
        default_value = "127.0.0.1"
    )]
    server_ip: Ipv4Addr,
    #[structopt(long = "memory", help = "Memory Mode", default_value = "DPDK")]
    memory_mode: MemoryMode,
    #[structopt(long = "num_mbufs", help = "Number of mbufs", default_value = "1")]
    num_mbufs: usize,
    #[structopt(
        long = "split_payload",
        help = "Amount of payload to put behind header in first mbuf.",
        default_value = "0"
    )]
    split_payload: usize,
    #[structopt(long = "use_c", help = "do everything in c for debugging")]
    use_c: bool,
    #[structopt(
        long = "num_threads",
        help = "Number of client threads",
        default_value = "1"
    )]
    num_threads: usize,
}

fn main() -> Result<()> {
    let opt = Opt::from_args();
    global_debug_init(opt.trace_level)?;
    #[cfg(feature = "mlx5")]
    {
        dpdk_bindings::load_mlx5_driver();
    }

    match opt.mode {
        AppMode::Server => {
            do_server(
                &opt.config_file,
                opt.zero_copy,
                opt.memory_mode,
                opt.num_mbufs,
                opt.split_payload,
                opt.use_c,
            )?;
        }
        AppMode::Client => {
            do_client(
                opt.rate,
                opt.total_time,
                opt.size,
                &opt.config_file,
                &opt.server_ip,
                opt.num_threads,
            )?;
        }
    }

    Ok(())
}
