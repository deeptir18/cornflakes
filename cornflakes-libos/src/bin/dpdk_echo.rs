use color_eyre::eyre::{Result, WrapErr};
use cornflakes_libos::{
    dpdk_bindings,
    dpdk_libos::{
        connection::{DPDKConnection, DPDKMode},
        echo::{EchoClient, EchoServer},
    },
    ClientSM, Datapath, ServerSM,
};
use cornflakes_utils::{global_debug_init, TraceLevel};
use std::{net::Ipv4Addr, process::exit, time::Duration};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "DPDK server", about = "DPDK server program")]

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
    let opt = Opt::from_args();
    global_debug_init(opt.trace_level)?;
    dpdk_bindings::load_mlx5_driver();

    let payload = vec![b'a'; opt.size];

    let mut connection = match opt.mode {
        DPDKMode::Server => DPDKConnection::new(&opt.config_file, opt.mode, opt.zero_copy)
            .wrap_err("Failed to initialize DPDK server connection.")?,
        DPDKMode::Client => DPDKConnection::new(&opt.config_file, opt.mode, true)
            .wrap_err("Failed to initialize DPDK server connection.")?,
    };
    match opt.mode {
        DPDKMode::Server => {
            let mut server = EchoServer::new()?;
            let histograms = connection.get_timers();
            let echo_histograms = server.get_histograms();
            {
                let h = histograms;
                let h2 = echo_histograms;
                ctrlc::set_handler(move || {
                    tracing::info!("In ctrl-c handler");
                    for timer_m in h.iter() {
                        let timer = timer_m.lock().unwrap();
                        timer.dump_stats();
                    }
                    for timer_m in h2.iter() {
                        let timer = timer_m.lock().unwrap();
                        timer.dump_stats();
                    }
                    exit(0);
                })?;
            }

            server.init(&mut connection)?;
            server.run_state_machine(&mut connection)?;
        }
        DPDKMode::Client => {
            let mut client =
                EchoClient::new(opt.size, opt.server_ip, opt.zero_copy, &payload.as_ref())?;
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
            let load = ((opt.size as f64) * (opt.rate) as f64) / (125000000 as f64);
            tracing::info!(load_gbps = ?load, "Sent at rate:");
            for timer_m in connection.get_timers().iter() {
                let timer = timer_m.lock().unwrap();
                timer.dump_stats();
            }
        }
    }
    Ok(())
}
