use affinity::*;
use color_eyre::eyre::{bail, Result};
use cornflakes_libos::{
    datapath::Datapath,
    loadgen::{
        client_threads::{dump_thread_stats, ThreadStats},
        request_schedule::{generate_schedules, DistributionType, PacketSchedule},
    },
    state_machine::client::ClientSM,
    utils::AddressInfo,
};
use cornflakes_utils::{
    get_thread_latlog, global_debug_init, parse_server_addr, AppMode, NetworkDatapath, TraceLevel,
};
use dpdk_datapath::dpdk_bindings;
use mlx5_datapath::datapath::connection::Mlx5Connection;
use simple_echo::{client::SimpleEchoClient, RequestShape};
use std::{
    net::Ipv4Addr,
    thread::{spawn, JoinHandle},
    time::Instant,
};
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
    #[structopt(long = "our_ip", help = "Our IP Address", default_value = "127.0.0.1")]
    our_ip: Ipv4Addr,
    #[structopt(
        long = "server_ip",
        help = "Our IP Address",
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

    match opt.datapath {
        NetworkDatapath::DPDK => {
            unimplemented!();
        }
        NetworkDatapath::MLX5 => {
            let server_addr = parse_server_addr(&opt.config_file, &opt.server_ip)?;
            let mut datapath_params =
                <Mlx5Connection as Datapath>::parse_config_file(&opt.config_file, &opt.our_ip)?;
            let addresses = <Mlx5Connection as Datapath>::compute_affinity(
                &datapath_params,
                opt.num_threads,
                Some(opt.server_ip.clone()),
                AppMode::Client,
            )?;
            let num_rtts = (opt.rate * opt.total_time * 2) as usize;
            let schedules =
                generate_schedules(num_rtts, opt.rate, opt.distribution, opt.num_threads)?;

            let per_thread_contexts = <Mlx5Connection as Datapath>::global_init(
                opt.num_threads,
                &mut datapath_params,
                addresses,
            )?;
            let mut threads: Vec<JoinHandle<Result<ThreadStats>>> = vec![];

            // spawn a thread to run client for each connection
            for (i, (schedule, per_thread_context)) in schedules
                .into_iter()
                .zip(per_thread_contexts.into_iter())
                .enumerate()
            {
                let server_addr_clone =
                    AddressInfo::new(server_addr.2, server_addr.1.clone(), server_addr.0.clone());
                let datapath_params_clone = datapath_params.clone();
                let request_shape = opt.request_shape.clone();
                let max_num_requests = num_rtts;
                let opt_clone = opt.clone();
                threads.push(spawn(move || {
                    match set_thread_affinity(&vec![i + 1]) {
                        Ok(_) => {}
                        Err(e) => {
                            bail!(
                                "Could not set thread affinity for thread {} on core {}: {:?}",
                                i,
                                i + 1,
                                e
                            )
                        }
                    }

                    let mut connection = <Mlx5Connection as Datapath>::per_thread_init(
                        datapath_params_clone,
                        per_thread_context,
                        AppMode::Client,
                    )?;

                    connection.set_copying_threshold(std::f64::INFINITY as _);

                    let mut echo_client: SimpleEchoClient<Mlx5Connection> =
                        SimpleEchoClient::new(server_addr_clone, &request_shape, max_num_requests);

                    echo_client.init(&mut connection)?;

                    run_client(i, &mut echo_client, &mut connection, &opt_clone, &schedule)
                }));
            }

            let mut thread_results: Vec<ThreadStats> = Vec::default();
            for child in threads {
                let s = match child.join() {
                    Ok(res) => match res {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::warn!("Thread failed: {:?}", e);
                            bail!("Failed thread");
                        }
                    },
                    Err(e) => {
                        tracing::warn!("Failed to join client thread: {:?}", e);
                        bail!("Failed to join thread");
                    }
                };
                thread_results.push(s);
            }

            let dump_per_thread = opt.logfile == None;
            dump_thread_stats(thread_results, opt.thread_log.clone(), dump_per_thread)?;
        }
    }
    Ok(())
}

fn run_client<D>(
    thread_id: usize,
    client: &mut SimpleEchoClient<D>,
    connection: &mut D,
    opt: &Opt,
    schedule: &PacketSchedule,
) -> Result<ThreadStats>
where
    D: Datapath,
{
    let start_run = Instant::now();
    let timeout = match opt.retries {
        true => cornflakes_libos::high_timeout_at_start,
        false => cornflakes_libos::no_retries_timeout,
    };

    client.run_open_loop(connection, schedule, opt.total_time, timeout, !opt.retries)?;

    let exp_duration = start_run.elapsed().as_nanos();
    client.sort_rtts(0)?;

    // per thread log latency
    match &opt.logfile {
        Some(x) => {
            let path = get_thread_latlog(&x, thread_id)?;
            client.log_rtts(&path, 0)?;
        }
        None => {}
    }

    let stats = ThreadStats::new(
        thread_id as u16,
        client.num_sent_cutoff(0),
        client.num_received_cutoff(0),
        client.num_retried(),
        exp_duration as _,
        opt.rate,
        opt.request_shape.message_size(),
        client.get_mut_rtts(),
        0,
    )?;

    Ok(stats)
}
