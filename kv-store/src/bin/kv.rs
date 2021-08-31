use affinity::*;
use color_eyre::eyre::{bail, Result, WrapErr};
use cornflakes_libos::{
    dpdk_bindings,
    dpdk_libos::connection::DPDKConnection,
    loadgen::{
        client_threads::{dump_thread_stats, ThreadStats},
        request_schedule::{generate_schedules, DistributionType, PacketSchedule},
    },
    timing::ManualHistogram,
    utils::AddressInfo,
    ClientSM, Datapath, ServerSM,
};

use cornflakes_utils::{
    global_debug_init, parse_server_port, AppMode, NetworkDatapath, SerializationType, TraceLevel,
};
use kv_store::{
    cornflakes_dynamic::CornflakesDynamicSerializer, KVSerializer, KVServer,
    SerializedRequestGenerator, YCSBClient,
};
use std::{
    net::Ipv4Addr,
    path::Path,
    process::exit,
    process::Command,
    thread::{spawn, JoinHandle},
    time::Instant,
};
use structopt::StructOpt;

#[derive(Debug, StructOpt, Clone)]
#[structopt(name = "KV Store App.", about = "KV store server and client.")]
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
    #[structopt(long = "mode", help = "KV server or client mode.")]
    mode: AppMode,
    #[structopt(
        short = "nd",
        long = "datapath",
        help = "Datapath to run",
        default_value = "dpdk"
    )]
    datapath: NetworkDatapath,
    #[structopt(
        long = "size",
        help = "size of values in kv store",
        default_value = "1024"
    )]
    value_size: usize,
    #[structopt(
        long = "num_values",
        help = "number of batched puts and gets per line in trace",
        default_value = "1"
    )]
    num_values: usize,
    #[structopt(long = "time", help = "max time to run exp for", default_value = "30")]
    time: usize,
    #[structopt(
        short = "t",
        long = "trace",
        help = "Trace file to load server side values from.",
        default_value = ""
    )]
    trace_file: String,
    #[structopt(
        short = "q",
        long = "queries",
        help = "Query file to load queries from.",
        default_value = ""
    )]
    queries: String,
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
        short = "ser",
        long = "serialization",
        help = "Serialization library to use",
        default_value = "cornflakes-dynamic"
    )]
    serialization: SerializationType,
    #[structopt(long = "retries", help = "Disable client retries.")]
    retries: bool,
    #[structopt(long = "logfile", help = "Logfile to log all client RTTs.")]
    logfile: Option<String>,
    #[structopt(long = "threadlog", help = "Logfile to log per thread statistics")]
    thread_log: Option<String>,
    #[structopt(
        long = "native_buffers",
        help = "Use Cornflakes, but serialize into a datapath native buffer."
    )]
    use_native_buffers: bool,
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
    num_clients: usize,
    #[structopt(long = "client_id", default_value = "0")]
    client_id: usize,
    #[structopt(long = "start_cutoff", default_value = "0")]
    start_cutoff: usize,
    #[structopt(long = "distribution", default_value = "exponential")]
    distribution: DistributionType,
}

/// Given a path, calculates the number of lines in the file.
/// Used to calculate the amount of memory needed for the KV store.
fn lines_in_file(path: &str) -> Result<usize> {
    let output = Command::new("wc")
        .arg("-l")
        .arg(path)
        .output()
        .wrap_err(format!("Failed to run wc on path: {:?}", path))?;
    let stdout = String::from_utf8(output.stdout).wrap_err("Not able to get string from stdout")?;
    let mut stdout_split = stdout.split(" ");
    let num = match stdout_split.next() {
        Some(x) => {
            let num = x
                .parse::<usize>()
                .wrap_err(format!("Could not turn wc output into usize: {:?}", stdout))?;
            num
        }
        None => {
            bail!("No string found in stdout of wc command");
        }
    };
    Ok(num)
}

fn get_num_requests(opt: &Opt) -> Result<usize> {
    let total = lines_in_file(&opt.queries)? / (opt.num_clients * opt.num_threads);
    let minimum = (opt.rate as usize * opt.time as usize) * 2;
    Ok(std::cmp::min(minimum, total))
}

#[macro_export]
macro_rules! init_kv_server(
    ($serializer: ty, $datapath: ty, $datapath_init: expr, $opt: ident) => {
        let mut connection = $datapath_init?;
        let mut kv_server: KVServer<$serializer,$datapath> = KVServer::new($opt.use_native_buffers)?;
        set_ctrlc_handler(&kv_server)?;
        // load values into kv store
        kv_server.init(&mut connection)?;
        kv_server.load(&$opt.trace_file, &mut connection, $opt.value_size, $opt.num_values)?;
        kv_server.run_state_machine(&mut connection)?;
    }
);

macro_rules! run_kv_client(
    ($serializer: ty, $datapath: ty, $datapath_global_init: expr, $datapath_init: ident, $opt: ident) => {
        let num_rtts = get_num_requests(&$opt)?;
        let schedules = generate_schedules(num_rtts, $opt.rate, $opt.distribution, $opt.num_threads)?;
        // do global init
        let (port, per_thread_info) = $datapath_global_init?;
        let mut threads: Vec<JoinHandle<Result<ThreadStats>>> = vec![];

        // spawn n client threads
        for i in 0..$opt.num_threads {
            let physical_port = port;
            let (rx_packet_allocator, addr) = per_thread_info[i].clone();
            let per_thread_options = $opt.clone();
            let hist = ManualHistogram::new(num_rtts);
            let schedule = schedules[i].clone();
            threads.push(spawn(move || {
                match set_thread_affinity(&vec![i+1]) {
                    Ok(_) => {}
                    Err(e) => {
                        bail!("Could not set thread affinity for thread {} on core {}: {:?}", i, i+1, e);
                    }
                }
                let mut connection = $datapath_init(physical_port, i, rx_packet_allocator, addr, &per_thread_options)?;
                let mut loadgen: YCSBClient<$serializer, $datapath> =
                    YCSBClient::new(per_thread_options.client_id, per_thread_options.value_size, per_thread_options.num_values, &per_thread_options.queries, i, per_thread_options.num_threads, per_thread_options.num_clients, per_thread_options.server_ip, hist, per_thread_options.retries, per_thread_options.start_cutoff).wrap_err("Failed to initialize loadgen")?;
                run_client(i, &mut loadgen, &mut connection, &per_thread_options, schedule).wrap_err("Failed to run client")
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
                    tracing::debug!("Failed to join client thread: {:?}", e);
                    bail!("Failed to join thread");
                }
            };
            thread_results.push(s);
        }

        // in experiments, no need to dump per thread
        let dump_per_thread = $opt.logfile == None;
        dump_thread_stats(thread_results, $opt.thread_log, dump_per_thread)?;
    }

);

fn set_ctrlc_handler<S, D>(server: &KVServer<S, D>) -> Result<()>
where
    S: KVSerializer<D>,
    D: Datapath,
{
    let echo_histograms = server.get_histograms();
    {
        let h = echo_histograms;
        ctrlc::set_handler(move || {
            tracing::info!("In ctrl-c handler");
            for timer_m in h.iter() {
                let timer = timer_m.lock().unwrap();
                timer.dump_stats();
            }
            exit(0);
        })?;
    }
    Ok(())
}

fn main() -> Result<()> {
    let opt = Opt::from_args();
    global_debug_init(opt.trace_level)?;

    let dpdk_global_init = || -> Result<(u16, Vec<(<DPDKConnection as Datapath>::RxPacketAllocator, AddressInfo)>)> {
        dpdk_bindings::load_mlx5_driver();
        let remote_ip = match opt.mode {
            AppMode::Client => {
                Some(opt.server_ip.clone())
            }
            AppMode::Server => None,
        };
        <DPDKConnection as Datapath>::global_init(&opt.config_file, opt.num_threads, opt.mode, remote_ip)
    };

    let dpdk_per_thread_init = |physical_port: u16,
                                thread_id: usize,
                                rx_mempool: <DPDKConnection as Datapath>::RxPacketAllocator,
                                addr_info: AddressInfo,
                                options: &Opt|
     -> Result<DPDKConnection> {
        let use_scatter_gather = options.mode == AppMode::Server
            && (options.serialization == SerializationType::CornflakesDynamic
                || options.serialization == SerializationType::CornflakesFixed);
        let connection = <DPDKConnection as Datapath>::per_thread_init(
            physical_port,
            &options.config_file,
            options.mode,
            use_scatter_gather,
            thread_id,
            rx_mempool,
            addr_info,
        )?;
        Ok(connection)
    };

    let dpdk_datapath = || -> Result<DPDKConnection> {
        let use_scatter_gather = opt.mode == AppMode::Server
            && (opt.serialization == SerializationType::CornflakesDynamic
                || opt.serialization == SerializationType::CornflakesFixed);
        let mut connection = DPDKConnection::new(&opt.config_file, opt.mode, use_scatter_gather)
            .wrap_err("Failed to initialize DPDK connection.")?;
        if opt.mode == AppMode::Server {
            // calculate the number of lines in the trace file
            let num_lines = lines_in_file(&opt.trace_file)?;
            connection
                .add_mempool("kv_buffer_pool", opt.value_size, num_lines * opt.num_values)
                .wrap_err("Could not add mempool to DPDK conneciton")?;
        }
        Ok(connection)
    };

    match opt.mode {
        AppMode::Server => match (opt.datapath, opt.serialization) {
            (NetworkDatapath::DPDK, SerializationType::CornflakesDynamic) => {
                init_kv_server!(
                    CornflakesDynamicSerializer<DPDKConnection>,
                    DPDKConnection,
                    dpdk_datapath(),
                    opt
                );
            }
            (NetworkDatapath::DPDK, SerializationType::CornflakesOneCopyDynamic) => {
                init_kv_server!(
                    CornflakesDynamicSerializer<DPDKConnection>,
                    DPDKConnection,
                    dpdk_datapath(),
                    opt
                );
            }
            _ => {
                unimplemented!();
            }
        },
        AppMode::Client => match (opt.datapath, opt.serialization) {
            (NetworkDatapath::DPDK, SerializationType::CornflakesDynamic) => {
                run_kv_client!(
                    CornflakesDynamicSerializer<DPDKConnection>,
                    DPDKConnection,
                    dpdk_global_init(),
                    dpdk_per_thread_init,
                    opt
                );
            }
            (NetworkDatapath::DPDK, SerializationType::CornflakesOneCopyDynamic) => {
                run_kv_client!(
                    CornflakesDynamicSerializer<DPDKConnection>,
                    DPDKConnection,
                    dpdk_global_init(),
                    dpdk_per_thread_init,
                    opt
                );
            }
            _ => {
                unimplemented!();
            }
        },
    }
    Ok(())
}

fn get_thread_latlog(name: &str, thread_id: usize) -> Result<String> {
    let filename = Path::new(name);
    let stem = match filename.file_stem() {
        Some(s) => s,
        None => {
            bail!("Could not get filestem for: {}", name);
        }
    };
    let mut file_parent = filename.to_path_buf();
    assert!(file_parent.pop());
    file_parent.push(&format!("{}-t{}.log", stem.to_str().unwrap(), thread_id));
    Ok(file_parent.to_str().unwrap().to_string())
}

fn run_client<S, D>(
    thread_id: usize,
    loadgen: &mut YCSBClient<S, D>,
    connection: &mut D,
    opt: &Opt,
    schedule: PacketSchedule,
) -> Result<ThreadStats>
where
    S: SerializedRequestGenerator<D>,
    D: Datapath,
{
    let server_port = parse_server_port(&opt.config_file)
        .wrap_err("Failed to parse server port from config file")?;
    tracing::debug!("Server port: {}", server_port);
    loadgen.init(connection)?;
    let timeout = match opt.retries {
        true => cornflakes_libos::high_timeout_at_start,
        false => cornflakes_libos::no_retries_timeout,
    };

    // if start cutoff is > 0, run_closed_loop for that many packets
    loadgen
        .run_closed_loop(
            connection,
            opt.start_cutoff as u64,
            timeout,
            &opt.server_ip,
            server_port,
        )
        .wrap_err("Failed to run warm up closed loop")?;

    let start_run = Instant::now();

    loadgen.run_open_loop(
        connection,
        &schedule,
        opt.time as u64,
        timeout,
        !opt.retries,
        &opt.server_ip,
        server_port,
    )?;

    let exp_duration = start_run.elapsed().as_nanos();
    loadgen.sort_rtts(opt.start_cutoff)?;
    // log latencies for this thread to per thread latency log file
    match &opt.logfile {
        Some(x) => {
            let path = get_thread_latlog(&x, thread_id)?;
            loadgen.log_rtts(&path, opt.start_cutoff)?;
        }
        None => {}
    }
    // debugging timers for this thread
    // todo: add thread info to these prints
    for timer_m in connection.get_timers().iter() {
        let timer = timer_m.lock().unwrap();
        timer.dump_stats();
    }

    let stats = ThreadStats::new(
        thread_id as u16,
        loadgen.get_num_sent(opt.start_cutoff),
        loadgen.get_num_recved(opt.start_cutoff),
        loadgen.get_num_retries(),
        exp_duration as _, // in nanos
        opt.rate,
        opt.value_size * opt.num_values,
        loadgen.get_mut_rtts(),
        opt.start_cutoff,
    )?;

    Ok(stats)
}
