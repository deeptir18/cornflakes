use color_eyre::eyre::{bail, Result, WrapErr};
use cornflakes_libos::{
    dpdk_bindings, dpdk_libos::connection::DPDKConnection, timing::ManualHistogram, ClientSM,
    Datapath, ServerSM,
};

use cornflakes_utils::{
    global_debug_init, AppMode, NetworkDatapath, SerializationType, TraceLevel,
};
use kv_store::{
    cornflakes_dynamic::CornflakesDynamicSerializer, KVSerializer, KVServer,
    SerializedRequestGenerator, YCSBClient,
};
use std::{net::Ipv4Addr, process::exit, process::Command, time::Instant};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
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

macro_rules! init_kv_client(
    ($serializer: ty, $datapath: ty, $datapath_init: expr, $opt: ident) => {
        let num_rtts = lines_in_file(&$opt.queries)? / ($opt.num_clients * $opt.num_threads);
        let hist = ManualHistogram::new(num_rtts);
        let mut connection = $datapath_init?;

        let mut loadgen: YCSBClient<$serializer, $datapath> =
            YCSBClient::new($opt.client_id, $opt.value_size, $opt.num_values, &$opt.queries, 0, $opt.num_threads, $opt.num_clients, $opt.server_ip, hist, $opt.retries)?;
        run_client(&mut loadgen, &mut connection, &$opt)?;
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

    let dpdk_datapath = |use_scatter_gather: bool| -> Result<DPDKConnection> {
        dpdk_bindings::load_mlx5_driver();
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
                    dpdk_datapath(true),
                    opt
                );
            }
            (NetworkDatapath::DPDK, SerializationType::CornflakesOneCopyDynamic) => {
                init_kv_server!(
                    CornflakesDynamicSerializer<DPDKConnection>,
                    DPDKConnection,
                    dpdk_datapath(false),
                    opt
                );
            }
            _ => {
                unimplemented!();
            }
        },
        AppMode::Client => match (opt.datapath, opt.serialization) {
            (NetworkDatapath::DPDK, SerializationType::CornflakesDynamic) => {
                init_kv_client!(
                    CornflakesDynamicSerializer<DPDKConnection>,
                    DPDKConnection,
                    dpdk_datapath(true),
                    opt
                );
            }
            (NetworkDatapath::DPDK, SerializationType::CornflakesOneCopyDynamic) => {
                init_kv_client!(
                    CornflakesDynamicSerializer<DPDKConnection>,
                    DPDKConnection,
                    dpdk_datapath(false),
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

fn run_client<S, D>(loadgen: &mut YCSBClient<S, D>, connection: &mut D, opt: &Opt) -> Result<()>
where
    S: SerializedRequestGenerator<D>,
    D: Datapath,
{
    loadgen.init(connection)?;
    let timeout = match opt.retries {
        true => cornflakes_libos::high_timeout_at_start,
        false => cornflakes_libos::no_retries_timeout,
    };

    // if start cutoff is > 0, run_closed_loop for that many packets
    loadgen
        .run_closed_loop(connection, opt.start_cutoff as u64, timeout)
        .wrap_err("Failed to run warm up closed loop")?;

    let start_run = Instant::now();

    loadgen.run_open_loop(
        connection,
        (1e9 / opt.rate as f64) as u64,
        opt.time as u64,
        timeout,
        !opt.retries,
    )?;

    let exp_duration = start_run.elapsed();
    loadgen.dump(opt.logfile.clone(), exp_duration, opt.start_cutoff)?;
    let exp_time = exp_duration.as_nanos() as f64 / 1000000000.0;
    let achieved_load_pps = (loadgen.get_num_recved(opt.start_cutoff) as f64) / exp_time as f64;
    let achieved_load_gbps =
        ((opt.value_size * opt.num_values) as f64 * achieved_load_pps as f64) / (125000000 as f64);
    let load_gbps =
        (((opt.value_size * opt.num_values) as f64) * (opt.rate) as f64) / (125000000 as f64);
    tracing::info!(
        load_gbps =? load_gbps,
        achieved_load_gbps =? achieved_load_gbps,
        opt.rate =? opt.rate,
        achieved_load_pps =? achieved_load_pps,
        percent_achieved =? (achieved_load_gbps / load_gbps),
        "Load statistics:"
    );
    for timer_m in connection.get_timers().iter() {
        let timer = timer_m.lock().unwrap();
        timer.dump_stats();
    }
    Ok(())
}
