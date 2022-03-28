use color_eyre::eyre::{Result, WrapErr};
use cornflakes_libos::{
    loadgen::request_schedule::{DistributionType, PacketSchedule},
    timing::ManualHistogram,
    ClientSM, Datapath, ServerSM,
};
use cornflakes_utils::{
    global_debug_init, parse_server_port, AppMode, NetworkDatapath, SerializationType,
    SimpleMessageType, TraceLevel,
};
use data_structure_query::{
    client::EchoClient,
    cornflakes_dynamic::{CornflakesDynamicEchoClient, CornflakesDynamicSerializer},
    get_equal_fields,
    server::EchoServer,
    CerealizeClient, CerealizeMessage, EchoMode,
};
use dpdk_datapath::{datapath::connection::DPDKConnection, dpdk_bindings};
use std::{net::Ipv4Addr, process::exit, time::Instant};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "Ds Query App.", about = "Data structure echo benchmark.")]
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
    mode: EchoMode,
    #[structopt(
        short = "nd",
        long = "datapath",
        help = "Datapath to run",
        default_value = "dpdk"
    )]
    datapath: NetworkDatapath,
    #[structopt(
        long = "size",
        help = "Size of message to be sent.",
        default_value = "1000"
    )]
    size: usize,
    #[structopt(
        long = "server_size",
        help = "Size of message to be sent.",
        default_value = "1000"
    )]
    server_size: usize,
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
        short = "msg",
        long = "message",
        help = "Message type to echo",
        default_value = "single"
    )]
    message: SimpleMessageType,
    #[structopt(
        long = "server_message",
        help = "Message type to echo",
        default_value = "single"
    )]
    server_message: SimpleMessageType,
    #[structopt(
        short = "ser",
        long = "serialization",
        help = "Serialization library to use",
        default_value = "cornflakes-dynamic"
    )]
    serialization: SerializationType,
    #[structopt(long = "no_retries", help = "Disable client retries.")]
    no_retries: bool,
    #[structopt(long = "logfile", help = "Logfile to log all client RTTs.")]
    logfile: Option<String>,
    #[structopt(
        long = "deserialize",
        help = "Whether the server should deserialize the received buffer."
    )]
    deserialize_received: bool,
    #[structopt(
        long = "distribution",
        help = "Arrival distribution",
        default_value = "exponential"
    )]
    distribution: DistributionType,
}

macro_rules! init_ds_query_server(
    ($serializer: ty, $datapath: ty, $datapath_init: expr, $opt: ident) => {
        let mut connection = $datapath_init?;
        let mut echo_server: EchoServer<$serializer, $datapath> =
            EchoServer::new($opt.message, $opt.size, $opt.deserialize_received)?;
        set_ctrlc_handler(&echo_server)?;
        echo_server.init(&mut connection)?;
        echo_server.run_state_machine(&mut connection)?;
    }
);

macro_rules! init_ds_query_client(
    ($serializer: ty, $datapath: ty, $datapath_init: expr, $opt: ident) => {
        let server_sizes = get_equal_fields($opt.server_message, $opt.server_size);
        let sizes = get_equal_fields($opt.message, $opt.size);
        let hist = ManualHistogram::init($opt.rate, $opt.total_time);
        let mut connection = $datapath_init?;
        let mut echo_client: EchoClient<$serializer, $datapath> =
        EchoClient::new(
            $opt.server_ip,
            $opt.message,
            $opt.server_message,
            sizes,
            server_sizes,
            hist,
        )?;
        let schedule = PacketSchedule::new(($opt.rate * $opt.total_time * 2) as _, $opt.rate, $opt.distribution)?;
        let mut ctx = echo_client.new_context();
        echo_client.init_state(&mut ctx, &mut connection)?;
        run_client(&mut echo_client, &mut connection, &$opt, &schedule)?;
    }
);
fn main() -> Result<()> {
    let opt = Opt::from_args();
    global_debug_init(opt.trace_level)?;
    let mode = match &opt.mode {
        EchoMode::Server => AppMode::Server,
        EchoMode::Client => AppMode::Client,
    };

    let dpdk_datapath = |use_scatter_gather: bool| -> Result<DPDKConnection> {
        dpdk_bindings::load_mlx5_driver();
        let connection = DPDKConnection::new(&opt.config_file, mode, use_scatter_gather)
            .wrap_err("Failed to initialize DPDK server connection.")?;
        Ok(connection)
    };

    match opt.mode {
        EchoMode::Server => match (opt.datapath, opt.serialization) {
            (NetworkDatapath::DPDK, SerializationType::CornflakesDynamic) => {
                init_ds_query_server!(
                    CornflakesDynamicSerializer,
                    DPDKConnection,
                    dpdk_datapath(true),
                    opt
                );
            }
            (NetworkDatapath::DPDK, SerializationType::CornflakesOneCopyDynamic) => {
                init_ds_query_server!(
                    CornflakesDynamicSerializer,
                    DPDKConnection,
                    dpdk_datapath(false),
                    opt
                );
            }
            _ => {
                unimplemented!();
            }
        },
        EchoMode::Client => match (opt.datapath, opt.serialization) {
            (NetworkDatapath::DPDK, SerializationType::CornflakesDynamic) => {
                init_ds_query_client!(
                    CornflakesDynamicEchoClient,
                    DPDKConnection,
                    dpdk_datapath(true),
                    opt
                );
            }
            (NetworkDatapath::DPDK, SerializationType::CornflakesOneCopyDynamic) => {
                init_ds_query_client!(
                    CornflakesDynamicEchoClient,
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

fn run_client<'normal, S, D>(
    client: &mut EchoClient<'normal, S, D>,
    connection: &mut D,
    opt: &Opt,
    schedule: &PacketSchedule,
) -> Result<()>
where
    S: CerealizeClient<'normal, D>,
    D: Datapath,
{
    let server_port =
        parse_server_port(&opt.config_file).wrap_err("Failed to parse server port")?;
    client.init(connection)?;
    let start_run = Instant::now();
    let timeout = match opt.no_retries {
        false => cornflakes_libos::high_timeout_at_start,
        true => cornflakes_libos::no_retries_timeout,
    };
    client.run_open_loop(
        connection,
        schedule,
        opt.total_time,
        timeout,
        opt.no_retries,
        &opt.server_ip,
        server_port,
    )?;
    let exp_duration = start_run.elapsed();
    client.dump(opt.logfile.clone(), exp_duration)?;
    let exp_time = exp_duration.as_nanos() as f64 / 1000000000.0;
    let achieved_load_pps = (client.get_num_recved() as f64) / exp_time as f64;
    let achieved_load_gbps = (opt.size as f64 * achieved_load_pps as f64) / (125000000 as f64);
    let load_gbps = ((opt.size as f64) * (opt.rate) as f64) / (125000000 as f64);
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

fn set_ctrlc_handler<S, D>(server: &EchoServer<S, D>) -> Result<()>
where
    S: CerealizeMessage<D>,
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
