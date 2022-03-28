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
    turn_off_ref_counting,
    utils::AddressInfo,
    ClientSM, Datapath, ServerSM,
};
use cornflakes_utils::{
    get_thread_latlog, global_debug_init, parse_server_port, AppMode, NetworkDatapath,
    SerializationType, SimpleMessageType, TraceLevel,
};
use echo_server::{
    baselines::{BaselineClient, IdealSerializer, OneCopySerializer, TwoCopySerializer},
    capnproto::{CapnprotoEchoClient, CapnprotoSerializer},
    cereal::{CerealEchoClient, CerealSerializer},
    client::EchoClient,
    cornflakes_dynamic::{CornflakesDynamicEchoClient, CornflakesDynamicSerializer},
    //cornflakes_fixed::{CornflakesFixedEchoClient, CornflakesFixedSerializer},
    flatbuffers::{FlatbuffersEchoClient, FlatbuffersSerializer},
    get_equal_fields,
    protobuf::{ProtobufEchoClient, ProtobufSerializer},
    server::EchoServer,
    CerealizeClient,
    CerealizeMessage,
};
use std::{
    net::Ipv4Addr,
    process::exit,
    thread::{spawn, JoinHandle},
    time::Instant,
};
use structopt::StructOpt;

#[derive(Debug, StructOpt, Clone)]
#[structopt(name = "Echo Server app.", about = "Data structure echo benchmark.")]
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
        short = "ser",
        long = "serialization",
        help = "Serialization library to use",
        default_value = "cornflakes-dynamic"
    )]
    serialization: SerializationType,
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
    #[structopt(long = "no_ref_counting", help = "Turn off ref counting")]
    no_ref_counting: bool,
}

macro_rules! init_echo_server(
    ($serializer: ty, $datapath_init: expr, $opt: ident) => {
        let mut connection = $datapath_init?;
        let serializer = <$serializer>::new($opt.message, $opt.size);
        let mut echo_server = EchoServer::new(serializer);
        if $opt.serialization == SerializationType::IdealBaseline {
            echo_server.set_echo_pkt_mode();
        }
        set_ctrlc_handler(&echo_server)?;
        echo_server.init(&mut connection)?;
        echo_server.run_state_machine(&mut connection)?;
    }
);

macro_rules! init_echo_client(
    ($serializer: ty, $datapath: ty, $datapath_global_init: expr, $datapath_init: ident, $opt: ident) => {
        let sizes = get_equal_fields($opt.message, $opt.size);
        let num_rtts = ($opt.rate * $opt.total_time * 2) as usize;
        let schedules = generate_schedules(num_rtts, $opt.rate, $opt.distribution, $opt.num_threads)?;
        let (port, per_thread_info) = $datapath_global_init?;
        let mut threads: Vec<JoinHandle<Result<ThreadStats>>> = vec![];

        for i in 0..$opt.num_threads {
            let physical_port = port;
            let (rx_packet_allocator, addr) = per_thread_info[i].clone();
            let per_thread_options = $opt.clone();
            let hist = ManualHistogram::new(num_rtts);
            let per_thread_sizes = sizes.clone();
            let schedule = schedules[i].clone();
            threads.push(spawn(move || {
                tracing::info!(ref_counting = unsafe {cornflakes_libos::USING_REF_COUNTING}, "Ref counting mode");
                match set_thread_affinity(&vec![i+1]) {
                    Ok(_) => {}
                    Err(e) => {
                        bail!("Could not set thread affinity for thread {} on core {}: {:?}", i, i+1, e);
                    }
                }

                let mut connection = $datapath_init(physical_port, i, rx_packet_allocator, addr, &per_thread_options)?;
                let mut echo_client: EchoClient<$serializer, $datapath> =
                    EchoClient::new(per_thread_options.server_ip, per_thread_options.message, per_thread_sizes, hist)?;
                let mut ctx = echo_client.new_context();
                echo_client.init_state(&mut ctx, &mut connection)?;
                run_client(i, &mut echo_client, &mut connection, &per_thread_options, &schedule).wrap_err("Failed to run client")
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

fn main() -> Result<()> {
    let opt = Opt::from_args();
    global_debug_init(opt.trace_level)?;
    if opt.no_ref_counting {
        turn_off_ref_counting();
    }

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

    let dpdk_datapath = |use_scatter_gather: bool| -> Result<DPDKConnection> {
        dpdk_bindings::load_mlx5_driver();
        let connection = DPDKConnection::new(&opt.config_file, opt.mode, use_scatter_gather)
            .wrap_err("Failed to initialize DPDK server connection.")?;
        Ok(connection)
    };

    match opt.mode {
        AppMode::Server => match (opt.datapath, opt.serialization) {
            (NetworkDatapath::DPDK, SerializationType::CornflakesDynamic) => {
                init_echo_server!(
                    CornflakesDynamicSerializer<DPDKConnection>,
                    dpdk_datapath(true),
                    opt
                );
            }
            /*(NetworkDatapath::DPDK, SerializationType::CornflakesFixed) => {
                init_echo_server!(CornflakesFixedSerializer, dpdk_datapath(true), opt);
            }*/
            (NetworkDatapath::DPDK, SerializationType::CornflakesOneCopyDynamic) => {
                init_echo_server!(
                    CornflakesDynamicSerializer<DPDKConnection>,
                    dpdk_datapath(false),
                    opt
                );
            }
            /*(NetworkDatapath::DPDK, SerializationType::CornflakesOneCopyFixed) => {
                init_echo_server!(CornflakesFixedSerializer, dpdk_datapath(false), opt);
            }*/
            (NetworkDatapath::DPDK, SerializationType::Protobuf) => {
                init_echo_server!(ProtobufSerializer, dpdk_datapath(false), opt);
            }
            (NetworkDatapath::DPDK, SerializationType::Flatbuffers) => {
                init_echo_server!(FlatbuffersSerializer, dpdk_datapath(false), opt);
            }
            (NetworkDatapath::DPDK, SerializationType::Capnproto) => {
                init_echo_server!(CapnprotoSerializer, dpdk_datapath(false), opt);
            }
            (NetworkDatapath::DPDK, SerializationType::Cereal) => {
                init_echo_server!(CerealSerializer, dpdk_datapath(false), opt);
            }
            (NetworkDatapath::DPDK, SerializationType::OneCopyBaseline) => {
                init_echo_server!(OneCopySerializer, dpdk_datapath(false), opt);
            }
            (NetworkDatapath::DPDK, SerializationType::TwoCopyBaseline) => {
                init_echo_server!(TwoCopySerializer, dpdk_datapath(false), opt);
            }
            (NetworkDatapath::DPDK, SerializationType::IdealBaseline) => {
                init_echo_server!(IdealSerializer, dpdk_datapath(false), opt);
            }
            _ => {
                unimplemented!();
            }
        },
        AppMode::Client => match (opt.datapath, opt.serialization) {
            (NetworkDatapath::DPDK, SerializationType::CornflakesDynamic) => {
                init_echo_client!(
                    CornflakesDynamicEchoClient<DPDKConnection>,
                    DPDKConnection,
                    dpdk_global_init(),
                    dpdk_per_thread_init,
                    opt
                );
            }
            /*(NetworkDatapath::DPDK, SerializationType::CornflakesFixed) => {
                init_echo_client!(
                    CornflakesFixedEchoClient,
                    DPDKConnection,
                    dpdk_global_init(),
                    dpdk_per_thread_init,
                    opt
                );
            }*/
            (NetworkDatapath::DPDK, SerializationType::CornflakesOneCopyDynamic) => {
                init_echo_client!(
                    CornflakesDynamicEchoClient<DPDKConnection>,
                    DPDKConnection,
                    dpdk_global_init(),
                    dpdk_per_thread_init,
                    opt
                );
            }
            /*(NetworkDatapath::DPDK, SerializationType::CornflakesOneCopyFixed) => {
                init_echo_client!(
                    CornflakesFixedEchoClient,
                    DPDKConnection,
                    dpdk_global_init(),
                    dpdk_per_thread_init,
                    opt
                );
            }*/
            (NetworkDatapath::DPDK, SerializationType::Protobuf) => {
                init_echo_client!(
                    ProtobufEchoClient,
                    DPDKConnection,
                    dpdk_global_init(),
                    dpdk_per_thread_init,
                    opt
                );
            }
            (NetworkDatapath::DPDK, SerializationType::Flatbuffers) => {
                init_echo_client!(
                    FlatbuffersEchoClient,
                    DPDKConnection,
                    dpdk_global_init(),
                    dpdk_per_thread_init,
                    opt
                );
            }
            (NetworkDatapath::DPDK, SerializationType::Capnproto) => {
                init_echo_client!(
                    CapnprotoEchoClient,
                    DPDKConnection,
                    dpdk_global_init(),
                    dpdk_per_thread_init,
                    opt
                );
            }
            (NetworkDatapath::DPDK, SerializationType::Cereal) => {
                init_echo_client!(
                    CerealEchoClient,
                    DPDKConnection,
                    dpdk_global_init(),
                    dpdk_per_thread_init,
                    opt
                );
            }
            (NetworkDatapath::DPDK, SerializationType::OneCopyBaseline)
            | (NetworkDatapath::DPDK, SerializationType::TwoCopyBaseline)
            | (NetworkDatapath::DPDK, SerializationType::IdealBaseline) => {
                init_echo_client!(
                    BaselineClient<DPDKConnection>,
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

fn run_client<'normal, S, D>(
    thread_id: usize,
    client: &mut EchoClient<'normal, S, D>,
    connection: &mut D,
    opt: &Opt,
    schedule: &PacketSchedule,
) -> Result<ThreadStats>
where
    S: CerealizeClient<'normal, D>,
    D: Datapath,
{
    let server_port =
        parse_server_port(&opt.config_file).wrap_err("Failed to parse server port")?;
    client.init(connection)?;
    let start_run = Instant::now();
    let timeout = match opt.retries {
        true => cornflakes_libos::high_timeout_at_start,
        false => cornflakes_libos::no_retries_timeout,
    };
    client.run_open_loop(
        connection,
        schedule,
        opt.total_time,
        timeout,
        !opt.retries,
        &opt.server_ip,
        server_port,
    )?;

    let exp_duration = start_run.elapsed().as_nanos();
    client.sort_rtts(0)?;

    // per thread log latency
    match &opt.logfile {
        Some(x) => {
            let path = get_thread_latlog(&x, thread_id)?;
            tracing::debug!("Going to log latencies into {}", path);
            client.log_rtts(&path, 0)?;
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
        client.get_num_sent(0),
        client.get_num_recved(0),
        client.get_num_retries(),
        exp_duration as _,
        opt.rate,
        opt.size,
        client.get_mut_rtts(),
        0,
    )?;

    Ok(stats)
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
