use color_eyre::eyre::{bail, Result, WrapErr};
use cornflakes_libos::{
    dpdk_bindings,
    dpdk_libos::connection::{DPDKConnection, DPDKMode, RecvMode},
    timing::ManualHistogram,
    ClientSM, Datapath, ServerSM,
};
use cornflakes_utils::{
    global_debug_init, NetworkDatapath, SerializationType, SimpleMessageType, TraceLevel,
};
use data_structure_query::{
    //capnproto::{CapnprotoEchoClient, CapnprotoSerializer},
    client::EchoClient,
    cornflakes_dynamic::{CornflakesDynamicEchoClient, CornflakesDynamicSerializer},
    //cornflakes_fixed::{CornflakesFixedEchoClient, CornflakesFixedSerializer},
    //flatbuffers::{FlatbuffersEchoClient, FlatbuffersSerializer},
    get_equal_fields,
    //protobuf::{ProtobufEchoClient, ProtobufSerializer},
    server::EchoServer,
    CerealizeClient,
    CerealizeMessage,
    EchoMode,
};
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
    #[structopt(
        short = "z",
        long = "zero_copy_recv",
        help = "Enable zero-copy on receive side"
    )]
    zero_copy_recv: bool,
    #[structopt(
        long = "copy_to_dma_memory",
        help = "Enable copying out to registered memory"
    )]
    copy_to_dmable_memory: bool,
    #[structopt(long = "no_retries", help = "Disable client retries.")]
    no_retries: bool,
    #[structopt(long = "logfile", help = "Logfile to log all client RTTs.")]
    logfile: Option<String>,
    #[structopt(
        long = "use_native_buffers",
        help = "For debugging, when allocating payloads, use natively allocated mbufs."
    )]
    use_native_buffers: bool,
    #[structopt(long = "prepend_header", help = "Prepend header to first mbuf region")]
    prepend_header: bool,
    #[structopt(
        long = "deserialize",
        help = "Whether the server should deserialize the received buffer."
    )]
    deserialize_received: bool,
}

fn main() -> Result<()> {
    let opt = Opt::from_args();
    if (opt.prepend_header || opt.use_native_buffers)
        && (opt.serialization != SerializationType::CornflakesDynamic
            && opt.serialization != SerializationType::CornflakesFixed)
    {
        tracing::warn!(prepend_header=opt.prepend_header, native_buffers = opt.use_native_buffers,
            serialization =? opt.serialization, "Flags prepend_header and native_buffers should only be true when using cornflakes serialization.");
        bail!("Invalid options.");
    }

    global_debug_init(opt.trace_level)?;
    let mode = match &opt.mode {
        EchoMode::Server => DPDKMode::Server,
        EchoMode::Client => DPDKMode::Client,
    };

    let dpdk_datapath = |zero_copy: bool,
                         copy_to_dmable_memory: bool,
                         use_scatter_gather: bool|
     -> Result<DPDKConnection> {
        let recv_mode = match (zero_copy, copy_to_dmable_memory) {
            (true, true) => {
                bail!("Can't have zero-copy-recv and copying to dmable memory turned on.");
            }
            (true, false) => RecvMode::ZeroCopyRecv,
            (false, true) => RecvMode::CopyToMbuf,
            (false, false) => RecvMode::CopyOut,
        };
        dpdk_bindings::load_mlx5_driver();
        let connection = DPDKConnection::new(
            &opt.config_file,
            mode,
            recv_mode,
            use_scatter_gather,
            !opt.use_native_buffers,
            opt.prepend_header,
        )
        .wrap_err("Failed to initialize DPDK server connection.")?;
        Ok(connection)
    };

    match opt.mode {
        EchoMode::Server => match (opt.datapath, opt.serialization) {
            (NetworkDatapath::DPDK, SerializationType::CornflakesDynamic) => {
                let mut connection =
                    dpdk_datapath(opt.zero_copy_recv, opt.copy_to_dmable_memory, true)?;
                let mut echo_server: EchoServer<CornflakesDynamicSerializer, DPDKConnection> =
                    EchoServer::new(
                        opt.message,
                        opt.size,
                        opt.deserialize_received,
                        opt.use_native_buffers,
                        opt.prepend_header,
                    )?;
                set_ctrlc_handler(&echo_server)?;
                echo_server.init(&mut connection)?;
                echo_server.run_state_machine(&mut connection)?;
            }
            (NetworkDatapath::DPDK, SerializationType::CornflakesFixed) => {
                /* let mut connection =
                    dpdk_datapath(opt.zero_copy_recv, opt.copy_to_dmable_memory, true)?;
                let mut echo_server: EchoServer<CornflakesFixedSerializer, DPDKConnection> =
                    EchoServer::new(
                        opt.message,
                        opt.size,
                        opt.deserialize_received,
                        opt.use_native_buffers,
                        opt.prepend_header,
                    )?;
                set_ctrlc_handler(&echo_server)?;
                echo_server.init(&mut connection)?;
                echo_server.run_state_machine(&mut connection)?;*/
            }
            (NetworkDatapath::DPDK, SerializationType::CornflakesOneCopyDynamic) => {
                let mut connection =
                    dpdk_datapath(opt.zero_copy_recv, opt.copy_to_dmable_memory, false)?;
                let mut echo_server: EchoServer<CornflakesDynamicSerializer, DPDKConnection> =
                    EchoServer::new(
                        opt.message,
                        opt.size,
                        opt.deserialize_received,
                        opt.use_native_buffers,
                        opt.prepend_header,
                    )?;
                set_ctrlc_handler(&echo_server)?;
                echo_server.init(&mut connection)?;
                echo_server.run_state_machine(&mut connection)?;
            }
            (NetworkDatapath::DPDK, SerializationType::CornflakesOneCopyFixed) => {
                /*let mut connection =
                    dpdk_datapath(opt.zero_copy_recv, opt.copy_to_dmable_memory, false)?;
                let mut echo_server: EchoServer<CornflakesFixedSerializer, DPDKConnection> =
                    EchoServer::new(
                        opt.message,
                        opt.size,
                        opt.deserialize_received,
                        opt.use_native_buffers,
                        opt.prepend_header,
                    )?;
                let mut echo_server = EchoServer::new(serializer);
                set_ctrlc_handler(&echo_server)?;
                echo_server.init(&mut connection)?;
                echo_server.run_state_machine(&mut connection)?;*/
            }
            (NetworkDatapath::DPDK, SerializationType::Protobuf) => {
                /*let mut connection =
                    dpdk_datapath(opt.zero_copy_recv, opt.copy_to_dmable_memory, false)?;
                let serializer = ProtobufSerializer::new(opt.message, opt.size,
                    opt.deserialize_received,
                    opt.use_native_buffers,
                    opt.prepend_header,
                );
                let mut echo_server = EchoServer::new(serializer);
                set_ctrlc_handler(&echo_server)?;
                echo_server.init(&mut connection)?;
                echo_server.run_state_machine(&mut connection)?;*/
            }
            (NetworkDatapath::DPDK, SerializationType::Flatbuffers) => {
                /*let mut connection =
                    dpdk_datapath(opt.zero_copy_recv, opt.copy_to_dmable_memory, false)?;
                let serializer = FlatbuffersSerializer::new(opt.message, opt.size,
                    opt.deserialize_received,
                    opt.use_native_buffers,
                    opt.prepend_header,
                );
                let mut echo_server = EchoServer::new(serializer);
                set_ctrlc_handler(&echo_server)?;
                echo_server.init(&mut connection)?;
                echo_server.run_state_machine(&mut connection)?;*/
            }
            (NetworkDatapath::DPDK, SerializationType::Capnproto) => {
                /*let mut connection =
                    dpdk_datapath(opt.zero_copy_recv, opt.copy_to_dmable_memory, false)?;
                let serializer = CapnprotoSerializer::new(opt.message, opt.size,
                    opt.deserialize_received,
                    opt.use_native_buffers,
                    opt.prepend_header,
                );
                let mut echo_server = EchoServer::new(serializer);
                set_ctrlc_handler(&echo_server)?;
                echo_server.init(&mut connection)?;
                echo_server.run_state_machine(&mut connection)?;*/
            }
        },
        EchoMode::Client => {
            let sizes = get_equal_fields(opt.message, opt.size);
            let server_sizes = get_equal_fields(opt.server_message, opt.server_size);
            let hist = ManualHistogram::init(opt.rate, opt.total_time);
            match (opt.datapath, opt.serialization) {
                (NetworkDatapath::DPDK, SerializationType::CornflakesDynamic) => {
                    let mut connection =
                        dpdk_datapath(opt.zero_copy_recv, opt.copy_to_dmable_memory, true)?;
                    let mut echo_client: EchoClient<CornflakesDynamicEchoClient, DPDKConnection> =
                        EchoClient::new(
                            opt.server_ip,
                            opt.message,
                            opt.server_message,
                            sizes,
                            server_sizes,
                            hist,
                        )?;
                    let mut ctx = echo_client.new_context();
                    echo_client.init_state(&mut ctx, &mut connection)?;
                    run_client(&mut echo_client, &mut connection, &opt)?;
                }
                (NetworkDatapath::DPDK, SerializationType::CornflakesFixed) => {
                    /*let mut connection =
                        dpdk_datapath(opt.zero_copy_recv, opt.copy_to_dmable_memory, true)?;
                    let mut echo_client: EchoClient<CornflakesFixedEchoClient, DPDKConnection> =
                        EchoClient::new(
                            opt.server_ip,
                            opt.message,
                            opt.server_message,
                            sizes,
                            server_sizes,
                            hist,
                        )?;
                    let mut ctx = echo_client.new_context();
                    echo_client.init_state(&mut ctx, &mut connection)?;
                    run_client(&mut echo_client, &mut connection, &opt)?;*/
                }
                (NetworkDatapath::DPDK, SerializationType::CornflakesOneCopyDynamic) => {
                    let mut connection =
                        dpdk_datapath(opt.zero_copy_recv, opt.copy_to_dmable_memory, false)?;
                    let mut echo_client: EchoClient<CornflakesDynamicEchoClient, DPDKConnection> =
                        EchoClient::new(
                            opt.server_ip,
                            opt.message,
                            opt.server_message,
                            sizes,
                            server_sizes,
                            hist,
                        )?;

                    let mut ctx = echo_client.new_context();
                    echo_client.init_state(&mut ctx, &mut connection)?;
                    run_client(&mut echo_client, &mut connection, &opt)?;
                }
                (NetworkDatapath::DPDK, SerializationType::CornflakesOneCopyFixed) => {
                    /*let mut connection =
                        dpdk_datapath(opt.zero_copy_recv, opt.copy_to_dmable_memory, false)?;
                    let mut echo_client: EchoClient<CornflakesFixedEchoClient, DPDKConnection> =
                        EchoClient::new(
                            opt.server_ip,
                            opt.message,
                            opt.server_message,
                            sizes,
                            server_sizes,
                            hist,
                        )?;
                    let mut ctx = echo_client.new_context();
                    echo_client.init_state(&mut ctx, &mut connection)?;
                    run_client(&mut echo_client, &mut connection, &opt)?;*/
                }
                (NetworkDatapath::DPDK, SerializationType::Protobuf) => {
                    /*let mut connection =
                        dpdk_datapath(opt.zero_copy_recv, opt.copy_to_dmable_memory, false)?;
                    let mut echo_client: EchoClient<ProtobufEchoClient, DPDKConnection> =
                        EchoClient::new(
                            opt.server_ip,
                            opt.message,
                            opt.server_message,
                            sizes,
                            server_sizes,
                            hist,
                        )?;
                    let mut ctx = echo_client.new_context();
                    echo_client.init_state(&mut ctx, &mut connection)?;
                    run_client(&mut echo_client, &mut connection, &opt)?;*/
                }
                (NetworkDatapath::DPDK, SerializationType::Flatbuffers) => {
                    /*let mut connection =
                        dpdk_datapath(opt.zero_copy_recv, opt.copy_to_dmable_memory, false)?;
                    let mut echo_client: EchoClient<FlatbuffersEchoClient, DPDKConnection> =
                        EchoClient::new(
                            opt.server_ip,
                            opt.message,
                            opt.server_message,
                            sizes,
                            server_sizes,
                            hist,
                        )?;
                    let mut ctx = echo_client.new_context();
                    echo_client.init_state(&mut ctx, &mut connection)?;
                    run_client(&mut echo_client, &mut connection, &opt)?;*/
                }
                (NetworkDatapath::DPDK, SerializationType::Capnproto) => {
                    /*let mut connection =
                        dpdk_datapath(opt.zero_copy_recv, opt.copy_to_dmable_memory, false)?;
                    let mut echo_client: EchoClient<CapnprotoEchoClient, DPDKConnection> =
                        EchoClient::new(
                            opt.server_ip,
                            opt.message,
                            opt.server_message,
                            sizes,
                            server_sizes,
                            hist,
                        )?;
                    let mut ctx = echo_client.new_context();
                    echo_client.init_state(&mut ctx, &mut connection)?;
                    run_client(&mut echo_client, &mut connection, &opt)?;*/
                }
            }
        }
    }

    Ok(())
}

fn run_client<'normal, S, D>(
    client: &mut EchoClient<'normal, S, D>,
    connection: &mut D,
    opt: &Opt,
) -> Result<()>
where
    S: CerealizeClient<'normal, D>,
    D: Datapath,
{
    client.init(connection)?;
    let start_run = Instant::now();
    let timeout = match opt.no_retries {
        false => cornflakes_libos::high_timeout_at_start,
        true => cornflakes_libos::no_retries_timeout,
    };
    client.run_open_loop(
        connection,
        (1e9 / opt.rate as f64) as u64,
        opt.total_time,
        timeout,
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
