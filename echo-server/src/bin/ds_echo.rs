use color_eyre::eyre::{Result, WrapErr};
use cornflakes_libos::{
    dpdk_bindings,
    dpdk_libos::connection::{DPDKConnection, DPDKMode},
    ClientSM, Datapath, ServerSM,
};
use cornflakes_utils::{
    global_debug_init, NetworkDatapath, SerializationType, SimpleMessageType, TraceLevel,
};
use echo_server::{
    capnproto::{CapnprotoEchoClient, CapnprotoSerializer},
    client::EchoClient,
    cornflakes::{CornflakesEchoClient, CornflakesSerializer},
    flatbuffers::{FlatbuffersEchoClient, FlatbuffersSerializer},
    get_equal_fields,
    protobuf::{ProtobufEchoClient, ProtobufSerializer},
    server::EchoServer,
    CerealizeClient, CerealizeMessage, EchoMode,
};
use std::{net::Ipv4Addr, process::exit, time::Duration};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
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
        short = "f",
        long = "config_file",
        help = "Folder containing shared config information."
    )]
    config_file: String,
    #[structopt(short = "m", long = "mode", help = "Echo server or client mode.")]
    mode: EchoMode,
    #[structopt(
        short = "d",
        long = "datapath",
        help = "Datapath to run",
        default_value = "dpdk"
    )]
    datapath: NetworkDatapath,
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
        default_value = "cornflakes"
    )]
    serialization: SerializationType,
}

fn main() -> Result<()> {
    let opt = Opt::from_args();
    global_debug_init(opt.trace_level)?;
    let mode = match &opt.mode {
        EchoMode::Server => DPDKMode::Server,
        EchoMode::Client => DPDKMode::Client,
    };

    let dpdk_datapath = || -> Result<DPDKConnection> {
        #[cfg(feature = "mlx5")]
        {
            dpdk_bindings::load_mlx5_driver();
        }
        let connection = DPDKConnection::new(&opt.config_file, mode)
            .wrap_err("Failed to initialize DPDK server connection.")?;
        Ok(connection)
    };

    match opt.mode {
        EchoMode::Server => match (opt.datapath, opt.serialization) {
            (NetworkDatapath::DPDK, SerializationType::Cornflakes) => {
                let mut connection = dpdk_datapath()?;
                let serializer = CornflakesSerializer::new(opt.message, opt.size);
                let mut echo_server = EchoServer::new(serializer);
                set_ctrlc_handler(&echo_server)?;
                echo_server.init(&mut connection)?;
                echo_server.run_state_machine(&mut connection)?;
            }
            (NetworkDatapath::DPDK, SerializationType::CornflakesOneCopy) => {
                unimplemented!();
            }
            (NetworkDatapath::DPDK, SerializationType::Protobuf) => {
                let mut connection = dpdk_datapath()?;
                let serializer = ProtobufSerializer::new(opt.message);
                let mut echo_server = EchoServer::new(serializer);
                set_ctrlc_handler(&echo_server)?;
                echo_server.init(&mut connection)?;
                echo_server.run_state_machine(&mut connection)?;
            }
            (NetworkDatapath::DPDK, SerializationType::Flatbuffers) => {
                let mut connection = dpdk_datapath()?;
                let serializer = FlatbuffersSerializer::new(opt.message);
                let mut echo_server = EchoServer::new(serializer);
                set_ctrlc_handler(&echo_server)?;
                echo_server.init(&mut connection)?;
                echo_server.run_state_machine(&mut connection)?;
            }
            (NetworkDatapath::DPDK, SerializationType::Capnproto) => {
                let mut connection = dpdk_datapath()?;
                let serializer = CapnprotoSerializer::new(opt.message);
                let mut echo_server = EchoServer::new(serializer);
                set_ctrlc_handler(&echo_server)?;
                echo_server.init(&mut connection)?;
                echo_server.run_state_machine(&mut connection)?;
            }
        },
        EchoMode::Client => match (opt.datapath, opt.serialization) {
            (NetworkDatapath::DPDK, SerializationType::Cornflakes) => {
                let sizes = get_equal_fields(opt.message, opt.size);
                let mut connection = dpdk_datapath()?;
                let mut echo_client: EchoClient<CornflakesEchoClient, DPDKConnection> =
                    EchoClient::new(opt.server_ip, opt.message, sizes)?;
                let mut ctx = echo_client.new_context();
                echo_client.init_state(&mut ctx, &mut connection)?;
                run_client(&mut echo_client, &mut connection, &opt)?;
            }
            (NetworkDatapath::DPDK, SerializationType::CornflakesOneCopy) => {
                unimplemented!();
            }
            (NetworkDatapath::DPDK, SerializationType::Protobuf) => {
                let sizes = get_equal_fields(opt.message, opt.size);
                let mut connection = dpdk_datapath()?;
                let mut echo_client: EchoClient<ProtobufEchoClient, DPDKConnection> =
                    EchoClient::new(opt.server_ip, opt.message, sizes)?;
                let mut ctx = echo_client.new_context();
                echo_client.init_state(&mut ctx, &mut connection)?;
                run_client(&mut echo_client, &mut connection, &opt)?;
            }
            (NetworkDatapath::DPDK, SerializationType::Flatbuffers) => {
                let sizes = get_equal_fields(opt.message, opt.size);
                let mut connection = dpdk_datapath()?;
                let mut echo_client: EchoClient<FlatbuffersEchoClient, DPDKConnection> =
                    EchoClient::new(opt.server_ip, opt.message, sizes)?;
                let mut ctx = echo_client.new_context();
                echo_client.init_state(&mut ctx, &mut connection)?;
                run_client(&mut echo_client, &mut connection, &opt)?;
            }
            (NetworkDatapath::DPDK, SerializationType::Capnproto) => {
                let sizes = get_equal_fields(opt.message, opt.size);
                let mut connection = dpdk_datapath()?;
                let mut echo_client: EchoClient<CapnprotoEchoClient, DPDKConnection> =
                    EchoClient::new(opt.server_ip, opt.message, sizes)?;
                let mut ctx = echo_client.new_context();
                echo_client.init_state(&mut ctx, &mut connection)?;
                run_client(&mut echo_client, &mut connection, &opt)?;
            }
        },
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
    client.run_open_loop(
        connection,
        (1e9 / opt.rate as f64) as u64,
        opt.total_time,
        Duration::new(0, 100000),
    )?;
    client.dump_stats();
    let load = ((opt.size as f64) * (opt.rate) as f64) / (125000000 as f64);
    tracing::info!(load_gbps = ?load, "Sent at rate:");
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
