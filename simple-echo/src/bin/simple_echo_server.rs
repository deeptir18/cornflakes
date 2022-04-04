use color_eyre::eyre::{bail, Result};
use cornflakes_libos::{
    datapath::{Datapath, PushBufType},
    state_machine::server::ServerSM,
};
use cornflakes_utils::{global_debug_init, AppMode, NetworkDatapath, TraceLevel};
use dpdk_datapath::{datapath::connection::DpdkConnection, dpdk_bindings};
use mlx5_datapath::datapath::connection::{InlineMode, Mlx5Connection};
use simple_echo::{server::SimpleEchoServer, RequestShape};
use std::net::Ipv4Addr;
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
        short = "ip",
        long = "server_ip",
        help = "Server IP Address",
        default_value = "127.0.0.1"
    )]
    server_ip: Ipv4Addr,
    #[structopt(
        short = "dp",
        long = "datapath",
        help = "Datapath to use",
        default_value = "mlx5"
    )]
    datapath: NetworkDatapath,
    #[structopt(
        short = "api",
        long = "push_api",
        help = "Whether to use Sga, RcSga, or Copy to single buf API"
    )]
    push_buf_type: PushBufType,
    #[structopt(
        short = "inline",
        long = "inline_mode",
        help = "For Mlx5 datapath, which inline mode to use. Note this can't be set for DPDK datapath.",
        default_value = "nothing"
    )]
    inline_mode: InlineMode,
    #[structopt(
        short = "copy_thresh",
        long = "copy_threshold",
        help = "Datapath copy threshold. Copies everything below this threshold. If set to infinity, tries to use zero-copy for everything. If set to 0, uses zero-copy for nothing.",
        default_value = "256"
    )]
    copying_threshold: usize,
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
        if opt.inline_mode != InlineMode::Nothing {
            bail!("DPDK datapath does not support inlining");
        }
        dpdk_bindings::load_mlx5_driver();
    }

    match opt.datapath {
        NetworkDatapath::DPDK => {
            let mut datapath_params =
                <DpdkConnection as Datapath>::parse_config_file(&opt.config_file, &opt.server_ip)?;
            let addresses = <DpdkConnection as Datapath>::compute_affinity(
                &datapath_params,
                1,
                None,
                AppMode::Server,
            )?;
            let per_thread_contexts =
                <DpdkConnection as Datapath>::global_init(1, &mut datapath_params, addresses)?;
            let mut connection = <DpdkConnection as Datapath>::per_thread_init(
                datapath_params,
                per_thread_contexts.into_iter().nth(0).unwrap(),
                AppMode::Server,
            )?;

            connection.set_copying_threshold(opt.copying_threshold);

            // init echo server
            let mut echo_server: SimpleEchoServer<DpdkConnection> =
                SimpleEchoServer::new(opt.push_buf_type, opt.request_shape);

            echo_server.init(&mut connection)?;

            echo_server.run_state_machine(&mut connection)?;
        }
        NetworkDatapath::MLX5 => {
            let mut datapath_params =
                <Mlx5Connection as Datapath>::parse_config_file(&opt.config_file, &opt.server_ip)?;
            let addresses = <Mlx5Connection as Datapath>::compute_affinity(
                &datapath_params,
                1,
                None,
                AppMode::Server,
            )?;
            let per_thread_contexts =
                <Mlx5Connection as Datapath>::global_init(1, &mut datapath_params, addresses)?;

            let mut connection = <Mlx5Connection as Datapath>::per_thread_init(
                datapath_params,
                per_thread_contexts.into_iter().nth(0).unwrap(),
                AppMode::Server,
            )?;

            // set command line parameters on copying threshold
            connection.set_copying_threshold(opt.copying_threshold);
            connection.set_inline_mode(opt.inline_mode);

            // initialize echo server object
            let mut echo_server: SimpleEchoServer<Mlx5Connection> =
                SimpleEchoServer::new(opt.push_buf_type, opt.request_shape);

            // initialize echo server
            echo_server.init(&mut connection)?;

            // run the state machine
            echo_server.run_state_machine(&mut connection)?;
        }
    }

    Ok(())
}
