use color_eyre::eyre::{bail, Result};
use cornflakes_libos::{
    datapath::{Datapath, InlineMode},
    state_machine::client::ClientSM,
    state_machine::server::ServerSM,
};
use cornflakes_utils::{global_debug_init, AppMode};
use dpdk_datapath::{datapath::connection::DpdkConnection, dpdk_bindings};
use simple_echo::{
    client::SimpleEchoClient, run_client, run_datapath::*, run_server, server::SimpleEchoServer,
};
use structopt::StructOpt;

fn main() -> Result<()> {
    let opt = SimpleEchoOpt::from_args();
    global_debug_init(opt.trace_level)?;
    if opt.inline_mode != InlineMode::Nothing {
        bail!("DPDK datapath does not support inlining");
    }
    dpdk_bindings::load_mlx5_driver();

    match opt.mode {
        AppMode::Server => {
            run_server!(SimpleEchoServer<DpdkConnection>, DpdkConnection, opt);
        }
        AppMode::Client => {
            run_client!(SimpleEchoClient<DpdkConnection>, DpdkConnection, opt);
        }
    }
    Ok(())
}
