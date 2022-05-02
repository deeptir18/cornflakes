use color_eyre::eyre::Result;
use cornflakes_libos::{
    datapath::Datapath,
    state_machine::client::ClientSM,
    state_machine::server::ServerSM,
};
use cornflakes_utils::{global_debug_init, AppMode};
use linux_datapath::datapath::connection::LinuxConnection;
use simple_echo::{
    client::SimpleEchoClient, run_client, run_datapath::*, run_server, server::SimpleEchoServer,
};
use structopt::StructOpt;

fn main() -> Result<()> {
    let opt = SimpleEchoOpt::from_args();
    global_debug_init(opt.trace_level)?;

    match opt.mode {
        AppMode::Server => {
            run_server!(SimpleEchoServer<LinuxConnection>, LinuxConnection, opt);
        }
        AppMode::Client => {
            run_client!(SimpleEchoClient<LinuxConnection>, LinuxConnection, opt);
        }
    }
    Ok(())
}
