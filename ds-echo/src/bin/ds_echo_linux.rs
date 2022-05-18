use color_eyre::eyre::Result;
use cornflakes_libos::{
    datapath::Datapath, state_machine::client::ClientSM, state_machine::server::ServerSM,
};
use cornflakes_utils::{global_debug_init, AppMode, SerializationType};
use ds_echo::{
    cornflakes_dynamic::{CornflakesEchoClient, CornflakesSerializer},
    get_equal_fields, run_client,
    run_datapath::*,
    run_server, EchoClient,
};
use linux_datapath::datapath::connection::LinuxConnection;
use structopt::StructOpt;

fn main() -> Result<()> {
    let mut opt = DsEchoOpt::from_args();
    global_debug_init(opt.trace_level)?;
    check_opt(&mut opt)?;

    match opt.mode {
        AppMode::Server => match opt.serialization {
            SerializationType::CornflakesDynamic | SerializationType::CornflakesOneCopyDynamic => {
                run_server!(CornflakesSerializer<LinuxConnection>, LinuxConnection, opt);
            }
            _ => {
                unimplemented!();
            }
        },
        AppMode::Client => match opt.serialization {
            SerializationType::CornflakesDynamic | SerializationType::CornflakesOneCopyDynamic => {
                run_client!(CornflakesEchoClient, LinuxConnection, opt);
            }
            _ => {
                unimplemented!();
            }
        },
    }
    Ok(())
}
