use color_eyre::eyre::Result;
use cornflakes_libos::{
    datapath::Datapath, state_machine::client::ClientSM, state_machine::server::ServerSM,
};
use cornflakes_utils::{global_debug_init, AppMode, SerializationType};
use ds_echo::{
    capnproto::{CapnprotoEchoClient, CapnprotoSerializer},
    cornflakes_dynamic::{CornflakesEchoClient, CornflakesSerializer},
    flatbuffers::{FlatbuffersEchoClient, FlatbuffersSerializer},
    get_equal_fields,
    protobuf::{ProtobufEchoClient, ProtobufSerializer},
    run_client,
    run_datapath::*,
    run_server, EchoClient,
};
use mlx5_datapath::datapath::connection::Mlx5Connection;
use structopt::StructOpt;

fn main() -> Result<()> {
    let mut opt = DsEchoOpt::from_args();
    global_debug_init(opt.trace_level)?;
    check_opt(&mut opt)?;

    match opt.mode {
        AppMode::Server => match opt.serialization {
            SerializationType::CornflakesDynamic | SerializationType::CornflakesOneCopyDynamic => {
                run_server!(CornflakesSerializer<Mlx5Connection>, Mlx5Connection, opt);
            }
            SerializationType::Flatbuffers => {
                run_server!(FlatbuffersSerializer<Mlx5Connection>, Mlx5Connection, opt);
            }
            SerializationType::Capnproto => {
                run_server!(CapnprotoSerializer<Mlx5Connection>, Mlx5Connection, opt);
            }
            SerializationType::Protobuf => {
                run_server!(ProtobufSerializer<Mlx5Connection>, Mlx5Connection, opt);
            }
            _ => {
                unimplemented!();
            }
        },
        AppMode::Client => match opt.serialization {
            SerializationType::CornflakesDynamic | SerializationType::CornflakesOneCopyDynamic => {
                run_client!(CornflakesEchoClient, Mlx5Connection, opt);
            }
            SerializationType::Flatbuffers => {
                run_client!(FlatbuffersEchoClient, Mlx5Connection, opt);
            }
            SerializationType::Capnproto => {
                run_client!(CapnprotoEchoClient, Mlx5Connection, opt);
            }
            SerializationType::Protobuf => {
                run_client!(ProtobufEchoClient, Mlx5Connection, opt);
            }
            _ => {
                unimplemented!();
            }
        },
    }
    Ok(())
}
