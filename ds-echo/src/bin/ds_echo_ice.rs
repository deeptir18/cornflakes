use color_eyre::eyre::Result;
use cornflakes_libos::{
    datapath::Datapath, state_machine::client::ClientSM, state_machine::server::ServerSM,
};
use cornflakes_utils::{global_debug_init, AppMode, SerializationType};
use ds_echo::{
    capnproto::{CapnprotoEchoClient, CapnprotoSerializer},
    cornflakes_dynamic::{CornflakesEchoClient, CornflakesSerializer},
    echo::{ManualZeroCopyEcho, OneCopyEcho, RawEcho, RawEchoClient, TwoCopyEcho},
    flatbuffers::{FlatbuffersEchoClient, FlatbuffersSerializer},
    get_equal_fields,
    protobuf::{ProtobufEchoClient, ProtobufSerializer},
    run_client,
    run_datapath::*,
    run_server, EchoClient,
};
use ice_datapath::datapath::connection::IceConnection;
use structopt::StructOpt;

fn main() -> Result<()> {
    let mut opt = DsEchoOpt::from_args();
    global_debug_init(opt.trace_level)?;
    check_opt(&mut opt)?;

    match opt.mode {
        AppMode::Server => match opt.serialization {
            SerializationType::CornflakesDynamic | SerializationType::CornflakesOneCopyDynamic => {
                run_server!(CornflakesSerializer<IceConnection>, IceConnection, opt);
            }
            SerializationType::Flatbuffers => {
                run_server!(FlatbuffersSerializer<IceConnection>, IceConnection, opt);
            }
            SerializationType::Capnproto => {
                run_server!(CapnprotoSerializer<IceConnection>, IceConnection, opt);
            }
            SerializationType::Protobuf => {
                run_server!(ProtobufSerializer<IceConnection>, IceConnection, opt);
            }
            SerializationType::TwoCopyBaseline => {
                run_server!(TwoCopyEcho<IceConnection>, IceConnection, opt);
            }
            SerializationType::OneCopyBaseline => {
                run_server!(OneCopyEcho<IceConnection>, IceConnection, opt);
            }
            SerializationType::ManualZeroCopyBaseline => {
                run_server!(ManualZeroCopyEcho<IceConnection>, IceConnection, opt);
            }
            SerializationType::IdealBaseline => {
                run_server!(RawEcho<IceConnection>, IceConnection, opt);
            }
            _ => {
                unimplemented!();
            }
        },
        AppMode::Client => match opt.serialization {
            SerializationType::CornflakesDynamic | SerializationType::CornflakesOneCopyDynamic => {
                run_client!(CornflakesEchoClient, IceConnection, opt);
            }
            SerializationType::Flatbuffers => {
                run_client!(FlatbuffersEchoClient, IceConnection, opt);
            }
            SerializationType::Capnproto => {
                run_client!(CapnprotoEchoClient, IceConnection, opt);
            }
            SerializationType::Protobuf => {
                run_client!(ProtobufEchoClient, IceConnection, opt);
            }
            SerializationType::IdealBaseline
            | SerializationType::OneCopyBaseline
            | SerializationType::TwoCopyBaseline
            | SerializationType::ManualZeroCopyBaseline => {
                run_client!(RawEchoClient, IceConnection, opt);
            }
            _ => {
                unimplemented!();
            }
        },
    }
    Ok(())
}
