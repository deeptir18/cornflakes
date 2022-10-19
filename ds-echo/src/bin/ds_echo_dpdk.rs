use color_eyre::eyre::Result;
use cornflakes_libos::{
    datapath::Datapath, state_machine::client::ClientSM, state_machine::server::ServerSM,
};
use cornflakes_utils::{global_debug_init, AppMode, SerializationType};
use dpdk_datapath::datapath::connection::DpdkConnection;
use ds_echo::{
    capnproto::{CapnprotoEchoClient, CapnprotoSerializer},
    cornflakes_dynamic::{CornflakesEchoClient, CornflakesSerializer},
    echo::{RawEcho, RawEchoClient},
    flatbuffers::{FlatbuffersEchoClient, FlatbuffersSerializer},
    get_equal_fields,
    protobuf::{ProtobufEchoClient, ProtobufSerializer},
    run_client,
    run_datapath::*,
    run_server, EchoClient,
};
use structopt::StructOpt;

fn main() -> Result<()> {
    let mut opt = DsEchoOpt::from_args();
    global_debug_init(opt.trace_level)?;
    check_opt(&mut opt)?;

    match opt.mode {
        AppMode::Server => match opt.serialization {
            SerializationType::CornflakesDynamic | SerializationType::CornflakesOneCopyDynamic => {
                run_server!(CornflakesSerializer<DpdkConnection>, DpdkConnection, opt);
            }
            SerializationType::Flatbuffers => {
                run_server!(FlatbuffersSerializer<DpdkConnection>, DpdkConnection, opt);
            }
            SerializationType::Capnproto => {
                run_server!(CapnprotoSerializer<DpdkConnection>, DpdkConnection, opt);
            }
            SerializationType::Protobuf => {
                run_server!(ProtobufSerializer<DpdkConnection>, DpdkConnection, opt);
            }
            SerializationType::IdealBaseline => {
                run_server!(RawEcho<DpdkConnection>, DpdkConnection, opt);
            }
            _ => {
                unimplemented!();
            }
        },
        AppMode::Client => match opt.serialization {
            SerializationType::CornflakesDynamic | SerializationType::CornflakesOneCopyDynamic => {
                run_client!(CornflakesEchoClient, DpdkConnection, opt);
            }
            SerializationType::Flatbuffers => {
                run_client!(FlatbuffersEchoClient, DpdkConnection, opt);
            }
            SerializationType::Capnproto => {
                run_client!(CapnprotoEchoClient, DpdkConnection, opt);
            }
            SerializationType::Protobuf => {
                run_client!(ProtobufEchoClient, DpdkConnection, opt);
            }
            SerializationType::IdealBaseline
            | SerializationType::OneCopyBaseline
            | SerializationType::TwoCopyBaseline => {
                run_client!(RawEchoClient, DpdkConnection, opt);
            }
            _ => {
                unimplemented!();
            }
        },
    }
    Ok(())
}
