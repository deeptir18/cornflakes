use cf_kv::{
    capnproto::{CapnprotoClient, CapnprotoKVServer},
    cornflakes_dynamic::{CornflakesClient, CornflakesKVServer},
    flatbuffers::{FlatbuffersClient, FlatbuffersKVServer},
    google_protobuf::{
        default_buckets, GoogleProtobufClient, GoogleProtobufServerLoader, ValueSizeDistribution,
    },
    protobuf::{ProtobufClient, ProtobufKVServer},
    run_client_google,
    run_google_protobuf::*,
    run_server_google, KVClient,
};
use color_eyre::eyre::Result;
use cornflakes_libos::{
    datapath::Datapath, state_machine::client::ClientSM, state_machine::server::ServerSM,
};
use cornflakes_utils::{global_debug_init, AppMode, SerializationType};
use dpdk_datapath::datapath::connection::DpdkConnection;
use structopt::StructOpt;

fn main() -> Result<()> {
    let mut opt = GoogleProtobufOpt::from_args();
    global_debug_init(opt.trace_level)?;
    check_opt(&mut opt)?;

    match opt.mode {
        AppMode::Server => match opt.serialization {
            SerializationType::CornflakesDynamic | SerializationType::CornflakesOneCopyDynamic => {
                run_server_google!(CornflakesKVServer<DpdkConnection>, DpdkConnection, opt);
            }
            SerializationType::Flatbuffers => {
                run_server_google!(FlatbuffersKVServer<DpdkConnection>, DpdkConnection, opt);
            }
            SerializationType::Capnproto => {
                run_server_google!(CapnprotoKVServer<DpdkConnection>, DpdkConnection, opt);
            }
            SerializationType::Protobuf => {
                run_server_google!(ProtobufKVServer<DpdkConnection>, DpdkConnection, opt);
            }
            _ => {
                unimplemented!();
            }
        },
        AppMode::Client => match opt.serialization {
            SerializationType::CornflakesDynamic | SerializationType::CornflakesOneCopyDynamic => {
                run_client_google!(CornflakesClient<DpdkConnection>, DpdkConnection, opt);
            }
            SerializationType::Flatbuffers => {
                run_client_google!(FlatbuffersClient<DpdkConnection>, DpdkConnection, opt);
            }
            SerializationType::Capnproto => {
                run_client_google!(CapnprotoClient<DpdkConnection>, DpdkConnection, opt);
            }
            SerializationType::Protobuf => {
                run_client_google!(ProtobufClient<DpdkConnection>, DpdkConnection, opt);
            }
            _ => {
                unimplemented!();
            }
        },
    }
    Ok(())
}
