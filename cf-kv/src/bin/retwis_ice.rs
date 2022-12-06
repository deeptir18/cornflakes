use cf_kv::{
    capnproto::{CapnprotoClient, CapnprotoKVServer},
    cornflakes_dynamic::{CornflakesClient, CornflakesKVServer},
    flatbuffers::{FlatbuffersClient, FlatbuffersKVServer},
    protobuf::{ProtobufClient, ProtobufKVServer},
    retwis::{retwis_keys, RetwisClient, RetwisServerLoader},
    retwis_run_datapath::*,
    run_client_retwis, run_server_retwis, KVClient,
};
use color_eyre::eyre::Result;
use cornflakes_libos::{
    datapath::Datapath, state_machine::client::ClientSM, state_machine::server::ServerSM,
};
use cornflakes_utils::{global_debug_init, AppMode, SerializationType};
use ice_datapath::datapath::connection::IceConnection;
use structopt::StructOpt;

fn main() -> Result<()> {
    let mut opt = RetwisOpt::from_args();
    global_debug_init(opt.trace_level)?;
    check_opt(&mut opt)?;

    match opt.mode {
        AppMode::Server => match opt.serialization {
            SerializationType::CornflakesDynamic | SerializationType::CornflakesOneCopyDynamic => {
                run_server_retwis!(CornflakesKVServer<IceConnection>, IceConnection, opt);
            }
            SerializationType::Flatbuffers => {
                run_server_retwis!(FlatbuffersKVServer<IceConnection>, IceConnection, opt);
            }
            SerializationType::Capnproto => {
                run_server_retwis!(CapnprotoKVServer<IceConnection>, IceConnection, opt);
            }
            SerializationType::Protobuf => {
                run_server_retwis!(ProtobufKVServer<IceConnection>, IceConnection, opt);
            }
            _ => {
                unimplemented!();
            }
        },
        AppMode::Client => match opt.serialization {
            SerializationType::CornflakesDynamic | SerializationType::CornflakesOneCopyDynamic => {
                run_client_retwis!(CornflakesClient<IceConnection>, IceConnection, opt);
            }
            SerializationType::Flatbuffers => {
                run_client_retwis!(FlatbuffersClient<IceConnection>, IceConnection, opt);
            }
            SerializationType::Capnproto => {
                run_client_retwis!(CapnprotoClient<IceConnection>, IceConnection, opt);
            }
            SerializationType::Protobuf => {
                run_client_retwis!(ProtobufClient<IceConnection>, IceConnection, opt);
            }
            _ => {
                unimplemented!();
            }
        },
    }
    Ok(())
}

