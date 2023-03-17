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
use mlx5_datapath::datapath::connection::Mlx5Connection;
use structopt::StructOpt;

fn main() -> Result<()> {
    let mut opt = GoogleProtobufOpt::from_args();
    global_debug_init(opt.trace_level)?;
    check_opt(&mut opt)?;

    match opt.mode {
        AppMode::Server => match opt.serialization {
            SerializationType::CornflakesDynamic | SerializationType::CornflakesOneCopyDynamic => {
                run_server_google!(CornflakesKVServer<Mlx5Connection>, Mlx5Connection, opt);
            }
            SerializationType::Flatbuffers => {
                run_server_google!(FlatbuffersKVServer<Mlx5Connection>, Mlx5Connection, opt);
            }
            SerializationType::Capnproto => {
                run_server_google!(CapnprotoKVServer<Mlx5Connection>, Mlx5Connection, opt);
            }
            SerializationType::Protobuf => {
                run_server_google!(ProtobufKVServer<Mlx5Connection>, Mlx5Connection, opt);
            }
            _ => {
                unimplemented!();
            }
        },
        AppMode::Client => match opt.serialization {
            SerializationType::CornflakesDynamic | SerializationType::CornflakesOneCopyDynamic => {
                run_client_google!(CornflakesClient<Mlx5Connection>, Mlx5Connection, opt);
            }
            SerializationType::Flatbuffers => {
                run_client_google!(FlatbuffersClient<Mlx5Connection>, Mlx5Connection, opt);
            }
            SerializationType::Capnproto => {
                run_client_google!(CapnprotoClient<Mlx5Connection>, Mlx5Connection, opt);
            }
            SerializationType::Protobuf => {
                run_client_google!(ProtobufClient<Mlx5Connection>, Mlx5Connection, opt);
            }
            _ => {
                unimplemented!();
            }
        },
    }
    Ok(())
}
