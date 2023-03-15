use cf_kv::{
    capnproto::{CapnprotoClient, CapnprotoKVServer},
    cornflakes_dynamic::{CornflakesClient, CornflakesKVServer},
    flatbuffers::{FlatbuffersClient, FlatbuffersKVServer},
    protobuf::{ProtobufClient, ProtobufKVServer},
    run_client_twitter, run_server_twitter,
    run_twitter::*,
    twitter::{TwitterClient, TwitterServerLoader},
    KVClient,
};
use color_eyre::eyre::Result;
use cornflakes_libos::{
    datapath::Datapath, state_machine::client::ClientSM, state_machine::server::ServerSM,
};
use cornflakes_utils::{global_debug_init, AppMode, SerializationType};
use mlx5_datapath::datapath::connection::Mlx5Connection;
use structopt::StructOpt;

fn main() -> Result<()> {
    let mut opt = TwitterOpt::from_args();
    global_debug_init(opt.trace_level)?;
    check_opt(&mut opt)?;

    match opt.mode {
        AppMode::Server => match opt.serialization {
            SerializationType::CornflakesDynamic | SerializationType::CornflakesOneCopyDynamic => {
                run_server_twitter!(CornflakesKVServer<Mlx5Connection>, Mlx5Connection, opt);
            }
            SerializationType::Flatbuffers => {
                run_server_twitter!(FlatbuffersKVServer<Mlx5Connection>, Mlx5Connection, opt);
            }
            SerializationType::Capnproto => {
                run_server_twitter!(CapnprotoKVServer<Mlx5Connection>, Mlx5Connection, opt);
            }
            SerializationType::Protobuf => {
                run_server_twitter!(ProtobufKVServer<Mlx5Connection>, Mlx5Connection, opt);
            }
            _ => {
                unimplemented!();
            }
        },
        AppMode::Client => match opt.serialization {
            SerializationType::CornflakesDynamic | SerializationType::CornflakesOneCopyDynamic => {
                run_client_twitter!(CornflakesClient<Mlx5Connection>, Mlx5Connection, opt);
            }
            SerializationType::Flatbuffers => {
                run_client_twitter!(FlatbuffersClient<Mlx5Connection>, Mlx5Connection, opt);
            }
            SerializationType::Capnproto => {
                run_client_twitter!(CapnprotoClient<Mlx5Connection>, Mlx5Connection, opt);
            }
            SerializationType::Protobuf => {
                run_client_twitter!(ProtobufClient<Mlx5Connection>, Mlx5Connection, opt);
            }
            _ => {
                unimplemented!();
            }
        },
    }
    Ok(())
}
