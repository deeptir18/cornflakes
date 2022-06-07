use cf_kv::{
    capnproto::{CapnprotoClient, CapnprotoKVServer},
    cornflakes_dynamic::{CornflakesClient, CornflakesKVServer},
    flatbuffers::{FlatbuffersClient, FlatbuffersKVServer},
    redis::{RedisClient},
    //protobuf::{ProtobufClient, ProtobufKVServer},
    run_client, run_server,
    ycsb::{YCSBClient, YCSBServerLoader},
    ycsb_run_datapath::*,
    KVClient, RequestGenerator,
};
use color_eyre::eyre::Result;
use cornflakes_libos::{
    datapath::Datapath, state_machine::client::ClientSM, state_machine::server::ServerSM,
};
use cornflakes_utils::{global_debug_init, AppMode, SerializationType};
use mlx5_datapath::datapath::connection::Mlx5Connection;
use structopt::StructOpt;

fn main() -> Result<()> {
    let mut opt = YCSBOpt::from_args();
    global_debug_init(opt.trace_level)?;
    check_opt(&mut opt)?;

    match opt.mode {
        AppMode::Server => match opt.serialization {
            SerializationType::CornflakesDynamic | SerializationType::CornflakesOneCopyDynamic => {
                run_server!(CornflakesKVServer<Mlx5Connection>, Mlx5Connection, opt);
            }
            SerializationType::Flatbuffers => {
                run_server!(FlatbuffersKVServer<Mlx5Connection>, Mlx5Connection, opt);
            }
            SerializationType::Capnproto => {
                run_server!(CapnprotoKVServer<Mlx5Connection>, Mlx5Connection, opt);
            }
            /*
            SerializationType::Protobuf => {
                run_server!(ProtobufKVServer<Mlx5Connection>, Mlx5Connection, opt);
            }
            */
            _ => {
                unimplemented!();
            }
        },
        AppMode::Client => match opt.serialization {
            SerializationType::CornflakesDynamic | SerializationType::CornflakesOneCopyDynamic => {
                run_client!(CornflakesClient<Mlx5Connection>, Mlx5Connection, opt);
            }
            SerializationType::Flatbuffers => {
                run_client!(FlatbuffersClient<Mlx5Connection>, Mlx5Connection, opt);
            }
            SerializationType::Capnproto => {
                run_client!(CapnprotoClient<Mlx5Connection>, Mlx5Connection, opt);
            }
            SerializationType::Redis => {
                run_client!(RedisClient<Mlx5Connection>, Mlx5Connection, opt);
            }
            /*
            SerializationType::Protobuf => {
                run_client!(ProtobufClient<Mlx5Connection>, Mlx5Connection, opt);
            }
            */
            _ => {
                unimplemented!();
            }
        },
    }
    Ok(())
}
