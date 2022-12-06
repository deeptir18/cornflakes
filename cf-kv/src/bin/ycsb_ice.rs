use cf_kv::{
    capnproto::{CapnprotoClient, CapnprotoKVServer},
    cornflakes_dynamic::{CornflakesClient, CornflakesKVServer},
    flatbuffers::{FlatbuffersClient, FlatbuffersKVServer},
    protobuf::{ProtobufClient, ProtobufKVServer},
    redis::RedisClient,
    run_client, run_server,
    ycsb::{YCSBClient, YCSBServerLoader},
    ycsb_run_datapath::*,
    KVClient,
};
use color_eyre::eyre::Result;
use cornflakes_libos::{
    datapath::Datapath, state_machine::client::ClientSM, state_machine::server::ServerSM,
};
use cornflakes_utils::{global_debug_init, AppMode, SerializationType};
use ice_datapath::datapath::connection::IceConnection;
use structopt::StructOpt;

fn main() -> Result<()> {
    let mut opt = YCSBOpt::from_args();
    global_debug_init(opt.trace_level)?;
    check_opt(&mut opt)?;

    match opt.mode {
        AppMode::Server => match opt.serialization {
            SerializationType::CornflakesDynamic | SerializationType::CornflakesOneCopyDynamic => {
                run_server!(CornflakesKVServer<IceConnection>, IceConnection, opt);
            }
            SerializationType::Flatbuffers => {
                run_server!(FlatbuffersKVServer<IceConnection>, IceConnection, opt);
            }
            SerializationType::Capnproto => {
                run_server!(CapnprotoKVServer<IceConnection>, IceConnection, opt);
            }

            SerializationType::Protobuf => {
                run_server!(ProtobufKVServer<IceConnection>, IceConnection, opt);
            }
            _ => {
                unimplemented!();
            }
        },
        AppMode::Client => match opt.serialization {
            SerializationType::CornflakesDynamic | SerializationType::CornflakesOneCopyDynamic => {
                run_client!(CornflakesClient<IceConnection>, IceConnection, opt);
            }
            SerializationType::Flatbuffers => {
                run_client!(FlatbuffersClient<IceConnection>, IceConnection, opt);
            }
            SerializationType::Capnproto => {
                run_client!(CapnprotoClient<IceConnection>, IceConnection, opt);
            }
            SerializationType::Redis => {
                run_client!(RedisClient<IceConnection>, IceConnection, opt);
            }
            SerializationType::Protobuf => {
                run_client!(ProtobufClient<IceConnection>, IceConnection, opt);
            }
            _ => {
                unimplemented!();
            }
        },
    }
    Ok(())
}

