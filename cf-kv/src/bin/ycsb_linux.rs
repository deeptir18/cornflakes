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
use linux_datapath::datapath::connection::LinuxConnection;
use structopt::StructOpt;

fn main() -> Result<()> {
    let mut opt = YCSBOpt::from_args();
    global_debug_init(opt.trace_level)?;
    check_opt(&mut opt)?;

    match opt.mode {
        AppMode::Server => match opt.serialization {
            SerializationType::CornflakesDynamic | SerializationType::CornflakesOneCopyDynamic => {
                run_server!(CornflakesKVServer<LinuxConnection>, LinuxConnection, opt);
            }
            SerializationType::Flatbuffers => {
                run_server!(FlatbuffersKVServer<LinuxConnection>, LinuxConnection, opt);
            }
            SerializationType::Capnproto => {
                run_server!(CapnprotoKVServer<LinuxConnection>, LinuxConnection, opt);
            }

            SerializationType::Protobuf => {
                run_server!(ProtobufKVServer<LinuxConnection>, LinuxConnection, opt);
            }
            _ => {
                unimplemented!();
            }
        },
        AppMode::Client => match opt.serialization {
            SerializationType::CornflakesDynamic | SerializationType::CornflakesOneCopyDynamic => {
                run_client!(CornflakesClient<LinuxConnection>, LinuxConnection, opt);
            }
            SerializationType::Flatbuffers => {
                run_client!(FlatbuffersClient<LinuxConnection>, LinuxConnection, opt);
            }
            SerializationType::Capnproto => {
                run_client!(CapnprotoClient<LinuxConnection>, LinuxConnection, opt);
            }
            SerializationType::Redis => {
                run_client!(RedisClient<LinuxConnection>, LinuxConnection, opt);
            }
            SerializationType::Protobuf => {
                run_client!(ProtobufClient<LinuxConnection>, LinuxConnection, opt);
            }
            _ => {
                unimplemented!();
            }
        },
    }
    Ok(())
}
