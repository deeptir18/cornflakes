use cf_kv::{
    capnproto::{CapnprotoClient, CapnprotoKVServer},
    cornflakes_dynamic::{CornflakesClient, CornflakesKVServer},
    flatbuffers::{FlatbuffersClient, FlatbuffersKVServer},
    protobuf::{ProtobufClient, ProtobufKVServer},
    redis::RedisClient,
    run_client_twitter, run_server_twitter,
    run_twitter::*,
    twitter::{TwitterClient, TwitterFileMetadata, TwitterServerLoader},
    KVClient,
};
use color_eyre::eyre::Result;
use cornflakes_libos::{
    datapath::Datapath, state_machine::client::ClientSM, state_machine::server::ServerSM,
};
use cornflakes_utils::{global_debug_init, AppMode, SerializationType};
use dpdk_datapath::datapath::connection::DpdkConnection;
use structopt::StructOpt;

fn main() -> Result<()> {
    let mut opt = TwitterOpt::from_args();
    global_debug_init(opt.trace_level)?;
    if opt.analyze {
        TwitterFileMetadata::analyze(
            &opt.trace_file.as_str(),
            opt.speed_factor as f64,
            opt.total_time as _,
        )?;
        return Ok(());
    }
    check_opt(&mut opt)?;

    match opt.mode {
        AppMode::Server => match opt.serialization {
            SerializationType::CornflakesDynamic | SerializationType::CornflakesOneCopyDynamic => {
                run_server_twitter!(CornflakesKVServer<DpdkConnection>, DpdkConnection, opt);
            }
            SerializationType::Flatbuffers => {
                run_server_twitter!(FlatbuffersKVServer<DpdkConnection>, DpdkConnection, opt);
            }
            SerializationType::Capnproto => {
                run_server_twitter!(CapnprotoKVServer<DpdkConnection>, DpdkConnection, opt);
            }
            SerializationType::Protobuf => {
                run_server_twitter!(ProtobufKVServer<DpdkConnection>, DpdkConnection, opt);
            }
            _ => {
                unimplemented!();
            }
        },
        AppMode::Client => match opt.serialization {
            SerializationType::CornflakesDynamic | SerializationType::CornflakesOneCopyDynamic => {
                run_client_twitter!(CornflakesClient<DpdkConnection>, DpdkConnection, opt);
            }
            SerializationType::Redis => {
                run_client_twitter!(RedisClient<DpdkConnection>, DpdkConnection, opt);
            }
            SerializationType::Flatbuffers => {
                run_client_twitter!(FlatbuffersClient<DpdkConnection>, DpdkConnection, opt);
            }
            SerializationType::Capnproto => {
                run_client_twitter!(CapnprotoClient<DpdkConnection>, DpdkConnection, opt);
            }
            SerializationType::Protobuf => {
                run_client_twitter!(ProtobufClient<DpdkConnection>, DpdkConnection, opt);
            }
            _ => {
                unimplemented!();
            }
        },
    }
    Ok(())
}
