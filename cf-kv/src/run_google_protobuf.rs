use super::google_protobuf::NumValuesDistribution;
use color_eyre::eyre::{bail, Result};
use cornflakes_libos::{
    datapath::{InlineMode, PushBufType},
    loadgen::request_schedule::DistributionType,
};
use cornflakes_utils::{AppMode, SerializationType, TraceLevel};
use std::net::Ipv4Addr;
use structopt::StructOpt;

#[macro_export]
macro_rules! run_server_google(
    ($kv_server: ty, $datapath: ty, $opt: ident) => {
        let is_baseline = is_baseline(&$opt);
        let mut datapath_params = <$datapath as Datapath>::parse_config_file(&$opt.config_file, &$opt.server_ip)?;
        let addresses = <$datapath as Datapath>::compute_affinity(&datapath_params, 1, None, AppMode::Server)?;
        let per_thread_contexts = <$datapath as Datapath>::global_init(1, &mut datapath_params, addresses)?;
        let mut connection = <$datapath as Datapath>::per_thread_init(datapath_params, per_thread_contexts.into_iter().nth(0).unwrap(),
        AppMode::Server)?;

        connection.set_copying_threshold($opt.copying_threshold);
        connection.set_inline_mode($opt.inline_mode);
        tracing::info!(threshold = $opt.copying_threshold, "Setting zero-copy copying threshold");

        // init retwis load generator
        let (buckets, probs) = default_buckets();
        let load_generator = GoogleProtobufServerLoader::new($opt.num_keys, $opt.key_size, ValueSizeDistribution::new($opt.max_size, buckets, probs)?,$opt.num_values_distribution);
        let mut kv_server = <$kv_server>::new("", load_generator, &mut connection, $opt.push_buf_type, true)?;
        kv_server.init(&mut connection)?;
        kv_server.write_ready($opt.ready_file.clone())?;
        if is_baseline {
            kv_server.run_state_machine_baseline(&mut connection)?;
        } else {
            kv_server.run_state_machine(&mut connection)?;
        }
    }
);

#[macro_export]
macro_rules! run_client_google(
    ($serializer: ty, $datapath: ty, $opt: ident) => {
        let server_addr = cornflakes_utils::parse_server_addr(&$opt.config_file, &$opt.server_ip)?;
        let mut datapath_params = <$datapath as Datapath>::parse_config_file(&$opt.config_file, &$opt.our_ip)?;
        let addresses = <$datapath as Datapath>::compute_affinity(
                &datapath_params,
                $opt.num_threads,
                Some($opt.server_ip.clone()),
                cornflakes_utils::AppMode::Client,
            )?;
        let num_rtts = ($opt.rate * $opt.total_time * 2) as usize;
        let schedules =
            cornflakes_libos::loadgen::request_schedule::generate_schedules(num_rtts, $opt.rate as _, $opt.distribution, $opt.num_threads)?;

        let per_thread_contexts = <$datapath as Datapath>::global_init(
            $opt.num_threads,
            &mut datapath_params,
            addresses,
        )?;
        let mut threads: Vec<std::thread::JoinHandle<Result<cornflakes_libos::loadgen::client_threads::MeasuredThreadStatsOnly>>> = vec![];

        // spawn a thread to run client for each connection
        for (i, (schedule, per_thread_context)) in schedules
            .into_iter()
            .zip(per_thread_contexts.into_iter())
            .enumerate()
        {
        let server_addr_clone =
            cornflakes_libos::utils::AddressInfo::new(server_addr.2, server_addr.1.clone(), server_addr.0.clone());
            let datapath_params_clone = datapath_params.clone();

            let max_num_requests = num_rtts;
            let opt_clone = $opt.clone();
            threads.push(std::thread::spawn(move || {
                match affinity::set_thread_affinity(&vec![i + 1]) {
                    Ok(_) => {}
                    Err(e) => {
                        color_eyre::eyre::bail!(
                            "Could not set thread affinity for thread {} on core {}: {:?}",
                            i,
                            i + 1,
                            e
                         )
                    }
                }

                let mut connection = <$datapath as Datapath>::per_thread_init(
                    datapath_params_clone,
                    per_thread_context,
                    cornflakes_utils::AppMode::Client,
                )?;

                connection.set_copying_threshold(std::usize::MAX);

                tracing::info!("Finished initializing datapath connection for thread {}", i);
                let (buckets, probs) = default_buckets();
                let size = ValueSizeDistribution::new($opt.max_size, buckets, probs)?.avg_size();

                let mut retwis_client = GoogleProtobufClient::new($opt.num_keys, $opt.key_size);
                tracing::info!("Finished initializing google protobuf client");

                let mut server_load_generator_opt: Option<(&str, GoogleProtobufServerLoader)> = None;
                let mut kv_client: KVClient<GoogleProtobufClient, $serializer, $datapath> = KVClient::new(retwis_client, server_addr_clone, max_num_requests,opt_clone.retries, server_load_generator_opt)?;


                kv_client.init(&mut connection)?;
                // TODO: create two custom functions for running with varied sizes at pps, and for
                // running the twitter trace

                cornflakes_libos::state_machine::client::run_variable_size_loadgen(i, &mut kv_client, &mut connection, opt_clone.total_time as _, opt_clone.logfile.clone(), schedule, opt_clone.num_threads as _)
            }));
        }

        let mut thread_results: Vec<cornflakes_libos::loadgen::client_threads::MeasuredThreadStatsOnly> = Vec::default();
        for child in threads {
            let s = match child.join() {
                Ok(res) => match res {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!("Thread failed: {:?}", e);
                        color_eyre::eyre::bail!("Failed thread");
                    }
                },
                Err(e) => {
                    tracing::warn!("Failed to join client thread: {:?}", e);
                    color_eyre::eyre::bail!("Failed to join thread");
                }
            };
            thread_results.push(s);
        }

        let dump_per_thread = $opt.logfile == None;
        cornflakes_libos::loadgen::client_threads::dump_measured_thread_stats(thread_results, $opt.thread_log.clone(), dump_per_thread)?;
    }
);

fn is_cf(opt: &GoogleProtobufOpt) -> bool {
    opt.serialization == SerializationType::CornflakesDynamic
        || opt.serialization == SerializationType::CornflakesOneCopyDynamic
}

pub fn is_baseline(opt: &GoogleProtobufOpt) -> bool {
    !(opt.serialization == SerializationType::CornflakesOneCopyDynamic
        || opt.serialization == SerializationType::CornflakesDynamic)
}

pub fn check_opt(opt: &mut GoogleProtobufOpt) -> Result<()> {
    if !is_cf(opt) && opt.push_buf_type != PushBufType::SingleBuf {
        bail!("For non-cornflakes serialization, push buf type must be single buffer.");
    }

    if opt.serialization == SerializationType::CornflakesOneCopyDynamic {
        // copy all segments
        opt.copying_threshold = usize::MAX;
    }

    Ok(())
}

#[derive(Debug, StructOpt, Clone)]
#[structopt(
    name = "Google Protobuf Distribution KV Store App.",
    about = "Google Protobuf KV store server and client."
)]
pub struct GoogleProtobufOpt {
    #[structopt(
        short = "debug",
        long = "debug_level",
        help = "Configure tracing settings.",
        default_value = "warn"
    )]
    pub trace_level: TraceLevel,
    #[structopt(
        short = "cf",
        long = "config_file",
        help = "Folder containing shared config information."
    )]
    pub config_file: String,
    #[structopt(long = "mode", help = "KV server or client mode.")]
    pub mode: AppMode,
    #[structopt(long = "time", help = "max time to run exp for", default_value = "30")]
    pub total_time: usize,
    #[structopt(
        long = "push_buf_type",
        help = "Push API to use",
        default_value = "sga"
    )]
    pub push_buf_type: PushBufType,
    #[structopt(
        long = "inline_mode",
        help = "For Mlx5 datapath, which inline mode to use. Note this can't be set for DPDK datapath.",
        default_value = "nothing"
    )]
    pub inline_mode: InlineMode,
    #[structopt(
        long = "copy_threshold",
        help = "Datapath copy threshold. Copies everything below this threshold. If set to 0, tries to use zero-copy for everything. If set to infinity, uses zero-copy for nothing.",
        default_value = "512"
    )]
    pub copying_threshold: usize,
    #[structopt(
        short = "r",
        long = "rate",
        help = "Rate of client (in pkts/sec)",
        default_value = "2000"
    )]
    pub rate: usize,
    #[structopt(
        long = "server_ip",
        help = "Server ip address",
        default_value = "127.0.0.1"
    )]
    pub server_ip: Ipv4Addr,
    #[structopt(long = "our_ip", help = "Our ip address", default_value = "127.0.0.1")]
    pub our_ip: Ipv4Addr,
    #[structopt(
        long = "serialization",
        help = "Serialization library to use",
        default_value = "cornflakes-dynamic"
    )]
    pub serialization: SerializationType,
    #[structopt(long = "retries", help = "Enable client retries.")]
    pub retries: bool,
    #[structopt(long = "logfile", help = "Logfile to log all client RTTs.")]
    pub logfile: Option<String>,
    #[structopt(long = "threadlog", help = "Logfile to log per thread statistics")]
    pub thread_log: Option<String>,
    #[structopt(
        long = "num_threads",
        help = "Number of (client) threads.",
        default_value = "1"
    )]
    pub num_threads: usize,
    #[structopt(
        long = "num_clients",
        help = "Total number of clients",
        default_value = "1"
    )]
    pub num_clients: usize,
    #[structopt(long = "client_id", default_value = "0")]
    pub client_id: usize,
    #[structopt(long = "start_cutoff", default_value = "0")]
    pub start_cutoff: usize,
    #[structopt(long = "distribution", default_value = "exponential")]
    pub distribution: DistributionType,
    #[structopt(
        long = "num_keys",
        default_value = "1000000",
        help = "Default number of keys to initialize the KV store with"
    )]
    pub num_keys: usize,
    #[structopt(long = "key_size", default_value = "64", help = "Default key size")]
    pub key_size: usize,
    #[structopt(long = "num_values_distribution", default_value = "SingleValue-1")]
    pub num_values_distribution: NumValuesDistribution,
    #[structopt(
        long = "ready_file",
        help = "File to indicate server is ready to receive requests"
    )]
    pub ready_file: Option<String>,
    #[structopt(
        long = "max_size",
        help = "Size for maximum packet size in distribution",
        default_value = "4096"
    )]
    pub max_size: usize,
}
