use color_eyre::eyre::{bail, Result};
use cornflakes_libos::{
    datapath::{InlineMode, PushBufType},
    loadgen::request_schedule::DistributionType,
};
use cornflakes_utils::{AppMode, SerializationType, TraceLevel};
use std::net::Ipv4Addr;
use structopt::StructOpt;

#[macro_export]
macro_rules! run_server_twitter(
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
        let twitter_server_loader = TwitterServerLoader::new($opt.total_time, $opt.min_num_keys, $opt.value_size.clone());
        let mut kv_server = <$kv_server>::new($opt.trace_file.as_str(), twitter_server_loader, &mut connection, $opt.push_buf_type, false)?;
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
macro_rules! run_client_twitter(
    ($serializer: ty, $datapath: ty, $opt: ident) => {
        let server_addr = cornflakes_utils::parse_server_addr(&$opt.config_file, &$opt.server_ip)?;
        let mut datapath_params = <$datapath as Datapath>::parse_config_file(&$opt.config_file, &$opt.our_ip)?;
        let addresses = <$datapath as Datapath>::compute_affinity(
                &datapath_params,
                $opt.num_threads,
                Some($opt.server_ip.clone()),
                cornflakes_utils::AppMode::Client,
            )?;

        let per_thread_contexts = <$datapath as Datapath>::global_init(
            $opt.num_threads,
            &mut datapath_params,
            addresses,
        )?;
        let mut threads: Vec<std::thread::JoinHandle<Result<cornflakes_libos::loadgen::client_threads::MeasuredThreadStatsOnly>>> = vec![];
        for (i, per_thread_context) in per_thread_contexts.into_iter().enumerate() {
        let server_addr_clone =
            cornflakes_libos::utils::AddressInfo::new(server_addr.2, server_addr.1.clone(), server_addr.0.clone());
            let datapath_params_clone = datapath_params.clone();
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

                // initialize datapath connection
                let mut connection = <$datapath as Datapath>::per_thread_init(
                    datapath_params_clone,
                    per_thread_context,
                    cornflakes_utils::AppMode::Client,
                )?;

                connection.set_copying_threshold(std::usize::MAX);

                // initialize twitter client to read from file
                let mut twitter_client = TwitterClient::new_twitter_client(opt_clone.client_id, i, opt_clone.num_clients, opt_clone.num_threads, opt_clone.total_time, $opt.value_size.clone(), $opt.ignore_sets, $opt.ignore_pps)?;
                let packet_schedule = twitter_client.generate_packet_schedule_and_metadata(opt_clone.trace_file.as_str(), opt_clone.speed_factor, opt_clone.distribution)?;
                let max_num_requests = packet_schedule.len();
                let server_load_generator_opt: Option<(&str, TwitterServerLoader)> = None;
                let mut kv_client: KVClient<TwitterClient, $serializer, $datapath> = KVClient::new(twitter_client, server_addr_clone, max_num_requests, false, server_load_generator_opt)?;
                kv_client.init(&mut connection)?;

                // TODO: create two custom functions for running with varied sizes at pps, and for
                // running the twitter trace
                let rate = (opt_clone.total_time as f64 / packet_schedule.len() as f64) as u64;
                cornflakes_libos::state_machine::client::run_variable_size_loadgen(i, opt_clone.num_threads as _, opt_clone.client_id as _, opt_clone.num_clients as _, &mut kv_client, &mut connection, opt_clone.total_time as _, opt_clone.logfile.clone(), packet_schedule, opt_clone.record_per_size_buckets, rate, opt_clone.ready_file.clone(), false)
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

        // TODO: somehow separate latencies by size bucket
        let dump_per_thread = $opt.logfile == None;
        cornflakes_libos::loadgen::client_threads::dump_measured_thread_stats(thread_results, $opt.thread_log.clone(), dump_per_thread)?;

    }
);

fn is_cf(opt: &TwitterOpt) -> bool {
    opt.serialization == SerializationType::CornflakesDynamic
        || opt.serialization == SerializationType::CornflakesOneCopyDynamic
}

pub fn is_baseline(opt: &TwitterOpt) -> bool {
    !(opt.serialization == SerializationType::CornflakesOneCopyDynamic
        || opt.serialization == SerializationType::CornflakesDynamic)
}

pub fn check_opt(opt: &mut TwitterOpt) -> Result<()> {
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
    name = "Twitter KV store app",
    about = "Twitter kv store client and server"
)]
pub struct TwitterOpt {
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
    #[structopt(
        long = "trace",
        help = "twitter trace file (postprocessed for cornflakes)"
    )]
    pub trace_file: String,
    #[structopt(long = "time", help = "max time to run exp for", default_value = "30")]
    pub total_time: usize,
    #[structopt(
        long = "speed_factor",
        help = "How much to speed up the trace by",
        default_value = "1.0"
    )]
    pub speed_factor: f64,
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
        default_value = "256"
    )]
    pub copying_threshold: usize,
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
    #[structopt(
        long = "min_keys_to_load",
        help = "Minimum keys to load on server initially",
        default_value = "1000000"
    )]
    pub min_num_keys: usize,
    #[structopt(long = "client_id", default_value = "0")]
    pub client_id: usize,
    #[structopt(long = "start_cutoff", default_value = "0")]
    pub start_cutoff: usize,
    #[structopt(long = "distribution", default_value = "exponential")]
    pub distribution: DistributionType,
    #[structopt(
        long = "ready_file",
        help = "File to indicate server is ready to receive requests"
    )]
    pub ready_file: Option<String>,
    #[structopt(long = "per_size_info", help = "Record per size info")]
    pub record_per_size_buckets: bool,
    #[structopt(long = "value_size", help = "Ignore trace values and use value size")]
    pub value_size: Option<usize>,
    #[structopt(long = "ignore_sets", help = "Ignore set requests")]
    pub ignore_sets: bool,
    #[structopt(long = "analyze", help = "Analyze")]
    pub analyze: bool,
    #[structopt(
        long = "ignore_pps",
        help = "Ignore pps and do distribution over total packets in given time"
    )]
    pub ignore_pps: bool,
}
