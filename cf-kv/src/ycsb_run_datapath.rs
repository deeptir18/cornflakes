use super::ycsb::YCSBValueSizeGenerator;
use color_eyre::eyre::{bail, Result};
use cornflakes_libos::{
    datapath::{InlineMode, PushBufType},
    loadgen::request_schedule::DistributionType,
};
use cornflakes_utils::{AppMode, CopyingThreshold, SerializationType, TraceLevel};
use std::net::Ipv4Addr;
use structopt::StructOpt;

#[macro_export]
macro_rules! run_server(
    ($kv_server: ty, $datapath: ty, $opt: ident) => {
        let is_baseline = is_baseline(&$opt);
        let mut datapath_params = <$datapath as Datapath>::parse_config_file(&$opt.config_file, &$opt.server_ip)?;
        let addresses = <$datapath as Datapath>::compute_affinity(&datapath_params, 1, None, AppMode::Server)?;
        let per_thread_contexts = <$datapath as Datapath>::global_init(1, &mut datapath_params, addresses)?;
        let mut connection = <$datapath as Datapath>::per_thread_init(datapath_params, per_thread_contexts.into_iter().nth(0).unwrap(),
        AppMode::Server)?;

        connection.set_copying_threshold($opt.copying_threshold.thresh());
        connection.set_inline_mode($opt.inline_mode);
        tracing::info!(threshold = $opt.copying_threshold.thresh(), "Setting zero-copy copying threshold");
        // init ycsb load generator
        let load_generator = YCSBServerLoader::new($opt.value_size_generator, $opt.num_values, $opt.num_keys, $opt.allocate_contiguously);
        let mut kv_server = <$kv_server>::new($opt.trace_file.as_str(), load_generator, &mut connection, $opt.push_buf_type, $opt.use_linked_list)?;
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
macro_rules! run_client(
    ($serializer: ty, $datapath: ty, $opt: ident) => {
        let server_addr = cornflakes_utils::parse_server_addr(&$opt.config_file, &$opt.server_ip)?;
        let mut datapath_params = <$datapath as Datapath>::parse_config_file(&$opt.config_file, &$opt.our_ip)?;
        let addresses = <$datapath as Datapath>::compute_affinity(
                &datapath_params,
                $opt.num_threads,
                Some($opt.server_ip.clone()),
                cornflakes_utils::AppMode::Client,
            )?;
        let num_rtts = ($opt.rate as f64 * $opt.total_time as f64 * 1.2) as usize;
        let schedules =
            cornflakes_libos::loadgen::request_schedule::generate_schedules(num_rtts, $opt.rate as _, $opt.distribution, $opt.num_threads)?;

        let per_thread_contexts = <$datapath as Datapath>::global_init(
            $opt.num_threads,
            &mut datapath_params,
            addresses,
        )?;
        let mut threads: Vec<std::thread::JoinHandle<Result<cornflakes_libos::loadgen::client_threads::ThreadStats>>> = vec![];

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

                connection.set_copying_threshold(usize::MAX);

                let mut ycsb_client = YCSBClient::new_ycsb_client(&opt_clone.queries.as_str(),opt_clone.client_id, i, opt_clone.num_clients, opt_clone.num_threads, opt_clone.value_size_generator.clone(), opt_clone.num_keys, opt_clone.num_values)?;

                let mut server_trace: Option<(&str, YCSBServerLoader)> = None;
                if cfg!(debug_assertions) {
                    if opt_clone.trace_file != "" {
                        let server_loader = YCSBServerLoader::new(opt_clone.value_size_generator.clone(), opt_clone.num_values, opt_clone.num_keys, false);
                        server_trace = Some((&opt_clone.trace_file.as_str(), server_loader));
                    }
                }
                let mut kv_client: KVClient<YCSBClient, $serializer, $datapath> = KVClient::new(ycsb_client, server_addr_clone, max_num_requests,opt_clone.retries, server_trace)?;

                kv_client.init(&mut connection)?;

                let avg_size = opt_clone.value_size_generator.avg_size();
                cornflakes_libos::state_machine::client::run_client_loadgen(i, &mut kv_client, &mut connection, opt_clone.retries, opt_clone.total_time as _, opt_clone.logfile.clone(), opt_clone.rate as _, (opt_clone.num_values * avg_size) as _, schedule, opt_clone.num_threads as _)
            }));
        }

        let mut thread_results: Vec<cornflakes_libos::loadgen::client_threads::ThreadStats> = Vec::default();
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
        cornflakes_libos::loadgen::client_threads::dump_thread_stats(thread_results, $opt.thread_log.clone(), dump_per_thread)?;
    }
);

pub fn is_baseline(opt: &YCSBOpt) -> bool {
    !(opt.serialization == SerializationType::CornflakesOneCopyDynamic
        || opt.serialization == SerializationType::CornflakesDynamic)
}
pub fn is_cf(opt: &YCSBOpt) -> bool {
    opt.serialization == SerializationType::CornflakesDynamic
        || opt.serialization == SerializationType::CornflakesOneCopyDynamic
}

pub fn check_opt(opt: &mut YCSBOpt) -> Result<()> {
    if !is_cf(opt) && opt.push_buf_type != PushBufType::SingleBuf {
        bail!("For non-cornflakes serialization, push buf type must be single buffer.");
    }

    if opt.serialization == SerializationType::CornflakesOneCopyDynamic {
        // copy all segments
        opt.copying_threshold = CopyingThreshold::new(usize::MAX)
    }

    Ok(())
}
#[derive(Debug, StructOpt, Clone)]
#[structopt(
    name = "YCSB KV Store App.",
    about = "YCSB KV store server and client."
)]
pub struct YCSBOpt {
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
        long = "value_size",
        help = "Value size distribution",
        default_value = "UniformOverSizes-1024"
    )]
    pub value_size_generator: YCSBValueSizeGenerator,
    #[structopt(
        long = "num_values",
        help = "number of batched puts and gets per line in trace",
        default_value = "1"
    )]
    pub num_values: usize,
    #[structopt(
        long = "num_keys",
        help = "Number of keys per line",
        default_value = "1"
    )]
    pub num_keys: usize,
    #[structopt(long = "time", help = "max time to run exp for", default_value = "30")]
    pub total_time: usize,
    #[structopt(
        short = "t",
        long = "trace",
        help = "Trace file to load server side values from.",
        default_value = ""
    )]
    pub trace_file: String,
    #[structopt(
        short = "q",
        long = "queries",
        help = "Query file to load queries from.",
        default_value = ""
    )]
    pub queries: String,
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
        help = "Datapath copy threshold. Copies everything below this threshold. If set to infinity, tries to use zero-copy for everything. If set to 0, uses zero-copy for nothing.",
        default_value = "256"
    )]
    pub copying_threshold: CopyingThreshold,
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
        long = "allocate_contiguously",
        help = "Allocate YCSB multiple values contiguously."
    )]
    pub allocate_contiguously: bool,
    #[structopt(
        long = "use_linked_list",
        help = "Use linked list version of kv store."
    )]
    pub use_linked_list: bool,
    #[structopt(
        long = "ready_file",
        help = "File to indicate server is ready to receive requests"
    )]
    pub ready_file: Option<String>,
}
