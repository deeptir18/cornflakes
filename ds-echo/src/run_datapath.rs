use color_eyre::eyre::{bail, Result};
use cornflakes_libos::{
    datapath::{InlineMode, PushBufType},
    loadgen::request_schedule::DistributionType,
};
use cornflakes_utils::{AppMode, SerializationType, SimpleMessageType, TraceLevel};
use std::net::Ipv4Addr;
use structopt::StructOpt;

#[macro_export]
macro_rules! run_server (
    ($echo_server: ty, $datapath: ty, $opt: ident) => {
        let mut datapath_params = <$datapath as Datapath>::parse_config_file(&$opt.config_file, &$opt.server_ip)?;
        let addresses = <$datapath as Datapath>::compute_affinity(&datapath_params, 1, None, AppMode::Server)?;
        let per_thread_contexts = <$datapath as Datapath>::global_init(1, &mut datapath_params, addresses)?;
        let mut connection = <$datapath as Datapath>::per_thread_init(datapath_params, per_thread_contexts.into_iter().nth(0).unwrap(),
        AppMode::Server)?;

        connection.set_copying_threshold($opt.copying_threshold);
        connection.set_inline_mode($opt.inline_mode);
        tracing::info!(threshold = $opt.copying_threshold, "Setting zero-copy copying threshold");

        // init echo server
        let mut echo_server: $echo_server =
            <$echo_server>::new($opt.message_type , $opt.push_buf_type);

            echo_server.init(&mut connection)?;

            echo_server.run_state_machine(&mut connection)?;
    }
);

#[macro_export]
macro_rules! run_client (
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
            cornflakes_libos::loadgen::request_schedule::generate_schedules(num_rtts, $opt.rate, $opt.distribution, $opt.num_threads)?;

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
            let message_type = $opt.message_type.clone();
            let request_sizes = vec![(message_type, get_equal_fields( message_type, $opt.size))];
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

                let mut client: EchoClient<$serializer, $datapath> =
                    EchoClient::new(server_addr_clone, request_sizes, max_num_requests, &connection)?;

                client.init(&mut connection)?;

                cornflakes_libos::state_machine::client::run_client_loadgen(i, &mut client, &mut connection, opt_clone.retries, opt_clone.total_time, opt_clone.logfile.clone(), opt_clone.rate, opt_clone.size, &schedule)
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

fn is_cf(opt: &DsEchoOpt) -> bool {
    opt.serialization == SerializationType::CornflakesDynamic
        || opt.serialization == SerializationType::CornflakesOneCopyDynamic
}

pub fn check_opt(opt: &mut DsEchoOpt) -> Result<()> {
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
    name = "Data structure echo command line",
    about = "Binary to test simple echo application without serialization"
)]
pub struct DsEchoOpt {
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
    #[structopt(long = "mode", help = "App mode: client or server")]
    pub mode: AppMode,
    #[structopt(long = "our_ip", help = "Our IP Address", default_value = "127.0.0.1")]
    pub our_ip: Ipv4Addr,
    #[structopt(
        long = "server_ip",
        help = "Our IP Address",
        default_value = "127.0.0.1"
    )]
    pub server_ip: Ipv4Addr,
    #[structopt(
        short = "r",
        long = "rate",
        help = "Rate of client (in pkts/sec)",
        default_value = "2000"
    )]
    pub rate: u64,
    #[structopt(
        short = "t",
        long = "time",
        help = "Time to run the benchmark for in seconds.",
        default_value = "1"
    )]
    pub total_time: u64,
    #[structopt(long = "retries", help = "Enable client retries.")]
    pub retries: bool,
    #[structopt(long = "logfile", help = "Logfile to log all client RTTs.")]
    pub logfile: Option<String>,
    #[structopt(long = "threadlog", help = "Logfile to log per thread statistics")]
    pub thread_log: Option<String>,
    #[structopt(
        long = "distribution",
        help = "Arrival distribution",
        default_value = "exponential"
    )]
    pub distribution: DistributionType,
    #[structopt(
        long = "num_threads",
        help = "Total number of threads",
        default_value = "1"
    )]
    pub num_threads: usize,
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
    pub copying_threshold: usize,
    #[structopt(long = "size", help = "Total message size", default_value = "1024")]
    pub size: usize,
    #[structopt(
        long = "serialization",
        help = "Serialization library to use",
        default_value = "cornflakes-dynamic"
    )]
    pub serialization: SerializationType,
    #[structopt(
        short = "msg",
        long = "message",
        help = "Message type to echo",
        default_value = "single"
    )]
    pub message_type: SimpleMessageType,
}
