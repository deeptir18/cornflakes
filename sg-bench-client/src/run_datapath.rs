use color_eyre::eyre::Result;
use cornflakes_libos::loadgen::request_schedule::DistributionType;
use cornflakes_utils::TraceLevel;
use rand::rngs::StdRng;
use rand::{
    distributions::{Distribution, Uniform},
    seq::SliceRandom,
    thread_rng, SeedableRng,
};
use std::net::Ipv4Addr;
use structopt::StructOpt;

#[macro_export]
macro_rules! run_client(
    ($datapath: ty, $opt: ident) => {
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
        let mut threads: Vec<std::thread::JoinHandle<Result<cornflakes_libos::loadgen::client_threads::ThreadStats>>> = vec![];
        let region_order = get_region_order($opt.random_seed, $opt.array_size / $opt.segment_size, $opt.num_refcnt_arrays)?;
        unsafe {
        REGION_ORDER = region_order.leak();
        }
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
                let mut sg_bench_client = SgBenchClient::new(server_addr_clone, opt_clone.segment_size, opt_clone.echo_mode , opt_clone.num_segments, opt_clone.array_size, opt_clone.send_packet_size,max_num_requests, i, opt_clone.client_id, opt_clone.num_threads, opt_clone.num_clients, opt_clone.num_refcnt_arrays)?;

                cornflakes_libos::state_machine::client::run_client_loadgen(i, &mut sg_bench_client, &mut connection, opt_clone.retries, opt_clone.total_time as _, opt_clone.logfile.clone(), opt_clone.rate as _, (opt_clone.num_segments * opt_clone.segment_size) as _, schedule, opt_clone.num_threads as _)
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

pub fn get_region_order(
    random_seed: usize,
    num_regions: usize,
    num_refcnt_arrays: usize,
) -> Result<Vec<(usize, usize)>> {
    let mut rng = StdRng::seed_from_u64(random_seed as u64);
    let mut indices: Vec<usize> = (0..num_regions).collect();
    for i in (1..num_regions).rev() {
        let between = Uniform::from(0..i);
        let j: usize = between.sample(&mut rng);
        indices.swap(i, j);
    }
    // create num regions random sequences from 0..num_refcnt_arrays
    let mut random_sequences: Vec<Vec<usize>> = Vec::with_capacity(num_regions);
    for _ in 0..num_regions {
        let mut arrays: Vec<usize> = (0..num_refcnt_arrays).collect();
        let slice = arrays.as_mut_slice();
        let mut rng = thread_rng();
        slice.shuffle(&mut rng);
        random_sequences.push(arrays);
    }
    let mut final_indices_order: Vec<(usize, usize)> = Vec::default();
    let mut cur_phys_region = indices[0];
    for refcnt_index in 0..num_refcnt_arrays {
        for _ in 0..num_regions {
            let refcnt_multiplier = &random_sequences[cur_phys_region][refcnt_index];
            cur_phys_region = indices[cur_phys_region];
            final_indices_order.push((cur_phys_region, *refcnt_multiplier));
        }
    }
    return Ok(final_indices_order);
}

#[derive(Debug, StructOpt, Clone)]
#[structopt(name = "Sg Bench Client", about = "Sg Bench client.")]
pub struct SgBenchOpt {
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
    #[structopt(
        long = "segment_size",
        help = "Number of segments requested at once.",
        default_value = "1024"
    )]
    pub segment_size: usize,
    #[structopt(long = "num_segments", help = "Number of mbufs.", default_value = "1")]
    pub num_segments: usize,
    #[structopt(
        long = "array_size",
        help = "Working Set Size.",
        default_value = "16384"
    )]
    pub array_size: usize,
    #[structopt(
        long = "send_packet_size",
        help = "Increased send packet size",
        default_value = "0"
    )]
    pub send_packet_size: usize,
    #[structopt(
        long = "random_seed",
        help = "Random seed to generate ordering with",
        default_value = "2"
    )]
    pub random_seed: usize,
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
    #[structopt(long = "echo_mode", help = "Whether or not server is in eco mode")]
    pub echo_mode: bool,
    #[structopt(long = "time", help = "max time to run exp for", default_value = "30")]
    pub total_time: usize,
    #[structopt(
        long = "num_refcnt_arrays",
        help = "Multiplicative factor to define number of refcounts"
    )]
    pub num_refcnt_arrays: usize,
}
