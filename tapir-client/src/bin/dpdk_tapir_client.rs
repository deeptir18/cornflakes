use color_eyre::eyre::Result;
use dpdk_datapath::datapath::connection::DpdkConnection;
use structopt::StructOpt;
use tapir_client::{TapirClient, TapirClientOpt};

fn main() -> Result<()> {
    // dpdk connection
    // make tapir client
    // run client
    //TapirClient::new()

    let opt = TapirClientOpt::from_args();

    let server_addr = cornflakes_utils::parse_server_addr(&opt.config_file, &opt.server_ip)?;
    let mut datapath_params = <$datapath as Datapath>::parse_config_file(&opt.config_file, &opt.our_ip)?;
    let addresses = <$datapath as Datapath>::compute_affinity(
            &datapath_params,
            opt.num_threads,
            Some(opt.server_ip.clone()),
            cornflakes_utils::AppMode::Client,
        )?;
    let num_rtts = ($opt.rate * $opt.total_time * 2) as usize;
    let schedules =
        cornflakes_libos::loadgen::request_schedule::generate_schedules(num_rtts, opt.rate as _, opt.distribution, opt.num_threads)?;

    let per_thread_contexts = <$datapath as Datapath>::global_init(
        opt.num_threads,
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
        let opt_clone = opt.clone();
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
            let mut client = TapirClient::new(server_addr_clone)?;
            
            client.run_open_loop(&mut connection, schedule, Duration::from_secs(total_time as u64), opt_clone.num_threads);

            // cornflakes_libos::state_machine::client::run_client_loadgen(i, opt_clone.num_threads as _, opt_clone.client_id as _, opt_clone.num_clients as _, &mut sg_bench_client, &mut connection, opt_clone.retries, opt_clone.total_time as _, opt_clone.logfile.clone(), opt_clone.rate as _, (opt_clone.num_segments * opt_clone.segment_size) as _, schedule, opt_clone.ready_file.clone())
        }));
    }

    for child in threads {
        match child.join() {
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
    }
}