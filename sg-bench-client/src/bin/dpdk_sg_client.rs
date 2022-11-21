use color_eyre::eyre::Result;
use cornflakes_libos::datapath::Datapath;
use cornflakes_utils::global_debug_init;
use dpdk_datapath::datapath::connection::DpdkConnection;
use sg_bench_client::{run_client, run_datapath::get_region_order, run_datapath::*, SgBenchClient};
use structopt::StructOpt;

fn main() -> Result<()> {
    let opt = SgBenchOpt::from_args();
    global_debug_init(opt.trace_level)?;
    run_client!(DpdkConnection, opt);
    Ok(())
}
