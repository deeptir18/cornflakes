use bytes::{BufMut, Bytes, BytesMut};
use color_eyre::eyre::{ensure, Result};
use cornflakes_libos::{
    datapath::{Datapath, ReceivedPkt},
    loadgen::request_schedule::DistributionType,
    // state_machine::client::ClientSM,
    // timing::{ManualHistogram, SizedManualHistogram},
    utils::AddressInfo,
    MsgID,
};
use rand::rngs::StdRng;
use rand::{
    distributions::{Distribution, Uniform},
    seq::SliceRandom,
    thread_rng, SeedableRng,
};
use std::net::Ipv4Addr;
use structopt::StructOpt;

static GLOBAL_THREAD_COUNT: AtomicUsize = AtomicUsize::new(0);

pub struct TapirClient<D>
where
    D: Datapath,
{
    server_addr: AddressInfo,
    last_sent_id: usize,
    received: usize,
    last_sent_bytes: [u8],
}

impl<D> TapirClient<D>
where
    D: Datapath,
{
    pub fn new(
        server_addr: AddressInfo,
        // thread_id: usize,
        // client_id: usize,
    ) -> Result<Self> {
        Ok(TapirClient {
            server_addr: server_addr,
            last_sent_id: 0,
            received: 0
        })
    }

    pub fn get_next_msg() -> Result<Option<(MsgID, &[u8])>> {
        // get the buffer for a given get request
        self.last_sent_bytes = [5, 0, 0, 0, 98, 0, 0, 0, 0, 0, 0, 0, 10, 96, 10, 81, 8, 1, 
            16, 177, 141, 157, 209, 187, 189, 252, 235, 153, 1, 26, 66, 10, 64,
            107, 101, 121, 95, 57, 56, 52, 56, 50, 97, 97, 97, 97, 97, 97, 97, 
            97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 
            97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 
            97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 16, 152, 
            143, 157, 209, 187, 189, 252, 235, 153, 1, 24, 1];
        Ok(Some((1 as u32, self.last_sent_bytes.as_ref())))
    }

    pub fn process_received_msg(
        &mut self,
        sga: ReceivedPkt<<Self as ClientSM>::Datapath>,
        _datapath: &<Self as ClientSM>::Datapath,
    ) -> Result<bool> {
        let msg_id = sga.msg_id();
        let msg_size = sga.data_len();
        println!("msg id: {}, msg size: {}", msg_id, msg_size);
        Ok(true);
    }

    pub fn run_open_loop(
        &mut self,
        datapath: &mut Self::Datapath,
        schedule: PacketSchedule,
        total_time: Duration,
        // time_out: impl Fn(usize) -> Duration,
        // no_retries: bool,
        num_threads: usize,
        // noop_time: Duration,
        // noop_schedule: PacketSchedule,
        // mut msg_ids_received: Option<&mut Vec<MsgID>>,
    ) -> Result<Duration> {
        let conn_id = datapath
            .connect(self.server_addr())
            .wrap_err("No more available connection IDs")?;

        // clients can prep for a certain number of messages they need to send
        // self.prep_requests(schedule.len(), datapath)?;
        // tracing::info!("Done with prepping requests");

        // wait for all threads to reach this function and "connect"
        // Run noops
        // tracing::info!("About to run noops");
        // let mut noop_spin_timer = SpinTimer::new(noop_schedule, noop_time);
        // let mut noop_buffer = vec![0u8; super::super::NOOP_LEN];
        // LittleEndian::write_u32(&mut noop_buffer.as_mut_slice(), super::super::NOOP_MAGIC);
        // loop {
        //     if noop_spin_timer.done() {
        //         tracing::info!("Noops done");
        //         break;
        //     }
        //     // send a noop message
        //     let id = self.get_current_id();
        //     let buffers = vec![(id, conn_id, noop_buffer.as_slice())];
        //     datapath.push_buffers_with_copy(buffers.as_slice())?;
        //     self.increment_noop_sent();
        //     // wait on the noop timer
        //     noop_spin_timer.wait(&mut |_warmup_done: bool| {
        //         let _recved_pkts = datapath.pop_with_durations()?;
        //         Ok(())
        //     })?;
        // }

        let _ = GLOBAL_THREAD_COUNT.fetch_add(1, Ordering::SeqCst);
        while GLOBAL_THREAD_COUNT.load(Ordering::SeqCst) != num_threads {}

        // run workload
        let start = Instant::now();
        let mut spin_timer = SpinTimer::new(schedule, total_time);

        while let Some((id, msg)) = self.get_next_msg(&datapath)? {
            if spin_timer.done() {
                // tracing::debug!("Total time done");
                break;
            }

            // Send the next message
            datapath.push_buffers_with_copy(&vec![(id, conn_id, msg)])?;
            self.last_sent_id += 1;

            spin_timer.wait(&mut |warmup_done: bool| {
                let recved_pkts = datapath.pop_with_durations()?;
                for (pkt, rtt) in recved_pkts.into_iter() {
                    // if pkt.is_noop() {
                    //     // received old noop response
                    //     continue;
                    // }
                    // let msg_id = pkt.msg_id();
                    // let msg_size = pkt.data_len();
                    if self.process_received_msg(pkt, &datapath).wrap_err(format!(
                        "Error in processing received response for pkt {}.",
                        msg_id
                    ))? {
                        // if warmup_done {
                        //     self.record_rtt(rtt);
                        //     if self.recording_size_rtts() {
                        //         self.record_sized_rtt(rtt, msg_size);
                        //     }
                        // }
                        if let Some(ref mut msg_ids) = &mut msg_ids_received {
                            msg_ids.push(msg_id);
                        }
                        self.received += 1;
                    }
                }

                // if !no_retries {
                //     for (id, conn) in datapath
                //         .timed_out(time_out(self.uniq_received_so_far()))?
                //         .iter()
                //     {
                //         self.increment_num_retried();
                //         self.increment_num_timed_out();
                //         datapath.push_buffers_with_copy(&vec![(
                //             *id,
                //             *conn,
                //             self.msg_timeout_cb(*id, &datapath)?,
                //         )])?;
                //     }
                // }
                Ok(())
            })?;
        }

        // tracing::debug!("Finished sending");
        Ok(start.elapsed())
    }
}

#[derive(Debug, StructOpt, Clone)]
#[structopt(name = "Tapir Client", about = "Tapir client.")]
pub struct TapirClientOpt {
    // #[structopt(
    //     short = "debug",
    //     long = "debug_level",
    //     help = "Configure tracing settings.",
    //     default_value = "warn"
    // )]
    // pub trace_level: TraceLevel,
    #[structopt(
        short = "cf",
        long = "config_file",
        help = "Folder containing shared config information."
    )]
    pub config_file: String,
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
    // #[structopt(long = "logfile", help = "Logfile to log all client RTTs.")]
    // pub logfile: Option<String>,
    // #[structopt(long = "threadlog", help = "Logfile to log per thread statistics")]
    // pub thread_log: Option<String>,
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
    #[structopt(long = "time", help = "max time to run exp for", default_value = "30")]
    pub total_time: usize,
    #[structopt(
        long = "num_refcnt_arrays",
        help = "Multiplicative factor to define number of refcounts"
    )]
    pub num_refcnt_arrays: usize,
    #[structopt(long = "ready_file", help = "Ready file")]
    pub ready_file: Option<String>,
}
