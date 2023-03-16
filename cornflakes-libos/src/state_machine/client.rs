use super::super::{
    datapath::{Datapath, ReceivedPkt},
    high_timeout_at_start,
    loadgen::{
        client_threads::{MeasuredThreadStatsOnly, ThreadStats},
        request_schedule::{PacketSchedule, SpinTimer},
    },
    no_retries_timeout,
    timing::{ManualHistogram, SizedManualHistogram},
    utils::AddressInfo,
    MsgID,
};
use color_eyre::eyre::{Result, WrapErr};
use cornflakes_utils::get_thread_latlog;
use std::time::{Duration, Instant};

use std::sync::atomic::{AtomicUsize, Ordering};

static GLOBAL_THREAD_COUNT: AtomicUsize = AtomicUsize::new(0);

pub trait ClientSM {
    type Datapath: Datapath;

    fn uniq_received_so_far(&self) -> usize;

    fn uniq_sent_so_far(&self) -> usize;

    fn num_retried(&self) -> usize;

    fn num_timed_out(&self) -> usize;

    fn increment_uniq_received(&mut self);

    fn increment_uniq_sent(&mut self);

    fn increment_num_retried(&mut self);

    fn increment_num_timed_out(&mut self);

    fn server_addr(&self) -> AddressInfo;

    fn get_next_msg(
        &mut self,
        datapath: &<Self as ClientSM>::Datapath,
    ) -> Result<Option<(MsgID, &[u8])>>;

    fn process_received_msg(
        &mut self,
        sga: ReceivedPkt<<Self as ClientSM>::Datapath>,
        datapath: &<Self as ClientSM>::Datapath,
    ) -> Result<bool>;

    /// What to do when a particular message times out.
    fn msg_timeout_cb(
        &mut self,
        id: MsgID,
        datapath: &<Self as ClientSM>::Datapath,
    ) -> Result<&[u8]>;

    /// Initializes any internal state with any datapath specific configuration,
    /// e.g., registering external memory.
    fn init(&mut self, connection: &mut Self::Datapath) -> Result<()>;

    fn cleanup(&mut self, connection: &mut Self::Datapath) -> Result<()>;

    fn get_mut_sized_rtts(&mut self) -> &mut SizedManualHistogram;

    fn get_sized_rtts(&self) -> &SizedManualHistogram;

    fn set_recording_size_rtts(&mut self);

    fn recording_size_rtts(&self) -> bool;

    fn get_mut_rtts(&mut self) -> &mut ManualHistogram;

    fn record_sized_rtt(&mut self, rtt: Duration, msg_size: usize) {
        self.get_mut_sized_rtts().record(msg_size, rtt);
    }

    fn record_rtt(&mut self, rtt: Duration) {
        self.get_mut_rtts().record(rtt.as_nanos() as u64);
    }

    fn num_sent_cutoff(&self, cutoff: usize) -> usize {
        self.uniq_sent_so_far() - cutoff
    }

    fn num_received_cutoff(&self, cutoff: usize) -> usize {
        self.uniq_received_so_far() - cutoff
    }

    fn sort_rtts(&mut self, start_cutoff: usize) -> Result<()> {
        self.get_mut_rtts().sort_and_truncate(start_cutoff)?;
        Ok(())
    }

    fn sort_sized_rtts(&mut self) -> Result<()> {
        self.get_mut_sized_rtts().sort()
    }

    fn log_rtts(&mut self, path: &str, start_cutoff: usize) -> Result<()> {
        self.get_mut_rtts().sort_and_truncate(start_cutoff)?;
        self.get_mut_rtts()
            .log_truncated_to_file(path, start_cutoff)?;
        Ok(())
    }

    /// Optional function to prep requests.
    fn prep_requests(
        &mut self,
        _num_packets: usize,
        _datapath: &<Self as ClientSM>::Datapath,
    ) -> Result<usize> {
        Ok(0)
    }

    fn dump(&mut self, path: Option<String>, total_time: Duration, app_name: &str) -> Result<()> {
        self.get_mut_rtts().sort()?;
        tracing::info!(
            sent = self.uniq_sent_so_far(),
            received = self.uniq_received_so_far(),
            goodput =? (self.uniq_received_so_far() as f64 / self.uniq_sent_so_far() as f64),
            timed_out = self.num_timed_out(),
            retries = self.num_retried(),
            total_time = ?total_time.as_secs_f64(),
            "High level sending stats"
        );
        self.get_mut_rtts()
            .dump(&format!("End-to-end client stats for {}: ", app_name))?;

        match path {
            Some(p) => {
                self.get_mut_rtts().log_to_file(&p)?;
            }
            None => {}
        }
        Ok(())
    }

    /// Run open loop client
    fn run_closed_loop(
        &mut self,
        datapath: &mut Self::Datapath,
        num_pkts: u64,
        time_out: impl Fn(usize) -> Duration,
    ) -> Result<()> {
        let mut recved = 0;
        if recved >= num_pkts {
            return Ok(());
        }
        let conn_id = datapath
            .connect(self.server_addr())
            .wrap_err("Could not get connection ID")?;

        while let Some((id, msg)) = self.get_next_msg(&datapath)? {
            if recved >= num_pkts {
                break;
            }
            datapath.push_buffers_with_copy(&vec![(id, conn_id, msg)])?;
            self.increment_uniq_sent();
            let recved_pkts = loop {
                let pkts = datapath.pop_with_durations()?;
                if pkts.len() > 0 {
                    break pkts;
                }
                for (id, conn) in datapath
                    .timed_out(time_out(self.uniq_received_so_far()))?
                    .iter()
                {
                    self.increment_num_retried();
                    self.increment_num_timed_out();
                    datapath.push_buffers_with_copy(&vec![(
                        *id,
                        *conn,
                        self.msg_timeout_cb(*id, &datapath)?,
                    )])?;
                }
            };

            for (pkt, rtt) in recved_pkts.into_iter() {
                let msg_id = pkt.msg_id();
                let size = pkt.data_len();
                if self.process_received_msg(pkt, &datapath).wrap_err(format!(
                    "Error in processing received response for pkt {}.",
                    msg_id
                ))? {
                    self.record_rtt(rtt);
                    if self.recording_size_rtts() {
                        self.record_sized_rtt(rtt, size);
                    }

                    self.increment_uniq_received();
                    recved += 1;
                }
            }
        }
        Ok(())
    }

    /// Run open loop client
    fn run_open_loop(
        &mut self,
        datapath: &mut Self::Datapath,
        schedule: PacketSchedule,
        total_time: Duration,
        time_out: impl Fn(usize) -> Duration,
        no_retries: bool,
        num_threads: usize,
    ) -> Result<()> {
        let conn_id = datapath
            .connect(self.server_addr())
            .wrap_err("No more available connection IDs")?;

        // clients can prep for a certain number of messages they need to send
        // self.prep_requests(schedule.len(), datapath)?;
        // tracing::info!("Done with prepping requests");

        // wait for all threads to reach this function and "connect"
        let _ = GLOBAL_THREAD_COUNT.fetch_add(1, Ordering::SeqCst);
        while GLOBAL_THREAD_COUNT.load(Ordering::SeqCst) != num_threads {}
        let mut spin_timer = SpinTimer::new(schedule, total_time);

        while let Some((id, msg)) = self.get_next_msg(&datapath)? {
            if spin_timer.done() {
                tracing::debug!("Total time done");
                break;
            }

            // Send the next message
            datapath.push_buffers_with_copy(&vec![(id, conn_id, msg)])?;
            self.increment_uniq_sent();

            spin_timer.wait(&mut || {
                let recved_pkts = datapath.pop_with_durations()?;
                for (pkt, rtt) in recved_pkts.into_iter() {
                    let msg_id = pkt.msg_id();
                    let msg_size = pkt.data_len();
                    if self.process_received_msg(pkt, &datapath).wrap_err(format!(
                        "Error in processing received response for pkt {}.",
                        msg_id
                    ))? {
                        self.record_rtt(rtt);
                        if self.recording_size_rtts() {
                            self.record_sized_rtt(rtt, msg_size);
                        }
                        self.increment_uniq_received();
                    }
                }

                if !no_retries {
                    for (id, conn) in datapath
                        .timed_out(time_out(self.uniq_received_so_far()))?
                        .iter()
                    {
                        self.increment_num_retried();
                        self.increment_num_timed_out();
                        datapath.push_buffers_with_copy(&vec![(
                            *id,
                            *conn,
                            self.msg_timeout_cb(*id, &datapath)?,
                        )])?;
                    }
                }
                Ok(())
            })?;
        }

        tracing::debug!("Finished sending");
        Ok(())
    }
}

/// for traces where calculating gbps doesn't make sense
pub fn run_variable_size_loadgen<D>(
    thread_id: usize,
    client: &mut impl ClientSM<Datapath = D>,
    connection: &mut D,
    total_time_seconds: u64,
    logfile: Option<String>,
    schedule: PacketSchedule,
    num_threads: usize,
) -> Result<MeasuredThreadStatsOnly>
where
    D: Datapath,
{
    let start_run = Instant::now();
    client.run_open_loop(
        connection,
        schedule,
        Duration::from_secs(total_time_seconds),
        no_retries_timeout,
        true,
        num_threads,
    )?;
    tracing::info!(thread = thread_id, "Finished running open loop");
    let exp_duration = start_run.elapsed().as_nanos();
    client.sort_rtts(0)?;
    if client.recording_size_rtts() {
        client.sort_sized_rtts()?;
    }
    // per thread log latency
    match logfile {
        Some(x) => {
            let path = get_thread_latlog(&x, thread_id)?;
            client.log_rtts(&path, 0)?;
        }
        None => {}
    }
    tracing::info!(thread = thread_id, "About to calculate stats");
    let sized_rtts = client.get_sized_rtts().clone();
    let stats = MeasuredThreadStatsOnly::new(
        thread_id,
        client.num_sent_cutoff(0),
        client.num_received_cutoff(0),
        client.num_retried(),
        exp_duration as _,
        client.get_mut_rtts(),
        sized_rtts,
        0,
    )?;
    Ok(stats)
}

pub fn run_client_loadgen<D>(
    thread_id: usize,
    client: &mut impl ClientSM<Datapath = D>,
    connection: &mut D,
    retries: bool,
    total_time_seconds: u64,
    logfile: Option<String>,
    rate: u64,
    message_size: usize,
    schedule: PacketSchedule,
    num_threads: usize,
) -> Result<ThreadStats>
where
    D: Datapath,
{
    let start_run = Instant::now();
    let timeout = match retries {
        true => high_timeout_at_start,
        false => no_retries_timeout,
    };

    client.run_open_loop(
        connection,
        schedule,
        Duration::from_secs(total_time_seconds),
        timeout,
        !retries,
        num_threads,
    )?;
    tracing::info!(thread = thread_id, "Finished running open loop");

    let exp_duration = start_run.elapsed().as_nanos();
    client.sort_rtts(0)?;

    tracing::info!(thread = thread_id, "Sorted RTTs");
    // per thread log latency
    match logfile {
        Some(x) => {
            let path = get_thread_latlog(&x, thread_id)?;
            client.log_rtts(&path, 0)?;
        }
        None => {}
    }

    tracing::info!(thread = thread_id, "About to calculate stats");
    let stats = ThreadStats::new(
        thread_id as u16,
        client.num_sent_cutoff(0),
        client.num_received_cutoff(0),
        client.num_retried(),
        exp_duration as _,
        rate,
        message_size,
        client.get_mut_rtts(),
        0,
    )?;

    Ok(stats)
}
