use super::super::{
    datapath::{Datapath, ReceivedPkt},
    high_timeout_at_start,
    loadgen::{client_threads::ThreadStats, request_schedule::PacketSchedule},
    no_retries_timeout,
    timing::ManualHistogram,
    utils::AddressInfo,
    MsgID,
};
use color_eyre::eyre::{Result, WrapErr};
use cornflakes_utils::get_thread_latlog;
use std::time::{Duration, Instant};

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
    ) -> Result<()>;

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

    fn get_mut_rtts(&mut self) -> &mut ManualHistogram;

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

    fn log_rtts(&mut self, path: &str, start_cutoff: usize) -> Result<()> {
        self.get_mut_rtts().sort_and_truncate(start_cutoff)?;
        self.get_mut_rtts()
            .log_truncated_to_file(path, start_cutoff)?;
        Ok(())
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
                self.record_rtt(rtt);
                let msg_id = pkt.msg_id();
                self.process_received_msg(pkt, &datapath).wrap_err(format!(
                    "Error in processing received response for pkt {}.",
                    msg_id
                ))?;
                self.increment_uniq_received();
                recved += 1;
            }
        }
        Ok(())
    }

    /// Run open loop client
    fn run_open_loop(
        &mut self,
        datapath: &mut Self::Datapath,
        schedule: &PacketSchedule,
        total_time: u64,
        time_out: impl Fn(usize) -> Duration,
        no_retries: bool,
    ) -> Result<()> {
        let freq = datapath.timer_hz(); // cycles per second
        let conn_id = datapath
            .connect(self.server_addr())
            .wrap_err("No more available connection IDs")?;
        let time_start = Instant::now();
        let mut idx = 0;

        let start = datapath.current_cycles();
        let mut deficit;
        let mut next = start;
        while let Some((id, msg)) = self.get_next_msg(&datapath)? {
            if datapath.current_cycles() > (total_time * freq + start) {
                tracing::debug!("Total time done");
                break;
            }

            // Send the next message
            tracing::debug!(time = ?time_start.elapsed(), "About to send next packet");
            datapath.push_buffers_with_copy(&vec![(id, conn_id, msg)])?;
            self.increment_uniq_sent();
            idx += 1;
            let last_sent = datapath.current_cycles();
            deficit = last_sent - next;
            next = schedule.get_next_in_cycles(idx, last_sent, freq, deficit);

            while datapath.current_cycles() <= next {
                let recved_pkts = datapath.pop_with_durations()?;
                for (pkt, rtt) in recved_pkts.into_iter() {
                    self.record_rtt(rtt);
                    let msg_id = pkt.msg_id();
                    self.process_received_msg(pkt, &datapath).wrap_err(format!(
                        "Error in processing received response for pkt {}.",
                        msg_id
                    ))?;
                    self.increment_uniq_received();
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
            }
        }

        tracing::debug!("Finished sending");
        Ok(())
    }
}

pub fn run_client_loadgen<D>(
    thread_id: usize,
    client: &mut impl ClientSM<Datapath = D>,
    connection: &mut D,
    retries: bool,
    total_time: u64,
    logfile: Option<String>,
    rate: u64,
    message_size: usize,
    schedule: &PacketSchedule,
) -> Result<ThreadStats>
where
    D: Datapath,
{
    let start_run = Instant::now();
    let timeout = match retries {
        true => high_timeout_at_start,
        false => no_retries_timeout,
    };

    client.run_open_loop(connection, schedule, total_time, timeout, !retries)?;
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
