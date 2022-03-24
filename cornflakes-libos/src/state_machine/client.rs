use super::super::{
    datapath::{Datapath, ReceivedPkt},
    loadgen::request_schedule::PacketSchedule,
    timing::ManualHistogram,
    utils::AddressInfo,
    MsgID,
};
use color_eyre::eyre::{Result, WrapErr};
use std::time::{Duration, Instant};

pub trait ClientSM {
    type Datapath: Datapath;

    fn server_addr(&self) -> AddressInfo;

    fn get_next_msg(&mut self) -> Result<Option<(MsgID, &[u8])>>;

    fn process_received_msg(
        &mut self,
        sga: ReceivedPkt<<Self as ClientSM>::Datapath>,
    ) -> Result<()>;

    /// What to do when a particular message times out.
    fn msg_timeout_cb(&mut self, id: MsgID) -> Result<&[u8]>;

    /// Initializes any internal state with any datapath specific configuration,
    /// e.g., registering external memory.
    fn init(&mut self, connection: &mut Self::Datapath) -> Result<()>;

    fn cleanup(&mut self, connection: &mut Self::Datapath) -> Result<()>;

    fn get_mut_rtts(&mut self) -> &mut ManualHistogram;

    fn record_rtt(&mut self, rtt: Duration) {
        self.get_mut_rtts().record(rtt.as_nanos() as u64);
    }

    fn uniq_received_so_far(&self) -> usize;

    fn uniq_sent_so_far(&self) -> usize;

    fn num_retried(&self) -> usize;

    fn num_timed_out(&self) -> usize;

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
        Ok(())
    }

    /// Run open loop client
    fn run_closed_loop(
        &mut self,
        datapath: &mut Self::Datapath,
        num_pkts: u64,
        time_out: impl Fn(usize) -> Duration,
        server_addr: &AddressInfo,
    ) -> Result<()> {
        let mut recved = 0;
        if recved >= num_pkts {
            return Ok(());
        }
        let conn_id = datapath
            .connect(server_addr.clone())
            .wrap_err("Could not get connection ID")?;

        while let Some((id, msg)) = self.get_next_msg()? {
            if recved >= num_pkts {
                break;
            }
            datapath.push_buffers_with_copy(vec![(id, conn_id, msg)])?;
            let recved_pkts = loop {
                let pkts = datapath.pop_with_durations()?;
                if pkts.len() > 0 {
                    break pkts;
                }
                for (id, conn) in datapath
                    .timed_out(time_out(self.uniq_received_so_far()))?
                    .iter()
                {
                    datapath.push_buffers_with_copy(vec![(
                        *id,
                        *conn,
                        self.msg_timeout_cb(*id)?,
                    )])?;
                }
            };

            for (pkt, rtt) in recved_pkts.into_iter() {
                self.record_rtt(rtt);
                let msg_id = pkt.msg_id();
                self.process_received_msg(pkt).wrap_err(format!(
                    "Error in processing received response for pkt {}.",
                    msg_id
                ))?;
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
        server_addr: &AddressInfo,
    ) -> Result<()> {
        let freq = datapath.timer_hz(); // cycles per second
        let conn_id = datapath
            .connect(server_addr.clone())
            .wrap_err("No more available connection IDs")?;
        let time_start = Instant::now();
        let mut idx = 0;

        let start = datapath.current_cycles();
        let mut deficit;
        let mut next = start;
        while let Some((id, msg)) = self.get_next_msg()? {
            if datapath.current_cycles() > (total_time * freq + start) {
                tracing::debug!("Total time done");
                break;
            }

            // Send the next message
            tracing::debug!(time = ?time_start.elapsed(), "About to send next packet");
            datapath.push_buffers_with_copy(vec![(id, conn_id, msg)])?;
            idx += 1;
            let last_sent = datapath.current_cycles();
            deficit = last_sent - next;
            next = schedule.get_next_in_cycles(idx, last_sent, freq, deficit);

            while datapath.current_cycles() <= next {
                let recved_pkts = datapath.pop_with_durations()?;
                for (pkt, rtt) in recved_pkts.into_iter() {
                    self.record_rtt(rtt);
                    let msg_id = pkt.msg_id();
                    self.process_received_msg(pkt).wrap_err(format!(
                        "Error in processing received response for pkt {}.",
                        msg_id
                    ))?;
                }

                if !no_retries {
                    for (id, conn) in datapath
                        .timed_out(time_out(self.uniq_received_so_far()))?
                        .iter()
                    {
                        datapath.push_buffers_with_copy(vec![(
                            *id,
                            *conn,
                            self.msg_timeout_cb(*id)?,
                        )])?;
                    }
                }
            }
        }

        tracing::debug!("Finished sending");
        Ok(())
    }
}
