use super::super::timing::ManualHistogram;
use color_eyre::eyre::Result;
use serde::{Deserialize, Serialize};
use serde_json::to_writer;
use std::fs::File;

const NANOS_IN_SEC: f64 = 1000000000.0;

pub fn pps_to_gbps(load: f64, size: usize) -> f64 {
    (size as f64 * load) / (125000000 as f64)
}

fn rolling_avg(avg: f64, val: f64, idx: usize) -> f64 {
    avg + (val - avg) / idx as f64
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Copy)]
pub struct ThreadLatencies {
    pub num_threads: usize,
    pub avg: f64,
    pub p5: f64,
    pub p25: f64,
    pub p50: f64,
    pub p75: f64,
    pub p95: f64,
    pub p99: f64,
    pub p999: f64,
    pub min: f64,
    pub max: f64,
}

impl Default for ThreadLatencies {
    fn default() -> Self {
        ThreadLatencies {
            min: f64::MAX,
            num_threads: 0,
            avg: 0.0,
            p5: 0.0,
            p25: 0.0,
            p50: 0.0,
            p75: 0.0,
            p95: 0.0,
            p99: 0.0,
            p999: 0.0,
            max: 0.0,
        }
    }
}

impl ThreadLatencies {
    fn dump(&self, thread_id: usize) {
        tracing::info!(
            p5_ns =? self.p5,
            p25_ns =? self.p25,
            p50_ns =? self.p50,
            p75_ns =? self.p75,
            p95_ns =? self.p95,
            p99_ns =? self.p99,
            p999_ns =? self.p999,
            avg_ns = ?self.avg,
            max_ns = ?self.max,
            min_ns = ?self.min,
            "thread {} latencies.",
            thread_id
        );

        tracing::info!(
            p50_ns =? self.p50,
            p99_ns =? self.p99,
            avg =? self.avg,
            "thread {} summary latencies.",
            thread_id
        );
    }
}

impl std::ops::Add for ThreadLatencies {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        let idx = self.num_threads + 1;
        assert!(other.num_threads == 1); // can only add a single thread in
        Self {
            num_threads: idx,
            avg: rolling_avg(self.avg, other.avg, idx),
            p5: rolling_avg(self.p5, other.p5, idx),
            p25: rolling_avg(self.p25, other.p25, idx),
            p50: rolling_avg(self.p50, other.p50, idx),
            p75: rolling_avg(self.p75, other.p75, idx),
            p95: rolling_avg(self.p95, other.p95, idx),
            p99: rolling_avg(self.p99, other.p99, idx),
            p999: rolling_avg(self.p999, other.p999, idx),
            min: self.min.min(other.min),
            max: self.max.max(other.max),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default, Copy)]
pub struct ThreadStats {
    pub thread_id: usize,
    pub num_sent: usize,
    pub num_received: usize,
    pub retries: usize,
    pub runtime: f64,
    pub offered_load_pps: f64,
    pub offered_load_gbps: f64,
    pub achieved_load_pps: f64,
    pub achieved_load_gbps: f64,
    pub latencies: ThreadLatencies,
}

impl ThreadStats {
    pub fn new(
        id: u16,
        sent: usize,
        recved: usize,
        retries: usize,
        runtime: f64, // in nanos
        offered_load_pps: u64,
        message_size: usize,
        hist: &mut ManualHistogram,
        cutoff_size: usize,
    ) -> Result<Self> {
        let offered_load_gbps = pps_to_gbps(offered_load_pps as f64, message_size);
        let achieved_load_pps = recved as f64 / (runtime / NANOS_IN_SEC);
        let achieved_load_gbps = pps_to_gbps(achieved_load_pps, message_size);

        if !hist.is_sorted() {
            hist.sort_and_truncate(cutoff_size)?;
        }

        Ok(ThreadStats {
            thread_id: id as usize,
            num_sent: sent,
            num_received: recved,
            retries: retries,
            runtime: runtime / NANOS_IN_SEC,
            offered_load_pps: offered_load_pps as f64,
            offered_load_gbps: offered_load_gbps,
            achieved_load_pps: achieved_load_pps,
            achieved_load_gbps: achieved_load_gbps,
            latencies: hist.thread_latencies()?,
        })
    }

    pub fn dump(&self) {
        tracing::info!(
            thread = self.thread_id,
            num_sent = self.num_sent,
            num_received = self.num_received,
            num_retries = self.retries,
            offered_pps =?self.offered_load_pps,
            offered_gbps = ?self.offered_load_gbps,
            achieved_pps = ?self.achieved_load_pps,
            achieved_gbps = ?self.achieved_load_gbps,
            percent_achieved = ?(self.achieved_load_gbps / self.offered_load_gbps),
            "thread {} summary stats", self.thread_id
        );
        self.latencies.dump(self.thread_id);
    }
}

impl std::ops::Add for ThreadStats {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            thread_id: other.thread_id,
            num_sent: self.num_sent + other.num_sent,
            num_received: self.num_received + other.num_received,
            retries: self.retries + other.retries,
            runtime: self.runtime + other.runtime,
            offered_load_pps: self.offered_load_pps + other.offered_load_pps,
            offered_load_gbps: self.offered_load_gbps + other.offered_load_gbps,
            achieved_load_pps: self.achieved_load_pps + other.achieved_load_pps,
            achieved_load_gbps: self.achieved_load_gbps + other.achieved_load_gbps,
            latencies: self.latencies + other.latencies,
        }
    }
}
pub fn dump_thread_stats(
    info: Vec<ThreadStats>,
    thread_info_path: Option<String>,
    dump_per_thread: bool,
) -> Result<()> {
    match thread_info_path {
        Some(p) => {
            to_writer(&File::create(&p)?, &info)?;
        }
        None => {}
    }

    // now iterate and dump per thread stats
    let mut mega_info = ThreadStats::default();
    for stats in info.iter() {
        if dump_per_thread {
            stats.dump();
        }
        mega_info = mega_info + *stats;
    }

    tracing::warn!("About to print out stats for all threads");

    mega_info.dump();
    Ok(())
}
