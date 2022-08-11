use super::super::timing::ManualHistogram;
use color_eyre::eyre::{bail, Result};
use serde::{Deserialize, Serialize};
use serde_json::to_writer;
use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    ops::AddAssign,
};

const NANOS_IN_SEC: f64 = 1000000000.0;

pub fn pps_to_gbps(load: f64, size: usize) -> f64 {
    (size as f64 * load) / (125000000 as f64)
}

fn rolling_avg(avg: f64, val: f64, idx: usize) -> f64 {
    avg + (val - avg) / idx as f64
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct SummaryHistogram {
    // Precision in terms of nanoseconds
    pub precision: u64,
    // Map from latency marker to count
    pub map: BTreeMap<u64, u64>,
    // Count of total latencies
    pub count: usize,
    // Number of client threads this histogram represents
    pub num_client_threads: usize,
}

impl Default for SummaryHistogram {
    fn default() -> Self {
        SummaryHistogram {
            precision: 1000,
            map: BTreeMap::default(),
            count: 0,
            num_client_threads: 0,
        }
    }
}

impl SummaryHistogram {
    fn new(
        precision: u64,
        map: BTreeMap<u64, u64>,
        count: usize,
        num_client_threads: usize,
    ) -> Self {
        SummaryHistogram {
            precision: precision,
            map: map,
            count: count,
            num_client_threads: num_client_threads,
        }
    }

    pub fn record(&mut self, latency: u64) {
        let divisor = latency / self.precision;
        let bucket = (divisor + 1) * self.precision;
        *self.map.entry(bucket).or_insert(0) += 1;
        self.count += 1;
    }

    fn count(&self) -> usize {
        self.count
    }

    fn num_client_threads(&self) -> usize {
        self.num_client_threads
    }

    fn precision(&self) -> u64 {
        self.precision
    }

    fn map(&self) -> &BTreeMap<u64, u64> {
        &self.map
    }

    fn value_at_quantile(&self, quantile: f64) -> Result<u64> {
        let index = (self.count as f64 * quantile) as usize;
        let mut ct = 0;
        for (key, value) in self.map.iter() {
            if *value == 0 {
                continue;
            }
            for _ in 0..*value {
                if ct == index {
                    return Ok(*key);
                }
                ct += 1;
            }
        }
        bail!(
            "Could not find quantile {:?} for map with count {}",
            quantile,
            self.count
        );
    }

    fn min(&self) -> Result<u64> {
        if self.count == 0 {
            bail!("Histogram empty");
        }
        return Ok(*self.map.keys().next().unwrap());
    }

    fn max(&self) -> Result<u64> {
        if self.count == 0 {
            bail!("Histogram empty");
        }

        return Ok(*self.map.keys().last().unwrap());
    }

    fn avg(&self) -> Result<f64> {
        let mut avg = 0f64;
        let mut idx = 0;
        for (key, ct) in self.map.iter() {
            for _ in 0..*ct {
                idx += 1;
                avg = rolling_avg(avg, *key as f64, idx);
            }
        }
        Ok(avg)
    }

    fn get_summary_latencies(&self) -> Result<ThreadLatencies> {
        // find p5, p25, p50, p75, p95, p99 and p999 latencies, as well as min or max
        Ok(ThreadLatencies {
            num_threads: 1,
            avg: self.avg()? as _,
            p5: self.value_at_quantile(0.05)? as _,
            p25: self.value_at_quantile(0.25)? as _,
            p50: self.value_at_quantile(0.50)? as _,
            p75: self.value_at_quantile(0.75)? as _,
            p95: self.value_at_quantile(0.95)? as _,
            p99: self.value_at_quantile(0.99)? as _,
            p999: self.value_at_quantile(0.999)? as _,
            min: self.min()? as _,
            max: self.max()? as _,
        })
    }
}

impl std::ops::Add for SummaryHistogram {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        let count = self.count() + other.count();
        let num_threads = self.num_client_threads() + other.num_client_threads();
        // if precision is the same, just combine the two hashmaps
        if self.precision() == other.precision() {
            let mut res = self.map().clone();
            for (key, count) in other.map().iter() {
                *res.entry(*key).or_insert(0) += *count;
            }
            return SummaryHistogram::new(self.precision(), res, count, num_threads);
        } else if self.precision() > other.precision() {
            let mut res = self.map().clone();
            for (key, count) in other.map().iter() {
                let bucket = (*key / self.precision() + 1) * self.precision();
                *res.entry(bucket).or_insert(0) += *count;
            }
            return SummaryHistogram::new(self.precision(), res, count, num_threads);
        } else {
            let mut res = other.map().clone();
            for (key, count) in self.map().iter() {
                let bucket = (*key / other.precision() + 1) * other.precision();
                *res.entry(bucket).or_insert(0) += *count;
            }
            return SummaryHistogram::new(other.precision(), res, count, num_threads);
        }
    }
}

impl AddAssign for SummaryHistogram {
    fn add_assign(&mut self, other: Self) {
        *self = self.clone() + other;
    }
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
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
    pub summary_histogram: SummaryHistogram,
    pub summary_latencies: ThreadLatencies,
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

        let summary_hist = hist.summary_histogram();
        let summary_latencies = summary_hist.get_summary_latencies()?;

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
            summary_histogram: summary_hist,
            summary_latencies: summary_latencies,
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
        self.summary_latencies.dump(self.thread_id);
    }

    pub fn clear_summary_histogram(&mut self) {
        self.summary_histogram = SummaryHistogram::default();
    }

    pub fn summary_histogram(&self) -> SummaryHistogram {
        self.summary_histogram.clone()
    }
}

impl std::ops::Add for ThreadStats {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        let histogram = self.summary_histogram() + other.summary_histogram();
        let latencies = histogram.get_summary_latencies().unwrap();
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
            summary_histogram: histogram,
            summary_latencies: latencies,
        }
    }
}

fn vec_to_map(vec: Vec<ThreadStats>) -> (SummaryHistogram, HashMap<usize, ThreadStats>) {
    let mut map: HashMap<usize, ThreadStats> = HashMap::default();
    let mut summary_histogram = SummaryHistogram::default();
    for (i, mut stats) in vec.into_iter().enumerate() {
        assert!(stats.thread_id == i);
        summary_histogram += stats.summary_histogram();
        stats.clear_summary_histogram();
        map.insert(i, stats);
    }
    (summary_histogram, map)
}

pub fn dump_thread_stats(
    info: Vec<ThreadStats>,
    thread_info_path: Option<String>,
    dump_per_thread: bool,
) -> Result<()> {
    match thread_info_path {
        Some(p) => {
            let map = vec_to_map(info.clone());
            to_writer(&File::create(&p)?, &map)?;
        }
        None => {}
    }

    // now iterate and dump per thread stats
    let mut mega_info = ThreadStats::default();
    for stats in info.iter() {
        if dump_per_thread {
            stats.dump();
        }
        mega_info = mega_info + stats.clone();
    }

    tracing::warn!("About to print out stats for all threads");

    mega_info.dump();
    Ok(())
}
