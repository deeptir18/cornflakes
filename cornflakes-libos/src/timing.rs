use super::{loadgen::client_threads::ThreadLatencies, MsgID};
use color_eyre::eyre::{bail, ensure, Result};
use hashbrown::HashMap;
use hdrhistogram::Histogram;
use std::{
    fs::File,
    io::Write,
    net::Ipv4Addr,
    sync::{Arc, Mutex},
    time::Instant,
};

#[inline]
pub fn timefunc(
    func: &mut dyn FnMut() -> Result<()>,
    cond: bool,
    timer: Option<Arc<Mutex<HistogramWrapper>>>,
) -> Result<()> {
    if cond {
        let start = Instant::now();
        func()?;
        let inner = timer.unwrap();
        inner
            .lock()
            .unwrap()
            .record(start.elapsed().as_nanos() as u64)?;
        Ok(())
    } else {
        func()
    }
}

#[inline]
pub fn record(timer: Option<Arc<Mutex<HistogramWrapper>>>, val: u64) -> Result<()> {
    match timer {
        Some(inner) => {
            inner.lock().unwrap().record(val)?;
        }
        None => {}
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManualHistogram {
    current_count: usize,
    latencies: Vec<u64>,
    sorted_latencies: Vec<u64>,
    is_sorted: bool,
}

impl ManualHistogram {
    pub fn new(num_values: usize) -> Self {
        ManualHistogram {
            current_count: 0,
            latencies: vec![0u64; num_values as usize],
            sorted_latencies: Vec::default(),
            is_sorted: false,
        }
    }

    pub fn init(rate_pps: u64, total_time_sec: u64) -> Self {
        // ten percent over
        let num_values = ((rate_pps * total_time_sec) as f64 * 1.10) as usize;
        ManualHistogram {
            current_count: 0,
            latencies: vec![0u64; num_values],
            sorted_latencies: Vec::default(),
            is_sorted: false,
        }
    }

    pub fn is_sorted(&self) -> bool {
        self.is_sorted
    }

    pub fn record(&mut self, val: u64) {
        if self.current_count == self.latencies.len() {
            self.latencies.push(val);
        } else {
            self.latencies[self.current_count] = val;
        }
        self.current_count += 1;
    }

    pub fn sort(&mut self) -> Result<()> {
        self.sort_and_truncate(0)
    }

    pub fn sort_and_truncate(&mut self, start: usize) -> Result<()> {
        if self.is_sorted {
            return Ok(());
        }
        ensure!(
            start < self.current_count,
            format!(
                "Cannot truncate entire array: start: {}, count: {}",
                start, self.current_count
            )
        );

        self.sorted_latencies = self.latencies.as_slice()[start..self.current_count].to_vec();
        self.sorted_latencies.sort();
        self.is_sorted = true;
        Ok(())
    }

    pub fn value_at_quantile(&self, quantile: f64) -> Result<u64> {
        if self.sorted_latencies.len() == 0 {
            bail!("Cannot run value_at_quantile until sort() has been called.");
        }
        let index = (self.sorted_latencies.len() as f64 * quantile) as usize;
        Ok(self.sorted_latencies[index])
    }

    /// Logs all the latencies to a file
    pub fn log_to_file(&self, path: &str) -> Result<()> {
        self.log_truncated_to_file(path, 0)
    }

    pub fn log_truncated_to_file(&self, path: &str, start: usize) -> Result<()> {
        let mut file = File::create(path)?;
        for idx in start..self.current_count {
            writeln!(file, "{}", self.latencies[idx])?;
        }
        Ok(())
    }

    fn mean(&self) -> Result<f64> {
        if self.sorted_latencies.len() == 0 {
            bail!("Cannot run value_at_quantile until sort() has been called.");
        }

        // TODO: use iterative algorithm that won't overflow
        let sum: u64 = self.sorted_latencies.iter().sum();
        Ok(sum as f64 / (self.sorted_latencies.len() as f64))
    }

    fn max(&self) -> Result<u64> {
        if self.sorted_latencies.len() == 0 {
            bail!("Cannot run value_at_quantile until sort() has been called.");
        }

        Ok(self.sorted_latencies[self.sorted_latencies.len() - 1])
    }

    fn min(&self) -> Result<u64> {
        if self.sorted_latencies.len() == 0 {
            bail!("Cannot run value_at_quantile until sort() has been called.");
        }

        Ok(self.sorted_latencies[0])
    }

    pub fn dump(&self, msg: &str) -> Result<()> {
        if self.current_count == 0 {
            return Ok(());
        }

        tracing::info!(
            msg,
            p5_ns = self.value_at_quantile(0.05)?,
            p25_ns = self.value_at_quantile(0.25)?,
            p50_ns = self.value_at_quantile(0.5)?,
            p75_ns = self.value_at_quantile(0.75)?,
            p95_ns = self.value_at_quantile(0.95)?,
            p99_ns = self.value_at_quantile(0.99)?,
            p999_ns = self.value_at_quantile(0.999)?,
            pkts_recved = self.current_count,
            min_ns = self.min()?,
            max_ns = self.max()?,
            avg_ns = ?self.mean()?,
        );
        Ok(())
    }

    pub fn thread_latencies(&self) -> Result<ThreadLatencies> {
        Ok(ThreadLatencies {
            num_threads: 1,
            avg: self.mean()? as _,
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

pub trait RTTHistogram {
    fn get_histogram_mut(&mut self) -> &mut Histogram<u64>;
    fn get_histogram(&self) -> &Histogram<u64>;

    fn count(&self) -> u64 {
        self.get_histogram().len()
    }

    fn add_latency(&mut self, val: u64) -> Result<()> {
        tracing::debug!(val_ns = val, "Adding latency to hist");
        self.get_histogram_mut().record(val)?;
        Ok(())
    }

    fn dump(&self, msg: &str) {
        if self.get_histogram().len() == 0 {
            return;
        }
        tracing::info!(
            msg,
            p5_ns = self.get_histogram().value_at_quantile(0.05),
            p25_ns = self.get_histogram().value_at_quantile(0.25),
            p50_ns = self.get_histogram().value_at_quantile(0.5),
            p75_ns = self.get_histogram().value_at_quantile(0.75),
            p95_ns = self.get_histogram().value_at_quantile(0.95),
            p99_ns = self.get_histogram().value_at_quantile(0.99),
            pkts_sent = self.get_histogram().len(),
            min_ns = self.get_histogram().min(),
            max_ns = self.get_histogram().max(),
            avg_ns = ?self.get_histogram().mean(),
        );
        tracing::info!(
            msg = ?format!("{}: summary statistics:", msg),
            p50_ns = self.get_histogram().value_at_quantile(0.5),
            avg_ns = ?self.get_histogram().mean(),
            p99_ns = self.get_histogram().value_at_quantile(0.99)
        );
    }
}

pub struct HistogramWrapper {
    /// Actual histogram
    hist: Histogram<u64>,
    /// Map of (IpAddr, PktID) => (start times). TODO: figure out how retries fit into this
    pkt_map: HashMap<(Ipv4Addr, MsgID), Vec<Instant>>,
    /// Name of the measurement
    name: String,
}

impl HistogramWrapper {
    pub fn new(name: &str) -> Result<HistogramWrapper> {
        Ok(HistogramWrapper {
            hist: Histogram::new_with_max(10_000_000_000, 2)?,
            pkt_map: HashMap::default(),
            name: name.to_string(),
        })
    }

    pub fn get_hist(&self) -> Histogram<u64> {
        self.hist.clone()
    }

    pub fn combine(&mut self, other: &HistogramWrapper) -> Result<()> {
        self.hist.add(other.get_hist())?;
        Ok(())
    }

    pub fn dump_stats(&self) {
        let name = self.name.clone();
        self.dump(name.as_ref());
    }

    pub fn record(&mut self, val: u64) -> Result<()> {
        self.add_latency(val)
    }

    pub fn start_entry(&mut self, addr: Ipv4Addr, id: MsgID) -> Result<()> {
        if self.pkt_map.contains_key(&(addr, id)) {
            tracing::warn!(
                hist = ?self.name,
                id = id,
                addr = ?addr,
                "Already contains key for inserton."
            );
            let vec = self.pkt_map.get_mut(&(addr, id)).unwrap();
            vec.push(Instant::now());
        } else {
            self.pkt_map.insert((addr, id), vec![Instant::now()]);
        }
        Ok(())
    }

    pub fn end_entry(&mut self, addr: Ipv4Addr, id: MsgID) -> Result<()> {
        let mut delete = false;
        let head_start = match self.pkt_map.get_mut(&(addr, id)) {
            Some(s) => {
                let head_start = s.pop().unwrap();
                if s.len() == 0 {
                    delete = true;
                }
                head_start
            }
            None => {
                tracing::error!(
                    hist = ?self.name,
                    id = id,
                    addr = ?addr,
                    "Histogram doesn't contain entry for this id and address."
                );
                bail!("Histogram insertion error.");
            }
        };
        self.add_latency(head_start.elapsed().as_nanos() as u64)?;
        if delete {
            let _ = self.pkt_map.remove(&(addr, id)).unwrap();
        }
        Ok(())
    }
}

impl RTTHistogram for HistogramWrapper {
    fn get_histogram_mut(&mut self) -> &mut Histogram<u64> {
        &mut self.hist
    }

    fn get_histogram(&self) -> &Histogram<u64> {
        &self.hist
    }
}
