use super::MsgID;
use color_eyre::eyre::{bail, Result};
use hashbrown::HashMap;
use hdrhistogram::Histogram;
use std::{
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

/*#[macro_export]
macro_rules! timefunc (
($func: expr, $cond: expr, $record_func: path, $timer: expr) => {
    if $cond {
        let start = Instant::now();
        let x = $func;
        $record_func($timer, start.elapsed().as_nanos())?;
        x
    } else {
        $func
    }
}
);*/
pub trait RTTHistogram {
    fn get_histogram_mut(&mut self) -> &mut Histogram<u64>;
    fn get_histogram(&self) -> &Histogram<u64>;

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
