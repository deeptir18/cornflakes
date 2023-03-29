use color_eyre::eyre::{bail, Result, WrapErr};
use quanta::Clock;
use rand::thread_rng;
use rand_distr::{Distribution, Exp};
use std::time::Duration;
const WARMUP_PERCENTAGE: f64 = 0.10;

pub struct SpinTimer {
    clk: quanta::Clock,
    interarrivals: PacketSchedule,
    deficit: Duration,
    cur_idx: usize,
    total_time: Duration,
    start_raw: u64,
    last_return: Option<u64>,
    no_more_pkts: bool,
}

impl SpinTimer {
    pub fn new(interarrivals: PacketSchedule, total_time: Duration) -> Self {
        let clk = Clock::new();
        let start_raw = clk.raw();
        SpinTimer {
            start_raw: start_raw,
            clk: clk,
            interarrivals: interarrivals,
            deficit: Duration::from_nanos(0),
            cur_idx: 0,
            total_time: total_time,
            last_return: Some(start_raw),
            no_more_pkts: false,
        }
    }

    pub fn done(&self) -> bool {
        self.clk.delta(self.start_raw, self.clk.raw()) >= self.total_time || self.no_more_pkts
    }

    pub fn warmup_done(&self) -> bool {
        // drop first x % of requests
        self.clk.delta(self.start_raw, self.clk.raw()) >= self.total_time.mul_f64(WARMUP_PERCENTAGE)
    }

    pub fn wait(&mut self, callback: &mut dyn FnMut(bool) -> Result<()>) -> Result<()> {
        let next_interarrival_ns = self.interarrivals.get(self.cur_idx);
        self.cur_idx += 1;
        if self.cur_idx == self.interarrivals.len() {
            // no more packets to send
            self.no_more_pkts = true;
        }

        // if built up deficit is too high, return
        if self.deficit > next_interarrival_ns {
            self.deficit -= next_interarrival_ns;
            self.last_return = Some(self.clk.raw());
            return Ok(());
        }

        if self.last_return.is_none() {
            self.last_return = Some(self.clk.raw());
        }

        while (self.clk.delta(self.last_return.unwrap(), self.clk.raw())) < next_interarrival_ns {
            callback(self.warmup_done())?;
        }
        self.deficit +=
            self.clk.delta(self.last_return.unwrap(), self.clk.raw()) - next_interarrival_ns;
        self.last_return = Some(self.clk.raw());
        Ok(())
    }
}
#[inline]
pub fn rate_pps_to_interarrival_nanos(rate: u64) -> f64 {
    tracing::debug!("Nanos intersend: {:?}", 1_000_000_000.0 / rate as f64);
    1_000_000_000.0 / rate as f64
}

#[inline]
pub fn nanos_to_hz(hz: u64, nanos: u64) -> u64 {
    ((hz as f64 / 1_000_000_000.0) * (nanos as f64)) as u64
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum DistributionType {
    Uniform,
    Exponential,
}

#[derive(Debug, Copy, Clone)]
pub enum PacketDistribution {
    Uniform(u64),
    Exponential(f64),
}

impl std::str::FromStr for DistributionType {
    type Err = color_eyre::eyre::Error;
    fn from_str(s: &str) -> Result<DistributionType> {
        Ok(match s {
            "uniform" | "Uniform" | "UNIFORM" => DistributionType::Uniform,
            "exponential" | "Exponential" | "EXPONENTIAL" | "exp" | "EXP" => {
                DistributionType::Exponential
            }
            x => bail!("{} distribution type unknown", x),
        })
    }
}

impl PacketDistribution {
    fn new(typ: DistributionType, rate_pps: u64) -> Result<Self> {
        let interarrival_nanos = rate_pps_to_interarrival_nanos(rate_pps);
        match typ {
            DistributionType::Uniform => Ok(PacketDistribution::Uniform(interarrival_nanos as u64)),
            DistributionType::Exponential => {
                let l = interarrival_nanos;
                Ok(PacketDistribution::Exponential(l))
            }
        }
    }

    fn get_interarrival_avg(&self) -> u64 {
        match self {
            PacketDistribution::Uniform(x) => *x,
            PacketDistribution::Exponential(x) => *x as u64,
        }
    }

    fn sample(&self) -> u64 {
        // TODO: how do we know the thread rngs are initialized?
        let mut rng = thread_rng();
        match *self {
            PacketDistribution::Uniform(interarrival_nanos) => interarrival_nanos,
            PacketDistribution::Exponential(l) => {
                let exp = Exp::new(1.0 / l)
                    .wrap_err("Not able to make exponential distribution")
                    .unwrap();
                exp.sample(&mut rng) as u64
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct PacketSchedule {
    pub interarrivals: Vec<Duration>,
    pub avg_interarrival: u64,
}

impl PacketSchedule {
    pub fn new(num_requests: usize, rate_pps: u64, dist_type: DistributionType) -> Result<Self> {
        tracing::debug!("Initializing packet schedule for {} requests", num_requests);
        let distribution = PacketDistribution::new(dist_type, rate_pps)
            .wrap_err("Failed to initialize distribution")?;
        let mut interarrivals: Vec<Duration> = Vec::with_capacity(num_requests);
        for _ in 0..num_requests {
            interarrivals.push(Duration::from_nanos(distribution.sample()));
        }

        Ok(PacketSchedule {
            interarrivals,
            avg_interarrival: distribution.get_interarrival_avg(),
        })
    }

    pub fn get_avg_interarrival(&self) -> u64 {
        self.avg_interarrival
    }

    pub fn start_at_offset(
        &mut self,
        thread_id: usize,
        client_id: usize,
        num_threads: usize,
        num_clients: usize,
    ) {
        let multiplier =
            (num_clients * thread_id + client_id) as f64 / (num_clients * num_threads) as f64;
        self.interarrivals[0] = self.interarrivals[0].mul_f64(multiplier);
    }

    pub fn get_last(&self) -> Option<Duration> {
        if self.interarrivals.len() > 0 {
            let last_idx = self.interarrivals.len() - 1;
            return Some(self.interarrivals[last_idx]);
        } else {
            return None;
        }
    }

    pub fn set_last(&mut self, dur: Duration) {
        let last_idx = self.interarrivals.len() - 1;
        self.interarrivals[last_idx] = dur;
    }

    pub fn append(&mut self, other: &mut PacketSchedule) {
        // get weighted avg
        let total_interarrivals = self.len() + other.len();
        self.avg_interarrival =
            (self.get_avg_interarrival() as f64 * (self.len() as f64 / total_interarrivals as f64)
                + other.get_avg_interarrival() as f64
                    * (other.len() as f64 / total_interarrivals as f64)) as u64;
        self.interarrivals.append(other.get_mut_interarrivals());
    }

    pub fn get_mut_interarrivals(&mut self) -> &mut Vec<Duration> {
        &mut self.interarrivals
    }

    pub fn len(&self) -> usize {
        self.interarrivals.len()
    }

    pub fn get(&self, idx: usize) -> Duration {
        self.interarrivals[idx]
    }
}

pub fn generate_schedules(
    requests: usize,
    rate_pps: u64,
    dist: DistributionType,
    num_threads: usize,
) -> Result<Vec<PacketSchedule>> {
    let mut schedules: Vec<PacketSchedule> = Vec::default();
    for _i in 0..num_threads {
        schedules.push(PacketSchedule::new(requests, rate_pps, dist)?);
    }
    Ok(schedules)
}
