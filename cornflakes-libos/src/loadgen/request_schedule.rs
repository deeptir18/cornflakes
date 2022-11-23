use color_eyre::eyre::{bail, Result, WrapErr};
use quanta::Clock;
use rand::thread_rng;
use rand_distr::{Distribution, Exp};
use std::time::Duration;

pub struct SpinTimer {
    clk: quanta::Clock,
    interarrivals: PacketSchedule,
    deficit: Duration,
    cur_idx: usize,
    total_time: Duration,
    start_raw: u64,
    last_return: Option<u64>,
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
            last_return: None,
        }
    }

    pub fn done(&self) -> bool {
        self.clk.delta(self.start_raw, self.clk.raw()) >= self.total_time
    }

    pub fn wait(&mut self, callback: &mut dyn FnMut() -> Result<()>) -> Result<()> {
        let next_interarrival_ns = self.interarrivals.get(self.cur_idx);
        self.cur_idx += 1;

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
            callback()?;
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
}

impl PacketSchedule {
    pub fn new(num_requests: usize, rate_pps: u64, dist_type: DistributionType) -> Result<Self> {
        tracing::info!("Initializing packet schedule for {} requests", num_requests);
        let distribution = PacketDistribution::new(dist_type, rate_pps)
            .wrap_err("Failed to initialize distribution")?;
        let mut interarrivals: Vec<Duration> = Vec::with_capacity(num_requests);
        for _ in 0..num_requests {
            interarrivals.push(Duration::from_nanos(distribution.sample()));
        }
        // first packet starts at time 0
        interarrivals[0] = Duration::from_nanos(0);

        Ok(PacketSchedule {
            interarrivals: interarrivals,
        })
    }

    fn get(&self, idx: usize) -> Duration {
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
