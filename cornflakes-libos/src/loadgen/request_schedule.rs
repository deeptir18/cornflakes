use color_eyre::eyre::{bail, Result, WrapErr};
use rand::thread_rng;
use rand_distr::{Distribution, Exp};

#[inline]
pub fn rate_pps_to_interarrival_nanos(rate: u64) -> f64 {
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
    Exponential(Exp<f64>),
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
                let l = 1.0 / interarrival_nanos;
                let exp = Exp::new(l).wrap_err("Not able to make exponential distribution")?;
                Ok(PacketDistribution::Exponential(exp))
            }
        }
    }

    fn sample(&self) -> u64 {
        // TODO: how do we know the thread rngs are initialized?
        let mut rng = thread_rng();
        match *self {
            PacketDistribution::Uniform(interarrival_nanos) => interarrival_nanos,
            PacketDistribution::Exponential(exp) => exp.sample(&mut rng) as u64,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct Packet {
    pub time_since_last: u64, // in nanos since the experiment start
}

#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct PacketSchedule {
    pub packets: Vec<Packet>,
    //distribution: PacketDistribution,
}

impl PacketSchedule {
    pub fn new(num_requests: usize, rate_pps: u64, dist_type: DistributionType) -> Result<Self> {
        tracing::info!("Initializing packet schedule for {} requests", num_requests);
        let distribution = PacketDistribution::new(dist_type, rate_pps)
            .wrap_err("Failed to initialize distribution")?;
        let mut packets: Vec<Packet> = vec![Packet::default(); num_requests];
        for i in 0..num_requests {
            packets[i] = Packet {
                time_since_last: distribution.sample(),
            };
        }
        // first packet starts at time 0
        packets[0] = Packet { time_since_last: 0 };

        Ok(PacketSchedule { packets: packets })
    }

    pub fn new_twitter (times: Vec<u64>) -> Result<Self> {
        let mut sum : u64 = 0;
        for i in 0..times.len() {
            sum += times[i];
        }
        let mut packets: Vec<Packet> = vec![Packet::default(); sum as usize];
        let mut index : usize = 0;
        for i in 0..times.len() {
            let nanosec : u64 = 1000000000;
            let time_lapse = nanosec/times[i];
            let mut base : u64 = i as u64;
            for j in 0..times[i] {
                packets[(index + (j as usize))] = Packet{ time_since_last: base };
                base += time_lapse;
            }
            index += times[i] as usize;
        }
        Ok(PacketSchedule { packets: packets })
    }

    fn get(&self, idx: usize) -> u64 {
        self.packets[idx].time_since_last
    }

    pub fn get_next_in_cycles(
        &self,
        idx: usize,
        last_cycle: u64,
        hz: u64,
        deficit_cycles: u64,
    ) -> u64 {
        let intersend = self.get(idx);
        let add = nanos_to_hz(hz, intersend);
        if deficit_cycles > add {
            return last_cycle;
        }
        last_cycle + (add - deficit_cycles)
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

pub fn generate_twitter_schedules(
    times: Vec<u64>,
    num_threads: usize,
) -> Result<Vec<PacketSchedule>> {
  let mut schedules : Vec<PacketSchedule> = Vec::default();
  for _i in 0..num_threads {
      schedules.push(PacketSchedule::new_twitter(times.clone())?);
  }
  Ok(schedules)
}
