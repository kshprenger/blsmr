use std::{fs::File, io::Write, path::PathBuf};

use blsmr::{
    BLSMRProtocol, KEY_ANNOUNCE_TIMEOUT, KEY_AVG_COMMIT_LATENCY, KEY_COMMIT_LATENCIES,
    KEY_CONFLICT_RATE, KEY_KEY_COUNT, KEY_PROTOCOL_TYPE, KEY_QUORUM_SYSTEM, KEY_SUBMIT_INTERVAL,
    KEY_SUBMIT_LIMIT, KEY_TRACK_CONFLICT_RATE, POOL_BLSMR, log, process::BLSMR,
};
use dscale::{
    BandwidthConfig, Distr, Jiffies, SimulationBuilder, mpi,
    rand::{SeedableRng, rngs::SmallRng, seq::SliceRandom},
    services::kv,
};

const REPLICAS: usize = 6;
const TIME_BUDGET: Jiffies = Jiffies(2_000_000);
const KEY_COUNT: usize = 32;
const COMMANDS_PER_REPLICA: usize = 20;
const ANNOUNCE_TIMEOUT: Jiffies = Jiffies(500);
const SEEDS: [u64; 5] = [42, 43, 44, 45, 46];
const EARTH_RADIUS_KM: f64 = 6_371.0;
const LIGHT_SPEED_KM_PER_SECOND: f64 = 299_792.458;

#[derive(Clone, Copy)]
struct Params {
    submit_interval: Jiffies,
    seed: u64,
}

struct Region {
    name: String,
    latitude: f64,
    longitude: f64,
}

fn sweep() -> Vec<Params> {
    [1, 19, 24, 31, 37, 46, 59, 78, 114, 200, 20_000]
        .into_iter()
        .flat_map(|submit_interval| {
            SEEDS.into_iter().map(move |seed| Params {
                submit_interval: Jiffies(submit_interval),
                seed,
            })
        })
        .collect()
}

fn regions(seed: u64) -> Vec<Region> {
    let mut regions = include_str!("latency_cdf/aws_regions.csv")
        .lines()
        .skip(1)
        .map(|line| {
            let mut fields = line.split(',');
            Region {
                name: fields.next().expect("missing AWS Region ID").to_owned(),
                latitude: fields
                    .next()
                    .expect("missing latitude")
                    .parse()
                    .expect("invalid latitude"),
                longitude: fields
                    .next()
                    .expect("missing longitude")
                    .parse()
                    .expect("invalid longitude"),
            }
        })
        .collect::<Vec<_>>();
    regions.shuffle(&mut SmallRng::seed_from_u64(seed));
    regions.truncate(REPLICAS);
    regions
}

fn fixed_latency(latency: Jiffies) -> Distr {
    Distr::Uniform {
        low: latency,
        high: latency,
    }
}

fn light_latency(from: &Region, to: &Region) -> Jiffies {
    let latitude_delta = (to.latitude - from.latitude).to_radians();
    let longitude_delta = (to.longitude - from.longitude).to_radians();
    let from_latitude = from.latitude.to_radians();
    let to_latitude = to.latitude.to_radians();
    let half_chord = (latitude_delta / 2.0).sin().powi(2)
        + from_latitude.cos() * to_latitude.cos() * (longitude_delta / 2.0).sin().powi(2);
    let distance_km = EARTH_RADIUS_KM * 2.0 * half_chord.sqrt().asin();
    Jiffies((distance_km / LIGHT_SPEED_KM_PER_SECOND * 1_000.0).ceil() as usize)
}

fn simulation(seed: u64) -> Box<dyn dscale::SimulationRunner> {
    let regions = regions(seed);
    let mut builder = SimulationBuilder::new()
        .default_bandwidth(BandwidthConfig::Unbounded)
        .time_budget(TIME_BUDGET)
        .seed(seed)
        .par_sched(dscale::ThreadNumber::MatchCores);
    for region in &regions {
        builder = builder.add_pool::<BLSMR>(&region.name, 1);
    }
    for region in &regions {
        builder = builder.within_pool_latency(&region.name, fixed_latency(Jiffies(0)));
    }
    for (index, from) in regions.iter().enumerate() {
        for to in &regions[index + 1..] {
            builder = builder.between_pool_latency(
                &from.name,
                &to.name,
                fixed_latency(light_latency(from, to)),
            );
        }
    }
    builder.build()
}

fn run_once(params: Params) -> (Params, f64, f64) {
    let mut simulation = simulation(params.seed);
    kv::set(KEY_PROTOCOL_TYPE, BLSMRProtocol::Wintermute);
    kv::set(KEY_SUBMIT_INTERVAL, params.submit_interval);
    kv::set(KEY_SUBMIT_LIMIT, COMMANDS_PER_REPLICA);
    kv::set(KEY_TRACK_CONFLICT_RATE, true);
    kv::set(KEY_ANNOUNCE_TIMEOUT, ANNOUNCE_TIMEOUT);
    kv::set(KEY_KEY_COUNT, KEY_COUNT);
    kv::set(
        KEY_QUORUM_SYSTEM,
        quorum::QuorumSystem::new_dissemination(dscale::list_pool(POOL_BLSMR)),
    );
    kv::set::<(usize, usize)>(KEY_AVG_COMMIT_LATENCY, (0, 0));
    kv::set::<Vec<Jiffies>>(KEY_COMMIT_LATENCIES, Vec::new());
    kv::set(
        KEY_CONFLICT_RATE,
        log::ConflictTracker::new(dscale::list_pool(POOL_BLSMR).len()),
    );
    simulation.run_full_budget();
    (
        params,
        log::conflict_rate_percentage(),
        log::average_commit_latency(),
    )
}

fn output_path() -> PathBuf {
    PathBuf::from(format!(
        "terrestrial_conflict_latency_rank{}.csv",
        mpi::rank()
    ))
}

fn main() {
    let results = mpi::distribute(sweep(), run_once);
    let path = output_path();
    let mut file = File::create(&path).expect("failed to create results file");
    writeln!(
        file,
        "key_count,commands_per_replica,submit_interval,announce_timeout,seed,conflict_rate_pct,avg_latency_jiffies"
    )
    .expect("failed to write header");
    for (params, conflict_rate, latency) in &results {
        writeln!(
            file,
            "{KEY_COUNT},{COMMANDS_PER_REPLICA},{},{},{},{conflict_rate:.4},{latency:.4}",
            params.submit_interval.0, ANNOUNCE_TIMEOUT.0, params.seed
        )
        .expect("failed to write row");
    }
    println!("wrote {} rows to {}", results.len(), path.display());
}
