use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::Write,
    path::PathBuf,
};

use blsmr::{
    BLSMRProtocol, KEY_ANNOUNCE_TIMEOUT, KEY_AVG_COMMIT_LATENCY, KEY_COMMIT_LATENCIES,
    KEY_CONFLICT_RATE, KEY_KEY_COUNT, KEY_PROTOCOL_TYPE, KEY_QUORUM_SYSTEM, KEY_SUBMIT_INTERVAL,
    KEY_SUBMIT_LIMIT, KEY_TRACK_CONFLICT_RATE, KEY_ZIPF_EXPONENT, POOL_BLSMR, log, process::BLSMR,
};
use dscale::{
    BandwidthConfig, Jiffies, LatencyDistribution, SimulationBuilder, mpi,
    rand::{SeedableRng, rngs::SmallRng, seq::SliceRandom},
    services::kv,
};

const REPLICAS: usize = 6;
const TIME_BUDGET: Jiffies = Jiffies(2_000_000_000);
const KEY_COUNT: usize = 32;
const COMMANDS_PER_REPLICA: usize = 20;
const ANNOUNCE_TIMEOUT: Jiffies = Jiffies(500_000);
const SEEDS: [u64; 5] = [42, 43, 44, 45, 46];
const REGION_COUNT: usize = 8;
const TERRESTRIAL_STD_DEV: usize = 593;

#[derive(Clone, Copy)]
struct Params {
    submit_interval: Jiffies,
    seed: u64,
}

struct Region {
    name: String,
}

fn sweep() -> Vec<Params> {
    [1, 19, 24, 31, 37, 46, 59, 78, 114, 200, 20_000]
        .into_iter()
        .flat_map(|submit_interval| {
            SEEDS.into_iter().map(move |seed| Params {
                submit_interval: Jiffies(submit_interval * 1_000),
                seed,
            })
        })
        .collect()
}

fn regions(seed: u64) -> Vec<Region> {
    let mut regions = include_str!("latency_cdf/aws_regions.csv")
        .lines()
        .skip(1)
        .map(|line| Region {
            name: line
                .split(',')
                .next()
                .expect("missing AWS Region ID")
                .to_owned(),
        })
        .collect::<Vec<_>>();
    regions.shuffle(&mut SmallRng::seed_from_u64(seed));
    regions.truncate(REPLICAS);
    regions
}

fn fixed_latency(latency: Jiffies) -> LatencyDistribution {
    LatencyDistribution::Uniform {
        low: latency,
        high: latency,
    }
}

fn terrestrial_pair_means() -> HashMap<(&'static str, &'static str), f64> {
    let regions: HashSet<_> = include_str!("latency_cdf/aws_regions.csv")
        .lines()
        .skip(1)
        .map(|line| line.split(',').next().expect("missing AWS Region ID"))
        .collect();
    assert_eq!(
        regions.len(),
        REGION_COUNT,
        "expected eight unique AWS regions"
    );
    let mut means = HashMap::new();
    for line in include_str!("latency_cdf/aws_pair_means.csv")
        .lines()
        .skip(1)
    {
        let mut fields = line.split(',');
        let from = fields.next().expect("missing source region");
        let to = fields.next().expect("missing destination region");
        let mean: f64 = fields
            .next()
            .expect("missing pair mean")
            .parse()
            .expect("invalid pair mean");
        assert!(
            from != to && regions.contains(from) && regions.contains(to),
            "invalid AWS region pair"
        );
        assert!(mean.is_finite() && mean > 0.0, "invalid pair mean");
        assert!(
            means.insert((from, to), mean).is_none(),
            "duplicate AWS region pair"
        );
        assert!(
            means.insert((to, from), mean).is_none(),
            "duplicate AWS region pair"
        );
    }
    assert_eq!(
        means.len(),
        REGION_COUNT * (REGION_COUNT - 1),
        "incomplete AWS region pairs"
    );
    means
}

fn normal_latency(mean_ms: f64) -> LatencyDistribution {
    LatencyDistribution::Normal {
        mean: Jiffies((mean_ms * 1_000.0).round() as usize),
        std_dev: Jiffies(TERRESTRIAL_STD_DEV),
        low: Jiffies(1),
        high: Jiffies((mean_ms * 1_000.0 + 6.0 * TERRESTRIAL_STD_DEV as f64).ceil() as usize),
    }
}

fn simulation(seed: u64) -> dscale::Runner {
    let regions = regions(seed);
    let means = terrestrial_pair_means();
    let mut builder = SimulationBuilder::new()
        .default_bandwidth(BandwidthConfig::Unbounded)
        .jiffies_per_second(1_000_000)
        .time_budget(TIME_BUDGET)
        .seed(seed)
        .par_sched(dscale::ThreadNumber::MatchCores);
    for region in &regions {
        builder = builder.add_to_pool::<BLSMR>(&region.name, 1);
    }
    for region in &regions {
        builder = builder.within_pool_latency(&region.name, fixed_latency(Jiffies(0)));
    }
    for (index, from) in regions.iter().enumerate() {
        for to in &regions[index + 1..] {
            builder = builder.between_pool_latency(
                &from.name,
                &to.name,
                normal_latency(
                    *means
                        .get(&(from.name.as_str(), to.name.as_str()))
                        .expect("missing AWS region pair"),
                ),
            );
        }
    }
    builder.build()
}

fn run_once(params: Params) -> (Params, f64, f64, f64) {
    let mut simulation = simulation(params.seed);
    kv::set::<Option<std::sync::Arc<client::ReplayTrace>>>(client::KEY_REPLAY_TRACE, None);
    kv::set(KEY_PROTOCOL_TYPE, BLSMRProtocol::Wintermute);
    kv::set(KEY_SUBMIT_INTERVAL, params.submit_interval);
    kv::set(KEY_SUBMIT_LIMIT, COMMANDS_PER_REPLICA);
    kv::set(KEY_TRACK_CONFLICT_RATE, true);
    kv::set(KEY_ANNOUNCE_TIMEOUT, ANNOUNCE_TIMEOUT);
    kv::set(KEY_KEY_COUNT, KEY_COUNT);
    kv::set::<Option<f64>>(KEY_ZIPF_EXPONENT, None);
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
        log::average_commit_latency() / 1_000.0,
        log::fast_path_percentage(),
    )
}

fn output_path() -> PathBuf {
    PathBuf::from(format!(
        "crates/sims/src/bin/conflict_rate/terrestrial_conflict_latency_rank{}.csv",
        mpi::rank()
    ))
}

fn main() {
    let results = mpi::distribute(sweep(), run_once);
    let path = output_path();
    std::fs::create_dir_all(path.parent().unwrap()).expect("failed to create results directory");
    let mut file = File::create(&path).expect("failed to create results file");
    writeln!(
        file,
        "key_count,commands_per_replica,submit_interval,announce_timeout,seed,conflict_rate_pct,avg_latency_jiffies,fast_path_rate_pct"
    )
    .expect("failed to write header");
    for (params, conflict_rate, latency, fast_path_rate) in &results {
        writeln!(
            file,
            "{KEY_COUNT},{COMMANDS_PER_REPLICA},{},{},{},{conflict_rate:.4},{latency:.4},{fast_path_rate:.4}",
            params.submit_interval.0 / 1_000, ANNOUNCE_TIMEOUT.0 / 1_000, params.seed
        )
        .expect("failed to write row");
    }
    println!("wrote {} rows to {}", results.len(), path.display());
}
