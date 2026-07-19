use std::fs::File;
use std::io::Write;

use blsmr::{
    BLSMRProtocol, KEY_ANNOUNCE_TIMEOUT, KEY_AVG_COMMIT_LATENCY, KEY_COMMIT_LATENCIES,
    KEY_CONFLICT_RATE, KEY_KEY_COUNT, KEY_PROTOCOL_TYPE, KEY_QUORUM_SYSTEM, KEY_SUBMIT_INTERVAL,
    POOL_BLSMR, log, process::BLSMR,
};
use dscale::{BandwidthConfig, Distr, Jiffies, SimulationBuilder, mpi, services::kv};
use itertools::Itertools;

const REPLICAS: usize = 11;
const TIME_BUDGET: Jiffies = Jiffies(200_000);
const BLSMR_UNIFORM_POOL: &str = "blsmr_uniform";

#[derive(Clone, Copy)]
struct Params {
    key_count: usize,
    submit_interval: Jiffies,
    announce_timeout: Jiffies,
    latency: Distr,
}

fn sweep() -> Vec<Params> {
    let key_counts = [1usize, 16, 128, 256, 1024, 65535];
    let submit_intervals = [
        Jiffies(1),
        Jiffies(2),
        Jiffies(5),
        Jiffies(10),
        Jiffies(20),
        Jiffies(50),
        Jiffies(100),
        Jiffies(200),
        Jiffies(500),
    ];
    let announce_timeouts = [
        Jiffies(20),
        Jiffies(50),
        Jiffies(100),
        Jiffies(200),
        Jiffies(500),
    ];
    let latencies = [Distr::Uniform {
        low: Jiffies(20),
        high: Jiffies(20),
    }];

    key_counts
        .into_iter()
        .cartesian_product(submit_intervals)
        .cartesian_product(announce_timeouts)
        .cartesian_product(latencies)
        .map(
            |(((key_count, submit_interval), announce_timeout), latency)| Params {
                key_count,
                submit_interval,
                announce_timeout,
                latency,
            },
        )
        .collect()
}

fn run_once(params: Params) -> (Params, f64, f64) {
    let mut sim = SimulationBuilder::new()
        .add_pool::<BLSMR>(BLSMR_UNIFORM_POOL, REPLICAS)
        .default_bandwidth(BandwidthConfig::Unbounded)
        .within_pool_latency(BLSMR_UNIFORM_POOL, params.latency)
        .time_budget(TIME_BUDGET)
        .seed(42)
        .seq_sched()
        .build();

    kv::set(KEY_PROTOCOL_TYPE, BLSMRProtocol::Wintermute);
    kv::set(KEY_SUBMIT_INTERVAL, params.submit_interval);
    kv::set(KEY_ANNOUNCE_TIMEOUT, params.announce_timeout);
    kv::set(KEY_KEY_COUNT, params.key_count);
    kv::set(
        KEY_QUORUM_SYSTEM,
        quorum::QuorumSystem::new_dissemination(dscale::list_pool(POOL_BLSMR)),
    );
    kv::set::<(usize, usize)>(KEY_AVG_COMMIT_LATENCY, (0, 0));
    kv::set::<Vec<Jiffies>>(KEY_COMMIT_LATENCIES, Vec::new());
    kv::set::<(usize, usize)>(KEY_CONFLICT_RATE, (0, 0));

    sim.run_full_budget();

    (
        params,
        log::conflict_rate_percentage(),
        log::average_commit_latency(),
    )
}

fn main() {
    let results = mpi::distribute(sweep(), run_once);

    let path = format!("blsmr_conflict_latency_rank{}.csv", mpi::rank());
    let mut file = File::create(&path).expect("failed to create results file");
    writeln!(
        file,
        "key_count,submit_interval,announce_timeout,latency_low,latency_high,conflict_rate_pct,avg_latency_jiffies"
    )
    .expect("failed to write header");
    for (params, conflict_rate, latency) in &results {
        let (latency_low, latency_high) = match params.latency {
            Distr::Uniform { low, high } => (low.0, high.0),
            _ => unreachable!("sweep only produces Uniform latency distributions"),
        };
        writeln!(
            file,
            "{},{},{},{},{},{:.4},{:.4}",
            params.key_count,
            params.submit_interval.0,
            params.announce_timeout.0,
            latency_low,
            latency_high,
            conflict_rate,
            latency
        )
        .expect("failed to write row");
    }

    println!("wrote {} rows to {path}", results.len());
}
