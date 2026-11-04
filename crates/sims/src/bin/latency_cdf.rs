use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::Write,
    path::PathBuf,
    sync::{Arc, atomic::AtomicUsize},
};

use blsmr::{
    BLSMRProtocol, KEY_ANNOUNCE_TIMEOUT, KEY_AVG_COMMIT_LATENCY, KEY_COMMIT_LATENCIES,
    KEY_CONFLICT_RATE, KEY_KEY_COUNT, KEY_PROTOCOL_TYPE, KEY_QUORUM_SYSTEM, KEY_SUBMIT_INTERVAL,
    KEY_SUBMIT_LIMIT, KEY_TRACK_CONFLICT_RATE, KEY_ZIPF_EXPONENT, POOL_BLSMR, log, process::BLSMR,
};
use bullshark::{
    Bullshark, KEY_BLOCK_COMMITS as BULLSHARK_BLOCK_COMMITS, KEY_LATENCIES as BULLSHARK_LATENCIES,
    KEY_SUBMIT_INTERVAL as BULLSHARK_SUBMIT_INTERVAL, KEY_SUBMIT_LIMIT as BULLSHARK_SUBMIT_LIMIT,
};
use dscale::{
    BandwidthConfig, Jiffies, LatencyDistribution, Process, SimulationBuilder,
    rand::{SeedableRng, rngs::SmallRng, seq::SliceRandom},
    services::kv,
};
use hotstuff::{
    B0, ChainedHotstuff, KEY_BLOCK_COMMITS as HOTSTUFF_BLOCK_COMMITS,
    KEY_LATENCIES as HOTSTUFF_LATENCIES, KEY_SUBMIT_INTERVAL as HOTSTUFF_SUBMIT_INTERVAL,
    KEY_SUBMIT_LIMIT as HOTSTUFF_SUBMIT_LIMIT, Node, NonRotatingHotstuff,
};

const TIME_BUDGET: Jiffies = Jiffies(2_000_000_000);
const UNIFORM_LATENCY: Jiffies = Jiffies(100_000);
const SEED: u64 = 42;
const ACCOUNT_COUNT: usize = 10_000;
const WORKLOAD_SEED: u64 = 42;
const WINTERMUTE_SUBMIT_INTERVAL: Jiffies = Jiffies(55_000);
const THREE_JANE_FAULTS: usize = 1;
const REGION_COUNT: usize = 8;
const TERRESTRIAL_STD_DEV: usize = 593;

#[derive(Clone, Copy)]
enum NetworkTopology {
    Uniform,
    Terrestrial,
}

impl NetworkTopology {
    fn name(self) -> &'static str {
        match self {
            Self::Uniform => "uniform",
            Self::Terrestrial => "terrestrial",
        }
    }
}

#[derive(Clone)]
struct Region {
    name: String,
}

fn csv_path() -> PathBuf {
    PathBuf::from("crates/sims/src/bin/latency_cdf/latency_cdf.csv")
}

fn path_csv_path() -> PathBuf {
    PathBuf::from("crates/sims/src/bin/latency_cdf/wintermute_paths.csv")
}

fn builder() -> SimulationBuilder {
    SimulationBuilder::new()
        .default_bandwidth(BandwidthConfig::Unbounded)
        .jiffies_per_second(1_000_000)
        .time_budget(TIME_BUDGET)
        .seed(SEED)
        .seq_sched()
}

fn fixed_latency(latency: Jiffies) -> LatencyDistribution {
    LatencyDistribution::Uniform {
        low: latency,
        high: latency,
    }
}

fn uniform_builder<P: Process + Default + Send + 'static>(
    pool: &'static str,
    replicas: usize,
) -> SimulationBuilder {
    builder()
        .add_to_pool::<P>(pool, replicas)
        .within_pool_latency(pool, fixed_latency(UNIFORM_LATENCY))
}

fn terrestrial_regions(replicas: usize) -> Vec<Region> {
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
    let unique: HashSet<_> = regions.iter().map(|region| &region.name).collect();
    assert_eq!(regions.len(), REGION_COUNT, "expected eight AWS regions");
    assert_eq!(unique.len(), REGION_COUNT, "duplicate AWS region");
    regions.shuffle(&mut SmallRng::seed_from_u64(SEED));
    regions.truncate(replicas);
    regions
}

fn terrestrial_builder<P: Process + Default + Send + 'static>(
    regions: &[Region],
    replicas: usize,
) -> SimulationBuilder {
    let means = terrestrial_pair_means();
    let mut simulation = builder();
    for region in regions.iter().cycle().take(replicas) {
        simulation = simulation.add_to_pool::<P>(&region.name, 1);
    }
    for region in regions {
        simulation = simulation.within_pool_latency(&region.name, fixed_latency(Jiffies(1)));
    }
    for (index, from) in regions.iter().enumerate() {
        for to in &regions[index + 1..] {
            simulation = simulation.between_pool_latency(
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
    simulation
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
        high: Jiffies(((mean_ms + 6.0 * 0.593) * 1_000.0).ceil() as usize),
    }
}

fn configure_replay(trace: &Arc<client::ReplayTrace>) -> Arc<client::ReplayMetrics> {
    let metrics = Arc::new(client::ReplayMetrics::new(trace.replica_count));
    kv::set(client::KEY_REPLAY_TRACE, Some(Arc::clone(trace)));
    kv::set(client::KEY_REPLAY_METRICS, Arc::clone(&metrics));
    metrics
}

fn report_replay(
    topology: NetworkTopology,
    protocol: &str,
    trace: &client::ReplayTrace,
    metrics: &client::ReplayMetrics,
    samples: usize,
) {
    let submitted = metrics.snapshot();
    let total = submitted.iter().sum::<usize>();
    assert!(samples <= total, "more samples than submitted commands");
    let mut submitted_by_replica = vec![0; trace.replica_count];
    for (client, count) in submitted.iter().enumerate() {
        submitted_by_replica[client % trace.replica_count] += count;
    }
    println!(
        "{} {protocol}: offered={}, submitted={total}, sampled={samples}, unfinished={}, submitted_by_replica={submitted_by_replica:?}, submitted_by_client={submitted:?}",
        topology.name(),
        trace.records.len(),
        total - samples
    );
    assert_eq!(total, trace.records.len(), "incomplete replay submission");
}

fn run_hotstuff<P: Process + Default + Send + 'static>(
    topology: NetworkTopology,
    uniform_pool: &'static str,
    protocol: &str,
    trace: &Arc<client::ReplayTrace>,
) -> Vec<Jiffies> {
    let regions = terrestrial_regions(4);
    let mut simulation = match topology {
        NetworkTopology::Uniform => uniform_builder::<P>(uniform_pool, 4),
        NetworkTopology::Terrestrial => terrestrial_builder::<P>(&regions, 4),
    }
    .build();
    let metrics = configure_replay(trace);
    kv::set(B0, Arc::new(Node::genesis()));
    kv::set(HOTSTUFF_SUBMIT_INTERVAL, WINTERMUTE_SUBMIT_INTERVAL);
    kv::set(HOTSTUFF_SUBMIT_LIMIT, usize::MAX);
    kv::set(HOTSTUFF_BLOCK_COMMITS, Arc::new(AtomicUsize::new(0)));
    kv::set::<Vec<Jiffies>>(HOTSTUFF_LATENCIES, Vec::new());
    simulation.run_full_budget();
    let samples: Vec<Jiffies> = kv::get(HOTSTUFF_LATENCIES);
    report_replay(topology, protocol, trace, &metrics, samples.len());
    samples
}

fn run_bullshark(topology: NetworkTopology, trace: &Arc<client::ReplayTrace>) -> Vec<Jiffies> {
    let regions = terrestrial_regions(4);
    let mut simulation = match topology {
        NetworkTopology::Uniform => uniform_builder::<Bullshark>("bullshark_uniform", 4),
        NetworkTopology::Terrestrial => terrestrial_builder::<Bullshark>(&regions, 4),
    }
    .build();
    let metrics = configure_replay(trace);
    kv::set(bullshark::KEY_ROUND_TIMEOUT, Jiffies(10_000_000));
    kv::set(BULLSHARK_SUBMIT_INTERVAL, WINTERMUTE_SUBMIT_INTERVAL);
    kv::set(BULLSHARK_SUBMIT_LIMIT, usize::MAX);
    kv::set(BULLSHARK_BLOCK_COMMITS, Arc::new(AtomicUsize::new(0)));
    kv::set::<Vec<Jiffies>>(BULLSHARK_LATENCIES, Vec::new());
    simulation.run_full_budget();
    let samples: Vec<Jiffies> = kv::get(BULLSHARK_LATENCIES);
    report_replay(topology, "Bullshark", trace, &metrics, samples.len());
    samples
}

fn run_blsmr(
    topology: NetworkTopology,
    protocol: BLSMRProtocol,
    replicas: usize,
    uniform_pool: &'static str,
    protocol_name: &str,
    trace: &Arc<client::ReplayTrace>,
) -> (Vec<Jiffies>, f64, f64, Vec<log::PathRecord>) {
    let regions = terrestrial_regions(replicas);
    let mut simulation = match topology {
        NetworkTopology::Uniform => uniform_builder::<BLSMR>(uniform_pool, replicas),
        NetworkTopology::Terrestrial => terrestrial_builder::<BLSMR>(&regions, replicas),
    }
    .build();
    let metrics = configure_replay(trace);
    let quorum_system = match &protocol {
        BLSMRProtocol::ThreeJane => quorum::QuorumSystem::new_witnessing_grid_with_faults(
            dscale::list_pool(POOL_BLSMR),
            THREE_JANE_FAULTS,
        ),
        _ => quorum::QuorumSystem::new_dissemination(dscale::list_pool(POOL_BLSMR)),
    };
    kv::set(KEY_PROTOCOL_TYPE, protocol);
    kv::set(KEY_SUBMIT_INTERVAL, WINTERMUTE_SUBMIT_INTERVAL);
    kv::set(KEY_SUBMIT_LIMIT, usize::MAX);
    kv::set(KEY_TRACK_CONFLICT_RATE, true);
    kv::set(KEY_ANNOUNCE_TIMEOUT, Jiffies(500_000));
    kv::set(KEY_KEY_COUNT, 512usize);
    kv::set::<Option<f64>>(KEY_ZIPF_EXPONENT, None);
    kv::set(KEY_QUORUM_SYSTEM, quorum_system);
    kv::set::<(usize, usize)>(KEY_AVG_COMMIT_LATENCY, (0, 0));
    kv::set::<Vec<Jiffies>>(KEY_COMMIT_LATENCIES, Vec::new());
    kv::set(
        KEY_CONFLICT_RATE,
        log::ConflictTracker::new(dscale::list_pool(POOL_BLSMR).len()),
    );
    simulation.run_full_budget();
    let samples: Vec<Jiffies> = kv::get(KEY_COMMIT_LATENCIES);
    report_replay(topology, protocol_name, trace, &metrics, samples.len());
    (
        samples,
        log::conflict_rate_percentage(),
        log::fast_path_percentage(),
        log::path_records(),
    )
}

fn write_samples(
    file: &mut File,
    topology: NetworkTopology,
    protocol: &str,
    samples: Vec<Jiffies>,
) {
    for latency in samples {
        writeln!(
            file,
            "{},{protocol},{:.3}",
            topology.name(),
            latency.0 as f64 / 1_000.0
        )
        .expect("failed to write latency sample");
    }
}

fn write_path_records(file: &mut File, topology: NetworkTopology, records: Vec<log::PathRecord>) {
    for record in records {
        let decided = record.deps.is_some();
        match record.deps.as_deref() {
            Some(deps) if !deps.is_empty() => {
                for dep in deps {
                    writeln!(
                        file,
                        "{},{},{},{},{},{},{}",
                        topology.name(),
                        record.cmd_id.pid,
                        record.cmd_id.id,
                        record.fast,
                        decided,
                        dep.pid,
                        dep.id
                    )
                    .expect("failed to write path record");
                }
            }
            _ => {
                writeln!(
                    file,
                    "{},{},{},{},{decided},,",
                    topology.name(),
                    record.cmd_id.pid,
                    record.cmd_id.id,
                    record.fast
                )
                .expect("failed to write path record");
            }
        }
    }
}

fn main() {
    let traces: HashMap<usize, Arc<client::ReplayTrace>> = [4, 6, 25]
        .into_iter()
        .map(|replicas| {
            (
                replicas,
                Arc::new(client::generate_visa_trace(
                    ACCOUNT_COUNT,
                    WORKLOAD_SEED,
                    replicas,
                )),
            )
        })
        .collect();
    let path = csv_path();
    let path_records_path = path_csv_path();
    std::fs::create_dir_all(path.parent().unwrap()).expect("failed to create results directory");
    let mut file = File::create(&path).expect("failed to create results file");
    let mut path_file =
        File::create(&path_records_path).expect("failed to create path results file");
    writeln!(file, "topology,protocol,latency_jiffies").expect("failed to write header");
    writeln!(
        path_file,
        "topology,command_pid,command_id,direct_fast,decided,dependency_pid,dependency_id"
    )
    .expect("failed to write path header");
    for topology in [NetworkTopology::Uniform, NetworkTopology::Terrestrial] {
        write_samples(
            &mut file,
            topology,
            "HotStuff",
            run_hotstuff::<ChainedHotstuff>(topology, "hotstuff_uniform", "HotStuff", &traces[&4]),
        );
        write_samples(
            &mut file,
            topology,
            "HotStuff*",
            run_hotstuff::<NonRotatingHotstuff>(
                topology,
                "hotstuff_star_uniform",
                "HotStuff*",
                &traces[&4],
            ),
        );
        write_samples(
            &mut file,
            topology,
            "Bullshark",
            run_bullshark(topology, &traces[&4]),
        );
        let (three_jane, _, _, _) = run_blsmr(
            topology,
            BLSMRProtocol::ThreeJane,
            25,
            "three_jane_uniform",
            "3Jane",
            &traces[&25],
        );
        write_samples(&mut file, topology, "3Jane", three_jane);
        let (wintermute, conflict_rate, fast_path_rate, path_records) = run_blsmr(
            topology,
            BLSMRProtocol::Wintermute,
            6,
            "wintermute_uniform",
            "Wintermute",
            &traces[&6],
        );
        write_samples(&mut file, topology, "Wintermute", wintermute);
        write_path_records(&mut path_file, topology, path_records);
        println!(
            "{} Wintermute conflict rate: {conflict_rate:.2}%, fast path rate: {fast_path_rate:.2}%",
            topology.name()
        );
    }
    println!("wrote {}", path.display());
    println!("wrote {}", path_records_path.display());
}
