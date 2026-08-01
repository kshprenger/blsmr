use std::{
    env,
    fs::File,
    io::Write,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use blsmr::{
    BLSMRProtocol, KEY_ANNOUNCE_TIMEOUT, KEY_AVG_COMMIT_LATENCY, KEY_COMMIT_LATENCIES,
    KEY_KEY_COUNT, KEY_PROTOCOL_TYPE, KEY_QUORUM_SYSTEM, KEY_SUBMIT_INTERVAL, KEY_SUBMIT_LIMIT,
    KEY_TRACK_CONFLICT_RATE, KEY_ZIPF_EXPONENT, POOL_BLSMR, process::BLSMR,
};
use bullshark::{
    Bullshark, KEY_BLOCK_COMMITS as BULLSHARK_BLOCK_COMMITS, KEY_LATENCIES as BULLSHARK_LATENCIES,
    KEY_SUBMIT_INTERVAL as BULLSHARK_SUBMIT_INTERVAL, KEY_SUBMIT_LIMIT as BULLSHARK_SUBMIT_LIMIT,
};
use dscale::{
    BandwidthConfig, Distr, Jiffies, MessagePtr, Pid, Process, SimulationBuilder, TimerId, mpi,
    rand::{SeedableRng, rngs::SmallRng, seq::SliceRandom},
    services::kv,
};
use hotstuff::{
    B0, ChainedHotstuff, KEY_BLOCK_COMMITS as HOTSTUFF_BLOCK_COMMITS,
    KEY_LATENCIES as HOTSTUFF_LATENCIES, KEY_SUBMIT_INTERVAL as HOTSTUFF_SUBMIT_INTERVAL,
    KEY_SUBMIT_LIMIT as HOTSTUFF_SUBMIT_LIMIT, Node,
};

const BASELINE_NODE_COUNTS: [usize; 9] = [4, 8, 16, 32, 64, 128, 256, 512, 1_024];
const WINTERMUTE_NODE_COUNTS: [usize; 9] = [6, 8, 16, 32, 64, 128, 256, 512, 1_024];
const THREE_JANE_NODE_COUNTS: [usize; 7] = [25, 36, 64, 121, 256, 529, 1_024];
const BLSMR_TIME_BUDGET: Jiffies = Jiffies(40_000);
const HOTSTUFF_TIME_BUDGET: Jiffies = Jiffies(5_000_000);
const BULLSHARK_TIME_BUDGET: Jiffies = Jiffies(100_000);
const SUBMIT_INTERVAL: Jiffies = Jiffies(500);
const THREE_JANE_FAULTS: usize = 1;
const THREE_JANE_ZIPF_EXPONENT: f64 = 0.99;
const SEED: u64 = 42;
const EARTH_RADIUS_KM: f64 = 6_371.0;
const LIGHT_SPEED_KM_PER_SECOND: f64 = 299_792.458;
const MAX_NODES: usize = 2_048;
static MESSAGE_COUNTS: [AtomicUsize; MAX_NODES] = [const { AtomicUsize::new(0) }; MAX_NODES];

struct Region {
    name: String,
    latitude: f64,
    longitude: f64,
}

struct Measured<P>(P);

impl<P: Default> Default for Measured<P> {
    fn default() -> Self {
        Self(P::default())
    }
}

impl<P: Process> Process for Measured<P> {
    fn on_start(&mut self) {
        self.0.on_start();
    }

    fn on_message(&mut self, from: Pid, message: MessagePtr) {
        if !hotstuff::is_command_submission(&message) && !bullshark::is_command_submission(&message)
        {
            MESSAGE_COUNTS[dscale::pid()].fetch_add(1, Ordering::Relaxed);
        }
        self.0.on_message(from, message);
    }

    fn on_timer(&mut self, id: TimerId) {
        self.0.on_timer(id);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Protocol {
    Bullshark,
    ThreeJane,
    ThreeJaneMaxFaults,
    Wintermute,
    Hotstuff,
}

impl Protocol {
    fn name(self) -> &'static str {
        match self {
            Self::Bullshark => "Bullshark",
            Self::ThreeJane => "3Jane",
            Self::ThreeJaneMaxFaults => "3Jane*",
            Self::Wintermute => "Wintermute",
            Self::Hotstuff => "HotStuff",
        }
    }
}

#[derive(Clone, Copy)]
struct Config {
    nodes: usize,
    protocol: Protocol,
}

fn configs() -> Vec<Config> {
    BASELINE_NODE_COUNTS
        .into_iter()
        .flat_map(|nodes| {
            [Protocol::Bullshark, Protocol::Hotstuff]
                .into_iter()
                .map(move |protocol| Config { nodes, protocol })
        })
        .chain(WINTERMUTE_NODE_COUNTS.into_iter().map(|nodes| Config {
            nodes,
            protocol: Protocol::Wintermute,
        }))
        .chain(THREE_JANE_NODE_COUNTS.into_iter().flat_map(|nodes| {
            [Protocol::ThreeJane, Protocol::ThreeJaneMaxFaults]
                .into_iter()
                .map(move |protocol| Config { nodes, protocol })
        }))
        .collect()
}

fn selected_configs() -> Vec<Config> {
    let nodes = env::var("SCALABILITY_NODES")
        .ok()
        .map(|nodes| nodes.parse::<usize>().expect("invalid SCALABILITY_NODES"));
    configs()
        .into_iter()
        .filter(|config| nodes.is_none_or(|nodes| config.nodes == nodes))
        .collect()
}

fn simulation<P: Process + Default + Send + 'static>(
    nodes: usize,
    time_budget: Jiffies,
) -> Box<dyn dscale::SimulationRunner> {
    let regions = terrestrial_regions(nodes);
    let mut builder = SimulationBuilder::new()
        .default_bandwidth(BandwidthConfig::Unbounded)
        .time_budget(time_budget)
        .seed(SEED)
        .seq_sched();
    for region in (0..nodes).map(|index| &regions[index % regions.len()]) {
        builder = builder
            .add_pool::<Measured<P>>(&region.name, 1)
            .within_pool_latency(&region.name, fixed_latency(Jiffies(1)));
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

fn terrestrial_regions(nodes: usize) -> Vec<Region> {
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
    regions.shuffle(&mut SmallRng::seed_from_u64(SEED));
    regions.truncate(nodes);
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

fn measure(
    mut simulation: Box<dyn dscale::SimulationRunner>,
    nodes: usize,
    metrics: impl FnOnce() -> (usize, f64, f64),
) -> (f64, f64, f64, f64) {
    for count in &MESSAGE_COUNTS[..nodes] {
        count.store(0, Ordering::Relaxed);
    }
    simulation.run_full_budget();
    let (committed, average_commit_latency, commit_latency_standard_deviation) = metrics();
    let (load, standard_deviation) = load_stats(
        &MESSAGE_COUNTS[..nodes]
            .iter()
            .map(|count| count.load(Ordering::Relaxed))
            .collect::<Vec<_>>(),
        committed,
    );
    (
        load,
        standard_deviation,
        average_commit_latency,
        commit_latency_standard_deviation,
    )
}

fn load_stats(calls: &[usize], committed: usize) -> (f64, f64) {
    if committed == 0 || calls.is_empty() {
        return (0.0, 0.0);
    }
    let mean = calls.iter().sum::<usize>() as f64 / committed as f64;
    let scale = calls.len() as f64 / committed as f64;
    let worst = calls
        .iter()
        .map(|calls| *calls as f64 * scale)
        .fold(0.0, f64::max);
    let variance = calls
        .iter()
        .map(|calls| (*calls as f64 * scale - mean).powi(2))
        .sum::<f64>()
        / calls.len() as f64;
    (worst, variance.sqrt())
}

fn latency_stats(latencies: &[Jiffies]) -> (f64, f64) {
    if latencies.is_empty() {
        (0.0, 0.0)
    } else {
        let mean = latencies.iter().map(|latency| latency.0).sum::<usize>() as f64
            / latencies.len() as f64;
        let variance = latencies
            .iter()
            .map(|latency| (latency.0 as f64 - mean).powi(2))
            .sum::<f64>()
            / latencies.len() as f64;
        (mean, variance.sqrt())
    }
}

fn run_hotstuff(nodes: usize) -> (f64, f64, f64, f64) {
    let simulation = simulation::<ChainedHotstuff>(nodes, HOTSTUFF_TIME_BUDGET);
    let block_commits = Arc::new(AtomicUsize::new(0));
    kv::set(B0, Arc::new(Node::genesis()));
    kv::set(HOTSTUFF_SUBMIT_INTERVAL, SUBMIT_INTERVAL);
    kv::set(HOTSTUFF_SUBMIT_LIMIT, usize::MAX);
    kv::set(HOTSTUFF_BLOCK_COMMITS, block_commits.clone());
    kv::set::<Vec<Jiffies>>(HOTSTUFF_LATENCIES, Vec::new());
    measure(simulation, nodes, || {
        let latencies = kv::get::<Vec<Jiffies>>(HOTSTUFF_LATENCIES);
        let (average, standard_deviation) = latency_stats(&latencies);
        (
            block_commits.load(Ordering::Relaxed),
            average,
            standard_deviation,
        )
    })
}

fn run_bullshark(nodes: usize) -> (f64, f64, f64, f64) {
    let simulation = simulation::<Bullshark>(nodes, BULLSHARK_TIME_BUDGET);
    let block_commits = Arc::new(AtomicUsize::new(0));
    kv::set(BULLSHARK_SUBMIT_INTERVAL, SUBMIT_INTERVAL);
    kv::set(BULLSHARK_SUBMIT_LIMIT, usize::MAX);
    kv::set(BULLSHARK_BLOCK_COMMITS, block_commits.clone());
    kv::set::<Vec<Jiffies>>(BULLSHARK_LATENCIES, Vec::new());
    measure(simulation, nodes, || {
        let latencies = kv::get::<Vec<Jiffies>>(BULLSHARK_LATENCIES);
        let (average, standard_deviation) = latency_stats(&latencies);
        (
            block_commits.load(Ordering::Relaxed),
            average,
            standard_deviation,
        )
    })
}

fn run_blsmr(
    nodes: usize,
    protocol: BLSMRProtocol,
    max_three_jane_faults: bool,
) -> (f64, f64, f64, f64) {
    let simulation = simulation::<BLSMR>(nodes, BLSMR_TIME_BUDGET);
    let pids = dscale::list_pool(POOL_BLSMR);
    let zipf_exponent =
        matches!(&protocol, BLSMRProtocol::ThreeJane).then_some(THREE_JANE_ZIPF_EXPONENT);
    let quorum_system = match &protocol {
        BLSMRProtocol::ThreeJane if max_three_jane_faults => {
            quorum::QuorumSystem::new_witnessing_grid(pids)
        }
        BLSMRProtocol::ThreeJane => {
            quorum::QuorumSystem::new_witnessing_grid_with_faults(pids, THREE_JANE_FAULTS)
        }
        _ => quorum::QuorumSystem::new_dissemination(pids),
    };
    kv::set(KEY_PROTOCOL_TYPE, protocol);
    kv::set(KEY_SUBMIT_INTERVAL, SUBMIT_INTERVAL);
    kv::set(KEY_SUBMIT_LIMIT, usize::MAX);
    kv::set(KEY_TRACK_CONFLICT_RATE, false);
    kv::set(KEY_ANNOUNCE_TIMEOUT, Jiffies(500));
    kv::set(KEY_KEY_COUNT, 10_000_000usize);
    kv::set(KEY_ZIPF_EXPONENT, zipf_exponent);
    kv::set(KEY_QUORUM_SYSTEM, quorum_system);
    kv::set::<(usize, usize)>(KEY_AVG_COMMIT_LATENCY, (0, 0));
    kv::set::<Vec<Jiffies>>(KEY_COMMIT_LATENCIES, Vec::new());
    measure(simulation, nodes, || {
        let latencies = kv::get::<Vec<Jiffies>>(KEY_COMMIT_LATENCIES);
        let (average, standard_deviation) = latency_stats(&latencies);
        (
            kv::get::<(usize, usize)>(KEY_AVG_COMMIT_LATENCY).1,
            average,
            standard_deviation,
        )
    })
}

fn run(config: Config) -> (Config, f64, f64, f64, f64) {
    let (load, standard_deviation, average_commit_latency, commit_latency_standard_deviation) =
        match config.protocol {
            Protocol::Bullshark => run_bullshark(config.nodes),
            Protocol::ThreeJane => run_blsmr(config.nodes, BLSMRProtocol::ThreeJane, false),
            Protocol::ThreeJaneMaxFaults => run_blsmr(config.nodes, BLSMRProtocol::ThreeJane, true),
            Protocol::Wintermute => run_blsmr(config.nodes, BLSMRProtocol::Wintermute, false),
            Protocol::Hotstuff => run_hotstuff(config.nodes),
        };
    (
        config,
        load,
        standard_deviation,
        average_commit_latency,
        commit_latency_standard_deviation,
    )
}

fn output_path() -> PathBuf {
    match env::var("SCALABILITY_NODES") {
        Ok(nodes) => PathBuf::from(format!("scalability_n{nodes}_rank{}.csv", mpi::rank())),
        Err(_) => PathBuf::from(format!("scalability_rank{}.csv", mpi::rank())),
    }
}

fn main() {
    let results = mpi::distribute(selected_configs(), run);
    let path = output_path();
    let mut file = File::create(&path).expect("failed to create results file");
    writeln!(
        file,
        "protocol,nodes,on_message_calls_per_committed_unit,standard_deviation,average_commit_latency_jiffies,commit_latency_standard_deviation_jiffies"
    )
    .expect("failed to write header");
    for (
        config,
        load,
        standard_deviation,
        average_commit_latency,
        commit_latency_standard_deviation,
    ) in results
    {
        writeln!(
            file,
            "{},{},{load:.6},{standard_deviation:.6},{average_commit_latency:.6},{commit_latency_standard_deviation:.6}",
            config.protocol.name(),
            config.nodes
        )
        .expect("failed to write row");
    }
    println!("wrote {}", path.display());
}
