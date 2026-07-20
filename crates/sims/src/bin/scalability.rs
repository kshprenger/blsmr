use std::{
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
    KEY_CONFLICT_RATE, KEY_KEY_COUNT, KEY_PROTOCOL_TYPE, KEY_QUORUM_SYSTEM, KEY_SUBMIT_INTERVAL,
    POOL_BLSMR, process::BLSMR,
};
use bullshark::{Bullshark, KEY_LATENCIES as BULLSHARK_LATENCIES};
use dscale::{
    BandwidthConfig, Distr, Jiffies, MessagePtr, Pid, Process, SimulationBuilder, TimerId, mpi,
    services::kv,
};
use hotstuff::{B0, ChainedHotstuff, KEY_LATENCIES as HOTSTUFF_LATENCIES, Node};

const NODE_COUNTS: [usize; 11] = [2, 4, 8, 16, 32, 64, 128, 256, 512, 1_024, 2_048];
const THREE_JANE_NODE_COUNTS: [usize; 10] = [4, 9, 16, 36, 64, 121, 256, 529, 1_024, 2_025];
const TIME_BUDGET: Jiffies = Jiffies(1_000);
const NETWORK_LATENCY: Jiffies = Jiffies(100);
const SUBMIT_INTERVAL: Jiffies = Jiffies(1_000);
static MESSAGE_COUNT: AtomicUsize = AtomicUsize::new(0);

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
        MESSAGE_COUNT.fetch_add(1, Ordering::Relaxed);
        self.0.on_message(from, message);
    }

    fn on_timer(&mut self, id: TimerId) {
        self.0.on_timer(id);
    }
}

#[derive(Clone, Copy)]
enum Protocol {
    Bullshark,
    ThreeJane,
    Wintermute,
    Hotstuff,
}

impl Protocol {
    fn name(self) -> &'static str {
        match self {
            Self::Bullshark => "Bullshark",
            Self::ThreeJane => "3Jane",
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
    NODE_COUNTS
        .into_iter()
        .flat_map(|nodes| {
            [
                Protocol::Bullshark,
                Protocol::Wintermute,
                Protocol::Hotstuff,
            ]
            .into_iter()
            .map(move |protocol| Config { nodes, protocol })
        })
        .chain(THREE_JANE_NODE_COUNTS.into_iter().map(|nodes| Config {
            nodes,
            protocol: Protocol::ThreeJane,
        }))
        .collect()
}

fn simulation<P: Process + Default + Send + 'static>(
    nodes: usize,
) -> Box<dyn dscale::SimulationRunner> {
    SimulationBuilder::new()
        .add_pool::<Measured<P>>("scalability", nodes)
        .default_bandwidth(BandwidthConfig::Unbounded)
        .within_pool_latency(
            "scalability",
            Distr::Uniform {
                low: NETWORK_LATENCY,
                high: NETWORK_LATENCY,
            },
        )
        .time_budget(TIME_BUDGET)
        .seed(42)
        .seq_sched()
        .build()
}

fn measure(mut simulation: Box<dyn dscale::SimulationRunner>, nodes: usize) -> f64 {
    MESSAGE_COUNT.store(0, Ordering::Relaxed);
    simulation.run_full_budget();
    MESSAGE_COUNT.load(Ordering::Relaxed) as f64 / (TIME_BUDGET.0 * nodes) as f64
}

fn run_hotstuff(nodes: usize) -> f64 {
    let simulation = simulation::<ChainedHotstuff>(nodes);
    kv::set(
        B0,
        Arc::new(Node {
            id: 0,
            parent: None,
            height: 0,
        }),
    );
    kv::set::<Vec<Jiffies>>(HOTSTUFF_LATENCIES, Vec::new());
    measure(simulation, nodes)
}

fn run_bullshark(nodes: usize) -> f64 {
    let simulation = simulation::<Bullshark>(nodes);
    kv::set::<Vec<Jiffies>>(BULLSHARK_LATENCIES, Vec::new());
    measure(simulation, nodes)
}

fn run_blsmr(nodes: usize, protocol: BLSMRProtocol) -> f64 {
    let simulation = simulation::<BLSMR>(nodes);
    let pids = dscale::list_pool(POOL_BLSMR);
    let quorum_system = match &protocol {
        BLSMRProtocol::ThreeJane => quorum::QuorumSystem::new_witnessing_grid(pids),
        _ => quorum::QuorumSystem::new_dissemination(pids),
    };
    kv::set(KEY_PROTOCOL_TYPE, protocol);
    kv::set(KEY_SUBMIT_INTERVAL, SUBMIT_INTERVAL);
    kv::set(KEY_ANNOUNCE_TIMEOUT, Jiffies(500));
    kv::set(KEY_KEY_COUNT, 32usize);
    kv::set(KEY_QUORUM_SYSTEM, quorum_system);
    kv::set::<(usize, usize)>(KEY_AVG_COMMIT_LATENCY, (0, 0));
    kv::set::<Vec<Jiffies>>(KEY_COMMIT_LATENCIES, Vec::new());
    kv::set::<(usize, usize)>(KEY_CONFLICT_RATE, (0, 0));
    measure(simulation, nodes)
}

fn run(config: Config) -> (Config, f64) {
    let load = match config.protocol {
        Protocol::Bullshark => run_bullshark(config.nodes),
        Protocol::ThreeJane => run_blsmr(config.nodes, BLSMRProtocol::ThreeJane),
        Protocol::Wintermute => run_blsmr(config.nodes, BLSMRProtocol::Wintermute),
        Protocol::Hotstuff => run_hotstuff(config.nodes),
    };
    (config, load)
}

fn output_path() -> PathBuf {
    PathBuf::from(format!("scalability_rank{}.csv", mpi::rank()))
}

fn main() {
    let results = mpi::distribute(configs(), run);
    let path = output_path();
    let mut file = File::create(&path).expect("failed to create results file");
    writeln!(
        file,
        "protocol,nodes,avg_on_message_calls_per_replica_per_jiffy"
    )
    .expect("failed to write header");
    for (config, load) in results {
        writeln!(
            file,
            "{},{},{load:.6}",
            config.protocol.name(),
            config.nodes
        )
        .expect("failed to write row");
    }
    println!("wrote {}", path.display());
}
