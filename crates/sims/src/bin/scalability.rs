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

const NODE_COUNTS: [usize; 14] = [
    2, 4, 8, 16, 32, 64, 128, 256, 512, 1_024, 2_048, 4_096, 8_192, 16_384,
];
const THREE_JANE_NODE_COUNTS: [usize; 13] = [
    4, 9, 16, 36, 64, 121, 256, 529, 1_024, 2_025, 4_096, 8_281, 16_384,
];
const TIME_BUDGET: Jiffies = Jiffies(500_000);
const HOTSTUFF_TIME_BUDGET: Jiffies = Jiffies(5_000_000);
const BULLSHARK_TIME_BUDGET: Jiffies = Jiffies(100_000);
const WINTERMUTE_TIME_BUDGET: Jiffies = Jiffies(50_000);
const NETWORK_LATENCY: Jiffies = Jiffies(100);
const SUBMIT_INTERVAL: Jiffies = Jiffies(2_000);
const THREE_JANE_FAULTS: usize = 1;
const MAX_NODES: usize = 16_384;
static MESSAGE_COUNTS: [AtomicUsize; MAX_NODES] = [const { AtomicUsize::new(0) }; MAX_NODES];

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
        MESSAGE_COUNTS[dscale::pid()].fetch_add(1, Ordering::Relaxed);
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
        .chain(THREE_JANE_NODE_COUNTS.into_iter().flat_map(|nodes| {
            [Protocol::ThreeJane, Protocol::ThreeJaneMaxFaults]
                .into_iter()
                .map(move |protocol| Config { nodes, protocol })
        }))
        .collect()
}

fn simulation<P: Process + Default + Send + 'static>(
    nodes: usize,
    time_budget: Jiffies,
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
        .time_budget(time_budget)
        .seed(42)
        .par_sched(dscale::ThreadNumber::MatchCores)
        .build()
}

fn measure(
    mut simulation: Box<dyn dscale::SimulationRunner>,
    nodes: usize,
    committed: impl FnOnce() -> usize,
) -> (f64, f64) {
    for count in &MESSAGE_COUNTS[..nodes] {
        count.store(0, Ordering::Relaxed);
    }
    simulation.run_full_budget();
    load_stats(
        &MESSAGE_COUNTS[..nodes]
            .iter()
            .map(|count| count.load(Ordering::Relaxed))
            .collect::<Vec<_>>(),
        committed(),
    )
}

fn load_stats(calls: &[usize], committed: usize) -> (f64, f64) {
    if committed == 0 || calls.is_empty() {
        return (0.0, 0.0);
    }
    let mean = calls.iter().sum::<usize>() as f64 / committed as f64;
    let scale = calls.len() as f64 / committed as f64;
    let variance = calls
        .iter()
        .map(|calls| (*calls as f64 * scale - mean).powi(2))
        .sum::<f64>()
        / calls.len() as f64;
    (mean, variance.sqrt())
}

fn run_hotstuff(nodes: usize) -> (f64, f64) {
    let simulation = simulation::<ChainedHotstuff>(nodes, HOTSTUFF_TIME_BUDGET);
    kv::set(
        B0,
        Arc::new(Node {
            id: 0,
            parent: None,
            height: 0,
        }),
    );
    kv::set::<Vec<Jiffies>>(HOTSTUFF_LATENCIES, Vec::new());
    measure(simulation, nodes, || {
        kv::get::<Vec<Jiffies>>(HOTSTUFF_LATENCIES).len()
    })
}

fn run_bullshark(nodes: usize) -> (f64, f64) {
    let simulation = simulation::<Bullshark>(nodes, BULLSHARK_TIME_BUDGET);
    kv::set::<Vec<Jiffies>>(BULLSHARK_LATENCIES, Vec::new());
    measure(simulation, nodes, || {
        kv::get::<Vec<Jiffies>>(BULLSHARK_LATENCIES).len()
    })
}

// scale: quorum for wintermute, 2^14, more budget
// Fix x axis on conflict rate. (defined on photo)

fn run_blsmr(nodes: usize, protocol: BLSMRProtocol, max_three_jane_faults: bool) -> (f64, f64) {
    let time_budget = match &protocol {
        BLSMRProtocol::Wintermute => WINTERMUTE_TIME_BUDGET,
        _ => TIME_BUDGET,
    };
    let simulation = simulation::<BLSMR>(nodes, time_budget);
    let pids = dscale::list_pool(POOL_BLSMR);
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
    kv::set(KEY_ANNOUNCE_TIMEOUT, Jiffies(500));
    kv::set(KEY_KEY_COUNT, 32usize);
    kv::set(KEY_QUORUM_SYSTEM, quorum_system);
    kv::set::<(usize, usize)>(KEY_AVG_COMMIT_LATENCY, (0, 0));
    kv::set::<Vec<Jiffies>>(KEY_COMMIT_LATENCIES, Vec::new());
    kv::set::<(usize, usize)>(KEY_CONFLICT_RATE, (0, 0));
    measure(simulation, nodes, || {
        kv::get::<(usize, usize)>(KEY_AVG_COMMIT_LATENCY).1
    })
}

fn run(config: Config) -> (Config, f64, f64) {
    let (load, standard_deviation) = match config.protocol {
        Protocol::Bullshark => run_bullshark(config.nodes),
        Protocol::ThreeJane => run_blsmr(config.nodes, BLSMRProtocol::ThreeJane, false),
        Protocol::ThreeJaneMaxFaults => run_blsmr(config.nodes, BLSMRProtocol::ThreeJane, true),
        Protocol::Wintermute => run_blsmr(config.nodes, BLSMRProtocol::Wintermute, false),
        Protocol::Hotstuff => run_hotstuff(config.nodes),
    };
    (config, load, standard_deviation)
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
        "protocol,nodes,on_message_calls_per_committed_unit,standard_deviation"
    )
    .expect("failed to write header");
    for (config, load, standard_deviation) in results {
        writeln!(
            file,
            "{},{},{load:.6},{standard_deviation:.6}",
            config.protocol.name(),
            config.nodes
        )
        .expect("failed to write row");
    }
    println!("wrote {}", path.display());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_commits_has_zero_load() {
        assert_eq!(load_stats(&[10, 20], 0), (0.0, 0.0));
    }

    #[test]
    fn load_stats_measure_per_replica_dispersion() {
        assert_eq!(load_stats(&[10, 20, 30], 30), (2.0, 0.816496580927726));
    }

    #[test]
    fn configs_include_both_three_jane_fault_bounds() {
        let configs = configs();
        assert_eq!(
            configs.len(),
            NODE_COUNTS.len() * 3 + THREE_JANE_NODE_COUNTS.len() * 2
        );
        for nodes in THREE_JANE_NODE_COUNTS {
            assert!(
                configs.iter().any(|config| {
                    config.nodes == nodes && config.protocol == Protocol::ThreeJane
                })
            );
            assert!(configs.iter().any(|config| {
                config.nodes == nodes && config.protocol == Protocol::ThreeJaneMaxFaults
            }));
        }
    }
}
