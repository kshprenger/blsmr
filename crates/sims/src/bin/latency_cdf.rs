use std::{fs::File, io::Write, path::PathBuf, sync::Arc};

use blsmr::{
    BLSMRProtocol, KEY_ANNOUNCE_TIMEOUT, KEY_AVG_COMMIT_LATENCY, KEY_COMMIT_LATENCIES,
    KEY_CONFLICT_RATE, KEY_KEY_COUNT, KEY_PROTOCOL_TYPE, KEY_QUORUM_SYSTEM, KEY_SUBMIT_INTERVAL,
    KEY_SUBMIT_LIMIT, KEY_TRACK_CONFLICT_RATE, POOL_BLSMR, log, process::BLSMR,
};
use bullshark::{Bullshark, KEY_LATENCIES as BULLSHARK_LATENCIES};
use dscale::{
    BandwidthConfig, Distr, Jiffies, Process, SimulationBuilder,
    rand::{SeedableRng, rngs::SmallRng, seq::SliceRandom},
    services::kv,
};
use hotstuff::{
    B0, ChainedHotstuff, KEY_LATENCIES as HOTSTUFF_LATENCIES, Node, NonRotatingHotstuff,
};

const TIME_BUDGET: Jiffies = Jiffies(2_000_000);
const UNIFORM_LATENCY: Jiffies = Jiffies(100);
const SEED: u64 = 42;
const WINTERMUTE_SUBMIT_INTERVAL: Jiffies = Jiffies(10);
const TERRESTRIAL_WINTERMUTE_SUBMIT_INTERVAL: Jiffies = Jiffies(900);
const THREE_JANE_FAULTS: usize = 1;
const EARTH_RADIUS_KM: f64 = 6_371.0;
const LIGHT_SPEED_KM_PER_SECOND: f64 = 299_792.458;

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
    latitude: f64,
    longitude: f64,
}

fn csv_path() -> PathBuf {
    PathBuf::from("crates/sims/src/bin/latency_cdf/latency_cdf.csv")
}

fn builder() -> SimulationBuilder {
    SimulationBuilder::new()
        .default_bandwidth(BandwidthConfig::Unbounded)
        .time_budget(TIME_BUDGET)
        .seed(SEED)
        .seq_sched()
}

fn fixed_latency(latency: Jiffies) -> Distr {
    Distr::Uniform {
        low: latency,
        high: latency,
    }
}

fn uniform_builder<P: Process + Default + Send + 'static>(
    pool: &'static str,
    replicas: usize,
) -> SimulationBuilder {
    builder()
        .add_pool::<P>(pool, replicas)
        .within_pool_latency(pool, fixed_latency(UNIFORM_LATENCY))
}

fn terrestrial_regions(replicas: usize) -> Vec<Region> {
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
    regions.truncate(replicas);
    regions
}

fn terrestrial_builder<P: Process + Default + Send + 'static>(
    regions: &[Region],
) -> SimulationBuilder {
    let mut simulation = builder();
    for region in regions {
        simulation = simulation.add_pool::<P>(&region.name, 1);
    }
    for region in regions {
        simulation = simulation.within_pool_latency(&region.name, fixed_latency(Jiffies(0)));
    }
    for (index, from) in regions.iter().enumerate() {
        for to in &regions[index + 1..] {
            simulation = simulation.between_pool_latency(
                &from.name,
                &to.name,
                fixed_latency(light_latency(from, to)),
            );
        }
    }
    simulation
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

fn wintermute_submit_interval(topology: NetworkTopology) -> Jiffies {
    match topology {
        NetworkTopology::Uniform => WINTERMUTE_SUBMIT_INTERVAL,
        NetworkTopology::Terrestrial => TERRESTRIAL_WINTERMUTE_SUBMIT_INTERVAL,
    }
}

fn run_hotstuff<P: Process + Default + Send + 'static>(
    topology: NetworkTopology,
    uniform_pool: &'static str,
) -> Vec<Jiffies> {
    let regions = terrestrial_regions(4);
    let mut simulation = match topology {
        NetworkTopology::Uniform => uniform_builder::<P>(uniform_pool, 4),
        NetworkTopology::Terrestrial => terrestrial_builder::<P>(&regions),
    }
    .build();
    kv::set(
        B0,
        Arc::new(Node {
            id: 0,
            parent: None,
            height: 0,
        }),
    );
    kv::set::<Vec<Jiffies>>(HOTSTUFF_LATENCIES, Vec::new());
    simulation.run_full_budget();
    kv::get(HOTSTUFF_LATENCIES)
}

fn run_bullshark(topology: NetworkTopology) -> Vec<Jiffies> {
    let regions = terrestrial_regions(4);
    let mut simulation = match topology {
        NetworkTopology::Uniform => uniform_builder::<Bullshark>("bullshark_uniform", 4),
        NetworkTopology::Terrestrial => terrestrial_builder::<Bullshark>(&regions),
    }
    .build();
    kv::set::<Vec<Jiffies>>(BULLSHARK_LATENCIES, Vec::new());
    simulation.run_full_budget();
    kv::get(BULLSHARK_LATENCIES)
}

fn run_blsmr(
    topology: NetworkTopology,
    protocol: BLSMRProtocol,
    replicas: usize,
    uniform_pool: &'static str,
) -> (Vec<Jiffies>, f64, f64) {
    let regions = terrestrial_regions(replicas);
    let mut simulation = match topology {
        NetworkTopology::Uniform => uniform_builder::<BLSMR>(uniform_pool, replicas),
        NetworkTopology::Terrestrial => terrestrial_builder::<BLSMR>(&regions),
    }
    .build();
    let quorum_system = match &protocol {
        BLSMRProtocol::ThreeJane => quorum::QuorumSystem::new_witnessing_grid_with_faults(
            dscale::list_pool(POOL_BLSMR),
            THREE_JANE_FAULTS,
        ),
        _ => quorum::QuorumSystem::new_dissemination(dscale::list_pool(POOL_BLSMR)),
    };
    kv::set(KEY_PROTOCOL_TYPE, protocol);
    kv::set(KEY_SUBMIT_INTERVAL, wintermute_submit_interval(topology));
    kv::set(KEY_SUBMIT_LIMIT, usize::MAX);
    kv::set(KEY_TRACK_CONFLICT_RATE, true);
    kv::set(KEY_ANNOUNCE_TIMEOUT, Jiffies(500));
    kv::set(KEY_KEY_COUNT, 1024usize);
    kv::set(KEY_QUORUM_SYSTEM, quorum_system);
    kv::set::<(usize, usize)>(KEY_AVG_COMMIT_LATENCY, (0, 0));
    kv::set::<Vec<Jiffies>>(KEY_COMMIT_LATENCIES, Vec::new());
    kv::set(
        KEY_CONFLICT_RATE,
        log::ConflictTracker::new(dscale::list_pool(POOL_BLSMR).len()),
    );
    simulation.run_full_budget();
    (
        kv::get(KEY_COMMIT_LATENCIES),
        log::conflict_rate_percentage(),
        log::fast_path_percentage(),
    )
}

fn write_samples(
    file: &mut File,
    topology: NetworkTopology,
    protocol: &str,
    samples: Vec<Jiffies>,
) {
    for latency in samples {
        writeln!(file, "{},{protocol},{}", topology.name(), latency.0)
            .expect("failed to write latency sample");
    }
}

fn main() {
    let path = csv_path();
    let mut file = File::create(&path).expect("failed to create results file");
    writeln!(file, "topology,protocol,latency_jiffies").expect("failed to write header");
    for topology in [NetworkTopology::Uniform, NetworkTopology::Terrestrial] {
        write_samples(
            &mut file,
            topology,
            "HotStuff",
            run_hotstuff::<ChainedHotstuff>(topology, "hotstuff_uniform"),
        );
        write_samples(
            &mut file,
            topology,
            "Hotstuff*",
            run_hotstuff::<NonRotatingHotstuff>(topology, "hotstuff_star_uniform"),
        );
        write_samples(&mut file, topology, "Bullshark", run_bullshark(topology));
        let (three_jane, _, _) =
            run_blsmr(topology, BLSMRProtocol::ThreeJane, 4, "three_jane_uniform");
        write_samples(&mut file, topology, "3Jane", three_jane);
        let (wintermute, conflict_rate, fast_path_rate) =
            run_blsmr(topology, BLSMRProtocol::Wintermute, 6, "wintermute_uniform");
        write_samples(&mut file, topology, "Wintermute", wintermute);
        println!(
            "{} Wintermute conflict rate: {conflict_rate:.2}%, fast path rate: {fast_path_rate:.2}%",
            topology.name()
        );
    }
    println!("wrote {}", path.display());
}
