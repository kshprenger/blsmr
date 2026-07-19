# Byzantine Leaderless State Machine Replication (BLSMR)

A [dscale](https://codeberg.org/kshprenger/dscale) simulation of BLSMR: a leaderless
replication protocol where each command's dependency (conflict) set is agreed via a
fast path, falling back to one-shot PBFT when replicas disagree. Commands execute
once stable, dependencies first, via Tarjan's SCC algorithm.

## Crates

- `client` — command/id types and the conflict model.
- `blsmr` — the protocol: fast-path conflict detection (`dds`), slow-path PBFT
  (`pbft`), the dependency log/executor (`log`), and process wiring (`process`).
- `hotstuff` — chained HotStuff process.
- `bullshark` — Bullshark DAG consensus process.
- `quorum` — quorum systems used by both paths.
- `sims` — simulation binaries.

## Running

```sh
cargo test --workspace

cargo run -p sims --bin latency --release
python crates/sims/src/bin/latency_plot.py

cargo run -p sims --bin latency_cdf --release
python crates/sims/src/bin/latency_cdf_plot.py
python crates/sims/src/bin/terrestrial_topology_plot.py

cargo run -p sims --bin scalability --release
python crates/sims/src/bin/scalability_plot.py
```

The CDF simulation includes fixed 100-jiffy and terrestrial AWS-region topologies.
The terrestrial model uses [official AWS Region IDs and geographies](https://docs.aws.amazon.com/global-infrastructure/latest/regions/aws-regions.html) with representative regional-city coordinates;
AWS does not publish individual data-center coordinates.
