# Byzantine Leaderless State remote_Machine Replication (BLSMR)

A [DScale](https://codeberg.org/kshprenger/dscale) simulation of BLSMR: a
leaderless replication protocol where each command's dependency set is agreed
through a fast path, falling back to quorum consensus when replicas disagree.
Commands execute once stable, dependencies first, using Tarjan's strongly
connected components algorithm.

All commands below must be run from the repository root.

## Prerequisites

The project requires a C toolchain, an MPI implementation, Rust, Python 3, and
Matplotlib. On Debian or Ubuntu, install the system dependencies with:

```sh
sudo apt-get update
sudo apt-get install -y build-essential curl git libopenmpi-dev openmpi-bin pkg-config python3 python3-venv
```

On macOS with Homebrew:

```sh
brew install open-mpi python
```

Install the latest stable Rust toolchain with
[`rustup`](https://www.rust-lang.org/tools/install):

```sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
. "$HOME/.cargo/env"
rustc --version
cargo --version
```

Create a Python environment for the plotting scripts:

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install matplotlib
```

Activate the environment again with `. .venv/bin/activate` before constructing
plots in a new shell.

## Project structure

```text
.
├── Cargo.toml
├── crates
│   ├── client
│   │   └── command identifiers, workload generation, and the conflict model
│   ├── quorum
│   │   └── quorum-system implementations
│   ├── blsmr
│   │   └── BLSMR protocols, dependency discovery, execution, and metrics
│   ├── hotstuff
│   │   └── Event-driven chained HotStuff implementation
│   ├── bullshark
│   │   └── Bullshark implementation
│   └── sims
│       └── Simulation binaries, input data, result CSV files, and plot scripts
└── README.md
```

The principal BLSMR modules are:

- `crates/blsmr/src/dds.rs`: dependency discovery and fast-path decisions.
- `crates/blsmr/src/quorum.rs`: slow-path quorum consensus.
- `crates/blsmr/src/log.rs`: command phases, dependency execution, latency
  measurement, and conflict-rate tracking.
- `crates/blsmr/src/process.rs`: process timers, messages, and protocol wiring.

The simulation binaries are:

- `latency_cdf`: latency distributions for all protocols on uniform and
  terrestrial topologies.
- `conflict_rate`: Wintermute latency while varying the command conflict rate.
- `scalability`: per-node message-processing cost while increasing the number
  of replicas.

## Local simulations

Run the latency and conflict-rate simulations locally. The scalability
simulation should be run with MPI as described in the next section.

### Latency CDF

```sh
cargo run -p sims --bin latency_cdf --release
python crates/sims/src/bin/latency_cdf_plot.py
```

The simulation writes
`crates/sims/src/bin/latency_cdf/latency_cdf.csv`. The plot shows one latency
CDF for the fixed 100-jiffy topology and one for the terrestrial topology.

The terrestrial topology uses
[official AWS Region IDs and geographies](https://docs.aws.amazon.com/global-infrastructure/latest/regions/aws-regions.html) Display the configured regions with:

```sh
python crates/sims/src/bin/terrestrial_topology_plot.py
```

### Conflict-rate sweep

```sh
cargo run -p sims --bin conflict_rate --release
python crates/sims/src/bin/conflict_rate_plot.py
```

A local run writes
`crates/sims/src/bin/conflict_rate/terrestrial_conflict_latency_rank0.csv`.
The plotting script also reads additional rank files from this directory when
the simulation is launched with multiple local MPI ranks.

## Scalability simulation with MPI

The scalability campaign runs Wintermute and HotStuff up to 2,048 replicas,
Bullshark up to 512 replicas, and the 3Jane variants up to their largest valid
grid size of 2,025 replicas. Each BLSMR run has a 40,000-jiffy budget.

The following workflow cross-compiles the simulation, copies it to the
evaluation cluster, and submits it through Slurm. Install
[`cross`](https://github.com/cross-rs/cross) and ensure Docker or Podman is
running:

```sh
cargo install cross --git https://github.com/cross-rs/cross
ssh remote_machine 'mkdir -p "$HOME/blsmr/scale"'
cross build --release --target x86_64-unknown-linux-musl --bin scalability
scp target/x86_64-unknown-linux-musl/release/scalability remote_machine:~/blsmr/scale/
ssh remote_machine 'sbatch -N 19 -n 19 --ntasks-per-node=1 --cpus-per-task=24 --mem=120G --exclusive --distribution=block:block --nodelist=allemagne,angleterre,autriche,belgique,espagne,finlande,france,groenland,hollande,hongrie,irlande,islande,lituanie,malte,monaco,pologne,portugal,roumanie,suede --chdir="$HOME/blsmr/scale" --wrap='"'"'for n in 2 4 8 9 16 32 36 64 121 128 256 512 529 1024 2025 2048; do srun -n 19 --ntasks-per-node=1 --cpus-per-task=24 --distribution=block:block env SCALABILITY_NODES=$n ./scalability; done'"'"''
```

The node-count list is the union of the regular power-of-two sizes and the
valid 3Jane grid sizes. The binary automatically excludes Bullshark above 512
replicas and excludes protocols that do not support a requested node count.

After Slurm reports that the job has completed, retrieve the rank-local CSV
files and construct the plot:

```sh
mkdir -p crates/sims/src/bin/scale
scp 'remote_machine:~/blsmr/scale/scalability_n*_rank*.csv' crates/sims/src/bin/scale/
python crates/sims/src/bin/scalability_plot.py
```

The plotting script combines every matching CSV file. Ensure
`crates/sims/src/bin/scale` contains only results from the campaign being
plotted.

To run a single node count under another MPI launcher, set
`SCALABILITY_NODES` for every rank:

```sh
cargo build -p sims --bin scalability --release
mpirun -n 4 env SCALABILITY_NODES=256 target/release/scalability
```

The simulation produces one `scalability_n256_rank*.csv` file per MPI rank.
