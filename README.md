# Byzantine Leaderless State Machine Replication

Simulator - https://git.kshprenger.com/kshprenger/dscale

Simulations of BLSMR,
HotStuff, and Bullshark. BLSMR agrees on command dependencies through a fast
path, falls back to quorum consensus when necessary, and executes stable
commands dependency-first.

Run every command from the repository root.

## Setup

Install a C toolchain, MPI, Python, and Rust. On Debian or Ubuntu:

```sh
sudo apt-get update
sudo apt-get install -y build-essential curl libopenmpi-dev openmpi-bin python3 python3-venv
```

On macOS:

```sh
brew install open-mpi python
```

Install Rust and Matplotlib:

```sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
. "$HOME/.cargo/env"
python3 -m venv .venv
.venv/bin/python -m pip install matplotlib
cargo check --workspace
```

## Layout

- `crates/blsmr`: BLSMR protocol and metrics.
- `crates/client`: commands and workloads.
- `crates/quorum`: quorum systems.
- `crates/hotstuff`: HotStuff.
- `crates/bullshark`: Bullshark.
- `crates/sims`: simulations and plotting scripts.

## Latency CDF

```sh
cargo run -p sims --bin latency_cdf --release
.venv/bin/python crates/sims/src/bin/latency_cdf_plot.py
```

Outputs:

- `crates/sims/src/bin/latency_cdf/latency_cdf.csv`
- `crates/sims/src/bin/latency_cdf/wintermute_paths.csv`
- `crates/sims/src/bin/latency_cdf_plot.svg`

The Python script prints Wintermute fast-path, combined slow-path, and chaining
rates, the number of chaining-affected commands, and the median terrestrial
latency of every protocol.

The terrestrial model uses representative coordinates for
[AWS regions](https://docs.aws.amazon.com/global-infrastructure/latest/regions/aws-regions.html).
Plot it with:

```sh
.venv/bin/python crates/sims/src/bin/terrestrial_topology_plot.py
```

## Conflict-rate sweep

```sh
cargo run -p sims --bin conflict_rate --release
.venv/bin/python crates/sims/src/bin/conflict_rate_plot.py
```

Outputs:

- `crates/sims/src/bin/conflict_rate/terrestrial_conflict_latency_rank0.csv`
- `crates/sims/src/bin/conflict_rate_plot.svg`

## Scalability

The simulation starts Bullshark and HotStuff at 4 replicas, Wintermute at 6,
and 3Jane at 25. Every protocol supports up to 1,024 replicas.

Local MPI run:

```sh
cargo build -p sims --bin scalability --release
mpirun -n 4 env SCALABILITY_NODES=256 target/release/scalability
```

Cluster run with `cross` and Slurm:

```sh
cargo install cross --git https://github.com/cross-rs/cross
ssh remote_machine 'mkdir -p "$HOME/blsmr/scale"'
cross build --release --target x86_64-unknown-linux-musl --bin scalability
scp target/x86_64-unknown-linux-musl/release/scalability remote_machine:~/blsmr/scale/
ssh remote_machine 'sbatch -N 19 -n 19 --ntasks-per-node=1 --cpus-per-task=24 --exclusive --distribution=block:block --chdir="$HOME/blsmr/scale" --wrap='"'"'for n in 4 6 8 16 25 32 36 64 121 128 256 512 529 1024; do srun -n 19 --ntasks-per-node=1 --cpus-per-task=24 --distribution=block:block env SCALABILITY_NODES=$n ./scalability; done'"'"''
```

Retrieve and plot the results:

```sh
mkdir -p crates/sims/src/bin/scale
scp 'remote_machine:~/blsmr/scale/scalability_n*_rank*.csv' crates/sims/src/bin/scale/
.venv/bin/python crates/sims/src/bin/scalability_plot.py
```
