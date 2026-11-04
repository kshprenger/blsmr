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

Each run replays 540,000 one-unit transfers over the half-open 300-second
arrival window at an aggregate 1,800 transactions/s. Every replica hosts ten
virtual clients: the 4-, 6-, and 25-replica variants have 40, 60, and 250
clients, offering 45, 30, and 7.2 transactions/s per client, respectively.
Clients are assigned round-robin to replica ordinals. Global event `i` is
scheduled at `round(i * 1,000,000 / 1,800)` microseconds, assigned to client
`i % client_count`, and uses that client's sequence `i / client_count`. Thus
arrivals are spread across clients and replicas rather than synchronized.

The seed-42 trace uses 10,000 accounts: each shuffled permutation is consumed
in ordered, distinct sender/receiver pairs, with amount 1. The global payload
sequence and timestamps are identical for every protocol and topology; client
assignment changes with replica count. Commands conflict when either account
overlaps. There are no balances or transfer execution. The simulation reports
actual submissions per client and replica, sampled completions, and the number
submitted but not sampled. The 2,000-second simulation budget is unchanged.

The CDF includes only commands sampled before the budget cutoff: BLSMR measures
origin submission to stability, while HotStuff and Bullshark measure submission
to first observed commit or DAG ordering. These are not client-response
latencies, and unfinished commands are absent. Despite the CSV column name
`latency_jiffies`, its values and the plot labels are in milliseconds. The
plot retains its existing selection and does not display HotStuff*.

The terrestrial CDF network uses pair-specific symmetric normal delays derived
from the [AWS All-Regions Ping dataset](https://doi.org/10.5281/zenodo.15590100)
by Valerio Schiavoni, licensed [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
The May 2024–January 2025 archive has 5,755 valid positive finite RTT snapshot
averages per direction for these pairs; 24 snapshots contain invalid JSON and
some hours are missing, so observations do not provide uniform time coverage.
Each one-way mean is approximated as `(mean RTT A→B + mean RTT B→A) / 4`;
this does not establish directional symmetry. The IID Gaussian standard
deviation of 0.593 ms is half the RTT standard deviation of 1.186 ms from the
[CALM Virginia–Frankfurt sample](https://github.com/iqbal-h/CALM/tree/main/sample_dataset)
([paper](https://doi.org/10.1109/TNET.2022.3148701)), assuming symmetric,
perfectly correlated directional halves. It is not a measured one-way jitter,
and the sample has non-Gaussian tails. Other assumptions and route or temporal
variation would produce different results.

Dscale samples native normal distributions in 1 µs jiffies, rounding the pair
mean to the nearest microsecond and samples to integer microseconds, then
clamping to `[1 µs, ceil(mean + 6 × 0.593 ms)]`. Within-region links, including
self-delivery, are fixed at 1 µs; the uniform topology remains fixed at 100 ms.
The runtime does not fetch measurements. To regenerate the embedded pair means,
download `cloudping_may24_jan25.tgz` from the dataset record (SHA-256
`549adec2a11a8bbf30a036d909784f456a104ad18d34d4cf0875b05966b13faf`) and run:

```sh
python3 crates/sims/preprocess_aws_pair_means.py /path/to/cloudping_may24_jan25.tgz > crates/sims/src/bin/latency_cdf/aws_pair_means.csv
```

The separate topology plot shows representative coordinates for
[AWS regions](https://docs.aws.amazon.com/global-infrastructure/latest/regions/aws-regions.html),
not the calibrated link delays. Plot it with:

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
