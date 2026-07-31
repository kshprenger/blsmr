#!/usr/bin/env python3
import csv
import sys
from pathlib import Path
from statistics import median

import matplotlib.pyplot as plt

from plot_style import PLOT_STYLE, PROTOCOL_COLORS, save_svg

PROTOCOLS = ("HotStuff", "Bullshark", "3Jane", "Wintermute")
PROTOCOL_ALIASES = {"Hotstuff*": "HotStuff*"}
LINESTYLES = {"HotStuff*": "--"}
TOPOLOGIES = ("uniform", "terrestrial")
plt.rcParams.update(PLOT_STYLE)


def load_samples(path):
    samples = {}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            protocol = PROTOCOL_ALIASES.get(row["protocol"], row["protocol"])
            key = (row["topology"], protocol)
            samples.setdefault(key, []).append(float(row["latency_jiffies"]))
    return samples


def load_paths(path):
    paths = {}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            command = (int(row["command_pid"]), int(row["command_id"]))
            record = paths.setdefault(row["topology"], {}).setdefault(
                command,
                {
                    "fast": row["direct_fast"] == "true",
                    "decided": row["decided"] == "true",
                    "deps": set(),
                },
            )
            if row["dependency_pid"]:
                record["deps"].add(
                    (int(row["dependency_pid"]), int(row["dependency_id"]))
                )
    return paths


def print_slow_path_summary(paths):
    for topology in TOPOLOGIES:
        commands = paths.get(topology, {})
        if not commands:
            continue
        direct_slow = {
            command for command, record in commands.items() if not record["fast"]
        }
        affected = set(direct_slow)
        while True:
            previous = len(affected)
            affected.update(
                command
                for command, record in commands.items()
                if record["deps"] & affected
            )
            if len(affected) == previous:
                break
        chained = {
            command for command in affected if commands[command]["fast"]
        }
        print(
            f"{topology} Wintermute fast-path rate: "
            f"{100 * (len(commands) - len(direct_slow)) / len(commands):.2f}%, "
            f"slow-path rate: "
            f"{100 * len(affected) / len(commands):.2f}%, "
            f"chaining-effect rate: {100 * len(chained) / len(commands):.2f}% "
            f"({len(chained)} commands)"
        )


def print_terrestrial_medians(samples):
    for (topology, protocol), values in samples.items():
        if topology == "terrestrial":
            print(
                f"terrestrial {protocol} median latency: "
                f"{median(values):.2f} jiffies"
            )


def main():
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent / "latency_cdf" / "latency_cdf.csv"
    path_records_path = Path(sys.argv[2]) if len(sys.argv) > 2 else path.with_name("wintermute_paths.csv")
    samples = load_samples(path)
    if not samples:
        raise SystemExit(f"no samples found in {path!r}")
    if not path_records_path.exists():
        raise SystemExit(
            f"no path records found in {path_records_path!r} — rerun the CDF simulation"
    )
    print_slow_path_summary(load_paths(path_records_path))
    print_terrestrial_medians(samples)

    fig, axes = plt.subplots(len(TOPOLOGIES), 1, figsize=(8, 9), dpi=150, sharey=True)
    for index, (ax, topology) in enumerate(zip(axes, TOPOLOGIES)):
        for protocol in PROTOCOLS:
            values = sorted(samples.get((topology, protocol), []))
            if not values:
                continue
            cdf = [(index + 1) / len(values) for index in range(len(values))]
            ax.step(
                values,
                cdf,
                where="post",
                label=protocol,
                color=PROTOCOL_COLORS[protocol],
                linestyle=LINESTYLES.get(protocol, "-"),
                linewidth=2,
            )
        ax.set_xlabel("latency (jiffies)")
        ax.set_ylabel("CDF")
        ax.set_xlim(left=0)
        ax.set_ylim(0, 1)
        ax.grid(True, color="#e1e0d9", linewidth=0.8)
        ax.text(
            0,
            1.02,
            f"({chr(ord('a') + index)}) {topology.title()} topology",
            transform=ax.transAxes,
            va="bottom",
            fontweight="bold",
        )

    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper left", bbox_to_anchor=(0.77, 0.98))
    fig.tight_layout(rect=(0, 0, 0.76, 1))
    save_svg(fig, __file__, crop=False)


if __name__ == "__main__":
    main()
