#!/usr/bin/env python3
import csv
import sys
from pathlib import Path

import matplotlib.pyplot as plt


COLORS = {
    "HotStuff": "#2a78d6",
    "Hotstuff*": "#d99a2b",
    "Bullshark": "#b65331",
    "3Jane": "#46855c",
    "Wintermute": "#7a5ca8",
}
LINESTYLES = {"Hotstuff*": "--", "3Jane": ":"}
TOPOLOGIES = ("uniform", "terrestrial")


def load_samples(path):
    samples = {}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            key = (row["topology"], row["protocol"])
            samples.setdefault(key, []).append(float(row["latency_jiffies"]))
    return samples


def main():
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent / "latency_cdf" / "latency_cdf.csv"
    samples = load_samples(path)
    if not samples:
        raise SystemExit(f"no samples found in {path!r}")

    fig, axes = plt.subplots(1, len(TOPOLOGIES), figsize=(12, 5), dpi=150, sharey=True)
    for ax, topology in zip(axes, TOPOLOGIES):
        for protocol in COLORS:
            values = sorted(samples.get((topology, protocol), []))
            if not values:
                continue
            cdf = [(index + 1) / len(values) for index in range(len(values))]
            ax.step(
                values,
                cdf,
                where="post",
                label=protocol,
                color=COLORS[protocol],
                linestyle=LINESTYLES.get(protocol, "-"),
                linewidth=2,
            )
        ax.set_title(f"{topology.title()} topology")
        ax.set_xlabel("latency (jiffies)")
        ax.set_xlim(left=0)
        ax.set_ylim(0, 1)
        ax.grid(True, color="#e1e0d9", linewidth=0.8)

    axes[0].set_ylabel("cumulative fraction")
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper right", bbox_to_anchor=(0.99, 0.95))
    fig.tight_layout(rect=(0, 0, 0.84, 1))
    plt.show()


if __name__ == "__main__":
    main()
