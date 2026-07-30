#!/usr/bin/env python3
import csv
import sys
from pathlib import Path

import matplotlib.pyplot as plt

from plot_style import PLOT_STYLE, PROTOCOL_COLORS, save_svg

PROTOCOLS = ("HotStuff", "Bullshark", "3Jane", "Wintermute")
PROTOCOL_ALIASES = {"Hotstuff*": "HotStuff*"}
LINESTYLES = {"HotStuff*": "--", "3Jane": ":"}
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


def main():
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent / "latency_cdf" / "latency_cdf.csv"
    samples = load_samples(path)
    if not samples:
        raise SystemExit(f"no samples found in {path!r}")

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
        ax.set_ylabel("cumulative fraction")
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
    save_svg(fig, __file__)


if __name__ == "__main__":
    main()
