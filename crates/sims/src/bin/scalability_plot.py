#!/usr/bin/env python3
import csv
import glob
import sys
from pathlib import Path

import matplotlib.pyplot as plt

from plot_style import PLOT_STYLE, PROTOCOL_COLORS, save_svg

PROTOCOLS = ("Bullshark", "3Jane*", "Wintermute", "HotStuff")
MIN_NODES = {"Bullshark": 4, "3Jane*": 25, "Wintermute": 6, "HotStuff": 4}
plt.rcParams.update(PLOT_STYLE)


def main():
    pattern = sys.argv[1] if len(sys.argv) > 1 else str(Path(__file__).parent / "scale" / "scalability*.csv")
    rows = []
    for path in glob.glob(pattern):
        with open(path, newline="") as f:
            rows.extend(csv.DictReader(f))
    if not rows:
        raise SystemExit(f"no rows found for pattern {pattern!r}")
    if any(
        "average_commit_latency_jiffies" not in row
        or "commit_latency_standard_deviation_jiffies" not in row
        for row in rows
    ):
        raise SystemExit("scalability results do not contain latency deviation — rerun the scalability simulation")

    fig, (load_ax, latency_ax) = plt.subplots(2, 1, figsize=(8, 8), dpi=150, sharex=True)
    for protocol in PROTOCOLS:
        points = sorted(
            (
                int(row["nodes"]),
                float(row["on_message_calls_per_committed_unit"]),
                float(row.get("standard_deviation", 0)),
                float(row["average_commit_latency_jiffies"]),
                float(row["commit_latency_standard_deviation_jiffies"]),
            )
            for row in rows
            if row["protocol"] == protocol
            and int(row["nodes"]) >= MIN_NODES[protocol]
        )
        if not points:
            continue
        nodes, loads, _, latencies, latency_deviations = zip(*points)
        load_ax.plot(
            nodes,
            loads,
            label=protocol.removesuffix("*"),
            color=PROTOCOL_COLORS[protocol],
            linewidth=2,
            marker="o",
        )
        latency_ax.errorbar(
            nodes,
            latencies,
            yerr=latency_deviations,
            color=PROTOCOL_COLORS[protocol],
            linewidth=2,
            marker="o",
            capsize=3,
        )

    load_ax.set_ylabel("worst-case messages per commit event")
    latency_ax.set_xlabel("nodes")
    latency_ax.set_ylabel("average latency (jiffies)")
    latency_ax.set_xscale("log", base=2)
    # load_ax.set_yscale("symlog", linthresh=1)
    load_ax.set_ylim(bottom=0)
    latency_ax.set_ylim(bottom=0)
    latency_ax.set_xticks([2**power for power in range(1, 12)])
    latency_ax.get_xaxis().set_major_formatter(plt.ScalarFormatter())
    for ax in (load_ax, latency_ax):
        ax.grid(True, color="#e1e0d9", linewidth=0.8)
    load_ax.legend(loc="upper left", bbox_to_anchor=(1.02, 1), borderaxespad=0)
    fig.tight_layout()
    save_svg(fig, __file__)


if __name__ == "__main__":
    main()
