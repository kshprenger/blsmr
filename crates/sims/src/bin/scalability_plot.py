#!/usr/bin/env python3
import csv
import glob
import sys
from pathlib import Path

import matplotlib.pyplot as plt


COLORS = {
    "Bullshark": "#b65331",
    "3Jane": "#46855c",
    "3Jane*": "#46855c",
    "Wintermute": "#7a5ca8",
    "HotStuff": "#2a78d6",
}
LINESTYLES = {"3Jane*": "--"}


def main():
    pattern = sys.argv[1] if len(sys.argv) > 1 else str(Path(__file__).parent / "scale" / "scalability*.csv")
    rows = []
    for path in glob.glob(pattern):
        with open(path, newline="") as f:
            rows.extend(csv.DictReader(f))
    if not rows:
        raise SystemExit(f"no rows found for pattern {pattern!r}")

    fig, ax = plt.subplots(figsize=(8, 5), dpi=150)
    for protocol, color in COLORS.items():
        points = sorted(
            (
                int(row["nodes"]),
                float(row["on_message_calls_per_committed_unit"]),
                float(row.get("standard_deviation", 0)),
            )
            for row in rows
            if row["protocol"] == protocol
        )
        if not points:
            continue
        nodes, loads, deviations = zip(*points)
        ax.errorbar(
            nodes,
            loads,
            yerr=deviations,
            label=protocol,
            color=color,
            linestyle=LINESTYLES.get(protocol, "-"),
            linewidth=2,
            marker="o",
            capsize=3,
        )

    ax.set_title("Protocol message-processing cost")
    ax.set_xlabel("nodes")
    ax.set_ylabel("on_message calls per committed unit")
    ax.set_xscale("log", base=2)
    ax.set_yscale("symlog", linthresh=1)
    ax.set_ylim(bottom=0)
    ax.set_xticks([2**power for power in range(1, 15)])
    ax.get_xaxis().set_major_formatter(plt.ScalarFormatter())
    ax.grid(True, color="#e1e0d9", linewidth=0.8)
    ax.legend()
    fig.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()
