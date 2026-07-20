#!/usr/bin/env python3
import csv
import glob
import sys
from pathlib import Path

import matplotlib.pyplot as plt


COLORS = {
    "Bullshark": "#b65331",
    "3Jane": "#46855c",
    "Wintermute": "#7a5ca8",
    "HotStuff": "#2a78d6",
}


def main():
    pattern = sys.argv[1] if len(sys.argv) > 1 else str(Path(__file__).parent / "scalability" / "scalability_rank*.csv")
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
                float(row["avg_on_message_calls_per_replica_per_jiffy"]),
            )
            for row in rows
            if row["protocol"] == protocol
        )
        ax.plot(*zip(*points), label=protocol, color=color, linewidth=2, marker="o")

    ax.set_title("Protocol message-processing load")
    ax.set_xlabel("nodes")
    ax.set_ylabel("average on_message calls per replica per jiffy")
    ax.set_xscale("log", base=2)
    ax.set_xticks([2**power for power in range(1, 12)])
    ax.get_xaxis().set_major_formatter(plt.ScalarFormatter())
    ax.grid(True, color="#e1e0d9", linewidth=0.8)
    ax.legend()
    fig.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()
