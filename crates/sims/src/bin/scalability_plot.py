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
    fig, load_ax = plt.subplots(figsize=(7, 4), dpi=150)
    for protocol in PROTOCOLS:
        points = sorted(
            (
                int(row["nodes"]),
                float(row["on_message_calls_per_committed_unit"]),
            )
            for row in rows
            if row["protocol"] == protocol
            and int(row["nodes"]) >= MIN_NODES[protocol]
        )
        if not points:
            continue
        nodes, loads = zip(*points)
        load_ax.plot(
            nodes,
            loads,
            label=protocol.removesuffix("*"),
            color=PROTOCOL_COLORS[protocol],
            linewidth=2,
            marker="o",
        )

    load_ax.set_ylabel("messages per commit event")
    load_ax.set_xlabel("nodes")
    load_ax.set_xscale("log", base=2)
    load_ax.set_ylim(bottom=0)
    load_ax.set_xticks([2**power for power in range(1, 12)])
    load_ax.get_xaxis().set_major_formatter(plt.ScalarFormatter())
    load_ax.grid(True, color="#e1e0d9", linewidth=0.8)
    load_ax.legend(loc="upper left", bbox_to_anchor=(1.02, 1), borderaxespad=0)
    fig.tight_layout()
    save_svg(fig, __file__, crop=False)


if __name__ == "__main__":
    main()
