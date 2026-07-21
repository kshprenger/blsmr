#!/usr/bin/env python3
import csv
import glob
import sys
from pathlib import Path

import matplotlib.pyplot as plt

SURFACE = "#fcfcfb"
MARKER = "#2a78d6"
GRIDLINE = "#e1e0d9"
BASELINE = "#c3c2b7"
MUTED = "#898781"
PRIMARY_INK = "#0b0b0b"
SECONDARY_INK = "#52514e"


def load_rows(pattern):
    rows = []
    for path in sorted(glob.glob(pattern)):
        with open(path, newline="") as f:
            for row in csv.DictReader(f):
                rows.append(row)
    return rows


def main():
    pattern = sys.argv[1] if len(sys.argv) > 1 else str(Path(__file__).parent / "conflict_rate" / "terrestrial_conflict_latency_rank*.csv")
    rows = load_rows(pattern)
    if not rows:
        raise SystemExit(f"no rows found for pattern {pattern!r} — run the latency sweep first")

    by_conflict_pct = {}
    for r in rows:
        by_conflict_pct.setdefault(round(float(r["conflict_rate_pct"]), 1), []).append(
            float(r["avg_latency_jiffies"])
        )
    conflict_pct = sorted(by_conflict_pct)
    latency = [sum(by_conflict_pct[x]) / len(by_conflict_pct[x]) for x in conflict_pct]

    fig, ax = plt.subplots(figsize=(7, 5), dpi=150)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)

    ax.plot(
        conflict_pct,
        latency,
        color=MARKER,
        linewidth=2,
        solid_capstyle="round",
        marker="o",
        markersize=8,
        markerfacecolor=MARKER,
        markeredgecolor=SURFACE,
        markeredgewidth=0.5,
        zorder=3,
    )

    ax.set_title("Wintermute: commit latency vs. conflict rate", color=PRIMARY_INK, fontsize=13, pad=12)
    ax.set_xlabel("conflict rate (%)", color=SECONDARY_INK, fontsize=10)
    ax.set_ylabel("average commit latency (jiffies)", color=SECONDARY_INK, fontsize=10)

    ax.grid(True, color=GRIDLINE, linewidth=0.8, zorder=0)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    for spine in ("left", "bottom"):
        ax.spines[spine].set_color(BASELINE)
    ax.tick_params(colors=MUTED, labelsize=9)

    fig.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()
