#!/usr/bin/env python3
import csv
import glob
import sys
from pathlib import Path
from statistics import fmean, stdev

import matplotlib.pyplot as plt
import numpy as np

from plot_style import PLOT_STYLE, PROTOCOL_COLORS, save_svg

SURFACE = "#ffffff"
MARKER = PROTOCOL_COLORS["Wintermute"]
FAST_PATH = "#46855c"
GRIDLINE = "#e1e0d9"
BASELINE = "#c3c2b7"
SECONDARY_INK = "#000000"
plt.rcParams.update(PLOT_STYLE)


def load_rows(pattern):
    rows = []
    for path in sorted(glob.glob(pattern)):
        with open(path, newline="") as f:
            for row in csv.DictReader(f):
                rows.append(row)
    return rows


def summarize(rows):
    by_interval = {}
    for row in rows:
        by_interval.setdefault(int(row["submit_interval"]), []).append(row)

    points = []
    for submit_interval, runs in by_interval.items():
        conflicts = [float(run["conflict_rate_pct"]) for run in runs]
        latencies = [float(run["avg_latency_jiffies"]) for run in runs]
        fast_paths = [float(run["fast_path_rate_pct"]) for run in runs]
        points.append(
            (
                fmean(conflicts),
                stdev(conflicts),
                fmean(latencies),
                stdev(latencies),
                fmean(fast_paths),
                stdev(fast_paths),
            )
        )
    return sorted(points)


def main():
    pattern = sys.argv[1] if len(sys.argv) > 1 else str(Path(__file__).parent / "conflict_rate" / "terrestrial_conflict_latency_rank*.csv")
    rows = load_rows(pattern)
    if not rows:
        raise SystemExit(f"no rows found for pattern {pattern!r} — run the latency sweep first")

    points = summarize(rows)
    conflict_pct, conflict_deviation, latency, latency_deviation, fast_path_pct, fast_path_deviation = map(list, zip(*points))
    pearson = np.corrcoef(latency, fast_path_pct)[0, 1]
    print(f"Execution latency versus fast-path rate Pearson correlation: {pearson:.4f}")

    fig, ax = plt.subplots(figsize=(7, 4), dpi=150)
    fast_path_ax = ax.twinx()
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)

    latency_line = ax.errorbar(
        conflict_pct,
        latency,
        xerr=conflict_deviation,
        yerr=latency_deviation,
        color=MARKER,
        linewidth=2,
        marker="o",
        markersize=7,
        markerfacecolor=MARKER,
        markeredgecolor=SURFACE,
        markeredgewidth=0.5,
        capsize=3,
        elinewidth=1,
        label="Execution latency",
        zorder=3,
    )
    fast_path_line = fast_path_ax.errorbar(
        conflict_pct,
        fast_path_pct,
        yerr=fast_path_deviation,
        color=FAST_PATH,
        linestyle="--",
        linewidth=2,
        marker="s",
        markersize=6,
        markerfacecolor=FAST_PATH,
        markeredgecolor=SURFACE,
        markeredgewidth=0.5,
        capsize=3,
        elinewidth=1,
        label="Fast-path rate",
        zorder=2,
    )

    ax.set_xlabel("conflict rate (%)", color=SECONDARY_INK)
    ax.set_ylabel("average latency (jiffies)", color=MARKER)
    fast_path_ax.set_ylabel("fast-path rate (%)", color=FAST_PATH)
    ax.set_xlim(0, 100)
    ax.set_ylim(bottom=0)
    fast_path_ax.set_ylim(0, 100)

    ax.grid(True, color=GRIDLINE, linewidth=0.8, zorder=0)
    fast_path_ax.spines["right"].set_color(FAST_PATH)
    for spine in ("left", "bottom"):
        ax.spines[spine].set_color(BASELINE)
    ax.tick_params(axis="x", colors="#000000")
    ax.tick_params(axis="y", colors="#000000")
    fast_path_ax.tick_params(axis="y", colors="#000000")
    ax.legend(
        [latency_line, fast_path_line],
        ["Execution latency", "Fast-path rate"],
        loc="lower center",
    )

    fig.tight_layout(rect=(0, 0, 0.88, 1))
    save_svg(fig, __file__, crop=False)


if __name__ == "__main__":
    main()
