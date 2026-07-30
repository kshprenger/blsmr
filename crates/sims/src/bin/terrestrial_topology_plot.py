#!/usr/bin/env python3
import csv
from pathlib import Path

import matplotlib.pyplot as plt

from plot_style import PLOT_STYLE, save_svg

DATA_PATH = Pat


h(__file__).parent / "latency_cdf" / "aws_regions.csv"
plt.rcParams.update(PLOT_STYLE)


def load_regions():
    with open(DATA_PATH, newline="") as f:
        return list(csv.DictReader(f))


def main():
    regions = load_regions()
    fig, ax = plt.subplots(figsize=(12, 6), dpi=150)
    fig.patch.set_facecolor("#f8fafc")
    ax.set_facecolor("#dceef8")

    ax.set_xlim(-180, 180)
    ax.set_ylim(-90, 90)
    ax.set_aspect("equal")
    ax.set_xticks(range(-180, 181, 30))
    ax.set_yticks(range(-90, 91, 30))
    ax.grid(True, color="#aab7c4", linewidth=0.7)
    ax.scatter(
        [float(region["longitude"]) for region in regions],
        [float(region["latitude"]) for region in regions],
        color="#b65331",
        edgecolors="#ffffff",
        linewidths=1.2,
        s=70,
        zorder=3,
    )
    for region in regions:
        ax.annotate(
            region["region"],
            (float(region["longitude"]), float(region["latitude"])),
            xytext=(6, 6),
            textcoords="offset points",
            fontsize="small",
        )

    ax.set_xlabel("longitude")
    ax.set_ylabel("latitude")
    fig.tight_layout()
    save_svg(fig, __file__)


if __name__ == "__main__":
    main()
