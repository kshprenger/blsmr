from pathlib import Path

PLOT_STYLE = {
    "font.size": 14,
    "axes.labelsize": 14,
    "xtick.labelsize": 12,
    "ytick.labelsize": 12,
    "legend.fontsize": 12,
}

PROTOCOL_COLORS = {
    "Bullshark": "#b65331",
    "3Jane": "#46855c",
    "3Jane*": "#46855c",
    "Wintermute": "#7a5ca8",
    "HotStuff": "#2a78d6",
    "HotStuff*": "#d9901a",
}


def save_svg(fig, script, crop=True):
    path = Path(script).with_suffix(".svg")
    fig.savefig(path, bbox_inches="tight" if crop else None)
    print(f"wrote {path}")
