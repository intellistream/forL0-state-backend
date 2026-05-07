#!/usr/bin/env python3

from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np


WORKLOADS = ["WC", "FD", "SD", "TM", "LG", "VS", "LR"]
HASHMAP = np.array([1.863, 0.284, 0.314, 0.132, 0.182, 0.032, 0.031])
BRISKSTATE = np.array([2.786, 0.327, 0.349, 0.198, 0.231, 0.032, 0.039])
COLORS = {
    "heap": "#b7c9e2",
    "briskstate": "#1f4e79",
}


def main() -> None:
    output_dir = Path(__file__).resolve().parent

    plt.rcParams.update(
        {
            "font.family": "DejaVu Serif",
            "font.size": 8,
            "axes.labelsize": 9,
            "axes.titlesize": 9,
            "legend.fontsize": 8,
            "xtick.labelsize": 7,
            "ytick.labelsize": 7,
            "axes.grid": True,
            "grid.alpha": 0.45,
            "grid.linewidth": 0.4,
            "savefig.bbox": "tight",
            "savefig.pad_inches": 0.02,
        }
    )

    x = np.arange(len(WORKLOADS))
    width = 0.34

    fig, ax = plt.subplots(figsize=(3.45, 2.35))
    ax.bar(
        x - width / 2,
        HASHMAP,
        width,
        color=COLORS["heap"],
        edgecolor="black",
        linewidth=0.35,
        label="Heap",
    )
    ax.bar(
        x + width / 2,
        BRISKSTATE,
        width,
        color=COLORS["briskstate"],
        edgecolor="black",
        linewidth=0.35,
        label="BriskState",
    )

    ymax = max(BRISKSTATE.max(), HASHMAP.max()) * 1.22
    for idx, (brisk_value, heap_value) in enumerate(zip(BRISKSTATE, HASHMAP)):
        speedup = ((brisk_value - heap_value) / heap_value) * 100 if heap_value > 0 else 0.0
        label_y = max(brisk_value, heap_value) + ymax * 0.025
        ax.text(
            idx,
            label_y,
            f"+{speedup:.0f}%",
            ha="center",
            va="bottom",
            fontsize=6.5,
            color=COLORS["briskstate"],
        )

    ax.set_xticks(x)
    ax.set_xticklabels(WORKLOADS)
    ax.set_ylabel("Throughput (M rec/s/core)")
    ax.set_ylim(0, ymax)
    ax.grid(axis="y", linestyle="--", linewidth=0.4, alpha=0.45)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    handles, labels = ax.get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=2, frameon=False, bbox_to_anchor=(0.5, 1.03))
    fig.subplots_adjust(left=0.13, right=0.995, bottom=0.22, top=0.79)

    fig.savefig(output_dir / "benchset_throughput_paper.pdf")
    fig.savefig(output_dir / "benchset_throughput_paper.png", dpi=300)


if __name__ == "__main__":
    main()