#!/usr/bin/env python3
"""Generate a paper-style comparison figure for NexMark execution times.

Outputs:
  results/figures/nexmark_comparison.pdf
  results/figures/nexmark_comparison.png
"""

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------
QUERIES = ["q4", "q5", "q8", "q9", "q11", "q18", "q19", "q20"]

# 鲲鹏 (TaskManager 4 cores)
# np.nan 表示无数据
NAN = float("nan")
KUNPENG = {
    "HashMap (heap)":   [716, 103, 72,  201, 98,  134, 638, 542],
    "ForL0 (w/  L0)":   [147, 103, 62,  105, 96,   93, 119,  91],
    "ForL0 (w/o L0)":   [159, 100, 62,  107, 102,  98, 121,  94],
    "Baseline (prior eval.)":  [181, 104, NAN, 111, 85, 116, 167, 121],
}

# Intel TaskManager memory sweep.  The formal report keeps only configurations
# where every query has a completed timing result.
INTEL = {
    ("18G", "heap"):  [156, 64, 36,  46, 53, 53, 279, 313],
    ("18G", "forl0"): [ 57, 52, 36,  46, 37, 38,  45,  38],
    ("20G", "heap"):  [ 57, 64, 38,  45, 37, 61, 123,  84],
    ("20G", "forl0"): [ 57, 52, 36,  45, 37, 35,  43,  38],
}


# ---------------------------------------------------------------------------
# Style
# ---------------------------------------------------------------------------
plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "font.size": 10,
    "axes.titlesize": 11,
    "axes.labelsize": 10,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.grid": True,
    "grid.linestyle": ":",
    "grid.linewidth": 0.6,
    "grid.color": "#b8bcc4",
    "axes.axisbelow": True,
    "legend.frameon": False,
    "legend.fontsize": 9,
    "xtick.direction": "out",
    "ytick.direction": "out",
})

# Color palette: cool/colorblind-friendly
C_HEAP     = "#9aa4b2"   # neutral gray for baseline
C_FORL0    = "#1e406e"   # accent dark blue (matches doc accent)
C_FORL0_2  = "#5a8fc7"   # lighter blue for the no-L0 variant
C_BASELINE = "#e0a96d"   # warm orange for prior baseline (收益评估)
C_BARS_INTEL = {
    "heap":  ["#a7b3c2", "#7d8da3"],   # gray family for 2 sizes
    "forl0": ["#3d78b8", "#1e406e"],   # blue family for 2 sizes
}
SIZE_ORDER = ["18G", "20G"]


# ---------------------------------------------------------------------------
# Figure
# ---------------------------------------------------------------------------
fig, axes = plt.subplots(1, 2, figsize=(12.0, 4.4),
                         gridspec_kw={"width_ratios": [1.0, 1.25],
                                       "wspace": 0.22})

# ---- (a) Kunpeng grouped bars --------------------------------------------------
ax = axes[0]
x = np.arange(len(QUERIES))
n_bars = len(KUNPENG)  # 4 bars per query
width = 0.20

bars = []
labels = list(KUNPENG.keys())
colors = [C_HEAP, C_FORL0, C_FORL0_2, C_BASELINE]
for i, (lab, vals) in enumerate(KUNPENG.items()):
    offset = (i - (n_bars - 1) / 2) * width
    plot_vals = [0 if (v != v) else v for v in vals]  # NaN → 0 (no bar)
    b = ax.bar(x + offset, plot_vals, width, label=lab, color=colors[i],
               edgecolor="white", linewidth=0.5)
    bars.append(b)
    # Mark missing baseline points with a small "--"
    for j, v in enumerate(vals):
        if v != v:  # NaN
            ax.text(x[j] + offset, 6, "\u2013", ha="center", va="bottom",
                    fontsize=9, color="#888")

# Annotate the heap-side outliers (Full-GC slowdowns) with a small marker
heap_off = (0 - (n_bars - 1) / 2) * width
for i, v in enumerate(KUNPENG["HashMap (heap)"]):
    if v > 200:  # outlier threshold for clear visual call-out
        ax.text(x[i] + heap_off, v + 14, f"{v}s", ha="center", va="bottom",
                fontsize=7.5, color="#7a1f1f", fontweight="bold")

ax.set_xticks(x)
ax.set_xticklabels(QUERIES)
ax.set_ylabel("Execution time (s)")
ax.set_title("(a) Kunpeng (openEuler, TM 4 cores, Heap 11.7 GB)")
ax.set_ylim(0, 820)
ax.legend(loc="upper center", bbox_to_anchor=(0.5, 1.0), ncols=2,
          handlelength=1.4, fontsize=8.2, columnspacing=1.1)


# ---- (b) Intel grouped bars across TM sizes -----------------------------------
ax = axes[1]
n_sizes = len(SIZE_ORDER)
n_backends = 2
group_w = 0.85
sub_w = group_w / (n_sizes * n_backends)

# inner offsets: per query group we plot 4 bars: heap18, fl0_18, heap20, fl0_20
offsets = []
for i, sz in enumerate(SIZE_ORDER):
    for j, be in enumerate(["heap", "forl0"]):
        offsets.append((sz, be, (i * n_backends + j - (n_sizes * n_backends - 1) / 2) * sub_w))

for sz, be, off in offsets:
    vals = list(INTEL[(sz, be)])
    plot_vals = [0 if np.isnan(v) else v for v in vals]
    color = C_BARS_INTEL[be][SIZE_ORDER.index(sz)]
    label = f"{be} {sz}"
    ax.bar(x + off, plot_vals, sub_w, color=color, edgecolor="white",
           linewidth=0.4, label=label)

ax.set_xticks(x)
ax.set_xticklabels(QUERIES)
ax.set_ylabel("Execution time (s)")
ax.set_title("(b) Intel x86_64 (TaskManager memory sweep: 18G / 20G)")
ax.set_ylim(0, 400)

# Custom compact legend: two rows (heap/forl0) × (18G/20G)
from matplotlib.patches import Patch
legend_handles = []
for be, name in [("heap", "HashMap"), ("forl0", "ForL0")]:
    for i, sz in enumerate(SIZE_ORDER):
        legend_handles.append(
            Patch(facecolor=C_BARS_INTEL[be][i], label=f"{name}  {sz}"))
ax.legend(handles=legend_handles, ncols=3, loc="upper center",
          bbox_to_anchor=(0.5, 1.0),
          handlelength=1.2, columnspacing=1.1, handletextpad=0.5)


# ---------------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------------
out_dir = Path(__file__).resolve().parent
out_pdf = out_dir / "nexmark_comparison.pdf"
out_png = out_dir / "nexmark_comparison.png"
fig.tight_layout()
fig.savefig(out_pdf, bbox_inches="tight")
fig.savefig(out_png, dpi=200, bbox_inches="tight")
print(f"wrote {out_pdf}")
print(f"wrote {out_png}")
