#!/usr/bin/env python3
"""
Sweep WordCount benchmark over (num_keys, skew_factor) grid and plot 3D bar chart.

For each (num_keys, skew_factor) combination, this script runs WordCount on
both HashMap and ForL0 state backends (by default) and records throughput.
The final output is a 3D bar chart where:
    X axis = num_keys
    Y axis = skew_factor
    Z axis = metric (speedup by default: forl0 / hashmap)

Usage:
    # Default grid (1M-10M keys, skew 0-1.0):
    python sweep_wordcount_grid.py

    # Custom grid:
    python sweep_wordcount_grid.py \
        --num-keys 1000000 2000000 4000000 6000000 8000000 10000000 \
        --skew-factors 0.0 0.2 0.4 0.6 0.8 1.0

    # Only forl0 (plot absolute throughput instead of speedup):
    python sweep_wordcount_grid.py --backends forl0

    # Repeat each cell N times and average:
    python sweep_wordcount_grid.py --repeat 2

    # Resume from previous run:
    python sweep_wordcount_grid.py --resume

    # Skip plotting:
    python sweep_wordcount_grid.py --no-plot
"""

import argparse
import copy
import json
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).parent))

from utils.config import load_config, get_results_dir, get_timestamp
from run_wordcount import run_wordcount


DEFAULT_NUM_KEYS = [1_000_000, 2_000_000, 4_000_000, 6_000_000, 8_000_000, 10_000_000]
DEFAULT_SKEW_FACTORS = [0.0, 0.2, 0.4, 0.6, 0.8, 1.0]


def make_config(base_config: dict, num_keys: int, skew_factor: float) -> dict:
    """Return a deep-copied config with wordcount.num_keys / skew_factor overridden."""
    cfg = copy.deepcopy(base_config)
    wc = cfg.setdefault('wordcount', {})
    wc['num_keys'] = int(num_keys)
    wc['skew_factor'] = float(skew_factor)
    return cfg


def cell_key(num_keys: int, skew_factor: float) -> str:
    return f'{int(num_keys)}_{float(skew_factor):.3f}'


def run_cell(base_config: dict, num_keys: int, skew_factor: float,
             backends: List[str], repeat: int, cooldown: int) -> Dict[str, Optional[dict]]:
    """Run one grid cell for each requested backend. Returns {backend: result_or_None}."""
    cfg = make_config(base_config, num_keys, skew_factor)
    out: Dict[str, Optional[dict]] = {}

    for backend in backends:
        print(f'\n{"=" * 66}')
        print(f'  cell: num_keys={num_keys:,}  skew_factor={skew_factor}  backend={backend}')
        print(f'{"=" * 66}')

        runs = []
        for trial in range(repeat):
            if repeat > 1:
                print(f'\n  Trial {trial + 1}/{repeat}')
            r = run_wordcount(cfg, backend=backend, profile_mode=None)
            if r:
                runs.append(r)
            if trial < repeat - 1:
                print(f'  Cooldown {cooldown}s ...')
                time.sleep(cooldown)

        if not runs:
            print(f'  WARNING: all trials failed for backend={backend}')
            out[backend] = None
            continue

        if len(runs) > 1:
            avg_tpc = sum(r['throughput_per_core'] for r in runs) / len(runs)
            avg_tp = sum(r['throughput'] for r in runs) / len(runs)
            avg_t = sum(r['total_time_seconds'] for r in runs) / len(runs)
            best = max(runs, key=lambda r: r['throughput_per_core'])
            best['throughput_per_core'] = avg_tpc
            best['throughput'] = avg_tp
            best['total_time_seconds'] = avg_t
            best['num_trials'] = len(runs)
            best['trial_throughputs'] = [r['throughput_per_core'] for r in runs]
            out[backend] = best
        else:
            out[backend] = runs[0]

    return out


def save_results(records: List[dict], output_dir: Path, suffix: str = '') -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = get_timestamp()
    name = f'wordcount_grid_sweep_{ts}{suffix}.json'
    path = output_dir / name
    with open(path, 'w') as f:
        json.dump({'timestamp': ts, 'records': records}, f, indent=2, default=str)
    print(f'\nResults saved to {path}')
    return path


def load_existing(output_dir: Path) -> List[dict]:
    files = sorted(output_dir.glob('wordcount_grid_sweep_*.json'), reverse=True)
    if not files:
        return []
    with open(files[0]) as f:
        data = json.load(f)
    return data.get('records', [])


def _render_3d_bar(Z, num_keys_axis: List[int], skew_axis: List[float],
                   zlabel: str, title: str, best_fmt: str,
                   output_dir: Path, filename_stem: str,
                   cmap_name: str = 'viridis'):
    """Render a single 3D bar chart from a 2D grid `Z`, saving pdf+png."""
    import matplotlib.pyplot as plt
    from mpl_toolkits.mplot3d import Axes3D  # noqa: F401
    import numpy as np

    valid = Z[~np.isnan(Z)]
    if valid.size == 0:
        print(f'WARNING: no valid data for {filename_stem}, skipping plot')
        return
    vmin, vmax = float(valid.min()), float(valid.max())
    if vmax == vmin:
        vmax = vmin + 1e-9

    try:
        cmap = plt.get_cmap(cmap_name)
    except Exception:
        cmap = None

    nx = len(num_keys_axis)
    dx = 0.7
    dy = 0.07 if max(skew_axis) - min(skew_axis) > 0 else 0.1

    fig = plt.figure(figsize=(12, 8))
    ax = fig.add_subplot(111, projection='3d')

    xs, ys, zs, dzs, colors = [], [], [], [], []
    for i, _nk in enumerate(num_keys_axis):
        for j, sk in enumerate(skew_axis):
            val = Z[i, j]
            if np.isnan(val):
                continue
            xs.append(i - dx / 2)  # categorical X (index-based) for even spacing
            ys.append(sk - dy / 2)
            zs.append(0)
            dzs.append(float(val))
            if cmap is not None:
                norm = (val - vmin) / (vmax - vmin)
                colors.append(cmap(norm))
            else:
                colors.append('#2196F3')

    ax.bar3d(xs, ys, zs, dx, dy, dzs, color=colors, shade=True,
             edgecolor='k', linewidth=0.3)

    ax.set_xticks(list(range(nx)))
    ax.set_xticklabels([f'{nk/1e6:g}M' for nk in num_keys_axis])
    ax.set_yticks(skew_axis)
    ax.set_yticklabels([f'{s:g}' for s in skew_axis])

    ax.set_xlabel('num_keys', fontsize=11, labelpad=10)
    ax.set_ylabel('skew_factor (Zipf s)', fontsize=11, labelpad=10)
    ax.set_zlabel(zlabel, fontsize=11, labelpad=8)
    ax.set_title(title, fontsize=13)

    flat_idx = np.nanargmax(Z)
    bi, bj = np.unravel_index(flat_idx, Z.shape)
    best_label = best_fmt.format(
        num_keys=num_keys_axis[bi] / 1e6,
        skew=skew_axis[bj],
        value=Z[bi, bj],
    )
    ax.text2D(0.02, 0.96, best_label, transform=ax.transAxes, fontsize=10,
              bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

    ax.view_init(elev=25, azim=-60)

    output_dir.mkdir(parents=True, exist_ok=True)
    for fmt in ['pdf', 'png']:
        path = output_dir / f'{filename_stem}.{fmt}'
        fig.savefig(str(path), bbox_inches='tight', dpi=150)
        print(f'Figure saved: {path}')
    plt.close(fig)


def plot_3d(records: List[dict], num_keys_axis: List[int], skew_axis: List[float],
            backends: List[str], output_dir: Path):
    """Generate 3D bar chart(s).

    - If both backends are present: produce three figures — hashmap throughput,
      forl0 throughput, and forl0/hashmap speedup.
    - Otherwise: produce a single figure with the requested backend's throughput.
    """
    try:
        import matplotlib
        matplotlib.use('Agg')
        import numpy as np
    except ImportError:
        print('WARNING: matplotlib/numpy not installed, skipping plot')
        return

    # Index results by (num_keys, skew_factor)
    idx: Dict[str, dict] = {}
    for rec in records:
        idx[cell_key(rec['num_keys'], rec['skew_factor'])] = rec

    nx, ny = len(num_keys_axis), len(skew_axis)

    def tpc_grid(backend_name: str):
        Z = np.full((nx, ny), np.nan)
        for i, nk in enumerate(num_keys_axis):
            for j, sk in enumerate(skew_axis):
                rec = idx.get(cell_key(nk, sk))
                if not rec:
                    continue
                r = rec.get('results', {}).get(backend_name)
                if r:
                    tpc = r.get('throughput_per_core', 0) or 0
                    Z[i, j] = tpc / 1e6
        return Z

    both = ('forl0' in backends) and ('hashmap' in backends)

    if both:
        Zh = tpc_grid('hashmap')
        Zf = tpc_grid('forl0')

        # Speedup grid (forl0 / hashmap); avoid div-by-zero
        Zs = np.full((nx, ny), np.nan)
        mask = (~np.isnan(Zh)) & (~np.isnan(Zf)) & (Zh > 0)
        Zs[mask] = Zf[mask] / Zh[mask]

        _render_3d_bar(
            Zh, num_keys_axis, skew_axis,
            zlabel='Throughput (M rec/s/core)',
            title='WordCount Throughput — HashMapStateBackend',
            best_fmt='best: num_keys={num_keys:g}M, skew={skew:g}, '
                     'throughput={value:.3f} M/s/core',
            output_dir=output_dir, filename_stem='wordcount_grid_hashmap',
            cmap_name='Blues',
        )
        _render_3d_bar(
            Zf, num_keys_axis, skew_axis,
            zlabel='Throughput (M rec/s/core)',
            title='WordCount Throughput — ForL0StateBackend',
            best_fmt='best: num_keys={num_keys:g}M, skew={skew:g}, '
                     'throughput={value:.3f} M/s/core',
            output_dir=output_dir, filename_stem='wordcount_grid_forl0',
            cmap_name='Oranges',
        )
        _render_3d_bar(
            Zs, num_keys_axis, skew_axis,
            zlabel='Speedup (forl0 / hashmap)',
            title='WordCount Speedup — ForL0 vs HashMap',
            best_fmt='best: num_keys={num_keys:g}M, skew={skew:g}, '
                     'speedup={value:.3f}x',
            output_dir=output_dir, filename_stem='wordcount_grid_speedup',
            cmap_name='viridis',
        )
    else:
        only = backends[0]
        Z = tpc_grid(only)
        _render_3d_bar(
            Z, num_keys_axis, skew_axis,
            zlabel='Throughput (M rec/s/core)',
            title=f'WordCount Throughput ({only})',
            best_fmt='best: num_keys={num_keys:g}M, skew={skew:g}, '
                     'throughput={value:.3f} M/s/core',
            output_dir=output_dir, filename_stem=f'wordcount_grid_{only}',
            cmap_name='viridis',
        )


def print_summary(records: List[dict], backends: List[str]):
    both = ('forl0' in backends) and ('hashmap' in backends)
    print(f'\n{"=" * 78}')
    print(f'  WordCount Grid Sweep — Summary')
    print(f'{"=" * 78}')
    header = f'  {"num_keys":>10}  {"skew":>6}'
    if 'hashmap' in backends:
        header += f'  {"hashmap (M/s/core)":>20}'
    if 'forl0' in backends:
        header += f'  {"forl0 (M/s/core)":>20}'
    if both:
        header += f'  {"speedup":>9}'
    print(header)
    print('  ' + '-' * (len(header) - 2))

    best = (None, -1.0)
    for rec in records:
        nk = rec['num_keys']
        sk = rec['skew_factor']
        row = f'  {nk:>10,}  {sk:>6.2f}'
        h = rec.get('results', {}).get('hashmap')
        f = rec.get('results', {}).get('forl0')
        h_tpc = (h or {}).get('throughput_per_core') if h else None
        f_tpc = (f or {}).get('throughput_per_core') if f else None
        if 'hashmap' in backends:
            row += f'  {(h_tpc/1e6 if h_tpc else float("nan")):>20.3f}'
        if 'forl0' in backends:
            row += f'  {(f_tpc/1e6 if f_tpc else float("nan")):>20.3f}'
        if both and h_tpc and f_tpc:
            sp = f_tpc / h_tpc
            row += f'  {sp:>9.3f}'
            if sp > best[1]:
                best = ((nk, sk), sp)
        elif not both and f_tpc is not None:
            if f_tpc > best[1]:
                best = ((nk, sk), f_tpc)
        print(row)

    if best[0] is not None:
        print()
        if both:
            print(f'  Best for ForL0: num_keys={best[0][0]:,}, skew={best[0][1]}, '
                  f'speedup={best[1]:.3f}x')
        else:
            print(f'  Best cell: num_keys={best[0][0]:,}, skew={best[0][1]}, '
                  f'throughput/core={best[1]:,.0f} rec/s/core')
    print()


def main():
    p = argparse.ArgumentParser(description='Sweep WordCount over (num_keys, skew_factor) grid')
    p.add_argument('--num-keys', type=int, nargs='+', default=DEFAULT_NUM_KEYS,
                   help=f'num_keys values (default: {DEFAULT_NUM_KEYS})')
    p.add_argument('--skew-factors', type=float, nargs='+', default=DEFAULT_SKEW_FACTORS,
                   help=f'skew_factor values (default: {DEFAULT_SKEW_FACTORS})')
    p.add_argument('--backends', nargs='+', default=['hashmap', 'forl0'],
                   choices=['hashmap', 'forl0'],
                   help='Backends to run (default: hashmap forl0)')
    p.add_argument('--repeat', type=int, default=1,
                   help='Repeat each cell N times and average (default: 1)')
    p.add_argument('--cooldown', type=int, default=10,
                   help='Seconds between runs (default: 10)')
    p.add_argument('--resume', action='store_true',
                   help='Resume: skip cells already present in the latest JSON')
    p.add_argument('--no-plot', action='store_true', help='Skip plot generation')
    args = p.parse_args()

    base_config = load_config()
    raw_dir = get_results_dir('raw')
    fig_dir = get_results_dir('figures')

    num_keys_axis = sorted(set(int(n) for n in args.num_keys))
    skew_axis = sorted(set(float(s) for s in args.skew_factors))

    print(f'\nGrid: {len(num_keys_axis)} num_keys x {len(skew_axis)} skew_factors '
          f'= {len(num_keys_axis) * len(skew_axis)} cells, backends={args.backends}, '
          f'repeat={args.repeat}')

    # Load existing records (for resume)
    existing_records: List[dict] = []
    done_cells: set = set()
    if args.resume:
        existing_records = load_existing(raw_dir)
        done_cells = {cell_key(r['num_keys'], r['skew_factor']) for r in existing_records}
        print(f'Resuming: {len(existing_records)} cells already collected')

    records: List[dict] = list(existing_records)

    total_cells = len(num_keys_axis) * len(skew_axis)
    idx = 0
    for nk in num_keys_axis:
        for sk in skew_axis:
            idx += 1
            key = cell_key(nk, sk)
            if key in done_cells:
                print(f'\n[{idx}/{total_cells}] skipping cell num_keys={nk}, skew={sk} (done)')
                continue

            print(f'\n[{idx}/{total_cells}] running cell num_keys={nk}, skew={sk}')
            results = run_cell(base_config, nk, sk, args.backends,
                               args.repeat, args.cooldown)

            records.append({
                'num_keys': nk,
                'skew_factor': sk,
                'results': results,
            })

            # Save incrementally
            save_results(records, raw_dir)

            # Cooldown between cells
            if idx < total_cells:
                print(f'  Cooldown {args.cooldown}s ...')
                time.sleep(args.cooldown)

    # Final save
    save_results(records, raw_dir, suffix='_final')

    # Summary
    print_summary(records, args.backends)

    # Plot
    if not args.no_plot and records:
        plot_3d(records, num_keys_axis, skew_axis, args.backends, fig_dir)

    print('Done.')


if __name__ == '__main__':
    main()
