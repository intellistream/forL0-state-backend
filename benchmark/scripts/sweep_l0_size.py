#!/usr/bin/env python3
"""
Sweep L0 Cache size and plot WordCount throughput line chart.

This script:
1. Iterates over a range of L0 cache sizes
2. Runs WordCount benchmark for each size (forl0 backend)
3. Optionally runs a hashmap baseline once (for reference line)
4. Saves results to JSON
5. Plots throughput vs L0 cache size line chart

Usage:
    # On Kunpeng server with Docker Flink cluster running:
    python sweep_l0_size.py

    # Custom sizes (in MB):
    python sweep_l0_size.py --sizes 0 2 4 8 12 16 20 24

    # Include hashmap baseline:
    python sweep_l0_size.py --baseline

    # Skip plotting (just collect data):
    python sweep_l0_size.py --no-plot

    # Repeat each size N times and average:
    python sweep_l0_size.py --repeat 3

    # Resume from a previous run (skip already-collected sizes):
    python sweep_l0_size.py --resume
"""

import argparse
import copy
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, List

sys.path.insert(0, str(Path(__file__).parent))

from utils.config import load_config, get_results_dir, get_timestamp
from run_wordcount import run_wordcount

# Default L0 sizes to sweep (MB)
# 0 = L0 disabled, then 2MB increments up to hardware limit (~20MB per NUMA)
DEFAULT_SIZES_MB = [0, 2, 4, 8, 12, 16, 20, 24]


def make_config_with_l0_size(base_config: dict, l0_size_mb: int) -> dict:
    """Create a config copy with the specified L0 cache size.

    When l0_size_mb == 0, L0 cache is disabled entirely so the run
    exercises the pure-heap path (same SwissTable, no L0).
    """
    config = copy.deepcopy(base_config)

    # Ensure forl0 backend has a config block
    for b in config.get('backends', []):
        if b.get('name') == 'forl0':
            if 'config' not in b:
                b['config'] = {}
            if l0_size_mb == 0:
                b['config']['l0_cache_enabled'] = False
            else:
                b['config']['l0_cache_enabled'] = True
                b['config']['l0_cache_size'] = f'{l0_size_mb}mb'
            break

    return config


def run_single_point(config: dict, l0_size_mb: int) -> Optional[dict]:
    """Run WordCount once for a given L0 size and return the result dict."""
    label = f'{l0_size_mb}MB' if l0_size_mb > 0 else 'disabled'
    print(f'\n{"=" * 60}')
    print(f'  L0 Cache Size: {label}')
    print(f'{"=" * 60}')

    patched = make_config_with_l0_size(config, l0_size_mb)
    result = run_wordcount(patched, backend='forl0', profile_mode=None)

    if result:
        result['l0_cache_size_mb'] = l0_size_mb
        tpc = result.get('throughput_per_core', 0)
        print(f'\n  >>> L0={label}  throughput/core = {tpc:,.0f} rec/s/core')
    else:
        print(f'\n  >>> L0={label}  FAILED')

    return result


def save_sweep_results(all_results: list, baseline: Optional[dict], output_dir: Path):
    """Persist sweep results to a timestamped JSON file."""
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = get_timestamp()
    path = output_dir / f'l0_sweep_{ts}.json'

    payload = {
        'timestamp': ts,
        'sweep_results': all_results,
        'baseline': baseline,
    }
    with open(path, 'w') as f:
        json.dump(payload, f, indent=2, default=str)

    print(f'\nResults saved to {path}')
    return path


def plot_sweep(all_results: list, baseline: Optional[dict], output_dir: Path):
    """Generate throughput vs L0 cache size line chart."""
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
    except ImportError:
        print('WARNING: matplotlib not installed, skipping plot')
        print('  Install with: pip install matplotlib')
        return

    # --- Prepare data ---
    sizes = []
    throughputs = []
    for r in all_results:
        sizes.append(r['l0_cache_size_mb'])
        throughputs.append(r['throughput_per_core'])

    # Convert to M rec/s/core for readability
    throughputs_m = [t / 1e6 for t in throughputs]

    fig, ax = plt.subplots(figsize=(10, 6))

    # ForL0 line
    ax.plot(sizes, throughputs_m, 'o-', color='#2196F3', linewidth=2,
            markersize=8, label='ForL0 StateBackend')

    # Annotate each point
    for x, y in zip(sizes, throughputs_m):
        ax.annotate(f'{y:.2f}', (x, y),
                    textcoords='offset points', xytext=(0, 12),
                    ha='center', fontsize=9)

    # Baseline reference line
    if baseline and 'throughput_per_core' in baseline:
        bl = baseline['throughput_per_core'] / 1e6
        ax.axhline(y=bl, color='#FF5722', linestyle='--', linewidth=1.5,
                    label=f'HashMap baseline ({bl:.2f} M)')

    ax.set_xlabel('L0 Cache Size (MB)', fontsize=12)
    ax.set_ylabel('Throughput (M records/s/core)', fontsize=12)
    ax.set_title('WordCount Throughput vs L0 Cache Size', fontsize=14)
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    ax.set_xticks(sizes)

    # Save
    output_dir.mkdir(parents=True, exist_ok=True)
    for fmt in ['pdf', 'png']:
        path = output_dir / f'l0_sweep_throughput.{fmt}'
        fig.savefig(str(path), bbox_inches='tight', dpi=150)
        print(f'Figure saved: {path}')

    plt.close(fig)


def load_existing_results(output_dir: Path) -> list:
    """Load the most recent sweep result file for --resume."""
    files = sorted(output_dir.glob('l0_sweep_*.json'), reverse=True)
    if not files:
        return []
    with open(files[0]) as f:
        data = json.load(f)
    return data.get('sweep_results', [])


def main():
    parser = argparse.ArgumentParser(
        description='Sweep L0 Cache size and plot WordCount throughput')
    parser.add_argument('--sizes', type=int, nargs='+', default=DEFAULT_SIZES_MB,
                        help=f'L0 cache sizes in MB (default: {DEFAULT_SIZES_MB})')
    parser.add_argument('--baseline', action='store_true',
                        help='Run HashMapStateBackend baseline for reference line')
    parser.add_argument('--no-plot', action='store_true',
                        help='Skip plot generation')
    parser.add_argument('--repeat', type=int, default=1,
                        help='Repeat each size N times and average (default: 1)')
    parser.add_argument('--resume', action='store_true',
                        help='Resume from previous run, skip already-collected sizes')
    parser.add_argument('--cooldown', type=int, default=10,
                        help='Seconds to wait between runs (default: 10)')
    args = parser.parse_args()

    config = load_config()
    raw_dir = get_results_dir('raw')
    fig_dir = get_results_dir('figures')

    # Resume support
    existing = []
    if args.resume:
        existing = load_existing_results(raw_dir)
        done_sizes = {r['l0_cache_size_mb'] for r in existing}
        print(f'Resuming: {len(existing)} results loaded, sizes done: {sorted(done_sizes)}')
    else:
        done_sizes = set()

    # --- Baseline ---
    baseline = None
    if args.baseline:
        print('\n=== Running HashMap Baseline ===')
        baseline = run_wordcount(config, backend='hashmap', profile_mode=None)
        if baseline:
            tpc = baseline.get('throughput_per_core', 0)
            print(f'\n  >>> HashMap baseline = {tpc:,.0f} rec/s/core')
        time.sleep(args.cooldown)

    # --- Sweep ---
    all_results = list(existing)

    for size_mb in sorted(args.sizes):
        if size_mb in done_sizes:
            print(f'\n  Skipping L0={size_mb}MB (already collected)')
            continue

        runs = []
        for trial in range(args.repeat):
            if args.repeat > 1:
                print(f'\n  Trial {trial + 1}/{args.repeat}')

            result = run_single_point(config, size_mb)
            if result:
                runs.append(result)

            if trial < args.repeat - 1:
                print(f'  Cooldown {args.cooldown}s ...')
                time.sleep(args.cooldown)

        if not runs:
            print(f'  WARNING: All trials failed for L0={size_mb}MB')
            continue

        # Average if multiple trials
        if len(runs) > 1:
            avg_tpc = sum(r['throughput_per_core'] for r in runs) / len(runs)
            avg_tp = sum(r['throughput'] for r in runs) / len(runs)
            avg_time = sum(r['total_time_seconds'] for r in runs) / len(runs)
            best = max(runs, key=lambda r: r['throughput_per_core'])
            best['throughput_per_core'] = avg_tpc
            best['throughput'] = avg_tp
            best['total_time_seconds'] = avg_time
            best['num_trials'] = len(runs)
            best['trial_throughputs'] = [r['throughput_per_core'] for r in runs]
            all_results.append(best)
        else:
            all_results.append(runs[0])

        # Save incrementally (in case of interruption)
        save_sweep_results(all_results, baseline, raw_dir)

        # Cooldown between sizes
        if size_mb != sorted(args.sizes)[-1]:
            print(f'  Cooldown {args.cooldown}s ...')
            time.sleep(args.cooldown)

    # Sort by size for plotting
    all_results.sort(key=lambda r: r['l0_cache_size_mb'])

    # Final save
    result_path = save_sweep_results(all_results, baseline, raw_dir)

    # --- Summary table ---
    print(f'\n{"=" * 60}')
    print(f'  L0 Cache Size Sweep — Summary')
    print(f'{"=" * 60}')
    print(f'  {"L0 Size":>10}  {"Throughput/Core":>18}  {"Total Time":>12}')
    print(f'  {"-" * 10}  {"-" * 18}  {"-" * 12}')
    for r in all_results:
        sz = f'{r["l0_cache_size_mb"]}MB' if r['l0_cache_size_mb'] > 0 else 'disabled'
        tpc = r['throughput_per_core']
        t = r['total_time_seconds']
        print(f'  {sz:>10}  {tpc:>14,.0f} r/s  {t:>9.1f} s')
    if baseline:
        bl = baseline.get('throughput_per_core', 0)
        print(f'  {"HashMap":>10}  {bl:>14,.0f} r/s  (baseline)')
    print()

    # --- Plot ---
    if not args.no_plot and len(all_results) >= 2:
        plot_sweep(all_results, baseline, fig_dir)

    print('Done.')


if __name__ == '__main__':
    main()
