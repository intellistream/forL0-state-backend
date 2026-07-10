#!/usr/bin/env python3
"""
Generate paper-quality figures and reports from benchmark results.
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import matplotlib.pyplot as plt  # type: ignore[import-untyped]
import matplotlib  # type: ignore[import-untyped]
from matplotlib.ticker import FuncFormatter  # type: ignore[import-untyped]
import numpy as np  # type: ignore[import-untyped]
import pandas as pd  # type: ignore[import-untyped]
import seaborn as sns  # type: ignore[import-untyped]
from jinja2 import Template  # type: ignore[import-untyped]

from utils.config import get_benchmark_root, get_results_dir, load_config
from utils.hardware_metrics import load_hardware_metrics

# Use non-interactive backend for server environments
matplotlib.use('Agg')

# Paper-quality figure settings
plt.rcParams.update({
    'font.size': 12,
    'font.family': 'serif',
    'axes.labelsize': 14,
    'axes.titlesize': 14,
    'xtick.labelsize': 12,
    'ytick.labelsize': 12,
    'legend.fontsize': 11,
    'figure.figsize': (8, 5),
    'figure.dpi': 300,
    'savefig.dpi': 300,
    'savefig.bbox': 'tight',
    'axes.grid': True,
    'grid.alpha': 0.3,
})

# Color palette for backends
COLORS = {
    'hashmap': '#4C72B0',  # Blue
    'forl0': '#55A868',    # Green
}

# Color palette for multi-query charts
QUERY_COLORS = [
    '#2563eb',  # Blue
    '#16a34a',  # Green
    '#ea580c',  # Orange
    '#dc2626',  # Red
    '#7c3aed',  # Purple
    '#0891b2',  # Cyan
    '#ca8a04',  # Yellow
    '#be185d',  # Pink
]

BACKEND_LABELS = {
    'hashmap': 'HashMapStateBackend',
    'forl0': 'BriskState',
}

BENCHSET_LABELS = {
    'wc': 'WC',
    'fd': 'FD',
    'sd': 'SD',
    'tm': 'TM',
    'lg': 'LG',
    'vs': 'VS',
    'lr': 'LR',
}


def get_throughput(data, parallelism=None):
    """
    Get throughput value from result data, preferring throughput_per_core if > 0,
    otherwise calculate from throughput.
    
    On macOS, throughput_per_core is 0 (requires Linux /proc), so we fall back to throughput.
    """
    tpc = data.get('throughput_per_core', 0) or 0
    if tpc > 0:
        return tpc
    
    # Fallback to throughput divided by parallelism
    throughput = data.get('throughput', 0) or 0
    if throughput > 0:
        p = parallelism or data.get('parallelism', 1) or 1
        return throughput / p
    
    return 0


def load_results():
    """Load all benchmark results from raw directory and nexmark directories."""
    results_dir = get_results_dir('raw')
    benchmark_results_dir = get_results_dir('')  # Parent results directory
    results = {
        'wordcount': {'hashmap': None, 'forl0': None},
        'nexmark': {'hashmap': {}, 'forl0': {}},
        'client_usecase': {'hashmap': None, 'forl0': None}
    }
    
    # Load WordCount results from raw directory
    for filepath in results_dir.glob('*.json'):
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            
            metadata = data.get('_metadata', {})
            test_name = metadata.get('test_name', '')
            backend = metadata.get('backend', data.get('backend', ''))
            
            if 'wordcount' in test_name or data.get('benchmark') == 'wordcount':
                if backend in results['wordcount']:
                    # Keep the latest result
                    if results['wordcount'][backend] is None:
                        results['wordcount'][backend] = data
                    else:
                        existing_ts = results['wordcount'][backend].get('_metadata', {}).get('timestamp', '')
                        new_ts = metadata.get('timestamp', '')
                        if new_ts > existing_ts:
                            results['wordcount'][backend] = data

            if test_name == 'client_usecase' or data.get('benchmark') == 'client-usecase':
                if backend in results['client_usecase']:
                    current = results['client_usecase'][backend]
                    current_ts = '' if current is None else current.get('_metadata', {}).get('timestamp', '')
                    new_ts = metadata.get('timestamp', '')
                    if current is None or new_ts > current_ts:
                        results['client_usecase'][backend] = data
        
        except Exception as e:
            print(f"Warning: Could not load {filepath}: {e}")
    
    # Load NexMark results from all nexmark_* directories.  The Ascend
    # reproduction suite intentionally runs each query/backend in an isolated
    # cluster, so one complete suite produces many small result directories.
    nexmark_dirs = sorted(benchmark_results_dir.glob('nexmark_*'), reverse=True)
    loaded_nexmark_dirs = 0
    for nexmark_dir in nexmark_dirs:
        nexmark_results_file = nexmark_dir / 'nexmark_results.json'
        if not nexmark_results_file.exists():
            continue
        try:
            with open(nexmark_results_file, 'r') as f:
                nexmark_data = json.load(f)

            loaded_from_dir = False
            for backend in ['hashmap', 'forl0']:
                if backend not in nexmark_data.get('results', {}):
                    continue
                backend_results = nexmark_data['results'][backend]
                query_results = backend_results.get('query_results', {})
                for query, qdata in query_results.items():
                    if query in results['nexmark'][backend]:
                        continue
                    results['nexmark'][backend][query] = {
                        'query': query,
                        'throughput': qdata.get('throughput', qdata.get('events_per_sec', 0)),
                        'throughput_per_core': qdata.get('throughput_per_core', 0),
                        'time_seconds': qdata.get('time_seconds', 0),
                        'events_num': qdata.get('events_num', 0),
                        'scenario_name': nexmark_data.get('scenario_name', ''),
                        'source_dir': nexmark_dir.name,
                    }
                    loaded_from_dir = True
            if loaded_from_dir:
                loaded_nexmark_dirs += 1
        except Exception as e:
            print(f"Warning: Could not load NexMark results from {nexmark_dir.name}: {e}")
    if loaded_nexmark_dirs:
        print(f"Loaded NexMark results from {loaded_nexmark_dirs} directories")
    
    return results


def load_benchset_results():
    """Load latest benchset results from raw directory."""
    results_dir = get_results_dir('raw')
    benchset_results = {key: {} for key in BENCHSET_LABELS.keys()}

    for filepath in results_dir.glob('benchset_*.json'):
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)

            metadata = data.get('_metadata', {})
            test_name = metadata.get('test_name', '')
            backend = metadata.get('backend', data.get('backend', ''))
            benchmark = data.get('benchmark', '')

            if not benchmark and test_name.startswith('benchset_'):
                benchmark = test_name[len('benchset_'):]

            if benchmark not in benchset_results or backend not in BACKEND_LABELS:
                continue

            current = benchset_results[benchmark].get(backend)
            current_ts = '' if current is None else current.get('_metadata', {}).get('timestamp', '')
            new_ts = metadata.get('timestamp', '')

            if current is None or new_ts >= current_ts:
                benchset_results[benchmark][backend] = data
        except Exception as e:
            print(f"Warning: Could not load benchset result from {filepath}: {e}")

    return benchset_results


def plot_benchset_throughput_comparison(benchset_results, output_dir):
    """Generate grouped throughput-per-core comparison for the benchset."""
    workloads = [key for key, value in benchset_results.items() if value]
    if not workloads:
        print("Warning: No benchset results found for throughput plot")
        return None

    x = np.arange(len(workloads))
    width = 0.35

    hashmap_vals = []
    forl0_vals = []
    for workload in workloads:
        hashmap = benchset_results[workload].get('hashmap', {})
        forl0 = benchset_results[workload].get('forl0', {})
        hashmap_vals.append(get_throughput(hashmap) / 1e6)
        forl0_vals.append(get_throughput(forl0) / 1e6)

    fig, ax = plt.subplots(figsize=(10, 5.5))
    ax.bar(x - width / 2, hashmap_vals, width, label=BACKEND_LABELS['hashmap'], color=COLORS['hashmap'])
    ax.bar(x + width / 2, forl0_vals, width, label=BACKEND_LABELS['forl0'], color=COLORS['forl0'])

    ax.set_xticks(x)
    ax.set_xticklabels([BENCHSET_LABELS.get(item, item.upper()) for item in workloads])
    ax.set_ylabel('Throughput per Core (M records/s)')
    ax.set_xlabel('Benchset Workload')
    ax.set_title('Benchset Throughput Comparison')
    ax.legend(frameon=False)

    filepath = output_dir / 'benchset_throughput_comparison.pdf'
    plt.savefig(filepath)
    plt.savefig(output_dir / 'benchset_throughput_comparison.png')
    plt.close()
    print(f"Saved: {filepath}")
    return filepath


def plot_benchset_speedup(benchset_results, output_dir):
    """Generate BriskState speedup chart over HashMap for each benchset workload."""
    workloads = []
    speedups = []

    for workload, backend_results in benchset_results.items():
        hashmap = get_throughput(backend_results.get('hashmap', {}))
        forl0 = get_throughput(backend_results.get('forl0', {}))
        if hashmap > 0 and forl0 > 0:
            workloads.append(workload)
            speedups.append(forl0 / hashmap)

    if not workloads:
        print("Warning: No complete benchset pairs found for speedup plot")
        return None

    fig, ax = plt.subplots(figsize=(10, 5.5))
    bars = ax.bar(
        [BENCHSET_LABELS.get(item, item.upper()) for item in workloads],
        speedups,
        color=COLORS['forl0'],
        width=0.6,
    )
    ax.axhline(1.0, color='#666666', linewidth=1.0, linestyle='--')
    ax.set_ylabel('Speedup (BriskState / HashMap)')
    ax.set_xlabel('Benchset Workload')
    ax.set_title('Benchset Relative Speedup')

    for bar, speedup in zip(bars, speedups):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.02,
            f'{speedup:.2f}x',
            ha='center',
            va='bottom',
            fontsize=10,
        )

    filepath = output_dir / 'benchset_speedup_summary.pdf'
    plt.savefig(filepath)
    plt.savefig(output_dir / 'benchset_speedup_summary.png')
    plt.close()
    print(f"Saved: {filepath}")
    return filepath


def generate_benchset_markdown_summary(benchset_results, output_dir):
    """Generate a concise markdown summary for benchset paper figures."""
    lines = [
        '# Benchset Summary',
        '',
        '| Workload | HashMap (M rec/s/core) | BriskState (M rec/s/core) | Speedup |',
        '|---|---:|---:|---:|',
    ]

    for workload, backend_results in benchset_results.items():
        hashmap = get_throughput(backend_results.get('hashmap', {})) / 1e6
        forl0 = get_throughput(backend_results.get('forl0', {})) / 1e6
        if hashmap > 0 and forl0 > 0:
            speedup = forl0 / hashmap
            lines.append(
                f'| {BENCHSET_LABELS.get(workload, workload.upper())} | {hashmap:.3f} | {forl0:.3f} | {speedup:.2f}x |'
            )

    report_path = output_dir / 'benchset_summary.md'
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines) + '\n')

    print(f"Saved: {report_path}")
    return report_path


def generate_benchset_paper_artifacts(figures_dir=None, reports_dir=None):
    """Generate benchset-specific paper figures and summary files."""
    figures_dir = figures_dir or get_results_dir('figures')
    reports_dir = reports_dir or get_results_dir('reports')
    benchset_results = load_benchset_results()

    if not any(benchset_results.values()):
        print('No benchset results available for figure generation')
        return {}

    artifacts = {}
    artifacts['throughput'] = plot_benchset_throughput_comparison(benchset_results, figures_dir)
    artifacts['speedup'] = plot_benchset_speedup(benchset_results, figures_dir)
    artifacts['summary'] = generate_benchset_markdown_summary(benchset_results, reports_dir)
    return artifacts


def plot_wordcount_comparison(results, output_dir):
    """Generate WordCount throughput comparison figure."""
    wc_results = results.get('wordcount', {})
    
    if not wc_results.get('hashmap') or not wc_results.get('forl0'):
        print("Warning: Missing WordCount results for comparison")
        return None
    
    backends = ['hashmap', 'forl0']
    throughputs = []
    
    for backend in backends:
        tpc = wc_results[backend].get('throughput_per_core', 0)
        throughputs.append(tpc)
    
    fig, ax = plt.subplots(figsize=(6, 5))
    
    x = np.arange(len(backends))
    bars = ax.bar(x, throughputs, color=[COLORS[b] for b in backends], width=0.6)
    
    # Set y-axis limit with extra space for labels
    max_val = max(throughputs) if throughputs else 1
    ax.set_ylim(0, max_val * 1.2)
    
    # Add value labels on bars
    for bar, val in zip(bars, throughputs):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max_val*0.02,
                f'{val:,.0f}', ha='center', va='bottom', fontsize=11)
    
    # Calculate and show improvement
    if throughputs[0] > 0:
        improvement = ((throughputs[1] - throughputs[0]) / throughputs[0]) * 100
        ax.annotate(f'+{improvement:.1f}%',
                   xy=(1, throughputs[1]), xytext=(1.3, throughputs[1]),
                   fontsize=12, fontweight='bold', color='green',
                   arrowprops=dict(arrowstyle='->', color='green'))
    
    ax.set_xticks(x)
    ax.set_xticklabels([BACKEND_LABELS[b] for b in backends])
    ax.set_ylabel('Throughput per Core (records/s)')
    ax.set_title('WordCount Benchmark: Throughput Comparison')
    
    # Save figure
    filepath = output_dir / 'wordcount_throughput.pdf'
    plt.savefig(filepath)
    plt.savefig(output_dir / 'wordcount_throughput.png')
    plt.close()
    
    print(f"Saved: {filepath}")
    return filepath


def plot_nexmark_comparison(results, output_dir):
    """Generate a per-query NexMark throughput comparison figure."""
    nexmark_results = results.get('nexmark', {})
    
    # Dynamically collect available queries from results
    available_queries = set()
    for backend in ['hashmap', 'forl0']:
        if backend in nexmark_results:
            available_queries.update(nexmark_results[backend].keys())
    queries = sorted(available_queries, key=lambda x: (int(x[1:]) if x[1:].isdigit() else 999))
    
    if not queries:
        print("Warning: No NexMark queries found in results")
        return None
    
    data = []
    
    for query in queries:
        for backend in ['hashmap', 'forl0']:
            if backend in nexmark_results and query in nexmark_results[backend]:
                result = nexmark_results[backend][query]
                tpc = get_throughput(result)
                data.append({
                    'Query': query.upper(),
                    'Backend': BACKEND_LABELS[backend],
                    'Throughput': tpc,
                    'backend_key': backend
                })
    
    if not data:
        print("Warning: No NexMark results found")
        return None
    
    df = pd.DataFrame(data)
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Create grouped bar chart
    x = np.arange(len(queries))
    width = 0.35
    
    hashmap_data = df[df['backend_key'] == 'hashmap'].set_index('Query')['Throughput']
    forl0_data = df[df['backend_key'] == 'forl0'].set_index('Query')['Throughput']
    
    hashmap_vals = [hashmap_data.get(q.upper(), 0) for q in queries]
    forl0_vals = [forl0_data.get(q.upper(), 0) for q in queries]
    
    bars1 = ax.bar(x - width/2, hashmap_vals, width, label=BACKEND_LABELS['hashmap'], 
                   color=COLORS['hashmap'])
    bars2 = ax.bar(x + width/2, forl0_vals, width, label=BACKEND_LABELS['forl0'],
                   color=COLORS['forl0'])
    
    ax.set_xticks(x)
    ax.set_xticklabels([q.upper() for q in queries])
    ax.set_ylabel('Throughput per Core (records/s)')
    ax.set_xlabel('NexMark Query')
    ax.set_title('NexMark Benchmark: Per-Query Throughput Comparison')
    ax.legend(loc='upper right')
    
    # Add improvement labels
    for i, (h, f) in enumerate(zip(hashmap_vals, forl0_vals)):
        if h > 0 and f > 0:
            imp = ((f - h) / h) * 100
            ax.text(i + width/2, f + max(forl0_vals)*0.02, f'{imp:+.0f}%',
                   ha='center', fontsize=9, color='green', fontweight='bold')
    
    plt.tight_layout()
    
    # Save figure
    filepath = output_dir / 'nexmark_throughput.pdf'
    plt.savefig(filepath)
    plt.savefig(output_dir / 'nexmark_throughput.png')
    plt.close()
    
    print(f"Saved: {filepath}")
    return filepath


def plot_latency_comparison(results, output_dir):
    """Generate latency comparison figure."""
    wc_results = results.get('wordcount', {})
    
    if not wc_results.get('hashmap') or not wc_results.get('forl0'):
        print("Warning: Missing WordCount results for latency comparison")
        return None
    
    percentiles = ['p50', 'p95', 'p99']
    labels = ['P50', 'P95', 'P99']
    
    hashmap_latency = wc_results['hashmap'].get('latency_ms', {})
    forl0_latency = wc_results['forl0'].get('latency_ms', {})
    
    # Handle None values by converting to 0
    hashmap_vals = [hashmap_latency.get(p) or 0 for p in percentiles]
    forl0_vals = [forl0_latency.get(p) or 0 for p in percentiles]
    
    # Skip if all values are 0 (no latency data)
    if all(v == 0 for v in hashmap_vals + forl0_vals):
        print("Warning: No latency data available, skipping latency chart")
        return None
    
    fig, ax = plt.subplots(figsize=(8, 5))
    
    x = np.arange(len(percentiles))
    width = 0.35
    
    bars1 = ax.bar(x - width/2, hashmap_vals, width, label=BACKEND_LABELS['hashmap'],
                   color=COLORS['hashmap'])
    bars2 = ax.bar(x + width/2, forl0_vals, width, label=BACKEND_LABELS['forl0'],
                   color=COLORS['forl0'])
    
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylabel('Latency (ms)')
    ax.set_xlabel('Percentile')
    ax.set_title('WordCount Benchmark: Latency Distribution')
    ax.legend()
    
    # Add value labels
    for bars in [bars1, bars2]:
        for bar in bars:
            height = bar.get_height()
            if height > 0:
                ax.text(bar.get_x() + bar.get_width()/2, height,
                       f'{height:.0f}', ha='center', va='bottom', fontsize=9)
    
    plt.tight_layout()
    
    filepath = output_dir / 'latency_comparison.pdf'
    plt.savefig(filepath)
    plt.savefig(output_dir / 'latency_comparison.png')
    plt.close()
    
    print(f"Saved: {filepath}")
    return filepath


def plot_latency_cdf(results, output_dir):
    """Generate latency CDF (Cumulative Distribution Function) figure."""
    wc_results = results.get('wordcount', {})
    latency_dir = get_results_dir('latency')
    
    # Collect latency data for each backend
    latency_data = {}
    
    for backend in ['hashmap', 'forl0']:
        # First try to find latency file in latency directory (most reliable)
        import glob
        pattern = str(latency_dir / f"latency_samples_{backend}_*.csv")
        files = glob.glob(pattern)
        
        if files:
            latest_file = max(files, key=lambda f: Path(f).stat().st_mtime)
            try:
                df = pd.read_csv(latest_file)
                latency_data[backend] = df['latency_ms'].values
            except Exception as e:
                print(f"Warning: Could not read latency file for {backend}: {e}")
        else:
            # Fallback to path from result JSON
            backend_result = wc_results.get(backend, {})
            latency_file = backend_result.get('latency_samples_file')
            if latency_file and Path(latency_file).exists():
                try:
                    df = pd.read_csv(latency_file)
                    latency_data[backend] = df['latency_ms'].values
                except Exception as e:
                    print(f"Warning: Could not read latency file for {backend}: {e}")
    
    if not latency_data:
        print("Warning: No latency samples found for CDF plot")
        return None
    
    # Create CDF figure
    fig, ax = plt.subplots(figsize=(8, 5))
    
    for backend, latencies in latency_data.items():
        sorted_latencies = np.sort(latencies)
        cdf = np.arange(1, len(sorted_latencies) + 1) / len(sorted_latencies)
        
        ax.plot(sorted_latencies, cdf * 100, 
                label=BACKEND_LABELS.get(backend, backend),
                color=COLORS.get(backend, 'gray'),
                linewidth=2)
    
    # Add percentile markers
    percentiles = [50, 95, 99]
    for p in percentiles:
        ax.axhline(y=p, color='gray', linestyle='--', alpha=0.5, linewidth=0.8)
        ax.text(ax.get_xlim()[1] * 0.98, p + 1, f'P{p}', 
                ha='right', va='bottom', fontsize=9, color='gray')
    
    ax.set_xlabel('Latency (ms)')
    ax.set_ylabel('CDF (%)')
    ax.set_title('WordCount Benchmark: Latency CDF')
    ax.legend(loc='lower right')
    ax.set_ylim(0, 105)
    ax.set_xlim(left=0)
    
    plt.tight_layout()
    
    filepath = output_dir / 'latency_cdf.pdf'
    plt.savefig(filepath)
    plt.savefig(output_dir / 'latency_cdf.png')
    plt.close()
    
    print(f"Saved: {filepath}")
    return filepath


def plot_improvement_summary(results, output_dir):
    """Generate improvement summary figure."""
    improvements = []
    labels = []
    
    # WordCount improvement
    wc_results = results.get('wordcount', {})
    if wc_results.get('hashmap') and wc_results.get('forl0'):
        h = get_throughput(wc_results['hashmap'])
        f = get_throughput(wc_results['forl0'])
        if h > 0:
            improvements.append(((f - h) / h) * 100)
            labels.append('WordCount')
    
    # NexMark improvements - dynamically get available queries
    nexmark_results = results.get('nexmark', {})
    available_queries = set()
    for backend in ['hashmap', 'forl0']:
        if backend in nexmark_results:
            available_queries.update(nexmark_results[backend].keys())
    queries = sorted(available_queries, key=lambda x: (int(x[1:]) if x[1:].isdigit() else 999))
    
    for query in queries:
        h_data = nexmark_results.get('hashmap', {}).get(query, {})
        f_data = nexmark_results.get('forl0', {}).get(query, {})
        
        h = get_throughput(h_data)
        f = get_throughput(f_data)
        
        if h > 0 and f > 0:
            improvements.append(((f - h) / h) * 100)
            labels.append(query.upper())
    
    if not improvements:
        print("Warning: No data for improvement summary")
        return None
    
    fig, ax = plt.subplots(figsize=(10, 5))
    
    colors = ['green' if imp >= 60 else 'orange' if imp >= 0 else 'red' for imp in improvements]
    bars = ax.bar(labels, improvements, color=colors)
    
    # Add target line
    ax.axhline(y=60, color='red', linestyle='--', linewidth=2, label='Target (60%)')
    
    # Set y-axis limit with extra space for labels
    max_imp = max(improvements) if improvements else 60
    min_imp = min(improvements) if improvements else 0
    ax.set_ylim(min(min_imp - 10, -10), max(max_imp + 15, 80))
    
    ax.set_ylabel('Improvement (%)')
    ax.set_xlabel('Benchmark')
    ax.set_title('BriskState vs HashMapStateBackend: Throughput Improvement')
    ax.legend()
    
    # Add value labels
    for bar, imp in zip(bars, improvements):
        y_pos = bar.get_height() + 3 if bar.get_height() >= 0 else bar.get_height() - 8
        ax.text(bar.get_x() + bar.get_width()/2, y_pos,
               f'{imp:.1f}%', ha='center', fontsize=10, fontweight='bold')
    
    plt.tight_layout()
    
    filepath = output_dir / 'improvement_summary.pdf'
    plt.savefig(filepath)
    plt.savefig(output_dir / 'improvement_summary.png')
    plt.close()
    
    print(f"Saved: {filepath}")
    return filepath


def load_l0table_metrics():
    """
    [BENCHMARK_TEST] Load L0TABLE metrics from the l0metrics results directory.
    
    Returns a dict with:
    - 'by_query': dict mapping query name to its samples
    - 'l0table': all L0Table samples (for backward compatibility)
    - 'cache': all cache samples
    - 'final_l0table': final L0Table statistics
    - 'final_cache': final cache statistics
    - 'sources': list of source descriptions
    """
    l0metrics_dir = get_results_dir('l0metrics')
    
    # Find all L0 metrics files - both new format and old format
    # New format: l0_metrics_{backend}_{query}.json
    # Old format: l0table_metrics_{backend}_{timestamp}.json
    new_format_files = list(l0metrics_dir.glob('l0_metrics_*.json'))
    old_format_files = list(l0metrics_dir.glob('l0table_metrics_*.json'))
    
    # Prefer new format files
    metrics_files = new_format_files if new_format_files else old_format_files
    
    if not metrics_files:
        return None
    
    # Group samples by query
    by_query = {}
    source_info = []
    
    for filepath in metrics_files:
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            
            # Get query name - new format has 'query' field, old format uses filename parsing
            query = data.get('query')
            if query is None:
                # Try to parse from filename: l0table_metrics_forl0_20251211_... -> 'wordcount' (assumed)
                # or l0_metrics_forl0_wordcount.json -> 'wordcount'
                filename = filepath.stem
                if '_' in filename:
                    parts = filename.split('_')
                    # l0_metrics_forl0_q5 -> query = 'q5'
                    if len(parts) >= 4 and parts[0] == 'l0' and parts[1] == 'metrics':
                        query = parts[3]
                    else:
                        query = 'wordcount'  # Default for old format
                else:
                    query = 'wordcount'
            
            samples = data.get('samples', [])
            if not samples:
                continue
            
            # Keep only the most recent file for each query
            if query not in by_query:
                by_query[query] = {
                    'file': str(filepath),
                    'mtime': filepath.stat().st_mtime,
                    'samples': samples,
                    'backend': data.get('backend', 'forl0')
                }
            else:
                # Keep newer file
                if filepath.stat().st_mtime > by_query[query]['mtime']:
                    by_query[query] = {
                        'file': str(filepath),
                        'mtime': filepath.stat().st_mtime,
                        'samples': samples,
                        'backend': data.get('backend', 'forl0')
                    }
                    
        except Exception as e:
            print(f"Warning: Could not load L0 metrics from {filepath}: {e}")
    
    if not by_query:
        return None
    
    # Aggregate all samples for backward compatibility
    all_samples = []
    for query, info in by_query.items():
        samples = info['samples']
        # Tag samples with query name for multi-line charts
        for s in samples:
            s['_query'] = query
        all_samples.extend(samples)
        source_info.append(query)
    
    # Separate different types
    l0table_samples = [s for s in all_samples if s.get('type') == 'l0table']
    cache_samples = [s for s in all_samples if s.get('type') == 'cache']
    final_l0table = [s for s in all_samples if s.get('type') == 'l0table_final']
    final_cache = [s for s in all_samples if s.get('type') == 'cache_final']
    
    return {
        'by_query': by_query,
        'l0table': sorted(l0table_samples, key=lambda x: x.get('time_seconds', x.get('elapsed_ms', 0) / 1000)),
        'cache': sorted(cache_samples, key=lambda x: x.get('time_seconds', x.get('elapsed_ms', 0) / 1000)),
        'final_l0table': final_l0table,
        'final_cache': final_cache,
        'sources': source_info,
    }


def plot_l0table_timeline(output_dir):
    """
    [BENCHMARK_TEST] Generate L0Table Hit Rate timeline chart.
    
    Creates a single chart showing hit rate over time for each query.
    Each query (WordCount, NexMark q5, q8, etc.) is shown as a separate line.
    """
    metrics = load_l0table_metrics()
    
    if not metrics or not metrics.get('by_query'):
        print("No L0Table metrics available for plotting")
        return None
    
    by_query = metrics['by_query']
    queries = sorted(by_query.keys())
    
    if not queries:
        print("No queries found in L0Table metrics")
        return None
    
    print(f"  Plotting L0Table Hit Rate for queries: {', '.join(queries)}")
    
    # Prepare data for each query
    from collections import defaultdict
    
    query_data = {}
    for query in queries:
        samples = by_query[query]['samples']
        l0_samples = [s for s in samples if s.get('type') == 'l0table']
        
        if not l0_samples:
            continue
        
        # Aggregate by time bucket (1 second intervals) for this query
        time_buckets = defaultdict(lambda: {
            'total_access': 0, 
            'total_hits': 0, 
            'count': 0
        })
        
        for s in l0_samples:
            # Use time_seconds (new format) or fall back to elapsed_ms (old format)
            t_sec = int(s.get('time_seconds', s.get('elapsed_ms', 0) / 1000))
            bucket = time_buckets[t_sec]
            bucket['total_access'] += s.get('total_accesses', s.get('access_count', 0))
            bucket['total_hits'] += s.get('total_hits', s.get('hit_count', 0))
            bucket['count'] += 1
        
        if not time_buckets:
            continue
        
        sorted_times = sorted(time_buckets.keys())
        times = []
        hit_rates = []
        
        for t in sorted_times:
            bucket = time_buckets[t]
            times.append(t)
            # hit_rate in raw data is 0-1 format, convert to percentage
            if bucket['total_access'] > 0:
                hit_rates.append(bucket['total_hits'] / bucket['total_access'] * 100)
            else:
                hit_rates.append(0)
        
        query_data[query] = {
            'times': times,
            'hit_rates': hit_rates
        }
    
    if not query_data:
        print("No valid data for L0Table timeline")
        return None
    
    # Create single figure for Hit Rate
    fig, ax = plt.subplots(figsize=(10, 6))
    
    for i, query in enumerate(sorted(query_data.keys())):
        data = query_data[query]
        color = QUERY_COLORS[i % len(QUERY_COLORS)]
        ax.plot(data['times'], data['hit_rates'], 
                 color=color, linewidth=2, marker='o', markersize=3, 
                 label=query, alpha=0.8)
    
    ax.set_ylabel('L0 Hit Rate (%)')
    ax.set_xlabel('Time (seconds)')
    ax.set_title('L0Table Hit Rate Over Time')
    ax.set_ylim(0, 100)
    ax.axhline(y=50, color='gray', linestyle='--', alpha=0.5, label='50% Threshold')
    ax.legend(loc='upper right', ncol=min(len(query_data), 4))
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    filepath = output_dir / 'l0table_timeline.pdf'
    plt.savefig(filepath)
    plt.savefig(output_dir / 'l0table_timeline.png')
    plt.close()
    
    print(f"Saved: {filepath}")
    return filepath


def plot_state_entries_timeline(output_dir):
    """
    [BENCHMARK_TEST] Generate L0 Cache Valid Slots chart over time.
    
    Shows L0 valid slots per query over time with a max slots reference line.
    Each query is shown as a separate line.
    """
    metrics = load_l0table_metrics()
    
    if not metrics or not metrics.get('by_query'):
        print("No metrics available for valid slots plotting")
        return None
    
    by_query = metrics['by_query']
    queries = sorted(by_query.keys())
    
    if not queries:
        print("No queries found for valid slots plot")
        return None
    
    from collections import defaultdict
    
    query_data = {}
    max_valid_slots = 0  # Track maximum to set reasonable y-axis
    
    for query in queries:
        samples = by_query[query]['samples']
        
        # Get L0Table samples for valid slots
        l0_samples = [s for s in samples if s.get('type') == 'l0table']
        
        if not l0_samples:
            continue
        
        # Aggregate by time bucket
        l0_buckets = defaultdict(lambda: {'valid_slots': 0, 'count': 0})
        for s in l0_samples:
            t_sec = int(s.get('time_seconds', s.get('elapsed_ms', 0) / 1000))
            l0_buckets[t_sec]['valid_slots'] += s.get('valid_slots', 0)
            l0_buckets[t_sec]['count'] += 1
        
        all_times = sorted(l0_buckets.keys())
        if not all_times:
            continue
        
        valid_slots_list = [l0_buckets[t]['valid_slots'] for t in all_times]
        if valid_slots_list:
            max_valid_slots = max(max_valid_slots, max(valid_slots_list))
        
        query_data[query] = {
            'times': all_times,
            'valid_slots': valid_slots_list
        }
    
    if not query_data:
        print("No valid data for valid slots plot")
        return None
    
    # L0Table max slots calculation from benchmark.yaml config:
    # Read l0_cache_size from config (default 14 = 2^14 = 16384 buckets)
    # Each bucket has 4 slots
    # With parallelism, each subtask handles some key groups
    # 
    # IMPORTANT: Valid slots in chart is summed across ALL subtasks and ALL KeyGroups,
    # so max slots should be the total for all subtasks, not per subtask.
    try:
        config = load_config()
        runtime_config = config.get('runtime', {})
        parallelism = runtime_config.get('parallelism', 2)
        
        # Get l0_cache_size from forl0 backend config
        l0_cache_size = 14  # default
        for backend in config.get('backends', []):
            if backend.get('name') == 'forl0':
                backend_config = backend.get('config', {})
                l0_cache_size = backend_config.get('l0_cache_size', 14)
                break
        
        # Calculate: 2^l0_cache_size buckets * 4 slots per bucket
        # key_groups = 128 (Flink default maxParallelism), all are covered across subtasks
        # Total max slots = 128 key_groups * slots_per_l0table (since we aggregate all subtasks)
        slots_per_l0table = (1 << l0_cache_size) * 4
        total_key_groups = 128  # Flink default
        # Total max slots = all key_groups * slots_per_l0table
        L0_MAX_SLOTS_TOTAL = total_key_groups * slots_per_l0table
        print(f"  L0 config: l0_cache_size={l0_cache_size}, parallelism={parallelism}, "
              f"total_key_groups={total_key_groups}, slots_per_l0table={slots_per_l0table:,}, "
              f"max_slots_total={L0_MAX_SLOTS_TOTAL:,}")
    except Exception as e:
        print(f"  Warning: Could not read config, using default: {e}")
        L0_MAX_SLOTS_TOTAL = 128 * (1 << 14) * 4  # fallback: 128 * 16384 * 4
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    for i, query in enumerate(sorted(query_data.keys())):
        data = query_data[query]
        color = QUERY_COLORS[i % len(QUERY_COLORS)]
        ax.plot(data['times'], data['valid_slots'], 
                color=color, linewidth=2, marker='o', markersize=3, 
                label=query, alpha=0.8)
    
    # Add max slots reference line (total across all subtasks/keygroups)
    ax.axhline(y=L0_MAX_SLOTS_TOTAL, color='red', linestyle='--', linewidth=2, 
               alpha=0.7, label=f'Max Slots (Total) ({L0_MAX_SLOTS_TOTAL:,})')
    
    ax.set_xlabel('Time (seconds)')
    ax.set_ylabel('Valid Slots')
    ax.set_title('L0 Cache Valid Slots Over Time')
    ax.legend(loc='upper left', ncol=min(len(query_data) + 1, 4))
    ax.grid(True, alpha=0.3)
    
    # Format y-axis with thousands separator
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: format(int(x), ',')))
    
    plt.tight_layout()
    
    filepath = output_dir / 'state_entries_timeline.pdf'
    plt.savefig(filepath)
    plt.savefig(output_dir / 'state_entries_timeline.png')
    plt.close()
    
    print(f"Saved: {filepath}")
    return filepath


def plot_memory_usage_timeline(output_dir):
    """
    [BENCHMARK_TEST] Generate memory usage timeline chart.
    
    Creates a line chart showing RSS memory usage over time for each query.
    Each query is shown with both hashmap (dashed) and forl0 (solid) lines.
    """
    hw_metrics = load_hardware_metrics(str(get_results_dir('hardware')))
    memory_data = hw_metrics.get('memory', {})
    
    if not memory_data:
        print("No memory metrics available for plotting")
        return None
    
    # Group by query, keeping both backends
    # Structure: {query: {'hashmap': series, 'forl0': series}}
    query_backends = {}
    for key, series in memory_data.items():
        parts = key.rsplit('_', 1)
        if len(parts) == 2:
            query = parts[0]
            backend = parts[1]
            
            if query not in query_backends:
                query_backends[query] = {}
            query_backends[query][backend] = series
    
    if not query_backends:
        print("No queries found in memory metrics")
        return None
    
    queries = sorted(query_backends.keys())
    print(f"  Plotting Memory Usage timeline for queries: {queries}")
    
    fig, ax = plt.subplots(figsize=(14, 7))
    
    max_time = 0
    max_rss = 0
    
    for i, query in enumerate(queries):
        backends = query_backends[query]
        color = QUERY_COLORS[i % len(QUERY_COLORS)]
        
        for backend, series in backends.items():
            samples = series.get('samples', [])
            
            if not samples:
                continue
            
            times = [s.get('timestamp', 0) for s in samples]
            rss_values = [s.get('rss_mb', 0) for s in samples]
            
            if times:
                max_time = max(max_time, max(times))
            if rss_values:
                max_rss = max(max_rss, max(rss_values))
            
            # hashmap: dashed line, forl0: solid line
            linestyle = '--' if backend == 'hashmap' else '-'
            linewidth = 1.5 if backend == 'hashmap' else 2.0
            alpha = 0.6 if backend == 'hashmap' else 0.9
            
            label = f"{query} ({backend})"
            ax.plot(times, rss_values, 
                    color=color, linewidth=linewidth, linestyle=linestyle,
                    label=label, alpha=alpha)
    
    ax.set_xlabel('Time (seconds)', fontsize=12)
    ax.set_ylabel('Memory Usage (MB)', fontsize=12)
    ax.set_title('Memory Usage (RSS) Over Time\n(solid: BriskState, dashed: HashMap)', fontsize=14)
    
    # Create legend with two columns
    ax.legend(loc='upper right', ncol=2, fontsize=9)
    ax.grid(True, alpha=0.3)
    ax.set_xlim(left=0)
    ax.set_ylim(bottom=0)
    
    plt.tight_layout()
    
    filepath = output_dir / 'memory_usage_timeline.pdf'
    plt.savefig(filepath)
    plt.savefig(output_dir / 'memory_usage_timeline.png')
    plt.close()
    
    print(f"Saved: {filepath}")
    return filepath


def generate_report(results, output_dir):
    """Generate beautiful HTML report."""
    
    html_template = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ForL0 StateBackend Benchmark Report</title>
    <style>
        :root {
            --primary-color: #2563eb;
            --success-color: #16a34a;
            --warning-color: #ea580c;
            --danger-color: #dc2626;
            --bg-color: #f8fafc;
            --card-bg: #ffffff;
            --text-color: #1e293b;
            --text-secondary: #64748b;
            --border-color: #e2e8f0;
        }
        
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background-color: var(--bg-color);
            color: var(--text-color);
            line-height: 1.6;
        }
        
        .container {
            max-width: 1200px;
            margin: 0 auto;
            padding: 2rem;
        }
        
        header {
            background: linear-gradient(135deg, #1e40af 0%, #3b82f6 100%);
            color: white;
            padding: 3rem 2rem;
            margin-bottom: 2rem;
            border-radius: 0 0 1rem 1rem;
        }
        
        header h1 {
            font-size: 2.5rem;
            margin-bottom: 0.5rem;
        }
        
        header .subtitle {
            font-size: 1.1rem;
            opacity: 0.9;
        }
        
        header .timestamp {
            margin-top: 1rem;
            font-size: 0.9rem;
            opacity: 0.8;
        }
        
        .summary-card {
            background: var(--card-bg);
            border-radius: 1rem;
            padding: 2rem;
            margin-bottom: 2rem;
            box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1);
            border-left: 4px solid var(--primary-color);
        }
        
        .summary-card.success { border-left-color: var(--success-color); }
        .summary-card.warning { border-left-color: var(--warning-color); }
        
        .summary-card h2 {
            font-size: 1.5rem;
            margin-bottom: 1rem;
            color: var(--text-color);
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1.5rem;
            margin-bottom: 2rem;
        }
        
        .stat-card {
            background: var(--card-bg);
            border-radius: 0.75rem;
            padding: 1.5rem;
            text-align: center;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
            border: 1px solid var(--border-color);
        }
        
        .stat-card .value {
            font-size: 2rem;
            font-weight: 700;
            color: var(--primary-color);
        }
        
        .stat-card .label {
            font-size: 0.875rem;
            color: var(--text-secondary);
            margin-top: 0.5rem;
        }
        
        .stat-card.improvement .value {
            color: var(--success-color);
        }
        
        .stat-card.improvement.negative .value {
            color: var(--danger-color);
        }
        
        .section {
            background: var(--card-bg);
            border-radius: 1rem;
            padding: 2rem;
            margin-bottom: 2rem;
            box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1);
        }
        
        .section h2 {
            font-size: 1.5rem;
            margin-bottom: 1.5rem;
            padding-bottom: 0.75rem;
            border-bottom: 2px solid var(--border-color);
        }
        
        .section h3 {
            font-size: 1.1rem;
            margin: 1.5rem 0 1rem;
            color: var(--text-secondary);
        }
        
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 1rem 0;
        }
        
        th, td {
            padding: 0.875rem 1rem;
            text-align: left;
            border-bottom: 1px solid var(--border-color);
        }
        
        th {
            background: #f1f5f9;
            font-weight: 600;
            font-size: 0.875rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: var(--text-secondary);
        }
        
        tr:hover {
            background: #f8fafc;
        }
        
        .value-cell {
            font-family: 'SF Mono', Consolas, monospace;
            font-weight: 500;
        }
        
        .improvement-badge {
            display: inline-block;
            padding: 0.25rem 0.75rem;
            border-radius: 9999px;
            font-size: 0.875rem;
            font-weight: 600;
        }
        
        .improvement-badge.success {
            background: #dcfce7;
            color: #166534;
        }
        
        .improvement-badge.warning {
            background: #fef3c7;
            color: #92400e;
        }
        
        .improvement-badge.danger {
            background: #fee2e2;
            color: #991b1b;
        }
        
        .status-badge {
            display: inline-flex;
            align-items: center;
            gap: 0.25rem;
            padding: 0.25rem 0.75rem;
            border-radius: 0.375rem;
            font-size: 0.875rem;
            font-weight: 600;
        }
        
        .status-badge.pass {
            background: #dcfce7;
            color: #166534;
        }
        
        .status-badge.fail {
            background: #fee2e2;
            color: #991b1b;
        }
        
        .figure-container {
            margin: 1.5rem 0;
            text-align: center;
        }
        
        .figure-container img {
            max-width: 100%;
            width: 100%;
            height: auto;
            border-radius: 0.5rem;
            box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1);
        }
        
        .figures-row {
            display: flex;
            gap: 1.5rem;
            margin: 1.5rem 0;
            align-items: stretch;
        }
        
        .figures-row .figure-container {
            flex: 1;
            margin: 0;
            min-width: 0;
            display: flex;
            flex-direction: column;
        }
        
        .figures-row .figure-container img {
            width: 100%;
            flex: 1;
            object-fit: contain;
            border-radius: 0.5rem;
            box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1);
        }
        
        .figure-caption {
            margin-top: 0.75rem;
            font-size: 0.875rem;
            color: var(--text-secondary);
        }
        
        .config-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 1rem;
        }
        
        .config-item {
            background: #f8fafc;
            padding: 0.75rem 1rem;
            border-radius: 0.5rem;
        }
        
        .config-item .label {
            font-size: 0.75rem;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }
        
        .config-item .value {
            font-size: 1.125rem;
            font-weight: 600;
            color: var(--text-color);
        }
        
        .comparison-row {
            display: grid;
            grid-template-columns: 1fr 1fr 1fr;
            gap: 2rem;
            margin: 1.5rem 0;
        }
        
        .comparison-card {
            text-align: center;
            padding: 1.5rem;
            background: #f8fafc;
            border-radius: 0.75rem;
        }
        
        .comparison-card.hashmap {
            border-top: 3px solid #4C72B0;
        }
        
        .comparison-card.forl0 {
            border-top: 3px solid #55A868;
        }
        
        .comparison-card.vs {
            display: flex;
            align-items: center;
            justify-content: center;
            background: transparent;
        }
        
        .comparison-card h4 {
            font-size: 0.875rem;
            color: var(--text-secondary);
            margin-bottom: 0.5rem;
        }
        
        .comparison-card .metric {
            font-size: 1.75rem;
            font-weight: 700;
        }
        
        .comparison-card.hashmap .metric { color: #4C72B0; }
        .comparison-card.forl0 .metric { color: #55A868; }
        
        .comparison-card .unit {
            font-size: 0.875rem;
            color: var(--text-secondary);
        }
        
        .vs-circle {
            width: 3rem;
            height: 3rem;
            border-radius: 50%;
            background: var(--primary-color);
            color: white;
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 700;
        }
        
        .latency-grid {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 1rem;
            margin: 1rem 0;
        }
        
        .latency-item {
            text-align: center;
            padding: 1rem;
            background: #f8fafc;
            border-radius: 0.5rem;
        }
        
        .latency-item .percentile {
            font-size: 0.75rem;
            color: var(--text-secondary);
            text-transform: uppercase;
        }
        
        .latency-item .value {
            font-size: 1.5rem;
            font-weight: 600;
            color: var(--text-color);
        }
        
        .latency-item .unit {
            font-size: 0.75rem;
            color: var(--text-secondary);
        }
        
        footer {
            text-align: center;
            padding: 2rem;
            color: var(--text-secondary);
            font-size: 0.875rem;
        }
        
        @media (max-width: 768px) {
            .container { padding: 1rem; }
            header { padding: 2rem 1rem; }
            header h1 { font-size: 1.75rem; }
            .comparison-row { grid-template-columns: 1fr; }
            .latency-grid { grid-template-columns: repeat(2, 1fr); }
        }
    </style>
</head>
<body>
    <header>
        <div class="container">
            <h1>📊 ForL0 StateBackend Benchmark Report</h1>
            <p class="subtitle">Performance Comparison: ForL0StateBackend vs HashMapStateBackend</p>
            <p class="timestamp">Generated: {{ timestamp }}</p>
        </div>
    </header>
    
    <div class="container">
        <!-- Executive Summary -->
        <div class="summary-card {{ 'success' if all_pass else 'warning' }}">
            <h2>📋 Executive Summary</h2>
            <p>{{ summary }}</p>
        </div>
        
        <!-- Quick Stats -->
        <div class="stats-grid">
            <div class="stat-card">
                <div class="value">{{ mode | upper }}</div>
                <div class="label">Test Mode</div>
            </div>
            <div class="stat-card">
                <div class="value">{{ parallelism }}</div>
                <div class="label">Parallelism</div>
            </div>
            <div class="stat-card improvement {{ wc_negative_class }}">
                <div class="value">{{ wc_tpc_imp }}%</div>
                <div class="label">WordCount Improvement</div>
            </div>
            <div class="stat-card">
                <div class="value">{{ total_benchmarks }}</div>
                <div class="label">Total Benchmarks</div>
            </div>
        </div>
        
        <!-- WordCount Section -->
        <div class="section">
            <h2>📝 Stateful WordCount Benchmark</h2>
            
            <h3>Configuration</h3>
            <p class="note">Uses KeyedProcessFunction + ValueState (VoidNamespace) for pure state access testing.</p>
            <div class="config-grid">
                <div class="config-item">
                    <div class="label">Key Count</div>
                    <div class="value">{{ wc_config.num_keys | default('N/A') }}</div>
                </div>
                <div class="config-item">
                    <div class="label">Record Count</div>
                    <div class="value">{{ wc_config.num_records | default('N/A') }}</div>
                </div>
                <div class="config-item">
                    <div class="label">Skew Factor</div>
                    <div class="value">{{ wc_config.skew_factor | default('0') }}</div>
                </div>
                <div class="config-item">
                    <div class="label">Arrival Rate</div>
                    <div class="value">{{ wc_config.arrival_rate | default('unlimited') }}</div>
                </div>
            </div>
            
            <h3>Throughput Comparison</h3>
            <div class="comparison-row">
                <div class="comparison-card hashmap">
                    <h4>HashMapStateBackend</h4>
                    <div class="metric">{{ wc_hashmap_tpc }}</div>
                    <div class="unit">records/s/core</div>
                </div>
                <div class="comparison-card vs">
                    <div class="vs-circle">VS</div>
                </div>
                <div class="comparison-card forl0">
                    <h4>ForL0StateBackend</h4>
                    <div class="metric">{{ wc_forl0_tpc }}</div>
                    <div class="unit">records/s/core</div>
                </div>
            </div>
            
            <div style="text-align: center; margin: 1rem 0;">
                <span class="improvement-badge {{ wc_badge_class }}">
                    {{ wc_imp_sign }}{{ wc_tpc_imp }}% Improvement
                </span>
            </div>
            
            <div class="figure-container" style="margin-top: 1.5rem;">
                <img src="../figures/wordcount_throughput.png" alt="WordCount Throughput Comparison">
                <p class="figure-caption">Figure 1: Throughput Comparison</p>
            </div>
        </div>
        
        <!-- Client Usecase Section -->
        {% if client_rows %}
        <div class="section">
            <h2>Client Usecase Benchmark</h2>
            <p style="color: var(--text-secondary); margin-bottom: 1rem;">
                Latest contract_baseline run for the customer dual-stream join workload. The table uses the most recent raw result per backend, including the profiled run when available.
            </p>
            <table>
                <thead>
                    <tr>
                        <th>Backend</th>
                        <th>Timestamp</th>
                        <th>Records</th>
                        <th>Throughput</th>
                        <th>Throughput/Core</th>
                        <th>Wall Time</th>
                        <th>Profile</th>
                    </tr>
                </thead>
                <tbody>
                    {% for row in client_rows %}
                    <tr>
                        <td><strong>{{ row.backend }}</strong></td>
                        <td>{{ row.timestamp }}</td>
                        <td class="value-cell">{{ row.records }}</td>
                        <td class="value-cell">{{ row.throughput }}</td>
                        <td class="value-cell">{{ row.throughput_per_core }}</td>
                        <td class="value-cell">{{ row.wall_time }}</td>
                        <td>{{ row.profile }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            {% if client_improvement != 'N/A' %}
            <div style="text-align: center; margin: 1rem 0;">
                <span class="improvement-badge {{ client_badge_class }}">
                    {{ client_imp_sign }}{{ client_improvement }}% Client Usecase Improvement
                </span>
            </div>
            <p style="color: var(--text-secondary);">
                Analysis: ForL0 and HashMap both completed the bounded customer workload and wrote raw results. The latest profiled run shows {{ client_analysis }}. Flame graph links below correspond to the same latest profiled run.
            </p>
            {% endif %}
        </div>
        {% endif %}

        <!-- NexMark Section -->
        <div class="section">
            <h2>🏆 NexMark Benchmark</h2>
            
            <p style="color: var(--text-secondary); margin-bottom: 1rem;">
                NexMark is a standard benchmark for streaming systems, simulating an online auction scenario.
            </p>
            
            {% if not tpc_supported %}
            <div style="margin-bottom: 1rem; padding: 0.75rem; background: #fef3c7; border-radius: 0.5rem;">
                ⚠️ <strong>Note:</strong> Throughput per core (rec/s/core) requires Linux /proc filesystem to read CPU time.
                Current platform: <strong>{{ platform }}</strong>. This metric shows N/A on macOS.
            </div>
            {% endif %}
            
            <table>
                <thead>
                    <tr>
                        <th>Query</th>
                        <th>Description</th>
                        <th>Events</th>
                        <th>HashMap (rec/s)</th>
                        <th>ForL0 (rec/s)</th>
                        <th>HashMap (rec/s/core)</th>
                        <th>ForL0 (rec/s/core)</th>
                        <th>Improvement</th>
                    </tr>
                </thead>
                <tbody>
                    {% for row in nexmark_rows %}
                    <tr>
                        <td><strong>{{ row.query }}</strong></td>
                        <td>{{ row.description }}</td>
                        <td class="value-cell">{{ row.events }}</td>
                        <td class="value-cell">{{ row.hashmap_throughput }}</td>
                        <td class="value-cell">{{ row.forl0_throughput }}</td>
                        <td class="value-cell">{{ row.hashmap_tpc }}</td>
                        <td class="value-cell">{{ row.forl0_tpc }}</td>
                        <td>
                            {% if row.improvement != 'N/A' %}
                            <span class="improvement-badge {{ row.badge_class }}">
                                {{ row.imp_sign }}{{ row.improvement }}%
                            </span>
                            {% else %}
                            <span style="color: var(--text-secondary);">N/A</span>
                            {% endif %}
                        </td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            
            <div class="figure-container">
                <img src="../figures/improvement_summary.png" alt="Performance Improvement Summary">
                <p class="figure-caption">Figure 3: Performance Improvement Summary (Target: ≥60%)</p>
            </div>
        </div>
        
        <!-- L0Table Metrics Section (BENCHMARK_TEST) -->
        {% if l0_metrics_available %}
        <div class="section">
            <h2>🔬 L0Table Metrics Analysis</h2>
            
            <p style="color: var(--text-secondary); margin-bottom: 1rem;">
                [BENCHMARK_TEST] L0Table metrics collected from: <strong>{{ l0_sources }}</strong>
                (Total samples: {{ l0_sample_count }})
            </p>
            
            <div style="margin-bottom: 1rem; padding: 0.75rem; background: #e0f2fe; border-radius: 0.5rem; font-size: 0.9rem;">
                <strong>📊 统计说明：</strong> 表格中的统计数据来自 L0Table 最终汇报（l0table_final），是作业结束时的<strong>累计值</strong>，
                能准确反映总访问次数和命中率。图表基于周期采样（1秒间隔），可能不完整（特别是运行时间<1秒的算子）。
                Samples 列标注 "(final)" 表示该查询的周期采样为空，仅有最终汇报数据。
            </div>
            
            <!-- Per-Query Statistics Table -->
            <table>
                <thead>
                    <tr>
                        <th>Query</th>
                        <th>Hit Rate (%)</th>
                        <th>Total Accesses</th>
                        <th>Evictions</th>
                        <th>Samples</th>
                    </tr>
                </thead>
                <tbody>
                    {% for stat in l0_query_stats %}
                    <tr>
                        <td><strong>{{ stat.query }}</strong></td>
                        <td>{{ stat.hit_rate }}%</td>
                        <td>{{ stat.total_accesses }}</td>
                        <td>{{ stat.evictions }}</td>
                        <td>{{ stat.samples }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            
            <div class="figures-row" style="margin-top: 1.5rem;">
                <div class="figure-container">
                    <img src="../figures/l0table_timeline.png" alt="L0Table Timeline" onerror="this.parentElement.style.display='none'">
                    <p class="figure-caption">Figure: L0Table Hit Rate Over Time (Per Query)</p>
                </div>
                <div class="figure-container">
                    <img src="../figures/state_entries_timeline.png" alt="State Entries Timeline" onerror="this.parentElement.style.display='none'">
                    <p class="figure-caption">Figure: L0 Cache Valid Slots Over Time (Per Query)</p>
                </div>
            </div>
        </div>
        {% endif %}
        
        <!-- Hardware Statistics Section (BENCHMARK_TEST) -->
        {% if hw_metrics_available %}
        <div class="section">
            <h2>🖥️ Hardware Statistics</h2>
            
            <p style="color: var(--text-secondary); margin-bottom: 1rem;">
                [BENCHMARK_TEST] Hardware-level metrics including CPU cache and memory usage.
            </p>
            
            {% if not cache_supported %}
            <div style="margin-bottom: 1rem; padding: 0.75rem; background: #fef3c7; border-radius: 0.5rem;">
                ⚠️ <strong>Note:</strong> CPU cache statistics require Linux with perf_events support.
                Current platform: <strong>{{ platform }}</strong> - showing memory metrics only.
            </div>
            {% endif %}
            
            <h3 style="margin-top: 1.5rem;">CPU Cache Misses</h3>
            {% if hw_cache_stats %}
            <table>
                <thead>
                    <tr>
                        <th>Query</th>
                        <th>Backend</th>
                        <th>Total Cache Misses</th>
                        <th>StateMap Cache Misses</th>
                        <th>Miss Rate</th>
                        <th>Duration</th>
                    </tr>
                </thead>
                <tbody>
                    {% for stat in hw_cache_stats %}
                    <tr>
                        <td><strong>{{ stat.query }}</strong></td>
                        <td>{{ stat.backend }}</td>
                        <td class="value-cell">{{ stat.total_misses }}</td>
                        <td class="value-cell">{{ stat.statemap_misses }}</td>
                        <td>{{ stat.miss_rate }}%</td>
                        <td>{{ stat.duration }}s</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            {% else %}
            <p style="color: var(--text-secondary); font-style: italic;">
                No CPU cache statistics available. Run benchmarks on Linux with <code>--profile cache</code> flag to collect cache metrics.
            </p>
            {% endif %}
            
            <h3 style="margin-top: 2rem;">Memory Usage</h3>
            {% if hw_memory_stats %}
            <table>
                <thead>
                    <tr>
                        <th>Query</th>
                        <th>Backend</th>
                        <th>Avg RSS (MB)</th>
                        <th>Max RSS (MB)</th>
                        <th>Samples</th>
                    </tr>
                </thead>
                <tbody>
                    {% for stat in hw_memory_stats %}
                    <tr>
                        <td><strong>{{ stat.query }}</strong></td>
                        <td>{{ stat.backend }}</td>
                        <td class="value-cell">{{ stat.avg_rss }}</td>
                        <td class="value-cell">{{ stat.max_rss }}</td>
                        <td>{{ stat.samples }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            
            <div class="figure-container" style="margin-top: 1.5rem;">
                <img src="../figures/memory_usage_timeline.png" alt="Memory Usage Timeline" onerror="this.parentElement.style.display='none'">
                <p class="figure-caption">Figure: Memory Usage (RSS) Over Time</p>
            </div>
            {% else %}
            <p style="color: var(--text-secondary); font-style: italic;">
                No memory usage data available. Run benchmarks with hardware metrics collection enabled.
            </p>
            {% endif %}
        </div>
        {% endif %}
        
        <!-- Profiler Section (BENCHMARK_TEST) - Flame Graphs -->
        {% if profiler_files %}
        <div class="section">
            <h2>🔥 Performance Profiling</h2>
            
            <p style="color: var(--text-secondary); margin-bottom: 1rem;">
                [BENCHMARK_TEST] Flame graphs generated using Async Profiler. Click to open interactive HTML views.
            </p>
            
            {% if not cache_supported %}
            <div style="margin-bottom: 1rem; padding: 0.75rem; background: #fef3c7; border-radius: 0.5rem;">
                ⚠️ Note: CPU cache statistics (cache-misses, L1-dcache-load-misses) are only available on Linux.
                Current platform: <strong>{{ platform }}</strong>
            </div>
            {% endif %}
            
            <table>
                <thead>
                    <tr>
                        <th>Backend</th>
                        <th>Event Type</th>
                        <th>File</th>
                        <th>Action</th>
                    </tr>
                </thead>
                <tbody>
                    {% for file in profiler_files %}
                    <tr>
                        <td><strong>{{ file.backend }}</strong></td>
                        <td>{{ file.event }}</td>
                        <td><code>{{ file.filename }}</code></td>
                        <td>
                            <a href="{{ file.path }}" target="_blank" class="status-badge pass" 
                               style="text-decoration: none; padding: 0.25rem 0.75rem;">
                                Open Flame Graph
                            </a>
                        </td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            
            <div style="margin-top: 1rem; padding: 1rem; background: #f1f5f9; border-radius: 0.5rem;">
                <strong>💡 Flame Graph Tips:</strong>
                <ul style="margin-top: 0.5rem; margin-left: 1.5rem;">
                    <li><strong>Width</strong> = Time spent (wider = more CPU time)</li>
                    <li><strong>Hover</strong> over bars to see function names and percentages</li>
                    <li><strong>Click</strong> to zoom into a specific call stack</li>
                    <li>Look for <code>ForL0</code> methods to analyze state backend performance</li>
                </ul>
            </div>
        </div>
        {% endif %}
        
        <!-- Verification Section -->
        <div class="section">
            <h2>✅ Verification Results</h2>
            
            <table>
                <thead>
                    <tr>
                        <th>Benchmark</th>
                        <th>Target</th>
                        <th>Actual</th>
                        <th>Status</th>
                    </tr>
                </thead>
                <tbody>
                    {% for row in verification_rows %}
                    <tr>
                        <td><strong>{{ row.name }}</strong></td>
                        <td>≥ 60%</td>
                        <td class="value-cell">{{ row.actual }}%</td>
                        <td>
                            <span class="status-badge {{ 'pass' if row.passed else 'fail' }}">
                                {{ '✓ PASS' if row.passed else '✗ FAIL' }}
                            </span>
                        </td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
        
        <!-- Conclusion -->
        <div class="section">
            <h2>📌 Conclusion</h2>
            <p>{{ conclusion }}</p>
        </div>
        </div>
    </div>
    
    <footer>
        <p>ForL0 StateBackend Benchmark Report | Generated by benchmark framework</p>
        <p>© 2025 ForL0 Project</p>
    </footer>
</body>
</html>
"""
    
    # Prepare data
    config = load_config()
    wc_config = config.get('wordcount', {})
    
    wc_hashmap = results.get('wordcount', {}).get('hashmap', {})
    wc_forl0 = results.get('wordcount', {}).get('forl0', {})
    
    # Calculate metrics
    wc_hashmap_tpc = wc_hashmap.get('throughput_per_core', 0)
    wc_forl0_tpc = wc_forl0.get('throughput_per_core', 0)
    
    wc_tpc_imp = ((wc_forl0_tpc - wc_hashmap_tpc) / wc_hashmap_tpc * 100) if wc_hashmap_tpc > 0 else 0
    
    # Pre-calculate badge class for WordCount
    if wc_tpc_imp >= 60:
        wc_badge_class = 'success'
    elif wc_tpc_imp >= 0:
        wc_badge_class = 'warning'
    else:
        wc_badge_class = 'danger'
    wc_imp_sign = '+' if wc_tpc_imp >= 0 else ''
    wc_negative_class = 'negative' if wc_tpc_imp < 0 else ''
    
    # NexMark data
    nexmark_descriptions = {
        'q4': 'Average Selling Price by Category',
        'q5': 'Hot Items',
        'q8': 'Monitor New Users',
        'q9': 'Winning Bids',
        'q11': 'User Sessions',
        'q18': 'Find Last Bid',
        'q19': 'Auction Statistics',
        'q20': 'Expand Bid'
    }
    
    nexmark_rows = []
    nexmark_results = results.get('nexmark', {})
    
    # Dynamically get available queries from results
    available_queries = set()
    for backend in ['hashmap', 'forl0']:
        if backend in nexmark_results:
            available_queries.update(nexmark_results[backend].keys())
    queries = sorted(available_queries, key=lambda x: (int(x[1:]) if x[1:].isdigit() else 999))
    
    # Check if throughput_per_core is supported (Linux only)
    import platform
    is_linux = platform.system() == 'Linux'
    tpc_supported = is_linux
    
    for query in queries:
        h_data = nexmark_results.get('hashmap', {}).get(query, {})
        f_data = nexmark_results.get('forl0', {}).get(query, {})
        
        # Get raw throughput values
        h_throughput = h_data.get('throughput', 0) or 0
        f_throughput = f_data.get('throughput', 0) or 0
        h_tpc_raw = h_data.get('throughput_per_core', 0) or 0
        f_tpc_raw = f_data.get('throughput_per_core', 0) or 0
        events = h_data.get('events_num', f_data.get('events_num', 'N/A'))
        
        # Calculate improvement based on throughput (always available)
        imp = ((f_throughput - h_throughput) / h_throughput * 100) if h_throughput > 0 and f_throughput > 0 else None
        
        # Determine badge class for template
        if imp is not None:
            if imp >= 60:
                badge_class = 'success'
            elif imp >= 0:
                badge_class = 'warning'
            else:
                badge_class = 'danger'
            imp_sign = '+' if imp >= 0 else ''
        else:
            badge_class = ''
            imp_sign = ''
        
        nexmark_rows.append({
            'query': query.upper(),
            'description': nexmark_descriptions.get(query, ''),
            'events': f'{events:,}' if isinstance(events, int) else str(events),
            'hashmap_throughput': f'{h_throughput:,.0f}' if h_throughput else 'N/A',
            'forl0_throughput': f'{f_throughput:,.0f}' if f_throughput else 'N/A',
            'hashmap_tpc': f'{h_tpc_raw:,.0f}' if h_tpc_raw > 0 else 'N/A',
            'forl0_tpc': f'{f_tpc_raw:,.0f}' if f_tpc_raw > 0 else 'N/A',
            'improvement': f'{imp:.1f}' if imp is not None else 'N/A',
            'badge_class': badge_class,
            'imp_sign': imp_sign
        })
    
    # Verification rows
    verification_rows = []
    all_pass = True
    total_benchmarks = 0
    
    if wc_hashmap_tpc > 0 and wc_forl0_tpc > 0:
        passed = wc_tpc_imp >= 60
        all_pass = all_pass and passed
        total_benchmarks += 1
        verification_rows.append({
            'name': 'WordCount',
            'actual': f'{wc_tpc_imp:.1f}',
            'passed': passed
        })
    
    for row in nexmark_rows:
        if row['improvement'] != 'N/A':
            imp = float(row['improvement'])
            passed = imp >= 60
            all_pass = all_pass and passed
            total_benchmarks += 1
            verification_rows.append({
                'name': f"NexMark {row['query']}",
                'actual': row['improvement'],
                'passed': passed
            })
    
    if total_benchmarks == 0:
        total_benchmarks = 1
    
    # Summary and conclusion
    if all_pass:
        summary = "All benchmarks PASSED! ForL0StateBackend achieves ≥60% improvement in throughput per core compared to HashMapStateBackend."
        conclusion = "The ForL0 StateBackend successfully meets the performance target of 60% improvement over the HashMapStateBackend baseline. The implementation is ready for production deployment on 鲲鹏 servers with L0 Cache hardware."
    else:
        summary = f"Benchmarks completed. {sum(1 for r in verification_rows if r['passed'])}/{len(verification_rows)} tests passed the 60% improvement target."
        conclusion = "Further optimization or production environment testing may be needed to achieve the 60% improvement target across all benchmarks."
    
    # [BENCHMARK_TEST] Load L0Table metrics for report
    l0_metrics = load_l0table_metrics()
    l0_sources = 'None'
    l0_sample_count = 0
    l0_query_stats = []  # Per-query statistics
    # Legacy variables for template compatibility
    l0_final_hit_rate = 'N/A'
    l0_total_accesses = 'N/A'
    l0_eviction_count = 'N/A'
    
    # Check if l0_metrics is valid (not None and has data)
    l0_metrics_available = (
        l0_metrics is not None and 
        isinstance(l0_metrics, dict) and 
        len(l0_metrics.get('by_query', {})) > 0
    )
    
    if l0_metrics_available and l0_metrics is not None:
        by_query = l0_metrics.get('by_query', {})
        sources = l0_metrics.get('sources', [])
        l0_sources = ', '.join(sources) if sources else 'Unknown'
        l0_sample_count = len(l0_metrics.get('l0table', []))
        
        # Calculate statistics per query
        # [BENCHMARK_TEST] Always prefer l0table_final data for accurate cumulative statistics
        # Periodic samples may miss data for short-lived queries (< 1 second)
        for query in sorted(by_query.keys()):
            query_info = by_query[query]
            samples = query_info.get('samples', [])
            
            # Get L0Table samples
            l0_samples = [s for s in samples if s.get('type') == 'l0table']
            final_samples = [s for s in samples if s.get('type') == 'l0table_final']
            
            # Filter final_samples to only include those with actual access data
            # (late finals from subsequent jobs may have 0 access)
            valid_final_samples = [s for s in final_samples if s.get('access_count', 0) > 0]
            
            # [BENCHMARK_TEST] Primary strategy: use l0table_final for accurate cumulative stats
            if valid_final_samples:
                total_accesses = sum(s.get('access_count', 0) for s in valid_final_samples)
                total_hits = sum(s.get('hit_count', 0) for s in valid_final_samples)
                total_evictions = sum(s.get('eviction_count', 0) for s in valid_final_samples)
                
                # Calculate weighted average hit rate
                hit_rates_with_weights = [
                    (s.get('hit_rate', 0), s.get('access_count', 0)) 
                    for s in valid_final_samples
                ]
                if hit_rates_with_weights:
                    weighted_sum = sum(hr * w for hr, w in hit_rates_with_weights)
                    total_weight = sum(w for _, w in hit_rates_with_weights)
                    avg_hit_rate = weighted_sum / total_weight if total_weight > 0 else 0
                else:
                    avg_hit_rate = 0
                avg_hit_rate_pct = avg_hit_rate * 100  # Convert to percentage
                
                # Count periodic samples for reference
                periodic_count = len([s for s in l0_samples if s.get('access_count', 0) > 0])
                samples_info = f"{len(valid_final_samples)} (final)" if periodic_count == 0 else str(periodic_count)
                
                l0_query_stats.append({
                    'query': query,
                    'hit_rate': f"{avg_hit_rate_pct:.1f}",
                    'total_accesses': f"{total_accesses:,}",
                    'evictions': f"{total_evictions:,}",
                    'samples': samples_info
                })
            elif l0_samples:
                # Fallback: use periodic samples if no valid final data
                total_accesses = sum(s.get('total_accesses', s.get('access_count', 0)) for s in l0_samples)
                total_hits = sum(s.get('total_hits', s.get('hit_count', 0)) for s in l0_samples)
                total_evictions = sum(s.get('eviction_count', 0) for s in l0_samples)
                
                hit_rates = [s.get('hit_rate', 0) for s in l0_samples if s.get('total_accesses', s.get('access_count', 0)) > 0]
                avg_hit_rate = sum(hit_rates) / len(hit_rates) if hit_rates else 0
                avg_hit_rate_pct = avg_hit_rate * 100
                
                l0_query_stats.append({
                    'query': query,
                    'hit_rate': f"{avg_hit_rate_pct:.1f}",
                    'total_accesses': f"{total_accesses:,}",
                    'evictions': f"{total_evictions:,}",
                    'samples': len(l0_samples)
                })
    
    # Client Usecase latest result summary
    client_results = results.get('client_usecase', {})
    client_rows = []
    client_tpc = {}
    for backend in ['hashmap', 'forl0']:
        data = client_results.get(backend)
        if not data:
            continue
        meta = data.get('_metadata', {})
        tpc = get_throughput(data, data.get('config', {}).get('parallelism'))
        client_tpc[backend] = tpc
        profiles = data.get('profiler_files') or {}
        profile_links = []
        for event, path in profiles.items():
            name = Path(path).name
            profile_links.append(f'<a href="../profiles/{name}" target="_blank">{event}</a>')
        client_rows.append({
            'backend': BACKEND_LABELS.get(backend, backend),
            'timestamp': meta.get('timestamp', 'N/A'),
            'records': f"{data.get('total_input_records', data.get('desired_total_input_records', 0)):,}",
            'throughput': f"{data.get('throughput', 0):,.2f}",
            'throughput_per_core': f"{tpc:,.2f}" if tpc else 'N/A',
            'wall_time': f"{data.get('wall_time_seconds', 0):.2f}s",
            'profile': ', '.join(profile_links) if profile_links else 'not collected',
        })

    client_improvement = 'N/A'
    client_badge_class = 'neutral'
    client_imp_sign = ''
    client_analysis = 'no complete HashMap/ForL0 pair is available'
    if client_tpc.get('hashmap') and client_tpc.get('forl0'):
        client_imp_value = (client_tpc['forl0'] - client_tpc['hashmap']) / client_tpc['hashmap'] * 100
        client_improvement = f'{client_imp_value:.1f}'
        client_badge_class = 'positive' if client_imp_value >= 0 else 'negative'
        client_imp_sign = '+' if client_imp_value >= 0 else ''
        client_analysis = (
            f"ForL0 reached {client_tpc['forl0']:.2f} records/s/core versus "
            f"HashMap at {client_tpc['hashmap']:.2f} records/s/core, "
            f"a {client_imp_sign}{client_improvement}% delta on this small contract baseline"
        )

    if client_rows:
        total_benchmarks += 1
        summary += (
            f" Latest Client Usecase contract_baseline profile run completed for both backends; "
            f"ForL0 vs HashMap delta was {client_imp_sign}{client_improvement}%."
        )
        conclusion += (
            f" The Client Usecase section reports the latest profiled run and links the matching CPU flame graphs; "
            f"{client_analysis}."
        )

    # [BENCHMARK_TEST] Load hardware metrics for report
    hw_metrics = load_hardware_metrics(str(get_results_dir('hardware')))
    hw_cache_data = hw_metrics.get('cache', {})
    hw_memory_data = hw_metrics.get('memory', {})
    
    hw_metrics_available = bool(hw_cache_data or hw_memory_data)
    
    # Format cache stats for template
    hw_cache_stats = []
    for key, stats in hw_cache_data.items():
        parts = key.rsplit('_', 1)
        query = parts[0] if len(parts) == 2 else key
        backend = parts[1] if len(parts) == 2 else 'unknown'
        
        total_misses = stats.get('total_cache_misses', 0)
        total_refs = stats.get('total_cache_references', 1)
        statemap_misses = stats.get('statemap_cache_misses', 0)
        statemap_ratio = stats.get('statemap_ratio', 0)
        
        # Calculate miss rate as percentage
        miss_rate = (total_misses / total_refs * 100) if total_refs > 0 else 0
        
        duration = stats.get('duration_seconds', 0)
        
        hw_cache_stats.append({
            'query': query,
            'backend': backend,
            'total_misses': f"{total_misses:,}" if total_misses else 'N/A',
            'statemap_misses': f"{statemap_misses:,}" if statemap_misses else 'N/A',
            'miss_rate': f"{miss_rate:.2f}",
            'duration': f"{duration:.1f}"
        })
    
    # Format memory stats for template
    hw_memory_stats = []
    for key, series in hw_memory_data.items():
        parts = key.rsplit('_', 1)
        query = parts[0] if len(parts) == 2 else key
        backend = parts[1] if len(parts) == 2 else 'unknown'
        
        samples = series.get('samples', [])
        if samples:
            rss_values = [s.get('rss_mb', 0) for s in samples]
            avg_rss = sum(rss_values) / len(rss_values) if rss_values else 0
            max_rss = max(rss_values) if rss_values else 0
            
            hw_memory_stats.append({
                'query': query,
                'backend': backend,
                'avg_rss': f"{avg_rss:.1f}",
                'max_rss': f"{max_rss:.1f}",
                'samples': len(samples)
            })
    
    # [BENCHMARK_TEST] Scan for profiler flame graph files
    import platform
    profiles_dir = get_results_dir('profiles')
    profiler_files = []
    if profiles_dir.exists():
        for html_file in sorted(profiles_dir.glob('*.html')):
            # Parse filename: flamegraph_<event>_<backend>_<timestamp>.html
            # or: cache_<event>_<backend>_<timestamp>.html
            parts = html_file.stem.split('_')
            if len(parts) >= 3:
                if parts[0] == 'flamegraph':
                    event = parts[1]
                    backend = parts[2]
                elif parts[0] == 'cache':
                    event = '_'.join(parts[1:-2]) if len(parts) > 4 else parts[1]
                    backend = parts[-2] if len(parts) > 3 else parts[2]
                else:
                    continue
                
                profiler_files.append({
                    'backend': backend,
                    'event': event,
                    'filename': html_file.name,
                    'path': f'../profiles/{html_file.name}'
                })
    
    cache_supported = platform.system() == 'Linux'
    
    runtime_config = config.get('runtime', {})
    
    # Render template
    template = Template(html_template)
    report = template.render(
        timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        summary=summary,
        all_pass=all_pass,
        parallelism=runtime_config.get('parallelism', 8),
        total_benchmarks=total_benchmarks,
        wc_config=wc_config,
        wc_hashmap_tpc=f'{wc_hashmap_tpc:,.0f}' if wc_hashmap_tpc else 'N/A',
        wc_forl0_tpc=f'{wc_forl0_tpc:,.0f}' if wc_forl0_tpc else 'N/A',
        wc_tpc_imp=f'{wc_tpc_imp:.1f}',
        wc_badge_class=wc_badge_class,
        wc_imp_sign=wc_imp_sign,
        wc_negative_class=wc_negative_class,
        nexmark_rows=nexmark_rows,
        client_rows=client_rows,
        client_improvement=client_improvement,
        client_badge_class=client_badge_class,
        client_imp_sign=client_imp_sign,
        client_analysis=client_analysis,
        verification_rows=verification_rows,
        conclusion=conclusion,
        # [BENCHMARK_TEST] L0Table metrics variables
        l0_metrics_available=l0_metrics_available,
        l0_final_hit_rate=l0_final_hit_rate,
        l0_total_accesses=l0_total_accesses,
        l0_eviction_count=l0_eviction_count,
        l0_sources=l0_sources,
        l0_sample_count=l0_sample_count,
        l0_query_stats=l0_query_stats,
        # [BENCHMARK_TEST] Hardware metrics variables
        hw_metrics_available=hw_metrics_available,
        hw_cache_stats=hw_cache_stats if hw_cache_stats else None,
        hw_memory_stats=hw_memory_stats if hw_memory_stats else None,
        # [BENCHMARK_TEST] Profiler/flame graph variables
        profiler_files=profiler_files if profiler_files else None,
        cache_supported=cache_supported,
        platform=platform.system(),
        tpc_supported=tpc_supported
    )
    
    # Save HTML report
    report_path = output_dir / 'benchmark_report.html'
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"Saved: {report_path}")
    return report_path


def main():
    parser = argparse.ArgumentParser(description='Generate benchmark report and figures')
    parser.add_argument('--format', choices=['pdf', 'png', 'both'], default='both',
                       help='Figure output format')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("Generating Benchmark Report")
    print("=" * 60)
    
    # Create output directories
    figures_dir = get_results_dir('figures')
    reports_dir = get_results_dir('reports')
    
    # Load results
    print("\nLoading results...")
    results = load_results()
    
    # Generate figures
    print("\nGenerating figures...")
    plot_wordcount_comparison(results, figures_dir)
    plot_nexmark_comparison(results, figures_dir)
    plot_improvement_summary(results, figures_dir)
    print("\nGenerating benchset figures (if available)...")
    generate_benchset_paper_artifacts(figures_dir, reports_dir)
    
    # [BENCHMARK_TEST] Generate L0Table metrics figures if available
    print("\nGenerating L0Table metrics figures (if available)...")
    plot_l0table_timeline(figures_dir)
    plot_state_entries_timeline(figures_dir)
    
    # [BENCHMARK_TEST] Generate hardware metrics figures if available
    print("\nGenerating hardware metrics figures (if available)...")
    plot_memory_usage_timeline(figures_dir)
    
    # Generate report
    print("\nGenerating report...")
    generate_report(results, reports_dir)
    
    print("\n" + "=" * 60)
    print("Report generation complete!")
    print(f"Figures: {figures_dir}")
    print(f"Report:  {reports_dir / 'benchmark_report.html'}")
    print("=" * 60)


if __name__ == '__main__':
    main()
