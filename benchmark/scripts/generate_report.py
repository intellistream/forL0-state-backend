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
import numpy as np  # type: ignore[import-untyped]
import pandas as pd  # type: ignore[import-untyped]
import seaborn as sns  # type: ignore[import-untyped]
from jinja2 import Template  # type: ignore[import-untyped]

from utils.config import get_benchmark_root, get_results_dir, load_config

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

BACKEND_LABELS = {
    'hashmap': 'HashMapStateBackend',
    'forl0': 'ForL0StateBackend',
}


def load_results():
    """Load all benchmark results from raw directory."""
    results_dir = get_results_dir('raw')
    results = {
        'wordcount': {'hashmap': None, 'forl0': None},
        'nexmark': {'hashmap': {}, 'forl0': {}}
    }
    
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
            
            elif 'nexmark' in test_name or data.get('benchmark') == 'nexmark':
                query = data.get('query', '')
                if backend in results['nexmark'] and query:
                    if query not in results['nexmark'][backend]:
                        results['nexmark'][backend][query] = data
                    else:
                        existing_ts = results['nexmark'][backend][query].get('_metadata', {}).get('timestamp', '')
                        new_ts = metadata.get('timestamp', '')
                        if new_ts > existing_ts:
                            results['nexmark'][backend][query] = data
        
        except Exception as e:
            print(f"Warning: Could not load {filepath}: {e}")
    
    return results


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
    """Generate NexMark throughput comparison figure for all queries."""
    nexmark_results = results.get('nexmark', {})
    
    # Collect data
    queries = ['q4', 'q5', 'q8', 'q9', 'q11', 'q18', 'q19', 'q20']
    data = []
    
    for query in queries:
        for backend in ['hashmap', 'forl0']:
            if backend in nexmark_results and query in nexmark_results[backend]:
                result = nexmark_results[backend][query]
                tpc = result.get('throughput_per_core', result.get('throughput', 0))
                parallelism = result.get('parallelism', 8)
                if 'throughput' in result and 'throughput_per_core' not in result:
                    tpc = result['throughput'] / parallelism
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
    ax.set_title('NexMark Benchmark: Throughput Comparison')
    ax.legend(loc='upper right')
    
    # Add improvement labels
    for i, (h, f) in enumerate(zip(hashmap_vals, forl0_vals)):
        if h > 0 and f > 0:
            imp = ((f - h) / h) * 100
            ax.text(i + width/2, f + max(forl0_vals)*0.02, f'+{imp:.0f}%',
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
        h = wc_results['hashmap'].get('throughput_per_core', 0)
        f = wc_results['forl0'].get('throughput_per_core', 0)
        if h > 0:
            improvements.append(((f - h) / h) * 100)
            labels.append('WordCount')
    
    # NexMark improvements
    nexmark_results = results.get('nexmark', {})
    for query in ['q4', 'q5', 'q8', 'q9', 'q11', 'q18', 'q19', 'q20']:
        h_data = nexmark_results.get('hashmap', {}).get(query, {})
        f_data = nexmark_results.get('forl0', {}).get(query, {})
        
        h = h_data.get('throughput_per_core', h_data.get('throughput', 0))
        f = f_data.get('throughput_per_core', f_data.get('throughput', 0))
        
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
    ax.set_title('ForL0 vs HashMapStateBackend: Throughput Improvement')
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
    
    Returns a list of metric samples sorted by elapsed_ms.
    Aggregates metrics from all test runs (WordCount, NexMark, etc.)
    """
    l0metrics_dir = get_results_dir('l0metrics')
    
    # Find all L0 metrics files
    metrics_files = list(l0metrics_dir.glob('l0table_metrics_*.json'))
    
    if not metrics_files:
        return None
    
    # Group files by test type
    all_metrics = {
        'wordcount': None,
        'nexmark': {}
    }
    
    for filepath in metrics_files:
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            
            filename = filepath.name
            samples = data.get('samples', [])
            
            if 'wordcount' in filename or data.get('query') is None:
                # WordCount metrics - keep the latest
                if all_metrics['wordcount'] is None:
                    all_metrics['wordcount'] = {'file': str(filepath), 'data': data}
                elif filepath.stat().st_mtime > Path(all_metrics['wordcount']['file']).stat().st_mtime:
                    all_metrics['wordcount'] = {'file': str(filepath), 'data': data}
            else:
                # NexMark metrics - group by query
                query = data.get('query', 'unknown')
                if query not in all_metrics['nexmark']:
                    all_metrics['nexmark'][query] = {'file': str(filepath), 'data': data}
                elif filepath.stat().st_mtime > Path(all_metrics['nexmark'][query]['file']).stat().st_mtime:
                    all_metrics['nexmark'][query] = {'file': str(filepath), 'data': data}
        except Exception as e:
            print(f"Warning: Could not load L0 metrics from {filepath}: {e}")
    
    # Aggregate all samples for timeline charts
    all_samples = []
    source_info = []
    
    if all_metrics['wordcount']:
        wc_data = all_metrics['wordcount']['data']
        all_samples.extend(wc_data.get('samples', []))
        source_info.append('WordCount')
    
    for query, info in all_metrics['nexmark'].items():
        nx_data = info['data']
        all_samples.extend(nx_data.get('samples', []))
        source_info.append(f'NexMark-{query}')
    
    if not all_samples:
        return None
    
    # Separate different types
    l0table_samples = [s for s in all_samples if s.get('type') == 'l0table']
    cache_samples = [s for s in all_samples if s.get('type') == 'cache']
    final_l0table = [s for s in all_samples if s.get('type') == 'l0table_final']
    final_cache = [s for s in all_samples if s.get('type') == 'cache_final']
    
    return {
        'l0table': sorted(l0table_samples, key=lambda x: x.get('elapsed_ms', 0)),
        'cache': sorted(cache_samples, key=lambda x: x.get('elapsed_ms', 0)),
        'final_l0table': final_l0table,
        'final_cache': final_cache,
        'sources': source_info,
        'all_metrics': all_metrics
    }


def plot_l0table_timeline(output_dir):
    """
    [BENCHMARK_TEST] Generate L0Table metrics timeline charts.
    
    Creates:
    - Hit rate over time
    - Access count over time (incremental per sample)
    - Valid slots (load factor proxy) over time
    
    Aggregates data from all tests (WordCount, NexMark).
    """
    metrics = load_l0table_metrics()
    
    if not metrics or not metrics.get('l0table'):
        print("No L0Table metrics available for plotting")
        return None
    
    l0table_samples = metrics['l0table']
    cache_samples = metrics.get('cache', [])
    sources = metrics.get('sources', [])
    
    if len(l0table_samples) < 2:
        print("Not enough L0Table samples for timeline plot")
        return None
    
    print(f"  Plotting L0Table metrics from: {', '.join(sources)}")
    
    # Prepare data for L0Table
    times_sec = [s['elapsed_ms'] / 1000.0 for s in l0table_samples]
    hit_rates = [s.get('hit_rate', 0) * 100 for s in l0table_samples]  # Convert to percentage
    access_counts = [s.get('access_count', 0) for s in l0table_samples]
    valid_slots = [s.get('valid_slots', 0) for s in l0table_samples]
    
    # Calculate incremental access counts (per interval)
    incremental_access = [access_counts[0]]
    for i in range(1, len(access_counts)):
        incremental_access.append(access_counts[i] - access_counts[i-1])
    
    # Create figure with 3 subplots
    fig, axes = plt.subplots(3, 1, figsize=(10, 10), sharex=True)
    
    # Plot 1: Hit Rate over time
    ax1 = axes[0]
    ax1.plot(times_sec, hit_rates, color=COLORS['forl0'], linewidth=2, marker='o', markersize=3)
    ax1.set_ylabel('L0 Hit Rate (%)')
    title_suffix = f" ({', '.join(sources)})" if sources else ""
    ax1.set_title(f'L0Table Metrics Timeline{title_suffix}')
    ax1.set_ylim(0, 100)
    ax1.axhline(y=50, color='orange', linestyle='--', alpha=0.5, label='50% threshold')
    ax1.legend(loc='upper right')
    
    # Plot 2: Incremental Access Count (throughput proxy)
    ax2 = axes[1]
    ax2.bar(times_sec, incremental_access, width=0.8, color=COLORS['forl0'], alpha=0.7)
    ax2.set_ylabel('Accesses per Interval')
    
    # Plot 3: Valid Slots (load factor)
    ax3 = axes[2]
    ax3.plot(times_sec, valid_slots, color=COLORS['hashmap'], linewidth=2, marker='s', markersize=3)
    ax3.set_ylabel('Valid Slots')
    ax3.set_xlabel('Time (seconds)')
    
    # Get max valid slots for reference line (16 for default 4-bucket L0Table)
    max_slots = 16  # 4 buckets × 4 slots
    ax3.axhline(y=max_slots, color='red', linestyle='--', alpha=0.5, label=f'Max Slots ({max_slots})')
    ax3.set_ylim(0, max_slots + 2)
    ax3.legend(loc='upper right')
    
    plt.tight_layout()
    
    filepath = output_dir / 'l0table_timeline.pdf'
    plt.savefig(filepath)
    plt.savefig(output_dir / 'l0table_timeline.png')
    plt.close()
    
    print(f"Saved: {filepath}")
    
    # Generate summary stats
    final_l0table = metrics.get('final_l0table', [])
    if final_l0table:
        print("\n[L0Table Final Statistics]")
        for stat in final_l0table:
            backend = stat.get('backend', '?')
            print(f"  Backend {backend}, Table {stat.get('table_index', '?')}: "
                  f"hit_rate={stat.get('hit_rate', 0):.2%}, "
                  f"accesses={stat.get('access_count', 0):,}, "
                  f"evictions={stat.get('eviction_count', 0):,}")
    
    return filepath


def plot_cache_hit_comparison(output_dir):
    """
    [BENCHMARK_TEST] Generate cache hit rate comparison chart.
    
    Shows L0 hit rate vs overall hit rate over time.
    """
    metrics = load_l0table_metrics()
    
    if not metrics or not metrics.get('cache'):
        print("No cache metrics available for plotting")
        return None
    
    cache_samples = metrics['cache']
    
    if len(cache_samples) < 2:
        print("Not enough cache samples for comparison plot")
        return None
    
    times_sec = [s['elapsed_ms'] / 1000.0 for s in cache_samples]
    l0_hit_rates = [s.get('l0_hit_rate', 0) * 100 for s in cache_samples]
    overall_hit_rates = [s.get('overall_hit_rate', 0) * 100 for s in cache_samples]
    
    fig, ax = plt.subplots(figsize=(10, 5))
    
    ax.plot(times_sec, l0_hit_rates, color=COLORS['forl0'], linewidth=2, 
            marker='o', markersize=4, label='L0 Cache Hit Rate')
    ax.plot(times_sec, overall_hit_rates, color=COLORS['hashmap'], linewidth=2, 
            marker='s', markersize=4, label='Overall Hit Rate (L0 + MainTable)')
    
    ax.set_xlabel('Time (seconds)')
    ax.set_ylabel('Hit Rate (%)')
    ax.set_title('Cache Hit Rate Over Time')
    ax.set_ylim(0, 105)
    ax.legend(loc='lower right')
    
    plt.tight_layout()
    
    filepath = output_dir / 'cache_hit_comparison.pdf'
    plt.savefig(filepath)
    plt.savefig(output_dir / 'cache_hit_comparison.png')
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
            <h2>📝 WordCount Benchmark</h2>
            
            <h3>Configuration</h3>
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
                    <div class="label">Window Size</div>
                    <div class="value">{{ wc_config.window_size | default('5000') }}ms</div>
                </div>
                <div class="config-item">
                    <div class="label">Slide Size</div>
                    <div class="value">{{ wc_config.slide_size | default('200') }}ms</div>
                </div>
                <div class="config-item">
                    <div class="label">Skew Factor</div>
                    <div class="value">{{ wc_config.skew_factor | default('1.1') }}</div>
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
            
            <h3>Latency Comparison</h3>
            <table>
                <thead>
                    <tr>
                        <th>Percentile</th>
                        <th>HashMap (ms)</th>
                        <th>ForL0 (ms)</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td><strong>P50</strong></td>
                        <td class="value-cell">{{ wc_hashmap_p50 }}</td>
                        <td class="value-cell">{{ wc_forl0_p50 }}</td>
                    </tr>
                    <tr>
                        <td><strong>P95</strong></td>
                        <td class="value-cell">{{ wc_hashmap_p95 }}</td>
                        <td class="value-cell">{{ wc_forl0_p95 }}</td>
                    </tr>
                    <tr>
                        <td><strong>P99</strong></td>
                        <td class="value-cell">{{ wc_hashmap_p99 }}</td>
                        <td class="value-cell">{{ wc_forl0_p99 }}</td>
                    </tr>
                    <tr>
                        <td><strong>Max</strong></td>
                        <td class="value-cell">{{ wc_hashmap_max }}</td>
                        <td class="value-cell">{{ wc_forl0_max }}</td>
                    </tr>
                </tbody>
            </table>
            
            <div class="figures-row">
                <div class="figure-container">
                    <img src="../figures/wordcount_throughput.png" alt="WordCount Throughput Comparison">
                    <p class="figure-caption">Figure 1: Throughput Comparison</p>
                </div>
                <div class="figure-container">
                    <img src="../figures/latency_cdf.png" alt="Latency CDF" onerror="this.parentElement.style.display='none'">
                    <p class="figure-caption">Figure 2: Latency CDF</p>
                </div>
            </div>
        </div>
        
        <!-- NexMark Section -->
        <div class="section">
            <h2>🏆 NexMark Benchmark</h2>
            
            <p style="color: var(--text-secondary); margin-bottom: 1rem;">
                NexMark is a standard benchmark for streaming systems, simulating an online auction scenario.
            </p>
            
            <table>
                <thead>
                    <tr>
                        <th>Query</th>
                        <th>Description</th>
                        <th>Events</th>
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
                        <td class="value-cell">{{ row.hashmap }}</td>
                        <td class="value-cell">{{ row.forl0 }}</td>
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
            </p>
            
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="value">{{ l0_final_hit_rate }}%</div>
                    <div class="label">L0 Hit Rate</div>
                </div>
                <div class="stat-card">
                    <div class="value">{{ l0_total_accesses }}</div>
                    <div class="label">Total Accesses</div>
                </div>
                <div class="stat-card">
                    <div class="value">{{ l0_eviction_count }}</div>
                    <div class="label">Evictions</div>
                </div>
                <div class="stat-card">
                    <div class="value">{{ l0_sample_count }}</div>
                    <div class="label">Samples Collected</div>
                </div>
            </div>
            
            <div class="figures-row">
                <div class="figure-container">
                    <img src="../figures/l0table_timeline.png" alt="L0Table Timeline" onerror="this.parentElement.style.display='none'">
                    <p class="figure-caption">Figure: L0Table Metrics Over Time</p>
                </div>
                <div class="figure-container">
                    <img src="../figures/cache_hit_comparison.png" alt="Cache Hit Comparison" onerror="this.parentElement.style.display='none'">
                    <p class="figure-caption">Figure: Cache Hit Rate Comparison</p>
                </div>
            </div>
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
            
            {% if not all_pass %}
            <div style="margin-top: 1rem; padding: 1rem; background: #fef3c7; border-radius: 0.5rem; border-left: 4px solid #f59e0b;">
                <strong>⚠️ Note:</strong> The current test was run in <strong>{{ mode }}</strong> mode. 
                {% if mode == 'local' %}
                ForL0 uses simulation mode on Mac, not real L0 Cache hardware. 
                The 60% improvement target is expected to be achieved on the production server (鲲鹏920 with L0 Cache).
                {% endif %}
            </div>
            {% endif %}
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
    mode = config.get('mode', 'local')
    mode_config = config.get(mode, {})
    wc_config = mode_config.get('wordcount', {})
    
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
    
    # Latency data
    hashmap_latency = wc_hashmap.get('latency_ms', {})
    forl0_latency = wc_forl0.get('latency_ms', {})
    
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
    queries = ['q4', 'q5', 'q8', 'q9', 'q11', 'q18', 'q19', 'q20']
    
    for query in queries:
        h_data = nexmark_results.get('hashmap', {}).get(query, {})
        f_data = nexmark_results.get('forl0', {}).get(query, {})
        
        h_tpc = h_data.get('throughput_per_core', h_data.get('throughput', 0))
        f_tpc = f_data.get('throughput_per_core', f_data.get('throughput', 0))
        events = h_data.get('events', f_data.get('events', 'N/A'))
        
        imp = ((f_tpc - h_tpc) / h_tpc * 100) if h_tpc > 0 and f_tpc > 0 else None
        
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
            'hashmap': f'{h_tpc:,.0f}' if h_tpc else 'N/A',
            'forl0': f'{f_tpc:,.0f}' if f_tpc else 'N/A',
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
    l0_metrics_available = l0_metrics is not None and len(l0_metrics.get('final_l0table', [])) > 0
    l0_final_hit_rate = 'N/A'
    l0_total_accesses = 'N/A'
    l0_eviction_count = 'N/A'
    l0_sources = 'None'
    l0_sample_count = 0
    
    if l0_metrics_available:
        final_stats = l0_metrics.get('final_l0table', [])
        sources = l0_metrics.get('sources', [])
        l0_sources = ', '.join(sources) if sources else 'Unknown'
        l0_sample_count = len(l0_metrics.get('l0table', []))
        
        if final_stats:
            # Aggregate from all tables
            total_hits = sum(s.get('hit_count', 0) for s in final_stats)
            total_acc = sum(s.get('access_count', 0) for s in final_stats)
            total_evict = sum(s.get('eviction_count', 0) for s in final_stats)
            
            l0_final_hit_rate = f"{(total_hits / total_acc * 100):.1f}" if total_acc > 0 else '0.0'
            l0_total_accesses = f"{total_acc:,}"
            l0_eviction_count = f"{total_evict:,}"
    
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
    
    # Render template
    template = Template(html_template)
    report = template.render(
        timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        summary=summary,
        all_pass=all_pass,
        mode=mode,
        parallelism=mode_config.get('parallelism', 8),
        total_benchmarks=total_benchmarks,
        wc_config=wc_config,
        wc_hashmap_tpc=f'{wc_hashmap_tpc:,.0f}' if wc_hashmap_tpc else 'N/A',
        wc_forl0_tpc=f'{wc_forl0_tpc:,.0f}' if wc_forl0_tpc else 'N/A',
        wc_tpc_imp=f'{wc_tpc_imp:.1f}',
        wc_badge_class=wc_badge_class,
        wc_imp_sign=wc_imp_sign,
        wc_negative_class=wc_negative_class,
        wc_hashmap_p50=hashmap_latency.get('p50', 'N/A'),
        wc_hashmap_p95=hashmap_latency.get('p95', 'N/A'),
        wc_hashmap_p99=hashmap_latency.get('p99', 'N/A'),
        wc_hashmap_max=hashmap_latency.get('max', 'N/A'),
        wc_forl0_p50=forl0_latency.get('p50', 'N/A'),
        wc_forl0_p95=forl0_latency.get('p95', 'N/A'),
        wc_forl0_p99=forl0_latency.get('p99', 'N/A'),
        wc_forl0_max=forl0_latency.get('max', 'N/A'),
        nexmark_rows=nexmark_rows,
        verification_rows=verification_rows,
        conclusion=conclusion,
        # [BENCHMARK_TEST] L0Table metrics variables
        l0_metrics_available=l0_metrics_available,
        l0_final_hit_rate=l0_final_hit_rate,
        l0_total_accesses=l0_total_accesses,
        l0_eviction_count=l0_eviction_count,
        l0_sources=l0_sources,
        l0_sample_count=l0_sample_count,
        # [BENCHMARK_TEST] Profiler/flame graph variables
        profiler_files=profiler_files if profiler_files else None,
        cache_supported=cache_supported,
        platform=platform.system()
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
    plot_latency_cdf(results, figures_dir)
    plot_improvement_summary(results, figures_dir)
    
    # [BENCHMARK_TEST] Generate L0Table metrics figures if available
    print("\nGenerating L0Table metrics figures (if available)...")
    plot_l0table_timeline(figures_dir)
    plot_cache_hit_comparison(figures_dir)
    
    # Generate report
    print("\nGenerating report...")
    generate_report(results, reports_dir)
    
    print("\n" + "=" * 60)
    print("Report generation complete!")
    print(f"Figures: {figures_dir}")
    print(f"Report:  {reports_dir / 'benchmark_report.html'}")
    print("=" * 60)
    
    # Open report in browser
    import webbrowser
    report_path = reports_dir / 'benchmark_report.html'
    webbrowser.open(f'file://{report_path}')


if __name__ == '__main__':
    main()
