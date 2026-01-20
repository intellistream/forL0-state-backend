#!/usr/bin/env python3
"""
Plot StateMap benchmark comparison chart.
Usage: python3 plot_statemap_comparison.py <results_dir> <output_png> [timestamp]
"""

import sys
import re
import os
import glob
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')

def parse_csv_results(results_dir, timestamp=None):
    """Parse CSV files to extract benchmark results with error bars.
    
    Filename format: StateMapBenchmark.method_MAPTYPE_TIMESTAMP.csv
    
    Returns dict: {method_name: {mapType: {'score': x, 'error': y}}}
    """
    results = {}
    
    pattern = os.path.join(results_dir, "*.csv")
    csv_files = glob.glob(pattern)
    
    if timestamp:
        csv_files = [f for f in csv_files if timestamp in f]
    
    for csv_file in csv_files:
        filename = os.path.basename(csv_file)
        
        # Format: StateMapBenchmark.method_MAPTYPE_TIMESTAMP.csv
        match = re.match(r'([^_]+)_([^_]+)_(\d{8}_\d{6})\.csv', filename)
        if not match:
            continue
        
        bench_name = match.group(1)
        map_type = match.group(2)
        file_timestamp = match.group(3)
        
        if timestamp and timestamp != file_timestamp:
            continue
        
        with open(csv_file, 'r') as f:
            lines = f.readlines()
            if len(lines) < 2:
                continue
            
            for data_line in lines[1:]:
                data_line = data_line.strip()
                if not data_line:
                    continue
                
                parts = data_line.split(',')
                if len(parts) >= 6:
                    try:
                        score = float(parts[4])
                        error = float(parts[5])
                    except ValueError:
                        continue
                    
                    if score != score:  # NaN check
                        continue
                    
                    full_bench_name = parts[0].strip('"')
                    name_parts = full_bench_name.split('.')
                    if len(name_parts) >= 2:
                        method = name_parts[-1]
                        if ':' in method:
                            continue
                        clean_name = method
                    else:
                        clean_name = full_bench_name
                    
                    # Use method as key, mapType as sub-key
                    if clean_name not in results:
                        results[clean_name] = {}
                    results[clean_name][map_type] = {'score': score, 'error': error}
    
    return results

def plot_chart(results, output_file):
    """Generate comparison bar chart with 2 bars per API (ForL0 vs CopyOnWrite)."""
    # Define display order for methods (StateMap interface methods)
    method_order = ['mapPut', 'mapGet', 'mapTransform', 'mapPutAndGetOld', 'mapContainsKey']
    benchmarks = [m for m in method_order if m in results]
    # Add any remaining methods not in the predefined order
    for m in sorted(results.keys()):
        if m not in benchmarks:
            benchmarks.append(m)
    
    # Define the 2 map types
    map_types = ['FORL0', 'COPYONWRITE']
    
    # Colors and labels
    colors = ['#F89E4F', '#5B8DBE']  # Orange for ForL0, Blue for CopyOnWrite
    labels = ['ForL0StateMap', 'CopyOnWriteStateMap']
    
    plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Arial']
    plt.rcParams['axes.unicode_minus'] = False
    
    fig, ax = plt.subplots(figsize=(14, 7))
    
    x = range(len(benchmarks))
    n_bars = len(map_types)
    width = 0.35
    
    all_scores = []
    all_errors = []
    bars_list = []
    
    for idx, map_type in enumerate(map_types):
        scores = [results[b].get(map_type, {}).get('score', 0) for b in benchmarks]
        errors = [results[b].get(map_type, {}).get('error', 0) for b in benchmarks]
        all_scores.extend(scores)
        all_errors.extend(errors)
        
        offset = (idx - (n_bars - 1) / 2) * width
        bars = ax.bar([i + offset for i in x], scores, width, 
                      label=labels[idx], color=colors[idx], edgecolor='none',
                      yerr=errors, capsize=3, error_kw={'elinewidth': 1, 'capthick': 1})
        bars_list.append((bars, errors))
    
    # Add value labels
    ymax = max(all_scores) if all_scores else 1
    for bars, errors in bars_list:
        for bar, err in zip(bars, errors):
            height = bar.get_height()
            if height > 0:
                label_y = height + err + (ymax * 0.01)
                if height >= 1000:
                    label_text = f'{height/1000:.1f}K'
                else:
                    label_text = f'{height:.0f}'
                ax.annotate(label_text,
                           xy=(bar.get_x() + bar.get_width() / 2, label_y),
                           ha='center', va='bottom',
                           fontsize=9, fontweight='bold',
                           color='#333333')
    
    ax.set_ylabel('Throughput (ops/ms)', fontsize=13)
    ax.set_title('StateMap Benchmark: ForL0StateMap vs CopyOnWriteStateMap\n'
                 '(Long key + VoidNamespace, SwissTableLongVoid specialization)', fontsize=13)
    ax.set_xticks(x)
    ax.set_xticklabels(benchmarks, rotation=0, ha='center', fontsize=11)
    
    max_error = max(all_errors) if all_errors else 0
    ax.set_ylim(0, (ymax + max_error) * 1.25)
    
    ax.legend(loc='upper right', frameon=True, fontsize=11)
    
    ax.grid(axis='y', alpha=0.3, linestyle='-', linewidth=0.5, color='gray')
    ax.set_axisbelow(True)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    print(f"Chart saved to: {output_file}")

def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <results_dir> <output_png> [timestamp]")
        sys.exit(1)
    
    results_dir = sys.argv[1]
    output_file = sys.argv[2]
    timestamp = sys.argv[3] if len(sys.argv) > 3 else None
    
    print(f"Parsing CSV files from: {results_dir}")
    if timestamp:
        print(f"Filtering by timestamp: {timestamp}")
    results = parse_csv_results(results_dir, timestamp)
    
    if not results:
        print("Error: No benchmark results found")
        sys.exit(1)
    
    print(f"Found {len(results)} benchmark methods:")
    for method, data in sorted(results.items()):
        print(f"  {method}: {list(data.keys())}")
    plot_chart(results, output_file)

if __name__ == '__main__':
    main()
