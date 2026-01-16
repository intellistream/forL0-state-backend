#!/usr/bin/env python3
"""
Plot benchmark comparison chart from CSV result files.
Usage: python3 plot_comparison.py <results_dir> <output_png> [timestamp]
       python3 plot_comparison.py <summary_file> <output_png>  (legacy mode)
"""

import sys
import re
import os
import glob
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend

def parse_csv_results(results_dir, timestamp=None):
    """Parse CSV files to extract benchmark results with error bars."""
    results = {}
    
    # Find CSV files, optionally filter by timestamp
    pattern = os.path.join(results_dir, "*.csv")
    csv_files = glob.glob(pattern)
    
    if timestamp:
        csv_files = [f for f in csv_files if timestamp in f]
    
    for csv_file in csv_files:
        filename = os.path.basename(csv_file)
        # Parse filename: BenchmarkClass.method_BACKEND_YYYYMMDD_HHMMSS.csv
        match = re.match(r'([^_]+)_([^_]+)_(\d{8}_\d{6})\.csv', filename)
        if not match:
            continue
        
        bench_name = match.group(1)  # e.g., "ValueStateBenchmark.valueAdd"
        backend = match.group(2)      # e.g., "HEAP" or "FORL0"
        file_timestamp = match.group(3)  # e.g., "20260106_090102"
        
        # Filter by timestamp if specified
        if timestamp and timestamp != file_timestamp:
            continue
        
        with open(csv_file, 'r') as f:
            lines = f.readlines()
            if len(lines) < 2:
                continue
            
            # Parse ALL data lines (skip header), CSV may contain multiple benchmarks
            for data_line in lines[1:]:
                data_line = data_line.strip()
                if not data_line:
                    continue
                
                # CSV format: "Benchmark","Mode","Threads","Samples","Score","Score Error (99.9%)","Unit","Param: backendType"
                parts = data_line.split(',')
                if len(parts) >= 6:
                    try:
                        score = float(parts[4])
                        error = float(parts[5])
                    except ValueError:
                        continue
                    
                    # Skip async profiler entries (NaN scores)
                    if score != score:  # NaN check
                        continue
                    
                    # Extract actual benchmark name from CSV content
                    # e.g., "org.apache.flink.state.benchmark.ListStateBenchmark.listAddAll"
                    full_bench_name = parts[0].strip('"')
                    name_parts = full_bench_name.split('.')
                    if len(name_parts) >= 2:
                        state_type = name_parts[-2].replace('Benchmark', '')
                        method = name_parts[-1]
                        # Skip :async suffix entries
                        if ':' in method:
                            continue
                        clean_name = f"{state_type}.{method}"
                    else:
                        clean_name = full_bench_name
                    
                    if clean_name not in results:
                        results[clean_name] = {}
                    results[clean_name][backend] = {'score': score, 'error': error}
    
    return results

def parse_summary(summary_file):
    """Parse summary file to extract benchmark results (legacy mode, no error bars)."""
    results = {}
    
    with open(summary_file, 'r') as f:
        current_backend = None
        for line in f:
            line = line.strip()
            
            # Detect backend type
            if line.startswith('Backend:'):
                current_backend = line.split(':', 1)[1].strip()
            
            # Parse benchmark results: "  BenchmarkClass.method: score unit"
            match = re.match(r'\s*"[^"]+\.([^.]+)\.([^"]+)":\s+([\d.]+)\s+', line)
            if match and current_backend:
                state_type = match.group(1)  # ValueStateBenchmark -> ValueState
                method = match.group(2)       # valueGet -> Get
                score = float(match.group(3))
                
                # Clean up names
                state_type = state_type.replace('Benchmark', '')
                if method.startswith(state_type.lower()):
                    method = method[len(state_type):].capitalize()
                else:
                    method = method.capitalize()
                
                benchmark_name = f"{state_type}.{method}"
                
                if benchmark_name not in results:
                    results[benchmark_name] = {}
                results[benchmark_name][current_backend] = {'score': score, 'error': 0}
    
    return results

def plot_chart(results, output_file):
    """Generate comparison bar chart with error bars and value labels."""
    # Extract benchmark names and sort by category
    benchmarks = sorted(results.keys(), key=lambda x: (
        0 if x.startswith('ValueState') else 
        1 if x.startswith('ListState') else 2,
        x
    ))
    
    forl0_scores = [results[b].get('FORL0', {}).get('score', 0) for b in benchmarks]
    forl0_errors = [results[b].get('FORL0', {}).get('error', 0) for b in benchmarks]
    heap_scores = [results[b].get('HEAP', {}).get('score', 0) for b in benchmarks]
    heap_errors = [results[b].get('HEAP', {}).get('error', 0) for b in benchmarks]
    
    # Set up the plot
    plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Arial']
    plt.rcParams['axes.unicode_minus'] = False
    
    fig, ax = plt.subplots(figsize=(20, 8))  # Wider figure for more benchmarks
    
    x = range(len(benchmarks))
    width = 0.35
    
    # Create bars with error bars - FORL0 (orange) on left, HEAP (blue) on right
    bars1 = ax.bar([i - width/2 for i in x], forl0_scores, width, 
                    label='ForL0', color='#F89E4F', edgecolor='none',
                    yerr=forl0_errors, capsize=3, error_kw={'elinewidth': 1, 'capthick': 1})
    bars2 = ax.bar([i + width/2 for i in x], heap_scores, width, 
                    label='Heap', color='#5B8DBE', edgecolor='none',
                    yerr=heap_errors, capsize=3, error_kw={'elinewidth': 1, 'capthick': 1})
    
    # Add value labels - inside bar if tall enough, above bar if too short
    def add_value_labels(bars, errors):
        ymax = max(forl0_scores + heap_scores)
        threshold = ymax * 0.15  # If bar is less than 15% of max, put label outside
        
        for bar, err in zip(bars, errors):
            height = bar.get_height()
            if height > 0:
                if height >= threshold:
                    # Tall bar: label inside, white text, vertical
                    ax.annotate(f'{height:.0f}',
                               xy=(bar.get_x() + bar.get_width() / 2, height * 0.95),
                               ha='center', va='top',
                               fontsize=7, fontweight='bold',
                               color='white', rotation=90)
                else:
                    # Short bar: label above, dark text, horizontal
                    label_y = height + err + (ymax * 0.02)
                    ax.annotate(f'{height:.0f}',
                               xy=(bar.get_x() + bar.get_width() / 2, label_y),
                               ha='center', va='bottom',
                               fontsize=7, fontweight='bold',
                               color='#333333')
    
    add_value_labels(bars1, forl0_errors)
    add_value_labels(bars2, heap_errors)
    
    # Customize plot to match reference
    ax.set_ylabel('Throughput (ops/ms)', fontsize=13)
    ax.set_title('Flink State Backend Benchmark Comparison', fontsize=15)
    ax.set_xticks(x)
    
    # Format benchmark names - remove state type prefix for cleaner labels
    clean_names = []
    for b in benchmarks:
        # Remove StateType prefix: "ValueState.valueAdd" -> "value.add"
        parts = b.split('.')
        if len(parts) == 2:
            method = parts[1]
            # Remove state type prefix from method name
            for prefix in ['value', 'list', 'map']:
                if method.lower().startswith(prefix):
                    method = prefix + '.' + method[len(prefix):]
                    break
            clean_names.append(method)
        else:
            clean_names.append(b)
    
    # X-axis labels rotated like reference image
    ax.set_xticklabels(clean_names, rotation=45, ha='right', fontsize=10)
    
    # Adjust y-axis to make room for labels
    ymax = max(forl0_scores + heap_scores)
    max_error = max(forl0_errors + heap_errors) if forl0_errors and heap_errors else 0
    ax.set_ylim(0, (ymax + max_error) * 1.15)
    
    # Legend at bottom center like reference
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.15), 
             ncol=2, frameon=False, fontsize=12)
    
    # Grid only on y-axis like reference
    ax.grid(axis='y', alpha=0.3, linestyle='-', linewidth=0.5, color='gray')
    ax.set_axisbelow(True)
    
    # Remove top and right spines for cleaner look
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    print(f"Chart saved to: {output_file}")

def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <results_dir> <output_png> [timestamp]")
        print(f"       {sys.argv[0]} <summary_file> <output_png>  (legacy mode)")
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_file = sys.argv[2]
    timestamp = sys.argv[3] if len(sys.argv) > 3 else None
    
    # Determine if input is a directory or file
    if os.path.isdir(input_path):
        print(f"Parsing CSV files from: {input_path}")
        if timestamp:
            print(f"Filtering by timestamp: {timestamp}")
        results = parse_csv_results(input_path, timestamp)
    else:
        print(f"Parsing summary file: {input_path} (legacy mode, no error bars)")
        results = parse_summary(input_path)
    
    if not results:
        print("Error: No benchmark results found")
        sys.exit(1)
    
    print(f"Found {len(results)} benchmark results")
    plot_chart(results, output_file)

if __name__ == '__main__':
    main()
