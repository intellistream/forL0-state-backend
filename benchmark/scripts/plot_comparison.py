#!/usr/bin/env python3
"""
Plot benchmark comparison chart from summary file.
Usage: python3 plot_comparison.py <summary_file> <output_png>
"""

import sys
import re
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend

def parse_summary(summary_file):
    """Parse summary file to extract benchmark results."""
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
                results[benchmark_name][current_backend] = score
    
    return results

def plot_chart(results, output_file):
    """Generate comparison bar chart matching reference style."""
    # Extract benchmark names and sort by category
    benchmarks = sorted(results.keys(), key=lambda x: (
        0 if x.startswith('ValueState') else 
        1 if x.startswith('ListState') else 2,
        x
    ))
    
    forl0_scores = [results[b].get('FORL0', 0) for b in benchmarks]
    heap_scores = [results[b].get('HEAP', 0) for b in benchmarks]
    
    # Set up the plot
    plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Arial']
    plt.rcParams['axes.unicode_minus'] = False
    
    fig, ax = plt.subplots(figsize=(16, 8))
    
    x = range(len(benchmarks))
    width = 0.35
    
    # Create bars - FORL0 (orange) on left, HEAP (blue) on right, matching reference
    bars1 = ax.bar([i - width/2 for i in x], forl0_scores, width, 
                    label='ForL0', color='#F89E4F', edgecolor='none')
    bars2 = ax.bar([i + width/2 for i in x], heap_scores, width, 
                    label='Heap', color='#5B8DBE', edgecolor='none')
    
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
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <summary_file> <output_png>")
        sys.exit(1)
    
    summary_file = sys.argv[1]
    output_file = sys.argv[2]
    
    print(f"Parsing summary file: {summary_file}")
    results = parse_summary(summary_file)
    
    if not results:
        print("Error: No benchmark results found in summary file")
        sys.exit(1)
    
    print(f"Found {len(results)} benchmark results")
    plot_chart(results, output_file)

if __name__ == '__main__':
    main()
