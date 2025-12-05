#!/usr/bin/env python3
"""
Main benchmark runner - unified entry point for all benchmarks.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from run_wordcount import run_wordcount, save_result
from run_nexmark import run_nexmark, NEXMARK_QUERIES
from utils.config import load_config, get_results_dir


def run_all_benchmarks(config, backends):
    """Run all benchmarks with specified backends."""
    results = {
        'wordcount': {},
        'nexmark': {}
    }
    
    for backend in backends:
        print(f"\n{'='*60}")
        print(f"Running all benchmarks with {backend} backend")
        print('='*60)
        
        # Run WordCount
        wc_result = run_wordcount(config, backend)
        if wc_result:
            results['wordcount'][backend] = wc_result
        
        # Run NexMark (TODO: update nexmark similarly)
        # nexmark_results = run_nexmark(config, backend)
        # results['nexmark'][backend] = nexmark_results
    
    return results


def print_summary(results, backends):
    """Print summary of all benchmark results."""
    print("\n" + "=" * 70)
    print("                    BENCHMARK SUMMARY")
    print("=" * 70)
    
    # WordCount summary
    print("\n## WordCount Benchmark")
    print("-" * 50)
    wc_results = results.get('wordcount', {})
    
    for backend in backends:
        if backend in wc_results:
            result = wc_results[backend]
            tput = result.get('throughput_per_core', 'N/A')
            if isinstance(tput, (int, float)):
                print(f"{backend:20s}: {tput:>15,.0f} records/s/core")
    
    # Calculate improvement
    if 'hashmap' in wc_results and 'forl0' in wc_results:
        hashmap_tpc = wc_results['hashmap'].get('throughput_per_core', 0)
        forl0_tpc = wc_results['forl0'].get('throughput_per_core', 0)
        if hashmap_tpc > 0:
            improvement = ((forl0_tpc - hashmap_tpc) / hashmap_tpc) * 100
            print(f"{'Improvement':20s}: {improvement:>15.1f}%")
            if improvement >= 60:
                print("Status: ✓ PASS (>= 60% improvement)")
            else:
                print(f"Status: ✗ FAIL (< 60% improvement)")
    
    # NexMark summary
    print("\n## NexMark Benchmark")
    print("-" * 50)
    nexmark_results = results.get('nexmark', {})
    
    print(f"{'Query':<10}", end='')
    for backend in backends:
        print(f"{backend:>20}", end='')
    if len(backends) == 2:
        print(f"{'Improvement':>15}", end='')
    print()
    print("-" * (10 + 20 * len(backends) + (15 if len(backends) == 2 else 0)))
    
    for query in NEXMARK_QUERIES.keys():
        print(f"{query.upper():<10}", end='')
        query_results = {}
        
        for backend in backends:
            if backend in nexmark_results and query in nexmark_results[backend]:
                tput = nexmark_results[backend][query].get('throughput_per_core', 
                       nexmark_results[backend][query].get('throughput', 'N/A'))
                query_results[backend] = tput
                if isinstance(tput, (int, float)):
                    print(f"{tput:>20,.0f}", end='')
                else:
                    print(f"{tput:>20}", end='')
            else:
                print(f"{'N/A':>20}", end='')
        
        # Calculate improvement
        if len(backends) == 2 and 'hashmap' in query_results and 'forl0' in query_results:
            h = query_results['hashmap']
            f = query_results['forl0']
            if isinstance(h, (int, float)) and isinstance(f, (int, float)) and h > 0:
                imp = ((f - h) / h) * 100
                print(f"{imp:>14.1f}%", end='')
        
        print()
    
    print("\n" + "=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description='ForL0 StateBackend Benchmark Runner',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all tests with both backends
  python run_benchmark.py --test all --backend all
  
  # Run only WordCount with ForL0 backend
  python run_benchmark.py --test wordcount --backend forl0
  
  # Run specific NexMark queries
  python run_benchmark.py --test nexmark --query q5,q8
        """
    )
    
    parser.add_argument('--test', choices=['wordcount', 'nexmark', 'all'], default='all',
                       help='Test to run (default: all)')
    parser.add_argument('--backend', choices=['hashmap', 'forl0', 'all'], default='all',
                       help='State backend to use (default: all)')
    parser.add_argument('--query', type=str, default='all',
                       help='NexMark queries to run (comma-separated, e.g., q5,q8)')
    
    args = parser.parse_args()
    
    config = load_config()
    mode = config.get('mode', 'local')
    
    # Determine backends
    backends = ['hashmap', 'forl0'] if args.backend == 'all' else [args.backend]
    
    print("=" * 60)
    print("ForL0 StateBackend Benchmark")
    print("=" * 60)
    print(f"Mode: {mode}")
    print(f"Test: {args.test}")
    print(f"Backends: {', '.join(backends)}")
    print("=" * 60)
    
    results = {'wordcount': {}, 'nexmark': {}}
    mode = config.get('mode', 'local')
    
    # Run benchmarks
    if args.test in ['wordcount', 'all']:
        for backend in backends:
            result = run_wordcount(config, backend)
            if result:
                results['wordcount'][backend] = result
                save_result(result, 'wordcount', backend, mode)
    
    if args.test in ['nexmark', 'all']:
        queries = None if args.query == 'all' else args.query.split(',')
        for backend in backends:
            # TODO: update nexmark similarly
            # nexmark_results = run_nexmark(config, backend, queries)
            # results['nexmark'][backend] = nexmark_results
            pass
    
    # Print summary
    print_summary(results, backends)
    
    # Suggest next steps
    print("\nNext steps:")
    print("  1. Review raw results in: benchmark/results/raw/")
    print("  2. Generate figures: python scripts/generate_report.py")
    print("  3. View figures in: benchmark/results/figures/")


if __name__ == '__main__':
    main()
