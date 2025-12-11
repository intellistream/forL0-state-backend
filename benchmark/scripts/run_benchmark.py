#!/usr/bin/env python3
"""
Main benchmark runner - unified entry point for all benchmarks.

Usage:
    python run_benchmark.py --test all --backend all          # Run everything
    python run_benchmark.py --test wordcount --backend forl0  # WordCount only
    python run_benchmark.py --test nexmark --query q5,q8      # NexMark specific queries
    python run_benchmark.py --test all --profile              # With flame graphs
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from run_wordcount import run_wordcount, save_result
from run_nexmark import NexMarkRunner
from utils.config import load_config


def run_all_benchmarks(config, backends, profile=False):
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
        wc_result = run_wordcount(config, backend, enable_profile=profile)
        if wc_result:
            results['wordcount'][backend] = wc_result
    
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
    nexmark_results = results.get('nexmark', {})
    if nexmark_results:
        print("\n## NexMark Benchmark")
        print("-" * 50)
        
        # Detect if running on macOS (no per-core metrics)
        import platform
        is_macos = platform.system() == 'Darwin'
        if is_macos:
            print("Note: Per-core throughput not available on macOS (requires Linux /proc)")
        
        # Collect all queries from results
        all_queries = set()
        for backend_results in nexmark_results.values():
            query_results = backend_results.get('query_results', {})
            all_queries.update(query_results.keys())
        
        if all_queries:
            # Print header
            header = f"{'Query':<8}{'Time(s)':<10}"
            for backend in backends:
                header += f"{backend + ' (eps)':>18}"
                if not is_macos:
                    header += f"{'(/core)':>12}"
            if len(backends) == 2:
                header += f"{'Improv':>10}"
            print(header)
            print("-" * len(header))
            
            for query in sorted(all_queries):
                # Get time from first available backend
                time_str = ""
                for backend in backends:
                    if backend in nexmark_results:
                        qr = nexmark_results[backend].get('query_results', {}).get(query, {})
                        if qr.get('time_seconds'):
                            time_str = f"{qr['time_seconds']:.2f}"
                            break
                
                line = f"{query.upper():<8}{time_str:<10}"
                query_perfs = {}
                
                for backend in backends:
                    if backend in nexmark_results:
                        qr = nexmark_results[backend].get('query_results', {}).get(query, {})
                        tput = qr.get('throughput', 0)
                        tput_per_core = qr.get('throughput_per_core', 0)
                        query_perfs[backend] = tput
                        
                        # Format throughput (e.g., "1.65 M/s")
                        if tput >= 1000000:
                            line += f"{tput/1000000:>15.2f} M/s"
                        elif tput >= 1000:
                            line += f"{tput/1000:>15.2f} K/s"
                        else:
                            line += f"{tput:>15.0f} /s"
                        
                        if not is_macos:
                            if tput_per_core >= 1000000:
                                line += f"{tput_per_core/1000000:>9.2f} M/s"
                            elif tput_per_core >= 1000:
                                line += f"{tput_per_core/1000:>9.2f} K/s"
                            else:
                                line += f"{tput_per_core:>9.0f} /s"
                    else:
                        line += f"{'N/A':>18}"
                        if not is_macos:
                            line += f"{'N/A':>12}"
                
                # Calculate improvement
                if len(backends) == 2 and 'hashmap' in query_perfs and 'forl0' in query_perfs:
                    h = query_perfs['hashmap']
                    f = query_perfs['forl0']
                    if h > 0:
                        imp = ((f - h) / h) * 100
                        line += f"{imp:>9.1f}%"
                
                print(line)
            
            # Print total/average
            print("-" * len(header))
            total_line = f"{'Total':<8}{'':<10}"
            total_perfs = {}
            for backend in backends:
                if backend in nexmark_results:
                    qrs = nexmark_results[backend].get('query_results', {})
                    total_tput = sum(qr.get('throughput', 0) for qr in qrs.values())
                    total_perfs[backend] = total_tput
                    if total_tput >= 1000000:
                        total_line += f"{total_tput/1000000:>15.2f} M/s"
                    else:
                        total_line += f"{total_tput:>15.0f} /s"
                    if not is_macos:
                        total_line += f"{'N/A':>12}"
                else:
                    total_line += f"{'N/A':>18}"
                    if not is_macos:
                        total_line += f"{'N/A':>12}"
            
            if len(backends) == 2 and 'hashmap' in total_perfs and 'forl0' in total_perfs:
                h = total_perfs['hashmap']
                f = total_perfs['forl0']
                if h > 0:
                    imp = ((f - h) / h) * 100
                    total_line += f"{imp:>9.1f}%"
            print(total_line)
        else:
            print("No NexMark results available.")
    
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
  
  # Run with flame graph profiling
  python run_benchmark.py --test wordcount --backend all --profile
        """
    )
    
    parser.add_argument('--test', choices=['wordcount', 'nexmark', 'all'], default='all',
                       help='Test to run (default: all)')
    parser.add_argument('--backend', choices=['hashmap', 'forl0', 'all'], default='all',
                       help='State backend to use (default: all)')
    parser.add_argument('--query', type=str, default=None,
                       help='NexMark queries to run (comma-separated, e.g., q5,q8). Default: from config')
    parser.add_argument('--profile', '-p', action='store_true',
                       help='Enable async profiler for flame graphs')
    
    args = parser.parse_args()
    
    config = load_config()
    mode = config.get('mode', 'local')
    mode_config = config.get(mode, {})
    
    # Determine backends
    backends = ['hashmap', 'forl0'] if args.backend == 'all' else [args.backend]
    
    # Determine NexMark queries
    nexmark_queries = args.query if args.query else mode_config.get('nexmark', {}).get('queries', 'q5')
    
    print("=" * 60)
    print("ForL0 StateBackend Benchmark")
    print("=" * 60)
    print(f"Mode: {mode}")
    print(f"Test: {args.test}")
    print(f"Backends: {', '.join(backends)}")
    if args.test in ['nexmark', 'all']:
        print(f"NexMark Queries: {nexmark_queries}")
    if args.profile:
        print(f"Profiling: Enabled (flame graphs)")
    print("=" * 60)
    
    results = {'wordcount': {}, 'nexmark': {}}
    
    # Run WordCount benchmarks
    if args.test in ['wordcount', 'all']:
        print("\n" + "=" * 60)
        print("Running WordCount Benchmark")
        print("=" * 60)
        for backend in backends:
            result = run_wordcount(config, backend, enable_profile=args.profile)
            if result:
                results['wordcount'][backend] = result
                save_result(result, 'wordcount', backend, mode)
    
    # Run NexMark benchmarks
    if args.test in ['nexmark', 'all']:
        print("\n" + "=" * 60)
        print("Running NexMark Benchmark")
        print("=" * 60)
        try:
            runner = NexMarkRunner(config)
            nexmark_results = runner.run(
                backends=backends,
                queries=nexmark_queries,
                profile=args.profile,
                restart_cluster=True
            )
            # Store results in our format
            for backend, metrics in nexmark_results.items():
                results['nexmark'][backend] = metrics
        except FileNotFoundError as e:
            print(f"\n[Warning] NexMark not available: {e}")
            print("To run NexMark, first compile it:")
            print("  cd benchmark/nexmark-src && mvn clean package -DskipTests")
        except Exception as e:
            print(f"\n[Error] NexMark failed: {e}")
    
    # Print summary
    print_summary(results, backends)
    
    # Suggest next steps
    print("\nNext steps:")
    print("  1. Review raw results in: benchmark/results/raw/")
    print("  2. Generate report: python scripts/generate_report.py")
    print("  3. View report in: benchmark/results/reports/")


if __name__ == '__main__':
    main()
