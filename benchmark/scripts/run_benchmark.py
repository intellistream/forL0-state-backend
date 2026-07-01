#!/usr/bin/env python3
"""
Main benchmark runner - unified entry point for all benchmarks.

Usage:
    python run_benchmark.py --test all --backend all          # Run everything
    python run_benchmark.py --test wordcount --backend forl0  # WordCount only
    python run_benchmark.py --test nexmark --query q5,q8      # NexMark specific queries
    python run_benchmark.py --test client_usecase --backend all  # Client usecase benchmark
    python run_benchmark.py --test all --profile cpu          # With flame graphs
    python run_benchmark.py --test all --profile uarch        # With VTune uarch analysis
    python run_benchmark.py --test all --profile memory       # With VTune memory analysis
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from run_wordcount import run_wordcount, run_wordcount_scenario, save_result
from run_nexmark import NexmarkRunner, apply_nexmark_scenario
from run_unittest import run_unittest
from run_client_usecase import run_client_usecase
from utils.config import load_config


APP_TEST_GROUP = {'wordcount', 'nexmark', 'client_usecase', 'benchset'}


def run_all_benchmarks(config, backends, profile=False):
    """Run all benchmarks with specified backends."""
    results = {
        'wordcount': {},
        'nexmark': {},
        'unittest': {},
        'benchset': {}
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
    def fmt_rate(value, width=15):
        if value >= 1000000:
            return f"{value/1000000:>{width - 4}.2f} M/s"
        if value >= 1000:
            return f"{value/1000:>{width - 4}.2f} K/s"
        return f"{value:>{width - 3}.0f} /s"

    print("\n" + "=" * 70)
    print("                    BENCHMARK SUMMARY")
    print("=" * 70)
    
    # Unit Test summary
    print("\n## Unit Test Benchmark")
    print("-" * 50)
    ut_results = results.get('unittest', {})
    
    for backend in backends:
        if backend in ut_results:
            result = ut_results[backend]
            tput = result.get('throughput', 'N/A')
            if isinstance(tput, (int, float)):
                print(f"{backend:20s}: {tput:>15,.0f} ops/sec")
    
    # Calculate improvement
    if 'hashmap' in ut_results and 'forl0' in ut_results:
        hashmap_tp = ut_results['hashmap'].get('throughput', 0)
        forl0_tp = ut_results['forl0'].get('throughput', 0)
        if hashmap_tp > 0:
            improvement = ((forl0_tp - hashmap_tp) / hashmap_tp) * 100
            print(f"{'Improvement':20s}: {improvement:>15.1f}%")
    
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

    # Client usecase summary
    print("\n## Client Usecase Benchmark")
    print("-" * 50)
    client_results = results.get('client_usecase', {})

    for backend in backends:
        if backend in client_results:
            result = client_results[backend]
            tput = result.get('throughput_per_core', 'N/A')
            if isinstance(tput, (int, float)):
                print(f"{backend:20s}: {tput:>15,.0f} records/s/core")

    if 'hashmap' in client_results and 'forl0' in client_results:
        hashmap_tpc = client_results['hashmap'].get('throughput_per_core', 0)
        forl0_tpc = client_results['forl0'].get('throughput_per_core', 0)
        if hashmap_tpc > 0:
            improvement = ((forl0_tpc - hashmap_tpc) / hashmap_tpc) * 100
            print(f"{'Improvement':20s}: {improvement:>15.1f}%")
    
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
            header = f"{'Query':<8}{'Proc(s)':<10}"
            for backend in backends:
                header += f"{backend + ' proc eps':>18}"
                header += f"{'report eps':>15}"
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
                        if qr.get('process_elapsed_seconds'):
                            time_str = f"{qr['process_elapsed_seconds']:.2f}"
                            break
                        if qr.get('time_seconds'):
                            time_str = f"{qr['time_seconds']:.2f}"
                            break
                
                line = f"{query.upper():<8}{time_str:<10}"
                query_perfs = {}
                
                for backend in backends:
                    if backend in nexmark_results:
                        qr = nexmark_results[backend].get('query_results', {}).get(query, {})
                        tput = qr.get('throughput', 0)
                        process_tput = qr.get('process_throughput') or tput
                        tput_per_core = qr.get('throughput_per_core', 0)
                        query_perfs[backend] = process_tput
                        
                        line += fmt_rate(process_tput, 18)
                        line += fmt_rate(tput, 15)
                        
                        if not is_macos:
                            line += fmt_rate(tput_per_core, 12)
                    else:
                        line += f"{'N/A':>18}{'N/A':>15}"
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

    # Run client usecase benchmark
    python run_benchmark.py --test client_usecase --backend all
  
  # Run with async-profiler flame graphs
  python run_benchmark.py --test wordcount --backend all --profile cpu
  
  # Run with VTune microarchitecture analysis
  python run_benchmark.py --test wordcount --backend all --profile uarch
  
  # Run with VTune memory access analysis
  python run_benchmark.py --test wordcount --backend all --profile memory
        """
    )
    
    parser.add_argument('--test', choices=['unittest', 'wordcount', 'nexmark', 'client_usecase', 'benchset', 'apps', 'all'], default='all',
                       help='Test to run (default: all)')
    parser.add_argument('--backend', choices=['hashmap', 'forl0', 'all'], default='all',
                       help='State backend to use (default: all)')
    parser.add_argument('--query', '--queries', dest='query', type=str, default=None,
                       help='NexMark queries to run (comma-separated, e.g., q5,q8). Default: from config')
    parser.add_argument('--scenario', type=str, default=None,
                       help='Named scenario to apply for scenario-aware benchmarks (NexMark or WordCount).')
    parser.add_argument('--profile', '-p', type=str, default=None, 
                       choices=['cpu', 'cache', 'uarch', 'memory', 'hotspots'],
                       help='Enable profiling: cpu (flame graphs), cache (cache stats), '
                            'uarch (VTune uarch-exploration), memory (VTune memory-access), '
                            'hotspots (VTune hotspots with call stacks)')
    parser.add_argument('--mini-batch', action='store_true',
                       help='Enable mini-batch mode (buffer + sort by key, no pre-aggregation). '
                            'Overrides config file setting.')
    
    args = parser.parse_args()
    
    config = load_config()

    if args.scenario:
        if args.test in ['nexmark', 'apps', 'all']:
            config = apply_nexmark_scenario(config, args.scenario)
        elif args.test == 'wordcount':
            if not any(s.get('name') == args.scenario for s in config.get('wordcount_scenarios', [])):
                print(f"ERROR: WordCount scenario '{args.scenario}' not found.")
                print(f"Available: {[s.get('name') for s in config.get('wordcount_scenarios', [])]}")
                sys.exit(1)
        else:
            print(f"ERROR: --scenario is supported for NexMark and WordCount runs, got --test {args.test}")
            sys.exit(1)
    
    # Determine backends
    backends = ['hashmap', 'forl0'] if args.backend == 'all' else [args.backend]
    
    # Determine NexMark queries
    nexmark_queries = args.query if args.query else config.get('nexmark', {}).get('queries', 'q5')
    
    print("=" * 60)
    print("ForL0 StateBackend Benchmark")
    print("=" * 60)
    print(f"Test: {args.test}")
    print(f"Backends: {', '.join(backends)}")
    if args.test in ['nexmark', 'apps', 'all']:
        print(f"NexMark Queries: {nexmark_queries}")
        if args.scenario:
            print(f"NexMark Scenario: {args.scenario}")
    if args.test == 'wordcount' and args.scenario:
        print(f"WordCount Scenario: {args.scenario}")
    if args.profile:
        profile_desc = {
            'cpu': 'CPU flame graphs (async-profiler)',
            'cache': 'Cache statistics (async-profiler)',
            'uarch': 'Microarchitecture analysis (Intel VTune)',
            'memory': 'Memory access analysis (Intel VTune)'
        }.get(args.profile, args.profile)
        print(f"Profiling: {profile_desc}")
    if args.mini_batch:
        print(f"Mini-batch: ENABLED (buffer + sort by key, no pre-aggregation)")
    print("=" * 60)
    
    results = {'unittest': {}, 'wordcount': {}, 'nexmark': {}, 'client_usecase': {}, 'benchset': {}}
    run_app_suite = args.test in ['apps', 'all']
    
    # Run Unit Test benchmarks
    if args.test in ['unittest', 'all']:
        print("\n" + "=" * 60)
        print("Running Unit Test Benchmark")
        print("=" * 60)
        unittest_config = config.get('unittest', {})
        for backend in backends:
            result = run_unittest(
                config, backend,
                num_keys=unittest_config.get('num_keys', 1000),
                state_size=unittest_config.get('state_size', 100),
                num_operations=unittest_config.get('num_operations', 1000000),
                zipf_exponent=unittest_config.get('zipf_exponent', 0),
                arrival_rate=unittest_config.get('arrival_rate', 0),
                profile_mode=args.profile
            )
            if result:
                results['unittest'][backend] = result
                save_result(result, 'unittest', backend)
    
    # Run WordCount benchmarks
    if args.test == 'wordcount' or run_app_suite:
        print("\n" + "=" * 60)
        print("Running WordCount Benchmark")
        print("=" * 60)
        
        # Get mini-batch settings from config or CLI override
        wc_config = config.get('wordcount', {})
        scenario = None
        if args.scenario:
            for candidate in config.get('wordcount_scenarios', []):
                if candidate.get('name') == args.scenario:
                    scenario = candidate
                    break
        for backend in backends:
            if scenario:
                result = run_wordcount_scenario(config, scenario, backend, profile_mode=args.profile)
            else:
                result = run_wordcount(
                    config, backend,
                    profile_mode=args.profile
                )
            if result:
                results['wordcount'][backend] = result
                save_result(result, 'wordcount', backend)
    
    # Run NexMark benchmarks
    if args.test == 'nexmark' or run_app_suite:
        print("\n" + "=" * 60)
        print("Running NexMark Benchmark")
        print("=" * 60)
        try:
            runner = NexmarkRunner(config)
            nexmark_results = runner.run(
                backends=backends,
                queries=nexmark_queries,
                profile_mode=args.profile
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

    # Run client usecase benchmark
    if args.test == 'client_usecase' or run_app_suite:
        print("\n" + "=" * 60)
        print("Running Client Usecase Benchmark")
        print("=" * 60)
        client_config = config.get('client_usecase', {})
        for backend in backends:
            result = run_client_usecase(
                config,
                backend,
                profile_mode=args.profile,
            )
            if result:
                results['client_usecase'][backend] = result
                save_result(result, 'client_usecase', backend)
    
    # Run Benchset (seven paper workloads)
    if args.test == 'benchset' or run_app_suite:
        from run_benchset import run_benchset, print_benchset_summary, BENCHMARKS as BENCHSET_BENCHMARKS
        from generate_report import generate_benchset_paper_artifacts

        print("\n" + "=" * 60)
        print("Running Benchset (Seven Paper Workloads)")
        print("=" * 60)
        try:
            benchset_results = run_benchset(
                config, 
                backends, 
                BENCHSET_BENCHMARKS,
                profile_mode=args.profile
            )
            results['benchset'] = benchset_results
            # Print benchset-specific summary
            print_benchset_summary(benchset_results, backends)
            print("\nGenerating benchset paper figures...")
            artifacts = generate_benchset_paper_artifacts()
            if artifacts:
                print("Benchset figures generated in: benchmark/results/figures/")
        except FileNotFoundError as e:
            print(f"\n[Warning] Benchset not available: {e}")
            print("To run Benchset, first compile it:")
            print("  cd benchmark/benchset && mvn clean package -DskipTests")
        except Exception as e:
            print(f"\n[Error] Benchset failed: {e}")
            import traceback
            traceback.print_exc()
    
    # Print summary
    print_summary(results, backends)
    
    # Suggest next steps
    print("\nNext steps:")
    print("  1. Review raw results in: benchmark/results/raw/")
    print("  2. Generate report: python scripts/generate_report.py")
    print("  3. View report in: benchmark/results/reports/")


if __name__ == '__main__':
    main()
