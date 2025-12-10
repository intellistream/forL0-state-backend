#!/usr/bin/env python3
"""
Run NexMark benchmark.

Usage:
    python run_nexmark.py --backend hashmap
    python run_nexmark.py --backend forl0 --query q5
    python run_nexmark.py --backend all --profile  # With flame graphs
"""

import argparse
import glob
import json
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from utils.config import (
    load_config, get_mode_config, get_benchmark_root,
    get_nexmark_jar, get_flink_home, get_results_dir,
    get_timestamp, parse_json_from_output, save_result
)
from utils.profiler import AsyncProfiler, find_taskmanager_pids, get_profiler_summary


# NexMark queries and their data volumes
NEXMARK_QUERIES = {
    'q4': {'events': 80000000, 'description': 'Average Selling Price by Category'},
    'q5': {'events': 80000000, 'description': 'Hot Items'},
    'q8': {'events': 100000000, 'description': 'Monitor New Users'},
    'q9': {'events': 40000000, 'description': 'Winning Bids'},
    'q11': {'events': 80000000, 'description': 'User Sessions'},
    'q18': {'events': 80000000, 'description': 'Find Last Bid'},
    'q19': {'events': 80000000, 'description': 'Auction Statistics'},
    'q20': {'events': 60000000, 'description': 'Expand Bid'},
}


def get_forl0_config_args(config, backend):
    """
    Get ForL0 StateBackend configuration as Flink -D arguments.
    """
    if backend != 'forl0':
        return []
    
    # Find forl0 backend config
    backend_config = None
    for b in config.get('backends', []):
        if b.get('name') == 'forl0':
            backend_config = b.get('config', {})
            break
    
    if not backend_config:
        return []
    
    args = []
    
    # Map YAML config keys to Flink configuration keys
    config_mapping = {
        'l0_cache_enabled': 'state.backend.forl0.l0-cache.enabled',
        'l0_cache_size': 'state.backend.forl0.l0-cache.size',
        'l0_cache_replacement_policy': 'state.backend.forl0.l0-cache.replacement-policy',
        'l0_memory_max_size': 'state.backend.forl0.l0-memory.max-size',
        'main_table_initial_size': 'state.backend.forl0.main-table.initial-size',
        'main_table_load_factor_threshold': 'state.backend.forl0.main-table.load-factor-threshold',
    }
    
    for yaml_key, flink_key in config_mapping.items():
        if yaml_key in backend_config:
            value = backend_config[yaml_key]
            if isinstance(value, bool):
                value = 'true' if value else 'false'
            args.append(f'-D{flink_key}={value}')
    
    # [BENCHMARK_TEST] Enable L0Table metrics collector
    args.append('-DforL0.metricsCollector.enabled=true')
    
    return args


def parse_l0table_metrics(flink_home):
    """
    [BENCHMARK_TEST] Parse L0TABLE_METRICS from TaskManager log.
    """
    log_pattern = f"{flink_home}/log/*taskexecutor*.log"
    log_files = glob.glob(log_pattern)
    
    if not log_files:
        return None
    
    log_file = max(log_files, key=lambda f: Path(f).stat().st_mtime)
    
    metrics = []
    try:
        with open(log_file, 'r') as f:
            for line in f:
                if 'L0TABLE_METRICS|' in line:
                    match = re.search(r'L0TABLE_METRICS\|(\{.+\})', line)
                    if match:
                        try:
                            json_data = json.loads(match.group(1))
                            metrics.append(json_data)
                        except json.JSONDecodeError:
                            pass
    except Exception:
        pass
    
    return metrics if metrics else None


def save_l0table_metrics(metrics, backend, query, results_dir):
    """Save L0TABLE metrics to JSON file."""
    timestamp = get_timestamp()
    filename = f"l0table_metrics_{backend}_{query}_{timestamp}.json"
    filepath = results_dir / filename
    
    with open(filepath, 'w') as f:
        json.dump({
            'backend': backend,
            'query': query,
            'timestamp': timestamp,
            'samples': metrics
        }, f, indent=2)
    
    return str(filepath)


def run_nexmark_query(query, backend, mode, config, enable_profile=False):
    """Run a single NexMark query."""
    mode_config = get_mode_config(config, mode)
    flink_home = get_flink_home()
    
    jar_path = get_nexmark_jar()
    if not jar_path:
        print("ERROR: NexMark JAR not found. Please download it to benchmark/lib/")
        return None
    
    # Get events count for this query
    if mode == 'local':
        events = mode_config.get('nexmark', {}).get('events', 100000)
    else:
        events_key = f'{query}_events'
        events = mode_config.get('nexmark', {}).get(events_key, NEXMARK_QUERIES[query]['events'])
    
    # Find backend class
    backends = {b['name']: b['class'] for b in config.get('backends', [])}
    backend_class = backends.get(backend, '')
    
    # Build command
    if mode == 'local':
        # Local execution with java
        cmd = [
            'java', '-cp', jar_path,
            'org.apache.flink.table.benchmark.BenchmarkMain',
            '--query', query,
            '--events', str(events),
        ]
        if backend_class:
            cmd.insert(1, f'-Dstate.backend={backend_class}')
        # [BENCHMARK_TEST] Add ForL0 metrics collector flag for local mode
        if backend == 'forl0':
            cmd.insert(1, '-DforL0.metricsCollector.enabled=true')
    else:
        # Cluster execution with flink run
        flink_bin = Path(flink_home) / 'bin' / 'flink'
        cmd = [
            str(flink_bin), 'run',
        ]
        if backend_class:
            cmd.extend([f'-Dstate.backend={backend_class}'])
        
        # [BENCHMARK_TEST] Add ForL0-specific configuration including metrics collector
        forl0_args = get_forl0_config_args(config, backend)
        cmd.extend(forl0_args)
        
        cmd.extend([
            '-Dexecution.checkpointing.interval=' + str(mode_config.get('checkpoint_interval', 10000)),
            jar_path,
            '--query', query,
            '--events', str(events),
            '--rate', '0',  # Max rate
        ])
    
    print(f"\n=== Running NexMark {query.upper()} ({backend} backend) ===")
    print(f"Events: {events:,}")
    print(f"Description: {NEXMARK_QUERIES[query]['description']}")
    print(f"Command: {' '.join(cmd)}\n")
    
    # [BENCHMARK_TEST] Initialize profiler if enabled
    profiler = None
    profiler_files = {}
    if enable_profile and mode != 'local':
        profiler = AsyncProfiler()
        if profiler.is_available():
            print(f"  Async Profiler: {profiler.get_version()}")
        else:
            print("  WARNING: Async Profiler not available")
            profiler = None
    
    try:
        # [BENCHMARK_TEST] Start profiling before job submission
        tm_pids = []
        if profiler:
            tm_pids = find_taskmanager_pids(flink_home)
            if tm_pids:
                print(f"  Profiling TaskManager PIDs: {tm_pids}")
                profiles_dir = get_results_dir('profiles')
                for pid in tm_pids[:1]:
                    profiler.start(
                        pid=pid,
                        output_dir=str(profiles_dir),
                        backend=f"{backend}_{query}",
                        duration=None
                    )
            else:
                print("  WARNING: No TaskManager PIDs found for profiling")
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=7200  # 2 hour timeout
        )
        
        output = result.stdout + result.stderr
        print(output)
        
        # [BENCHMARK_TEST] Stop profiler and collect results
        if profiler and tm_pids:
            for pid in tm_pids[:1]:
                profiler_files = profiler.stop(pid)
            if profiler_files:
                print(f"  Profiler output files: {list(profiler_files.keys())}")
        
        # [BENCHMARK_TEST] Parse L0TABLE metrics for ForL0 backend
        l0_metrics = None
        l0_metrics_file = None
        if backend == 'forl0' and flink_home:
            l0_metrics = parse_l0table_metrics(flink_home)
            if l0_metrics:
                l0_metrics_file = save_l0table_metrics(
                    l0_metrics, backend, query, get_results_dir('l0metrics'))
                print(f"  L0Table metrics saved to: {l0_metrics_file}")
                print(f"  L0Table samples collected: {len(l0_metrics)}")
        
        # Parse result
        benchmark_result = parse_json_from_output(output)
        if benchmark_result:
            benchmark_result['query'] = query
            benchmark_result['backend'] = backend
            benchmark_result['events'] = events
            # [BENCHMARK_TEST] Include L0 metrics file path
            if l0_metrics_file:
                benchmark_result['l0_metrics_file'] = l0_metrics_file
            if profiler_files:
                benchmark_result['profiler_files'] = profiler_files
            return benchmark_result
        else:
            # Try to extract metrics from NexMark output format
            result = parse_nexmark_output(output, query, backend, events)
            if result:
                if l0_metrics_file:
                    result['l0_metrics_file'] = l0_metrics_file
                if profiler_files:
                    result['profiler_files'] = profiler_files
            return result
            
    except subprocess.TimeoutExpired:
        print("ERROR: Benchmark timed out")
        return None
    except Exception as e:
        print(f"ERROR: {e}")
        return None


def parse_nexmark_output(output, query, backend, events):
    """Parse NexMark standard output format."""
    result = {
        'benchmark': 'nexmark',
        'query': query,
        'backend': backend,
        'events': events,
    }
    
    # NexMark typically outputs metrics in specific format
    # This is a placeholder - adjust based on actual NexMark output
    lines = output.split('\n')
    for line in lines:
        if 'throughput' in line.lower():
            try:
                # Try to extract number
                parts = line.split(':')
                if len(parts) >= 2:
                    value = float(''.join(c for c in parts[-1] if c.isdigit() or c == '.'))
                    result['throughput'] = value
            except:
                pass
        elif 'latency' in line.lower() and 'p99' in line.lower():
            try:
                parts = line.split(':')
                if len(parts) >= 2:
                    value = float(''.join(c for c in parts[-1] if c.isdigit() or c == '.'))
                    result['latency_p99'] = value
            except:
                pass
    
    return result if 'throughput' in result else None


def run_nexmark(mode='local', backend='hashmap', queries=None, enable_profile=False):
    """Run NexMark benchmark."""
    config = load_config()
    
    if queries is None:
        queries = list(NEXMARK_QUERIES.keys())
    elif isinstance(queries, str):
        queries = [queries]
    
    results = {}
    for query in queries:
        if query not in NEXMARK_QUERIES:
            print(f"WARNING: Unknown query {query}, skipping")
            continue
        
        result = run_nexmark_query(query, backend, mode, config, enable_profile=enable_profile)
        if result:
            results[query] = result
            save_result(result, f'nexmark_{query}', backend, mode)
    
    return results


def main():
    parser = argparse.ArgumentParser(description='Run NexMark Benchmark')
    parser.add_argument('--mode', choices=['local', 'cluster'], default='local',
                       help='Run mode (default: local)')
    parser.add_argument('--backend', choices=['hashmap', 'forl0', 'all'], default='hashmap',
                       help='State backend to use (default: hashmap)')
    parser.add_argument('--query', type=str, default='all',
                       help='Query to run (q4,q5,q8,q9,q11,q18,q19,q20 or all)')
    parser.add_argument('--profile', action='store_true',
                       help='Enable async-profiler for flame graphs (requires ASYNC_PROFILER_HOME)')
    
    args = parser.parse_args()
    
    # [BENCHMARK_TEST] Show profiler info if enabled
    if args.profile:
        summary = get_profiler_summary()
        print(f"\n=== Profiler Configuration ===")
        print(f"  Platform: {summary['platform']}")
        print(f"  Async Profiler: {'available' if summary['async_profiler_available'] else 'NOT AVAILABLE'}")
        if summary['async_profiler_available']:
            print(f"  Version: {summary['async_profiler_version']}")
            print(f"  Events: {summary['supported_events']}")
        if not summary['cache_events_supported']:
            print(f"  Note: CPU cache statistics not available on {summary['platform']}")
    
    # Parse queries
    if args.query == 'all':
        queries = list(NEXMARK_QUERIES.keys())
    else:
        queries = [q.strip() for q in args.query.split(',')]
    
    # Parse backends
    backends = ['hashmap', 'forl0'] if args.backend == 'all' else [args.backend]
    
    all_results = {}
    for backend in backends:
        results = run_nexmark(args.mode, backend, queries, enable_profile=args.profile)
        all_results[backend] = results
    
    # Print summary
    print("\n" + "=" * 60)
    print("NEXMARK BENCHMARK SUMMARY")
    print("=" * 60)
    
    for query in queries:
        print(f"\n{query.upper()}: {NEXMARK_QUERIES[query]['description']}")
        for backend in backends:
            if backend in all_results and query in all_results[backend]:
                result = all_results[backend][query]
                tput = result.get('throughput', 'N/A')
                if isinstance(tput, (int, float)):
                    print(f"  {backend}: {tput:,.0f} records/s")
                else:
                    print(f"  {backend}: {tput}")


if __name__ == '__main__':
    main()
