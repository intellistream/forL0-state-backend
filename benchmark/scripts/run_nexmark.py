#!/usr/bin/env python3
"""
Run NexMark benchmark.
"""

import argparse
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from utils.config import (
    load_config, get_mode_config, get_benchmark_root,
    get_nexmark_jar, get_flink_home, get_results_dir,
    get_timestamp, parse_json_from_output, save_result
)


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


def run_nexmark_query(query, backend, mode, config):
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
    else:
        # Cluster execution with flink run
        flink_bin = Path(flink_home) / 'bin' / 'flink'
        cmd = [
            str(flink_bin), 'run',
        ]
        if backend_class:
            cmd.extend([f'-Dstate.backend={backend_class}'])
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
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=7200  # 2 hour timeout
        )
        
        output = result.stdout + result.stderr
        print(output)
        
        # Parse result
        benchmark_result = parse_json_from_output(output)
        if benchmark_result:
            benchmark_result['query'] = query
            benchmark_result['backend'] = backend
            benchmark_result['events'] = events
            return benchmark_result
        else:
            # Try to extract metrics from NexMark output format
            return parse_nexmark_output(output, query, backend, events)
            
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


def run_nexmark(mode='local', backend='hashmap', queries=None):
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
        
        result = run_nexmark_query(query, backend, mode, config)
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
    
    args = parser.parse_args()
    
    # Parse queries
    if args.query == 'all':
        queries = list(NEXMARK_QUERIES.keys())
    else:
        queries = [q.strip() for q in args.query.split(',')]
    
    # Parse backends
    backends = ['hashmap', 'forl0'] if args.backend == 'all' else [args.backend]
    
    all_results = {}
    for backend in backends:
        results = run_nexmark(args.mode, backend, queries)
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
