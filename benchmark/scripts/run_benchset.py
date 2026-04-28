#!/usr/bin/env python3
"""
Run the seven-application paper benchset aligned with Zhang et al. 2017.

Available benchmarks:
    - wc: Stateful Word Count
    - fd: Fraud Detection
    - sd: Spike Detection
    - tm: Traffic Monitoring
    - lg: Log Processing
    - vs: Spam Detection in VoIP
    - lr: Linear Road

Usage:
        python run_benchset.py --backend all
        python run_benchset.py --backend forl0 --benchmark wc
        python run_benchset.py --backend all --profile cpu
"""

import argparse
import json
import re
import requests
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional, List, Dict

sys.path.insert(0, str(Path(__file__).parent))

from utils.config import (
    load_config, get_benchmark_root,
    get_flink_home, get_results_dir,
    get_timestamp, save_result
)
from utils.profiler import AsyncProfiler, find_taskmanager_pids
from generate_report import generate_benchset_paper_artifacts
from run_wordcount import run_wordcount, get_forl0_config_args


# Benchmark configurations from the paper
BENCHMARKS = ['wc', 'fd', 'sd', 'tm', 'lg', 'vs', 'lr']


def get_benchset_jar() -> Optional[str]:
    """Get path to benchset JAR file."""
    benchmark_root = get_benchmark_root()
    jar_path = benchmark_root / 'benchset' / 'target' / 'benchset-1.0-SNAPSHOT.jar'
    if jar_path.exists():
        return str(jar_path)
    return None


def check_flink_cluster(rest_url: str) -> bool:
    """Check if Flink cluster is running."""
    try:
        resp = requests.get(f"{rest_url}/overview", timeout=5)
        return resp.status_code == 200
    except Exception:
        return False


def submit_job_and_wait(cmd: list, rest_url: str, num_records: int, timeout: int = 7200) -> Optional[dict]:
    """Submit job asynchronously, wait for completion, and return results.
    
    Args:
        cmd: Flink run command
        rest_url: Flink REST API URL
        num_records: Number of input records (for throughput calculation)
        timeout: Job timeout in seconds
        
    Returns:
        Result dict with throughput = numRecords / time
    """
    
    # Submit in detached mode
    detached_cmd = cmd.copy()
    run_idx = detached_cmd.index('run')
    detached_cmd.insert(run_idx + 1, '-d')
    
    try:
        result = subprocess.run(
            detached_cmd,
            capture_output=True,
            text=True,
            timeout=120
        )
        
        output = result.stdout + result.stderr

        if result.returncode != 0:
            print("  ERROR: Job submission failed before JobID was returned")
            if output.strip():
                print(output.strip())
            return None
        
        # Parse job ID
        match = re.search(r'JobID\s*[:=]?\s*([A-Fa-f0-9]+)', output)
        if not match:
            print(f"  ERROR: Could not parse job ID from output")
            if output.strip():
                print(output.strip())
            return None
        
        job_id = match.group(1)
        print(f"  Job submitted: {job_id}")
        
    except subprocess.TimeoutExpired:
        print("  ERROR: Job submission timed out")
        return None
    except Exception as e:
        print(f"  ERROR: Job submission failed: {e}")
        return None
    
    # Wait for job completion
    start_time = time.time()
    last_state = None
    
    while time.time() - start_time < timeout:
        try:
            resp = requests.get(f"{rest_url}/jobs/{job_id}", timeout=10)
            if resp.status_code == 200:
                status = resp.json()
                state = status.get('state', 'UNKNOWN')
                
                if state != last_state:
                    print(f"  Job state: {state}")
                    last_state = state
                
                if state in ['FINISHED', 'FAILED', 'CANCELED']:
                    if state == 'FINISHED':
                        # Get job duration from REST API
                        duration_ms = status.get('duration', 0)
                        duration_sec = duration_ms / 1000.0
                        return calculate_throughput(num_records, duration_sec)
                    return None
        except Exception:
            pass
        
        time.sleep(2)
    
    print("  ERROR: Job timed out")
    return None


def calculate_throughput(num_records: int, duration_sec: float) -> Optional[dict]:
    """Calculate throughput from num_records and duration.
    
    Args:
        num_records: Number of input records
        duration_sec: Job duration in seconds
        
    Returns:
        Result dict with throughput = num_records / duration
    """
    if duration_sec <= 0:
        print("  WARN: Invalid duration")
        return None
    
    throughput = num_records / duration_sec
    
    return {
        'total_records': num_records,
        'time_seconds': duration_sec,
        'throughput': throughput
    }


def run_single_benchmark(
    config: dict, 
    backend: str, 
    benchmark_name: str,
    profile_mode: Optional[str] = None
) -> Optional[dict]:
    """Run a single benchmark from the benchset.
    
    Args:
        config: Benchmark configuration
        backend: State backend ('hashmap' or 'forl0')
        benchmark_name: Benchmark name ('wc', 'fd', 'sd', 'tm', 'lg', 'vs', 'lr')
        profile_mode: Profiling mode ('cpu', 'cache', None)
        
    Returns:
        Result dict or None if failed
    """
    if benchmark_name == 'wc':
        return run_wordcount_via_original_benchmark(config, backend, profile_mode)

    runtime_config = config.get('runtime', {})
    benchset_config = config.get('benchset', {})
    benchmark_config = benchset_config.get(benchmark_name, {})
    flink_config = config.get('flink', {})
    
    flink_home = get_flink_home()
    rest_url = flink_config.get('rest_url', 'http://localhost:8081')
    
    if not flink_home:
        print("ERROR: FLINK_HOME not set")
        return None
    
    if not check_flink_cluster(rest_url):
        print(f"ERROR: Flink cluster not running at {rest_url}")
        return None
    
    jar_path = get_benchset_jar()
    if not jar_path:
        print("ERROR: Benchset JAR not found.")
        print("  Run: cd benchmark/benchset && mvn package -DskipTests")
        return None
    
    # Find backend class
    backends_list = {b['name']: b['class'] for b in config.get('backends', [])}
    backend_class = backends_list.get(backend, '')
    
    # Build flink run command
    flink_bin = Path(flink_home) / 'bin' / 'flink'
    
    cmd = [str(flink_bin), 'run']
    
    # Add state backend configuration
    if backend_class:
        cmd.append(f'-Dstate.backend.type={backend_class}')
    cmd.extend(get_forl0_config_args(config, backend))
    
    # Get benchmark-specific parameters
    num_keys = benchmark_config.get('num_keys', 1000000)
    num_records = benchmark_config.get('num_records', 100000000)
    skew_factor = benchmark_config.get('skew_factor', 0)
    batch_size = benchmark_config.get('batch_size', 10)
    
    # Add JAR and arguments
    cmd.extend([
        jar_path,
        '--benchmark', benchmark_name,
        '--numKeys', str(num_keys),
        '--numRecords', str(num_records),
        '--skewFactor', str(skew_factor),
        '--batchSize', str(batch_size),
        '--parallelism', str(runtime_config.get('parallelism', 8)),
        '--checkpointInterval', str(runtime_config.get('checkpoint_interval', 0)),
        '--backend', backend,
    ])
    
    print(f"\n=== Running {benchmark_name.upper()} Benchmark ({backend}) ===")
    print(f"  numKeys: {num_keys:,}")
    print(f"  numRecords: {num_records:,}")
    print(f"  batchSize: {batch_size}")
    print(f"  Command: {' '.join(cmd[:6])} ... {benchmark_name}\n")
    
    # Initialize profiler if enabled
    profiler = None
    tm_pids = []
    
    if profile_mode == 'cpu':
        profiler = AsyncProfiler()
        if profiler.is_available():
            tm_pids = find_taskmanager_pids(flink_home)
            if tm_pids:
                profiles_dir = get_results_dir('profiles')
                profiler.start(
                    pid=tm_pids[0],
                    events=['cpu', 'alloc'],
                    output_dir=str(profiles_dir),
                    backend=f"{backend}_{benchmark_name}",
                    output_format='html',
                    duration=None
                )
                print(f"  Started CPU profiling")
    
    try:
        # Submit job and wait for results
        # Throughput = numRecords / time_seconds
        parallelism = runtime_config.get('parallelism', 8)
        result = submit_job_and_wait(cmd, rest_url, num_records)
        
        # Stop profiler
        if profiler and tm_pids:
            profiler.stop(tm_pids[0])
            print(f"  Profiler stopped, files saved to profiles/")
        
        if result:
            result['backend'] = backend
            result['benchmark'] = benchmark_name
            result['parallelism'] = parallelism
            result['throughput_per_core'] = result['throughput'] / parallelism
            print(f"\n  Result: {result.get('throughput', 0):,.0f} records/s")
            print(f"          {result.get('throughput_per_core', 0):,.0f} records/s/core")
        
        return result
        
    except Exception as e:
        print(f"  ERROR: {e}")
        if profiler and tm_pids:
            profiler.stop(tm_pids[0])
        return None


def run_wordcount_via_original_benchmark(
    config: dict,
    backend: str,
    profile_mode: Optional[str] = None,
) -> Optional[dict]:
    """Run the benchset WC entry via the original WordCount benchmark."""
    print("\n=== Running WC Benchmark via original WordCount benchmark ===")
    result = run_wordcount(config, backend, profile_mode)
    if not result:
        return None

    result['benchmark'] = 'wc'
    if 'total_time_seconds' in result and 'time_seconds' not in result:
        result['time_seconds'] = result['total_time_seconds']
    return result


def run_benchset(
    config: dict, 
    backends: List[str], 
    benchmarks: List[str],
    profile_mode: Optional[str] = None
) -> Dict[str, Dict[str, dict]]:
    """Run the complete benchmark set.
    
    Args:
        config: Benchmark configuration
        backends: List of backends to test
        benchmarks: List of benchmarks to run
        profile_mode: Profiling mode
        
    Returns:
        Nested dict: results[benchmark_name][backend] = result
    """
    results = {b: {} for b in benchmarks}
    
    for benchmark in benchmarks:
        print(f"\n{'='*60}")
        print(f"  BENCHMARK: {benchmark.upper()}")
        print(f"{'='*60}")
        
        for backend in backends:
            result = run_single_benchmark(config, backend, benchmark, profile_mode)
            if result:
                results[benchmark][backend] = result
                save_result(result, f'benchset_{benchmark}', backend)
    
    return results


def print_benchset_summary(results: Dict[str, Dict[str, dict]], backends: List[str]):
    """Print summary of all benchmark results."""
    print("\n" + "=" * 70)
    print("                    BENCHSET SUMMARY")
    print("=" * 70)
    
    # Header
    header = f"{'Benchmark':<15}{'Metric':<20}"
    for backend in backends:
        header += f"{backend:>15}"
    if len(backends) == 2:
        header += f"{'Improvement':>12}"
    print(header)
    print("-" * len(header))
    
    total_improvement = []
    
    for benchmark, backend_results in results.items():
        # Throughput row
        row = f"{benchmark:<15}{'Throughput (M/s)':<20}"
        tputs = {}
        for backend in backends:
            if backend in backend_results:
                tput = backend_results[backend].get('throughput', 0)
                tputs[backend] = tput
                row += f"{tput/1e6:>15.2f}"
            else:
                row += f"{'N/A':>15}"
        
        # Improvement
        if len(backends) == 2 and 'hashmap' in tputs and 'forl0' in tputs:
            h, f = tputs['hashmap'], tputs['forl0']
            if h > 0:
                imp = ((f - h) / h) * 100
                row += f"{imp:>11.1f}%"
                total_improvement.append(imp)
        print(row)
        
        # Throughput/core row
        row = f"{'':<15}{'Per Core (M/s)':<20}"
        for backend in backends:
            if backend in backend_results:
                tpc = backend_results[backend].get('throughput_per_core', 0)
                row += f"{tpc/1e6:>15.2f}"
            else:
                row += f"{'N/A':>15}"
        print(row)
        print()
    
    # Average improvement
    if total_improvement:
        avg_imp = sum(total_improvement) / len(total_improvement)
        print("-" * 70)
        print(f"Average ForL0 improvement: {avg_imp:.1f}%")
        if avg_imp >= 60:
            print("Status: ✓ PASS (>= 60% improvement target)")
        else:
            print(f"Status: ✗ Below target (< 60% improvement)")
    
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description='ForL0 Benchmark Set Runner',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all benchmarks with both backends
  python run_benchset.py --backend all
  
  # Run specific benchmark with one backend
  python run_benchset.py --backend forl0 --benchmark iot
  
  # Run with CPU profiling
  python run_benchset.py --backend all --profile cpu
  
  # Run subset of benchmarks
  python run_benchset.py --backend all --benchmark adclick,userprofile
        """
    )
    
    parser.add_argument('--backend', choices=['hashmap', 'forl0', 'all'], default='all',
                       help='State backend to use (default: all)')
    parser.add_argument('--benchmark', type=str, default=None,
                       help='Benchmarks to run (comma-separated). Default: all')
    parser.add_argument('--profile', '-p', type=str, default=None,
                       choices=['cpu', 'cache'],
                       help='Enable profiling: cpu (flame graphs), cache (cache stats)')
    
    args = parser.parse_args()
    
    config = load_config()
    
    # Determine backends
    backends = ['hashmap', 'forl0'] if args.backend == 'all' else [args.backend]
    
    # Determine benchmarks
    if args.benchmark:
        benchmarks = [b.strip() for b in args.benchmark.split(',')]
        for b in benchmarks:
            if b not in BENCHMARKS:
                print(f"ERROR: Unknown benchmark '{b}'")
                print(f"Available: {', '.join(BENCHMARKS)}")
                sys.exit(1)
    else:
        benchmarks = BENCHMARKS
    
    print("=" * 60)
    print("ForL0 Benchmark Set - Seven Paper Workloads")
    print("=" * 60)
    print(f"Backends: {', '.join(backends)}")
    print(f"Benchmarks: {', '.join(benchmarks)}")
    if args.profile:
        print(f"Profiling: {args.profile}")
    print("=" * 60)
    
    # Check JAR exists
    if not get_benchset_jar():
        print("\nERROR: Benchset JAR not found. Build it first:")
        print("  cd benchmark/benchset && mvn package -DskipTests")
        sys.exit(1)
    
    # Run benchmarks
    results = run_benchset(config, backends, benchmarks, args.profile)
    
    # Print summary
    print_benchset_summary(results, backends)

    print("\nGenerating benchset paper figures...")
    artifacts = generate_benchset_paper_artifacts()
    if artifacts:
        print("Figures saved to: benchmark/results/figures/")
        print("Summary saved to: benchmark/results/reports/benchset_summary.md")
    
    print("\nResults saved to: benchmark/results/raw/")


if __name__ == '__main__':
    main()
