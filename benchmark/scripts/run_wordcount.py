#!/usr/bin/env python3
"""
Run WordCount benchmark via Flink cluster.

Both local and remote modes submit jobs to a Flink cluster via REST API.
- Local: Start Flink cluster locally with `$FLINK_HOME/bin/start-cluster.sh`
- Remote: Connect to remote Flink cluster

Usage:
    python run_wordcount.py --backend hashmap
    python run_wordcount.py --backend forl0
    python run_wordcount.py --backend all
    python run_wordcount.py --backend all --profile  # With flame graphs + hardware metrics
"""

import argparse
import json
import requests  # type: ignore
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from datetime import datetime
from utils.config import (
    load_config, get_mode_config, get_benchmark_root,
    get_wordcount_jar, get_flink_home, get_results_dir,
    get_timestamp, parse_json_from_output, save_result
)
from utils.profiler import AsyncProfiler, find_taskmanager_pids, get_profiler_summary
from utils.l0_metrics import (
    parse_l0table_metrics_by_time, 
    normalize_metrics_time,
    save_l0table_metrics as save_l0_metrics_file,
    get_l0_metrics_summary
)
from utils.hardware_metrics import HardwareMetricsCollector


def check_flink_cluster(rest_url: str) -> bool:
    """Check if Flink cluster is running."""
    try:
        resp = requests.get(f"{rest_url}/overview", timeout=5)
        return resp.status_code == 200
    except Exception:
        return False


def get_job_status(rest_url: str, job_id: str) -> dict:
    """Get job status from Flink REST API."""
    try:
        resp = requests.get(f"{rest_url}/jobs/{job_id}", timeout=10)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return {}


def wait_for_job_completion(rest_url: str, job_id: str, timeout: int = 3600) -> dict:
    """Wait for job to complete and return final status."""
    start_time = time.time()
    last_status = None
    
    while time.time() - start_time < timeout:
        status = get_job_status(rest_url, job_id)
        state = status.get('state', 'UNKNOWN')
        
        if state != last_status:
            print(f"  Job state: {state}")
            last_status = state
        
        if state in ['FINISHED', 'FAILED', 'CANCELED', 'CANCELLING']:
            return status
        
        time.sleep(2)
    
    print("ERROR: Job timed out")
    return {'state': 'TIMEOUT'}


def parse_job_runtime(output: str, wc_config: dict, mode_config: dict) -> Optional[dict]:
    """Parse Job Runtime from flink run output and calculate metrics."""
    import re
    
    # Look for "Job Runtime: 57578 ms"
    match = re.search(r'Job Runtime:\s*(\d+)\s*ms', output)
    if not match:
        return None
    
    runtime_ms = int(match.group(1))
    runtime_s = runtime_ms / 1000.0
    
    num_records = wc_config.get('num_records', 10000000)
    parallelism = mode_config.get('parallelism', 2)
    
    throughput = num_records / runtime_s
    throughput_per_core = throughput / parallelism
    
    return {
        'benchmark': 'wordcount',
        'total_time_seconds': runtime_s,
        'total_records': num_records,
        'parallelism': parallelism,
        'throughput': throughput,
        'throughput_per_core': throughput_per_core,
        'latency_ms': {
            'p50': None,
            'p95': None,
            'p99': None,
            'max': None
        }
    }


def parse_latency_file_path(output: str, flink_home: str) -> Optional[str]:
    """Parse latency samples file path from output or TaskManager stdout."""
    import re
    import glob
    
    # First try to find in output
    match = re.search(r'LATENCY_SAMPLES_FILE:(.+)', output)
    if match:
        return match.group(1).strip()
    
    # Try to find in TaskManager stdout (.out file)
    log_pattern = f"{flink_home}/log/*taskexecutor*.out"
    log_files = glob.glob(log_pattern)
    
    if log_files:
        log_file = max(log_files, key=lambda f: Path(f).stat().st_mtime)
        try:
            with open(log_file, 'r') as f:
                content = f.read()
            match = re.search(r'LATENCY_SAMPLES_FILE:(.+)', content)
            if match:
                return match.group(1).strip()
        except Exception:
            pass
    
    return None


def collect_l0_metrics_for_query(
    flink_home: str, 
    backend: str, 
    query: str, 
    start_time: datetime,
    results_dir: Path
) -> Optional[str]:
    """
    [BENCHMARK_TEST] Collect L0 metrics for a specific query run.
    
    Args:
        flink_home: Path to Flink installation
        backend: Backend name (e.g., "forl0")
        query: Query name (e.g., "wordcount")
        start_time: Job start timestamp - only collect metrics after this time
        results_dir: Directory to save the metrics file
        
    Returns:
        Path to the saved metrics file, or None if no metrics collected
    """
    end_time = datetime.now()
    
    # Parse metrics within time window
    metrics = parse_l0table_metrics_by_time(flink_home, start_time, end_time)
    if not metrics:
        print(f"  [L0 Metrics] No metrics found for {query}")
        return None
    
    # Normalize time to be relative to job start
    metrics = normalize_metrics_time(metrics, start_time)
    
    # Save to file
    filepath = save_l0_metrics_file(metrics, backend, query, results_dir)
    
    # Print summary
    summary = get_l0_metrics_summary(metrics)
    print(f"  [L0 Metrics] Collected {len(metrics)} samples for {query}")
    if summary:
        print(f"  [L0 Metrics] Overall hit rate: {summary.get('overall_hit_rate', 0):.1f}%")
    
    return filepath


def parse_taskmanager_log(flink_home: str, wc_config: dict, mode_config: dict) -> Optional[dict]:
    """Parse benchmark results from TaskManager stdout (.out file)."""
    import glob
    
    # Look for .out files (stdout), not .log files
    log_pattern = f"{flink_home}/log/*taskexecutor*.out"
    log_files = glob.glob(log_pattern)
    
    if not log_files:
        return None
    
    # Get the most recent log file
    log_file = max(log_files, key=lambda f: Path(f).stat().st_mtime)
    
    try:
        with open(log_file, 'r') as f:
            content = f.read()
        
        # Look for JSON_RESULT_START ... JSON_RESULT_END
        result = parse_json_from_output(content)
        if result:
            return result
    except Exception:
        pass
    
    return None


def get_forl0_config_args(config: dict, backend: str) -> list:
    """
    Get ForL0 StateBackend configuration as Flink -D arguments.
    
    Args:
        config: Full benchmark config
        backend: Backend name ('hashmap' or 'forl0')
        
    Returns:
        List of -D arguments for flink run command
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
        'arena_initial_size': 'state.backend.forl0.arena.initial-size',
    }
    
    for yaml_key, flink_key in config_mapping.items():
        if yaml_key in backend_config:
            value = backend_config[yaml_key]
            # Convert Python bool to lowercase string
            if isinstance(value, bool):
                value = 'true' if value else 'false'
            args.append(f'-D{flink_key}={value}')
    
    # [BENCHMARK_TEST] Enable L0Table metrics collector
    args.append('-DforL0.metricsCollector.enabled=true')
    
    return args


def run_wordcount(config: dict, backend: str, enable_profile: bool = False) -> Optional[dict]:
    """Run WordCount benchmark on Flink cluster.
    
    Args:
        config: Benchmark configuration
        backend: State backend to use ('hashmap' or 'forl0')
        enable_profile: Enable profiling (flame graphs + hardware metrics)
    """
    
    mode = config.get('mode', 'local')
    mode_config = get_mode_config(config, mode)
    wc_config = mode_config.get('wordcount', {})
    flink_config = config.get('flink', {})
    
    flink_home = get_flink_home()
    rest_url = flink_config.get('rest_url', 'http://localhost:8081')
    
    if not flink_home:
        print("ERROR: FLINK_HOME not set")
        return None
    
    # Check if cluster is running
    if not check_flink_cluster(rest_url):
        print(f"ERROR: Flink cluster not running at {rest_url}")
        print(f"  Start cluster with: {flink_home}/bin/start-cluster.sh")
        return None
    
    jar_path = get_wordcount_jar()
    if not jar_path:
        print("ERROR: WordCount JAR not found. Run 'mvn package -Plocal -DskipTests' first.")
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
    
    # Add ForL0-specific configuration parameters
    forl0_args = get_forl0_config_args(config, backend)
    cmd.extend(forl0_args)
    
    # Set up latency samples directory
    latency_dir = get_results_dir('latency')
    
    # Add JAR and arguments
    cmd.extend([
        jar_path,
        '--numKeys', str(wc_config.get('num_keys', 1000000)),
        '--numRecords', str(wc_config.get('num_records', 100000000)),
        '--arrivalRate', str(wc_config.get('arrival_rate', 230000)),
        '--skewFactor', str(wc_config.get('skew_factor', 1.1)),
        '--windowSize', str(wc_config.get('window_size', 5)),
        '--slideSize', str(wc_config.get('slide_size', 200)),
        '--parallelism', str(mode_config.get('parallelism', 2)),
        '--checkpointInterval', str(mode_config.get('checkpoint_interval', 10000)),
        '--latencyDir', str(latency_dir),
        '--backend', backend,
    ])
    
    print(f"\n=== Running WordCount Benchmark ({backend} backend) ===\n")
    print(f"Flink cluster: {rest_url}")
    print(f"Command: {' '.join(cmd)}\n")
    
    # [BENCHMARK_TEST] Initialize profiler and hardware metrics if enabled
    profiler = None
    profiler_files = {}
    hw_collector = None
    if enable_profile:
        profiler = AsyncProfiler()
        if profiler.is_available():
            print(f"  Async Profiler: {profiler.get_version()}")
            print(f"  Supported events: {profiler.get_supported_events()}")
        else:
            print("  WARNING: Async Profiler not available (set ASYNC_PROFILER_HOME)")
            profiler = None
        
        # Also enable hardware metrics collection when profiling
        hw_collector = HardwareMetricsCollector(str(get_results_dir('hardware')))
        print(f"  Hardware metrics: enabled (perf available: {hw_collector.is_perf_available()})")
    
    # [BENCHMARK_TEST] Record job start time for L0 metrics filtering
    job_start_time = datetime.now()
    
    try:
        # [BENCHMARK_TEST] Start profiling before job submission
        tm_pids = []
        if profiler:
            tm_pids = find_taskmanager_pids(flink_home)
            if tm_pids:
                print(f"  Profiling TaskManager PIDs: {tm_pids}")
                profiles_dir = get_results_dir('profiles')
                # Start profiling on first TaskManager (or all)
                for pid in tm_pids[:1]:  # Profile first TM only to reduce overhead
                    profiler.start(
                        pid=pid,
                        output_dir=str(profiles_dir),
                        backend=backend,
                        duration=None  # Will stop after job completes
                    )
            else:
                print("  WARNING: No TaskManager PIDs found for profiling")
        
        # [BENCHMARK_TEST] Start hardware metrics collection before job
        if hw_collector:
            if not tm_pids:
                tm_pids = find_taskmanager_pids(flink_home)
            if tm_pids:
                hw_collector.start_memory_collection(
                    pid=tm_pids[0],
                    query='wordcount',
                    backend=backend,
                    interval=1.0
                )
        
        # Submit job (blocking mode - wait for completion)
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600
        )
        
        output = result.stdout + result.stderr
        print(output)
        
        # [BENCHMARK_TEST] Stop hardware metrics collection
        if hw_collector:
            hw_collector.stop_memory_collection()
            hw_collector.save_results("wordcount_hw")
        
        # [BENCHMARK_TEST] Stop profiler and collect results
        if profiler and tm_pids:
            for pid in tm_pids[:1]:
                profiler_files = profiler.stop(pid)
            if profiler_files:
                print(f"  Profiler output files: {list(profiler_files.keys())}")
        
        # Parse result - first try from TaskManager log (has full metrics)
        benchmark_result = parse_taskmanager_log(flink_home, wc_config, mode_config)
        
        # If no result from log, try to parse Job Runtime from flink run output
        if not benchmark_result:
            benchmark_result = parse_job_runtime(output, wc_config, mode_config)
        
        # Parse latency samples file path from output
        latency_file = parse_latency_file_path(output, flink_home)
        
        # [BENCHMARK_TEST] Collect L0TABLE metrics for ForL0 backend with time filtering
        l0_metrics_file = None
        if backend == 'forl0':
            l0_metrics_file = collect_l0_metrics_for_query(
                flink_home=flink_home,
                backend=backend,
                query='wordcount',
                start_time=job_start_time,
                results_dir=get_results_dir('l0metrics')
            )
        
        if benchmark_result:
            benchmark_result['backend'] = backend
            benchmark_result['mode'] = mode
            if latency_file:
                benchmark_result['latency_samples_file'] = latency_file
            # [BENCHMARK_TEST] Include L0 metrics file path
            if l0_metrics_file:
                benchmark_result['l0_metrics_file'] = l0_metrics_file
            # [BENCHMARK_TEST] Include profiler output files
            if profiler_files:
                benchmark_result['profiler_files'] = profiler_files
            return benchmark_result
        else:
            print("WARNING: Could not parse benchmark result from output")
            return None
            
    except subprocess.TimeoutExpired:
        print("ERROR: Benchmark timed out")
        return None
    except Exception as e:
        print(f"ERROR: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description='Run WordCount Benchmark')
    parser.add_argument('--backend', choices=['hashmap', 'forl0', 'all'], default='hashmap',
                       help='State backend to use (default: hashmap)')
    parser.add_argument('--profile', action='store_true',
                       help='Enable profiling (flame graphs + hardware metrics)')
    
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
    
    config = load_config()
    backends = ['hashmap', 'forl0'] if args.backend == 'all' else [args.backend]
    results = {}
    
    for backend in backends:
        result = run_wordcount(config, backend, enable_profile=args.profile)
        if result:
            results[backend] = result
            save_result(result, 'wordcount', backend, config.get('mode', 'local'))
    
    # Print comparison if both backends were run
    if len(results) == 2:
        print("\n=== COMPARISON ===")
        hashmap = results.get('hashmap', {})
        forl0 = results.get('forl0', {})
        
        if 'throughput_per_core' in hashmap and 'throughput_per_core' in forl0:
            hashmap_tpc = hashmap['throughput_per_core']
            forl0_tpc = forl0['throughput_per_core']
            improvement = ((forl0_tpc - hashmap_tpc) / hashmap_tpc) * 100
            
            print(f"HashMapStateBackend: {hashmap_tpc:,.0f} records/s/core")
            print(f"ForL0StateBackend:   {forl0_tpc:,.0f} records/s/core")
            print(f"Improvement:         {improvement:+.1f}%")
            
            if improvement >= 60:
                print("\n✓ PASS: ForL0 achieves 60%+ improvement!")
            else:
                print(f"\n✗ FAIL: Improvement is {improvement:.1f}%, target is 60%")
    
    return results


if __name__ == '__main__':
    main()
