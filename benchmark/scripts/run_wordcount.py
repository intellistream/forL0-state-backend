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
    python run_wordcount.py --backend all --profile cpu      # Async-profiler flame graphs
    python run_wordcount.py --backend all --profile uarch    # VTune uarch analysis
    python run_wordcount.py --backend all --profile memory   # VTune memory analysis
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
from utils.vtune_profiler import VTuneProfiler, get_profiler_summary as get_vtune_summary
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


def cancel_job(rest_url: str, job_id: str) -> bool:
    """Cancel a running Flink job via REST API."""
    try:
        # Use PATCH to cancel (Flink 1.12+)
        resp = requests.patch(
            f"{rest_url}/jobs/{job_id}",
            params={'mode': 'cancel'},
            timeout=30
        )
        if resp.status_code in [200, 202]:
            return True
        # Fallback to DELETE for older versions
        resp = requests.delete(f"{rest_url}/jobs/{job_id}/cancel", timeout=30)
        return resp.status_code in [200, 202]
    except Exception as e:
        print(f"  WARNING: Failed to cancel job: {e}")
        return False


def submit_job_async(cmd: list, rest_url: str) -> Optional[str]:
    """Submit a Flink job asynchronously and return job ID."""
    import re
    
    # Run flink run in detached mode
    detached_cmd = cmd.copy()
    # Insert -d flag after 'flink run'
    run_idx = detached_cmd.index('run')
    detached_cmd.insert(run_idx + 1, '-d')
    
    try:
        result = subprocess.run(
            detached_cmd,
            capture_output=True,
            text=True,
            timeout=60
        )
        
        output = result.stdout + result.stderr
        
        # Parse job ID from output: "Job has been submitted with JobID <id>"
        match = re.search(r'JobID\s+([a-f0-9]+)', output)
        if match:
            return match.group(1)
        
        print(f"  WARNING: Could not parse job ID from output:\n{output}")
        return None
        
    except subprocess.TimeoutExpired:
        print("  ERROR: Job submission timed out")
        return None
    except Exception as e:
        print(f"  ERROR: Job submission failed: {e}")
        return None


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
        'main_table_load_factor_threshold': 'state.backend.forl0.main-table.load-factor-threshold',
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


def run_warmup_job(cmd: list, rest_url: str, warmup_duration: int, backend: str) -> bool:
    """
    Run a warmup job for JIT compilation and cache warming.
    
    Args:
        cmd: Flink run command
        rest_url: Flink REST API URL
        warmup_duration: Warmup duration in seconds
        backend: Backend name for logging
        
    Returns:
        True if warmup completed successfully
    """
    print(f"\n--- Warmup Phase ({warmup_duration}s) ---")
    print(f"  Submitting warmup job...")
    
    # Create warmup command: disable latency sampling and L0 metrics
    warmup_cmd = []
    skip_next = False
    for i, arg in enumerate(cmd):
        if skip_next:
            skip_next = False
            continue
        # Skip latency directory argument
        if arg == '--latencyDir':
            skip_next = True
            continue
        # Disable L0 metrics collector during warmup
        if 'metricsCollector.enabled=true' in arg:
            warmup_cmd.append(arg.replace('enabled=true', 'enabled=false'))
        else:
            warmup_cmd.append(arg)
    
    job_id = submit_job_async(warmup_cmd, rest_url)
    if not job_id:
        print("  WARNING: Failed to submit warmup job, skipping warmup")
        return False
    
    print(f"  Warmup job submitted: {job_id}")
    
    # Wait for warmup duration
    start_time = time.time()
    last_state = None
    
    while time.time() - start_time < warmup_duration:
        status = get_job_status(rest_url, job_id)
        state = status.get('state', 'UNKNOWN')
        
        if state != last_state:
            print(f"  Job state: {state}")
            last_state = state
        
        # If job finished early (e.g., data exhausted), that's fine
        if state in ['FINISHED', 'FAILED', 'CANCELED']:
            print(f"  Warmup job ended early with state: {state}")
            return state != 'FAILED'
        
        elapsed = int(time.time() - start_time)
        remaining = warmup_duration - elapsed
        if remaining > 0 and remaining % 10 == 0:
            print(f"  Warmup: {elapsed}s elapsed, {remaining}s remaining...")
        
        time.sleep(1)
    
    # Cancel warmup job
    print(f"  Warmup complete, cancelling job...")
    if cancel_job(rest_url, job_id):
        # Wait for job to be fully cancelled
        for _ in range(30):
            status = get_job_status(rest_url, job_id)
            state = status.get('state', 'UNKNOWN')
            if state in ['CANCELED', 'FINISHED', 'FAILED']:
                print(f"  Warmup job cancelled successfully")
                break
            time.sleep(0.5)
    else:
        print(f"  WARNING: Failed to cancel warmup job")
    
    # Small delay to let resources be released
    time.sleep(2)
    
    print(f"--- Warmup Phase Complete ---\n")
    return True


def run_wordcount(config: dict, backend: str, profile_mode: Optional[str] = None) -> Optional[dict]:
    """Run WordCount benchmark on Flink cluster.
    
    Args:
        config: Benchmark configuration
        backend: State backend to use ('hashmap' or 'forl0')
        profile_mode: Profiling mode ('cpu' for flame graphs, 'cache' for cache statistics, None to disable)
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
    
    # [BENCHMARK_TEST] Run warmup phase if configured
    warmup_duration = wc_config.get('warmup_duration', 0)
    if warmup_duration > 0:
        run_warmup_job(cmd, rest_url, warmup_duration, backend)
    
    # [BENCHMARK_TEST] Initialize profiler and hardware metrics if enabled
    # Note: Profiling starts AFTER warmup to capture steady-state performance
    profiler = None
    vtune_profiler = None
    hw_collector = None
    
    if profile_mode:
        # Determine if using VTune or async-profiler
        if profile_mode in ['uarch', 'memory', 'hotspots']:
            # VTune profiler
            vtune_profiler = VTuneProfiler()
            if vtune_profiler.is_available():
                print(f"  Intel VTune: {vtune_profiler.get_version()}")
                analysis_type = vtune_profiler.ANALYSIS_TYPES[profile_mode]
                print(f"  Analysis type: {analysis_type}")
                print(f"  Note: VTune will start 20s after job begins (steady state)")
            else:
                print("  WARNING: Intel VTune Profiler not available")
                print("           Check if vtune is in PATH or set VTUNE_PROFILER_DIR")
                vtune_profiler = None
        else:
            # Async profiler (cpu/cache)
            profiler = AsyncProfiler()
            if profiler.is_available():
                print(f"  Async Profiler: {profiler.get_version()}")
                print(f"  Supported events: {profiler.get_supported_events()}")
                print(f"  Profiling mode: {profile_mode}")
            else:
                print("  WARNING: Async Profiler not available (set ASYNC_PROFILER_HOME)")
                profiler = None
        
        # Hardware metrics collection
        # - For 'cpu' mode: collect memory only (profiler handles CPU)
        # - For 'cache' mode: pass profiler for JFR analysis
        # - For VTune modes: no additional hardware collection (VTune handles it)
        if profile_mode == 'cache':
            hw_collector = HardwareMetricsCollector(str(get_results_dir('hardware')), profiler=profiler)
            print(f"  Hardware metrics: enabled (cache mode, perf available: {hw_collector.is_perf_available()})")
        elif profile_mode == 'cpu':
            # CPU mode: only collect memory, no cache stats
            hw_collector = HardwareMetricsCollector(str(get_results_dir('hardware')), profiler=None)
            print(f"  Hardware metrics: memory only (cpu mode)")
    
    # [BENCHMARK_TEST] Record job start time for L0 metrics filtering
    job_start_time = datetime.now()
    
    try:
        # [BENCHMARK_TEST] Start profiling before job submission
        tm_pids = []
        jfr_file = None
        profiler_files = None  # Initialize profiler_files
        vtune_result_dir = None
        
        # Find TaskManager PIDs
        if profiler or vtune_profiler:
            tm_pids = find_taskmanager_pids(flink_home)
            if not tm_pids:
                print("  WARNING: No TaskManager PIDs found for profiling")
        
        # Start async-profiler if needed (cpu/cache modes)
        if profiler and tm_pids:
            print(f"  Profiling TaskManager PIDs: {tm_pids}")
            profiles_dir = get_results_dir('profiles')
            
            if profile_mode == 'cpu':
                # CPU profiling: flame graphs
                profiler.start(
                    pid=tm_pids[0],
                    events=['cpu', 'alloc'],
                    output_dir=str(profiles_dir),
                    backend=backend,
                    output_format='html',
                    duration=None
                )
                print(f"  Started CPU profiling (cpu + alloc)")
            elif profile_mode == 'cache':
                # Cache profiling: HTML flame graph for cache-misses
                profiler.start(
                    pid=tm_pids[0],
                    events=['cache-misses'],
                    output_dir=str(profiles_dir),
                    backend=backend,
                    output_format='html',
                    duration=None
                )
                print(f"  Started cache profiling (cache-misses)")
        
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
                # [BENCHMARK_TEST] Start cache miss collection in background (only in cache mode)
                if profile_mode == 'cache':
                    hw_collector.start_cache_collection(
                        pid=tm_pids[0],
                        query='wordcount',
                        backend=backend
                    )
        
        # [BENCHMARK_TEST] Start VTune profiler in background thread (with 20s delay)
        vtune_thread = None
        if vtune_profiler and tm_pids:
            import threading
            
            def start_vtune_delayed():
                """Start VTune profiling after 20 second delay."""
                nonlocal vtune_result_dir
                # VTune results will be saved to ~/vtune-results (default)
                vtune_result_dir = vtune_profiler.start(
                    pid=tm_pids[0],
                    analysis_type=profile_mode,
                    backend=backend,
                    query='wordcount',
                    duration=60,  # Profile for 60 seconds
                    delay=20      # Wait 20 seconds before starting
                )
            
            vtune_thread = threading.Thread(target=start_vtune_delayed, daemon=True)
            vtune_thread.start()
            print(f"  VTune profiling will start in background (20s delay)")
        
        # Submit job (blocking mode - wait for completion)
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600
        )
        
        output = result.stdout + result.stderr
        print(output)
        
        # [BENCHMARK_TEST] Stop profiler first (generates flame graph)
        if profiler and tm_pids:
            for pid in tm_pids[:1]:
                profiler_files = profiler.stop(pid)
            if profiler_files:
                print(f"  Profiler output files: {list(profiler_files.keys())}")
        
        # [BENCHMARK_TEST] Stop hardware metrics collection
        if hw_collector:
            hw_collector.stop_memory_collection()
            if profile_mode == 'cache':
                cache_stats = hw_collector.stop_cache_collection()
                if cache_stats:
                    print(f"  [HW] Cache miss rate: {cache_stats.cache_miss_rate:.2%}")
            hw_collector.save_results("wordcount_hw")
        
        # [BENCHMARK_TEST] Wait for VTune thread to complete
        if vtune_thread:
            print("  Waiting for VTune profiling to complete...")
            vtune_thread.join(timeout=120)  # Wait up to 2 minutes
            if vtune_result_dir:
                print(f"  VTune results saved to: {vtune_result_dir}")
        
        # Parse result - first try from TaskManager log (has full metrics)
        benchmark_result = parse_taskmanager_log(flink_home, wc_config, mode_config)
        
        # If no result from log, try to parse Job Runtime from flink run output
        if not benchmark_result:
            benchmark_result = parse_job_runtime(output, wc_config, mode_config)
        
        # Parse latency samples file path from output
        latency_file = parse_latency_file_path(output, flink_home)
        
        if benchmark_result:
            benchmark_result['backend'] = backend
            benchmark_result['mode'] = mode
            if latency_file:
                benchmark_result['latency_samples_file'] = latency_file
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
        import traceback
        traceback.print_exc()
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
        result = run_wordcount(config, backend, profile_mode='cpu' if args.profile else None)
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
