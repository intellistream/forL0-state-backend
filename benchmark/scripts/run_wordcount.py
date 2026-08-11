#!/usr/bin/env python3
"""
Run WordCount benchmark via Flink cluster.

Submit jobs to a Flink cluster via REST API.
Start Flink cluster with `$FLINK_HOME/bin/start-cluster.sh`

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
import subprocess
import sys
import time
import os
import statistics
from pathlib import Path
from typing import Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))
from utils import requests_shim as requests  # type: ignore[assignment]

from datetime import datetime
from utils.config import (
    load_config, get_benchmark_root,
    get_wordcount_jar, get_flink_home, get_results_dir,
    get_timestamp, parse_json_from_output, save_result
)
from utils.forl0_config import build_forl0_config_args
from utils.profiler import AsyncProfiler, find_taskmanager_pids, get_profiler_summary
from utils.vtune_profiler import VTuneProfiler, get_profiler_summary as get_vtune_summary
from utils.hardware_metrics import HardwareMetricsCollector


def run_checked_command(command: list[str], timeout: int = 30) -> subprocess.CompletedProcess:
    """Run a command and raise on failure with captured stderr/stdout."""
    return subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=True,
    )


def start_container_profiler(
    profile_mode: str,
    backend: str,
) -> Optional[dict]:
    """Start async-profiler inside flink-taskmanager-1 container for Docker deployments."""
    profiler_home = os.environ.get('ASYNC_PROFILER_HOME')
    if not profiler_home:
        return None

    host_asprof = Path(profiler_home) / 'bin' / 'asprof'
    if not host_asprof.exists():
        return None

    container = 'flink-taskmanager-1'
    target_dir = '/tmp/async-profiler'
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    event = 'cpu' if profile_mode == 'cpu' else 'cache-misses'
    local_profiles_dir = get_results_dir('profiles')
    local_name = f'flamegraph_{event}_{backend}_{timestamp}.html'
    tmp_output = f'/tmp/{local_name}'

    try:
        run_checked_command([
            'sudo', '-n', 'docker', 'cp', str(Path(profiler_home)), f'{container}:{target_dir}'
        ], timeout=60)

        run_checked_command([
            'sudo', '-n', 'docker', 'exec', container,
            'sh', '-lc', f'{target_dir}/bin/asprof --version >/dev/null'
        ], timeout=15)

        # Clear any stale async-profiler session before starting a fresh capture.
        try:
            run_checked_command([
                'sudo', '-n', 'docker', 'exec', container,
                'sh', '-lc',
                (
                    f'{target_dir}/bin/asprof stop 1 >/dev/null 2>&1 || true; '
                    f'{target_dir}/bin/asprof stop -f {tmp_output} 1 >/dev/null 2>&1 || true'
                )
            ], timeout=15)
        except Exception:
            pass

        interval = '10000' if event == 'cache-misses' else '10ms'
        run_checked_command([
            'sudo', '-n', 'docker', 'exec', container,
            'sh', '-lc',
            f'{target_dir}/bin/asprof start -e {event} -i {interval} -f {tmp_output} 1'
        ], timeout=20)

        return {
            'container': container,
            'tmp_output': tmp_output,
            'local_output': str(local_profiles_dir / local_name),
            'event': event,
        }
    except Exception as error:
        print(f"  WARNING: Container profiler startup failed: {error}")
        return None


def stop_container_profiler(session: dict) -> Optional[str]:
    """Stop container profiler and copy output back to host."""
    container = session['container']
    tmp_output = session['tmp_output']
    local_output = session['local_output']

    try:
        run_checked_command([
            'sudo', '-n', 'docker', 'exec', container,
            'sh', '-lc', f'/tmp/async-profiler/bin/asprof stop -f {tmp_output} 1'
        ], timeout=60)

        run_checked_command([
            'sudo', '-n', 'docker', 'cp', f'{container}:{tmp_output}', local_output
        ], timeout=60)
        return local_output
    except Exception as error:
        print(f"  WARNING: Container profiler stop/copy failed: {error}")
        return None

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


def list_running_jobs(rest_url: str) -> list[dict]:
    """List currently RUNNING Flink jobs."""
    try:
        resp = requests.get(f"{rest_url}/jobs/overview", timeout=10)
        if resp.status_code != 200:
            return []
        jobs = resp.json().get('jobs', [])
        return [job for job in jobs if job.get('state') == 'RUNNING']
    except Exception:
        return []


def cleanup_running_jobs(rest_url: str, reason: str) -> None:
    """Best-effort cancellation of lingering jobs to avoid benchmark interference."""
    running = list_running_jobs(rest_url)
    if not running:
        return

    print(f"  [WordCount] Cleaning {len(running)} running job(s) before {reason}")
    for job in running:
        job_id = job.get('jid')
        name = job.get('name', 'unknown')
        if not job_id:
            continue
        if cancel_job(rest_url, job_id):
            print(f"    canceled {job_id} ({name})")
        else:
            print(f"    WARNING: failed to cancel {job_id} ({name})")

    # Give the cluster a short settling window after cancellation.
    time.sleep(2)


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


def all_sources_finished(job_status: dict) -> bool:
    """Return true when all source vertices report FINISHED."""
    vertices = job_status.get('vertices', [])
    source_vertices = [vertex for vertex in vertices if vertex.get('name', '').startswith('Source:')]
    return bool(source_vertices) and all(vertex.get('status') == 'FINISHED' for vertex in source_vertices)


def wait_for_bounded_job_completion(rest_url: str, job_id: str, timeout: int = 3600) -> tuple[str, bool, float]:
    """
    Poll a detached WordCount job until bounded input is consumed.

    Sliding processing-time windows can keep the streaming job RUNNING after
    the bounded source has finished.  Treat source completion as the benchmark
    end point, cancel the job for cleanup, and let the sink close print the
    machine-readable result.
    """
    start_time = time.time()
    bounded_completion_time = None
    auto_stopped = False
    last_state = None

    while time.time() - start_time < timeout:
        job_status = get_job_status(rest_url, job_id)
        state = job_status.get('state', 'UNKNOWN')

        if state != last_state:
            print(f"  Job state: {state}")
            last_state = state

        if state in ('FAILED', 'FINISHED'):
            effective_end = bounded_completion_time or time.time()
            return state, auto_stopped, effective_end - start_time

        if state == 'CANCELED':
            effective_end = bounded_completion_time or time.time()
            return ('AUTO_STOPPED' if auto_stopped else 'CANCELED'), auto_stopped, effective_end - start_time

        if bounded_completion_time is None and all_sources_finished(job_status):
            bounded_completion_time = time.time()
            print("  All sources finished; cancelling job for clean bounded benchmark shutdown...")
            auto_stopped = cancel_job(rest_url, job_id)
            if not auto_stopped:
                print("  WARNING: auto-cancel request failed; waiting for terminal state")

        time.sleep(2)

    if cancel_job(rest_url, job_id):
        print("  Timed out; cancellation requested for lingering WordCount job")
    effective_end = bounded_completion_time or time.time()
    return 'TIMEOUT', auto_stopped, effective_end - start_time


def parse_job_runtime(output: str, wc_config: dict, runtime_config: dict) -> Optional[dict]:
    """Parse Job Runtime from flink run output and calculate metrics."""
    import re
    
    # Look for "Job Runtime: 57578 ms"
    match = re.search(r'Job Runtime:\s*(\d+)\s*ms', output)
    if not match:
        return None
    
    runtime_ms = int(match.group(1))
    runtime_s = runtime_ms / 1000.0
    
    num_records = wc_config.get('num_records', 10000000)
    parallelism = runtime_config.get('parallelism', 2)
    
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


def build_wall_time_result(
    wc_config: dict,
    runtime_config: dict,
    wall_time_seconds: float,
    job_id: str,
    final_state: str,
    auto_stopped: bool,
) -> dict:
    """Build a result from REST-monitored wall time when sink JSON is unavailable."""
    num_records = wc_config.get('num_records', 10000000)
    parallelism = runtime_config.get('parallelism', 2)
    throughput = num_records / wall_time_seconds if wall_time_seconds > 0 else 0
    throughput_per_core = throughput / parallelism if parallelism > 0 else 0

    return {
        'benchmark': 'wordcount',
        'total_time_seconds': wall_time_seconds,
        'total_records': num_records,
        'parallelism': parallelism,
        'throughput': throughput,
        'throughput_per_core': throughput_per_core,
        'job_id': job_id,
        'job_state': final_state,
        'auto_stopped': auto_stopped,
        'latency_ms': {
            'p50': None,
            'p95': None,
            'p99': None,
            'max': None,
        },
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


def parse_taskmanager_log(flink_home: str, wc_config: dict, runtime_config: dict) -> Optional[dict]:
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


def get_forl0_config_args(config: dict, backend: str, workload_key: str = 'wordcount') -> list:
    """Get effective ForL0 arguments for a WordCount workload."""
    return build_forl0_config_args(
        config, backend, workload_key=workload_key,
        include_workload_section=True)


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
    
    runtime_config = config.get('runtime', {})
    wc_config = config.get('wordcount', {})
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

    cleanup_running_jobs(rest_url, f'wordcount:{backend}')
    
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
    forl0_args = get_forl0_config_args(config, backend, workload_key='wordcount')
    cmd.extend(forl0_args)
    
    checkpoint_interval = runtime_config.get(
        'wordcount_checkpoint_interval',
        runtime_config.get('checkpoint_interval', 0),
    )

    # Add JAR and arguments
    cmd.extend([
        jar_path,
        '--numKeys', str(wc_config.get('num_keys', 1000000)),
        '--numRecords', str(wc_config.get('num_records', 100000000)),
        '--arrivalRate', str(wc_config.get('arrival_rate', 0)),
        '--skewFactor', str(wc_config.get('skew_factor', 0)),
        '--workloadMode', str(wc_config.get('workload_mode', 'stateful_counter')),
        '--keyType', str(wc_config.get('key_type', 'string')),
        '--windowSize', str(wc_config.get('window_size', 5)),
        '--slideSize', str(wc_config.get('slide_size', 200)),
        '--parallelism', str(runtime_config.get('parallelism', 2)),
        '--checkpointInterval', str(checkpoint_interval),
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
        container_profiler_session = None
        
        # Find TaskManager PIDs
        if profiler or vtune_profiler:
            tm_pids = find_taskmanager_pids(flink_home)
            if not tm_pids:
                print("  WARNING: No TaskManager PIDs found for profiling")
        
        # Start async-profiler if needed (cpu/cache modes)
        if profile_mode in ['cpu', 'cache']:
            container_profiler_session = start_container_profiler(profile_mode, backend)
            if container_profiler_session:
                print(
                    f"  Started {profile_mode} profiling in container "
                    f"({container_profiler_session['container']}, event={container_profiler_session['event']})"
                )
            elif profiler and tm_pids:
                print(f"  Profiling TaskManager PIDs: {tm_pids}")
                profiles_dir = get_results_dir('profiles')

                if profile_mode == 'cpu':
                    started = profiler.start(
                        pid=tm_pids[0],
                        events=['cpu', 'alloc'],
                        output_dir=str(profiles_dir),
                        backend=backend,
                        output_format='html',
                        duration=None
                    )
                    if started:
                        print("  Started CPU profiling (host-side)")
                    else:
                        print("  WARNING: Host-side CPU profiler failed to start")
                elif profile_mode == 'cache':
                    started = profiler.start(
                        pid=tm_pids[0],
                        events=['cache-misses'],
                        output_dir=str(profiles_dir),
                        backend=backend,
                        output_format='html',
                        duration=None
                    )
                    if started:
                        print("  Started cache profiling (host-side)")
                    else:
                        print("  WARNING: Host-side cache profiler failed to start")
        
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
        
        timeout_seconds = int(wc_config.get('timeout_seconds', runtime_config.get('wordcount_timeout_seconds', 3600)))
        job_id = submit_job_async(cmd, rest_url)
        if not job_id:
            print("ERROR: Failed to submit WordCount job")
            return None

        print(f"  Job submitted: {job_id}")
        final_state, auto_stopped, wall_time = wait_for_bounded_job_completion(
            rest_url,
            job_id,
            timeout=timeout_seconds,
        )

        output = ""
        
        # [BENCHMARK_TEST] Stop profiler first (generates flame graph)
        if container_profiler_session:
            profiler_file = stop_container_profiler(container_profiler_session)
            if profiler_file:
                profiler_files = {'container': profiler_file}
                print(f"  Profiler output: {profiler_file}")
            else:
                print("  WARNING: Container profiler did not produce output")
        elif profiler and tm_pids:
            for pid in tm_pids[:1]:
                profiler_files = profiler.stop(pid)
            if profiler_files:
                print(f"  Profiler output files: {list(profiler_files.values())}")
        
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
        
        if final_state in ('FAILED', 'TIMEOUT', 'CANCELED'):
            print(f"ERROR: Job ended with state: {final_state}")
            return None

        # Parse result - first try from TaskManager log (has full metrics)
        benchmark_result = parse_taskmanager_log(flink_home, wc_config, runtime_config)
        
        # If no result from log, try to parse Job Runtime from flink run output
        if not benchmark_result:
            benchmark_result = parse_job_runtime(output, wc_config, runtime_config)

        if not benchmark_result:
            benchmark_result = build_wall_time_result(
                wc_config,
                runtime_config,
                wall_time,
                job_id,
                final_state,
                auto_stopped,
            )
        
        # Parse latency samples file path from output
        latency_file = parse_latency_file_path(output, flink_home)
        
        if benchmark_result:
            benchmark_result['backend'] = backend
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


def run_wordcount_scenario(config: dict, scenario: dict, backend: str,
                           profile_mode: Optional[str] = None) -> Optional[dict]:
    """Run a single WordCount scenario (from wordcount_scenarios list).

    This overrides the top-level wordcount config with scenario-specific values
    so that each scenario is self-contained.
    """
    # Merge scenario into the wordcount config temporarily
    wc_config = dict(config.get('wordcount', {}))
    for key in ('workload_mode', 'num_keys', 'num_records', 'skew_factor',
                'arrival_rate', 'window_size', 'slide_size', 'key_type',
                'forl0_overrides'):
        if key in scenario:
            wc_config[key] = scenario[key]

    runtime_config = dict(config.get('runtime', {}))
    if 'parallelism' in scenario:
        runtime_config['parallelism'] = scenario['parallelism']
    if 'checkpoint_interval' in scenario:
        runtime_config['wordcount_checkpoint_interval'] = scenario['checkpoint_interval']

    patched_config = dict(config)
    patched_config['wordcount'] = wc_config
    patched_config['runtime'] = runtime_config

    repeat_runs = int(scenario.get('repeat_runs', 1) or 1)
    repeat_policy = str(scenario.get('repeat_policy', 'single')).lower()
    samples = []
    for repeat_idx in range(repeat_runs):
        if repeat_runs > 1:
            print(f"\n--- WordCount scenario repeat {repeat_idx + 1}/{repeat_runs} ({repeat_policy}) ---")
        sample = run_wordcount(patched_config, backend, profile_mode=profile_mode)
        if not sample:
            return None
        sample['repeat_index'] = repeat_idx + 1
        samples.append(sample)

    if repeat_policy == 'best' and samples:
        result = max(samples, key=lambda item: item.get('throughput_per_core') or item.get('throughput') or 0)
    elif repeat_policy == 'median' and samples:
        result = dict(samples[-1])
        for field in ('throughput', 'throughput_per_core', 'total_time_seconds'):
            values = [sample.get(field) for sample in samples]
            numeric_values = [value for value in values if isinstance(value, (int, float))]
            if numeric_values:
                result[field] = float(statistics.median(numeric_values))
        result['repeat_aggregate'] = 'median'
    else:
        result = samples[-1] if samples else None
    if result:
        result['scenario'] = scenario.get('name', 'default')
        result['scenario_description'] = scenario.get('description', '')
        if repeat_runs > 1:
            result['repeat_runs'] = repeat_runs
            result['repeat_policy'] = repeat_policy
            result['repeat_samples'] = [
                {
                    'repeat_index': sample.get('repeat_index'),
                    'job_id': sample.get('job_id'),
                    'throughput': sample.get('throughput'),
                    'throughput_per_core': sample.get('throughput_per_core'),
                    'total_time_seconds': sample.get('total_time_seconds'),
                }
                for sample in samples
            ]
    return result


def print_scenario_comparison(scenario_name: str, results: dict):
    """Print a side-by-side comparison for a single scenario."""
    hashmap = results.get('hashmap', {})
    forl0 = results.get('forl0', {})
    if not hashmap or not forl0:
        return

    hm_tpc = hashmap.get('throughput_per_core', 0)
    fl_tpc = forl0.get('throughput_per_core', 0)
    hm_tp = hashmap.get('throughput', 0)
    fl_tp = forl0.get('throughput', 0)

    if hm_tpc > 0:
        improvement = ((fl_tpc - hm_tpc) / hm_tpc) * 100
    else:
        improvement = 0

    print(f"\n{'=' * 60}")
    print(f"  Scenario: {scenario_name}")
    if hashmap.get('scenario_description'):
        print(f"  {hashmap['scenario_description']}")
    print(f"{'=' * 60}")
    print(f"  {'Metric':<25s} {'HashMap':>14s} {'ForL0':>14s} {'Δ':>10s}")
    print(f"  {'-' * 63}")
    print(f"  {'Throughput (rec/s)':<25s} {hm_tp:>14,.0f} {fl_tp:>14,.0f} {improvement:>+9.1f}%")
    print(f"  {'Throughput/core':<25s} {hm_tpc:>14,.0f} {fl_tpc:>14,.0f} {improvement:>+9.1f}%")
    hm_time = hashmap.get('total_time_seconds', 0)
    fl_time = forl0.get('total_time_seconds', 0)
    print(f"  {'Total time (s)':<25s} {hm_time:>14.1f} {fl_time:>14.1f}")
    print(f"  {'Parallelism':<25s} {hashmap.get('parallelism', '?'):>14} {forl0.get('parallelism', '?'):>14}")
    print()


def main():
    parser = argparse.ArgumentParser(description='Run WordCount Benchmark')
    parser.add_argument('--backend', choices=['hashmap', 'forl0', 'all'], default='hashmap',
                       help='State backend to use (default: hashmap)')
    parser.add_argument('--profile', action='store_true',
                       help='Enable profiling (flame graphs + hardware metrics)')
    parser.add_argument('--scenario', type=str, default=None,
                       help='Run a specific scenario from wordcount_scenarios in config')
    parser.add_argument('--all-scenarios', action='store_true',
                       help='Run all scenarios from wordcount_scenarios in config')
    
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
    profile_mode = 'cpu' if args.profile else None

    # ---- Scenario-based execution ----
    scenarios = config.get('wordcount_scenarios', [])
    if args.all_scenarios and scenarios:
        all_results = {}
        for scenario in scenarios:
            sname = scenario.get('name', 'unknown')
            print(f"\n{'#' * 60}")
            print(f"# Scenario: {sname}")
            print(f"# {scenario.get('description', '')}")
            print(f"{'#' * 60}\n")
            all_results[sname] = {}
            for backend in backends:
                result = run_wordcount_scenario(config, scenario, backend, profile_mode=profile_mode)
                if result:
                    all_results[sname][backend] = result
                    tag = f"wordcount_{sname}"
                    save_result(result, tag, backend)
        # Print summary table
        print(f"\n\n{'#' * 60}")
        print(f"#  FULL SCENARIO COMPARISON SUMMARY")
        print(f"{'#' * 60}")
        for sname, sresults in all_results.items():
            print_scenario_comparison(sname, sresults)
        return all_results

    if args.scenario and scenarios:
        scenario = None
        for s in scenarios:
            if s.get('name') == args.scenario:
                scenario = s
                break
        if not scenario:
            print(f"ERROR: Scenario '{args.scenario}' not found in wordcount_scenarios")
            print(f"Available: {[s.get('name') for s in scenarios]}")
            return None
        results = {}
        for backend in backends:
            result = run_wordcount_scenario(config, scenario, backend, profile_mode=profile_mode)
            if result:
                results[backend] = result
                tag = f"wordcount_{scenario['name']}"
                save_result(result, tag, backend)
        if len(results) == 2:
            print_scenario_comparison(scenario['name'], results)
        return results

    # ---- Original single-mode execution (backward compatible) ----
    results = {}
    
    for backend in backends:
        result = run_wordcount(config, backend, profile_mode=profile_mode)
        if result:
            results[backend] = result
            save_result(result, 'wordcount', backend)
    
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
