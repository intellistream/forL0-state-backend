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

from utils.config import (
    load_config, get_mode_config, get_benchmark_root,
    get_wordcount_jar, get_flink_home, get_results_dir,
    get_timestamp, parse_json_from_output, save_result
)


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


def parse_taskmanager_log(flink_home: str, wc_config: dict, mode_config: dict) -> Optional[dict]:
    """Parse benchmark results from TaskManager log."""
    import glob
    
    log_pattern = f"{flink_home}/log/*taskexecutor*.log"
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
    }
    
    for yaml_key, flink_key in config_mapping.items():
        if yaml_key in backend_config:
            value = backend_config[yaml_key]
            # Convert Python bool to lowercase string
            if isinstance(value, bool):
                value = 'true' if value else 'false'
            args.append(f'-D{flink_key}={value}')
    
    return args


def run_wordcount(config: dict, backend: str) -> Optional[dict]:
    """Run WordCount benchmark on Flink cluster."""
    
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
    ])
    
    print(f"\n=== Running WordCount Benchmark ({backend} backend) ===\n")
    print(f"Flink cluster: {rest_url}")
    print(f"Command: {' '.join(cmd)}\n")
    
    try:
        # Submit job (blocking mode - wait for completion)
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600
        )
        
        output = result.stdout + result.stderr
        print(output)
        
        # Parse result from output - first try JSON from output
        benchmark_result = parse_json_from_output(output)
        
        # If no JSON result, try to parse Job Runtime from flink run output
        if not benchmark_result:
            benchmark_result = parse_job_runtime(output, wc_config, mode_config)
        
        # If still no result, try to read from TaskManager log
        if not benchmark_result:
            benchmark_result = parse_taskmanager_log(flink_home, wc_config, mode_config)
        
        if benchmark_result:
            benchmark_result['backend'] = backend
            benchmark_result['mode'] = mode
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
    
    args = parser.parse_args()
    
    config = load_config()
    backends = ['hashmap', 'forl0'] if args.backend == 'all' else [args.backend]
    results = {}
    
    for backend in backends:
        result = run_wordcount(config, backend)
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
