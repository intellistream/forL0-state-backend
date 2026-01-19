#!/usr/bin/env python3
"""
Run Unit Test Benchmark via Flink cluster.

A minimal and fully controllable benchmark for analyzing StateBackend behavior.

Usage:
    python run_unittest.py --backend hashmap
    python run_unittest.py --backend forl0
    python run_unittest.py --backend all
    
    # Custom parameters
    python run_unittest.py --backend forl0 --numKeys 10000 --stateSize 256 --numOperations 5000000
"""

import argparse
import subprocess
import sys
import time
import requests
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from utils.config import (
    load_config, get_flink_home, get_results_dir,
    get_timestamp, save_result
)
from utils.profiler import AsyncProfiler, find_taskmanager_pids
from utils.vtune_profiler import VTuneProfiler


def get_unittest_jar():
    """Get path to Unit Test benchmark JAR."""
    benchmark_root = Path(__file__).parent.parent
    unittest_dir = benchmark_root / "unit-test" / "target"
    
    # Find shaded JAR (not original)
    for jar in unittest_dir.glob("unit-test-benchmark-*.jar"):
        if "original" not in str(jar):
            return str(jar)
    return None


def check_flink_cluster(rest_url: str) -> bool:
    """Check if Flink cluster is running."""
    try:
        resp = requests.get(f"{rest_url}/overview", timeout=5)
        return resp.status_code == 200
    except Exception:
        return False


def run_unittest(config: dict, backend: str, 
                 num_keys: int, state_size: int, num_operations: int,
                 zipf_exponent: float, arrival_rate: int, 
                 profile_mode: str = None) -> dict:
    """Run Unit Test benchmark on Flink cluster."""
    
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
    
    jar_path = get_unittest_jar()
    if not jar_path:
        print("ERROR: Unit Test JAR not found.")
        print("  Run 'cd benchmark/unit-test && mvn package -DskipTests' first.")
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
    
    # Add JAR and arguments
    cmd.extend([
        jar_path,
        '--numKeys', str(num_keys),
        '--stateSize', str(state_size),
        '--numOperations', str(num_operations),
        '--zipfExponent', str(zipf_exponent),
        '--arrivalRate', str(arrival_rate),
    ])
    
    print(f"\n=== Running Unit Test Benchmark ({backend} backend) ===\n")
    print(f"Flink cluster: {rest_url}")
    print(f"Parameters: numKeys={num_keys}, stateSize={state_size}, numOperations={num_operations}")
    print(f"            zipfExponent={zipf_exponent}, arrivalRate={arrival_rate}")
    if profile_mode:
        print(f"Profiling: {profile_mode}")
    print(f"Command: {' '.join(cmd)}\n")
    
    # Initialize profilers if enabled
    profiler = None
    vtune_profiler = None
    
    if profile_mode:
        if profile_mode in ['uarch', 'memory', 'hotspots']:
            # VTune profiler
            vtune_profiler = VTuneProfiler()
            if vtune_profiler.is_available():
                print(f"  Intel VTune: {vtune_profiler.get_version()}")
            else:
                print("  WARNING: Intel VTune Profiler not available")
                vtune_profiler = None
        else:
            # Async-profiler (cpu/cache)
            profiler = AsyncProfiler()
            if profiler.is_available():
                print(f"  Async Profiler: {profiler.get_version()}")
            else:
                print("  WARNING: Async Profiler not available")
                profiler = None
    
    try:
        start_time = time.time()
        
        # Find TaskManager PID for profiling
        tm_pids = []
        if profile_mode:
            tm_pids = find_taskmanager_pids(flink_home)
            if not tm_pids:
                print("  WARNING: No TaskManager PIDs found for profiling")
        
        # Start profiling before job submission
        profiler_files = None
        vtune_result_dir = None
        
        if profiler and tm_pids:
            profiles_dir = get_results_dir('profiles')
            if profile_mode == 'cpu':
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
                profiler.start(
                    pid=tm_pids[0],
                    events=['cache-misses'],
                    output_dir=str(profiles_dir),
                    backend=backend,
                    output_format='html',
                    duration=None
                )
                print(f"  Started cache profiling (cache-misses)")
        
        if vtune_profiler and tm_pids:
            import threading
            
            def start_vtune_delayed():
                """Start VTune profiling after 20 second delay."""
                nonlocal vtune_result_dir
                vtune_result_dir = vtune_profiler.start(
                    pid=tm_pids[0],
                    analysis_type=profile_mode,
                    backend=backend,
                    query='unittest',
                    duration=60,
                    delay=20
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
        
        end_time = time.time()
        
        # Stop profiling
        if profiler and tm_pids:
            profiler_files = profiler.stop(tm_pids[0])
            if profiler_files:
                print(f"  Profiler output files: {list(profiler_files.keys())}")
        
        output = result.stdout + result.stderr
        print(output)
        
        # Check if job succeeded
        if result.returncode != 0 or 'Job execution failed' in output or 'Exception' in output:
            print("ERROR: Job failed")
            return None
        
        # Calculate result directly from wall time
        wall_time = end_time - start_time
        throughput = num_operations / wall_time
        
        benchmark_result = {
            'benchmark': 'unit-test',
            'backend': backend,
            'config': {
                'num_keys': num_keys,
                'state_size': state_size,
                'num_operations': num_operations,
                'zipf_exponent': zipf_exponent,
                'arrival_rate': arrival_rate,
            },
            'completed_operations': num_operations,
            'wall_time_seconds': wall_time,
            'throughput': throughput
        }
        
        # Include profiler output files if available
        if profiler_files:
            benchmark_result['profiler_files'] = profiler_files
        if vtune_result_dir:
            benchmark_result['vtune_result_dir'] = vtune_result_dir
            
        return benchmark_result
            
    except subprocess.TimeoutExpired:
        print("ERROR: Benchmark timed out")
        return None
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return None


def parse_output(output: str, num_keys: int, state_size: int, num_operations: int,
                 zipf_exponent: float, arrival_rate: int, backend: str) -> dict:
    """Parse benchmark result from output."""
    import re
    
    result = {
        'benchmark': 'unit-test',
        'backend': backend,
        'config': {
            'num_keys': num_keys,
            'state_size': state_size,
            'num_operations': num_operations,
            'zipf_exponent': zipf_exponent,
            'arrival_rate': arrival_rate,
        }
    }
    
    # Parse "Completed X operations in Y ms"
    match = re.search(r'Completed\s+(\d+)\s+operations\s+in\s+(\d+)\s+ms', output)
    if match:
        result['completed_operations'] = int(match.group(1))
        result['elapsed_ms'] = int(match.group(2))
        # Calculate accurate throughput from elapsed time
        result['throughput'] = result['completed_operations'] * 1000.0 / result['elapsed_ms']
    
    # Also try to parse "Throughput: X ops/sec" but prefer calculated one
    match = re.search(r'Throughput:\s+([\d.]+)\s+ops/sec', output)
    if match and 'throughput' not in result:
        result['throughput'] = float(match.group(1))
    
    if 'throughput' in result:
        return result
    
    return None


def parse_taskmanager_log(flink_home: str, num_keys: int, state_size: int, 
                          num_operations: int, zipf_exponent: float, 
                          arrival_rate: int, backend: str) -> dict:
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
        
        # Parse from the log content
        result = parse_output(content, num_keys, state_size, num_operations,
                             zipf_exponent, arrival_rate, backend)
        if result:
            return result
    except Exception as e:
        print(f"  Warning: Failed to parse TaskManager log: {e}")
    
    return None


def main():
    parser = argparse.ArgumentParser(description='Run Unit Test Benchmark')
    parser.add_argument('--backend', choices=['hashmap', 'forl0', 'all'], default='hashmap',
                       help='State backend to use (default: hashmap)')
    
    # Benchmark parameters
    parser.add_argument('--numKeys', type=int, default=1000,
                       help='Number of unique keys (default: 1000)')
    parser.add_argument('--stateSize', type=int, default=100,
                       help='State value size in bytes (default: 100)')
    parser.add_argument('--numOperations', type=int, default=1000000,
                       help='Total number of operations (default: 1000000)')
    parser.add_argument('--zipfExponent', type=float, default=0,
                       help='Zipf exponent, 0=uniform (default: 0)')
    parser.add_argument('--arrivalRate', type=int, default=0,
                       help='Arrival rate in ops/sec, 0=unlimited (default: 0)')
    
    args = parser.parse_args()
    
    config = load_config()
    backends = ['hashmap', 'forl0'] if args.backend == 'all' else [args.backend]
    results = {}
    
    for backend in backends:
        result = run_unittest(
            config, backend,
            num_keys=args.numKeys,
            state_size=args.stateSize,
            num_operations=args.numOperations,
            zipf_exponent=args.zipfExponent,
            arrival_rate=args.arrivalRate
        )
        if result:
            results[backend] = result
            save_result(result, 'unit-test', backend, config.get('mode', 'local'))
    
    # Print comparison if both backends were run
    if len(results) == 2:
        print("\n=== COMPARISON ===")
        hashmap = results.get('hashmap', {})
        forl0 = results.get('forl0', {})
        
        if 'throughput' in hashmap and 'throughput' in forl0:
            hashmap_tp = hashmap['throughput']
            forl0_tp = forl0['throughput']
            improvement = ((forl0_tp - hashmap_tp) / hashmap_tp) * 100
            
            print(f"HashMapStateBackend: {hashmap_tp:,.0f} ops/sec")
            print(f"ForL0StateBackend:   {forl0_tp:,.0f} ops/sec")
            print(f"Improvement:         {improvement:+.1f}%")
    
    return results


if __name__ == '__main__':
    main()
