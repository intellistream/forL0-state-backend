#!/usr/bin/env python3
"""
Run client usecase benchmark via Flink cluster.

This integrates client_usecase/XX_6000c_Demo into the unified benchmark flow.
The workload is bounded by total input records from benchmark.yaml so it can be
configured like WordCount.
"""

import argparse
import math
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent))
from utils import requests_shim as requests  # type: ignore[assignment]

from utils.config import load_config, get_flink_home, save_result
from utils.profiler import AsyncProfiler, find_taskmanager_pids


def get_forl0_config_args(config: dict, backend: str, workload_key: str = 'client_usecase') -> list:
    """Get ForL0 backend config args and merge workload-specific overrides."""
    if backend != 'forl0':
        return []

    backend_config = None
    for b in config.get('backends', []):
        if b.get('name') == 'forl0':
            backend_config = b.get('config', {})
            break

    if not backend_config:
        return []

    effective_config = dict(backend_config)
    workload_overrides = backend_config.get('workload_overrides', {})
    if isinstance(workload_overrides, dict):
        workload_cfg = workload_overrides.get(workload_key, {})
        if isinstance(workload_cfg, dict):
            effective_config.update(workload_cfg)

    config_mapping = {
        'initial_table_capacity': 'state.backend.forl0.initial-table-capacity',
        'max_table_capacity': 'state.backend.forl0.max-table-capacity',
        'l0_cache_enabled': 'state.backend.forl0.l0-cache.enabled',
        'l0_cache_size': 'state.backend.forl0.l0-cache.size',
        'l0_cache_replacement_policy': 'state.backend.forl0.l0-cache.replacement-policy',
        'l0_memory_max_size': 'state.backend.forl0.l0-memory.max-size',
        'main_table_load_factor_threshold': 'state.backend.forl0.main-table.load-factor-threshold',
    }

    args = []
    for yaml_key, flink_key in config_mapping.items():
        if yaml_key in effective_config:
            value = effective_config[yaml_key]
            if isinstance(value, bool):
                value = 'true' if value else 'false'
            args.append(f'-D{flink_key}={value}')

    args.append('-DforL0.metricsCollector.enabled=true')
    return args


def get_client_usecase_jar() -> Optional[str]:
    """Get path to the packaged client usecase JAR."""
    project_root = Path(__file__).parent.parent.parent
    target_dir = project_root / 'client_usecase' / 'XX_6000c_Demo' / 'target'
    deploy_dir = project_root / 'docker' / 'deploy'

    preferred_patterns = [
        'flink-keyedcoprocessfunction-example-*-jar-with-dependencies.jar',
        '*-jar-with-dependencies.jar',
        '*.jar',
    ]

    for search_dir in [target_dir, deploy_dir]:
        for pattern in preferred_patterns:
            for jar in sorted(search_dir.glob(pattern)):
                if 'original' not in jar.name and 'sources' not in jar.name and 'javadoc' not in jar.name:
                    return str(jar)
    return None


def check_flink_cluster(rest_url: str) -> bool:
    """Check if Flink cluster is running."""
    try:
        resp = requests.get(f"{rest_url}/overview", timeout=5)
        return resp.status_code == 200
    except Exception:
        return False


def get_job_status(rest_url: str, job_id: str) -> dict:
    """Fetch detailed Flink job status."""
    try:
        resp = requests.get(f"{rest_url}/jobs/{job_id}", timeout=10)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return {}


def cancel_job(rest_url: str, job_id: str) -> bool:
    """Cancel a Flink job via REST."""
    try:
        resp = requests.patch(
            f"{rest_url}/jobs/{job_id}",
            params={'mode': 'cancel'},
            timeout=30,
        )
        if resp.status_code in (200, 202):
            return True
        resp = requests.delete(f"{rest_url}/jobs/{job_id}/cancel", timeout=30)
        return resp.status_code in (200, 202)
    except Exception:
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

    print(f"  [ClientUsecase] Cleaning {len(running)} running job(s) before {reason}")
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


def submit_job_async(cmd: list[str]) -> Optional[str]:
    """Submit the Flink job detached and return its JobID."""
    detached_cmd = cmd.copy()
    run_index = detached_cmd.index('run')
    detached_cmd.insert(run_index + 1, '-d')

    result = subprocess.run(
        detached_cmd,
        capture_output=True,
        text=True,
        timeout=60,
    )
    output = result.stdout + result.stderr
    print(output)

    if result.returncode != 0:
        return None

    match = re.search(r'JobID\s+([a-f0-9]+)', output)
    return match.group(1) if match else None


def all_sources_finished(job_status: dict) -> bool:
    """Return true when all source vertices report FINISHED."""
    vertices = job_status.get('vertices', [])
    source_vertices = [vertex for vertex in vertices if vertex.get('name', '').startswith('Source:')]
    return bool(source_vertices) and all(vertex.get('status') == 'FINISHED' for vertex in source_vertices)


def wait_for_job_completion(rest_url: str, job_id: str, timeout: int) -> tuple[str, bool, float]:
    """
    Poll a detached client_usecase job until completion.

    The workload is bounded at the sources, but the downstream pipeline can stay RUNNING.
    Once all source vertices have finished, cancel the job and treat that controlled stop as
    successful completion for benchmark purposes.
    """
    start_time = time.time()
    bounded_completion_time = None
    auto_stopped = False
    last_state = None

    while time.time() - start_time < timeout:
        job_status = get_job_status(rest_url, job_id)
        state = job_status.get('state', 'UNKNOWN')

        if state != last_state:
            print(f'  Job state: {state}')
            last_state = state

        if state in ('FAILED', 'FINISHED'):
            effective_end = bounded_completion_time or time.time()
            return state, auto_stopped, effective_end - start_time

        if state == 'CANCELED':
            effective_end = bounded_completion_time or time.time()
            return ('AUTO_STOPPED' if auto_stopped else 'CANCELED'), auto_stopped, effective_end - start_time

        if bounded_completion_time is None and all_sources_finished(job_status):
            bounded_completion_time = time.time()
            print('  All sources finished; cancelling job for clean bounded benchmark shutdown...')
            auto_stopped = cancel_job(rest_url, job_id)
            if not auto_stopped:
                print('  WARNING: auto-cancel request failed; waiting for terminal state')

        time.sleep(2)

    effective_end = bounded_completion_time or time.time()
    return 'TIMEOUT', auto_stopped, effective_end - start_time


def split_total_records(total_records: int) -> tuple[int, int]:
    """Split total input records across the two input streams."""
    left_records = (total_records + 1) // 2
    right_records = total_records // 2
    return left_records, right_records


def records_per_source_subtask(total_records: int, parallelism: int) -> int:
    """
    Compensate for the client usecase source implementation.

    The customer job applies env parallelism to both sources and each source subtask
    emits maxRecords independently. To approximate a desired total record count for a
    stream, pass per-subtask records here.
    """
    if parallelism <= 0:
        raise ValueError('parallelism must be > 0')
    return max(1, math.ceil(total_records / parallelism))


def run_client_usecase(
    config: dict,
    backend: str,
    profile_mode: str = None,
) -> Optional[dict]:
    """Run the client usecase benchmark on a Flink cluster."""
    client_config = config.get('client_usecase', {})
    total_input_records = int(client_config.get('num_records', 0))
    if total_input_records <= 0:
        print('ERROR: client_usecase.num_records must be > 0 in benchmark.yaml')
        return None

    flink_config = config.get('flink', {})
    runtime_config = config.get('runtime', {})
    flink_home = get_flink_home()
    rest_url = flink_config.get('rest_url', 'http://localhost:8081')

    if not flink_home:
        print('ERROR: FLINK_HOME not set')
        return None

    if not check_flink_cluster(rest_url):
        print(f'ERROR: Flink cluster not running at {rest_url}')
        print(f'  Start cluster with: {flink_home}/bin/start-cluster.sh')
        return None

    cleanup_running_jobs(rest_url, f'client_usecase:{backend}')

    jar_path = get_client_usecase_jar()
    if not jar_path:
        print('ERROR: Client usecase JAR not found.')
        print('  Build locally and commit/copy one of these JARs into docker/deploy/:')
        print('    flink-keyedcoprocessfunction-example-*-jar-with-dependencies.jar')
        return None

    left_records, right_records = split_total_records(total_input_records)
    parallelism = int(runtime_config.get('parallelism', 1))
    left_records_per_subtask = records_per_source_subtask(left_records, parallelism)
    right_records_per_subtask = records_per_source_subtask(right_records, parallelism)
    estimated_total_records = parallelism * (left_records_per_subtask + right_records_per_subtask)

    backends_list = {b['name']: b['class'] for b in config.get('backends', [])}
    backend_class = backends_list.get(backend, '')
    flink_bin = Path(flink_home) / 'bin' / 'flink'

    cmd = [str(flink_bin), 'run']
    if backend_class:
        cmd.append(f'-Dstate.backend.type={backend_class}')
    cmd.extend(get_forl0_config_args(config, backend, workload_key='client_usecase'))

    cmd.extend([
        jar_path,
        '--leftNumRecords', str(left_records_per_subtask),
        '--rightNumRecords', str(right_records_per_subtask),
        '--parallelism', str(parallelism),
        '--checkpointInterval', str(runtime_config.get('checkpoint_interval', 0)),
    ])

    print(f"\n=== Running Client Usecase Benchmark ({backend} backend) ===\n")
    print(f"Flink cluster: {rest_url}")
    print(
        'Parameters: '
        f'desiredTotalInputRecords={total_input_records}, '
        f'parallelism={parallelism}, '
        f'leftStreamTarget={left_records}, '
        f'rightStreamTarget={right_records}, '
        f'leftPerSubtask={left_records_per_subtask}, '
        f'rightPerSubtask={right_records_per_subtask}, '
        f'estimatedActualTotal={estimated_total_records}'
    )
    print(
        'Warning: customer source applies env parallelism to each source instance and uses event-time timers '
        'without watermark emission. This runner compensates the record count at submission time, but long-running '
        'jobs can still accumulate state faster than expected.'
    )
    supported_profile_modes = {'cpu', 'cache'}
    if profile_mode and profile_mode not in supported_profile_modes:
        print(f"WARNING: Unsupported profile mode '{profile_mode}' for client_usecase; supported: cpu, cache")
        profile_mode = None
    print(f"Command: {' '.join(cmd)}\n")

    timeout = max(3600, int(config.get('client_usecase', {}).get('timeout_seconds', 7200)))

    profiler = None
    profiler_files = None
    tm_pids = []

    if profile_mode:
        profiler = AsyncProfiler()
        if profiler.is_available():
            tm_pids = find_taskmanager_pids(flink_home)
            if tm_pids:
                profiles_dir = Path(__file__).parent.parent / 'results' / 'profiles'
                profiles_dir.mkdir(parents=True, exist_ok=True)
                events = ['cpu', 'alloc'] if profile_mode == 'cpu' else ['cache-misses']
                started = profiler.start(
                    pid=tm_pids[0],
                    events=events,
                    output_dir=str(profiles_dir),
                    backend=backend,
                    query='client_usecase',
                    output_format='html',
                    duration=None,
                )
                if started:
                    print(f'  Started {profile_mode} profiling on TaskManager PID {tm_pids[0]}')
                else:
                    print('  WARNING: Profiler failed to start; continuing without profiling')
                    profiler = None
            else:
                print('  WARNING: No TaskManager PID found; skipping profiling')
                profiler = None
        else:
            print('  WARNING: Async Profiler not available (set ASYNC_PROFILER_HOME)')
            profiler = None

    try:
        job_id = submit_job_async(cmd)
        if not job_id:
            print('ERROR: Failed to submit job')
            return None

        print(f'  Job submitted: {job_id}')
        final_state, auto_stopped, wall_time = wait_for_job_completion(rest_url, job_id, timeout)

        if profiler and tm_pids:
            try:
                profiler_files = profiler.stop(tm_pids[0])
                if profiler_files:
                    print(f"  Profiler output files: {list(profiler_files.keys())}")
            except Exception as exc:
                print(f'  WARNING: Failed to stop profiler cleanly: {exc}')

        if final_state in ('FAILED', 'TIMEOUT', 'CANCELED'):
            print(f'ERROR: Job ended with state: {final_state}')
            return None

        throughput = estimated_total_records / wall_time if wall_time > 0 else 0
        result = {
            'benchmark': 'client-usecase',
            'backend': backend,
            'job_id': job_id,
            'job_state': final_state,
            'auto_stopped': auto_stopped,
            'config': {
                'num_records': total_input_records,
                'left_num_records_target': left_records,
                'right_num_records_target': right_records,
                'left_num_records_per_subtask': left_records_per_subtask,
                'right_num_records_per_subtask': right_records_per_subtask,
                'estimated_actual_total_records': estimated_total_records,
                'parallelism': parallelism,
                'checkpoint_interval': runtime_config.get('checkpoint_interval', 0),
            },
            'total_input_records': estimated_total_records,
            'desired_total_input_records': total_input_records,
            'wall_time_seconds': wall_time,
            'throughput': throughput,
            'throughput_per_core': throughput / parallelism if parallelism > 0 else throughput,
        }
        if profiler_files:
            result['profiler_files'] = profiler_files
        return result
    except Exception as exc:
        print(f'ERROR: {exc}')
        import traceback
        traceback.print_exc()
        return None


def apply_client_usecase_scenario(config: dict, scenario_name: str) -> dict:
    """Apply a client_usecase_scenarios entry to the config dict (returns a copy)."""
    import copy
    scenarios = config.get('client_usecase_scenarios', [])
    scenario = None
    for s in scenarios:
        if s.get('name') == scenario_name:
            scenario = s
            break
    if not scenario:
        print(f"ERROR: client_usecase scenario '{scenario_name}' not found.")
        print(f"Available: {[s.get('name') for s in scenarios]}")
        sys.exit(1)

    config = copy.deepcopy(config)

    # Override client_usecase settings
    for key in ('num_records', 'timeout_seconds', 'checkpoint_interval'):
        if key in scenario:
            config['client_usecase'][key] = scenario[key]

    # ForL0 backend overrides
    forl0_overrides = scenario.get('forl0_overrides', {})
    if forl0_overrides:
        for b in config.get('backends', []):
            if b.get('name') == 'forl0':
                b.setdefault('config', {}).setdefault('workload_overrides', {}).setdefault('client_usecase', {})
                b['config']['workload_overrides']['client_usecase'].update(forl0_overrides)

    print(f"[Client Usecase] Scenario: {scenario_name} — {scenario.get('description', '')}")
    return config


def main():
    parser = argparse.ArgumentParser(description='Run client usecase benchmark on Flink cluster')
    parser.add_argument('--backend', choices=['hashmap', 'forl0', 'all'], default='all')
    parser.add_argument('--profile', type=str, default=None)
    parser.add_argument('--scenario', type=str, default=None,
                       help='Run a named scenario from client_usecase_scenarios in config')
    args = parser.parse_args()

    config = load_config()

    # Apply scenario overrides if specified
    if args.scenario:
        config = apply_client_usecase_scenario(config, args.scenario)

    backends = ['hashmap', 'forl0'] if args.backend == 'all' else [args.backend]

    results = {}
    for backend in backends:
        result = run_client_usecase(
            config,
            backend,
            profile_mode=args.profile,
        )
        if result:
            results[backend] = result
            tag = f"client_usecase_{args.scenario}" if args.scenario else 'client_usecase'
            save_result(result, tag, backend)

    if results:
        print('\nSummary:')
        for backend, result in results.items():
            print(f"  {backend:10s}: {result['throughput_per_core']:>12,.0f} records/s/core")


if __name__ == '__main__':
    main()