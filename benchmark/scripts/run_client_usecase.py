#!/usr/bin/env python3
"""
Run client usecase benchmark via Flink cluster.

This integrates client_usecase/XX_6000c_Demo into the unified benchmark flow.
The workload is bounded by total input records from benchmark.yaml so it can be
configured like WordCount.
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

import requests

sys.path.insert(0, str(Path(__file__).parent))

from utils.config import load_config, get_flink_home, save_result


def get_client_usecase_jar() -> Optional[str]:
    """Get path to the packaged client usecase JAR."""
    benchmark_root = Path(__file__).parent.parent.parent
    target_dir = benchmark_root / 'client_usecase' / 'XX_6000c_Demo' / 'target'
    deploy_dir = benchmark_root / '..' / 'docker' / 'deploy'

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


def split_total_records(total_records: int) -> tuple[int, int]:
    """Split total input records across the two input streams."""
    left_records = (total_records + 1) // 2
    right_records = total_records // 2
    return left_records, right_records


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

    jar_path = get_client_usecase_jar()
    if not jar_path:
        print('ERROR: Client usecase JAR not found.')
        print('  Build locally and commit/copy one of these JARs into docker/deploy/:')
        print('    flink-keyedcoprocessfunction-example-*-jar-with-dependencies.jar')
        return None

    left_records, right_records = split_total_records(total_input_records)

    backends_list = {b['name']: b['class'] for b in config.get('backends', [])}
    backend_class = backends_list.get(backend, '')
    flink_bin = Path(flink_home) / 'bin' / 'flink'

    cmd = [str(flink_bin), 'run']
    if backend_class:
        cmd.append(f'-Dstate.backend.type={backend_class}')

    cmd.extend([
        jar_path,
        '--leftNumRecords', str(left_records),
        '--rightNumRecords', str(right_records),
        '--parallelism', str(runtime_config.get('parallelism', 1)),
        '--checkpointInterval', str(runtime_config.get('checkpoint_interval', 0)),
    ])

    print(f"\n=== Running Client Usecase Benchmark ({backend} backend) ===\n")
    print(f"Flink cluster: {rest_url}")
    print(f"Parameters: totalInputRecords={total_input_records}, leftRecords={left_records}, rightRecords={right_records}")
    if profile_mode:
        print(f"Profiling requested but not implemented for client_usecase yet: {profile_mode}")
    print(f"Command: {' '.join(cmd)}\n")

    timeout = 3600

    try:
        start_time = time.time()
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        end_time = time.time()

        output = result.stdout + result.stderr
        print(output)

        if result.returncode != 0 or 'Job execution failed' in output or 'Exception' in output:
            print('ERROR: Job failed')
            return None

        wall_time = end_time - start_time
        throughput = total_input_records / wall_time if wall_time > 0 else 0
        parallelism = runtime_config.get('parallelism', 1)

        return {
            'benchmark': 'client-usecase',
            'backend': backend,
            'config': {
                'num_records': total_input_records,
                'left_num_records': left_records,
                'right_num_records': right_records,
                'parallelism': parallelism,
                'checkpoint_interval': runtime_config.get('checkpoint_interval', 0),
            },
            'total_input_records': total_input_records,
            'wall_time_seconds': wall_time,
            'throughput': throughput,
            'throughput_per_core': throughput / parallelism if parallelism > 0 else throughput,
        }
    except subprocess.TimeoutExpired:
        print('ERROR: Benchmark timed out')
        return None
    except Exception as exc:
        print(f'ERROR: {exc}')
        import traceback
        traceback.print_exc()
        return None


def main():
    parser = argparse.ArgumentParser(description='Run client usecase benchmark on Flink cluster')
    parser.add_argument('--backend', choices=['hashmap', 'forl0', 'all'], default='all')
    parser.add_argument('--profile', type=str, default=None)
    args = parser.parse_args()

    config = load_config()
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
            save_result(result, 'client_usecase', backend)

    if results:
        print('\nSummary:')
        for backend, result in results.items():
            print(f"  {backend:10s}: {result['throughput_per_core']:>12,.0f} records/s/core")


if __name__ == '__main__':
    main()