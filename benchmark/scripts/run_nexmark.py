#!/usr/bin/env python3
""" NexMark DataStream Benchmark Runner for ForL0 State Backend

This script runs NexMark DataStream benchmark for state backend comparison.
It reads configuration from benchmark.yaml and runs each query individually.

Usage:
    python run_nexmark.py                       # Run with both backends
    python run_nexmark.py --backend forl0       # Run only ForL0
    python run_nexmark.py --queries q5,q8       # Specific queries
    python run_nexmark.py --profile cpu         # Enable CPU flame graphs
    python run_nexmark.py --profile cache       # Enable cache statistics
"""

import argparse
import json
import os
import socket
import shutil
import subprocess
import sys
import time
import glob
import re
import math
import statistics
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))
from utils import requests_shim as requests  # type: ignore[assignment]
from utils.config import load_config, parse_json_from_output
from utils.profiler import AsyncProfiler, find_taskmanager_pids
from utils.flamegraph_quality import analyze_flamegraph_quality


NEXMARK_MAIN_CLASS = 'com.github.nexmark.flink.Benchmark'


def find_free_tcp_port() -> int:
    """Reserve a free local TCP port number for short-lived config generation."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind(('0.0.0.0', 0))
        return server_socket.getsockname()[1]


def run_checked_command(command: list[str], timeout: int = 30) -> subprocess.CompletedProcess:
    """Run a command and raise on failure with captured stderr/stdout."""
    return subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=True,
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


def cancel_job(rest_url: str, job_id: str) -> bool:
    """Cancel a running Flink job via REST API."""
    try:
        resp = requests.patch(
            f"{rest_url}/jobs/{job_id}",
            params={'mode': 'cancel'},
            timeout=30
        )
        if resp.status_code in [200, 202]:
            return True
        resp = requests.delete(f"{rest_url}/jobs/{job_id}/cancel", timeout=30)
        return resp.status_code in [200, 202]
    except Exception as e:
        print(f"  WARNING: Failed to cancel job: {e}")
        return False


def parse_nexmark_summary(output: str) -> Optional[dict]:
    """Parse the Total row from NexMark benchmark summary output."""
    total_line = None
    for line in output.splitlines():
        if line.strip().startswith('|Total'):
            total_line = line.strip()

    if not total_line:
        return None

    parts = [part.strip() for part in total_line.split('|') if part.strip()]

    def parse_float(value: str) -> float:
        cleaned = value.replace(',', '').replace('/s', '').strip()
        if cleaned in {'NaN', 'nan', 'N/A', ''}:
            return 0.0
        if cleaned == '�':
            return 0.0

        multiplier = 1.0
        suffix_multipliers = {
            'K': 1_000.0,
            'M': 1_000_000.0,
            'G': 1_000_000_000.0,
        }
        suffix = cleaned[-1:]
        if suffix in suffix_multipliers:
            multiplier = suffix_multipliers[suffix]
            cleaned = cleaned[:-1].strip()

        parsed = float(cleaned)
        if math.isnan(parsed):
            return 0.0
        return parsed * multiplier

    if len(parts) >= 7:
        try:
            events_num = int(parts[1].replace(',', ''))
            cores = parse_float(parts[2])
            time_seconds = parse_float(parts[3])
            cores_time = parse_float(parts[4])
            throughput = parse_float(parts[5])
            throughput_per_core = parse_float(parts[6])
            return {
                'events_num': events_num,
                'cpu': cores,
                'time_seconds': time_seconds,
                'cores_multiply_time_seconds': cores_time,
                'throughput': throughput,
                'throughput_per_core': throughput_per_core,
            }
        except ValueError:
            return None

    if len(parts) >= 4:
        try:
            throughput = parse_float(parts[1])
            cores = parse_float(parts[2])
            throughput_per_core = parse_float(parts[3])
            return {
                'cpu': cores,
                'throughput': throughput,
                'throughput_per_core': throughput_per_core,
            }
        except ValueError:
            return None

    return None


def parse_taskmanager_log(flink_home: str) -> Optional[dict]:
    """Parse benchmark results from TaskManager stdout (.out file)."""
    log_pattern = f"{flink_home}/log/*taskexecutor*.out"
    log_files = glob.glob(log_pattern)
    
    if not log_files:
        return None
    
    log_file = max(log_files, key=lambda f: Path(f).stat().st_mtime)
    
    try:
        with open(log_file, 'r') as f:
            content = f.read()
        
        result = parse_json_from_output(content)
        if result:
            return result
    except Exception:
        pass
    
    return None


def clear_taskmanager_logs(flink_home: str):
    """Clear TaskManager log files to get fresh results."""
    log_pattern = f"{flink_home}/log/*taskexecutor*.out"
    for log_file in glob.glob(log_pattern):
        try:
            with open(log_file, 'w') as f:
                f.write('')
        except Exception:
            pass


def get_forl0_config_args(config: dict, backend: str, query: Optional[str] = None) -> list:
    """Get ForL0 StateBackend configuration as Flink -D arguments.

    Supports optional per-query overrides under:
        backends[].config.query_overrides.<query_name>.*
    """
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
        nexmark_cfg = workload_overrides.get('nexmark', {})
        if isinstance(nexmark_cfg, dict):
            effective_config.update(nexmark_cfg)

    query_overrides = backend_config.get('query_overrides', {})
    if query and isinstance(query_overrides, dict):
        query_cfg = query_overrides.get(query, {})
        if isinstance(query_cfg, dict):
            effective_config.update(query_cfg)

    args = []
    config_mapping = {
        'initial_table_capacity': 'state.backend.forl0.initial-table-capacity',
        'max_table_capacity': 'state.backend.forl0.max-table-capacity',
        'l0_cache_enabled': 'state.backend.forl0.l0-cache.enabled',
        'l0_cache_size': 'state.backend.forl0.l0-cache.size',
        'l0_cache_replacement_policy': 'state.backend.forl0.l0-cache.replacement-policy',
        'l0_memory_max_size': 'state.backend.forl0.l0-memory.max-size',
        'main_table_load_factor_threshold': 'state.backend.forl0.main-table.load-factor-threshold',
    }
    
    for yaml_key, flink_key in config_mapping.items():
        if yaml_key in effective_config:
            value = effective_config[yaml_key]
            if isinstance(value, bool):
                value = 'true' if value else 'false'
            args.append(f'-D{flink_key}={value}')
    
    return args


def run_warmup_job(cmd: list, rest_url: str, warmup_duration: int, backend: str) -> bool:
    """Run a warmup job for JIT compilation and cache warming."""
    print(f"\n--- Warmup Phase ({warmup_duration}s) ---")
    print(f"  Submitting warmup job...")
    
    job_id = submit_job_async(cmd, rest_url)
    if not job_id:
        print("  WARNING: Failed to submit warmup job, skipping warmup")
        return False
    
    print(f"  Warmup job submitted: {job_id}")
    
    start_time = time.time()
    last_state = None
    
    while time.time() - start_time < warmup_duration:
        status = get_job_status(rest_url, job_id)
        state = status.get('state', 'UNKNOWN')
        
        if state != last_state:
            print(f"  Job state: {state}")
            last_state = state
        
        if state in ['FINISHED', 'FAILED', 'CANCELED']:
            print(f"  Warmup job ended early with state: {state}")
            return state != 'FAILED'
        
        elapsed = int(time.time() - start_time)
        remaining = warmup_duration - elapsed
        if remaining > 0 and remaining % 10 == 0:
            print(f"  Warmup: {elapsed}s elapsed, {remaining}s remaining...")
        
        time.sleep(1)
    
    print(f"  Warmup complete, cancelling job...")
    if cancel_job(rest_url, job_id):
        for _ in range(30):
            status = get_job_status(rest_url, job_id)
            state = status.get('state', 'UNKNOWN')
            if state in ['CANCELED', 'FINISHED', 'FAILED']:
                print(f"  Warmup job cancelled successfully")
                break
            time.sleep(0.5)
    else:
        print(f"  WARNING: Failed to cancel warmup job")
    
    time.sleep(2)
    print(f"--- Warmup Phase Complete ---\n")
    return True


class NexmarkRunner:
    """NexMark DataStream benchmark runner."""
    
    def __init__(self, config: dict):
        self.config = config
        self.project_root = Path(__file__).parent.parent.parent
        self.benchmark_root = Path(__file__).parent.parent
        
        # Get configuration
        self.runtime_config = config.get('runtime', {})
        self.nexmark_config = config.get('nexmark', {})
        self.flink_config = config.get('flink', {})
        
        # Run mode (benchmark, test, etc.)
        self.mode = self.nexmark_config.get('mode', 'benchmark')
        
        # Flink paths — auto-detect via shared utility
        from utils.config import get_flink_home
        fh = get_flink_home()
        self.flink_home = Path(fh) if fh else Path.home() / 'flink' / 'flink-1.20.0'
        
        self.rest_url = self.flink_config.get('rest_url', 'http://localhost:8081')
        
        # Nexmark DataStream JAR
        self.nexmark_jar = self._find_nexmark_jar()
        self.nexmark_home = self._find_nexmark_home()
        
        # ForL0 JAR
        self.forl0_jar = self._find_forl0_jar()
        
        # Results directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.results_dir = self.benchmark_root / "results" / f"nexmark_{timestamp}"

        self.metric_reporter_host = self._resolve_metric_reporter_host()
        self.metric_reporter_port: Optional[int] = None
        
        # Profiler
        self.profiler = None
        self.metric_sender_containers = ['flink-taskmanager-1', 'flink-taskmanager-2']
        self.container_profiler_session: Optional[Dict[str, str]] = None

    def _resolve_metric_reporter_host(self) -> str:
        """Use the Docker bridge gateway so taskmanagers can report CPU metrics back to the host."""
        try:
            result = subprocess.run(
                ['sudo', '-n', 'docker', 'network', 'inspect', 'flink-net', '--format', '{{(index .IPAM.Config 0).Gateway}}'],
                capture_output=True,
                text=True,
                timeout=10,
                check=True,
            )
            gateway = result.stdout.strip()
            if gateway:
                return gateway
        except Exception:
            pass

        return '127.0.0.1'

    def _start_metric_senders(self, env: dict) -> None:
        """Start CpuMetricSender inside each taskmanager container for the current run."""
        if self.metric_reporter_port is None:
            return

        metric_conf = (
            f'nexmark.metric.reporter.host: {self.metric_reporter_host}\n'
            f'nexmark.metric.reporter.port: {self.metric_reporter_port}\n'
            'nexmark.metric.monitor.interval: 5 s\n'
        )
        for container in self.metric_sender_containers:
            try:
                self._stop_metric_sender(container)
                run_checked_command([
                    'sudo', '-n', 'docker', 'exec', container,
                    'sh', '-lc',
                    "mkdir -p /tmp/nexmark-metric-conf; "
                    "cat > /tmp/nexmark-metric-conf/nexmark.yaml <<'EOF'\n"
                    f"{metric_conf}"
                    "EOF\n"
                    'export FLINK_HOME=/opt/flink; export NEXMARK_CONF_DIR=/tmp/nexmark-metric-conf; '
                    '(nohup java -cp /opt/flink/lib/nexmark-flink-0.3-SNAPSHOT.jar:/opt/flink/lib/* '
                    'com.github.nexmark.flink.metric.cpu.CpuMetricSender '
                    '>/tmp/cpu-metric-sender.log 2>&1 </dev/null &)'
                ])
            except Exception as error:
                print(f'  WARNING: Failed to start CpuMetricSender in {container}: {error}')

    def _stop_metric_sender(self, container: str) -> None:
        """Stop CpuMetricSender inside a single taskmanager container."""
        try:
            run_checked_command([
                'sudo', '-n', 'docker', 'exec', container,
                'sh', '-lc',
                "pkill -f '[C]puMetricSender' >/dev/null 2>&1 || true"
            ])
        except Exception:
            pass

    def _stop_metric_senders(self) -> None:
        """Stop CpuMetricSender inside each taskmanager container."""
        for container in self.metric_sender_containers:
            self._stop_metric_sender(container)

    def _list_running_jobs(self) -> list[dict]:
        """List running Flink jobs via REST API."""
        try:
            resp = requests.get(f"{self.rest_url}/jobs/overview", timeout=10)
            if resp.status_code != 200:
                return []
            jobs = resp.json().get('jobs', [])
            return [j for j in jobs if j.get('state') == 'RUNNING']
        except Exception:
            return []

    def _cancel_running_jobs(self, reason: str) -> None:
        """Best-effort cancellation of all running jobs to avoid benchmark interference."""
        running = self._list_running_jobs()
        if not running:
            return

        print(f"  [Nexmark] Cleaning {len(running)} running job(s) before {reason}")
        for job in running:
            jid = job.get('jid')
            name = job.get('name', 'unknown')
            if not jid:
                continue
            if cancel_job(self.rest_url, jid):
                print(f"    canceled {jid} ({name})")
            else:
                print(f"    WARNING: failed to cancel {jid} ({name})")

        # Give the cluster a brief settling window after cancellation.
        time.sleep(2)

    def _start_container_profiler(self, backend: str, query: str, profile_mode: str) -> bool:
        """Start async-profiler inside taskmanager container and return whether startup succeeded."""
        profiler_home = os.environ.get('ASYNC_PROFILER_HOME')
        if not profiler_home:
            return False

        host_asprof = Path(profiler_home) / 'bin' / 'asprof'
        if not host_asprof.exists():
            return False

        container = self.metric_sender_containers[0]
        target_dir = '/tmp/async-profiler'
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        event = 'cpu' if profile_mode == 'cpu' else 'cache-misses'
        local_profiles_dir = self.results_dir / 'profiles'
        local_profiles_dir.mkdir(parents=True, exist_ok=True)
        local_name = f'flamegraph_{event}_{backend}_{query}_{timestamp}.html'
        tmp_output = f'/tmp/{local_name}'

        try:
            # Ensure async-profiler exists in container. Re-copy every run to keep tooling deterministic.
            run_checked_command([
                'sudo', '-n', 'docker', 'cp', str(Path(profiler_home)), f'{container}:{target_dir}'
            ], timeout=60)

            run_checked_command([
                'sudo', '-n', 'docker', 'exec', container,
                'sh', '-lc', f'{target_dir}/bin/asprof --version >/dev/null'
            ], timeout=15)

            # Best-effort cleanup of a stale previous session.
            run_checked_command([
                'sudo', '-n', 'docker', 'exec', container,
                'sh', '-lc', f'{target_dir}/bin/asprof stop 1 >/dev/null 2>&1 || true'
            ], timeout=15)

            interval = '10000' if event == 'cache-misses' else '10ms'
            run_checked_command([
                'sudo', '-n', 'docker', 'exec', container,
                'sh', '-lc',
                f'{target_dir}/bin/asprof start -e {event} -i {interval} -f {tmp_output} 1'
            ], timeout=20)

            self.container_profiler_session = {
                'container': container,
                'tmp_output': tmp_output,
                'local_output': str(local_profiles_dir / local_name),
                'event': event,
            }
            return True
        except Exception as error:
            print(f'  WARNING: Container profiler startup failed: {error}')
            self.container_profiler_session = None
            return False

    def _stop_container_profiler(self) -> Optional[str]:
        """Stop container profiler if active and copy output file to host."""
        if not self.container_profiler_session:
            return None

        session = self.container_profiler_session
        self.container_profiler_session = None
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
            print(f'  WARNING: Container profiler stop/copy failed: {error}')
            return None

    def _report_flamegraph_quality(self, profiler_file: str) -> None:
        """Print a concise quality report for generated flamegraphs."""
        quality = analyze_flamegraph_quality(profiler_file)
        if not quality:
            print('  WARNING: Could not parse flamegraph for quality analysis')
            return

        idle_ratio = quality.get('idle_ratio', 0.0)
        status = 'IDLE-DOMINATED' if quality.get('idle_dominated') else 'OK'
        print(f"  Flamegraph quality: {status} (idle={idle_ratio * 100:.1f}%)")
        for item in quality.get('top_non_idle', [])[:5]:
            frame = item.get('frame', 'unknown')
            ratio = item.get('ratio', 0.0)
            print(f"    non-idle hotspot {ratio * 100:5.1f}%  {frame}")
        
    def _find_nexmark_jar(self) -> Path:
        """Find the Nexmark DataStream JAR."""
        candidates = [
            (self.project_root / 'benchmark' / 'nexmark-src' / 'nexmark-flink' / 'target' / 'nexmark-flink-bin' / 'nexmark-flink' / 'lib', 'nexmark-flink-*.jar'),
            (self.benchmark_root / "nexmark-datastream" / "target", "nexmark-datastream-*.jar"),
            (self.project_root / "docker" / "deploy", "nexmark-flink-*.jar"),
            (self.project_root / "docker" / "deploy", "nexmark-datastream-*.jar"),
        ]

        for target_dir, pattern in candidates:
            if not target_dir.exists():
                continue
            for jar in target_dir.glob(pattern):
                if "sources" not in jar.name and "javadoc" not in jar.name and "original" not in jar.name:
                    return jar

        raise FileNotFoundError(
            "Nexmark DataStream JAR not found. "
            "Expected either benchmark/nexmark-datastream/target/nexmark-datastream-*.jar "
            "or docker/deploy/nexmark-flink-*.jar."
        )

    def _find_nexmark_home(self) -> Path:
        """Find the packaged NexMark distribution directory with bin/conf/queries."""
        candidates = [
            self.project_root / 'benchmark' / 'nexmark-src' / 'nexmark-flink' / 'target' / 'nexmark-flink-bin' / 'nexmark-flink',
            self.project_root / 'docker' / 'deploy' / 'nexmark-flink',
        ]

        for candidate in candidates:
            if (candidate / 'bin').exists() and (candidate / 'conf').exists() and (candidate / 'queries').exists():
                return candidate

        raise FileNotFoundError(
            'NexMark distribution directory not found. Expected a packaged nexmark-flink directory with bin/conf/queries.'
        )
    
    def _find_forl0_jar(self) -> Path:
        """Find the ForL0 StateBackend JAR."""
        candidates = [
            (self.project_root / "target", "flink-statebackend-forL0-*.jar"),
            (self.project_root / "docker" / "deploy", "flink-statebackend-forL0-*.jar"),
            (self.project_root / "docker" / "deploy", "flink-statebackend-forl0-*.jar"),
        ]

        for target_dir, pattern in candidates:
            if not target_dir.exists():
                continue
            for jar in target_dir.glob(pattern):
                if "sources" not in jar.name and "javadoc" not in jar.name:
                    return jar

        raise FileNotFoundError(
            "ForL0 JAR not found. Expected either target/flink-statebackend-forL0-*.jar "
            "or docker/deploy/flink-statebackend-forL0-*.jar."
        )
    
    def _get_query_events(self, query: str) -> int:
        """Get events number for a specific query from config."""
        query_events_key = f"{query}_events"
        query_events = self.nexmark_config.get(query_events_key)
        
        if query_events is not None:
            return query_events
        
        return self.nexmark_config.get('events', 10000000)
    
    def _copy_forl0_jar_if_needed(self):
        """Copy ForL0 JAR to Flink lib if not present."""
        flink_lib_jar = self.flink_home / "lib" / self.forl0_jar.name
        if not flink_lib_jar.exists():
            shutil.copy(self.forl0_jar, flink_lib_jar)
            print(f"[Nexmark] Copied ForL0 JAR to Flink lib")

    def _copy_nexmark_jar_if_needed(self):
        """Copy NexMark connector/driver JAR to Flink lib for SQL client discovery."""
        flink_lib_jar = self.flink_home / 'lib' / self.nexmark_jar.name
        if not flink_lib_jar.exists() or flink_lib_jar.read_bytes() != self.nexmark_jar.read_bytes():
            shutil.copy(self.nexmark_jar, flink_lib_jar)
            print('[Nexmark] Synced NexMark JAR to Flink lib')

    def _write_nexmark_conf(
        self,
        query: str,
        num_events: int,
        tps: int,
        warmup_duration: int,
        checkpoint_interval_ms: int,
    ) -> Path:
        """Write a temporary nexmark.yaml tuned for the requested query run."""
        conf_dir = Path(tempfile.mkdtemp(prefix='nexmark-conf-'))
        shutil.copy(self.nexmark_home / 'conf' / 'config.yaml', conf_dir / 'config.yaml')
        shutil.copy(self.nexmark_home / 'conf' / 'log4j.properties', conf_dir / 'log4j.properties')

        config_yaml_path = conf_dir / 'config.yaml'
        config_yaml = config_yaml_path.read_text()
        config_yaml = re.sub(
            r'^execution\.checkpointing\.interval:.*$',
            f'execution.checkpointing.interval: {checkpoint_interval_ms}',
            config_yaml,
            flags=re.MULTILINE,
        )
        config_yaml_path.write_text(config_yaml)

        warmup_seconds = max(0, int(warmup_duration))
        warmup_events = num_events if warmup_seconds > 0 else 0
        workload_tps = max(0, int(tps))
        warmup_tps = workload_tps if warmup_seconds > 0 else 0
        self.metric_reporter_port = find_free_tcp_port()

        nexmark_yaml = (
            f'nexmark.metric.reporter.host: {self.metric_reporter_host}\n'
            f'nexmark.metric.reporter.port: {self.metric_reporter_port}\n'
            f'nexmark.workload.suite.run.events.num: {num_events}\n'
            f'nexmark.workload.suite.run.tps: {workload_tps}\n'
            f'nexmark.workload.suite.run.queries: "{query}"\n'
            f'nexmark.workload.suite.run.warmup.duration: {warmup_seconds}s\n'
            f'nexmark.workload.suite.run.warmup.events.num: {warmup_events}\n'
            f'nexmark.workload.suite.run.warmup.tps: {warmup_tps}\n'
            f'flink.rest.address: {self.rest_url.split("//", 1)[-1].split(":", 1)[0]}\n'
            f'flink.rest.port: {self.rest_url.rsplit(":", 1)[-1]}\n'
        )
        (conf_dir / 'nexmark.yaml').write_text(nexmark_yaml)
        return conf_dir

    def _build_driver_command(
        self,
        query: str,
        backend: str,
        num_events: int,
        tps: int,
        warmup_duration: int,
        checkpoint_interval_ms: int,
    ) -> tuple[list[str], dict, Path]:
        """Build the NexMark benchmark-driver command and environment."""
        conf_dir = self._write_nexmark_conf(query, num_events, tps, warmup_duration, checkpoint_interval_ms)
        classpath = f"{self.nexmark_home / 'lib'}/*:{self.flink_home / 'lib'}/*"
        forl0_args = get_forl0_config_args(self.config, backend, query)
        cmd = [
            'java',
            f'-Dlog.file={self.nexmark_home / "log" / "nexmark-flink.log"}',
            f'-Dlog4j.configuration=file:{conf_dir / "log4j.properties"}',
            f'-Dlog4j.configurationFile=file:{conf_dir / "log4j.properties"}',
        ]
        cmd.extend(forl0_args)
        cmd.extend([
            '-cp',
            classpath,
            NEXMARK_MAIN_CLASS,
            '--location',
            str(self.nexmark_home),
            '--queries',
            query,
            '--category',
            'oa',
        ])

        env = os.environ.copy()
        env['FLINK_HOME'] = str(self.flink_home)
        env['NEXMARK_CONF_DIR'] = str(conf_dir)

        return cmd, env, conf_dir
    
    def run_query(
            self, 
            query: str, 
            backend: str, 
            profile_mode: Optional[str] = None
    ) -> Optional[dict]:
        """Run a single Nexmark query.
        
        Args:
            query: Query name (e.g., 'q5')
            backend: State backend ('hashmap' or 'forl0')
            profile_mode: 'cpu' or 'cache' for profiling
            
        Returns:
            Query result metrics or None on failure
        """
        
        parallelism = self.runtime_config.get('parallelism', 4)
        checkpoint_interval = self.runtime_config.get(
            'nexmark_checkpoint_interval',
            self.runtime_config.get('checkpoint_interval', 0),
        )
        num_events = self._get_query_events(query)
        tps = self.nexmark_config.get('tps', 0)
        warmup_duration = self.nexmark_config.get('warmup_duration', 0)
        
        # Event proportions (Nexmark default: 1:3:46)
        person_proportion = self.nexmark_config.get('person_proportion', 1)
        auction_proportion = self.nexmark_config.get('auction_proportion', 3)
        bid_proportion = self.nexmark_config.get('bid_proportion', 46)
        
        self._copy_nexmark_jar_if_needed()

        # Add ForL0-specific configuration
        if backend == 'forl0':
            self._copy_forl0_jar_if_needed()
        cmd, env, conf_dir = self._build_driver_command(
            query,
            backend,
            num_events,
            tps,
            warmup_duration,
            checkpoint_interval,
        )
        
        print(f"\n=== Running Nexmark {query.upper()} ({backend} backend) ===")
        print(f"Events: {num_events:,}, TPS: {tps if tps > 0 else 'unlimited'}")
        print(f"Proportions: Person({person_proportion}):Auction({auction_proportion}):Bid({bid_proportion})")
        print(f"Command: {' '.join(cmd)}\n")
        
        max_attempts = max(1, int(self.nexmark_config.get('max_attempts_per_query', 3)))
        query_timeout = max(3600, int(self.nexmark_config.get('timeout_seconds', 7200)))
        retry_backoff_seconds = max(1, int(self.nexmark_config.get('retry_backoff_seconds', 4)))
        min_profile_cpu_cores = float(self.nexmark_config.get('min_profile_cpu_cores', 0.05))

        try:
            for attempt in range(1, max_attempts + 1):
                if attempt > 1:
                    print(f"  [Nexmark] Retry attempt {attempt}/{max_attempts} for {query} ({backend})")

                self._cancel_running_jobs(f"{backend}:{query} attempt {attempt}")

                # Initialize profiler if enabled (per-attempt lifecycle).
                tm_pids = []
                using_container_profiler = False
                if profile_mode and profile_mode in ['cpu', 'cache']:
                    # Prefer container-side profiling in Docker deployments; fall back to host-side attach.
                    using_container_profiler = self._start_container_profiler(backend, query, profile_mode)
                    if using_container_profiler and self.container_profiler_session:
                        print(
                            f"  Started {profile_mode} profiling in container "
                            f"({self.container_profiler_session['container']}, event={self.container_profiler_session['event']})"
                        )
                    else:
                        profiler_home = os.environ.get('ASYNC_PROFILER_HOME')
                        self.profiler = AsyncProfiler(profiler_home)
                        if self.profiler.is_available():
                            tm_pids = find_taskmanager_pids(str(self.flink_home))
                            if tm_pids:
                                profiles_dir = self.results_dir / "profiles"
                                profiles_dir.mkdir(parents=True, exist_ok=True)

                                events = ['cpu', 'alloc'] if profile_mode == 'cpu' else ['cache-misses']
                                started = self.profiler.start(
                                    pid=tm_pids[0],
                                    events=events,
                                    output_dir=str(profiles_dir),
                                    backend=backend,
                                    query=query,
                                    output_format='html',
                                    duration=None
                                )
                                if started:
                                    print(f"  Started {profile_mode} profiling (host PID: {tm_pids[0]})")
                                else:
                                    print("  WARNING: Host-side profiler failed to start")
                            else:
                                print("  WARNING: No TaskManager PID found for host-side profiling")
                        else:
                            print("  WARNING: Async Profiler not available; profiling disabled")

                should_retry = False
                try:
                    self._start_metric_senders(env)
                    result = subprocess.run(
                        cmd,
                        env=env,
                        capture_output=True,
                        text=True,
                        timeout=query_timeout,
                    )
                    output = result.stdout + result.stderr
                    print(output)

                    output_lower = output.lower()
                    retryable_error = (
                        "metric reporter doesn't collect any metrics" in output_lower
                        or "can't find tps metric name from the response" in output_lower
                        or 'process failed due to timeout' in output_lower
                        or 'profiler already started' in output_lower
                    )

                    if result.returncode != 0:
                        print('  ERROR: NexMark benchmark driver failed')
                        should_retry = retryable_error
                    else:
                        parsed = parse_nexmark_summary(output)
                        if parsed:
                            # Filter clearly invalid samples from noisy metric reporter runs.
                            cpu = parsed.get('cpu', 0)
                            tpc = parsed.get('throughput_per_core', 0)
                            invalid_sample = cpu <= 0 or tpc <= 0

                            # For profiling runs, reject samples that are too idle to expose real hotspots.
                            if profile_mode and profile_mode in ['cpu', 'cache'] and cpu < min_profile_cpu_cores:
                                print(
                                    f'  WARNING: Profile sample too idle (cpu={cpu:.3f} < '
                                    f'min_profile_cpu_cores={min_profile_cpu_cores:.3f}), retrying'
                                )
                                invalid_sample = True

                            if invalid_sample:
                                print('  WARNING: Invalid sample detected (cpu/throughput too low), retrying')
                                should_retry = True
                            else:
                                parsed.update({
                                    'benchmark': 'nexmark',
                                    'backend': backend,
                                    'query': query,
                                    'parallelism': parallelism,
                                    'checkpoint_interval': checkpoint_interval,
                                    'person_proportion': person_proportion,
                                    'auction_proportion': auction_proportion,
                                    'bid_proportion': bid_proportion,
                                })
                                print(f"\n  Result: {parsed.get('throughput', 0):,.2f} events/sec")
                                return parsed
                        else:
                            print('  WARNING: Could not parse NexMark summary output')
                            should_retry = True

                except Exception as e:
                    print(f"  ERROR: {e}")
                    should_retry = True
                finally:
                    # Always stop profiler sessions, including benchmark-driver failures.
                    if using_container_profiler:
                        profiler_file = self._stop_container_profiler()
                        if profiler_file:
                            print(f"  Profiler output: {profiler_file}")
                            self._report_flamegraph_quality(profiler_file)
                    elif self.profiler and tm_pids:
                        try:
                            stopped_files = self.profiler.stop(tm_pids[0])
                            if stopped_files:
                                print(f"  Profiler output files: {list(stopped_files.values())}")
                                for profiler_file in stopped_files.values():
                                    self._report_flamegraph_quality(profiler_file)
                        except Exception as e:
                            print(f"  Profiler stop error: {e}")
                    self._stop_metric_senders()

                if not should_retry:
                    return None
                if attempt < max_attempts:
                    self._cancel_running_jobs(f"retry backoff {backend}:{query} attempt {attempt}")
                    backoff = retry_backoff_seconds * attempt
                    print(f"  [Nexmark] Backoff {backoff}s before next attempt")
                    time.sleep(backoff)

            return None
        finally:
            shutil.rmtree(conf_dir, ignore_errors=True)
    
    def run(
            self, 
            backends: List[str], 
            queries: Optional[str] = None,
            profile_mode: Optional[str] = None,
            repeat: Optional[int] = None
    ) -> dict:
        """Run Nexmark benchmark for specified backends and queries.
        
        Args:
            backends: List of backends to test
            queries: Comma-separated query list (e.g., 'q5,q8')
            profile_mode: 'cpu' or 'cache' for profiling
            
        Returns:
            Results dictionary
        """
        
        # Check Flink cluster
        if not check_flink_cluster(self.rest_url):
            print(f"ERROR: Flink cluster not running at {self.rest_url}")
            print(f"  Start cluster with: {self.flink_home}/bin/start-cluster.sh")
            return {}
        
        # Get queries from config if not specified
        if queries is None:
            queries = self.nexmark_config.get('queries', 'q5')
        
        query_list = [q.strip() for q in queries.split(',')]
        repeat_runs = max(1, int(repeat if repeat is not None else self.nexmark_config.get('repeat_per_query', 1)))
        min_success_samples = int(self.nexmark_config.get('min_success_samples', repeat_runs))
        min_success_samples = max(1, min(min_success_samples, repeat_runs))
        
        print(f"\n{'='*60}")
        print(f"Nexmark DataStream Benchmark")
        print(f"{'='*60}")
        print(f"Queries: {query_list}")
        print(f"Backends: {backends}")
        print(f"Repeat per query: {repeat_runs}")
        print(f"Min successful samples: {min_success_samples}")
        print(f"Mode: {self.mode}")
        print(f"{'='*60}\n")

        # Defensive cleanup: benchmark quality degrades sharply with lingering jobs.
        self._cancel_running_jobs('benchmark run start')
        
        results = {}
        
        for backend in backends:
            print(f"\n{'='*60}")
            print(f"Backend: {backend}")
            print(f"{'='*60}")
            
            backend_results = {
                'query_results': {},
                'failed_queries': []
            }
            
            for query in query_list:
                run_samples = []
                for run_idx in range(1, repeat_runs + 1):
                    if repeat_runs > 1:
                        print(f"\n[Nexmark] Sample {run_idx}/{repeat_runs} for {backend}:{query}")
                    result = self.run_query(query, backend, profile_mode)
                    if result:
                        run_samples.append(result)

                if run_samples:
                    if len(run_samples) < min_success_samples:
                        print(
                            f"  [Nexmark] Insufficient valid samples for {backend}:{query}: "
                            f"{len(run_samples)}/{min_success_samples} required"
                        )
                        backend_results['failed_queries'].append(query)
                        continue

                    if len(run_samples) == 1:
                        backend_results['query_results'][query] = run_samples[0]
                    else:
                        aggregated = self._aggregate_query_results(run_samples)
                        aggregated['samples_collected'] = len(run_samples)
                        backend_results['query_results'][query] = aggregated
                        print(
                            f"  [Nexmark] Aggregated median for {query}: "
                            f"{aggregated.get('throughput', 0):,.0f} events/sec "
                            f"({aggregated.get('throughput_per_core', 0):,.0f}/core)"
                        )
                else:
                    backend_results['failed_queries'].append(query)
            
            results[backend] = backend_results
            
            # Print summary
            print(f"\n--- {backend} Summary ---")
            for q, r in backend_results['query_results'].items():
                throughput = r.get('throughput', 0)
                throughput_per_core = r.get('throughput_per_core', 0)
                print(f"  {q}: {throughput:,.0f} events/sec ({throughput_per_core:,.0f}/core)")
            if backend_results['failed_queries']:
                print(f"  Failed: {backend_results['failed_queries']}")
        
        # Save results
        self._save_results(results)
        
        # Print comparison
        self._print_comparison(results)
        
        return results

    def _aggregate_query_results(self, samples: List[dict]) -> dict:
        """Aggregate repeated query samples with median to reduce noise."""
        template = dict(samples[0])

        numeric_fields = [
            'cpu',
            'time_seconds',
            'cores_multiply_time_seconds',
            'throughput',
            'throughput_per_core',
        ]

        for field in numeric_fields:
            values = [s.get(field) for s in samples if isinstance(s.get(field), (int, float))]
            if values:
                template[field] = float(statistics.median(values))

        events_values = [int(s.get('events_num')) for s in samples if s.get('events_num') is not None]
        if events_values:
            template['events_num'] = int(statistics.median(events_values))

        return template
    
    def _save_results(self, results: dict):
        """Save benchmark results to JSON."""
        self.results_dir.mkdir(parents=True, exist_ok=True)
        output_file = self.results_dir / "nexmark_results.json"
        
        results_with_meta = {
            'timestamp': datetime.now().isoformat(),
            'mode': self.mode,
            'nexmark_config': self.nexmark_config,
            'results': results
        }
        
        with open(output_file, 'w') as f:
            json.dump(results_with_meta, f, indent=2)
        
        print(f"\n[Nexmark] Results saved to: {output_file}")
    
    def _print_comparison(self, results: dict):
        """Print comparison between backends."""
        if len(results) < 2:
            return
        
        print(f"\n{'='*60}")
        print("Backend Comparison")
        print(f"{'='*60}")
        
        backends = list(results.keys())
        base_backend = "hashmap" if "hashmap" in backends else backends[0]
        
        for backend in backends:
            if backend == base_backend:
                continue
            
            base_queries = results.get(base_backend, {}).get('query_results', {})
            comp_queries = results.get(backend, {}).get('query_results', {})
            
            print(f"\n{backend} vs {base_backend}:")
            
            for query in sorted(set(base_queries.keys()) | set(comp_queries.keys())):
                base_perf = base_queries.get(query, {}).get('throughput_per_core', 0)
                comp_perf = comp_queries.get(query, {}).get('throughput_per_core', 0)
                
                if base_perf > 0:
                    diff_pct = ((comp_perf - base_perf) / base_perf) * 100
                    symbol = "+" if diff_pct > 0 else ""
                    print(f"  {query}: {comp_perf:,.0f} vs {base_perf:,.0f} ({symbol}{diff_pct:.1f}%)")


def apply_nexmark_scenario(config: dict, scenario_name: str) -> dict:
    """Apply a nexmark_scenarios entry to the config dict (returns a copy)."""
    import copy
    scenarios = config.get('nexmark_scenarios', [])
    scenario = None
    for s in scenarios:
        if s.get('name') == scenario_name:
            scenario = s
            break
    if not scenario:
        print(f"ERROR: NexMark scenario '{scenario_name}' not found.")
        print(f"Available: {[s.get('name') for s in scenarios]}")
        sys.exit(1)

    config = copy.deepcopy(config)

    # Override queries
    if 'queries' in scenario:
        config['nexmark']['queries'] = scenario['queries']

    # Override checkpoint interval
    if 'checkpoint_interval' in scenario:
        config['runtime']['nexmark_checkpoint_interval'] = scenario['checkpoint_interval']

    # Scale event counts
    mult = scenario.get('event_multiplier', 1)
    if mult != 1:
        for key in list(config['nexmark'].keys()):
            if key.endswith('_events'):
                config['nexmark'][key] = int(config['nexmark'][key] * mult)

    # ForL0 backend overrides
    forl0_overrides = scenario.get('forl0_overrides', {})
    if forl0_overrides:
        for b in config.get('backends', []):
            if b.get('name') == 'forl0':
                b.setdefault('config', {}).setdefault('workload_overrides', {}).setdefault('nexmark', {})
                b['config']['workload_overrides']['nexmark'].update(forl0_overrides)

    print(f"[NexMark] Scenario: {scenario_name} — {scenario.get('description', '')}")
    return config


def main():
    parser = argparse.ArgumentParser(description="Run Nexmark DataStream benchmark")
    parser.add_argument("--backend", "-b", 
                       choices=["hashmap", "forl0", "all"],
                       default="all",
                       help="State backend to test")
    parser.add_argument("--queries", "-q",
                       default=None,
                       help="Queries to run (comma-separated, e.g., q5,q8)")
    parser.add_argument("--profile", "-p",
                       type=str, default=None, choices=['cpu', 'cache'],
                       help="Enable profiling: cpu (flame graphs) or cache (cache statistics)")
    parser.add_argument("--repeat", "-r",
                       type=int, default=None,
                       help="Repeat each query N times and aggregate by median")
    parser.add_argument("--scenario", type=str, default=None,
                       help="Run a named scenario from nexmark_scenarios in config")
    
    args = parser.parse_args()
    
    # Load config
    config = load_config()

    # Apply scenario overrides if specified
    if args.scenario:
        config = apply_nexmark_scenario(config, args.scenario)
    
    # Determine backends
    if args.backend == "all":
        backends = ["hashmap", "forl0"]
    else:
        backends = [args.backend]
    
    # Run benchmark
    runner = NexmarkRunner(config)
    results = runner.run(
        backends=backends,
        queries=args.queries,
        profile_mode=args.profile,
        repeat=args.repeat,
    )
    
    # Return non-zero if any query failed
    for backend, metrics in results.items():
        if metrics.get('failed_queries'):
            sys.exit(1)
    
    sys.exit(0)


if __name__ == "__main__":
    main()
