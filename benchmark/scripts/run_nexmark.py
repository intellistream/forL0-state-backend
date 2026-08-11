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
from utils.config import (
    get_results_dir, load_config, parse_json_from_output,
)
from utils.forl0_config import (
    FORL0_CONFIG_MAPPING, build_forl0_config_args,
    get_forl0_effective_config as resolve_forl0_effective_config,
    render_forl0_config_args,
)
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
    summary_match = re.search(
        r'Summary Average:\s+Throughput=([^,]+),\s+Cores=([^\s]+)',
        output,
    )

    def parse_float(value: str) -> float:
        cleaned = value.replace(',', '').replace('/s', '').strip()
        if cleaned in {'NaN', 'nan', 'N/A', ''}:
            return 0.0
        if cleaned == '-':
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

    if summary_match:
        throughput = parse_float(summary_match.group(1))
        cores = parse_float(summary_match.group(2))
        return {
            'cpu': cores,
            'throughput': throughput,
            'throughput_per_core': throughput / cores if cores > 0 else 0.0,
        }

    total_line = None
    first_query_line = None
    for line in output.splitlines():
        if line.strip().startswith('|Total'):
            total_line = line.strip()
        elif re.match(r'^\|\s*q\d+', line.strip(), re.IGNORECASE):
            first_query_line = line.strip()

    if total_line and '|-' in total_line and first_query_line:
        total_line = first_query_line
    if not total_line:
        total_line = first_query_line
    if not total_line:
        return None

    parts = [part.strip() for part in total_line.split('|') if part.strip()]

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


def is_benign_post_summary_cancel_conflict(output: str) -> bool:
    """Accept only the known race where an already-finished job returns 409 on cancel."""
    lowered = output.lower()
    required = (
        'summary average:' in lowered,
        'stop job query' in lowered,
        'status code is 409' in lowered,
        'canceljob' in lowered,
    )
    fatal_markers = (
        'job execution failed',
        'taskmanager lost',
        'outofmemoryerror',
        'container killed',
        'exit code 137',
    )
    return all(required) and not any(marker in lowered for marker in fatal_markers)


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


FULL_GC_PATTERNS = (
    re.compile(r'\bFull GC\b', re.IGNORECASE),
    re.compile(r'\bPause Full\b', re.IGNORECASE),
)


def _taskmanager_gc_log_files(flink_home: str) -> list[Path]:
    """Return TaskManager log/out files that may contain JVM GC lines."""
    log_dir = Path(flink_home) / 'log'
    if not log_dir.exists():
        return []

    files: list[Path] = []
    for pattern in ('*taskexecutor*.log', '*taskexecutor*.out', '*taskmanager*.log', '*taskmanager*.out'):
        files.extend(log_dir.glob(pattern))
    return sorted(set(files))


def _count_full_gc_in_text(content: str) -> int:
    count = 0
    for line in content.splitlines():
        if any(pattern.search(line) for pattern in FULL_GC_PATTERNS):
            count += 1
    return count


def snapshot_taskmanager_full_gc(flink_home: str) -> dict[str, dict[str, int]]:
    """Snapshot current Full GC counters per TaskManager log file.

    The benchmark uses the difference between two snapshots to enforce
    no-Full-GC scenarios without truncating user-visible logs.
    """
    snapshot: dict[str, dict[str, int]] = {}
    for log_file in _taskmanager_gc_log_files(flink_home):
        try:
            content = log_file.read_text(errors='ignore')
        except Exception:
            continue
        snapshot[str(log_file)] = {
            'size': log_file.stat().st_size,
            'full_gc_count': _count_full_gc_in_text(content),
        }
    return snapshot


def full_gc_delta(before: dict[str, dict[str, int]], after: dict[str, dict[str, int]]) -> int:
    """Return total Full GC count increase between two snapshots."""
    total = 0
    for path, after_info in after.items():
        before_count = before.get(path, {}).get('full_gc_count', 0)
        delta = int(after_info.get('full_gc_count', 0)) - int(before_count)
        if delta > 0:
            total += delta
    return total


def get_forl0_effective_config(config: dict, backend: str, query: Optional[str] = None) -> dict:
    """Get the effective ForL0 configuration for NexMark/query wrappers."""
    return resolve_forl0_effective_config(
        config, backend, workload_key='nexmark', query=query)


def get_forl0_config_args(config: dict, backend: str, query: Optional[str] = None) -> list:
    """Get ForL0 StateBackend configuration as JVM -D arguments."""
    # Preserve the historical NexMark command shape: unlike the other two
    # runners it emitted the harmless expected-engine property for HashMap.
    if backend != 'forl0':
        return render_forl0_config_args(
            {}, config.get('runtime', {}).get('parallelism', 1))
    return build_forl0_config_args(
        config, backend, workload_key='nexmark', query=query)


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
        self.results_dir = get_results_dir(f"nexmark_{timestamp}")

        self.metric_reporter_host = self._resolve_metric_reporter_host()
        self.metric_reporter_port: Optional[int] = None
        
        # Profiler
        self.profiler = None
        self.metric_sender_containers = ['flink-taskmanager-1', 'flink-taskmanager-2']
        self.host_metric_sender: Optional[subprocess.Popen] = None
        self.host_metric_conf_dir: Optional[Path] = None
        self.container_profiler_session: Optional[Dict[str, str]] = None
        self.baseline_failed_job_ids: set[str] = set()

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
        """Start CpuMetricSender for the current run.

        Docker deployments monitor TaskManagers from inside the containers.  For
        local standalone runs, fall back to a host-side sender that discovers
        TaskManagerRunner via jps.
        """
        if self.metric_reporter_port is None:
            return

        metric_conf = (
            f'nexmark.metric.reporter.host: {self.metric_reporter_host}\n'
            f'nexmark.metric.reporter.port: {self.metric_reporter_port}\n'
            'nexmark.metric.monitor.interval: 5 s\n'
        )
        started_container_sender = False
        running_containers = self._running_metric_sender_containers()
        for container in running_containers:
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
            started_container_sender = True

        if not started_container_sender:
            self._start_host_metric_sender(metric_conf, env)

    def _running_metric_sender_containers(self) -> list[str]:
        """Return configured TaskManager containers that are currently running."""
        running = []
        for container in self.metric_sender_containers:
            try:
                result = subprocess.run(
                    ['sudo', '-n', 'docker', 'inspect', '-f', '{{.State.Running}}', container],
                    capture_output=True,
                    text=True,
                    timeout=5,
                    check=True,
                )
                if result.stdout.strip().lower() == 'true':
                    running.append(container)
            except Exception:
                continue
        return running

    def _select_container_profiler_target(self) -> Optional[Dict[str, str]]:
        """Pick a running TaskManager container and its Java PID for async-profiler."""
        requested_container = os.environ.get('FLINK_TASKMANAGER_CONTAINER')
        containers = self._running_metric_sender_containers()
        if requested_container:
            containers = [requested_container] + [
                container for container in containers if container != requested_container
            ]

        for container in containers:
            try:
                result = run_checked_command([
                    'sudo', '-n', 'docker', 'exec', container, 'sh', '-lc',
                    (
                        "pid=$(jps -q 2>/dev/null | head -n1 || true); "
                        "if [ -z \"$pid\" ]; then "
                        "  pid=$(pgrep -f 'org.apache.flink.runtime.taskexecutor.TaskManagerRunner' | head -n1 || true); "
                        "fi; "
                        "if [ -z \"$pid\" ] && ps -p 1 -o args= | grep -q 'TaskManagerRunner'; then pid=1; fi; "
                        "if [ -n \"$pid\" ]; then echo \"$pid\"; fi"
                    )
                ], timeout=10)
                pid = result.stdout.strip().splitlines()[-1] if result.stdout.strip() else ''
                if pid.isdigit():
                    return {'container': container, 'pid': pid}
            except Exception:
                continue
        return None

    def _start_host_metric_sender(self, metric_conf: str, env: dict) -> None:
        """Start CpuMetricSender on the host for non-Docker standalone runs."""
        if self.host_metric_sender and self.host_metric_sender.poll() is None:
            return

        self.host_metric_conf_dir = Path(tempfile.mkdtemp(prefix='nexmark-metric-conf-'))
        (self.host_metric_conf_dir / 'nexmark.yaml').write_text(metric_conf)
        classpath = f"{self.nexmark_home / 'lib'}/*:{self.flink_home / 'lib'}/*"
        sender_env = env.copy()
        sender_env['FLINK_HOME'] = str(self.flink_home)
        sender_env['NEXMARK_CONF_DIR'] = str(self.host_metric_conf_dir)
        try:
            self.host_metric_sender = subprocess.Popen(
                [
                    'java',
                    '-cp',
                    classpath,
                    'com.github.nexmark.flink.metric.cpu.CpuMetricSender',
                ],
                env=sender_env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            print(f'  Started host CpuMetricSender (pid={self.host_metric_sender.pid})')
        except Exception as error:
            print(f'  WARNING: Failed to start host CpuMetricSender: {error}')
            shutil.rmtree(self.host_metric_conf_dir, ignore_errors=True)
            self.host_metric_conf_dir = None

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
        if self.host_metric_sender and self.host_metric_sender.poll() is None:
            self.host_metric_sender.terminate()
            try:
                self.host_metric_sender.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.host_metric_sender.kill()
                self.host_metric_sender.wait(timeout=5)
        self.host_metric_sender = None
        if self.host_metric_conf_dir:
            shutil.rmtree(self.host_metric_conf_dir, ignore_errors=True)
            self.host_metric_conf_dir = None

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

    def _failed_job_ids(self) -> set[str]:
        """Return FAILED job IDs currently reported by Flink REST."""
        try:
            jobs_resp = requests.get(f"{self.rest_url}/jobs/overview", timeout=5)
            if jobs_resp.status_code != 200:
                return set()
            return {
                str(job.get('jid'))
                for job in jobs_resp.json().get('jobs', [])
                if job.get('state') == 'FAILED' and job.get('jid')
            }
        except Exception:
            return set()

    def _failed_job_detail(self, job_id: str) -> str:
        """Return a compact Flink REST exception for a failed job."""
        try:
            response = requests.get(
                f"{self.rest_url}/jobs/{job_id}/exceptions?maxExceptions=8",
                timeout=5,
            )
            if response.status_code != 200:
                return f"exception endpoint HTTP {response.status_code}"
            payload = response.json()
            messages = []
            root = payload.get('root-exception')
            if root:
                messages.append(str(root))
            history = payload.get('exceptionHistory', {})
            for entry in history.get('entries', []) if isinstance(history, dict) else []:
                message = entry.get('exception') if isinstance(entry, dict) else None
                if message and str(message) not in messages:
                    messages.append(str(message))
            compact = ' | '.join(' '.join(message.split()) for message in messages)
            return compact[:4000] if compact else 'Flink returned no exception text'
        except Exception as exc:
            return f"cannot query job exception: {exc}"

    def _cluster_health_issue(self) -> Optional[str]:
        """Return a fail-fast reason if the Flink cluster or active jobs are unhealthy."""
        try:
            overview_resp = requests.get(f"{self.rest_url}/overview", timeout=5)
            if overview_resp.status_code != 200:
                return f"Flink overview returned HTTP {overview_resp.status_code}"
            overview = overview_resp.json()
        except Exception as exc:
            return f"Cannot query Flink overview: {exc}"

        expected_tms = int(os.environ.get('FORL0_EXPECTED_TASKMANAGERS', '2'))
        expected_slots = int(os.environ.get('FORL0_EXPECTED_SLOTS', '8'))
        tm_count = int(overview.get('taskmanagers', 0) or 0)
        slots_total = int(overview.get('slots-total', 0) or 0)
        if tm_count < expected_tms:
            return f"TaskManager count dropped below expected: {tm_count} < {expected_tms}"
        if slots_total < expected_slots:
            return f"Slot count dropped below expected: {slots_total} < {expected_slots}"

        try:
            jobs_resp = requests.get(f"{self.rest_url}/jobs/overview", timeout=5)
            if jobs_resp.status_code != 200:
                return f"Flink jobs overview returned HTTP {jobs_resp.status_code}"
            for job in jobs_resp.json().get('jobs', []):
                jid = str(job.get('jid')) if job.get('jid') else ''
                if job.get('state') == 'FAILED' and jid not in self.baseline_failed_job_ids:
                    detail = self._failed_job_detail(jid)
                    return (
                        "Flink job failed during NexMark run: "
                        f"{job.get('jid')} {job.get('name', 'unknown')}\n"
                        f"Flink failure detail: {detail}"
                    )
        except Exception as exc:
            return f"Cannot query Flink jobs overview: {exc}"

        return None

    def _run_nexmark_driver(self, cmd: list[str], env: dict, timeout_seconds: int) -> tuple[int, str, float]:
        """Run the Java NexMark driver while monitoring Flink health.

        The NexMark driver can get stuck repeatedly stopping an already FAILED job.  In
        that case a large subprocess timeout wastes the offline experiment window, so
        fail fast as soon as REST reports a failed job or a degraded cluster.
        """
        process_start = time.perf_counter()
        self.baseline_failed_job_ids = self._failed_job_ids()
        with tempfile.TemporaryFile(mode='w+', encoding='utf-8', errors='replace') as output_file:
            proc = subprocess.Popen(
                cmd,
                env=env,
                stdout=output_file,
                stderr=subprocess.STDOUT,
                text=True,
            )
            fail_reason: Optional[str] = None

            while True:
                returncode = proc.poll()
                if returncode is not None:
                    break

                elapsed = time.perf_counter() - process_start
                if elapsed > timeout_seconds:
                    fail_reason = f"NexMark driver timed out after {timeout_seconds}s"
                    break

                fail_reason = self._cluster_health_issue()
                if fail_reason:
                    break

                time.sleep(5)

            if fail_reason:
                print(f"  ERROR: {fail_reason}")
                proc.terminate()
                try:
                    proc.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=20)
                output_file.write(f"\n[NexMark fail-fast] {fail_reason}\n")

            elapsed = time.perf_counter() - process_start
            output_file.seek(0)
            output = output_file.read()
            return proc.returncode if proc.returncode is not None else 124, output, elapsed

    def _start_container_profiler(self, backend: str, query: str, profile_mode: str) -> bool:
        """Start async-profiler inside taskmanager container and return whether startup succeeded."""
        profiler_home = os.environ.get('ASYNC_PROFILER_HOME')
        if not profiler_home:
            return False

        host_asprof = Path(profiler_home) / 'bin' / 'asprof'
        if not host_asprof.exists():
            return False

        target = self._select_container_profiler_target()
        if not target:
            return False

        container = target['container']
        pid = target['pid']
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
                'sh', '-lc', f'{target_dir}/bin/asprof stop {pid} >/dev/null 2>&1 || true'
            ], timeout=15)

            interval = '10000' if event == 'cache-misses' else '10ms'
            run_checked_command([
                'sudo', '-n', 'docker', 'exec', container,
                'sh', '-lc',
                f'{target_dir}/bin/asprof start -e {event} -i {interval} -f {tmp_output} {pid}'
            ], timeout=20)

            self.container_profiler_session = {
                'container': container,
                'pid': pid,
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
        pid = session.get('pid', '1')
        tmp_output = session['tmp_output']
        local_output = session['local_output']

        try:
            stop_script = (
                f'/tmp/async-profiler/bin/asprof stop -f {tmp_output} {pid} '
                f'|| test -s {tmp_output}'
            )
            run_checked_command([
                'sudo', '-n', 'docker', 'exec', container, 'sh', '-lc', stop_script
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
        configured_home = os.environ.get('NEXMARK_HOME', '').strip()
        candidates = [
            Path(configured_home).expanduser() if configured_home else None,
            self.project_root / 'benchmark' / 'nexmark-src' / 'nexmark-flink' / 'target' / 'nexmark-flink-bin' / 'nexmark-flink',
            self.project_root / 'docker' / 'deploy' / 'nexmark-flink',
        ]

        for candidate in candidates:
            if candidate is None:
                continue
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

    def _get_query_override(self, query: str, key: str, default=None):
        """Get a NexMark setting with optional per-query override."""
        overrides = self.nexmark_config.get('query_overrides', {})
        if isinstance(overrides, dict):
            query_override = overrides.get(query, {})
            if isinstance(query_override, dict) and key in query_override:
                return query_override[key]
        return self.nexmark_config.get(key, default)

    def _get_query_category(self) -> str:
        """Return a safe NexMark query-category name from the scenario config."""
        category = str(self.nexmark_config.get('category', 'oa')).strip().lower()
        if not re.fullmatch(r'[a-z0-9][a-z0-9_-]*', category):
            raise ValueError(f'Invalid NexMark query category: {category!r}')
        return category

    @staticmethod
    def _get_query_category_suffix(category: str) -> str:
        return '' if category == 'oa' else f'.{category}'

    def _prepare_driver_location(self, conf_dir: Path, category: str) -> tuple[Path, str]:
        """Create a category-compatible distribution view for the Java driver.

        The Java-8 offline driver shipped in the repository accepts ``--category``
        but an old Benchmark.class reads the option's default value (``oa``)
        instead of the parsed command line.  Non-OA workloads consequently fail
        before submission with "workload ... is not defined".

        Normalize a custom category to OA in an isolated temporary view.  Its
        ``queries`` entry points at the requested category directory, while the
        other distribution entries continue to point at the packaged NexMark
        home.  This preserves category-specific SQL and works with both the old
        offline class and corrected drivers.
        """
        if category == 'oa':
            return self.nexmark_home, category

        category_queries = self.nexmark_home / f'queries-{category}'
        if not category_queries.is_dir():
            raise FileNotFoundError(
                f'NexMark query category {category!r} is missing: {category_queries}'
            )

        driver_home = conf_dir / 'nexmark-home'
        driver_home.mkdir()
        for entry in self.nexmark_home.iterdir():
            if entry.name == 'queries' or entry.name.startswith('queries-'):
                continue
            os.symlink(entry.resolve(), driver_home / entry.name,
                       target_is_directory=entry.is_dir())

        # q10 and any future file-output query should retain durable output
        # outside the temporary configuration directory.
        data_dir = self.nexmark_home / 'data'
        data_dir.mkdir(exist_ok=True)
        if not (driver_home / 'data').exists():
            os.symlink(data_dir.resolve(), driver_home / 'data', target_is_directory=True)
        os.symlink(category_queries.resolve(), driver_home / 'queries', target_is_directory=True)
        return driver_home, 'oa'
    
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

        driver_lib_dir = self.nexmark_home / 'lib'
        driver_lib_dir.mkdir(parents=True, exist_ok=True)
        driver_lib_jar = driver_lib_dir / self.nexmark_jar.name
        if not driver_lib_jar.exists() or driver_lib_jar.read_bytes() != self.nexmark_jar.read_bytes():
            for stale_jar in driver_lib_dir.glob('nexmark-flink-*.jar'):
                stale_jar.unlink()
            shutil.copy(self.nexmark_jar, driver_lib_jar)
            print('[Nexmark] Synced Java-compatible NexMark JAR to driver distribution')

    def _write_nexmark_conf(
        self,
        query: str,
        backend: str,
        num_events: int,
        tps: int,
        warmup_duration: int,
        checkpoint_interval_ms: int,
        driver_category: str,
    ) -> Path:
        """Write a temporary nexmark.yaml tuned for the requested query run."""
        conf_dir = Path(tempfile.mkdtemp(prefix='nexmark-conf-'))
        shutil.copy(self.nexmark_home / 'conf' / 'config.yaml', conf_dir / 'config.yaml')
        shutil.copy(self.nexmark_home / 'conf' / 'log4j.properties', conf_dir / 'log4j.properties')

        config_yaml_path = conf_dir / 'config.yaml'
        config_lines = config_yaml_path.read_text().splitlines()

        def format_yaml_value(value) -> str:
            if isinstance(value, bool):
                return 'true' if value else 'false'
            return str(value)

        def set_config_line(key: str, value) -> None:
            rendered = f'{key}: {format_yaml_value(value)}'
            for idx, line in enumerate(config_lines):
                if re.match(rf'^{re.escape(key)}\s*:', line):
                    config_lines[idx] = rendered
                    return
            config_lines.append(rendered)

        def remove_config_line(key: str) -> None:
            config_lines[:] = [
                line for line in config_lines
                if not re.match(rf'^{re.escape(key)}\s*:', line)
            ]

        backends_list = {b['name']: b['class'] for b in self.config.get('backends', [])}
        backend_class = backends_list.get(backend, backend)
        remove_config_line('state.backend')
        set_config_line('state.backend.type', backend_class)
        if checkpoint_interval_ms > 0:
            set_config_line('execution.checkpointing.interval', checkpoint_interval_ms)
        else:
            remove_config_line('execution.checkpointing.interval')
        state_dir = self.project_root / 'docker' / 'generated' / 'flink-state'
        checkpoint_dir = state_dir / 'checkpoints'
        savepoint_dir = state_dir / 'savepoints'
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        savepoint_dir.mkdir(parents=True, exist_ok=True)
        set_config_line('state.checkpoints.dir', f'file://{checkpoint_dir}')
        set_config_line('state.savepoints.dir', f'file://{savepoint_dir}')
        set_config_line('parallelism.default', self.runtime_config.get('parallelism', 4))

        for key, value in self.nexmark_config.get('flink_config_overrides', {}).items():
            set_config_line(str(key), value)

        effective_forl0 = get_forl0_effective_config(self.config, backend, query)
        rendered_forl0 = render_forl0_config_args(
            effective_forl0, self.runtime_config.get('parallelism', 1))
        for argument in rendered_forl0:
            key, value = argument[2:].split('=', 1)
            set_config_line(key, value)

        config_yaml_path.write_text('\n'.join(config_lines) + '\n')

        warmup_seconds = max(0, int(warmup_duration))
        warmup_events = num_events if warmup_seconds > 0 else 0
        workload_tps = max(0, int(tps))
        warmup_tps = workload_tps if warmup_seconds > 0 else 0
        self.metric_reporter_port = find_free_tcp_port()

        person_proportion = self._get_query_override(query, 'person_proportion', 1)
        auction_proportion = self._get_query_override(query, 'auction_proportion', 3)
        bid_proportion = self._get_query_override(query, 'bid_proportion', 46)
        metric_monitor_delay = self._get_query_override(query, 'metric_monitor_delay')
        metric_monitor_interval = self._get_query_override(query, 'metric_monitor_interval')
        metric_monitor_duration = self._get_query_override(query, 'metric_monitor_duration')
        metric_tps_vertex = self._get_query_override(query, 'metric_tps_vertex')
        bid_hot_ratio_auctions = self._get_query_override(query, 'bid_hot_ratio_auctions')
        bid_hot_ratio_bidders = self._get_query_override(query, 'bid_hot_ratio_bidders')
        auction_hot_ratio_sellers = self._get_query_override(query, 'auction_hot_ratio_sellers')

        nexmark_yaml = (
            f'nexmark.metric.reporter.host: {self.metric_reporter_host}\n'
            f'nexmark.metric.reporter.port: {self.metric_reporter_port}\n'
            f'nexmark.workload.suite.run.events.num: {num_events}\n'
            f'nexmark.workload.suite.run.tps: {workload_tps}\n'
            f'nexmark.workload.suite.run.queries{self._get_query_category_suffix(driver_category)}: "{query}"\n'
            f'nexmark.workload.suite.run.percentage: "bid:{bid_proportion},'
            f'auction:{auction_proportion},'
            f'person:{person_proportion}"\n'
            f'nexmark.workload.suite.run.warmup.duration: {warmup_seconds}s\n'
            f'nexmark.workload.suite.run.warmup.events.num: {warmup_events}\n'
            f'nexmark.workload.suite.run.warmup.tps: {warmup_tps}\n'
            f'flink.rest.address: {self.rest_url.split("//", 1)[-1].split(":", 1)[0]}\n'
            f'flink.rest.port: {self.rest_url.rsplit(":", 1)[-1]}\n'
        )
        if metric_monitor_delay:
            nexmark_yaml += f'nexmark.metric.monitor.delay: {metric_monitor_delay}\n'
        if metric_monitor_interval:
            nexmark_yaml += f'nexmark.metric.monitor.interval: {metric_monitor_interval}\n'
        if metric_monitor_duration:
            nexmark_yaml += f'nexmark.metric.monitor.duration: {metric_monitor_duration}\n'
        if metric_tps_vertex:
            nexmark_yaml += f'nexmark.metric.tps.vertex: {metric_tps_vertex}\n'
        if bid_hot_ratio_auctions is not None:
            nexmark_yaml += f'bid.hot-ratio.auctions: {int(bid_hot_ratio_auctions)}\n'
        if bid_hot_ratio_bidders is not None:
            nexmark_yaml += f'bid.hot-ratio.bidders: {int(bid_hot_ratio_bidders)}\n'
        if auction_hot_ratio_sellers is not None:
            nexmark_yaml += f'auction.hot-ratio.sellers: {int(auction_hot_ratio_sellers)}\n'
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
        category = self._get_query_category()
        conf_dir = self._write_nexmark_conf(
            query,
            backend,
            num_events,
            tps,
            warmup_duration,
            checkpoint_interval_ms,
            'oa' if category != 'oa' else category,
        )
        try:
            driver_home, driver_category = self._prepare_driver_location(conf_dir, category)
        except Exception:
            shutil.rmtree(conf_dir, ignore_errors=True)
            raise
        if driver_category != category:
            print(
                f'[Nexmark] Category compatibility view: {category} -> '
                f'{driver_category} ({driver_home / "queries"})'
            )
        classpath = f"{self.nexmark_home / 'lib'}/*:{self.flink_home / 'lib'}/*"
        forl0_args = get_forl0_config_args(self.config, backend, query)
        backends_list = {b['name']: b['class'] for b in self.config.get('backends', [])}
        backend_class = backends_list.get(backend, '')
        cmd = [
            'java',
            f'-Dlog.file={self.nexmark_home / "log" / "nexmark-flink.log"}',
            f'-Dlog4j.configuration=file:{conf_dir / "log4j.properties"}',
            f'-Dlog4j.configurationFile=file:{conf_dir / "log4j.properties"}',
        ]
        if backend_class:
            cmd.append(f'-Dstate.backend.type={backend_class}')
        cmd.extend(forl0_args)
        cmd.extend([
            '-cp',
            classpath,
            NEXMARK_MAIN_CLASS,
            '--location',
            str(driver_home),
            '--queries',
            query,
            '--category',
            driver_category,
        ])

        env = os.environ.copy()
        env['FLINK_HOME'] = str(self.flink_home)
        env['NEXMARK_CONF_DIR'] = str(conf_dir)
        env['FLINK_CONF_DIR'] = str(conf_dir)

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
        tps = self._get_query_override(query, 'tps', 0)
        warmup_duration = self._get_query_override(query, 'warmup_duration', 0)
        
        # Event proportions (Nexmark default: 1:3:46)
        person_proportion = self._get_query_override(query, 'person_proportion', 1)
        auction_proportion = self._get_query_override(query, 'auction_proportion', 3)
        bid_proportion = self._get_query_override(query, 'bid_proportion', 46)
        bid_hot_ratio_auctions = self._get_query_override(query, 'bid_hot_ratio_auctions')
        bid_hot_ratio_bidders = self._get_query_override(query, 'bid_hot_ratio_bidders')
        
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
        if bid_hot_ratio_auctions is not None or bid_hot_ratio_bidders is not None:
            print(
                "Hot ratios: "
                f"bid.auctions={bid_hot_ratio_auctions if bid_hot_ratio_auctions is not None else 'default'}, "
                f"bid.bidders={bid_hot_ratio_bidders if bid_hot_ratio_bidders is not None else 'default'}"
            )
        print(f"Command: {' '.join(cmd)}\n")
        
        max_attempts = max(1, int(self.nexmark_config.get('max_attempts_per_query', 3)))
        query_timeout = max(3600, int(self.nexmark_config.get('timeout_seconds', 7200)))
        retry_backoff_seconds = max(1, int(self.nexmark_config.get('retry_backoff_seconds', 4)))
        min_cpu_cores = float(self.nexmark_config.get('min_cpu_cores', 0.0))
        min_profile_cpu_cores = float(self.nexmark_config.get('min_profile_cpu_cores', 0.05))
        reject_full_gc = bool(self.nexmark_config.get('reject_full_gc', False))
        collect_full_gc = reject_full_gc or bool(self.nexmark_config.get('collect_full_gc', False))
        max_full_gc_delta = int(self.nexmark_config.get('max_full_gc_delta', 0))

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
                    gc_before = snapshot_taskmanager_full_gc(str(self.flink_home)) if collect_full_gc else {}
                    self._start_metric_senders(env)
                    returncode, output, process_elapsed = self._run_nexmark_driver(cmd, env, query_timeout)
                    print(output)
                    gc_after = snapshot_taskmanager_full_gc(str(self.flink_home)) if collect_full_gc else {}
                    run_full_gc_delta = full_gc_delta(gc_before, gc_after) if collect_full_gc else 0

                    output_lower = output.lower()
                    retryable_error = (
                        "metric reporter doesn't collect any metrics" in output_lower
                        or "can't find tps metric name from the response" in output_lower
                        or 'process failed due to timeout' in output_lower
                        or 'profiler already started' in output_lower
                    )

                    parsed = parse_nexmark_summary(output)
                    if returncode != 0:
                        if parsed and is_benign_post_summary_cancel_conflict(output):
                            print('  WARNING: NexMark cancel returned 409 after a complete summary; keeping parsed sample')
                            parsed['driver_cleanup_conflict_409'] = True
                        else:
                            print('  ERROR: NexMark driver failed; rejecting sample even if a partial summary exists')
                            parsed = None
                            should_retry = True
                    if parsed:
                        events_num = parsed.get('events_num') or num_events
                        if process_elapsed > 0 and events_num:
                            parsed['process_elapsed_seconds'] = process_elapsed
                            parsed['process_throughput'] = float(events_num) / process_elapsed

                        # Filter clearly invalid samples from noisy metric reporter runs.
                        cpu = parsed.get('cpu', 0)
                        tpc = parsed.get('throughput_per_core', 0)
                        invalid_sample = cpu <= 0 or tpc <= 0

                        if min_cpu_cores > 0 and cpu < min_cpu_cores:
                            print(
                                f'  WARNING: Sample too idle (cpu={cpu:.3f} < '
                                f'min_cpu_cores={min_cpu_cores:.3f}), retrying'
                            )
                            invalid_sample = True

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
                        elif reject_full_gc and run_full_gc_delta > max_full_gc_delta:
                            print(
                                f'  WARNING: Full GC detected during sample '
                                f'(delta={run_full_gc_delta}, max={max_full_gc_delta}), rejecting sample'
                            )
                            invalid_sample = True
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
                                'bid_hot_ratio_auctions': bid_hot_ratio_auctions,
                                'bid_hot_ratio_bidders': bid_hot_ratio_bidders,
                                'auction_hot_ratio_sellers': self._get_query_override(query, 'auction_hot_ratio_sellers'),
                                'configured_tps': tps,
                                'metric_tps_vertex': self._get_query_override(query, 'metric_tps_vertex'),
                                'metric_monitor_duration': self._get_query_override(query, 'metric_monitor_duration'),
                                'collect_full_gc': collect_full_gc,
                                'reject_full_gc': reject_full_gc,
                                'max_full_gc_delta': max_full_gc_delta,
                                'full_gc_delta': run_full_gc_delta,
                            })
                            print(f"\n  Result: {parsed.get('throughput', 0):,.2f} events/sec")
                            if parsed.get('process_throughput'):
                                print(
                                    "  Process wall-clock: "
                                    f"{parsed['process_elapsed_seconds']:.2f}s, "
                                    f"{parsed['process_throughput']:,.2f} events/sec"
                                )
                            return parsed
                    else:
                        if returncode == 0:
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
        self.nexmark_config['queries'] = ','.join(query_list)
        self.selected_queries = query_list
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
                process_throughput = r.get('process_throughput', 0)
                process_elapsed = r.get('process_elapsed_seconds', 0)
                if process_throughput and process_elapsed:
                    print(
                        f"  {q}: {throughput:,.0f} events/sec ({throughput_per_core:,.0f}/core), "
                        f"process {process_throughput:,.0f} events/sec in {process_elapsed:.2f}s"
                    )
                else:
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
            'process_elapsed_seconds',
            'process_throughput',
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
            'run_id': os.environ.get('FORL0_RUN_ID', '').strip() or None,
            'run_started_epoch': int(os.environ['FORL0_RUN_STARTED_EPOCH'])
            if os.environ.get('FORL0_RUN_STARTED_EPOCH', '').isdigit() else None,
            'variant': os.environ.get('FORL0_VARIANT', '').strip() or None,
            'control_revision': os.environ.get('FORL0_CONTROL_REVISION', '').strip() or None,
            'mode': self.mode,
            'scenario_name': self.nexmark_config.get('scenario_name'),
            'selected_queries': getattr(self, 'selected_queries', []),
            'cli_args': self.nexmark_config.get('cli_args', {}),
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
                base_result = base_queries.get(query, {})
                comp_result = comp_queries.get(query, {})
                base_perf = base_result.get('process_throughput') or base_result.get('throughput', 0)
                comp_perf = comp_result.get('process_throughput') or comp_result.get('throughput', 0)
                
                if base_perf > 0:
                    diff_pct = ((comp_perf - base_perf) / base_perf) * 100
                    symbol = "+" if diff_pct > 0 else ""
                    metric = "process throughput" if base_result.get('process_throughput') else "throughput"
                    print(f"  {query}: {comp_perf:,.0f} vs {base_perf:,.0f} ({symbol}{diff_pct:.1f}%, {metric})")


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

    runtime_overrides = scenario.get('runtime_overrides', {})
    if isinstance(runtime_overrides, dict):
        config.setdefault('runtime', {}).update(runtime_overrides)

    # Direct NexMark workload overrides.  These are intentionally scenario-scoped
    # so the contract baseline remains untouched while pressure tests can push
    # the source and metric filters hard enough to expose backend differences.
    for key in (
        'tps',
        'warmup_duration',
        'warmup_events',
        'timeout_seconds',
        'max_attempts_per_query',
        'repeat_per_query',
        'min_success_samples',
        'min_cpu_cores',
        'min_profile_cpu_cores',
        'person_proportion',
        'auction_proportion',
        'bid_proportion',
        'flink_config_overrides',
        'metric_monitor_delay',
        'metric_monitor_interval',
        'metric_monitor_duration',
        'metric_tps_vertex',
        'collect_full_gc',
        'reject_full_gc',
        'max_full_gc_delta',
        'query_overrides',
        'category',
    ):
        if key in scenario:
            config['nexmark'][key] = scenario[key]

    # Per-query absolute event counts, e.g. {q19: 300000000}.  This is more
    # useful than a blanket multiplier for NexMark because q9/q20 have different
    # state growth profiles from q19.
    query_events = scenario.get('query_events', {})
    if isinstance(query_events, dict):
        for query, events in query_events.items():
            config['nexmark'][f'{query}_events'] = int(events)

    # Scale event counts
    mult = scenario.get('event_multiplier', 1)
    if mult != 1:
        for key in list(config['nexmark'].keys()):
            if key.endswith('_events'):
                config['nexmark'][key] = int(config['nexmark'][key] * mult)

    # Some historical probe results were collected with the ForL0 backend's
    # built-in defaults, not with the benchmark-wide tuning block below.
    if scenario.get('forl0_config_mode') == 'backend_defaults':
        scenario_queries = [
            q.strip()
            for q in str(config.get('nexmark', {}).get('queries', '')).split(',')
            if q.strip()
        ]
        for b in config.get('backends', []):
            if b.get('name') != 'forl0':
                continue
            backend_cfg = b.setdefault('config', {})
            for yaml_key in FORL0_CONFIG_MAPPING:
                backend_cfg.pop(yaml_key, None)
            workload_overrides = backend_cfg.get('workload_overrides')
            if isinstance(workload_overrides, dict):
                workload_overrides.pop('nexmark', None)
            query_overrides = backend_cfg.get('query_overrides')
            if isinstance(query_overrides, dict):
                for query in scenario_queries:
                    query_overrides.pop(query, None)

    # ForL0 backend overrides
    forl0_overrides = scenario.get('forl0_overrides', {})
    if forl0_overrides:
        scenario_queries = [
            q.strip()
            for q in str(config.get('nexmark', {}).get('queries', '')).split(',')
            if q.strip()
        ]
        for b in config.get('backends', []):
            if b.get('name') == 'forl0':
                backend_cfg = b.setdefault('config', {})
                backend_cfg.setdefault('workload_overrides', {}).setdefault('nexmark', {})
                backend_cfg['workload_overrides']['nexmark'].update(forl0_overrides)

                # Scenario overrides are intentionally stronger than the
                # conservative per-query defaults used by the contract runs.
                query_overrides = backend_cfg.setdefault('query_overrides', {})
                for query in scenario_queries:
                    query_overrides.setdefault(query, {}).update(forl0_overrides)

    config['nexmark']['scenario_name'] = scenario_name
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

    if args.queries:
        config['nexmark']['queries'] = args.queries
    config['nexmark']['cli_args'] = {
        'backend': args.backend,
        'queries': args.queries,
        'profile': args.profile,
        'repeat': args.repeat,
        'scenario': args.scenario,
    }
    
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
