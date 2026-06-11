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
import requests
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))
from utils.config import load_config, parse_json_from_output
from utils.profiler import AsyncProfiler, find_taskmanager_pids


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


def get_forl0_config_args(config: dict, backend: str) -> list:
    """Get ForL0 StateBackend configuration as Flink -D arguments."""
    if backend != 'forl0':
        return []
    
    backend_config = None
    for b in config.get('backends', []):
        if b.get('name') == 'forl0':
            backend_config = b.get('config', {})
            break
    
    if not backend_config:
        return []
    
    args = []
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
        
        # Flink paths
        flink_home_env = os.environ.get('FLINK_HOME')
        if flink_home_env:
            self.flink_home = Path(flink_home_env)
        else:
            self.flink_home = Path.home() / "flink" / "flink-1.20.0"
        
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

    def _write_nexmark_conf(self, query: str, num_events: int, tps: int, warmup_duration: int) -> Path:
        """Write a temporary nexmark.yaml tuned for the requested query run."""
        conf_dir = Path(tempfile.mkdtemp(prefix='nexmark-conf-'))
        shutil.copy(self.nexmark_home / 'conf' / 'config.yaml', conf_dir / 'config.yaml')
        shutil.copy(self.nexmark_home / 'conf' / 'log4j.properties', conf_dir / 'log4j.properties')

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

    def _build_driver_command(self, query: str, backend: str, num_events: int, tps: int, warmup_duration: int) -> tuple[list[str], dict, Path]:
        """Build the NexMark benchmark-driver command and environment."""
        conf_dir = self._write_nexmark_conf(query, num_events, tps, warmup_duration)
        classpath = f"{self.nexmark_home / 'lib'}/*:{self.flink_home / 'lib'}/*"
        cmd = [
            'java',
            f'-Dlog.file={self.nexmark_home / "log" / "nexmark-flink.log"}',
            f'-Dlog4j.configuration=file:{conf_dir / "log4j.properties"}',
            f'-Dlog4j.configurationFile=file:{conf_dir / "log4j.properties"}',
            '-cp',
            classpath,
            NEXMARK_MAIN_CLASS,
            '--location',
            str(self.nexmark_home),
            '--queries',
            query,
            '--category',
            'oa',
        ]

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
        checkpoint_interval = self.runtime_config.get('checkpoint_interval', 0)
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
        cmd, env, conf_dir = self._build_driver_command(query, backend, num_events, tps, warmup_duration)
        
        print(f"\n=== Running Nexmark {query.upper()} ({backend} backend) ===")
        print(f"Events: {num_events:,}, TPS: {tps if tps > 0 else 'unlimited'}")
        print(f"Proportions: Person({person_proportion}):Auction({auction_proportion}):Bid({bid_proportion})")
        print(f"Command: {' '.join(cmd)}\n")
        
        # Initialize profiler if enabled
        tm_pids = []
        if profile_mode and profile_mode in ['cpu', 'cache']:
            profiler_home = os.environ.get('ASYNC_PROFILER_HOME')
            self.profiler = AsyncProfiler(profiler_home)
            
            if self.profiler.is_available():
                tm_pids = find_taskmanager_pids(str(self.flink_home))
                if tm_pids:
                    profiles_dir = self.results_dir / "profiles"
                    profiles_dir.mkdir(parents=True, exist_ok=True)
                    
                    events = ['cpu', 'alloc'] if profile_mode == 'cpu' else ['cache-misses']
                    self.profiler.start(
                        pid=tm_pids[0],
                        events=events,
                        output_dir=str(profiles_dir),
                        backend=backend,
                        query=query,
                        output_format='html',
                        duration=None
                    )
                    print(f"  Started {profile_mode} profiling (PID: {tm_pids[0]})")
        
        try:
            self._start_metric_senders(env)
            result = subprocess.run(
                cmd,
                env=env,
                capture_output=True,
                text=True,
                timeout=max(3600, int(self.nexmark_config.get('timeout_seconds', 7200))),
            )
            output = result.stdout + result.stderr
            print(output)

            if result.returncode != 0:
                print('  ERROR: NexMark benchmark driver failed')
                return None
            
            # Stop profiler
            if self.profiler and tm_pids:
                try:
                    self.profiler.stop(tm_pids[0])
                    print(f"  Profiler stopped")
                except Exception as e:
                    print(f"  Profiler stop error: {e}")
            
            parsed = parse_nexmark_summary(output)
            if parsed:
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

            print('  WARNING: Could not parse NexMark summary output')
            return None
                
        except Exception as e:
            print(f"  ERROR: {e}")
            return None
        finally:
            self._stop_metric_senders()
            shutil.rmtree(conf_dir, ignore_errors=True)
    
    def run(
            self, 
            backends: List[str], 
            queries: Optional[str] = None,
            profile_mode: Optional[str] = None
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
        
        print(f"\n{'='*60}")
        print(f"Nexmark DataStream Benchmark")
        print(f"{'='*60}")
        print(f"Queries: {query_list}")
        print(f"Backends: {backends}")
        print(f"Mode: {self.mode}")
        print(f"{'='*60}\n")
        
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
                result = self.run_query(query, backend, profile_mode)
                
                if result:
                    backend_results['query_results'][query] = result
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
    
    args = parser.parse_args()
    
    # Load config
    config = load_config()
    
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
        profile_mode=args.profile
    )
    
    # Return non-zero if any query failed
    for backend, metrics in results.items():
        if metrics.get('failed_queries'):
            sys.exit(1)
    
    sys.exit(0)


if __name__ == "__main__":
    main()
