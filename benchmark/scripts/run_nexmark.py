#!/usr/bin/env python3
"""
NexMark Benchmark Runner for ForL0 State Backend

This script runs NexMark benchmark using the official NexMark benchmark tool.
It reads configuration from benchmark.yaml and dynamically generates nexmark.yaml.

Usage:
    python run_nexmark.py                       # Run with both backends
    python run_nexmark.py --backend forl0       # Run only ForL0
    python run_nexmark.py --queries q5,q8       # Specific queries
    python run_nexmark.py --profile             # Enable flame graphs + hardware metrics
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))
from utils.config import load_config
from utils.profiler import AsyncProfiler
from utils.l0_metrics import (
    parse_l0table_metrics_by_time,
    normalize_metrics_time,
    save_l0table_metrics,
    get_l0_metrics_summary
)
from utils.hardware_metrics import HardwareMetricsCollector


class NexMarkRunner:
    """NexMark benchmark runner with state backend comparison"""
    
    def __init__(self, config: dict):
        self.config = config
        self.project_root = Path(__file__).parent.parent.parent
        self.benchmark_root = Path(__file__).parent.parent
        
        # Get mode-specific config
        self.mode = config.get('mode', 'local')
        self.mode_config = config.get(self.mode, {})
        self.nexmark_config = self.mode_config.get('nexmark', {})
        
        # NexMark paths
        self.nexmark_home = self.benchmark_root / "nexmark-src" / "nexmark-flink" / "target" / "nexmark-flink-bin" / "nexmark-flink"
        self.nexmark_conf_dir = self.nexmark_home / "conf"
        self.nexmark_yaml = self.nexmark_conf_dir / "nexmark.yaml"
        self.nexmark_yaml_backup = self.nexmark_conf_dir / "nexmark.yaml.original"
        
        # Flink paths
        flink_home_env = os.environ.get('FLINK_HOME')
        if flink_home_env:
            self.flink_home = Path(flink_home_env)
        else:
            self.flink_home = Path.home() / "flink" / "flink-1.20.0"
        
        self.flink_config = self.flink_home / "conf" / "config.yaml"
        self.flink_config_backup = self.flink_home / "conf" / "config.yaml.bak"
        
        # ForL0 JAR
        self.forl0_jar = self._find_forl0_jar()
        
        # Results directory (can be overridden by run_all.py)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.results_dir = self.benchmark_root / "results" / f"nexmark_{timestamp}"
        
        # L0 metrics results directory (shared across runs)
        self.l0_metrics_dir = self.benchmark_root / "results" / "l0metrics"
        
        # Hardware metrics results directory
        self.hw_metrics_dir = self.benchmark_root / "results" / "hardware"
        
        # Profiler (optional)
        self.profiler = None
        
        # Hardware metrics collector (optional)
        self.hw_collector = None
        
        # Current backend being tested (for L0 metrics)
        self.current_backend = None
        
    def _find_forl0_jar(self) -> Path:
        """Find the ForL0 StateBackend JAR"""
        target_dir = self.project_root / "target"
        pattern = "flink-statebackend-forL0-*.jar"
        
        for jar in target_dir.glob(pattern):
            if "sources" not in jar.name and "javadoc" not in jar.name:
                return jar
        
        raise FileNotFoundError(f"ForL0 JAR not found in {target_dir}. Run 'mvn package' first.")
    
    def _check_nexmark(self):
        """Check if NexMark is compiled"""
        if not self.nexmark_home.exists():
            raise FileNotFoundError(
                f"NexMark not found at {self.nexmark_home}. "
                "Please compile NexMark first: cd benchmark/nexmark-src && mvn clean package -DskipTests"
            )
        
        lib_dir = self.nexmark_home / "lib"
        nexmark_jars = list(lib_dir.glob("nexmark-flink-*.jar"))
        if not nexmark_jars:
            raise FileNotFoundError(f"NexMark JAR not found in {lib_dir}")
        
        print(f"[NexMark] Using NexMark from: {self.nexmark_home}")
        print(f"[NexMark] Found JAR: {nexmark_jars[0].name}")
    
    def _check_flink(self):
        """Check if Flink is available"""
        if not self.flink_home.exists():
            raise FileNotFoundError(f"Flink not found at {self.flink_home}. Set FLINK_HOME env var.")
        print(f"[NexMark] Using Flink from: {self.flink_home}")
    
    def _backup_configs(self):
        """Backup Flink and NexMark config files"""
        if self.flink_config.exists():
            shutil.copy(self.flink_config, self.flink_config_backup)
        if self.nexmark_yaml.exists() and not self.nexmark_yaml_backup.exists():
            shutil.copy(self.nexmark_yaml, self.nexmark_yaml_backup)
        print(f"[NexMark] Config files backed up")
    
    def _restore_configs(self):
        """Restore Flink config file (not NexMark config, which we want to keep)"""
        if self.flink_config_backup.exists():
            shutil.copy(self.flink_config_backup, self.flink_config)
        # Note: Don't restore nexmark.yaml - we want to keep our generated config
        print(f"[NexMark] Flink config restored")
    
    def _generate_nexmark_yaml(self, queries: str):
        """Generate nexmark.yaml from benchmark.yaml config"""
        events = self.nexmark_config.get('events', 10000000)
        tps = self.nexmark_config.get('tps', 500000)
        warmup_events = self.nexmark_config.get('warmup_events', 100000)
        warmup_duration = self.nexmark_config.get('warmup_duration', 10)
        
        # Always set monitorDelay to 0 to avoid hanging when job finishes quickly
        # This is safe because we're using events.num mode which waits for job completion
        monitor_delay = 0
        
        # Build nexmark.yaml content
        content = f"""################################################################################
# NexMark Configuration (auto-generated from benchmark.yaml)
################################################################################

# Metric reporter
nexmark.metric.reporter.host: localhost
nexmark.metric.reporter.port: 9098

# Flink REST
flink.rest.address: localhost
flink.rest.port: 8081

# Metric monitoring settings (adjusted for events mode)
# When using events.num mode, set delay to 0 to start monitoring immediately
nexmark.metric.monitor.delay: {monitor_delay}s
nexmark.metric.monitor.interval: 1s

# Workload suite - generated from benchmark.yaml
nexmark.workload.suite.benchmark.events.num: {events}
nexmark.workload.suite.benchmark.tps: {tps}
nexmark.workload.suite.benchmark.queries: "{queries}"
nexmark.workload.suite.benchmark.warmup.duration: {warmup_duration}s
nexmark.workload.suite.benchmark.warmup.events.num: {warmup_events}
nexmark.workload.suite.benchmark.warmup.tps: {tps}
"""
        
        with open(self.nexmark_yaml, 'w') as f:
            f.write(content)
        
        print(f"[NexMark] Generated nexmark.yaml with {events:,} events, {tps:,} tps")
    
    def _configure_state_backend(self, backend: str):
        """Configure Flink state backend in config.yaml"""
        if not self.flink_config.exists():
            raise FileNotFoundError(f"Flink config not found: {self.flink_config}")
        
        with open(self.flink_config, 'r') as f:
            config_content = f.read()
        
        # Remove existing state backend config
        config_content = re.sub(r'state\.backend:.*\n', '', config_content)
        config_content = re.sub(r'state\.backend\.type:.*\n', '', config_content)
        config_content = re.sub(r'state\.backend\.forl0\..*\n', '', config_content)
        
        # Get ForL0 config from benchmark.yaml
        backends_config = self.config.get('backends', [])
        forl0_config = next((b.get('config', {}) for b in backends_config if b.get('name') == 'forl0'), {})
        
        # Add new config
        if backend == "hashmap":
            new_config = "\n# State Backend (configured by NexMark runner)\nstate.backend.type: hashmap\n"
        elif backend == "forl0":
            l0_size = forl0_config.get('l0_cache_size', 14)
            l0_policy = forl0_config.get('l0_cache_replacement_policy', 'CLOCK')
            l0_enabled = forl0_config.get('l0_cache_enabled', True)
            l0_memory_max = forl0_config.get('l0_memory_max_size', '0')
            main_table_size = forl0_config.get('main_table_initial_size', 16)
            main_table_load_factor = forl0_config.get('main_table_load_factor_threshold', 1.5)
            arena_initial_size = forl0_config.get('arena_initial_size', '0')
            new_config = f"""
# State Backend (configured by NexMark runner)
state.backend: org.apache.flink.runtime.state.heap.ForL0StateBackendFactory
state.backend.forl0.l0-cache.enabled: {str(l0_enabled).lower()}
state.backend.forl0.l0-cache.size: {l0_size}
state.backend.forl0.l0-cache.replacement-policy: {l0_policy}
state.backend.forl0.l0-memory.max-size: {l0_memory_max}
state.backend.forl0.main-table.initial-size: {main_table_size}
state.backend.forl0.main-table.load-factor-threshold: {main_table_load_factor}
state.backend.forl0.arena.initial-size: {arena_initial_size}
"""
            # Copy ForL0 JAR to Flink lib if not present
            flink_lib_jar = self.flink_home / "lib" / self.forl0_jar.name
            if not flink_lib_jar.exists():
                shutil.copy(self.forl0_jar, flink_lib_jar)
                print(f"[NexMark] Copied ForL0 JAR to Flink lib")
        else:
            raise ValueError(f"Unknown backend: {backend}")
        
        config_content += new_config
        
        with open(self.flink_config, 'w') as f:
            f.write(config_content)
        
        print(f"[NexMark] Configured state backend: {backend}")
    
    def _restart_flink(self):
        """Restart Flink cluster to apply new config"""
        print("[NexMark] Restarting Flink cluster...")
        
        # Stop cluster
        stop_script = self.flink_home / "bin" / "stop-cluster.sh"
        if stop_script.exists():
            subprocess.run([str(stop_script)], capture_output=True)
            time.sleep(3)
        
        # Start cluster
        start_script = self.flink_home / "bin" / "start-cluster.sh"
        if start_script.exists():
            subprocess.run([str(start_script)], capture_output=True)
            time.sleep(5)
        
        print("[NexMark] Flink cluster restarted")
    
    def _collect_l0_metrics_for_query(self, query: str, start_time: datetime):
        """
        [BENCHMARK_TEST] Collect L0 metrics for a specific NexMark query.
        
        Args:
            query: Query name (e.g., "q5", "q8")
            start_time: Query start timestamp - only collect metrics after this time
        """
        if not self.current_backend:
            return
            
        end_time = datetime.now()
        
        # Parse metrics within time window
        metrics = parse_l0table_metrics_by_time(
            str(self.flink_home), 
            start_time, 
            end_time
        )
        
        if not metrics:
            print(f"  [L0 Metrics] No metrics found for {query}")
            return
        
        # Normalize time to be relative to query start
        metrics = normalize_metrics_time(metrics, start_time)
        
        # Create L0 metrics directory
        self.l0_metrics_dir.mkdir(parents=True, exist_ok=True)
        
        # Save to file with consistent naming
        filepath = save_l0table_metrics(
            metrics, 
            self.current_backend, 
            query, 
            self.l0_metrics_dir
        )
        
        # Print summary
        summary = get_l0_metrics_summary(metrics)
        print(f"  [L0 Metrics] Collected {len(metrics)} samples for {query}")
        if summary:
            print(f"  [L0 Metrics] Overall hit rate: {summary.get('overall_hit_rate', 0):.1f}%")
        print(f"  [L0 Metrics] Saved to: {filepath}")
    
    def _start_cpu_monitor(self):
        """Start NexMark CPU metric monitor (CpuMetricSender)"""
        import platform
        if platform.system() == "Darwin":
            print("[NexMark] Warning: CPU monitoring not available on macOS (requires /proc filesystem)")
            print("[NexMark] Cores metric will show as 0. This works correctly on Linux.")
            return
        
        setup_script = self.nexmark_home / "bin" / "setup_cluster.sh"
        if setup_script.exists():
            print("[NexMark] Starting CPU monitor...")
            env = os.environ.copy()
            env['FLINK_HOME'] = str(self.flink_home)
            subprocess.run([str(setup_script)], capture_output=True, env=env, cwd=str(self.nexmark_home))
            time.sleep(2)
            print("[NexMark] CPU monitor started")
        else:
            print("[NexMark] Warning: setup_cluster.sh not found, CPU metrics won't be collected")
    
    def _stop_cpu_monitor(self):
        """Stop NexMark CPU metric monitor"""
        import platform
        if platform.system() == "Darwin":
            return  # CPU monitor not started on macOS
        
        shutdown_script = self.nexmark_home / "bin" / "shutdown_cluster.sh"
        if shutdown_script.exists():
            print("[NexMark] Stopping CPU monitor...")
            env = os.environ.copy()
            env['FLINK_HOME'] = str(self.flink_home)
            subprocess.run([str(shutdown_script)], capture_output=True, env=env, cwd=str(self.nexmark_home))
            print("[NexMark] CPU monitor stopped")
    
    def _run_nexmark_benchmark(self, queries: str) -> dict:
        """Run NexMark benchmark and return results.
        
        Runs each query individually so that one failed query doesn't block others.
        """
        query_list = [q.strip() for q in queries.split(',')]
        print(f"\n[NexMark] Running {len(query_list)} queries individually: {query_list}")
        
        # Set environment variable for NexMark config
        env = os.environ.copy()
        env['NEXMARK_CONF_DIR'] = str(self.nexmark_conf_dir)
        
        # Build classpath
        nexmark_lib = self.nexmark_home / "lib"
        flink_lib = self.flink_home / "lib"
        classpath = f"{nexmark_lib}/*:{flink_lib}/*"
        
        all_metrics = {
            'query_results': {},
            'total_events': 0,
            'elapsed_seconds': 0,
            'queries': queries,
            'return_code': 0,
            'failed_queries': []
        }
        
        self.results_dir.mkdir(parents=True, exist_ok=True)
        total_start = time.time()
        
        for query in query_list:
            print(f"\n[NexMark] === Running query: {query} ===")
            
            cmd = [
                "java",
                "-cp", classpath,
                "com.github.nexmark.flink.Benchmark",
                "--location", str(self.nexmark_home),
                "--queries", query
            ]
            
            # [BENCHMARK_TEST] Record query start time for L0 metrics filtering
            query_start_time = datetime.now()
            
            # [BENCHMARK_TEST] Start per-query memory collection
            if self.hw_collector and self.current_backend:
                tm_pid = self._find_taskmanager_pid()
                if tm_pid:
                    self.hw_collector.start_memory_collection(
                        pid=tm_pid,
                        query=query,
                        backend=self.current_backend,
                        interval=1.0
                    )
            
            try:
                start_time = time.time()
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    cwd=str(self.nexmark_home),
                    env=env
                )
                elapsed = time.time() - start_time
                
                # [BENCHMARK_TEST] Stop per-query memory collection
                if self.hw_collector:
                    self.hw_collector.stop_memory_collection()
                
                # [BENCHMARK_TEST] Collect L0 metrics for this query if using forl0 backend
                if self.current_backend == 'forl0':
                    self._collect_l0_metrics_for_query(query, query_start_time)
                
                # Save raw output for this query
                output_file = self.results_dir / f"nexmark_output_{query}.txt"
                with open(output_file, 'w') as f:
                    f.write(f"=== STDOUT ===\n{result.stdout}\n\n=== STDERR ===\n{result.stderr}")
                
                # Parse results
                full_output = result.stdout + "\n" + result.stderr
                query_metrics = self._parse_nexmark_output(full_output)
                
                if query in query_metrics.get('query_results', {}):
                    all_metrics['query_results'][query] = query_metrics['query_results'][query]
                    print(f"[NexMark] {query} completed in {elapsed:.1f}s: {query_metrics['query_results'][query].get('throughput', 0):,.0f} events/sec")
                else:
                    print(f"[NexMark] {query} completed but no results parsed (return code: {result.returncode})")
                    all_metrics['failed_queries'].append(query)
                    if result.returncode != 0:
                        all_metrics['return_code'] = result.returncode
                        # Print error info but continue with next query
                        stderr_preview = result.stderr[-500:] if result.stderr else ""
                        print(f"[NexMark] {query} stderr: {stderr_preview}")
                        
            except Exception as e:
                print(f"[NexMark] {query} ERROR: {e} - skipping")
                all_metrics['failed_queries'].append(query)
        
        all_metrics['elapsed_seconds'] = time.time() - total_start
        
        # Save combined output
        output_file = self.results_dir / f"nexmark_output_{queries.replace(',', '_')}.txt"
        with open(output_file, 'w') as f:
            f.write(f"Combined results for queries: {queries}\n")
            f.write(f"Successful: {list(all_metrics['query_results'].keys())}\n")
            f.write(f"Failed: {all_metrics['failed_queries']}\n")
        
        print(f"\n[NexMark] All queries completed. Success: {len(all_metrics['query_results'])}, Failed: {len(all_metrics['failed_queries'])}")
        
        return all_metrics
    
    def _cancel_running_jobs(self):
        """Cancel any running Flink jobs to clean up after timeout"""
        try:
            import requests
            resp = requests.get("http://localhost:8081/jobs", timeout=5)
            if resp.status_code == 200:
                jobs = resp.json().get('jobs', [])
                for job in jobs:
                    if job.get('status') == 'RUNNING':
                        job_id = job.get('id')
                        print(f"[NexMark] Cancelling job {job_id}...")
                        requests.patch(f"http://localhost:8081/jobs/{job_id}?mode=cancel", timeout=10)
        except Exception as e:
            print(f"[NexMark] Failed to cancel jobs: {e}")
    
    def _parse_nexmark_output(self, output: str) -> dict:
        """Parse NexMark benchmark output for metrics"""
        metrics = {
            'query_results': {},
            'total_events': 0,
        }
        
        # NexMark outputs results in a table format (events.num mode):
        # | Query| Events Num      | Cores  | Time(s)  | Cores * Time(s) | Throughput   | Throughput/Cores|
        # |q4    |20,000,000       |0       |6.329     |0.000            |3.16 M/s      |0/s              |
        
        for line in output.split('\n'):
            # Clean up any invalid UTF-8 characters that NexMark might output on macOS
            line = ''.join(c if ord(c) < 128 or c.isalnum() or c in '|., /-' else ' ' for c in line)
            
            # Look for query results - match lines starting with | q
            if '|' in line:
                parts = [p.strip() for p in line.split('|')]
                parts = [p for p in parts if p]  # Remove empty strings
                if len(parts) >= 5:  # Minimum: query, events, time, throughput, throughput/core
                    query = parts[0]
                    if query.startswith('q') and query[1:].isdigit():
                        try:
                            # Find throughput column by looking for M/s or K/s pattern
                            throughput = 0.0
                            throughput_per_core = 0.0
                            time_seconds = 0.0
                            events_num = 0
                            
                            for i, part in enumerate(parts):
                                # Parse events num (looks like "20,000,000")
                                if ',' in part and part.replace(',', '').isdigit():
                                    events_num = int(part.replace(',', ''))
                                # Parse throughput (looks like "3.14 M/s") - check this BEFORE time!
                                elif 'M/s' in part or 'K/s' in part:
                                    throughput = self._parse_throughput(part)
                                # Parse time (looks like "6.363") - must not contain M/s or K/s
                                elif '.' in part and len(part) < 10 and '/' not in part:
                                    try:
                                        val = float(part)
                                        if 0 < val < 1000:  # Reasonable time range
                                            time_seconds = val
                                    except ValueError:
                                        pass
                                elif part == '0/s' and throughput > 0:
                                    throughput_per_core = 0.0  # Already set throughput, this is per-core
                            
                            if throughput > 0:
                                metrics['query_results'][query] = {
                                    'events_num': events_num,
                                    'time_seconds': time_seconds,
                                    'throughput': throughput,
                                    'throughput_per_core': throughput_per_core,
                                    # Keep old names for compatibility
                                    'events_per_sec': throughput,
                                    'cores': 0,  # Not available on macOS
                                    'events_per_sec_per_core': throughput_per_core
                                }
                                print(f"[NexMark] Parsed {query}: {throughput:,.0f} events/sec, {time_seconds:.2f}s")
                        except (ValueError, IndexError) as e:
                            pass
        
        return metrics
    
    def _parse_throughput(self, throughput_str: str) -> float:
        """Parse throughput string like '3.16 M/s' or '902.98 K/s' to float"""
        throughput_str = throughput_str.strip()
        if not throughput_str or throughput_str == '0/s':
            return 0.0
        
        # Remove '/s' suffix
        throughput_str = throughput_str.replace('/s', '').strip()
        
        # Handle scientific notation like "9.22 E"
        if ' E' in throughput_str:
            return 0.0  # Invalid, NexMark outputs this when cores=0
        
        # Parse multiplier
        multiplier = 1.0
        if throughput_str.endswith('M'):
            multiplier = 1_000_000
            throughput_str = throughput_str[:-1].strip()
        elif throughput_str.endswith('K'):
            multiplier = 1_000
            throughput_str = throughput_str[:-1].strip()
        
        try:
            return float(throughput_str) * multiplier
        except ValueError:
            return 0.0
    
    def _find_taskmanager_pid(self) -> Optional[int]:
        """Find Flink TaskManager process ID"""
        try:
            result = subprocess.run(
                ["pgrep", "-f", "TaskManagerRunner"],
                capture_output=True,
                text=True
            )
            if result.returncode == 0 and result.stdout.strip():
                pid = int(result.stdout.strip().split('\n')[0])
                return pid
        except Exception:
            pass
        return None
    
    def run(self, backends: list, queries: Optional[str] = None, 
            profile: bool = False, restart_cluster: bool = True) -> dict:
        """Run NexMark benchmark for specified backends
        
        Args:
            backends: List of backends to test ('hashmap', 'forl0')
            queries: Comma-separated list of queries (e.g., 'q5,q8')
            profile: Enable profiling (flame graphs + hardware metrics)
            restart_cluster: Restart Flink cluster between backends
        """
        
        # Checks
        self._check_nexmark()
        self._check_flink()
        
        # Get queries from config if not specified
        if queries is None:
            queries = self.nexmark_config.get('queries', 'q5')
        
        # Ensure queries is a string
        queries_str: str = str(queries) if queries else 'q5'
        
        # Setup profiler and hardware metrics collector if requested
        if profile:
            profiler_home = os.environ.get('ASYNC_PROFILER_HOME')
            self.profiler = AsyncProfiler(profiler_home)
            
            # Also enable hardware metrics collection when profiling
            self.hw_collector = HardwareMetricsCollector(str(self.hw_metrics_dir))
            print(f"[NexMark] Profiling enabled (perf available: {self.hw_collector.is_perf_available()})")
        
        # Backup configs
        self._backup_configs()
        
        # Generate nexmark.yaml with our config
        self._generate_nexmark_yaml(queries_str)
        
        results = {}
        
        try:
            for backend in backends:
                print(f"\n{'='*60}")
                print(f"Running NexMark ({queries_str}) with backend: {backend}")
                print(f"{'='*60}")
                
                # [BENCHMARK_TEST] Track current backend for L0 metrics collection
                self.current_backend = backend
                
                # Configure backend
                self._configure_state_backend(backend)
                
                # Restart Flink if requested
                if restart_cluster:
                    self._restart_flink()
                
                # Start CPU monitor for metrics collection
                self._start_cpu_monitor()
                
                # Start profiler if enabled
                tm_pid = None
                if self.profiler and self.profiler.is_available():
                    tm_pid = self._find_taskmanager_pid()
                    if tm_pid:
                        self.results_dir.mkdir(parents=True, exist_ok=True)
                        self.profiler.start(
                            pid=tm_pid,
                            events=['cpu'] if not self.profiler.is_macos else ['itimer'],
                            output_dir=str(self.results_dir),
                            backend=backend
                        )
                
                # Note: Hardware metrics (memory) collection is now done per-query in _run_nexmark_benchmark
                
                # Run benchmark
                metrics = self._run_nexmark_benchmark(queries_str)
                results[backend] = metrics
                
                # Stop profiler
                if self.profiler and tm_pid:
                    try:
                        self.profiler.stop(tm_pid)
                    except Exception as e:
                        print(f"[NexMark] Profiler stop error: {e}")
                
                # Stop CPU monitor
                self._stop_cpu_monitor()
                
                # Print results
                print(f"\n[NexMark] Results for {backend}:")
                for q, m in metrics.get('query_results', {}).items():
                    print(f"  {q}: {m['events_per_sec']:,.0f} events/sec ({m['events_per_sec_per_core']:,.0f}/core)")
                if metrics.get('failed_queries'):
                    print(f"  Failed queries: {metrics['failed_queries']}")
        
        finally:
            # Restore configs
            self._restore_configs()
            
            # [BENCHMARK_TEST] Save hardware metrics
            if self.hw_collector:
                self.hw_collector.save_results("nexmark_hw")
        
        # Save results
        self._save_results(results)
        
        # Print comparison
        self._print_comparison(results)
        
        return results
    
    def _save_results(self, results: dict):
        """Save benchmark results to JSON"""
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
        
        print(f"\n[NexMark] Results saved to: {output_file}")
    
    def _print_comparison(self, results: dict):
        """Print comparison between backends"""
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
                base_perf = base_queries.get(query, {}).get('events_per_sec_per_core', 0)
                comp_perf = comp_queries.get(query, {}).get('events_per_sec_per_core', 0)
                
                if base_perf > 0:
                    diff_pct = ((comp_perf - base_perf) / base_perf) * 100
                    symbol = "+" if diff_pct > 0 else ""
                    print(f"  {query}: {comp_perf:,.0f} vs {base_perf:,.0f} ({symbol}{diff_pct:.1f}%)")


def main():
    parser = argparse.ArgumentParser(description="Run NexMark benchmark")
    parser.add_argument("--backend", "-b", 
                       choices=["hashmap", "forl0", "all"],
                       default="all",
                       help="State backend to test")
    parser.add_argument("--queries", "-q",
                       default=None,
                       help="Queries to run (comma-separated, e.g., q5,q8)")
    parser.add_argument("--profile", "-p",
                       action="store_true",
                       help="Enable profiling (flame graphs + hardware metrics)")
    parser.add_argument("--no-restart",
                       action="store_true",
                       help="Don't restart Flink cluster between backends")
    
    args = parser.parse_args()
    
    # Load config
    config = load_config()
    
    # Determine backends
    if args.backend == "all":
        backends = ["hashmap", "forl0"]
    else:
        backends = [args.backend]
    
    # Run benchmark
    runner = NexMarkRunner(config)
    results = runner.run(
        backends=backends,
        queries=args.queries,
        profile=args.profile,
        restart_cluster=not args.no_restart
    )
    
    # Return non-zero if any benchmark failed
    for backend, metrics in results.items():
        if metrics.get('return_code', 0) != 0:
            sys.exit(1)
    
    sys.exit(0)


if __name__ == "__main__":
    main()
