#!/usr/bin/env python3
"""
[BENCHMARK_TEST] Hardware metrics collection for CPU cache and memory statistics.

This module provides:
1. CPU cache miss statistics collection using Linux perf or Async Profiler
2. Memory usage (RSS) time series collection
3. Helper functions for data processing and visualization

Platform support:
- macOS: Memory collection only (cache metrics not available)
- Linux: Full support (perf stat + memory collection)

Cache Miss Collection Strategy:
- Use `perf stat -e cache-misses,cache-references` for accurate counters
- Parse perf output to extract counts
- Async Profiler flame graphs can be used to identify StateMap-related cache misses

Memory Collection Strategy:
- Periodically sample process RSS using ps or /proc/<pid>/status
- Store time series data for plotting
"""

import json
import os
import platform
import re
import subprocess
import threading
import time
from dataclasses import dataclass, asdict, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple


@dataclass
class CacheMissStats:
    """Statistics for CPU cache misses during a benchmark run."""
    query: str
    backend: str
    total_cache_misses: int = 0
    total_cache_references: int = 0
    statemap_cache_misses: int = 0  # Estimated from flame graph analysis
    cache_miss_rate: float = 0.0
    duration_seconds: float = 0.0
    timestamp: str = ""
    
    def to_dict(self) -> dict:
        return asdict(self)


@dataclass 
class MemorySample:
    """A single memory usage sample."""
    timestamp: float  # Seconds since collection start
    rss_mb: float     # Resident Set Size in MB
    heap_mb: float = 0.0   # JVM heap (if available)
    non_heap_mb: float = 0.0  # JVM non-heap (if available)


@dataclass
class MemoryTimeSeries:
    """Time series of memory usage for a benchmark run."""
    query: str
    backend: str
    samples: List[MemorySample] = field(default_factory=list)
    start_time: str = ""
    
    def to_dict(self) -> dict:
        return {
            'query': self.query,
            'backend': self.backend,
            'start_time': self.start_time,
            'samples': [asdict(s) for s in self.samples]
        }


class HardwareMetricsCollector:
    """Collect hardware metrics including CPU cache and memory usage."""
    
    def __init__(self, output_dir: str = "./results/hardware"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.is_linux = platform.system() == 'Linux'
        self.is_macos = platform.system() == 'Darwin'
        
        # Memory collection state
        self._memory_thread: Optional[threading.Thread] = None
        self._memory_stop_event = threading.Event()
        self._memory_samples: List[MemorySample] = []
        self._memory_query: str = ""
        self._memory_backend: str = ""
        self._memory_start_time: float = 0.0
        
        # Cache miss results storage
        self.cache_stats: Dict[str, CacheMissStats] = {}  # key: "{query}_{backend}"
        
        # Memory results storage
        self.memory_series: Dict[str, MemoryTimeSeries] = {}  # key: "{query}_{backend}"
    
    def is_perf_available(self) -> bool:
        """Check if perf is available for cache statistics."""
        if not self.is_linux:
            return False
        try:
            result = subprocess.run(
                ['perf', 'stat', '--version'],
                capture_output=True, text=True, timeout=5
            )
            return result.returncode == 0
        except Exception:
            return False
    
    def collect_cache_stats(
        self,
        pid: int,
        duration: int,
        query: str,
        backend: str
    ) -> Optional[CacheMissStats]:
        """
        Collect CPU cache miss statistics using perf stat.
        
        Only available on Linux with perf support.
        
        Args:
            pid: Process ID to monitor
            duration: Collection duration in seconds
            query: Query/benchmark name (e.g., 'q5', 'wordcount')
            backend: Backend name ('hashmap' or 'forl0')
        
        Returns:
            CacheMissStats object or None if not supported
        """
        if not self.is_linux:
            print(f"  [HW] Cache statistics not available on {platform.system()}")
            return None
        
        if not self.is_perf_available():
            print("  [HW] perf not available, skipping cache statistics")
            return None
        
        print(f"  [HW] Collecting cache stats for PID {pid}, duration {duration}s...")
        
        # Run perf stat
        cmd = [
            'perf', 'stat',
            '-e', 'cache-misses,cache-references,L1-dcache-load-misses,L1-dcache-loads',
            '-p', str(pid),
            '--', 'sleep', str(duration)
        ]
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=duration + 30
            )
            
            # Parse perf output (output goes to stderr)
            stats = self._parse_perf_output(result.stderr)
            
            cache_misses = stats.get('cache-misses', 0)
            cache_refs = stats.get('cache-references', 0)
            miss_rate = (cache_misses / cache_refs * 100) if cache_refs > 0 else 0.0
            
            cache_stats = CacheMissStats(
                query=query,
                backend=backend,
                total_cache_misses=cache_misses,
                total_cache_references=cache_refs,
                cache_miss_rate=miss_rate,
                duration_seconds=duration,
                timestamp=datetime.now().isoformat()
            )
            
            # Store results
            key = f"{query}_{backend}"
            self.cache_stats[key] = cache_stats
            
            print(f"  [HW] Cache misses: {cache_misses:,}, refs: {cache_refs:,}, rate: {miss_rate:.2f}%")
            
            return cache_stats
            
        except subprocess.TimeoutExpired:
            print(f"  [HW] perf stat timed out")
        except Exception as e:
            print(f"  [HW] Error collecting cache stats: {e}")
        
        return None
    
    def _parse_perf_output(self, output: str) -> Dict[str, int]:
        """Parse perf stat output to extract counter values."""
        stats = {}
        
        # Pattern: "        1,234,567      cache-misses"
        # or:      "    1,234,567      cache-misses:u" (with modifier)
        pattern = r'^\s*([\d,]+)\s+([\w-]+(?::\w+)?)'
        
        for line in output.splitlines():
            match = re.match(pattern, line.strip())
            if match:
                value_str = match.group(1).replace(',', '')
                event = match.group(2).split(':')[0]  # Remove :u modifier
                try:
                    stats[event] = int(value_str)
                except ValueError:
                    pass
        
        return stats
    
    def start_memory_collection(
        self,
        pid: int,
        query: str,
        backend: str,
        interval: float = 1.0
    ):
        """
        Start collecting memory usage in background thread.
        
        Args:
            pid: Process ID to monitor
            query: Query/benchmark name
            backend: Backend name
            interval: Sampling interval in seconds
        """
        if self._memory_thread is not None and self._memory_thread.is_alive():
            self.stop_memory_collection()
        
        self._memory_stop_event.clear()
        self._memory_samples = []
        self._memory_query = query
        self._memory_backend = backend
        self._memory_start_time = time.time()
        
        self._memory_thread = threading.Thread(
            target=self._memory_collection_loop,
            args=(pid, interval),
            daemon=True,
            name=f"MemoryCollector-{query}"
        )
        self._memory_thread.start()
        
        print(f"  [HW] Started memory collection for {query} ({backend})")
    
    def _memory_collection_loop(self, pid: int, interval: float):
        """Background thread for memory collection."""
        while not self._memory_stop_event.is_set():
            try:
                rss_mb = self._get_process_rss(pid)
                if rss_mb is not None:
                    elapsed = time.time() - self._memory_start_time
                    sample = MemorySample(
                        timestamp=elapsed,
                        rss_mb=rss_mb
                    )
                    self._memory_samples.append(sample)
            except Exception as e:
                # Process may have terminated
                break
            
            # Wait for interval or stop signal
            self._memory_stop_event.wait(timeout=interval)
    
    def _get_process_rss(self, pid: int) -> Optional[float]:
        """Get RSS memory of a process in MB."""
        try:
            if self.is_linux:
                # Read from /proc/<pid>/status
                status_path = Path(f'/proc/{pid}/status')
                if status_path.exists():
                    content = status_path.read_text()
                    for line in content.splitlines():
                        if line.startswith('VmRSS:'):
                            # Format: "VmRSS:    12345 kB"
                            parts = line.split()
                            if len(parts) >= 2:
                                kb = int(parts[1])
                                return kb / 1024.0
            else:
                # Use ps command (works on macOS and Linux)
                result = subprocess.run(
                    ['ps', '-o', 'rss=', '-p', str(pid)],
                    capture_output=True, text=True, timeout=5
                )
                if result.returncode == 0 and result.stdout.strip():
                    kb = int(result.stdout.strip())
                    return kb / 1024.0
        except Exception:
            pass
        
        return None
    
    def stop_memory_collection(self) -> Optional[MemoryTimeSeries]:
        """
        Stop memory collection and return results.
        
        Returns:
            MemoryTimeSeries object with all collected samples
        """
        if self._memory_thread is None:
            return None
        
        self._memory_stop_event.set()
        self._memory_thread.join(timeout=5)
        
        if not self._memory_samples:
            print(f"  [HW] No memory samples collected for {self._memory_query}")
            return None
        
        series = MemoryTimeSeries(
            query=self._memory_query,
            backend=self._memory_backend,
            samples=self._memory_samples.copy(),
            start_time=datetime.now().isoformat()
        )
        
        # Store results
        key = f"{self._memory_query}_{self._memory_backend}"
        self.memory_series[key] = series
        
        # Calculate statistics
        rss_values = [s.rss_mb for s in self._memory_samples]
        avg_rss = sum(rss_values) / len(rss_values) if rss_values else 0
        max_rss = max(rss_values) if rss_values else 0
        
        print(f"  [HW] Stopped memory collection: {len(self._memory_samples)} samples, "
              f"avg={avg_rss:.1f}MB, max={max_rss:.1f}MB")
        
        # Reset state
        self._memory_thread = None
        self._memory_samples = []
        
        return series
    
    def save_results(self, filename_prefix: str = "hardware_metrics"):
        """Save all collected metrics to JSON files."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save cache stats
        if self.cache_stats:
            cache_file = self.output_dir / f"{filename_prefix}_cache_{timestamp}.json"
            cache_data = {
                'timestamp': timestamp,
                'platform': platform.system(),
                'stats': {k: v.to_dict() for k, v in self.cache_stats.items()}
            }
            with open(cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
            print(f"  [HW] Saved cache stats to {cache_file}")
        
        # Save memory series
        if self.memory_series:
            memory_file = self.output_dir / f"{filename_prefix}_memory_{timestamp}.json"
            memory_data = {
                'timestamp': timestamp,
                'platform': platform.system(),
                'series': {k: v.to_dict() for k, v in self.memory_series.items()}
            }
            with open(memory_file, 'w') as f:
                json.dump(memory_data, f, indent=2)
            print(f"  [HW] Saved memory series to {memory_file}")
    
    def get_summary(self) -> dict:
        """Get summary of all collected metrics."""
        return {
            'platform': platform.system(),
            'perf_available': self.is_perf_available(),
            'cache_stats_count': len(self.cache_stats),
            'memory_series_count': len(self.memory_series),
            'cache_stats': list(self.cache_stats.keys()),
            'memory_series': list(self.memory_series.keys())
        }


def estimate_statemap_cache_misses(
    flamegraph_file: str,
    total_cache_misses: int
) -> int:
    """
    Estimate StateMap-related cache misses from flame graph data.
    
    This is an approximation based on sampling the flame graph.
    The flame graph shows the proportion of cache misses in each call stack.
    
    Args:
        flamegraph_file: Path to HTML flame graph file
        total_cache_misses: Total cache miss count from perf stat
    
    Returns:
        Estimated cache misses attributable to StateMap operations
    """
    # TODO: Implement flame graph parsing
    # For now, return a placeholder based on heuristics
    # In ForL0 StateBackend, we expect ~30-50% of cache misses to be in StateMap operations
    
    if not Path(flamegraph_file).exists():
        return 0
    
    try:
        content = Path(flamegraph_file).read_text()
        
        # Look for StateMap-related patterns in the flame graph
        # Patterns: ForL0StateMap, L0Table, MainTable, get, put, computeIfAbsent
        statemap_patterns = [
            'ForL0StateMap',
            'L0Table',
            'MainTable',
            'StateMap.get',
            'StateMap.put',
            'computeIfAbsent'
        ]
        
        # Count occurrences (rough estimate)
        total_occurrences = content.count('samples')  # Total samples in flame graph
        statemap_occurrences = 0
        for pattern in statemap_patterns:
            statemap_occurrences += content.count(pattern)
        
        if total_occurrences > 0:
            ratio = min(statemap_occurrences / total_occurrences, 0.8)  # Cap at 80%
            return int(total_cache_misses * ratio)
    except Exception as e:
        print(f"  [HW] Warning: Could not parse flame graph: {e}")
    
    # Default: estimate 30% are StateMap-related
    return int(total_cache_misses * 0.3)


def load_hardware_metrics(results_dir: str = "./results/hardware") -> dict:
    """
    Load all hardware metrics from results directory.
    
    Returns dict with:
    - 'cache': Dict mapping "{query}_{backend}" to CacheMissStats dict
    - 'memory': Dict mapping "{query}_{backend}" to MemoryTimeSeries dict
    """
    results_path = Path(results_dir)
    if not results_path.exists():
        return {'cache': {}, 'memory': {}}
    
    cache_data = {}
    memory_data = {}
    
    # Load cache metrics (find all files matching *_cache_*.json pattern)
    cache_files = sorted(results_path.glob('*_cache_*.json'), reverse=True)
    for cache_file in cache_files:
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
                stats = data.get('stats', {})
                for k, v in stats.items():
                    if k not in cache_data:
                        cache_data[k] = v
        except Exception as e:
            print(f"  Warning: Could not load cache metrics from {cache_file}: {e}")
    
    # Load memory metrics (find all files matching *_memory_*.json pattern)
    memory_files = sorted(results_path.glob('*_memory_*.json'), reverse=True)
    for memory_file in memory_files:
        try:
            with open(memory_file, 'r') as f:
                data = json.load(f)
                series = data.get('series', {})
                for k, v in series.items():
                    if k not in memory_data:
                        memory_data[k] = v
        except Exception as e:
            print(f"  Warning: Could not load memory metrics from {memory_file}: {e}")
    
    return {
        'cache': cache_data,
        'memory': memory_data
    }


if __name__ == '__main__':
    # Test mode
    import json
    
    collector = HardwareMetricsCollector()
    
    print("=== Hardware Metrics Collector Test ===")
    print(f"Platform: {platform.system()}")
    print(f"perf available: {collector.is_perf_available()}")
    
    # Test memory collection on current process
    pid = os.getpid()
    print(f"\nTesting memory collection on PID {pid}...")
    
    collector.start_memory_collection(pid, 'test', 'test', interval=0.5)
    time.sleep(3)
    series = collector.stop_memory_collection()
    
    if series:
        print(f"Collected {len(series.samples)} samples")
        for s in series.samples[:5]:
            print(f"  t={s.timestamp:.1f}s, RSS={s.rss_mb:.1f}MB")
    
    print("\nSummary:")
    print(json.dumps(collector.get_summary(), indent=2))
