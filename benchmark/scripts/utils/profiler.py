#!/usr/bin/env python3
"""
[BENCHMARK_TEST] Async Profiler integration for flame graphs and CPU cache statistics.

Platform support:
- macOS: Flame graphs (itimer, wall, alloc)
- Linux: Flame graphs (cpu, wall, alloc) + CPU cache statistics (cache-misses, L1-dcache-load-misses)

Usage:
    profiler = AsyncProfiler(async_profiler_home='/path/to/async-profiler')
    profiler.start(pid=1234, events=['cpu', 'alloc'], output_dir='./profiles', backend='forl0')
    # ... run benchmark ...
    profiler.stop()
    # Profiler will generate HTML flame graphs
"""

import os
import platform
import shutil
import signal
import subprocess
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple


class AsyncProfiler:
    """Wrapper for async-profiler for Java profiling."""
    
    # Events supported on each platform
    MACOS_EVENTS = ['itimer', 'wall', 'alloc']
    LINUX_EVENTS = ['cpu', 'wall', 'alloc']
    LINUX_CACHE_EVENTS = ['cache-misses', 'L1-dcache-load-misses', 'LLC-load-misses']
    
    def __init__(self, async_profiler_home: Optional[str] = None):
        """
        Initialize AsyncProfiler.
        
        Args:
            async_profiler_home: Path to async-profiler installation.
                                 Defaults to ASYNC_PROFILER_HOME env var.
        """
        self.home = async_profiler_home or os.environ.get('ASYNC_PROFILER_HOME')
        self.is_macos = platform.system() == 'Darwin'
        self.is_linux = platform.system() == 'Linux'
        self.asprof_path = None
        self.active_profiles: Dict[str, Path] = {}  # event -> output_file (for async mode)
        self.output_files: Dict[str, str] = {}
        self.profiled_pid: Optional[int] = None  # PID being profiled (for stop)
        
        if self.home:
            # Check for asprof binary
            for candidate in ['asprof', 'bin/asprof', 'profiler.sh']:
                path = Path(self.home) / candidate
                if path.exists() and path.is_file():
                    self.asprof_path = str(path)
                    break
    
    def is_available(self) -> bool:
        """Check if async-profiler is available."""
        if not self.asprof_path:
            return False
        try:
            result = subprocess.run(
                [self.asprof_path, '--version'],
                capture_output=True, text=True, timeout=5
            )
            return result.returncode == 0
        except Exception:
            return False
    
    def get_version(self) -> Optional[str]:
        """Get async-profiler version."""
        if not self.asprof_path:
            return None
        try:
            result = subprocess.run(
                [self.asprof_path, '--version'],
                capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                return result.stdout.strip() or result.stderr.strip()
        except Exception:
            pass
        return None
    
    def get_supported_events(self, include_cache: bool = True) -> List[str]:
        """
        Get list of supported profiling events for current platform.
        
        Args:
            include_cache: Whether to include CPU cache events (Linux only)
        """
        if self.is_macos:
            return self.MACOS_EVENTS.copy()
        elif self.is_linux:
            events = self.LINUX_EVENTS.copy()
            if include_cache:
                events.extend(self.LINUX_CACHE_EVENTS)
            return events
        return []
    
    def _normalize_event(self, event: str) -> str:
        """Normalize event name for current platform."""
        # On macOS, 'cpu' should use 'itimer' as fallback
        if self.is_macos and event == 'cpu':
            return 'itimer'
        return event
    
    def _is_cache_event(self, event: str) -> bool:
        """Check if event is a CPU cache event."""
        return event in self.LINUX_CACHE_EVENTS
    
    def start(
        self,
        pid: int,
        events: Optional[List[str]] = None,
        output_dir: str = './profiles',
        backend: str = 'unknown',
        duration: Optional[int] = None,
        interval: str = '10ms'
    ) -> Dict[str, str]:
        """
        Start profiling a Java process.
        
        Note: async-profiler only supports ONE profiling session per JVM at a time.
        If multiple events are specified, only the first valid one will be used.
        Use profile_sync() with duration if you need multiple events sequentially.
        
        Args:
            pid: Process ID to profile
            events: List of events to profile (only first valid event will be used)
            output_dir: Directory to save flame graphs
            backend: Backend name (used in output filenames)
            duration: Duration in seconds (None for indefinite until stop() is called)
            interval: Sampling interval (default: 10ms)
        
        Returns:
            Dict mapping event name to output file path
        """
        if not self.is_available():
            print("WARNING: async-profiler not available")
            return {}
        
        if not self.asprof_path:
            print("WARNING: asprof path not set")
            return {}
        
        # Default events based on platform - only use one primary event for async mode
        if events is None:
            events = ['cpu'] if self.is_linux else ['itimer']
        
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        timestamp = time.strftime('%Y%m%d_%H%M%S')
        
        # Only use the first valid event (async-profiler limitation: one session per JVM)
        selected_event = None
        actual_event = None
        for event in events:
            # Normalize event for platform
            actual_event = self._normalize_event(event)
            
            # Skip unsupported events
            if self._is_cache_event(actual_event) and self.is_macos:
                print(f"  Skipping {actual_event}: not supported on macOS")
                continue
            
            selected_event = event
            break
        
        if not selected_event or not actual_event:
            print("  No valid profiling event available")
            return {}
        
        # Build output filename
        output_file = output_path / f"flamegraph_{actual_event}_{backend}_{timestamp}.html"
        
        # Build command
        cmd: List[str] = [self.asprof_path]
        
        if duration:
            # Run for specific duration
            cmd.extend(['-d', str(duration)])
        else:
            # Start profiling (will be stopped later)
            cmd.append('start')
        
        cmd.extend([
            '-e', actual_event,
            '-i', interval,
            '-f', str(output_file),
            str(pid)
        ])
        
        print(f"  Starting profiler: {actual_event} -> {output_file.name}")
        
        try:
            if duration:
                # Blocking call - run for duration
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=duration + 30)
                if result.returncode == 0:
                    self.output_files[selected_event] = str(output_file)
                else:
                    print(f"    WARNING: Profiler returned error: {result.stderr}")
            else:
                # Non-blocking - use asprof start to attach to process
                # asprof start will return immediately, we need to call stop later
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
                if result.returncode == 0:
                    self.output_files[selected_event] = str(output_file)
                    # Store output file path for later stop
                    self.active_profiles[selected_event] = output_file
                    self.profiled_pid = pid
                else:
                    print(f"    WARNING: Profiler start failed: {result.stderr}")
        except subprocess.TimeoutExpired:
            print(f"    WARNING: Profiler timed out for {actual_event}")
        except Exception as e:
            print(f"    ERROR starting profiler: {e}")
        
        return self.output_files.copy()
    
    def stop(self, pid: int) -> Dict[str, str]:
        """
        Stop profiling and collect results.
        
        Args:
            pid: Process ID that was being profiled
            
        Returns:
            Dict mapping event name to output file path
        """
        if not self.is_available():
            return {}
        
        if not self.active_profiles:
            print("  No active profiling sessions to stop")
            return self.output_files.copy()
        
        # Use the stored PID if available (for async mode)
        target_pid = self.profiled_pid or pid
        
        print(f"  Stopping profiler for PID {target_pid}...")
        
        # Stop all active profiling sessions
        for event, output_file in list(self.active_profiles.items()):
            try:
                # Send stop signal via asprof with output file
                stop_cmd = [
                    self.asprof_path, 
                    'stop',
                    '-f', str(output_file),
                    str(target_pid)
                ]
                print(f"    Stopping {event} profiler -> {output_file.name}")
                result = subprocess.run(stop_cmd, capture_output=True, text=True, timeout=60)
                if result.returncode != 0:
                    print(f"    WARNING: Stop command returned error: {result.stderr}")
                elif output_file.exists():
                    print(f"    SUCCESS: Flame graph saved to {output_file.name}")
                else:
                    print(f"    WARNING: Output file not created: {output_file}")
            except subprocess.TimeoutExpired:
                print(f"    WARNING: Stop command timed out for {event}")
            except Exception as e:
                print(f"    WARNING: Error stopping profiler for {event}: {e}")
        
        self.active_profiles.clear()
        self.profiled_pid = None
        
        # Return collected output files
        return self.output_files.copy()
    
    def profile_sync(
        self,
        pid: int,
        duration: int,
        events: Optional[List[str]] = None,
        output_dir: str = './profiles',
        backend: str = 'unknown',
        interval: str = '10ms'
    ) -> Dict[str, str]:
        """
        Profile synchronously for a specific duration.
        
        This is a convenience method that starts profiling, waits, and stops.
        
        Args:
            pid: Process ID to profile
            duration: Duration in seconds
            events: List of events to profile
            output_dir: Directory to save flame graphs
            backend: Backend name
            interval: Sampling interval
            
        Returns:
            Dict mapping event name to output file path
        """
        return self.start(
            pid=pid,
            events=events,
            output_dir=output_dir,
            backend=backend,
            duration=duration,
            interval=interval
        )
    
    def collect_cache_stats(
        self,
        pid: int,
        duration: int,
        output_dir: str = './profiles',
        backend: str = 'unknown'
    ) -> Optional[Dict]:
        """
        Collect CPU cache statistics (Linux only).
        
        This method uses async-profiler to collect cache-miss events and
        generates a flame graph showing where cache misses occur.
        
        Args:
            pid: Process ID to profile
            duration: Duration in seconds
            output_dir: Directory to save results
            backend: Backend name
            
        Returns:
            Dict with cache statistics and file paths, or None if not supported
        """
        if not self.is_linux:
            print("  WARNING: CPU cache statistics not available on this platform")
            return None
        
        if not self.is_available():
            print("  WARNING: async-profiler not available")
            return None
        
        if not self.asprof_path:
            return None
        
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        timestamp = time.strftime('%Y%m%d_%H%M%S')
        results: Dict[str, str] = {}
        
        # Collect cache-misses
        cache_events = ['cache-misses', 'L1-dcache-load-misses']
        
        for event in cache_events:
            output_file = output_path / f"cache_{event}_{backend}_{timestamp}.html"
            
            cmd: List[str] = [
                self.asprof_path,
                '-d', str(duration),
                '-e', event,
                '-f', str(output_file),
                str(pid)
            ]
            
            print(f"  Collecting {event} for {duration}s...")
            
            try:
                result = subprocess.run(
                    cmd, capture_output=True, text=True,
                    timeout=duration + 60
                )
                
                if result.returncode == 0:
                    results[event] = str(output_file)
                    print(f"    Saved: {output_file.name}")
                else:
                    print(f"    WARNING: {event} collection failed: {result.stderr}")
                    
            except subprocess.TimeoutExpired:
                print(f"    WARNING: {event} collection timed out")
            except Exception as e:
                print(f"    ERROR: {e}")
        
        return results if results else None


def find_taskmanager_pids(flink_home: Optional[str] = None) -> List[int]:
    """
    Find PIDs of Flink TaskManager processes.
    
    Args:
        flink_home: Flink home directory (optional, used for logging)
    
    Returns:
        List of TaskManager PIDs
    """
    pids = []
    try:
        # Use jps to find TaskManagerRunner
        result = subprocess.run(
            ['jps', '-l'],
            capture_output=True, text=True, timeout=10
        )
        
        for line in result.stdout.splitlines():
            if 'TaskManagerRunner' in line or 'TaskExecutor' in line:
                parts = line.strip().split()
                if parts:
                    try:
                        pids.append(int(parts[0]))
                    except ValueError:
                        pass
    except Exception as e:
        print(f"WARNING: Could not find TaskManager PIDs: {e}")
    
    return pids


def get_profiler_summary() -> Dict:
    """
    Get profiler capability summary for current platform.
    
    Returns:
        Dict with platform info and supported features
    """
    profiler = AsyncProfiler()
    is_available = profiler.is_available()
    
    return {
        'platform': platform.system(),
        'async_profiler_available': is_available,
        'async_profiler_version': profiler.get_version() if is_available else None,
        'async_profiler_home': profiler.home,
        'supported_events': profiler.get_supported_events() if is_available else [],
        'cpu_event': 'itimer' if profiler.is_macos else 'cpu',
        'cache_events_supported': profiler.is_linux,
        'notes': [
            'macOS: CPU cache statistics not available (no perf_events)',
            'macOS: Using itimer for CPU sampling (lower precision)',
            'Linux: Full perf_events support including cache-misses',
        ] if profiler.is_macos else [
            'Linux: Full perf_events support',
            'Linux: CPU cache statistics available (cache-misses, L1-dcache-load-misses)',
        ]
    }


if __name__ == '__main__':
    # Test mode - print profiler info
    import json
    summary = get_profiler_summary()
    print("=== Async Profiler Summary ===")
    print(json.dumps(summary, indent=2))
    
    # Try to find TaskManager
    pids = find_taskmanager_pids()
    if pids:
        print(f"\nTaskManager PIDs found: {pids}")
    else:
        print("\nNo TaskManager processes found")
