#!/usr/bin/env python3
"""
[BENCHMARK_TEST] Intel VTune Profiler integration for microarchitecture analysis.

Supported analysis types:
- uarch-exploration: CPU microarchitecture analysis (pipeline, branch prediction, etc.)
- memory-access: Memory hierarchy analysis (cache misses, memory bandwidth, etc.)

Requirements:
- Intel VTune Profiler installed
- vtune command available in PATH or VTUNE_PROFILER_DIR env var set

Usage:
    profiler = VTuneProfiler()
    profiler.start(pid=1234, analysis_type='uarch-exploration', output_dir='./profiles')
    # ... run benchmark ...
    profiler.stop()
    # Results will be in output_dir/vtune_results_*
"""

import os
import platform
import shutil
import subprocess
import time
from pathlib import Path
from typing import Dict, List, Optional


class VTuneProfiler:
    """Wrapper for Intel VTune Profiler."""
    
    # Supported analysis types
    ANALYSIS_TYPES = {
        'uarch': 'uarch-exploration',  # Microarchitecture exploration
        'memory': 'memory-access',      # Memory access analysis
        'hotspots': 'hotspots'          # Hotspots analysis (with call stacks)
    }
    
    def __init__(self, vtune_dir: Optional[str] = None):
        """
        Initialize VTuneProfiler.
        
        Args:
            vtune_dir: Path to VTune installation directory.
                      Defaults to VTUNE_PROFILER_DIR env var or searches PATH.
        """
        self.vtune_dir = vtune_dir or os.environ.get('VTUNE_PROFILER_DIR')
        self.vtune_path = self._find_vtune_binary()
        self.is_linux = platform.system() == 'Linux'
        self.active_pid: Optional[int] = None
        self.result_dir: Optional[Path] = None
        self.analysis_type: Optional[str] = None
        
    def _find_vtune_binary(self) -> Optional[str]:
        """Find vtune binary in PATH or installation directory."""
        # Try command in PATH first
        vtune_cmd = shutil.which('vtune')
        if vtune_cmd:
            return vtune_cmd
        
        # Try standard Intel installation paths
        if self.vtune_dir:
            candidates = [
                Path(self.vtune_dir) / 'bin64' / 'vtune',
                Path(self.vtune_dir) / 'vtune',
            ]
        else:
            candidates = [
                Path('/opt/intel/oneapi/vtune/latest/bin64/vtune'),
                Path('/opt/intel/vtune_profiler/bin64/vtune'),
                Path(os.path.expanduser('~/intel/oneapi/vtune/latest/bin64/vtune')),
            ]
        
        for path in candidates:
            if path.exists() and path.is_file():
                return str(path)
        
        return None
    
    def is_available(self) -> bool:
        """Check if VTune Profiler is available."""
        if not self.is_linux:
            return False
        
        if not self.vtune_path:
            return False
        
        try:
            result = subprocess.run(
                [self.vtune_path, '--version'],
                capture_output=True, text=True, timeout=30
            )
            return result.returncode == 0
        except Exception:
            return False
    
    def get_version(self) -> Optional[str]:
        """Get VTune Profiler version."""
        if not self.vtune_path:
            return None
        
        try:
            result = subprocess.run(
                [self.vtune_path, '--version'],
                capture_output=True, text=True, timeout=30
            )
            if result.returncode == 0:
                # Parse version from output (e.g., "Intel(R) VTune(TM) Profiler 2023.2.0")
                lines = result.stdout.strip().split('\n')
                for line in lines:
                    if 'VTune' in line:
                        return line.strip()
            return None
        except Exception:
            return None
    
    def start(
        self,
        pid: int,
        analysis_type: str = 'uarch',
        output_dir: Optional[str] = None,
        backend: str = 'unknown',
        query: Optional[str] = None,
        duration: int = 60,
        delay: int = 20
    ) -> Optional[str]:
        """
        Start VTune profiling with delayed attach.
        
        VTune will wait for 'delay' seconds before attaching to allow the application
        to reach steady state.
        
        Args:
            pid: Process ID to profile
            analysis_type: Analysis type ('uarch' or 'memory')
            output_dir: Directory to save results (default: ~/vtune-results)
            backend: Backend name (used in result directory name)
            query: Query name (optional)
            duration: Profiling duration in seconds (default: 60s)
            delay: Seconds to wait before starting profiling (default: 20s)
        
        Returns:
            Path to result directory or None if failed
        """
        if not self.is_available():
            print("WARNING: Intel VTune Profiler not available")
            return None
        
        if analysis_type not in self.ANALYSIS_TYPES:
            print(f"ERROR: Unknown analysis type: {analysis_type}")
            print(f"       Supported types: {list(self.ANALYSIS_TYPES.keys())}")
            return None
        
        analysis_name = self.ANALYSIS_TYPES[analysis_type]
        
        # Default to ~/vtune-results if not specified
        if output_dir is None:
            output_dir = os.path.expanduser('~/vtune-results')
        
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        timestamp = time.strftime('%Y%m%d_%H%M%S')
        query_part = f"_{query}" if query else ""
        result_dir_name = f"vtune_{analysis_type}_{backend}{query_part}_{timestamp}"
        result_dir = output_path / result_dir_name
        
        print(f"  VTune will start in {delay}s: {analysis_name} -> {result_dir.name}")
        
        # Wait for the specified delay to let benchmark reach steady state
        time.sleep(delay)
        
        # Build VTune command
        cmd = [
            self.vtune_path,
            '-collect', analysis_name,
            '-result-dir', str(result_dir),
            '-target-pid', str(pid),
            '-duration', str(duration)
        ]
        
        # Additional options based on analysis type
        if analysis_type == 'uarch':
            # Enable detailed microarchitecture metrics
            cmd.extend(['-knob', 'sampling-interval=1'])
        elif analysis_type == 'memory':
            # Enable detailed memory access tracking
            cmd.extend(['-knob', 'analyze-mem-objects=true'])
        
        print(f"  Starting VTune profiling (PID: {pid}, duration: {duration}s)...")
        
        try:
            # Run VTune in background
            # VTune will run for the specified duration and automatically stop
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=duration + 60  # Allow extra time for startup/shutdown
            )
            
            if result.returncode == 0:
                print(f"  VTune profiling completed: {result_dir}")
                self.result_dir = result_dir
                self.analysis_type = analysis_type
                
                # Generate report
                self._generate_report(result_dir)
                
                return str(result_dir)
            else:
                print(f"  WARNING: VTune returned error code {result.returncode}")
                if result.stderr:
                    print(f"  Error: {result.stderr}")
                return None
                
        except subprocess.TimeoutExpired:
            print(f"  WARNING: VTune profiling timed out")
            return None
        except Exception as e:
            print(f"  ERROR: VTune profiling failed: {e}")
            return None
    
    def _generate_report(self, result_dir: Path):
        """Generate text and HTML reports from VTune results."""
        try:
            # Generate text report
            report_txt = result_dir / 'report.txt'
            cmd_txt = [
                self.vtune_path,
                '-report', 'summary',
                '-result-dir', str(result_dir),
                '-format', 'text',
                '-report-output', str(report_txt)
            ]
            subprocess.run(cmd_txt, capture_output=True, text=True, timeout=30)
            
            # Generate HTML report (can be viewed in browser without GUI)
            report_html = result_dir / 'report.html'
            cmd_html = [
                self.vtune_path,
                '-report', 'summary',
                '-result-dir', str(result_dir),
                '-format', 'html',
                '-report-output', str(report_html)
            ]
            subprocess.run(cmd_html, capture_output=True, text=True, timeout=30)
            
            print(f"  Generated reports: {report_txt.name}, {report_html.name}")
            print(f"  View in browser: file://{report_html.resolve()}")
            
        except Exception as e:
            print(f"  WARNING: Failed to generate report: {e}")
    
    def stop(self):
        """
        Stop VTune profiling.
        
        Note: VTune with -duration flag stops automatically, so this is a no-op.
        Kept for API compatibility.
        """
        pass
    
    def get_results_summary(self) -> Optional[Dict[str, any]]:
        """
        Parse VTune results summary.
        
        Returns:
            Dictionary with key metrics or None if not available
        """
        if not self.result_dir or not self.result_dir.exists():
            return None
        
        report_file = self.result_dir / 'report.txt'
        if not report_file.exists():
            return None
        
        try:
            with open(report_file, 'r') as f:
                content = f.read()
            
            # Basic parsing - extract key metrics based on analysis type
            metrics = {
                'analysis_type': self.analysis_type,
                'result_dir': str(self.result_dir)
            }
            
            # Add more detailed parsing if needed
            if self.analysis_type == 'uarch':
                # Extract microarchitecture metrics
                if 'Retiring' in content:
                    metrics['has_pipeline_metrics'] = True
            elif self.analysis_type == 'memory':
                # Extract memory metrics
                if 'DRAM' in content or 'Cache' in content:
                    metrics['has_memory_metrics'] = True
            
            return metrics
            
        except Exception as e:
            print(f"  WARNING: Failed to parse results: {e}")
            return None


def find_java_pids(pattern: str = 'flink') -> List[int]:
    """
    Find Java process PIDs matching pattern.
    
    Args:
        pattern: Pattern to match in command line (case-insensitive)
    
    Returns:
        List of PIDs
    """
    try:
        result = subprocess.run(
            ['pgrep', '-f', f'java.*{pattern}'],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            return [int(pid) for pid in result.stdout.strip().split('\n') if pid]
        return []
    except Exception:
        return []


def get_profiler_summary(vtune: VTuneProfiler) -> str:
    """
    Get summary string for VTune profiler status.
    
    Args:
        vtune: VTuneProfiler instance
    
    Returns:
        Summary string
    """
    if not vtune or not vtune.is_available():
        return "VTune: Not available"
    
    version = vtune.get_version()
    if version:
        return f"VTune: {version}"
    return "VTune: Available"
