#!/usr/bin/env python3
"""
[BENCHMARK_TEST] L0Table Metrics Collection Utilities

This module provides functions to parse and save L0TABLE_METRICS from Flink TaskManager logs.
Used by both WordCount and NexMark benchmarks.

Key features:
- Time-filtered log parsing: Only extract metrics within a specific time window
- Per-query metrics files: Save metrics separately for each (backend, query) combination
- Relative time normalization: Convert absolute timestamps to job-relative seconds
"""

import glob
import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Tuple


def get_flink_home() -> Optional[str]:
    """Get Flink home directory from environment."""
    return os.environ.get('FLINK_HOME')


def find_taskmanager_log(flink_home: str) -> Optional[Path]:
    """
    Find the most recent TaskManager log file.
    
    Args:
        flink_home: Path to Flink installation
        
    Returns:
        Path to log file, or None if not found
    """
    log_pattern = f"{flink_home}/log/*taskexecutor*.log"
    log_files = glob.glob(log_pattern)
    
    if not log_files:
        return None
    
    # Get the most recent log file
    return Path(max(log_files, key=lambda f: Path(f).stat().st_mtime))


def parse_log_timestamp(log_line: str) -> Optional[datetime]:
    """
    Parse timestamp from a log line.
    
    Expected format: "2025-12-11 10:45:05,383 INFO ..."
    
    Args:
        log_line: A line from TaskManager log
        
    Returns:
        datetime object, or None if parsing fails
    """
    # Match pattern: YYYY-MM-DD HH:MM:SS,mmm
    match = re.match(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})', log_line)
    if match:
        try:
            return datetime.strptime(match.group(1), '%Y-%m-%d %H:%M:%S,%f')
        except ValueError:
            pass
    return None


def parse_l0table_metrics_by_time(
    flink_home: str,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None
) -> Optional[List[Dict]]:
    """
    Parse L0TABLE_METRICS from TaskManager log within a time window.
    
    Args:
        flink_home: Path to Flink installation
        start_time: Only include metrics after this time (inclusive)
        end_time: Only include metrics before this time (inclusive)
        
    Returns:
        List of metric samples, or None if no metrics found
        
    Example output:
        [
            {
                "type": "l0table",
                "backend_id": "subtask-0",
                "time_seconds": 1.0,
                "hit_rate": 0.75,
                "accesses": 1000,
                "hits": 750,
                ...
            },
            ...
        ]
    """
    log_file = find_taskmanager_log(flink_home)
    if not log_file:
        return None
    
    metrics = []
    earliest_timestamp = None
    
    try:
        with open(log_file, 'r') as f:
            for line in f:
                # Check if line contains our marker
                if 'L0TABLE_METRICS|' not in line:
                    continue
                
                # Parse timestamp from log line
                log_ts = parse_log_timestamp(line)
                if log_ts is None:
                    continue
                
                # Apply time filters
                if start_time and log_ts < start_time:
                    continue
                if end_time and log_ts > end_time:
                    continue
                
                # Track earliest timestamp for relative time calculation
                if earliest_timestamp is None:
                    earliest_timestamp = log_ts
                
                # Parse JSON payload
                match = re.search(r'L0TABLE_METRICS\|(\{.+\})', line)
                if match:
                    try:
                        json_data = json.loads(match.group(1))
                        # Add log timestamp for reference
                        json_data['_log_timestamp'] = log_ts.isoformat()
                        metrics.append(json_data)
                    except json.JSONDecodeError:
                        pass
                        
    except Exception as e:
        print(f"[L0 Metrics] Error reading log file: {e}")
        return None
    
    return metrics if metrics else None


def normalize_metrics_time(metrics: List[Dict], job_start_time: Optional[datetime] = None) -> List[Dict]:
    """
    Normalize time_seconds in metrics to be relative to job start time.
    
    If job_start_time is not provided, uses the earliest metric timestamp.
    
    Args:
        metrics: List of metric samples
        job_start_time: Optional job start timestamp
        
    Returns:
        List of metrics with normalized time_seconds
    """
    if not metrics:
        return metrics
    
    # Find earliest timestamp if job_start_time not provided
    if job_start_time is None:
        timestamps = []
        for m in metrics:
            if '_log_timestamp' in m:
                try:
                    ts = datetime.fromisoformat(m['_log_timestamp'])
                    timestamps.append(ts)
                except ValueError:
                    pass
        if timestamps:
            job_start_time = min(timestamps)
    
    if job_start_time is None:
        return metrics
    
    # Normalize time_seconds
    for m in metrics:
        if '_log_timestamp' in m:
            try:
                ts = datetime.fromisoformat(m['_log_timestamp'])
                # Calculate relative time in seconds
                relative_secs = (ts - job_start_time).total_seconds()
                m['time_seconds'] = round(relative_secs, 1)
            except (ValueError, TypeError):
                pass
    
    return metrics


def save_l0table_metrics(
    metrics: List[Dict],
    backend: str,
    query: str,
    results_dir: Path,
    extra_metadata: Optional[Dict] = None
) -> str:
    """
    Save L0TABLE metrics to JSON file with consistent naming.
    
    File naming convention: l0_metrics_{backend}_{query}.json
    
    Args:
        metrics: List of metric samples
        backend: Backend name (e.g., "forl0")
        query: Query name (e.g., "wordcount", "q5", "q8")
        results_dir: Directory to save the file
        extra_metadata: Optional extra fields to include in the output
        
    Returns:
        Path to the saved file
    """
    results_dir = Path(results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)
    
    filename = f"l0_metrics_{backend}_{query}.json"
    filepath = results_dir / filename
    
    output = {
        'backend': backend,
        'query': query,
        'timestamp': datetime.now().isoformat(),
        'sample_count': len(metrics),
        'samples': metrics
    }
    
    if extra_metadata:
        output.update(extra_metadata)
    
    with open(filepath, 'w') as f:
        json.dump(output, f, indent=2)
    
    return str(filepath)


def aggregate_metrics_by_time(
    metrics: List[Dict],
    bucket_seconds: float = 1.0
) -> List[Dict]:
    """
    Aggregate metrics across subtasks by time buckets.
    
    This combines metrics from all subtasks at each time point into a single
    aggregated sample with weighted averages for rates and sums for counts.
    
    Args:
        metrics: List of metric samples from multiple subtasks
        bucket_seconds: Time bucket size in seconds
        
    Returns:
        List of aggregated metric samples, one per time bucket
    """
    if not metrics:
        return []
    
    # Group by time bucket
    buckets: Dict[int, List[Dict]] = {}
    for m in metrics:
        time_sec = m.get('time_seconds', 0)
        bucket_key = int(time_sec / bucket_seconds)
        if bucket_key not in buckets:
            buckets[bucket_key] = []
        buckets[bucket_key].append(m)
    
    aggregated = []
    for bucket_key in sorted(buckets.keys()):
        samples = buckets[bucket_key]
        
        # Aggregate based on metric type
        sample_type = samples[0].get('type', 'unknown')
        
        if sample_type in ('l0table', 'l0table_final'):
            agg = _aggregate_l0table_samples(samples, bucket_key * bucket_seconds)
        elif sample_type in ('cache', 'cache_final'):
            agg = _aggregate_cache_samples(samples, bucket_key * bucket_seconds)
        else:
            agg = samples[0].copy()
            agg['time_seconds'] = bucket_key * bucket_seconds
            agg['subtask_count'] = len(samples)
        
        aggregated.append(agg)
    
    return aggregated


def _aggregate_l0table_samples(samples: List[Dict], time_seconds: float) -> Dict:
    """Aggregate L0Table metrics across subtasks."""
    total_accesses = sum(s.get('total_accesses', 0) for s in samples)
    total_hits = sum(s.get('total_hits', 0) for s in samples)
    valid_slots = sum(s.get('valid_slots', 0) for s in samples)
    max_slots = sum(s.get('max_slots', 0) for s in samples)
    table_count = sum(s.get('table_count', 0) for s in samples)
    
    hit_rate = (total_hits / total_accesses * 100) if total_accesses > 0 else 0.0
    
    return {
        'type': samples[0].get('type', 'l0table'),
        'time_seconds': time_seconds,
        'hit_rate': hit_rate,
        'total_accesses': total_accesses,
        'total_hits': total_hits,
        'valid_slots': valid_slots,
        'max_slots': max_slots,
        'table_count': table_count,
        'subtask_count': len(samples),
        'backend_id': 'aggregated'
    }


def _aggregate_cache_samples(samples: List[Dict], time_seconds: float) -> Dict:
    """Aggregate cache metrics across subtasks."""
    l0_entries = sum(s.get('l0_entries', 0) for s in samples)
    maintable_entries = sum(s.get('maintable_entries', 0) for s in samples)
    table_count = sum(s.get('table_count', 0) for s in samples)
    
    return {
        'type': samples[0].get('type', 'cache'),
        'time_seconds': time_seconds,
        'l0_entries': l0_entries,
        'maintable_entries': maintable_entries,
        'table_count': table_count,
        'subtask_count': len(samples),
        'backend_id': 'aggregated'
    }


def get_l0_metrics_summary(metrics: List[Dict]) -> Dict:
    """
    Generate a summary of L0 metrics.
    
    Returns:
        Dict with summary statistics
    """
    if not metrics:
        return {}
    
    l0table_samples = [m for m in metrics if m.get('type') in ('l0table', 'l0table_final')]
    
    if not l0table_samples:
        return {}
    
    hit_rates = [m.get('hit_rate', 0) for m in l0table_samples]
    # Handle both old format (access_count/hit_count) and new format (total_accesses/total_hits)
    total_accesses = max((m.get('total_accesses', m.get('access_count', 0)) for m in l0table_samples), default=0)
    total_hits = max((m.get('total_hits', m.get('hit_count', 0)) for m in l0table_samples), default=0)
    
    return {
        'sample_count': len(l0table_samples),
        'hit_rate_avg': sum(hit_rates) / len(hit_rates) if hit_rates else 0,
        'hit_rate_min': min(hit_rates) if hit_rates else 0,
        'hit_rate_max': max(hit_rates) if hit_rates else 0,
        'total_accesses': total_accesses,
        'total_hits': total_hits,
        'overall_hit_rate': (total_hits / total_accesses * 100) if total_accesses > 0 else 0
    }
