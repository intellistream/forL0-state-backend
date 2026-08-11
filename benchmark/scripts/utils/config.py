#!/usr/bin/env python3
"""
Utility functions for benchmark scripts.
"""

import os
import yaml  # type: ignore[import-untyped]
import subprocess
import re
from pathlib import Path
from datetime import datetime


FORL0_CONFIG_MAPPING = {
    'initial_table_capacity': 'state.backend.forl0.initial-table-capacity',
    'max_table_capacity': 'state.backend.forl0.max-table-capacity',
    'l0_cache_enabled': 'state.backend.forl0.l0-cache.enabled',
    # This is a job-wide budget. The backend divides it by the expected
    # number of parallel keyed StateBackend instances.
    'l0_cache_size': 'state.backend.forl0.l0-cache.total-size',
    'l0_cache_expected_engines': 'state.backend.forl0.l0-cache.expected-engines',
    'l0_cache_strict_allocation': 'state.backend.forl0.l0-cache.strict-allocation',
    'l0_cache_state_size': 'state.backend.forl0.l0-cache.state-size',
    'l0_cache_write_bypass_threshold': 'state.backend.forl0.l0-cache.write-bypass-threshold',
    'l0_cache_replacement_policy': 'state.backend.forl0.l0-cache.replacement-policy',
    'l0_memory_max_size': 'state.backend.forl0.native-memory.max-size',
    'main_table_load_factor_threshold': 'state.backend.forl0.main-table.load-factor-threshold',
    'metrics_collector_enabled': 'forL0.metricsCollector.enabled',
}


def render_forl0_config_args(effective_config, expected_engines=1):
    """Render one canonical set of ForL0 JVM properties.

    Environment overrides are intentionally narrow so the one-click ablation
    runner can switch L0 without editing benchmark.yaml on an offline host.
    """
    effective = dict(effective_config or {})
    effective.setdefault('l0_cache_expected_engines', max(1, int(expected_engines)))
    env_enabled = os.environ.get('FORL0_L0_CACHE_OVERRIDE', '').strip().lower()
    if env_enabled in ('on', 'true', '1'):
        effective['l0_cache_enabled'] = True
    elif env_enabled in ('off', 'false', '0'):
        effective['l0_cache_enabled'] = False
    env_strict = os.environ.get('FORL0_L0_STRICT_OVERRIDE', '').strip().lower()
    if env_strict in ('on', 'true', '1'):
        effective['l0_cache_strict_allocation'] = True
    elif env_strict in ('off', 'false', '0'):
        effective['l0_cache_strict_allocation'] = False
    env_engines = os.environ.get('FORL0_L0_EXPECTED_ENGINES_OVERRIDE', '').strip()
    if env_engines:
        effective['l0_cache_expected_engines'] = max(1, int(env_engines))
    env_total_size = os.environ.get('FORL0_L0_TOTAL_SIZE_OVERRIDE', '').strip()
    if env_total_size:
        effective['l0_cache_size'] = env_total_size

    args = []
    for yaml_key, flink_key in FORL0_CONFIG_MAPPING.items():
        if yaml_key not in effective:
            continue
        value = effective[yaml_key]
        if isinstance(value, bool):
            value = 'true' if value else 'false'
        args.append(f'-D{flink_key}={value}')
    return args


def get_benchmark_root():
    """Get the benchmark root directory."""
    return Path(__file__).parent.parent.parent


def get_project_root():
    """Get the repository/install root directory."""
    return get_benchmark_root().parent


def load_config():
    """Load benchmark configuration."""
    config_path = get_benchmark_root() / "config" / "benchmark.yaml"
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def get_flink_home():
    """Get Flink home directory. Auto-detects from common locations if not set."""
    # 1. Check environment variable first
    env_val = os.environ.get('FLINK_HOME', '')
    if env_val and Path(env_val).is_dir():
        return env_val

    # 2. Check config file (non-placeholder values)
    config = load_config()
    cfg_val = config.get('flink', {}).get('home', '')
    if cfg_val and not cfg_val.startswith('${') and Path(cfg_val).is_dir():
        os.environ['FLINK_HOME'] = cfg_val
        return cfg_val

    # 3. Auto-detect: search common locations (newest version first)
    for search_dir in [Path.home(), Path.home() / 'flink', Path('/opt'), Path('/usr/local')]:
        if not search_dir.exists():
            continue
        candidates = sorted(search_dir.glob('flink-1.20.*'), reverse=True)
        for candidate in candidates:
            if (candidate / 'bin' / 'start-cluster.sh').exists():
                os.environ['FLINK_HOME'] = str(candidate)
                return str(candidate)

    # 4. Fallback
    return ''


def get_results_dir(subdir='raw'):
    """Get results directory path."""
    configured_root = os.environ.get('FORL0_RESULTS_DIR', '').strip()
    results_root = (
        Path(configured_root).expanduser()
        if configured_root
        else get_benchmark_root() / "results"
    )
    results_dir = results_root / subdir
    results_dir.mkdir(parents=True, exist_ok=True)
    return results_dir


def get_timestamp():
    """Get current timestamp string for filenames."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def run_command(cmd, capture_output=True, timeout=None):
    """Run a shell command and return the result."""
    print(f"Running: {' '.join(cmd)}")
    try:
        result = subprocess.run(
            cmd,
            capture_output=capture_output,
            text=True,
            timeout=timeout
        )
        return result
    except subprocess.TimeoutExpired:
        print(f"Command timed out after {timeout} seconds")
        return None
    except Exception as e:
        print(f"Error running command: {e}")
        return None


def find_jar(pattern, search_dir):
    """Find JAR file matching pattern in directory."""
    search_path = Path(search_dir)
    matches = list(search_path.glob(pattern))
    if matches:
        return str(matches[0])
    return None


def get_wordcount_jar():
    """Get path to WordCount benchmark JAR."""
    env_jar = os.environ.get('WORDCOUNT_BENCHMARK_JAR', '')
    if env_jar and Path(env_jar).is_file():
        return env_jar

    wordcount_dir = get_benchmark_root() / "wordcount" / "target"
    jar = find_jar("wordcount-benchmark-*.jar", wordcount_dir)
    if jar and "original" not in jar:
        return jar
    # Try shaded jar
    shaded = find_jar("wordcount-benchmark-*-SNAPSHOT.jar", wordcount_dir)
    if shaded and "original" not in shaded:
        return shaded
    # Fallback: look in docker/deploy/ (pre-built JAR for air-gapped servers)
    for deploy_dir in [
        get_project_root() / "docker" / "deploy",
        get_benchmark_root() / "docker" / "deploy",
    ]:
        jar = find_jar("wordcount-benchmark-*.jar", deploy_dir)
        if jar and "original" not in jar:
            return jar
    return None


def get_nexmark_jar():
    """Get path to NexMark JAR."""
    env_jar = os.environ.get('NEXMARK_FLINK_JAR', '')
    if env_jar and Path(env_jar).is_file():
        return env_jar

    for search_dir in [
        get_benchmark_root() / "lib",
        get_project_root() / "docker" / "deploy",
        get_benchmark_root() / "docker" / "deploy",
    ]:
        jar = find_jar("nexmark-flink-*.jar", search_dir)
        if jar and "original" not in jar:
            return jar
    return None


def parse_json_from_output(output, start_marker="JSON_RESULT_START", end_marker="JSON_RESULT_END"):
    """Extract JSON from command output between markers. Takes the last complete JSON block."""
    import json
    import re
    
    # Find all JSON blocks between markers
    pattern = f'{start_marker}\\s*(.*?)\\s*{end_marker}'
    matches = re.findall(pattern, output, re.DOTALL)
    
    if matches:
        # Try to parse the last match (most recent result)
        for json_str in reversed(matches):
            try:
                return json.loads(json_str.strip())
            except json.JSONDecodeError:
                continue
    return None


def save_result(result, test_name, backend):
    """Save benchmark result to JSON file."""
    import json
    
    results_dir = get_results_dir('raw')
    timestamp = get_timestamp()
    variant = os.environ.get('FORL0_VARIANT', '').strip()
    safe_variant = re.sub(r'[^A-Za-z0-9_.-]+', '_', variant) if variant else ''
    filename = f"{test_name}_{backend}{'_' + safe_variant if safe_variant else ''}_{timestamp}.json"
    filepath = results_dir / filename
    
    # Add metadata
    result['_metadata'] = {
        'test_name': test_name,
        'backend': backend,
        'timestamp': timestamp,
        'datetime': datetime.now().isoformat(),
        'run_id': os.environ.get('FORL0_RUN_ID', '').strip() or None,
        'run_started_epoch': int(os.environ['FORL0_RUN_STARTED_EPOCH'])
        if os.environ.get('FORL0_RUN_STARTED_EPOCH', '').isdigit() else None,
        'variant': variant or None,
    }
    
    with open(filepath, 'w') as f:
        json.dump(result, f, indent=2)
    
    print(f"Result saved to: {filepath}")
    return filepath
