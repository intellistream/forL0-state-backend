"""Canonical ForL0 benchmark configuration resolution and rendering."""

import os
from typing import Optional


FORL0_CONFIG_MAPPING = {
    'initial_table_capacity': 'state.backend.forl0.initial-table-capacity',
    'max_table_capacity': 'state.backend.forl0.max-table-capacity',
    'l0_cache_enabled': 'state.backend.forl0.l0-cache.enabled',
    # Job-wide budget; the backend divides it by expected engine count.
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


def get_forl0_effective_config(
        config: dict,
        backend: str,
        workload_key: Optional[str] = None,
        query: Optional[str] = None,
        include_workload_section: bool = False) -> dict:
    """Resolve backend, workload, query, then scenario-section overrides."""
    if backend != 'forl0':
        return {}

    backend_config = next((
        item.get('config', {})
        for item in config.get('backends', [])
        if item.get('name') == 'forl0'
    ), None)
    if not backend_config:
        return {}

    effective = dict(backend_config)
    workload_overrides = backend_config.get('workload_overrides', {})
    if workload_key and isinstance(workload_overrides, dict):
        workload_config = workload_overrides.get(workload_key, {})
        if isinstance(workload_config, dict):
            effective.update(workload_config)

    query_overrides = backend_config.get('query_overrides', {})
    if query and isinstance(query_overrides, dict):
        query_config = query_overrides.get(query, {})
        if isinstance(query_config, dict):
            effective.update(query_config)

    if include_workload_section and workload_key:
        workload_section = config.get(workload_key, {})
        if isinstance(workload_section, dict):
            scenario_overrides = workload_section.get('forl0_overrides', {})
            if isinstance(scenario_overrides, dict):
                effective.update(scenario_overrides)

    return effective


def render_forl0_config_args(effective_config: dict, expected_engines: int = 1) -> list:
    """Render one canonical set of ForL0 JVM properties."""
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


def build_forl0_config_args(
        config: dict,
        backend: str,
        workload_key: Optional[str] = None,
        query: Optional[str] = None,
        include_workload_section: bool = False) -> list:
    """Resolve and render ForL0 arguments using the configured parallelism."""
    if backend != 'forl0':
        return []
    effective = get_forl0_effective_config(
        config, backend, workload_key, query, include_workload_section)
    return render_forl0_config_args(
        effective, config.get('runtime', {}).get('parallelism', 1))
