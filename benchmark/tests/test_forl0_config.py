#!/usr/bin/env python3
"""Regression tests for deterministic L0 budget rendering."""

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / 'scripts'
sys.path.insert(0, str(SCRIPTS_DIR))

from utils.config import load_config, render_forl0_config_args  # noqa: E402
from utils.forl0_config import (  # noqa: E402
    build_forl0_config_args, get_forl0_effective_config,
)


class ForL0ConfigTest(unittest.TestCase):
    def test_job_budget_is_divided_by_backend_and_not_mapped_as_per_engine_size(self) -> None:
        args = render_forl0_config_args({
            'l0_cache_enabled': True,
            'l0_cache_size': '512mb',
            'l0_memory_max_size': '1gb',
        }, expected_engines=8)
        self.assertIn('-Dstate.backend.forl0.l0-cache.total-size=512mb', args)
        self.assertIn('-Dstate.backend.forl0.l0-cache.expected-engines=8', args)
        self.assertIn('-Dstate.backend.forl0.native-memory.max-size=1gb', args)
        self.assertFalse(any('l0-cache.size=' in arg for arg in args))

    def test_ablation_environment_overrides_only_l0_controls(self) -> None:
        with patch.dict(os.environ, {
            'FORL0_L0_CACHE_OVERRIDE': 'off',
            'FORL0_L0_STRICT_OVERRIDE': 'true',
            'FORL0_L0_EXPECTED_ENGINES_OVERRIDE': '4',
            'FORL0_L0_TOTAL_SIZE_OVERRIDE': '64mb',
        }, clear=False):
            args = render_forl0_config_args({'l0_cache_enabled': True}, 8)
        self.assertIn('-Dstate.backend.forl0.l0-cache.enabled=false', args)
        self.assertIn('-Dstate.backend.forl0.l0-cache.strict-allocation=true', args)
        self.assertIn('-Dstate.backend.forl0.l0-cache.expected-engines=4', args)
        self.assertIn('-Dstate.backend.forl0.l0-cache.total-size=64mb', args)

    def test_effective_config_precedence_is_explicit(self) -> None:
        config = {
            'backends': [{
                'name': 'forl0',
                'config': {
                    'initial_table_capacity': 64,
                    'workload_overrides': {
                        'nexmark': {'initial_table_capacity': 128},
                    },
                    'query_overrides': {
                        'q18': {'initial_table_capacity': 256},
                    },
                },
            }],
            'nexmark': {
                'forl0_overrides': {'initial_table_capacity': 512},
            },
        }

        nexmark = get_forl0_effective_config(
            config, 'forl0', workload_key='nexmark', query='q18')
        scenario = get_forl0_effective_config(
            config, 'forl0', workload_key='nexmark', query='q18',
            include_workload_section=True)

        self.assertEqual(256, nexmark['initial_table_capacity'])
        self.assertEqual(512, scenario['initial_table_capacity'])
        self.assertEqual({}, get_forl0_effective_config(config, 'hashmap', 'nexmark'))

    def test_non_forl0_runner_args_stay_empty(self) -> None:
        config = {'runtime': {'parallelism': 8}, 'backends': []}
        self.assertEqual([], build_forl0_config_args(config, 'hashmap', 'wordcount'))

    def test_offline_regression_scenarios_budget_for_actual_backend_instances(self) -> None:
        config = load_config()
        wordcount = {
            item['name']: item for item in config['wordcount_scenarios']
        }['stateful_counter_p4_probe']['forl0_overrides']
        self.assertFalse(wordcount['l0_cache_enabled'])
        self.assertEqual(1048576, wordcount['initial_table_capacity'])

        nexmark = {item['name']: item for item in config['nexmark_scenarios']}
        self.assertEqual(
            16,
            nexmark['forl0_no_full_gc_pressure']['forl0_overrides'][
                'l0_cache_expected_engines'],
        )
        self.assertEqual(
            16,
            nexmark['forl0_no_full_gc_allq_pressure']['forl0_overrides'][
                'l0_cache_expected_engines'],
        )
        late = nexmark['forl0_no_full_gc_lateq_deep']
        self.assertEqual(4, late['runtime_overrides']['parallelism'])
        self.assertEqual(8, late['forl0_overrides']['l0_cache_expected_engines'])
        q3 = nexmark['forl0_no_full_gc_extra_sql']
        self.assertEqual(200000, q3['query_overrides']['q3']['tps'])
        self.assertEqual(4194304, q3['forl0_overrides']['max_table_capacity'])

        client = {
            item['name']: item for item in config['client_usecase_scenarios']
        }['scalar_state_probe_2m_ops64_batch']
        self.assertEqual(8, client['forl0_overrides']['l0_cache_expected_engines'])


if __name__ == '__main__':
    unittest.main()
