#!/usr/bin/env python3
"""Regression tests for matched benchmark-result selection."""

import sys
import unittest
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / 'scripts'
sys.path.insert(0, str(SCRIPTS_DIR))

from utils.result_selection import (  # noqa: E402
    nexmark_workload_identity, newest_complete_pair,
    scoped_nexmark_workload_identity, wordcount_workload_identity,
)


class ResultSelectionTest(unittest.TestCase):
    def test_q8_different_scenarios_cannot_form_a_pair(self) -> None:
        hashmap_identity = nexmark_workload_identity(
            'forl0_no_full_gc_q8_q11_deep', 'q8',
            {'configured_tps': 1_000_000, 'parallelism': 4},
        )
        forl0_identity = nexmark_workload_identity(
            'contract_baseline', 'q8',
            {'configured_tps': 0, 'parallelism': 4},
        )
        candidates = {
            hashmap_identity: {'hashmap': {'_selection_rank': '20260710_050207'}},
            forl0_identity: {'forl0': {'_selection_rank': '20260710_021408'}},
        }
        self.assertEqual({}, newest_complete_pair(candidates))

    def test_same_scenario_but_different_tps_cannot_form_a_pair(self) -> None:
        hashmap_identity = nexmark_workload_identity(
            'probe', 'q18', {'configured_tps': 5_000_000, 'parallelism': 4})
        forl0_identity = nexmark_workload_identity(
            'probe', 'q18', {'configured_tps': 12_000_000, 'parallelism': 4})
        self.assertEqual({}, newest_complete_pair({
            hashmap_identity: {'hashmap': {'_selection_rank': '1'}},
            forl0_identity: {'forl0': {'_selection_rank': '2'}},
        }))

    def test_wordcount_repeat_policy_is_part_of_identity(self) -> None:
        best = wordcount_workload_identity({
            'scenario': 'stateful_counter_p4_probe', 'total_records': 200,
            'parallelism': 4, 'repeat_runs': 3, 'repeat_policy': 'best',
        })
        median = wordcount_workload_identity({
            'scenario': 'stateful_counter_p4_probe', 'total_records': 200,
            'parallelism': 4, 'repeat_runs': 3, 'repeat_policy': 'median',
        })
        self.assertNotEqual(best, median)

    def test_unscoped_isolated_directories_cannot_form_a_pair(self) -> None:
        workload = {'configured_tps': 1_000_000, 'parallelism': 4}
        hashmap_identity = scoped_nexmark_workload_identity(
            None, 'nexmark_hashmap', 'probe', 'q8', workload)
        forl0_identity = scoped_nexmark_workload_identity(
            None, 'nexmark_forl0', 'probe', 'q8', workload)
        self.assertEqual({}, newest_complete_pair({
            hashmap_identity: {'hashmap': {'_selection_rank': '1'}},
            forl0_identity: {'forl0': {'_selection_rank': '2'}},
        }))

    def test_run_id_allows_isolated_directories_to_pair(self) -> None:
        workload = {'configured_tps': 1_000_000, 'parallelism': 4}
        first = scoped_nexmark_workload_identity(
            'campaign-1', 'nexmark_hashmap', 'probe', 'q8', workload)
        second = scoped_nexmark_workload_identity(
            'campaign-1', 'nexmark_forl0', 'probe', 'q8', workload)
        self.assertEqual(first, second)

    def test_newest_complete_pair_wins(self) -> None:
        old = {
            'hashmap': {'_selection_rank': '1'},
            'forl0': {'_selection_rank': '2'},
        }
        new = {
            'hashmap': {'_selection_rank': '3'},
            'forl0': {'_selection_rank': '4'},
        }
        self.assertIs(new, newest_complete_pair({'old': old, 'new': new}))


if __name__ == '__main__':
    unittest.main()
