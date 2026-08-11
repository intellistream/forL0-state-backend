#!/usr/bin/env python3
"""Regression tests for offline NexMark category handling."""

import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / 'scripts'
sys.path.insert(0, str(SCRIPTS_DIR))

from run_nexmark import (  # noqa: E402
    NexmarkRunner, apply_nexmark_scenario,
    is_benign_post_summary_cancel_conflict,
)


class NexmarkCategoryCompatibilityTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_root = Path(tempfile.mkdtemp(prefix='nexmark-category-test-'))
        self.nexmark_home = self.temp_root / 'nexmark-home'
        self.flink_home = self.temp_root / 'flink'
        for directory in (
            self.nexmark_home / 'bin',
            self.nexmark_home / 'conf',
            self.nexmark_home / 'lib',
            self.nexmark_home / 'log',
            self.nexmark_home / 'queries',
            self.nexmark_home / 'queries-forl0',
            self.flink_home / 'lib',
        ):
            directory.mkdir(parents=True, exist_ok=True)

        (self.nexmark_home / 'conf' / 'config.yaml').write_text(
            'parallelism.default: 1\n'
        )
        (self.nexmark_home / 'conf' / 'log4j.properties').write_text('')
        (self.nexmark_home / 'queries' / 'q5.sql').write_text('ordinary q5\n')
        (self.nexmark_home / 'queries-forl0' / 'q5.sql').write_text('forl0 q5\n')

        self.runner = NexmarkRunner.__new__(NexmarkRunner)
        self.runner.project_root = self.temp_root
        self.runner.nexmark_home = self.nexmark_home
        self.runner.flink_home = self.flink_home
        self.runner.rest_url = 'http://localhost:8081'
        self.runner.metric_reporter_host = '127.0.0.1'
        self.runner.metric_reporter_port = None
        self.runner.runtime_config = {'parallelism': 1}
        self.runner.config = {
            'runtime': self.runner.runtime_config,
            'backends': [{'name': 'hashmap', 'class': 'hashmap'}],
        }
        self.runner.nexmark_config = {'category': 'forl0'}

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_root, ignore_errors=True)

    def test_custom_category_uses_oa_driver_with_specialized_queries(self) -> None:
        cmd, env, conf_dir = self.runner._build_driver_command(
            'q5', 'hashmap', 100, 0, 0, 0
        )
        self.addCleanup(shutil.rmtree, conf_dir, True)

        location = Path(cmd[cmd.index('--location') + 1])
        category = cmd[cmd.index('--category') + 1]
        nexmark_yaml = Path(env['NEXMARK_CONF_DIR'], 'nexmark.yaml').read_text()

        self.assertEqual(category, 'oa')
        self.assertEqual((location / 'queries' / 'q5.sql').read_text(), 'forl0 q5\n')
        self.assertIn('nexmark.workload.suite.run.queries: "q5"', nexmark_yaml)
        self.assertNotIn('queries.forl0', nexmark_yaml)
        self.assertEqual(env['FLINK_CONF_DIR'], str(conf_dir))

    def test_oa_category_keeps_original_distribution(self) -> None:
        self.runner.nexmark_config['category'] = 'oa'
        cmd, _, conf_dir = self.runner._build_driver_command(
            'q5', 'hashmap', 100, 0, 0, 0
        )
        self.addCleanup(shutil.rmtree, conf_dir, True)

        self.assertEqual(Path(cmd[cmd.index('--location') + 1]), self.nexmark_home)
        self.assertEqual(cmd[cmd.index('--category') + 1], 'oa')

    def test_invalid_category_is_rejected_before_path_construction(self) -> None:
        self.runner.nexmark_config['category'] = '../forl0'
        with self.assertRaisesRegex(ValueError, 'Invalid NexMark query category'):
            self.runner._get_query_category()

    def test_only_post_summary_cancel_409_is_benign(self) -> None:
        benign = (
            'Summary Average: Throughput=1.05 M, Cores=45.97\n'
            'Stop job query q18\n'
            'RuntimeException: http execute failed,status code is 409\n'
            'at com.github.nexmark.flink.QueryRunner.cancelJob(QueryRunner.java:113)\n'
        )
        self.assertTrue(is_benign_post_summary_cancel_conflict(benign))
        self.assertFalse(is_benign_post_summary_cancel_conflict(benign.replace('409', '500')))
        self.assertFalse(is_benign_post_summary_cancel_conflict(
            benign + 'OutOfMemoryError: native allocation failed\n'))

    def test_scenario_applies_retry_backoff(self) -> None:
        config = {
            'runtime': {},
            'nexmark': {},
            'nexmark_scenarios': [{
                'name': 'retry_probe',
                'retry_backoff_seconds': 17,
            }],
            'backends': [],
        }
        applied = apply_nexmark_scenario(config, 'retry_probe')
        self.assertEqual(17, applied['nexmark']['retry_backoff_seconds'])

    @patch('run_nexmark.requests.get')
    def test_failed_job_health_issue_includes_rest_exception(self, mock_get) -> None:
        class Response:
            def __init__(self, payload):
                self.status_code = 200
                self._payload = payload

            def json(self):
                return self._payload

        mock_get.side_effect = [
            Response({'taskmanagers': 2, 'slots-total': 8}),
            Response({'jobs': [{
                'jid': 'deadbeef', 'name': 'nexmark_q18', 'state': 'FAILED',
            }]}),
            Response({'root-exception': 'NoResourceAvailableException: no slots'}),
        ]
        self.runner.baseline_failed_job_ids = set()
        with patch.dict('os.environ', {
            'FORL0_EXPECTED_TASKMANAGERS': '2',
            'FORL0_EXPECTED_SLOTS': '8',
        }):
            issue = self.runner._cluster_health_issue()

        self.assertIn('deadbeef', issue)
        self.assertIn('NoResourceAvailableException: no slots', issue)


if __name__ == '__main__':
    unittest.main()
