#!/usr/bin/env python3
"""Regression tests for offline NexMark category handling."""

import shutil
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / 'scripts'
sys.path.insert(0, str(SCRIPTS_DIR))

from run_nexmark import NexmarkRunner  # noqa: E402


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


if __name__ == '__main__':
    unittest.main()
