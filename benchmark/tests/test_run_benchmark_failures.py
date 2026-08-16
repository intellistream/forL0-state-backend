import sys
import unittest
from pathlib import Path
from unittest.mock import patch

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / 'scripts'
sys.path.insert(0, str(SCRIPTS_DIR))

import run_benchmark


class BenchmarkFailurePropagationTest(unittest.TestCase):
    def test_client_failure_exits_nonzero(self) -> None:
        config = {
            'client_usecase': {},
            'nexmark': {},
        }
        argv = [
            'run_benchmark.py',
            '--test', 'client_usecase',
            '--backend', 'forl0',
        ]
        with (
            patch.object(sys, 'argv', argv),
            patch.object(run_benchmark, 'load_config', return_value=config),
            patch.object(run_benchmark, 'run_client_usecase', return_value=None),
            self.assertRaises(SystemExit) as raised,
        ):
            run_benchmark.main()
        self.assertEqual(raised.exception.code, 1)


if __name__ == '__main__':
    unittest.main()
