import tempfile
import unittest
import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / 'scripts'
sys.path.insert(0, str(SCRIPTS_DIR))

from generate_report import generate_report


class PartialReportTest(unittest.TestCase):
    def test_report_without_wordcount_results(self) -> None:
        results = {
            'wordcount': {'hashmap': None, 'forl0': None},
            'nexmark': {'hashmap': {}, 'forl0': {}},
            'client_usecase': {},
            '_run_id': 'smoke-test',
        }
        with tempfile.TemporaryDirectory() as tmp:
            report = generate_report(results, Path(tmp))
            self.assertTrue(report.is_file())


if __name__ == '__main__':
    unittest.main()
