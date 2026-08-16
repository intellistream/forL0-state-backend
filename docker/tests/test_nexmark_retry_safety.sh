#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PYTHONPATH="$REPO_ROOT/benchmark/scripts" python3 - <<'PY'
from unittest.mock import Mock, patch

from run_nexmark import NexmarkRunner

runner = NexmarkRunner.__new__(NexmarkRunner)
runner.rest_url = 'http://localhost:8081'

healthy = Mock(status_code=200)
healthy.json.return_value = {'taskmanagers': 2, 'slots-total': 8}
with patch('run_nexmark.requests.get', return_value=healthy):
    assert runner._cluster_capacity_issue() is None

degraded = Mock(status_code=200)
degraded.json.return_value = {'taskmanagers': 1, 'slots-total': 4}
with patch('run_nexmark.requests.get', return_value=degraded):
    issue = runner._cluster_capacity_issue()
assert 'TaskManager count is still degraded' in issue
print('nexmark retry safety tests passed')
PY
