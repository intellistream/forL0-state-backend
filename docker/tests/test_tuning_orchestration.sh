#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TEST_TMP="$(mktemp -d /tmp/forl0-tuning-test.XXXXXX)"
cleanup() {
    rm -rf "$TEST_TMP"
}
trap cleanup EXIT

FORL0_RESULTS_BASE="$TEST_TMP/results" \
FORL0_RUN_ID=tuning_test \
FORL0_TUNING_MAX_TRIALS=3 \
    "$REPO_ROOT/reproduce-all" --full --simulate --foreground

python3 - "$TEST_TMP/results/tuning/tuning_test" <<'PY'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
manifest = json.loads((root / 'tuning_manifest.json').read_text())
ranking = json.loads((root / 'ranking.json').read_text())
assert manifest['evidence_label'] == 'simulation/model'
assert manifest['status'] == 'complete'
assert manifest['planned_trials'] == 3
assert manifest['total_parameter_combinations'] == 162
assert manifest['exhaustive'] is False
assert manifest['workload_count'] == 24
assert len(list(root.glob('trial_*/trial_manifest.json'))) == 3
assert 0 < len(ranking['ranked_trials']) <= 3
for result_path in root.glob('trial_*/simulation_result.json'):
    result = json.loads(result_path.read_text())
    assert result['evidence_label'] == 'simulation/model'
    assert len(result['predicted_throughput']) == 24
PY

PYTHONPATH="$REPO_ROOT/benchmark/scripts" python3 - "$TEST_TMP/real-resume" "$REPO_ROOT" <<'PY'
import json
import pathlib
import sys

from tuning_runner import run_real_trial

trial = pathlib.Path(sys.argv[1])
repo = pathlib.Path(sys.argv[2])
ids = ['W01', 'W02'] + [f'N{i:02d}' for i in range(1, 15)] + [f'C{i:02d}' for i in range(1, 9)]
for workload_id in ids:
    root = trial / 'workloads' / workload_id
    root.mkdir(parents=True)
    score = 100.0 if int(workload_id[1:]) % 2 else 120.0
    (root / 'workload_manifest.json').write_text(json.dumps({
        'status': 'complete', 'workload_id': workload_id, 'score': score,
    }))
result = run_real_trial(repo, trial, 'resume-test', trial / 'unused.yaml', ids)
assert result['status'] == 'complete'
assert result['workloads_complete'] == 24
assert result['pair_ratios_complete'] == 12
assert abs(result['objective'] - 1.2) < 1e-9
PY

# A second invocation with the same run ID must resume rather than duplicate.
FORL0_RESULTS_BASE="$TEST_TMP/results" \
FORL0_RUN_ID=tuning_test \
FORL0_TUNING_MAX_TRIALS=3 \
    "$REPO_ROOT/reproduce-all" --full --simulate --foreground
[[ "$(find "$TEST_TMP/results/tuning/tuning_test" -name trial_manifest.json | wc -l)" -eq 3 ]]

echo "tuning orchestration tests passed"
