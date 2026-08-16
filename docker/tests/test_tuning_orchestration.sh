#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TEST_TMP="$(mktemp -d /tmp/forl0-tuning-test.XXXXXX)"
cleanup() {
    rm -rf "$TEST_TMP"
}
trap cleanup EXIT

cat > "$TEST_TMP/target-l0.json" <<'JSON'
{
  "status": "complete",
  "evidence_label": "real-hardware-calibration",
  "cpu_hash_mix_mops_s": 220.0,
  "heap": [
    {"working_set_bytes": 65536, "random_read_ns": 3.0, "sequential_read_gib_s": 20.0, "sequential_write_gib_s": 30.0},
    {"working_set_bytes": 1048576, "random_read_ns": 12.0, "sequential_read_gib_s": 20.0, "sequential_write_gib_s": 30.0}
  ],
  "l0": [
    {"working_set_bytes": 65536, "random_read_ns": 2.0, "sequential_read_gib_s": 22.0, "sequential_write_gib_s": 40.0},
    {"working_set_bytes": 1048576, "random_read_ns": 4.0, "sequential_read_gib_s": 22.0, "sequential_write_gib_s": 40.0}
  ],
  "heap_parallel_read_scaling": [
    {"workers": 1, "sequential_read_gib_s": 10.0},
    {"workers": 2, "sequential_read_gib_s": 18.0},
    {"workers": 4, "sequential_read_gib_s": 24.0}
  ],
  "l0_parallel_read_scaling": [
    {"workers": 1, "sequential_read_gib_s": 12.0},
    {"workers": 2, "sequential_read_gib_s": 24.0},
    {"workers": 4, "sequential_read_gib_s": 28.0}
  ],
  "l0_hotset_pressure_curve": [
    {"requested_active_bytes": 1048576, "actual_active_bytes": 786432, "sets": 4096, "init_ns_per_set": 4.0, "lookup_ns_per_op": 8.0, "update_ns_per_op": 9.0},
    {"requested_active_bytes": 6291456, "actual_active_bytes": 6291456, "sets": 32768, "init_ns_per_set": 5.0, "lookup_ns_per_op": 10.0, "update_ns_per_op": 12.0}
  ]
}
JSON

FORL0_RESULTS_BASE="$TEST_TMP/results" \
FORL0_RUN_ID=tuning_test \
FORL0_TUNING_MAX_TRIALS=3 \
FORL0_TARGET_CALIBRATION="$TEST_TMP/target-l0.json" \
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
assert manifest['evaluated_trials'] == 3
assert manifest['failed_trials'] == 0
assert manifest['completed_trials'] + manifest['rejected_trials'] == 3
assert manifest['total_parameter_combinations'] == 162
assert manifest['exhaustive'] is False
assert manifest['workload_count'] == 24
assert len(manifest['calibration_model_sha256']) == 64
assert (root / 'calibration_model.json').is_file()
assert len(list(root.glob('trial_*/trial_manifest.json'))) == 3
assert 0 < len(ranking['ranked_trials']) <= 3
for result_path in root.glob('trial_*/simulation_result.json'):
    result = json.loads(result_path.read_text())
    assert result['evidence_label'] == 'simulation/model'
    assert result['calibration_model_sha256'] == manifest['calibration_model_sha256']
    assert len(result['predicted_throughput']) == 24
    assert result['model_inputs']['target_l0_vs_heap_latency_speedup'] == 3.0
PY

PYTHONPATH="$REPO_ROOT/benchmark/scripts" python3 - "$TEST_TMP/results/tuning/tuning_test/calibration_model.json" <<'PY'
import copy
import json
import sys

from tuning_runner import simulation_metrics

model = json.load(open(sys.argv[1], encoding='utf-8'))
params = {
    'initial_table_capacity': 2048,
    'max_table_capacity': 2097152,
    'main_table_load_factor_threshold': 0.80,
    'l0_cache_size': '256mb',
    'l0_memory_max_size': '1024mb',
}
fast = simulation_metrics(params, 1, model)
slow_model = copy.deepcopy(model)
for row in slow_model['rows']:
    row['target_l0_random_read_ns'] = row['target_heap_random_read_ns']
for row in slow_model['parallel_read_rows']:
    row['target_l0_gib_s'] = row['target_heap_gib_s']
slow = simulation_metrics(params, 1, slow_model)
assert fast['objective'] > slow['objective'], (fast, slow)
PY

mkdir -p "$TEST_TMP/legacy-simulation"
cat > "$TEST_TMP/legacy-simulation/tuning_manifest.json" <<'JSON'
{"mode": "simulate", "status": "complete"}
JSON
if PYTHONPATH="$REPO_ROOT/benchmark/scripts" python3 \
        "$REPO_ROOT/benchmark/scripts/tuning_runner.py" \
        --project-root "$REPO_ROOT" \
        --output-dir "$TEST_TMP/legacy-simulation" \
        --mode simulate \
        --calibration-model "$TEST_TMP/results/tuning/tuning_test/calibration_model.json" \
        --max-trials 1 >"$TEST_TMP/legacy-resume.log" 2>&1; then
    echo "legacy simulation without model provenance unexpectedly resumed" >&2
    exit 1
fi
grep -q "refusing to resume legacy simulation" "$TEST_TMP/legacy-resume.log"

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

PYTHONPATH="$REPO_ROOT/benchmark/scripts" python3 - "$TEST_TMP/real-failure" "$REPO_ROOT" <<'PY'
import json
import pathlib
import sys

import tuning_runner

output = pathlib.Path(sys.argv[1])
repo = pathlib.Path(sys.argv[2])
tuning_runner.run_real_trial = lambda *args, **kwargs: {
    'status': 'failed', 'objective': None, 'workloads_complete': 0,
}
sys.argv = [
    'tuning_runner.py', '--project-root', str(repo), '--output-dir', str(output),
    '--mode', 'real', '--max-trials', '1',
]
assert tuning_runner.main() == 1
manifest = json.loads((output / 'tuning_manifest.json').read_text())
assert manifest['status'] == 'failed', manifest
assert manifest['evaluated_trials'] == 0, manifest
assert manifest['failed_trials'] == 1, manifest
PY

# A second invocation with the same run ID must resume rather than duplicate.
FORL0_RESULTS_BASE="$TEST_TMP/results" \
FORL0_RUN_ID=tuning_test \
FORL0_TUNING_MAX_TRIALS=3 \
FORL0_TARGET_CALIBRATION="$TEST_TMP/target-l0.json" \
    "$REPO_ROOT/reproduce-all" --full --simulate --foreground
[[ "$(find "$TEST_TMP/results/tuning/tuning_test" -name trial_manifest.json | wc -l)" -eq 3 ]]

echo "tuning orchestration tests passed"
