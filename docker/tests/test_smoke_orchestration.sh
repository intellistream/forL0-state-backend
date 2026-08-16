#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$REPO_ROOT/docker/lib/smoke_plan.sh"

mapfile -t plan < <(forl0_smoke_plan)
[[ "${#plan[@]}" -eq 4 ]]
[[ "${plan[0]}" == 'S01|client_usecase|contract_baseline||hashmap|'* ]]
[[ "${plan[1]}" == 'S02|client_usecase|contract_baseline||forl0|'* ]]
[[ "${plan[2]}" == 'S03|nexmark|forl0_tps_probe|q18|hashmap|'* ]]
[[ "${plan[3]}" == 'S04|nexmark|forl0_tps_probe|q18|forl0|'* ]]
if printf '%s\n' "${plan[@]}" | grep -q '|all|'; then
    echo "smoke plan must not share a cluster through backend=all" >&2
    exit 1
fi

TEST_TMP="$(mktemp -d /tmp/forl0-reproduce-plan.XXXXXX)"
cleanup() {
    rm -rf "$TEST_TMP"
}
trap cleanup EXIT
mkdir -p "$TEST_TMP/benchmark/results"
cp "$REPO_ROOT/reproduce-all" "$TEST_TMP/reproduce-all"
mkdir -p "$TEST_TMP/docker/lib"
cp "$REPO_ROOT/docker/lib/result_layout.sh" "$TEST_TMP/docker/lib/result_layout.sh"
cp "$REPO_ROOT/docker/lib/l0_detector.sh" "$TEST_TMP/docker/lib/l0_detector.sh"
mkdir -p "$TEST_TMP/benchmark/scripts"
cp "$REPO_ROOT/benchmark/scripts/capture_hardware_snapshot.py" \
    "$TEST_TMP/benchmark/scripts/capture_hardware_snapshot.py"
cp "$REPO_ROOT/benchmark/scripts/collect_profile.py" \
    "$TEST_TMP/benchmark/scripts/collect_profile.py"
mkdir -p "$TEST_TMP/benchmark/native"
cp "$REPO_ROOT/benchmark/native/l0_calibrate.cpp" \
    "$TEST_TMP/benchmark/native/l0_calibrate.cpp"

cat > "$TEST_TMP/reproduce-smoke" <<'EOF'
#!/usr/bin/env bash
mkdir -p "$FORL0_RESULTS_DIR"
printf 'smoke output\n' > "$FORL0_RESULTS_DIR/smoke.txt"
printf 'smoke:%s:%s:%s\n' "$FORL0_RUN_ID" "$FORL0_RUN_STARTED_EPOCH" "$FORL0_RESULTS_DIR" >> "$FORL0_TEST_TRACE"
EOF
cat > "$TEST_TMP/run-forl0-offline.sh" <<'EOF'
#!/usr/bin/env bash
mkdir -p "$FORL0_RESULTS_DIR"
printf 'formal output\n' > "$FORL0_RESULTS_DIR/formal.txt"
printf 'formal:%s:%s:%s:%s\n' "$FORL0_RUN_ID" "$FORL0_RUN_STARTED_EPOCH" "$FORL0_RESULTS_DIR" "$*" >> "$FORL0_TEST_TRACE"
[[ "${FORL0_TEST_FAIL_FORMAL:-false}" != "true" ]] || exit 7
EOF

export FORL0_TEST_TRACE="$TEST_TMP/trace"
FORL0_RUN_ID=campaign42 \
FORL0_RUN_STARTED_EPOCH=123 \
FORL0_CONTROL_REVISION=test \
    bash "$TEST_TMP/reproduce-all" --worker

grep -q '^smoke:campaign42_smoke:.*:.*/benchmark/results/runs/campaign42/smoke$' "$FORL0_TEST_TRACE"
grep -q '^formal:campaign42:.*:.*/benchmark/results/runs/campaign42/formal:' "$FORL0_TEST_TRACE"
grep -q '^formal:campaign42:.*:--reproduce-ascend --keep-going --no-report --skip-docker-load$' "$FORL0_TEST_TRACE"
if grep -q '^formal:campaign42:123:' "$FORL0_TEST_TRACE"; then
    echo "formal campaign reused the smoke start epoch" >&2
    exit 1
fi
[[ -f "$TEST_TMP/benchmark/results/latest/smoke__smoke.txt" ]]
[[ -f "$TEST_TMP/benchmark/results/latest/formal__formal.txt" ]]
[[ -f "$TEST_TMP/benchmark/results/latest/campaign.log" ]]
[[ -f "$TEST_TMP/benchmark/results/latest/run_manifest.json" ]]
[[ -f "$TEST_TMP/benchmark/results/latest/hardware_snapshot.json" ]]
[[ -f "$TEST_TMP/benchmark/results/latest/l0_calibration.json" ]]
grep -q '"evidence_label": "real-hardware-context"' \
    "$TEST_TMP/benchmark/results/latest/hardware_snapshot.json"
if find "$TEST_TMP/benchmark/results/latest" -mindepth 1 -type d -print -quit | grep -q .; then
    echo "published latest contains a directory" >&2
    exit 1
fi
[[ ! -e "$TEST_TMP/benchmark/results/runs/campaign42" ]]

if FORL0_RUN_ID=campaign-fail \
   FORL0_CONTROL_REVISION=test \
   FORL0_TEST_FAIL_FORMAL=true \
   bash "$TEST_TMP/reproduce-all" --worker; then
    echo "failed formal campaign unexpectedly succeeded" >&2
    exit 1
fi
grep -q '"run_id": "campaign42"' "$TEST_TMP/benchmark/results/latest/run_manifest.json"
[[ -f "$TEST_TMP/benchmark/results/runs/campaign-fail/FAILED.txt" ]]
grep -q '"status": "failed"' "$TEST_TMP/benchmark/results/runs/campaign-fail/run_manifest.json"

echo "smoke orchestration tests passed"
