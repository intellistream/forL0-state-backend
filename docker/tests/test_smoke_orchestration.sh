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

cat > "$TEST_TMP/reproduce-smoke" <<'EOF'
#!/usr/bin/env bash
printf 'smoke:%s:%s\n' "$FORL0_RUN_ID" "$FORL0_RUN_STARTED_EPOCH" >> "$FORL0_TEST_TRACE"
EOF
cat > "$TEST_TMP/run-forl0-offline.sh" <<'EOF'
#!/usr/bin/env bash
printf 'formal:%s:%s\n' "$FORL0_RUN_ID" "$FORL0_RUN_STARTED_EPOCH" >> "$FORL0_TEST_TRACE"
EOF

export FORL0_TEST_TRACE="$TEST_TMP/trace"
FORL0_RUN_ID=campaign42 \
FORL0_RUN_STARTED_EPOCH=123 \
FORL0_CONTROL_REVISION=test \
    bash "$TEST_TMP/reproduce-all" --worker

grep -q '^smoke:campaign42_smoke:' "$FORL0_TEST_TRACE"
grep -q '^formal:campaign42:' "$FORL0_TEST_TRACE"
if grep -q '^formal:campaign42:123$' "$FORL0_TEST_TRACE"; then
    echo "formal campaign reused the smoke start epoch" >&2
    exit 1
fi

echo "smoke orchestration tests passed"
