#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../lib/benchmark_evidence.sh"

TEST_TMP="$(mktemp -d /tmp/forl0-evidence-test.XXXXXX)"
cleanup() {
    rm -rf "$TEST_TMP"
}
trap cleanup EXIT

BACKEND=forl0
FORL0_REQUIRE_L0_PROOF=true
EVIDENCE_LOG="${TEST_TMP}/positive.log"
cat > "$EVIDENCE_LOG" <<'EOF'
[ForL0-HotCache] engine_start active=1 key_groups=0:64 requested_bytes=1 actual_bytes=1 total_sets=1 strict=1
[ForL0-HotCache] state_attach name=value requested_bytes=192 actual_bytes=192 sets=1
EOF
validate_l0_proof >/dev/null

EVIDENCE_LOG="${TEST_TMP}/fallback.log"
cat > "$EVIDENCE_LOG" <<'EOF'
[ForL0-HotCache] WARN: L0 hardware not available
EOF
if validate_l0_proof >/dev/null 2>&1; then
    echo "expected hardware fallback evidence to fail" >&2
    exit 1
fi

BACKEND=hashmap
EVIDENCE_LOG="${TEST_TMP}/missing.log"
validate_l0_proof >/dev/null

echo "benchmark evidence tests passed"
