#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TEST_TMP="$(mktemp -d /tmp/forl0-profile-failure.XXXXXX)"
cleanup() {
    rm -rf "$TEST_TMP"
}
trap cleanup EXIT

# A real profile without a detected device/library is not valid calibration.
if env -u FORL0_L0_LIBRARY_PATH -u FORL0_L0_DEVICE_PATH \
    python3 "$REPO_ROOT/benchmark/scripts/collect_profile.py" \
        --project-root "$REPO_ROOT" --output-dir "$TEST_TMP/profile"; then
    echo "real profile without L0 unexpectedly succeeded" >&2
    exit 1
fi
grep -q '"status": "failed"' "$TEST_TMP/profile/profile_manifest.json"
grep -q 'real profile requires a detected L0 device' \
    "$TEST_TMP/profile/profile_manifest.json"

echo "profile failure semantics tests passed"
