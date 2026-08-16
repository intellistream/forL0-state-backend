#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TEST_TMP="$(mktemp -d /tmp/forl0-config-override.XXXXXX)"
cleanup() {
    rm -rf "$TEST_TMP"
}
trap cleanup EXIT

printf 'sentinel: tuning-config\n' > "$TEST_TMP/config.yaml"
FORL0_BENCHMARK_CONFIG="$TEST_TMP/config.yaml" \
PYTHONPATH="$REPO_ROOT/benchmark/scripts" python3 - <<'PY'
from utils.config import load_config
assert load_config() == {'sentinel': 'tuning-config'}
print('benchmark config override tests passed')
PY
