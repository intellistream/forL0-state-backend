#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TEST_TMP="$(mktemp -d /tmp/forl0-offline-preflight-test.XXXXXX)"
cleanup() {
    rm -rf "$TEST_TMP"
}
trap cleanup EXIT

mkdir -p "$TEST_TMP/repo/docker" "$TEST_TMP/flink/bin" \
    "$TEST_TMP/flink/lib" "$TEST_TMP/flink/native" "$TEST_TMP/bin" \
    "$TEST_TMP/venv/bin"
cp "$REPO_ROOT/docker/run_all_apps.sh" "$TEST_TMP/repo/docker/"
cp -a "$REPO_ROOT/docker/lib" "$TEST_TMP/repo/docker/"
ln -s "$REPO_ROOT/docker/deploy" "$TEST_TMP/repo/docker/deploy"
ln -s "$REPO_ROOT/benchmark" "$TEST_TMP/repo/benchmark"
: > "$TEST_TMP/repo/docker/forl0-local.env"
install -m 0755 /bin/true "$TEST_TMP/flink/bin/flink"

# This interpreter models a dependency-complete offline venv while asserting
# that every Python subprocess, including the early dependency probe, receives
# the repository root needed by config loading.
cat > "$TEST_TMP/python-base" <<'EOF'
#!/usr/bin/env bash
[[ -n "${REPO_ROOT:-}" ]] || {
    echo "REPO_ROOT was not exported to preflight Python" >&2
    exit 1
}
case "$0" in
    */venv/bin/python) exit 0 ;;
    *)
        echo "preflight bypassed the selected venv interpreter" >&2
        exit 1
        ;;
esac
EOF
chmod +x "$TEST_TMP/python-base"
ln -s ../../python-base "$TEST_TMP/venv/bin/python"

# Keep the fixture hermetic even when the developer machine already has a
# Flink REST endpoint on localhost:8081.
cat > "$TEST_TMP/bin/curl" <<'EOF'
#!/usr/bin/env bash
exit 1
EOF
chmod +x "$TEST_TMP/bin/curl"

PATH="$TEST_TMP/bin:$PATH" \
FORL0_BENCHMARK_PYTHON_BIN="$TEST_TMP/venv/bin/python" \
FORL0_OFFLINE_ONLY=true \
FORL0_RESULTS_DIR="$TEST_TMP/results" \
FORL0_RUN_ID=offline_preflight_test \
    bash "$TEST_TMP/repo/docker/run_all_apps.sh" \
        --offline --preflight-only --test apps --backend all \
        --flink-home "$TEST_TMP/flink"

echo "offline preflight tests passed"
