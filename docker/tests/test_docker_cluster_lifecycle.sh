#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TEST_TMP="$(mktemp -d /tmp/forl0-docker-lifecycle.XXXXXX)"
cleanup() {
    rm -rf "$TEST_TMP"
}
trap cleanup EXIT

mkdir -p "$TEST_TMP/bin" "$TEST_TMP/flink"/{bin,lib,plugins,opt} \
    "$TEST_TMP/native" "$TEST_TMP/conf"
: > "$TEST_TMP/native/libforl0_engine.so"

cat > "$TEST_TMP/bin/docker" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$DOCKER_TEST_LOG"
if [[ "$1" == "inspect" && "$*" == *"--format"* ]]; then
    printf '172.21.0.2\n'
elif [[ "$1" == "run" || "$1 $2" == "network create" ]]; then
    printf 'fixture-id\n'
fi
EOF

cat > "$TEST_TMP/bin/curl" <<'EOF'
#!/usr/bin/env bash
printf '{"taskmanagers":2,"slots-total":8}\n'
EOF
chmod +x "$TEST_TMP/bin/docker" "$TEST_TMP/bin/curl"

export DOCKER_TEST_LOG="$TEST_TMP/docker.log"
export DOCKER_BIN="$TEST_TMP/bin/docker"
export FLINK_HOME="$TEST_TMP/flink"
export FORL0_NATIVE_DIR="$TEST_TMP/native"
export FORL0_FLINK_CONF_DIR="$TEST_TMP/conf"
export PATH="$TEST_TMP/bin:$PATH"

bash "$REPO_ROOT/docker/docker_run.sh" start >/dev/null

cleanup_line="$(grep -n '^rm -f flink-taskmanager-2 flink-taskmanager-1 flink-jobmanager$' \
    "$DOCKER_TEST_LOG" | head -n 1 | cut -d: -f1)"
create_line="$(grep -n '^network create flink-net$' "$DOCKER_TEST_LOG" | \
    head -n 1 | cut -d: -f1)"
if [[ -z "$cleanup_line" || -z "$create_line" || "$cleanup_line" -ge "$create_line" ]]; then
    echo "start did not remove stale containers before creating the network" >&2
    exit 1
fi

: > "$DOCKER_TEST_LOG"
bash "$REPO_ROOT/docker/docker_run.sh" stop >/dev/null
grep -q '^rm -f flink-taskmanager-2 flink-taskmanager-1 flink-jobmanager$' \
    "$DOCKER_TEST_LOG"
grep -q '^network rm flink-net$' "$DOCKER_TEST_LOG"

echo "docker cluster lifecycle tests passed"
