#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TEST_TMP="$(mktemp -d /tmp/forl0-bootstrap-resolution.XXXXXX)"
cleanup() {
    rm -rf "$TEST_TMP"
}
trap cleanup EXIT

make_stub_launcher() {
    local root="$1"
    cat > "${root}/forl0-offline-app.sh" <<'EOF'
#!/usr/bin/env bash
printf '%s|%s|%s\n' "$FORL0_APP_ROOT" "$FORL0_CONTROL_ROOT" "$*" > "$FORL0_TEST_TRACE"
EOF
    chmod +x "${root}/forl0-offline-app.sh"
}

make_runtime_layout() {
    local root="$1"
    mkdir -p "${root}/docker/lib" "${root}/docker/deploy/nexmark-flink/lib" \
        "${root}/benchmark/config" "${root}/benchmark/scripts"
    : > "${root}/docker/run_all_apps.sh"
    : > "${root}/docker/server_setup.sh"
    : > "${root}/docker/lib/l0_detector.sh"
    : > "${root}/benchmark/config/benchmark.yaml"
    : > "${root}/benchmark/scripts/run_benchmark.py"
    : > "${root}/docker/deploy/flink-statebackend-forL0-1.0-SNAPSHOT.jar"
    : > "${root}/docker/deploy/libforl0_engine.so"
    : > "${root}/docker/deploy/wordcount-benchmark-1.0-SNAPSHOT.jar"
    : > "${root}/docker/deploy/nexmark-flink-0.3-SNAPSHOT.jar"
    : > "${root}/docker/deploy/nexmark-flink/lib/nexmark-flink-0.3-SNAPSHOT.jar"
    : > "${root}/docker/deploy/flink-keyedcoprocessfunction-example-1.0-SNAPSHOT-jar-with-dependencies.jar"
}

# Regression for the real-server failure: an incomplete stale bundle must not
# shadow the complete repository at /root/forL0.
repo_case="${TEST_TMP}/repo"
mkdir -p "$repo_case"
cp "$REPO_ROOT/run-forl0-offline.sh" "$repo_case/run-forl0-offline.sh"
make_stub_launcher "$repo_case"
make_runtime_layout "$repo_case"
mkdir -p "${repo_case}/forl0-offline-linux-arm64-py310-20260721"
trace_repo="${TEST_TMP}/repo.trace"
FORL0_TEST_TRACE="$trace_repo" \
FORL0_OFFLINE_ONLY=true \
FORL0_RESULTS_DIR="${TEST_TMP}/repo-results" \
    bash "$repo_case/run-forl0-offline.sh" --smoke-only --skip-docker-load
grep -Fq "${repo_case}|${repo_case}|" "$trace_repo"

# Running the copied launcher from inside a complete extracted bundle must keep
# bundle checksum verification enabled rather than treating it as a repository.
bundle_case="${TEST_TMP}/bundle"
mkdir -p "$bundle_case"
cp "$REPO_ROOT/run-forl0-offline.sh" "$bundle_case/run-forl0-offline.sh"
make_stub_launcher "$bundle_case"
make_runtime_layout "$bundle_case"
: > "${bundle_case}/docker/install_offline_bundle.sh"
(cd "$bundle_case" && sha256sum forl0-offline-app.sh > offline_bundle_sha256.txt)
trace_bundle="${TEST_TMP}/bundle.trace"
FORL0_TEST_TRACE="$trace_bundle" \
FORL0_OFFLINE_ONLY=true \
FORL0_RESULTS_DIR="${TEST_TMP}/bundle-results" \
    bash "$bundle_case/run-forl0-offline.sh" --smoke-only --skip-docker-load
grep -Fq "${bundle_case}|${bundle_case}|" "$trace_bundle"

# A standalone control launcher may reuse an already installed runtime when no
# complete archive/bundle exists. The incomplete directory remains untouched.
control_case="${TEST_TMP}/control"
installed_case="${TEST_TMP}/installed"
mkdir -p "$control_case"
cp "$REPO_ROOT/run-forl0-offline.sh" "$control_case/run-forl0-offline.sh"
make_stub_launcher "$control_case"
make_runtime_layout "$installed_case"
mkdir -p "${control_case}/forl0-offline-linux-arm64-py310-20260721"
trace_installed="${TEST_TMP}/installed.trace"
FORL0_TEST_TRACE="$trace_installed" \
FORL0_OFFLINE_ONLY=true \
FORL0_INSTALL_DIR="$installed_case" \
FORL0_RESULTS_DIR="${TEST_TMP}/installed-results" \
    bash "$control_case/run-forl0-offline.sh" --smoke-only --skip-docker-load
grep -Fq "${installed_case}|${installed_case}|" "$trace_installed"
[[ -d "${control_case}/forl0-offline-linux-arm64-py310-20260721" ]]

echo "offline bootstrap resolution tests passed"
