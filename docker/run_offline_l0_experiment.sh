#!/usr/bin/env bash
set -euo pipefail

################################################################################
# Offline L0 target one-click experiment runner.
#
# Target flow after scp-ing the repository to an offline server:
#   cd /path/to/forL0-state-backend/docker
#   ./run_offline_l0_experiment.sh --flink-home /path/to/flink-1.20.3
#
# Default behavior:
#   1. deploy repository-local artifacts and run preflight
#   2. run the complete apps suite once without profiling and without report
#   3. run the same suite again with CPU profiling and generate the HTML report
#   4. verify that the report was regenerated after this script started and that
#      it references the latest profiled raw/profile artifacts
#
# The script intentionally leaves the Flink cluster running for inspection.
################################################################################

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

FLINK_DIR="${FLINK_HOME:-}"
TEST_NAME="apps"
BACKEND="all"
PROFILE_MODE="cpu"
RUN_PREFLIGHT="true"
RUN_NO_PROFILE_PASS="true"
RUN_PROFILE_PASS="true"
CLEAN_CLUSTER="true"
EXTRA_ARGS=()

usage() {
    cat <<'EOF'
Usage:
  ./run_offline_l0_experiment.sh [options]

Options:
  --flink-home PATH          Flink installation directory. Defaults to FLINK_HOME
                             or server_setup.sh auto-detection.
  --test NAME                Benchmark suite to run. Default: apps.
  --backend NAME             Backend to run. Default: all.
  --profile MODE             Profile mode for the second pass. Default: cpu.
  --scenario NAME            Forward a scenario to run_all_apps.sh.
  --query LIST               Forward a NexMark query list to run_all_apps.sh.
  --skip-preflight           Skip dependency/config preflight.
  --skip-no-profile-pass     Skip the first no-profile/no-report pass.
  --skip-profile-pass        Skip the second profile+report pass.
  --reuse-cluster             Reuse the current Flink cluster instead of restarting it first.
  -h, --help                 Show this help.

Default behavior runs two passes for offline target validation:
  pass 1: --offline --no-profile --no-report
  pass 2: --offline --profile cpu, then report generation

The script leaves the Flink cluster running after completion.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --flink-home)
            FLINK_DIR="$2"
            shift 2
            ;;
        --test)
            TEST_NAME="$2"
            shift 2
            ;;
        --backend)
            BACKEND="$2"
            shift 2
            ;;
        --profile)
            PROFILE_MODE="$2"
            shift 2
            ;;
        --scenario|--query)
            EXTRA_ARGS+=("$1" "$2")
            shift 2
            ;;
        --skip-preflight)
            RUN_PREFLIGHT="false"
            shift
            ;;
        --skip-no-profile-pass)
            RUN_NO_PROFILE_PASS="false"
            shift
            ;;
        --skip-profile-pass)
            RUN_PROFILE_PASS="false"
            shift
            ;;
        --reuse-cluster)
            CLEAN_CLUSTER="false"
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if [[ -n "$FLINK_DIR" ]]; then
    export FLINK_HOME="$FLINK_DIR"
fi

RUN_ID="$(date +%Y%m%d_%H%M%S)"
RUN_STARTED_EPOCH="$(date +%s)"
LOG_DIR="${REPO_ROOT}/benchmark/results/run_logs"
mkdir -p "$LOG_DIR"
LOG_FILE="${LOG_DIR}/offline_l0_experiment_${RUN_ID}.log"

exec > >(tee -a "$LOG_FILE") 2>&1

echo "============================================================"
echo "  Offline L0 one-click experiment"
echo "============================================================"
echo "  Repo:       ${REPO_ROOT}"
echo "  Test:       ${TEST_NAME}"
echo "  Backend:    ${BACKEND}"
echo "  Profile:    ${PROFILE_MODE}"
echo "  Started:    $(date '+%F %T %z')"
echo "  Log:        ${LOG_FILE}"
echo

bootstrap_async_profiler() {
    local archive="${REPO_ROOT}/benchmark/offline-packages/async-profiler-4.4-linux-arm64.tar.gz"
    local bundled_home="${REPO_ROOT}/tools/async-profiler-4.4-linux-arm64"
    local stable_link="${REPO_ROOT}/tools/async-profiler"

    if [[ -n "${ASYNC_PROFILER_HOME:-}" && -x "${ASYNC_PROFILER_HOME}/bin/asprof" ]]; then
        ASYNC_PROFILER_HOME="$(readlink -f "$ASYNC_PROFILER_HOME")"
        export ASYNC_PROFILER_HOME
        echo "[profiler] using ASYNC_PROFILER_HOME=${ASYNC_PROFILER_HOME}"
        return 0
    fi

    if [[ ! -x "${stable_link}/bin/asprof" && -f "$archive" ]]; then
        mkdir -p "${REPO_ROOT}/tools"
        tar -xzf "$archive" -C "${REPO_ROOT}/tools"
        ln -sfn "$(basename "$bundled_home")" "$stable_link"
    fi

    if [[ -x "${stable_link}/bin/asprof" ]]; then
        export ASYNC_PROFILER_HOME="$bundled_home"
        echo "[profiler] using bundled async-profiler: ${ASYNC_PROFILER_HOME}"
        "${ASYNC_PROFILER_HOME}/bin/asprof" --version
    else
        echo "[profiler] ERROR: async-profiler is required for the profile pass." >&2
        echo "[profiler] Put async-profiler-4.4-linux-arm64.tar.gz under benchmark/offline-packages/." >&2
        exit 1
    fi
}

run_all_apps() {
    "${SCRIPT_DIR}/run_all_apps.sh" "$@"
}

if [[ "$RUN_PREFLIGHT" == "true" ]]; then
    echo
    echo "[1/4] Deploy repository-local artifacts and run preflight"
    if [[ -n "${FLINK_HOME:-}" ]]; then
        "${SCRIPT_DIR}/server_setup.sh" --flink-home "$FLINK_HOME" --no-start
    else
        "${SCRIPT_DIR}/server_setup.sh" --no-start
    fi
    run_all_apps --offline --preflight-only --test "$TEST_NAME" --backend "$BACKEND" "${EXTRA_ARGS[@]}"
fi

if [[ "$CLEAN_CLUSTER" == "true" ]]; then
    echo
    echo "[cluster] Start from a clean Flink Docker cluster"
    # shellcheck disable=SC1091
    source "${REPO_ROOT}/docker/forl0-local.env"
    "${SCRIPT_DIR}/docker_run.sh" stop || true
    "${SCRIPT_DIR}/docker_run.sh" start
fi

if [[ "$RUN_NO_PROFILE_PASS" == "true" ]]; then
    echo
    echo "[2/4] Run complete suite without profiling and without report"
    run_all_apps --offline --test "$TEST_NAME" --backend "$BACKEND" --no-profile --no-report "${EXTRA_ARGS[@]}"
fi

if [[ "$RUN_PROFILE_PASS" == "true" ]]; then
    echo
    echo "[3/4] Run complete suite with profiling and generate report"
    bootstrap_async_profiler
    run_all_apps --offline --test "$TEST_NAME" --backend "$BACKEND" --profile "$PROFILE_MODE" "${EXTRA_ARGS[@]}"
fi

echo
echo "[4/4] Verify regenerated report and latest profiled artifacts"
python3 - "$REPO_ROOT" "$RUN_STARTED_EPOCH" "$TEST_NAME" "$BACKEND" <<'VERIFY_REPORT'
import json
import sys
from html.parser import HTMLParser
from pathlib import Path

repo = Path(sys.argv[1])
started = int(sys.argv[2])
test_name = sys.argv[3]
backend_arg = sys.argv[4]
report = repo / 'benchmark/results/reports/benchmark_report.html'
raw_dir = repo / 'benchmark/results/raw'

class TextParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts = []

    def handle_data(self, data):
        data = data.strip()
        if data:
            self.parts.append(data)

if not report.exists():
    raise SystemExit(f'ERROR: report not found: {report}')
if report.stat().st_mtime < started:
    raise SystemExit(f'ERROR: report was not regenerated after this run: {report}')

latest = {}
for path in raw_dir.glob('*.json'):
    try:
        data = json.loads(path.read_text())
    except Exception:
        continue
    meta = data.get('_metadata', {})
    test = meta.get('test_name') or data.get('benchmark')
    backend = meta.get('backend') or data.get('backend')
    ts = meta.get('timestamp', '')
    if not test or not backend:
        continue
    key = (test, backend)
    if key not in latest or ts > latest[key][0]:
        latest[key] = (ts, path, data)

profiled = [
    item for item in latest.values()
    if item[2].get('profiler_files') and item[1].stat().st_mtime >= started
]
if not profiled:
    raise SystemExit('ERROR: this run did not produce fresh profiled raw results')

expected_backends = []
if backend_arg == 'all':
    expected_backends = ['hashmap', 'forl0']
elif backend_arg in ('hashmap', 'forl0'):
    expected_backends = [backend_arg]

if test_name in ('client_usecase', 'wordcount') and expected_backends:
    fresh_profiled_backends = {
        data.get('_metadata', {}).get('backend') or data.get('backend')
        for _, _, data in profiled
        if (data.get('_metadata', {}).get('test_name') or data.get('benchmark')) in (test_name, 'client-usecase')
    }
    missing_backends = [backend for backend in expected_backends if backend not in fresh_profiled_backends]
    if missing_backends:
        raise SystemExit('ERROR: missing fresh profiled raw results for backend(s): ' + ', '.join(missing_backends))

parser = TextParser()
parser.feed(report.read_text(errors='ignore'))
text = '\n'.join(parser.parts)

missing = []
for ts, path, data in profiled:
    if ts not in text:
        missing.append(ts)
    for profile_path in data.get('profiler_files', {}).values():
        profile_file = Path(profile_path)
        name = profile_file.name
        if not profile_file.exists() or profile_file.stat().st_mtime < started:
            missing.append(name + ' (not fresh)')
        if name not in text:
            missing.append(name)
if missing:
    raise SystemExit('ERROR: report does not reference latest profiled artifacts: ' + ', '.join(missing))

print(f'      report: {report}')
print(f'      report_mtime: {report.stat().st_mtime:.0f}')
for ts, path, data in profiled:
    print(f'      profiled_raw: {path.name} ({data.get("backend")}, {ts})')
    for profile_path in data.get('profiler_files', {}).values():
        print(f'      profile: {Path(profile_path).name}')
print('      verification: PASS')
VERIFY_REPORT

echo
echo "============================================================"
echo "  Offline L0 experiment completed"
echo "============================================================"
echo "  Report: ${REPO_ROOT}/benchmark/results/reports/benchmark_report.html"
echo "  Log:    ${LOG_FILE}"
echo "  Flink cluster is intentionally left running for inspection."
