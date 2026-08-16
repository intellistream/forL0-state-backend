#!/usr/bin/env bash
# Failure markers, TaskManager evidence capture, and L0 proof gates.
# This file is sourced by docker/run_all_apps.sh and relies on its run-context
# variables plus flink_cluster_summary() and cancel_running_jobs().

write_failed_marker() {
    local status="$1"
    local results_dir="${FORL0_RESULTS_DIR:-${REPO_ROOT}/benchmark/results/runs/${FORL0_RUN_ID:-manual}/formal}"
    local log_dir="${results_dir}/run_logs"
    mkdir -p "$log_dir"
    local marker="${log_dir}/FAILED_${TEST_NAME}_${BACKEND}_$(date '+%Y%m%d_%H%M%S').txt"
    {
        echo "ForL0 benchmark failed"
        echo "date=$(date '+%Y-%m-%d %H:%M:%S %z')"
        echo "repo=${REPO_ROOT}"
        echo "test=${TEST_NAME}"
        echo "backend=${BACKEND}"
        echo "status=${status}"
        echo "profile=${PROFILE_MODE:-disabled}"
        echo "expected_taskmanagers=${EXPECTED_TASKMANAGERS}"
        echo "expected_slots=${EXPECTED_SLOTS}"
        echo "extra_args=${EXTRA_ARGS[*]:-}"
        echo "taskmanager_evidence=${EVIDENCE_LOG:-unavailable}"
        echo "evidence_since_epoch=${EVIDENCE_SINCE_EPOCH:-unavailable}"
        echo ""
        echo "cluster_summary:"
        flink_cluster_summary || true
        echo ""
        echo "running_or_recent_jobs:"
        python3 - <<'PY' || true
import json
import urllib.request

try:
    jobs = json.load(urllib.request.urlopen("http://localhost:8081/jobs/overview", timeout=5)).get("jobs", [])
    for job in jobs[:20]:
        print(job)
except Exception as exc:
    print(f"cannot query jobs: {exc}")
PY
    } > "$marker"
    echo "[cleanup] Wrote failure marker: $marker"
}

capture_taskmanager_evidence() {
    local results_dir="${FORL0_RESULTS_DIR:-${REPO_ROOT}/benchmark/results/runs/${FORL0_RUN_ID:-manual}/formal}"
    local log_dir="${results_dir}/run_logs"
    mkdir -p "$log_dir"
    EVIDENCE_LOG="${log_dir}/taskmanager_${TEST_NAME}_${BACKEND}_$(date '+%Y%m%d_%H%M%S').log"
    : > "$EVIDENCE_LOG"
    # Scope proof to this command. Reusing the suite-wide start time would let
    # an earlier workload's state_attach line satisfy a later workload's gate.
    local since_arg="${EVIDENCE_SINCE_EPOCH:-${FORL0_RUN_STARTED_EPOCH:-0}}"
    local container
    for container in flink-taskmanager-1 flink-taskmanager-2; do
        if ! docker inspect "$container" >/dev/null 2>&1; then
            continue
        fi
        {
            echo "=== ${container} ==="
            docker logs --since "$since_arg" "$container" 2>&1 || true
        } | grep -E "^(===|.*\[ForL0-(HotCache|Memory)\]|.*(OutOfMemoryError|exit code 137|TaskManager.*(lost|disconnect)|Job execution failed|Caused by:))" \
          >> "$EVIDENCE_LOG" || true
    done
    echo "[evidence] TaskManager evidence: ${EVIDENCE_LOG}"
    if [[ -s "$EVIDENCE_LOG" ]]; then
        sed -n '1,240p' "$EVIDENCE_LOG"
    fi
}

validate_l0_proof() {
    [[ "${FORL0_REQUIRE_L0_PROOF:-false}" == "true" ]] || return 0
    [[ "$BACKEND" == "forl0" || "$BACKEND" == "all" ]] || return 0
    if [[ ! -s "$EVIDENCE_LOG" ]]; then
        echo "✗ L0 proof required, but no TaskManager evidence was captured."
        return 1
    fi
    if grep -Eq "hardware not available|active=0|strict L0 allocation|state_attach_failed" "$EVIDENCE_LOG"; then
        echo "✗ L0 proof failed: hardware fallback or strict allocation failure detected."
        return 1
    fi
    if ! grep -Eq "\[ForL0-HotCache\] engine_start .*active=1" "$EVIDENCE_LOG"; then
        echo "✗ L0 proof failed: no active engine_start record found."
        return 1
    fi
    if ! grep -Eq "\[ForL0-HotCache\] state_attach " "$EVIDENCE_LOG"; then
        echo "✗ L0 proof failed: no eligible state was attached to the L0 pool."
        return 1
    fi
    echo "✓ L0 proof passed: active native L0 engine records captured."
}

on_benchmark_exit() {
    local status=$?
    if [[ "$CLEANUP_ON_EXIT" == "true" && -z "$EVIDENCE_LOG" ]]; then
        capture_taskmanager_evidence || true
    fi
    if [[ "$CLEANUP_ON_EXIT" == "true" && "$status" -ne 0 ]]; then
        echo "[cleanup] Benchmark command exited with status ${status}; canceling orphan Flink jobs."
        write_failed_marker "$status"
        cancel_running_jobs
    fi
    return "$status"
}
