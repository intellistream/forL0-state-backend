#!/usr/bin/env bash
# Run inside the container after PMU is enabled, during one steady workload.
# Keep the CLI's raw report and vendor archive; plot only its ALL aggregate.
set -euo pipefail
runtime=${BRISKSTATE_RUNTIME:-/root/briskstate-runtime}
devkit=${BRISKSTATE_DEVKIT:-/root/opt/DevKit-CLI-26.2.RC1-Linux-Kunpeng/devkit}
output=${1:?Usage: collect_topdown.sh OUTPUT_DIRECTORY [SECONDS]}
duration=${2:-30}
mkdir -p "$output"
output=$(cd "$output" && pwd)
python3 /root/forL0-state-backend/benchmark/scripts/probe_pmu.py > "$output/pmu-capability.json"
python3 - "$output/pmu-capability.json" <<'PY'
import json,sys
p=json.load(open(sys.argv[1]))
if not p.get('user_cycles_open',{}).get('success'):
    sys.exit('PMU unavailable. Fix host perf policy/capabilities first; see docs/BriskState_PMU_Access.md.')
PY
pids=()
for role in tm1 tm2; do
  pidfile=("$runtime/$role/pid/"*.pid)
  [[ ${#pidfile[@]} == 1 && -f ${pidfile[0]} ]]
  pids+=("$(cat "${pidfile[0]}")")
done
printf '%s\n' "${pids[@]}" > "$output/taskmanager-pids.txt"
cd "$output"
"$devkit" tuner top-down -L 0 -d "$duration" -p "${pids[0]},${pids[1]}" > report.txt 2>&1
