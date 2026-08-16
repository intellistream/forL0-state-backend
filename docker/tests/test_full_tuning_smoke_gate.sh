#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TEST_TMP="$(mktemp -d /tmp/forl0-full-gate.XXXXXX)"
cleanup() {
    rm -rf "$TEST_TMP"
}
trap cleanup EXIT

cp "$REPO_ROOT/reproduce-all" "$TEST_TMP/reproduce-all"
mkdir -p "$TEST_TMP/docker/lib" "$TEST_TMP/benchmark/scripts"
cp "$REPO_ROOT/docker/lib/result_layout.sh" "$TEST_TMP/docker/lib/"
cp "$REPO_ROOT/docker/lib/l0_detector.sh" "$TEST_TMP/docker/lib/"
cp "$REPO_ROOT/docker/lib/benchmark_python.sh" "$TEST_TMP/docker/lib/"
mkdir -p "$TEST_TMP/tools/python/bin"
cat > "$TEST_TMP/tools/python/bin/python3" <<'EOF'
#!/usr/bin/env bash
# Executable is not sufficient: this fixture represents the server's portable
# Python that cannot import yaml.
exit 1
EOF
chmod +x "$TEST_TMP/tools/python/bin/python3"
cat > "$TEST_TMP/docker/docker_run.sh" <<'EOF'
#!/usr/bin/env bash
[[ "$1" == "stop" ]]
printf 'stop\n' >> "$(cd "$(dirname "$0")/.." && pwd)/trace"
EOF

cat > "$TEST_TMP/benchmark/scripts/collect_profile.py" <<'PY'
import argparse, json
from pathlib import Path
p=argparse.ArgumentParser(); p.add_argument('--project-root'); p.add_argument('--output-dir'); p.add_argument('--simulate', action='store_true')
a=p.parse_args(); out=Path(a.output_dir); out.mkdir(parents=True, exist_ok=True)
(out/'profile_manifest.json').write_text(json.dumps({'status':'complete'}))
with (Path(a.project_root)/'trace').open('a') as f: f.write('profile\n')
PY
cat > "$TEST_TMP/benchmark/scripts/tuning_runner.py" <<'PY'
import argparse
from pathlib import Path
p=argparse.ArgumentParser(); p.add_argument('--project-root'); p.add_argument('--output-dir'); p.add_argument('--mode')
a=p.parse_args()
with (Path(a.project_root)/'trace').open('a') as f: f.write('tuning\n')
PY
cat > "$TEST_TMP/reproduce-smoke" <<'EOF'
#!/usr/bin/env bash
printf 'smoke\n' >> "$(cd "$(dirname "$0")" && pwd)/trace"
mkdir -p "$FORL0_RESULTS_DIR"
EOF
chmod +x "$TEST_TMP/reproduce-all" "$TEST_TMP/reproduce-smoke" "$TEST_TMP/docker/docker_run.sh"

FORL0_RESULTS_BASE="$TEST_TMP/results" FORL0_RUN_ID=full_gate \
    "$TEST_TMP/reproduce-all" --full --foreground

[[ "$(tr '\n' ' ' < "$TEST_TMP/trace")" == "stop profile smoke tuning " ]]
[[ -f "$TEST_TMP/results/tuning/full_gate/smoke/PASSED" ]]

# A failed cached profile must never bypass real-hardware recollection.
: > "$TEST_TMP/trace"
mkdir -p "$TEST_TMP/results/tuning/full_retry/profile"
printf '{"status":"failed"}\n' \
    > "$TEST_TMP/results/tuning/full_retry/profile/profile_manifest.json"
FORL0_RESULTS_BASE="$TEST_TMP/results" FORL0_RUN_ID=full_retry \
    "$TEST_TMP/reproduce-all" --full --foreground
[[ "$(tr '\n' ' ' < "$TEST_TMP/trace")" == "stop profile smoke tuning " ]]
grep -q '"status": "complete"' \
    "$TEST_TMP/results/tuning/full_retry/profile/profile_manifest.json"
echo "full tuning smoke gate tests passed"
