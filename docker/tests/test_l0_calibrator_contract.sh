#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TEST_TMP="$(mktemp -d /tmp/forl0-l0-calibrator.XXXXXX)"
cleanup() {
    rm -rf "$TEST_TMP"
}
trap cleanup EXIT

cat > "$TEST_TMP/fake_l0.cpp" <<'CPP'
#include <cstdlib>
#include <cstddef>
#include <csignal>

struct Tuner { size_t capacity; };

extern "C" int cache_tuner_init(void** output, size_t capacity) {
    if (capacity < (64ULL << 20)) return 1;
    *output = new Tuner{capacity};
    return 0;
}
extern "C" int cache_tuner_destroy(void* tuner) {
    delete static_cast<Tuner*>(tuner);
    return 0;
}
extern "C" void* l0_mem_alloc(void* raw_tuner, size_t bytes) {
    Tuner* tuner = static_cast<Tuner*>(raw_tuner);
    if (!tuner || bytes > tuner->capacity) return nullptr;
    size_t crash_limit_mb = 16;
    if (const char* configured = std::getenv("FAKE_L0_CRASH_LIMIT_MB")) {
        crash_limit_mb = std::strtoull(configured, nullptr, 10);
    }
    if (bytes > (crash_limit_mb << 20)) std::raise(SIGSEGV);
    void* memory = nullptr;
    return posix_memalign(&memory, 64, bytes) == 0 ? memory : nullptr;
}
extern "C" int l0_mem_free(void*, void* memory) {
    std::free(memory);
    return 0;
}
CPP

g++ -O2 -std=c++17 -shared -fPIC "$TEST_TMP/fake_l0.cpp" -o "$TEST_TMP/libl0mempool.so"
g++ -O2 -std=c++17 -pthread "$REPO_ROOT/benchmark/native/l0_calibrate.cpp" \
    -ldl -o "$TEST_TMP/l0_calibrate"

# The fake vendor runtime deliberately rejects a 1 MiB tuner. Calibration must
# keep the 1 MiB measured allocation separate from the 64 MiB process pool.
"$TEST_TMP/l0_calibrate" "$TEST_TMP/libl0mempool.so" "$TEST_TMP/result.json" \
    1048576 contract-test 67108864

# Reproduce the old bug explicitly: the same 1 MiB allocation used as tuner
# capacity must be rejected at the vendor-init stage, not misreported.
"$TEST_TMP/l0_calibrate" "$TEST_TMP/libl0mempool.so" "$TEST_TMP/old-shape.json" \
    1048576 contract-test

python3 - "$TEST_TMP/result.json" "$TEST_TMP/old-shape.json" <<'PY'
import json
import sys

data = json.load(open(sys.argv[1], encoding="utf-8"))
assert data["status"] == "complete", data
assert data["requested_bytes"] == 1 << 20, data
assert data["tuner_capacity_bytes"] == 64 << 20, data
assert data["allocation_bytes"] == 1 << 20, data
assert data["l0"], data

old = json.load(open(sys.argv[2], encoding="utf-8"))
assert old["status"] == "failed", old
assert old["failure_stage"] == "cache_tuner_init", old
assert old["cache_tuner_init_rc"] == 1, old
PY

# A vendor crash must leave an exact last-stage breadcrumb for diagnosis.
python3 - "$TEST_TMP/l0_calibrate" "$TEST_TMP/libl0mempool.so" \
    "$TEST_TMP/crash.json" <<'PY'
import signal
import subprocess
import sys
from pathlib import Path

completed = subprocess.run(
    [sys.argv[1], sys.argv[2], sys.argv[3], str(64 << 20), "contract-test", str(64 << 20)],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
)
assert completed.returncode == -signal.SIGSEGV, completed
assert Path(sys.argv[3] + ".stage").read_text(encoding="utf-8").strip() == "l0_mem_alloc"
PY

# Exercise the Python orchestrator with the fake ABI too. Hide the host taskset
# affinity policy so this remains deterministic inside constrained CI cpusets.
mkdir -p "$TEST_TMP/bin"
cat > "$TEST_TMP/bin/taskset" <<'SH'
#!/usr/bin/env bash
shift 2
exec "$@"
SH
chmod +x "$TEST_TMP/bin/taskset"
PATH="$TEST_TMP/bin:$PATH" \
FORL0_L0_LIBRARY_PATH="$TEST_TMP/libl0mempool.so" \
FORL0_L0_DEVICE_PATH=/dev/null \
FORL0_EXPECTED_TASKMANAGERS=2 \
FORL0_PROFILE_CLUSTER_CLEANED=true \
    python3 "$REPO_ROOT/benchmark/scripts/collect_profile.py" \
        --project-root "$REPO_ROOT" --output-dir "$TEST_TMP/profile" --timeout 60

python3 - "$TEST_TMP/profile/profile_manifest.json" <<'PY'
import json
import sys

data = json.load(open(sys.argv[1], encoding="utf-8"))
assert data["status"] == "complete", data
assert data["l0_successful_probes"] == 4, data
sizes = [probe["allocation_mb"] for probe in data["probes"] if probe["kind"] == "l0"]
assert sizes == [1, 2, 4, 6], sizes
for probe in data["probes"]:
    if probe["kind"] != "l0":
        continue
    payload = json.load(open(sys.argv[1].replace("profile_manifest.json", probe["file"]),
                             encoding="utf-8"))
    assert payload["tuner_capacity_bytes"] == 64 << 20, payload
    assert probe["tuner_capacity_mb"] == 64, probe
    assert payload["requested_bytes"] == probe["allocation_mb"] << 20, payload
    assert payload["dense_measurement_bytes"] == 1 << 20, payload
    assert payload["access_pattern"] == "dense-up-to-1mb-plus-hotset-sparse", payload
    pressure = payload["l0_hotset_pressure_curve"]
    assert pressure[-1]["requested_active_bytes"] == probe["allocation_mb"] << 20, pressure
parallel = data["parallel_instance_probe"]
assert parallel["expected_instances"] == 2, parallel
assert parallel["allocation_mb_per_instance"] == 6, parallel
assert parallel["tuner_capacity_mb_per_instance"] == 64, parallel
assert parallel["status"] == "complete", parallel
PY

# A partial staged curve is diagnostic evidence, not a complete calibration.
set +e
PATH="$TEST_TMP/bin:$PATH" \
FAKE_L0_CRASH_LIMIT_MB=4 \
FORL0_L0_LIBRARY_PATH="$TEST_TMP/libl0mempool.so" \
FORL0_L0_DEVICE_PATH=/dev/null \
FORL0_EXPECTED_TASKMANAGERS=2 \
FORL0_PROFILE_CLUSTER_CLEANED=true \
    python3 "$REPO_ROOT/benchmark/scripts/collect_profile.py" \
        --project-root "$REPO_ROOT" --output-dir "$TEST_TMP/partial-profile" --timeout 60
partial_rc=$?
set -e
[[ "$partial_rc" -ne 0 ]]

python3 - "$TEST_TMP/partial-profile/profile_manifest.json" <<'PY'
import json
import sys
from pathlib import Path

manifest_path = Path(sys.argv[1])
data = json.load(open(manifest_path, encoding="utf-8"))
assert data["status"] == "failed", data
assert data["l0_successful_probes"] == 3, data
assert data["reason"] == "staged L0 calibration incomplete: 3/4 probes succeeded", data
failed = json.load(open(manifest_path.parent / "l0_calibration_6mb.json", encoding="utf-8"))
assert failed["signal"] == 11, failed
assert failed["failure_stage"] == "l0_mem_alloc", failed
PY

echo "L0 calibrator contract tests passed"
