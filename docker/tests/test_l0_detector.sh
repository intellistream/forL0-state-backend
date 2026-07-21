#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../lib/l0_detector.sh"

TEST_ROOT="$(mktemp -d /tmp/forl0-l0-detector-test.XXXXXX)"
mkdir -p "$TEST_ROOT/empty" "$TEST_ROOT/search"
: > "$TEST_ROOT/empty-ldconfig.txt"
CASE_NUMBER=0

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

assert_eq() {
    local expected="$1"
    local actual="$2"
    local label="$3"
    [[ "$expected" == "$actual" ]] || fail "$label: expected '$expected', got '$actual'"
}

reset_detector_env() {
    CASE_NUMBER=$((CASE_NUMBER + 1))
    unset L0_DEVICE_HOST_PATH L0_MEMPOOL_LIB_HOST_PATH NUMA_LIB_HOST_PATH
    unset LD_LIBRARY_PATH
    export FORL0_SYSTEM_ROOT="$TEST_ROOT/system-$CASE_NUMBER"
    mkdir -p "$FORL0_SYSTEM_ROOT"
    export FORL0_LDCONFIG_CACHE_FILE="$TEST_ROOT/empty-ldconfig.txt"
    export FORL0_L0_SEARCH_ROOTS="$TEST_ROOT/empty"
    export FORL0_NUMA_SEARCH_ROOTS="$TEST_ROOT/empty"
    export FORL0_MULTIARCH="aarch64-linux-gnu"
}

echo "[1/6] explicit overrides and device"
reset_detector_env
mkdir -p "$TEST_ROOT/explicit"
: > "$TEST_ROOT/explicit/libl0mempool.so"
: > "$TEST_ROOT/explicit/libnuma.so.1"
: > "$TEST_ROOT/explicit/l0-device"
export L0_MEMPOOL_LIB_HOST_PATH="$TEST_ROOT/explicit/libl0mempool.so"
export NUMA_LIB_HOST_PATH="$TEST_ROOT/explicit/libnuma.so.1"
export L0_DEVICE_HOST_PATH="$TEST_ROOT/explicit/l0-device"
forl0_detect_l0_environment
assert_eq "$TEST_ROOT/explicit/libl0mempool.so" "$FORL0_L0_LIBRARY_PATH" "explicit L0 path"
assert_eq "explicit override" "$FORL0_L0_LIBRARY_SOURCE" "explicit source"
assert_eq "$TEST_ROOT/explicit/l0-device" "$FORL0_L0_DEVICE_PATH" "explicit device"

echo "[2/6] LD_LIBRARY_PATH"
reset_detector_env
mkdir -p "$TEST_ROOT/ld-library-path"
: > "$TEST_ROOT/ld-library-path/libl0mempool.so"
export LD_LIBRARY_PATH="$TEST_ROOT/ld-library-path"
forl0_detect_l0_environment || true
assert_eq "$TEST_ROOT/ld-library-path/libl0mempool.so" "$FORL0_L0_LIBRARY_PATH" "LD_LIBRARY_PATH result"
assert_eq "LD_LIBRARY_PATH" "$FORL0_L0_LIBRARY_SOURCE" "LD_LIBRARY_PATH source"

echo "[3/6] ldconfig cache"
reset_detector_env
mkdir -p "$TEST_ROOT/ldconfig"
: > "$TEST_ROOT/ldconfig/libl0mempool.so.1"
printf 'libl0mempool.so (libc6,AArch64) => %s\n' "$TEST_ROOT/ldconfig/libl0mempool.so.1" > "$TEST_ROOT/ldconfig.txt"
export FORL0_LDCONFIG_CACHE_FILE="$TEST_ROOT/ldconfig.txt"
forl0_detect_l0_environment || true
assert_eq "$TEST_ROOT/ldconfig/libl0mempool.so.1" "$FORL0_L0_LIBRARY_PATH" "ldconfig result"
assert_eq "ldconfig cache" "$FORL0_L0_LIBRARY_SOURCE" "ldconfig source"

echo "[4/6] multiarch directory"
reset_detector_env
mkdir -p "$FORL0_SYSTEM_ROOT/usr/lib/aarch64-linux-gnu"
: > "$FORL0_SYSTEM_ROOT/usr/lib/aarch64-linux-gnu/libl0mempool.so"
forl0_detect_l0_environment || true
assert_eq "$FORL0_SYSTEM_ROOT/usr/lib/aarch64-linux-gnu/libl0mempool.so" "$FORL0_L0_LIBRARY_PATH" "multiarch result"
assert_eq "known library directory" "$FORL0_L0_LIBRARY_SOURCE" "multiarch source"

echo "[5/6] bounded vendor search"
reset_detector_env
mkdir -p "$TEST_ROOT/search/vendor/runtime/lib"
: > "$TEST_ROOT/search/vendor/runtime/lib/libl0mempool.so"
export FORL0_L0_SEARCH_ROOTS="$TEST_ROOT/search"
forl0_detect_l0_environment || true
assert_eq "$TEST_ROOT/search/vendor/runtime/lib/libl0mempool.so" "$FORL0_L0_LIBRARY_PATH" "bounded search result"
assert_eq "bounded search under $TEST_ROOT/search" "$FORL0_L0_LIBRARY_SOURCE" "bounded search source"

echo "[6/6] invalid explicit path does not silently fall back"
reset_detector_env
export L0_MEMPOOL_LIB_HOST_PATH="$TEST_ROOT/missing/libl0mempool.so"
export LD_LIBRARY_PATH="$TEST_ROOT/ld-library-path"
forl0_detect_l0_environment || true
assert_eq "" "$FORL0_L0_LIBRARY_PATH" "invalid explicit result"

echo "PASS: L0 detector fixtures ($TEST_ROOT)"
