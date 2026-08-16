#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TEST_TMP="$(mktemp -d /tmp/forl0-runtime-sync.XXXXXX)"
cleanup() {
    rm -rf "$TEST_TMP"
}
trap cleanup EXIT

# Load only the two pure synchronization functions. Sourcing the complete app
# would intentionally start its preflight/experiment control flow.
awk '/^copy_file_unless_same\(\)/,/^}/' "$REPO_ROOT/forl0-offline-app.sh" \
    > "$TEST_TMP/runtime_sync_functions.sh"
awk '/^sync_runtime_control_plane\(\)/,/^}/' "$REPO_ROOT/forl0-offline-app.sh" \
    >> "$TEST_TMP/runtime_sync_functions.sh"
source "$TEST_TMP/runtime_sync_functions.sh"

runtime_root="$TEST_TMP/repository"
mkdir -p "$runtime_root/docker/deploy/nexmark-flink/lib" "$TEST_TMP/flink/lib"
top_jar="$runtime_root/docker/deploy/nexmark-flink-0.3-SNAPSHOT.jar"
driver_jar="$runtime_root/docker/deploy/nexmark-flink/lib/nexmark-flink-0.3-SNAPSHOT.jar"
printf 'current-java8-jar\n' > "$top_jar"
printf 'stale-driver-jar\n' > "$driver_jar"
printf 'obsolete-driver-jar\n' > "$runtime_root/docker/deploy/nexmark-flink/lib/nexmark-flink-old.jar"

# Exact regression from campaign 20260816_110145: repository mode makes the
# control, app, and runtime roots identical. The top-level copy must be a no-op,
# while the driver distribution still receives the current JAR.
CONTROL_ROOT="$runtime_root"
APP_ROOT="$runtime_root"
RUNTIME_ROOT="$runtime_root"
FLINK_DIR="$TEST_TMP/flink"
sync_runtime_control_plane

grep -Fqx 'current-java8-jar' "$top_jar"
cmp -s "$top_jar" "$driver_jar"
[[ ! -e "$runtime_root/docker/deploy/nexmark-flink/lib/nexmark-flink-old.jar" ]]

# Aliased source/destination paths must also be recognized as the same file.
copy_file_unless_same "$top_jar" "$runtime_root/docker/deploy/./$(basename "$top_jar")"

echo "runtime control sync tests passed"
