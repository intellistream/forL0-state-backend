#!/usr/bin/env bash
if [ -z "${BASH_VERSION:-}" ]; then exec bash "$0" "$@"; fi
set -euo pipefail
PACKAGE=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
if [[ "$(uname -s)" != Linux || "$(uname -m)" != aarch64 ]]; then
    echo 'ERROR: 此离线包适用于 Linux ARM64（与验证裸机相同的架构）。' >&2
    exit 2
fi
ARCHIVE_SHA=$(cat "$PACKAGE/payload/archive.sha256")
CACHE_ROOT="${TMPDIR:-/tmp}/briskstate-l0-$(id -u)-${ARCHIVE_SHA:0:16}"
# Flink expands JVM option strings; keep its installation path free of spaces.
if [[ "$CACHE_ROOT" == *' '* ]]; then CACHE_ROOT="/tmp/briskstate-l0-$(id -u)-${ARCHIVE_SHA:0:16}"; fi
mkdir -p "$CACHE_ROOT"
exec 9>"$CACHE_ROOT/install.lock"
flock 9
echo '[offline] 校验离线依赖分片（不联网）'
(cd "$PACKAGE/payload" && sha256sum -c SHA256SUMS)
if [[ ! -f "$CACHE_ROOT/READY" || "$(cat "$CACHE_ROOT/READY")" != "$ARCHIVE_SHA" ]]; then
    STAGE=$(mktemp -d "$CACHE_ROOT/extract.XXXXXX")
    trap 'rm -rf -- "${STAGE:-}"' EXIT
    cat "$PACKAGE"/payload/runtime.tar.gz.part* > "$STAGE/runtime.tar.gz"
    ACTUAL=$(sha256sum "$STAGE/runtime.tar.gz"); ACTUAL=${ACTUAL%% *}
    [[ "$ACTUAL" == "$ARCHIVE_SHA" ]] || { echo 'ERROR: archive checksum mismatch' >&2; exit 3; }
    tar -xzf "$STAGE/runtime.tar.gz" -C "$STAGE"
    rm -rf "$CACHE_ROOT/runtime"
    mv "$STAGE/runtime" "$CACHE_ROOT/runtime"
    printf '%s\n' "$ARCHIVE_SHA" > "$CACHE_ROOT/READY"
    rm -rf "$STAGE"
    trap - EXIT
fi
flock -u 9
exec 9>&-
RUNTIME="$CACHE_ROOT/runtime"
"$RUNTIME/python/bin/python3" -I -c '
import hashlib,json,sys
from pathlib import Path
r=Path(sys.argv[1]);m=json.loads((r/"MANIFEST.json").read_text())
for n,h in m.items():
 p=r/n
 if not p.is_file() or hashlib.sha256(p.read_bytes()).hexdigest()!=h:raise SystemExit("Runtime checksum failed: "+n)
print("[offline] Java/Flink/Python/作业/后端校验通过")
' "$RUNTIME"
DEFAULT_RESULTS="$PACKAGE/results/$(date +%Y%m%d-%H%M%S)-$$"
mkdir -p "$(dirname "$DEFAULT_RESULTS")"
exec "$RUNTIME/python/bin/python3" -I "$PACKAGE/scripts/run_benchset.py" \
    --runtime "$RUNTIME" --results "$DEFAULT_RESULTS" "$@"
