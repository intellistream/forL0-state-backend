#!/usr/bin/env bash
set -euo pipefail
RUN_DIR=$1
RESULT_FILE=$2
PACKAGE=$3
CACHE_ROOT=$4
shift 4
BOOTSTRAP_STAGE='校验离线依赖'
worker_exit() {
    result_code=$?
    if ((result_code != 0)); then
        if ((result_code == 130 || result_code == 143)); then
            final_state='stopped'
            final_message='用户已停止离线环境准备'
        else
            final_state='failed'
            final_message='离线环境准备失败'
        fi
        printf '状态：%s。\n阶段：%s\n退出码：%s。\n诊断日志：%s/console.log\n' \
            "$final_message" "$BOOTSTRAP_STAGE" "$result_code" "$RUN_DIR" > "$RUN_DIR/status.txt"
        printf '{"schema_version":2,"status":"%s","error":{"stage":"bootstrap","message":"%s","exit_code":%s},"runs":[]}\n' \
            "$final_state" "$final_message" "$result_code" > "$RESULT_FILE.tmp"
        mv "$RESULT_FILE.tmp" "$RESULT_FILE"
    fi
}
trap worker_exit EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
export BRISKSTATE_RUN_DIR="$RUN_DIR" BRISKSTATE_RESULT_FILE="$RESULT_FILE"
exec 9>"$CACHE_ROOT/install.lock"
flock 9
ARCHIVE_SHA=$(cat "$PACKAGE/payload/archive.sha256")
printf '状态：正在校验离线依赖，无需联网。\n' > "$RUN_DIR/status.txt"
(cd "$PACKAGE/payload" && sha256sum -c SHA256SUMS)
if [[ ! -f "$CACHE_ROOT/READY" || "$(cat "$CACHE_ROOT/READY")" != "$ARCHIVE_SHA" ]]; then
    BOOTSTRAP_STAGE='解包离线环境'
    printf '状态：首次解包离线环境，请稍候。\n' > "$RUN_DIR/status.txt"
    STAGE=$(mktemp -d "$CACHE_ROOT/extract.XXXXXX")
    cat "$PACKAGE"/payload/runtime.tar.gz.part* > "$STAGE/runtime.tar.gz"
    ACTUAL=$(sha256sum "$STAGE/runtime.tar.gz"); ACTUAL=${ACTUAL%% *}
    [[ "$ACTUAL" == "$ARCHIVE_SHA" ]] || { echo '离线依赖整体校验失败。' >&2; exit 3; }
    tar -xzf "$STAGE/runtime.tar.gz" -C "$STAGE"
    rm -rf "$CACHE_ROOT/runtime"
    mv "$STAGE/runtime" "$CACHE_ROOT/runtime"
    printf '%s\n' "$ARCHIVE_SHA" > "$CACHE_ROOT/READY"
    rm -rf "$STAGE"
fi
flock -u 9
exec 9>&-
BOOTSTRAP_STAGE='校验Java、Flink与作业'
printf '状态：正在校验Java、Flink和作业文件。\n' > "$RUN_DIR/status.txt"
RUNTIME="$CACHE_ROOT/runtime"
"$RUNTIME/python/bin/python3" -I -c '
import hashlib,json,sys
from pathlib import Path
runtime=Path(sys.argv[1])
for name,expected in json.loads((runtime/"MANIFEST.json").read_text()).items():
 path=runtime/name
 if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest()!=expected:
  raise SystemExit("运行时文件校验失败："+name)
' "$RUNTIME"
BOOTSTRAP_STAGE='启动实验控制程序'
exec "$RUNTIME/python/bin/python3" -I "$PACKAGE/scripts/run_benchset.py" \
    --runtime "$RUNTIME" --work-dir "$RUN_DIR" --results "$RESULT_FILE" "$@"
