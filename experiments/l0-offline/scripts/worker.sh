#!/usr/bin/env bash
set -euo pipefail
RUN_DIR=$1
RESULT_FILE=$2
PACKAGE=$3
CACHE_ROOT=$4
shift 4
BOOTSTRAP_STAGE='校验离线依赖'
json_quote() {
    local encoded=$1
    encoded=${encoded//\\/\\\\}
    encoded=${encoded//\"/\\\"}
    encoded=${encoded//$'\n'/\\n}
    printf '"%s"' "$encoded"
}
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
        details=$(tail -n 20 "$RUN_DIR/console.log" | LC_ALL=C sed 's/[[:cntrl:]]//g')
        error_code='BOOTSTRAP_FAILED'
        next_step='请按原始错误检查完整离线文件和运行目录；无法定位时返回此JSON。'
        owner='运行人员或管理员'
        case "$details" in
            *'Permission denied'*|*'Operation not permitted'*)
                error_code='ACCESS_DENIED'
                next_step='让管理员检查原始错误中目录的读写/执行权限及文件系统noexec设置。' ;;
            *'No space left'*)
                error_code='DISK_SPACE_LOW'
                next_step='用df -h检查运行缓存和结果路径，释放空间后重新执行。' ;;
            *'No such file'*|*'checksum'*|*'校验失败'*|*'FAILED'*)
                error_code='OFFLINE_FILES_INVALID'
                next_step='重新获取或复制完整briskstate_workspace分支，包括payload全部分片，再启动。' ;;
        esac
        if [[ "$final_state" == stopped ]]; then
            error_code='USER_STOPPED'
            next_step='用户主动停止，无需修复；需要测试时重新启动即可。'
        fi
        printf '状态：%s。\n阶段：%s\n错误编号：%s\n处理建议：%s\n处理人员：%s\n原始错误末尾：\n%s\n诊断日志：%s/console.log\n' \
            "$final_message" "$BOOTSTRAP_STAGE" "$error_code" "$next_step" "$owner" \
            "$(printf '%s\n' "$details" | tail -n 3)" "$RUN_DIR" > "$RUN_DIR/status.txt"
        {
            printf '{"schema_version":2,"status":"%s","error":{"stage":' "$final_state"
            json_quote "$BOOTSTRAP_STAGE"
            printf ',"message":"%s","exit_code":%s,"evidence":' "$final_message" "$result_code"
            json_quote "$details"
            printf ',"guidance":{"error_code":"%s","responsible_party":' "$error_code"
            json_quote "$owner"
            printf ',"next_steps":['
            json_quote "$next_step"
            printf ']}},"runs":[]}\n'
        } > "$RESULT_FILE.tmp"
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
