#!/usr/bin/env bash
if [ -z "${BASH_VERSION:-}" ]; then exec bash "$0" "$@"; fi
set -euo pipefail
PACKAGE=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
if [[ "${1:-}" == --help || "${1:-}" == -h ]]; then
    cat <<'USAGE'
用法：./run-l0-benchset [测试参数]
自动后台执行，退出SSH不影响测试；结果自动保存为一个JSON文件。

  --status                   查看当前进度或上次执行结果
  --stop                     停止正在执行的测试，保留已完成数据
  --diagnose                 只自动检查L0设备、驱动和SDK，不启动Flink
  --smoke --workloads wc      仅短测WC，验证机器是否可运行
  --repeats N                每个条件运行次数（默认1次）
  --workloads wc,fd          只运行指定workload
  --arms heap,brisk_off      只运行指定组别
  --results /path/run.json   指定结果JSON路径，须为尚不存在的文件

默认：七项workload；Heap、BriskState无L0、共享2/4/8/16/24/32MiB六档L0。
每TM4核32GiB；并行度8；每条件1次，初始化后预热60秒、测量60秒；checkpoint关闭。
USAGE
    exit 0
fi
if [[ "$(uname -s)" != Linux || "$(uname -m)" != aarch64 ]]; then
    printf '无法启动：需要Linux ARM64，当前为%s/%s。请在指定L0机器上运行。\n' "$(uname -s)" "$(uname -m)" >&2
    exit 2
fi
if [[ ! -r "$PACKAGE/payload/archive.sha256" ]]; then
    printf '无法启动：离线文件不完整，缺少%s/payload/archive.sha256。请重新复制完整分支。\n' "$PACKAGE" >&2
    exit 3
fi
ARCHIVE_SHA=$(cat "$PACKAGE/payload/archive.sha256")
CACHE_ROOT="${TMPDIR:-/tmp}/briskstate-l0-$(id -u)-${ARCHIVE_SHA:0:16}"
if [[ "$CACHE_ROOT" == *' '* ]]; then CACHE_ROOT="/tmp/briskstate-l0-$(id -u)-${ARCHIVE_SHA:0:16}"; fi
mkdir -p "$CACHE_ROOT/runs"
exec 8>"$CACHE_ROOT/launch.lock"
flock 8
ACTIVE_RUN=$(cat "$CACHE_ROOT/current" 2>/dev/null || true)
ACTIVE_PID=''
if [[ -n "$ACTIVE_RUN" ]]; then ACTIVE_PID=$(cat "$ACTIVE_RUN/pid" 2>/dev/null || true); fi
running=false
if [[ "$ACTIVE_PID" =~ ^[0-9]+$ ]] && kill -0 "$ACTIVE_PID" 2>/dev/null; then
    if tr '\0' '\n' < "/proc/$ACTIVE_PID/cmdline" | grep -Fx -- "$ACTIVE_RUN" >/dev/null; then running=true; fi
fi
show_status() {
    if [[ -z "$ACTIVE_RUN" ]]; then echo '尚未启动过测试。'; return; fi
    if $running; then echo "测试正在后台运行（进程号：$ACTIVE_PID）。"; else echo '上次后台任务已结束。'; fi
    cat "$ACTIVE_RUN/status.txt" 2>/dev/null || true
    printf '结果文件：%s\n' "$(cat "$ACTIVE_RUN/result-path")"
    printf '诊断日志：%s/console.log\n' "$ACTIVE_RUN"
}
case "${1:-}" in
    --status) show_status; exit 0 ;;
    --stop)
        if $running; then
            kill -TERM "$ACTIVE_PID"
            echo '已请求停止。正在关闭本次JM/TM并保存已完成数据，请稍后用 --status 查看。'
        else show_status; fi
        exit 0 ;;
esac
if $running; then
    echo '已有测试正在运行，本次不会重复启动。'
    show_status
    exit 0
fi
RUN_ID=$(date +%Y%m%d-%H%M%S)-$$
RUN_DIR="$CACHE_ROOT/runs/$RUN_ID"
RESULT_FILE="$PACKAGE/results/$RUN_ID.json"
FORWARD=()
while (($#)); do
    case "$1" in
        --results)
            [[ $# -ge 2 ]] || { echo '参数错误：--results 后面需要JSON文件路径。' >&2; exit 2; }
            RESULT_FILE=$2; shift 2 ;;
        --results=*) RESULT_FILE=${1#*=}; shift ;;
        *) FORWARD+=("$1"); shift ;;
    esac
done
[[ "$RESULT_FILE" == /* ]] || RESULT_FILE="$PWD/$RESULT_FILE"
[[ "$RESULT_FILE" == *.json ]] || { echo '参数错误：结果文件名需要以 .json 结尾。' >&2; exit 2; }
[[ ! -e "$RESULT_FILE" ]] || { echo '无法启动：结果文件已存在，请换一个文件名。' >&2; exit 2; }
mkdir -p "$RUN_DIR" "$(dirname "$RESULT_FILE")"
printf '%s\n' "$RESULT_FILE" > "$RUN_DIR/result-path"
printf '状态：准备离线运行环境。\n' > "$RUN_DIR/status.txt"
(set -o noclobber; printf '{"schema_version":2,"status":"preparing","runs":[]}\n' > "$RESULT_FILE")
printf '%s\n' "$RUN_DIR" > "$CACHE_ROOT/current"
nohup setsid bash "$PACKAGE/scripts/worker.sh" "$RUN_DIR" "$RESULT_FILE" "$PACKAGE" "$CACHE_ROOT" \
    "${FORWARD[@]}" </dev/null >"$RUN_DIR/console.log" 2>&1 8>&- &
printf '%s\n' "$!" > "$RUN_DIR/pid"
printf '测试已在后台启动（进程号：%s），可以退出SSH。\n' "$!"
if [[ " ${FORWARD[*]} " == *' --diagnose '* ]]; then
    printf '本次仅检查L0设备、驱动和SDK；不启动Flink，不申请L0容量。\n'
else
    printf '先自动检查设备、驱动与SDK，再测试共享2、4、8、16、24、32 MiB；各档失败独立处理。\n'
fi
printf '查看进度：./run-l0-benchset --status\n停止测试：./run-l0-benchset --stop\n'
printf '结果文件：%s\n' "$RESULT_FILE"
