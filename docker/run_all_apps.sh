#!/usr/bin/env bash
################################################################################
#  ForL0 全量应用一键运行脚本
#
#  默认行为:
#    1. 如果尚未部署，先执行 server_setup.sh
#    2. 确保 Docker Flink 集群已启动
#    3. 运行 benchmark/scripts/run_benchmark.py --test all --backend all
#
#  用法:
#    ./run_all_apps.sh
#    ./run_all_apps.sh --flink-home /path/to/flink
#    ./run_all_apps.sh --profile cpu
#    ./run_all_apps.sh --backend forl0
#    ./run_all_apps.sh --test wordcount
################################################################################

set -euo pipefail
cd "$(dirname "$0")"

REPO_ROOT="$(cd .. && pwd)"
FLINK_DIR="${FLINK_HOME:-}"
PROFILE_MODE=""
BACKEND="all"
TEST_NAME="all"
EXTRA_ARGS=()

usage() {
    cat <<'EOF'
用法:
  ./run_all_apps.sh [选项]

选项:
  --flink-home PATH     Flink 安装目录；不传则复用 server_setup.sh 的自动探测
  --profile MODE        透传给 run_benchmark.py，例如 cpu / cache / uarch / memory / hotspots
  --backend NAME        默认 all，可选 hashmap / forl0 / all
  --test NAME           默认 all，可选 unittest / wordcount / nexmark / client_usecase / benchset / all
  -h, --help            显示帮助
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --flink-home)
            FLINK_DIR="$2"
            shift 2
            ;;
        --profile)
            PROFILE_MODE="$2"
            shift 2
            ;;
        --backend)
            BACKEND="$2"
            shift 2
            ;;
        --test)
            TEST_NAME="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            EXTRA_ARGS+=("$1")
            shift
            ;;
    esac
done

if [[ -f "${REPO_ROOT}/docker/forl0-local.env" ]]; then
    # shellcheck disable=SC1091
    source "${REPO_ROOT}/docker/forl0-local.env"
fi

if [[ -n "$FLINK_DIR" ]]; then
    export FLINK_HOME="$FLINK_DIR"
fi

if [[ ! -f "${REPO_ROOT}/docker/forl0-local.env" ]]; then
    echo "[1/3] 未发现部署环境，先执行 server_setup.sh"
    if [[ -n "${FLINK_HOME:-}" ]]; then
        bash "${REPO_ROOT}/docker/server_setup.sh" --flink-home "$FLINK_HOME"
    else
        bash "${REPO_ROOT}/docker/server_setup.sh"
    fi
    # shellcheck disable=SC1091
    source "${REPO_ROOT}/docker/forl0-local.env"
fi

if ! curl -sf http://localhost:8081/overview >/dev/null 2>&1; then
    echo "[2/3] Flink 集群未运行，启动 docker_run.sh"
    # shellcheck disable=SC1091
    source "${REPO_ROOT}/docker/forl0-local.env"
    bash "${REPO_ROOT}/docker/docker_run.sh" start
else
    echo "[2/3] Flink 集群已就绪"
fi

echo "[3/3] 开始运行 benchmark/scripts/run_benchmark.py"

cmd=(python3 "${REPO_ROOT}/benchmark/scripts/run_benchmark.py" --test "$TEST_NAME" --backend "$BACKEND")
if [[ -n "$PROFILE_MODE" ]]; then
    cmd+=(--profile "$PROFILE_MODE")
fi
if [[ ${#EXTRA_ARGS[@]} -gt 0 ]]; then
    cmd+=("${EXTRA_ARGS[@]}")
fi

export REPO_ROOT
echo "Command: ${cmd[*]}"
"${cmd[@]}"