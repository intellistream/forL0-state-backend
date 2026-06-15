#!/usr/bin/env bash
################################################################################
#  ForL0 全量应用一键运行脚本
#
#  默认行为:
#    1. 如果尚未部署，先执行 server_setup.sh
#    2. 确保 Docker Flink 集群已启动
#    3. 运行 benchmark/scripts/run_benchmark.py --test apps --backend all
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
TEST_NAME="apps"
EXTRA_ARGS=()
ENABLE_PROFILE=true
BENCH_PYTHON=""

bootstrap_async_profiler() {
    local discovered_home="${ASYNC_PROFILER_HOME:-}"

    if [[ "$ENABLE_PROFILE" != "true" ]]; then
        return 0
    fi

    if [[ -n "$discovered_home" && -x "$discovered_home/bin/asprof" ]]; then
        export ASYNC_PROFILER_HOME="$discovered_home"
        echo "      ✓ 使用 ASYNC_PROFILER_HOME=$ASYNC_PROFILER_HOME"
        return 0
    fi

    for candidate in \
        "${REPO_ROOT}/tools/async-profiler" \
        "$HOME/async-profiler" \
        "$HOME/async-profiler-4.4-linux-arm64" \
        "$HOME/async-profiler-4.4-linux-x64" \
        /opt/async-profiler; do
        if [[ -x "$candidate/bin/asprof" ]]; then
            discovered_home="$candidate"
            break
        fi
    done

    if [[ -z "$discovered_home" ]]; then
        discovered_home="$(find "$HOME" -maxdepth 2 -type f -path '*/bin/asprof' 2>/dev/null | head -n 1 | xargs -r dirname | xargs -r dirname)"
    fi

    if [[ -n "$discovered_home" && -x "$discovered_home/bin/asprof" ]]; then
        export ASYNC_PROFILER_HOME="$discovered_home"
        echo "      ✓ 自动发现 async-profiler: $ASYNC_PROFILER_HOME"
    else
        echo "      ⚠ 未发现 async-profiler（CPU 火焰图将无法生成）"
    fi
}

bootstrap_benchmark_python() {
    local requirements_file="${REPO_ROOT}/benchmark/requirements.txt"
    local offline_wheels_dir=""
    local venv_dir="${REPO_ROOT}/.venv-benchmark"

    for candidate in \
        "${REPO_ROOT}/offline-packages" \
        "${REPO_ROOT}/benchmark/offline-packages"; do
        if [[ -d "$candidate" ]]; then
            offline_wheels_dir="$candidate"
            break
        fi
    done

    if ! command -v python3 >/dev/null 2>&1; then
        echo "✗ 未找到 python3，无法运行 benchmark 脚本"
        exit 1
    fi

    if [[ ! -f "$requirements_file" ]]; then
        echo "✗ 缺少 requirements 文件: $requirements_file"
        exit 1
    fi

    if [[ ! -x "$venv_dir/bin/python" ]]; then
        echo "[3/4] 创建 benchmark Python 虚拟环境..."
        python3 -m venv "$venv_dir"
    else
        echo "[3/4] 复用已有 benchmark Python 虚拟环境"
    fi

    local py_bin="$venv_dir/bin/python"
    local pip_bin="$venv_dir/bin/pip"
    local deps_ok=true

    if ! "$py_bin" - <<'PY' >/dev/null 2>&1
import yaml
import pandas
import numpy
import matplotlib
import seaborn
import jinja2
import requests
import tqdm
PY
    then
        deps_ok=false
    fi

    if [[ "$deps_ok" == "false" ]]; then
        echo "      安装 benchmark Python 依赖（优先离线）..."
        if [[ -n "$offline_wheels_dir" && -d "$offline_wheels_dir" ]]; then
            if "$pip_bin" install --no-index --find-links "$offline_wheels_dir" -r "$requirements_file"; then
                echo "      ✓ 离线依赖安装成功"
            else
                echo "      ⚠ 离线依赖安装失败，尝试在线安装"
                "$pip_bin" install -r "$requirements_file"
            fi
        else
            echo "      ⚠ 未找到离线包目录（offline-packages），尝试在线安装"
            "$pip_bin" install -r "$requirements_file"
        fi
    else
        echo "      ✓ benchmark Python 依赖已满足"
    fi

    BENCH_PYTHON="$py_bin"
}

usage() {
    cat <<'EOF'
用法:
  ./run_all_apps.sh [选项]

选项:
  --flink-home PATH     Flink 安装目录；不传则复用 server_setup.sh 的自动探测
  --profile MODE        透传给 run_benchmark.py，例如 cpu / cache / uarch / memory / hotspots
    --no-profile          关闭 profiling（默认开启 cpu 火焰图）
  --backend NAME        默认 all，可选 hashmap / forl0 / all
    --test NAME           默认 apps，可选 unittest / wordcount / nexmark / client_usecase / benchset / apps / all
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
        --no-profile)
            ENABLE_PROFILE=false
            shift
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

if [[ "$ENABLE_PROFILE" == "true" && -z "$PROFILE_MODE" ]]; then
    PROFILE_MODE="cpu"
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

bootstrap_benchmark_python
bootstrap_async_profiler

echo "[4/4] 开始运行 benchmark/scripts/run_benchmark.py"

cmd=("${BENCH_PYTHON}" "${REPO_ROOT}/benchmark/scripts/run_benchmark.py" --test "$TEST_NAME" --backend "$BACKEND")
if [[ -n "$PROFILE_MODE" ]]; then
    cmd+=(--profile "$PROFILE_MODE")
fi
if [[ ${#EXTRA_ARGS[@]} -gt 0 ]]; then
    cmd+=("${EXTRA_ARGS[@]}")
fi

export REPO_ROOT
echo "Command: ${cmd[*]}"
"${cmd[@]}"