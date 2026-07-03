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
#    ./run_all_apps.sh --test nexmark --scenario forl0_q5_tps_probe --backend forl0 --query q5
#    ./run_all_apps.sh --test wordcount --scenario high_cardinality --backend all
#    ./run_all_apps.sh --offline --test apps --backend all
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
OFFLINE_MODE="${FORL0_OFFLINE:-auto}"
GENERATE_REPORT=true
REPORT_ONLY=false
PREFLIGHT_ONLY=false

print_provenance() {
    echo "============================================================"
    echo "  ForL0 benchmark run context"
    echo "============================================================"
    echo "  Repo:        ${REPO_ROOT}"
    echo "  Test:        ${TEST_NAME}"
    echo "  Backend:     ${BACKEND}"
    echo "  Profile:     ${PROFILE_MODE:-disabled}"
    echo "  Offline:     ${OFFLINE_MODE}"
    if command -v git >/dev/null 2>&1 && git -C "${REPO_ROOT}" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
        echo "  GitCommit:   $(git -C "${REPO_ROOT}" rev-parse HEAD)"
    else
        echo "  GitCommit:   unavailable"
    fi
    echo "  Date:        $(date '+%Y-%m-%d %H:%M:%S %z')"
    echo ""
}

check_file() {
    local label="$1"
    local path="$2"
    if [[ -f "$path" ]]; then
        echo "      ✓ ${label}: ${path}"
        return 0
    fi
    echo "      ✗ ${label}: ${path}"
    return 1
}

check_glob() {
    local label="$1"
    local pattern="$2"
    local match
    match="$(compgen -G "$pattern" | head -n 1 || true)"
    if [[ -n "$match" && -f "$match" ]]; then
        echo "      ✓ ${label}: ${match}"
        return 0
    fi
    echo "      ✗ ${label}: ${pattern}"
    return 1
}

preflight_check() {
    local failed=0

    echo "[preflight] 检查 Flink 与 benchmark 产物"
    if [[ -z "${FLINK_HOME:-}" || ! -x "${FLINK_HOME}/bin/flink" ]]; then
        echo "      ✗ FLINK_HOME 无效或缺少 bin/flink: ${FLINK_HOME:-unset}"
        failed=1
    else
        echo "      ✓ FLINK_HOME: ${FLINK_HOME}"
    fi

    check_file "benchmark config" "${REPO_ROOT}/benchmark/config/benchmark.yaml" || failed=1
    check_file "benchmark entry" "${REPO_ROOT}/benchmark/scripts/run_benchmark.py" || failed=1
    check_file "requirements" "${REPO_ROOT}/benchmark/requirements.txt" || failed=1

    if [[ "$TEST_NAME" == "wordcount" || "$TEST_NAME" == "apps" || "$TEST_NAME" == "all" ]]; then
        check_glob "WordCount JAR" "${REPO_ROOT}/docker/deploy/wordcount-benchmark-*.jar" || failed=1
    fi
    if [[ "$TEST_NAME" == "nexmark" || "$TEST_NAME" == "apps" || "$TEST_NAME" == "all" ]]; then
        check_glob "NexMark JAR" "${REPO_ROOT}/docker/deploy/nexmark-flink-*.jar" || failed=1
    fi
    if [[ "$TEST_NAME" == "client_usecase" || "$TEST_NAME" == "apps" || "$TEST_NAME" == "all" ]]; then
        check_glob "Client usecase JAR" "${REPO_ROOT}/docker/deploy/flink-keyedcoprocessfunction-example-*-jar-with-dependencies.jar" || failed=1
    fi
    if [[ "$TEST_NAME" == "client_usecase" || "$TEST_NAME" == "apps" || "$TEST_NAME" == "all" ]]; then
        if compgen -G "${REPO_ROOT}/docker/deploy/client-drift-benchmark-*.jar" >/dev/null; then
            check_glob "Client hotspot-drift JAR" "${REPO_ROOT}/docker/deploy/client-drift-benchmark-*.jar" || failed=1
        else
            echo "      - Client hotspot-drift JAR: not required unless hotspot_drift_* scenarios are selected"
        fi
    fi

    if [[ -n "${FLINK_HOME:-}" ]]; then
        check_glob "ForL0 backend JAR in Flink lib" "${FLINK_HOME}/lib/flink-statebackend-for[Ll]0-*.jar" || failed=1
        if [[ -f "${FLINK_HOME}/native/libforl0_engine.so" ]]; then
            echo "      ✓ native library: ${FLINK_HOME}/native/libforl0_engine.so"
        elif [[ -f "${REPO_ROOT}/docker/deploy/libforl0_engine.so" ]]; then
            echo "      ✓ native library: ${REPO_ROOT}/docker/deploy/libforl0_engine.so"
        else
            echo "      ✗ native library: missing libforl0_engine.so"
            failed=1
        fi
    fi

    bootstrap_benchmark_python
    "${BENCH_PYTHON}" - <<'PY'
import os
import sys
from pathlib import Path
repo_root = Path(os.environ["REPO_ROOT"]).resolve()
sys.path.insert(0, str(repo_root / "benchmark" / "scripts"))
from utils.config import load_config
cfg = load_config()
print("      ✓ config loaded")
print("      ✓ nexmark queries:", cfg.get("nexmark", {}).get("queries", ""))
print("      ✓ client scenarios:", ",".join(s.get("name", "") for s in cfg.get("client_usecase_scenarios", [])))
PY

    if [[ "$failed" -ne 0 ]]; then
        echo "[preflight] 检查失败，请补齐上述缺失项后再运行实验。"
        exit 1
    fi
    echo "[preflight] 检查通过。"
}

bootstrap_async_profiler() {
    local discovered_home="${ASYNC_PROFILER_HOME:-}"

    if [[ "$ENABLE_PROFILE" != "true" ]]; then
        return 0
    fi

    if [[ -n "$discovered_home" && -x "$discovered_home/bin/asprof" ]]; then
        discovered_home="$(readlink -f "$discovered_home")"
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
        discovered_home="$(readlink -f "$discovered_home")"
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
        echo "[3/5] 创建 benchmark Python 虚拟环境..."
        python3 -m venv "$venv_dir"
    else
        echo "[3/5] 复用已有 benchmark Python 虚拟环境"
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
        echo "      安装 benchmark Python 依赖（离线优先）..."
        if [[ -n "$offline_wheels_dir" && -d "$offline_wheels_dir" ]]; then
            if "$pip_bin" install --no-index --find-links "$offline_wheels_dir" -r "$requirements_file"; then
                echo "      ✓ 离线依赖安装成功"
            else
                if [[ "$OFFLINE_MODE" == "true" ]]; then
                    echo "✗ 离线依赖安装失败，且当前为 --offline 模式"
                    echo "  请在联网机器执行 docker/package_offline_bundle.sh 补齐 offline-packages 后再拷贝到离线环境"
                    exit 1
                fi
                echo "      ⚠ 离线依赖安装失败，尝试在线安装"
                "$pip_bin" install -r "$requirements_file"
            fi
        else
            if [[ "$OFFLINE_MODE" == "true" ]]; then
                echo "✗ 未找到离线包目录：${REPO_ROOT}/offline-packages 或 ${REPO_ROOT}/benchmark/offline-packages"
                echo "  离线运行前请先准备 Python wheels"
                exit 1
            fi
            echo "      ⚠ 未找到离线包目录（offline-packages），尝试在线安装"
            "$pip_bin" install -r "$requirements_file"
        fi
    else
        echo "      ✓ benchmark Python 依赖已满足"
    fi

    BENCH_PYTHON="$py_bin"
}

generate_benchmark_report() {
    if [[ "$GENERATE_REPORT" != "true" ]]; then
        return 0
    fi

    echo "[5/5] 生成 benchmark 报告"
    "${BENCH_PYTHON}" "${REPO_ROOT}/benchmark/scripts/generate_report.py"
}

wait_for_flink_rest() {
    local max_attempts="${1:-30}"

    for _ in $(seq 1 "$max_attempts"); do
        if curl -sf http://localhost:8081/overview >/dev/null 2>&1; then
            return 0
        fi
        sleep 2
        printf "."
    done
    printf "\n"
    return 1
}

start_local_flink_cluster() {
    if [[ -z "${FLINK_HOME:-}" || ! -x "${FLINK_HOME}/bin/start-cluster.sh" ]]; then
        return 1
    fi

    echo "      Docker 启动失败，尝试本机 Flink standalone: ${FLINK_HOME}"
    local standalone_conf="${REPO_ROOT}/docker/generated/flink-standalone-conf"
    local standalone_state="${REPO_ROOT}/docker/generated/flink-state"
    rm -rf "$standalone_conf"
    mkdir -p "$standalone_conf" "$standalone_state/checkpoints" "$standalone_state/savepoints" "$standalone_state/tmp"
    cp -a "${FLINK_HOME}/conf/." "$standalone_conf/"
    cat >> "${standalone_conf}/config.yaml" <<'EOF'

# ForL0 benchmark standalone fallback overrides.
taskmanager.memory.process.size: 20G
jobmanager.memory.process.size: 4G
taskmanager.numberOfTaskSlots: 32
parallelism.default: 4
heartbeat.timeout: 300000
heartbeat.interval: 10000
EOF
    cat >> "${standalone_conf}/config.yaml" <<EOF
state.checkpoints.dir: file://${standalone_state}/checkpoints
state.savepoints.dir: file://${standalone_state}/savepoints
io.tmp.dirs: ${standalone_state}/tmp
EOF
    export FLINK_CONF_DIR="$standalone_conf"
    echo "      ✓ 使用 standalone Flink 配置: $FLINK_CONF_DIR"

    if [[ -z "${JAVA_HOME:-}" ]]; then
        for candidate in \
            /usr/lib/jvm/java-11-openjdk-arm64 \
            /usr/lib/jvm/java-11-openjdk-amd64 \
            /usr/lib/jvm/default-java; do
            if [[ -x "$candidate/bin/java" ]]; then
                export JAVA_HOME="$candidate"
                echo "      ✓ 自动设置 JAVA_HOME=$JAVA_HOME"
                break
            fi
        done
    fi

    "${FLINK_HOME}/bin/start-cluster.sh"
    wait_for_flink_rest 30
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
  --scenario NAME       透传场景名，例如 NexMark forl0_q5_tps_probe 或 WordCount high_cardinality
  --offline             禁止联网安装依赖；缺少离线包时直接失败
  --online              允许离线包失败后回退在线安装（默认 auto）
  --no-report           只跑实验，不生成 benchmark HTML 报告
  --report-only         不跑实验，只基于已有结果生成 benchmark HTML 报告
  --preflight-only      只检查离线运行依赖、JAR、配置与 Python 环境，不启动实验
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
        --scenario)
            EXTRA_ARGS+=("$1" "$2")
            shift 2
            ;;
        --no-profile)
            ENABLE_PROFILE=false
            shift
            ;;
        --offline)
            OFFLINE_MODE=true
            shift
            ;;
        --online)
            OFFLINE_MODE=false
            shift
            ;;
        --no-report)
            GENERATE_REPORT=false
            shift
            ;;
        --report-only)
            REPORT_ONLY=true
            shift
            ;;
        --preflight-only)
            PREFLIGHT_ONLY=true
            GENERATE_REPORT=false
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

print_provenance

if [[ "$REPORT_ONLY" == "true" ]]; then
    bootstrap_benchmark_python
    generate_benchmark_report
    exit 0
fi

if [[ ! -f "${REPO_ROOT}/docker/forl0-local.env" ]]; then
    echo "[1/5] 未发现部署环境，先执行 server_setup.sh"
    if [[ -n "${FLINK_HOME:-}" ]]; then
        bash "${REPO_ROOT}/docker/server_setup.sh" --flink-home "$FLINK_HOME" --no-start
    else
        bash "${REPO_ROOT}/docker/server_setup.sh" --no-start
    fi
    # shellcheck disable=SC1091
    source "${REPO_ROOT}/docker/forl0-local.env"
fi

if [[ "$PREFLIGHT_ONLY" == "true" ]]; then
    preflight_check
    exit 0
fi

if ! curl -sf http://localhost:8081/overview >/dev/null 2>&1; then
    echo "[2/5] Flink 集群未运行，启动 docker_run.sh"
    # shellcheck disable=SC1091
    source "${REPO_ROOT}/docker/forl0-local.env"
    if ! bash "${REPO_ROOT}/docker/docker_run.sh" start; then
        start_local_flink_cluster || {
            echo "✗ Docker 与本机 standalone Flink 均启动失败"
            exit 1
        }
    fi
else
    echo "[2/5] Flink 集群已就绪"
fi

bootstrap_benchmark_python
bootstrap_async_profiler

echo "[4/5] 开始运行 benchmark/scripts/run_benchmark.py"

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

generate_benchmark_report
