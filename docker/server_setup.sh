#!/usr/bin/env bash
################################################################################
#  ForL0 仓库一键部署脚本
#
#  适用场景:
#    1. 整个仓库已经被原封不动拷到目标机器
#    2. 目标机器完全不联网
#    3. 你希望直接在仓库目录里执行一个脚本完成部署
#
#  推荐用法:
#    cd ~/forL0-state-backend/docker
#    ./server_setup.sh
#
#  可选:
#    ./server_setup.sh --flink-home /path/to/flink
#    ./server_setup.sh --no-start
#    ./server_setup.sh --skip-docker-load
################################################################################

set -euo pipefail
cd "$(dirname "$0")"

REPO_ROOT="$(cd .. && pwd)"
FLINK_DIR="${FLINK_HOME:-}"
START_DOCKER=true
SKIP_DOCKER_LOAD=false
DOCKER_BIN="${DOCKER_BIN:-}"

usage() {
    cat <<'EOF'
用法:
  ./server_setup.sh [选项]

选项:
  --flink-home PATH     Flink 安装目录；不传则自动探测
  --no-start            只安装，不启动 Docker 集群
  --skip-docker-load    不执行 docker load
  -h, --help            显示帮助
EOF
}

pick_first_file() {
    for candidate in "$@"; do
        if [[ -f "$candidate" ]]; then
            echo "$candidate"
            return 0
        fi
    done
    return 1
}

detect_flink_home() {
    if [[ -n "${FLINK_HOME:-}" && -d "${FLINK_HOME}" ]]; then
        echo "${FLINK_HOME}"
        return 0
    fi

    for candidate in \
        "$HOME/flink-1.20.3" \
        "$HOME/flink" \
        /opt/flink \
        /usr/local/flink; do
        if [[ -d "$candidate" ]]; then
            echo "$candidate"
            return 0
        fi
    done

    local detected
    detected="$(find "$HOME" -maxdepth 1 -type d \( -name 'flink-1.20.3' -o -name 'flink-*' \) | head -n 1 || true)"
    if [[ -n "$detected" ]]; then
        echo "$detected"
        return 0
    fi

    return 1
}

detect_docker_bin() {
    if [[ -n "$DOCKER_BIN" ]]; then
        echo "$DOCKER_BIN"
        return 0
    fi

    if docker ps >/dev/null 2>&1; then
        echo "docker"
        return 0
    fi

    if sudo -n docker ps >/dev/null 2>&1; then
        echo "sudo -n docker"
        return 0
    fi

    return 1
}

report_l0_status() {
    local l0_device=""
    local l0_available=true
    local numa_found=false
    local l0_lib=""

    if [[ -e /dev/l0 ]]; then
        l0_device="/dev/l0"
    elif [[ -e /dev/hisi_l0 ]]; then
        l0_device="/dev/hisi_l0"
    else
        l0_available=false
    fi

    for l0_candidate in \
        /usr/lib64/libl0mempool.so \
        /usr/lib/libl0mempool.so \
        /lib64/libl0mempool.so \
        /lib/libl0mempool.so; do
        if [[ -f "$l0_candidate" ]]; then
            l0_lib="$l0_candidate"
            break
        fi
    done

    if [[ -n "$l0_lib" ]]; then
        echo "      ✓ libl0mempool.so 存在 (${l0_lib})"
    else
        echo "      ✗ libl0mempool.so 不存在 (/usr/lib64, /usr/lib, /lib64, /lib)"
        l0_available=false
    fi

    for numa_candidate in \
        /lib/aarch64-linux-gnu/libnuma.so.1 \
        /usr/lib/aarch64-linux-gnu/libnuma.so.1 \
        /lib64/libnuma.so.1 \
        /usr/lib64/libnuma.so.1; do
        if [[ -f "$numa_candidate" ]]; then
            echo "      ✓ libnuma.so.1 存在 (${numa_candidate})"
            numa_found=true
            break
        fi
    done

    if [[ "$numa_found" == "false" ]]; then
        echo "      ✗ libnuma.so.1 不存在"
        l0_available=false
    fi

    if [[ -n "$l0_device" ]]; then
        echo "      ✓ ${l0_device} 设备存在"
    else
        echo "      ✗ /dev/l0 和 /dev/hisi_l0 都不存在"
    fi

    if [[ "$l0_available" == "true" ]]; then
        echo "      ✓ L0 硬件环境可用"
    else
        echo "      ⚠ 当前不会使用真实 L0 硬件加速"
    fi
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --flink-home)
            FLINK_DIR="$2"
            shift 2
            ;;
        --no-start)
            START_DOCKER=false
            shift
            ;;
        --skip-docker-load)
            SKIP_DOCKER_LOAD=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "未知参数: $1"
            usage
            exit 1
            ;;
    esac
done

if [[ -z "$FLINK_DIR" ]]; then
    FLINK_DIR="$(detect_flink_home || true)"
fi

if [[ -z "$FLINK_DIR" || ! -d "$FLINK_DIR" ]]; then
    echo "✗ 未能自动找到 FLINK_HOME"
    echo "  请手工执行: ./server_setup.sh --flink-home /path/to/flink"
    exit 1
fi

DOCKER_BIN="$(detect_docker_bin || true)"
if [[ -z "$DOCKER_BIN" && "$START_DOCKER" == "true" ]]; then
    echo "✗ 当前用户无法访问 Docker。"
    echo "  请确认当前用户可直接执行 docker，或可执行 sudo -n docker；如只需安装环境，可加 --no-start。"
    exit 1
fi
if [[ -z "$DOCKER_BIN" ]]; then
    DOCKER_BIN="docker"
fi

BACKEND_JAR="$(pick_first_file \
    "${REPO_ROOT}/docker/deploy/flink-statebackend-forl0-1.0-SNAPSHOT.jar" \
    "${REPO_ROOT}/docker/deploy/flink-statebackend-forL0-1.0-SNAPSHOT.jar" \
    "${REPO_ROOT}/target/flink-statebackend-forL0-1.0-SNAPSHOT.jar")"

NATIVE_LIB="$(pick_first_file \
    "${REPO_ROOT}/src/main/resources/native/libforl0_engine.so" \
    "${REPO_ROOT}/src/main/native/libforl0_engine.so")"

WORDCOUNT_JAR="$(pick_first_file \
    "${REPO_ROOT}/benchmark/wordcount/target/wordcount-benchmark-1.0-SNAPSHOT.jar" \
    "${REPO_ROOT}/docker/deploy/wordcount-benchmark-1.0-SNAPSHOT.jar")"

UNITTEST_JAR="$(pick_first_file \
    "${REPO_ROOT}/benchmark/unit-test/target/unit-test-benchmark-1.0-SNAPSHOT.jar" \
    "${REPO_ROOT}/docker/deploy/unit-test-benchmark-1.0-SNAPSHOT.jar" || true)"

NEXMARK_JAR="$(pick_first_file \
    "${REPO_ROOT}/benchmark/nexmark-src/nexmark-flink/target/nexmark-flink-bin/nexmark-flink/lib/nexmark-flink-0.3-SNAPSHOT.jar" \
    "${REPO_ROOT}/docker/deploy/nexmark-flink-0.3-SNAPSHOT.jar" || true)"

CLIENT_JAR="$(pick_first_file \
    "${REPO_ROOT}/client_usecase/XX_6000c_Demo/target/flink-keyedcoprocessfunction-example-1.0-SNAPSHOT-jar-with-dependencies.jar" \
    "${REPO_ROOT}/docker/deploy/flink-keyedcoprocessfunction-example-1.0-SNAPSHOT-jar-with-dependencies.jar" || true)"

ASYNC_PROFILER_DIR=""
ASYNC_PROFILER_ARCHIVE="$(pick_first_file \
    "${REPO_ROOT}/offline-packages/async-profiler-4.4-linux-arm64.tar.gz" \
    "${REPO_ROOT}/offline-packages/async-profiler-4.4-linux-x64.tar.gz" \
    "${REPO_ROOT}/benchmark/offline-packages/async-profiler-4.4-linux-arm64.tar.gz" \
    "${REPO_ROOT}/benchmark/offline-packages/async-profiler-4.4-linux-x64.tar.gz" \
    "${REPO_ROOT}/docker/deploy/async-profiler-4.4-linux-arm64.tar.gz" \
    "${REPO_ROOT}/docker/deploy/async-profiler-4.4-linux-x64.tar.gz" || true)"

if [[ -x "${REPO_ROOT}/tools/async-profiler/bin/asprof" ]]; then
    ASYNC_PROFILER_DIR="${REPO_ROOT}/tools/async-profiler"
elif [[ -n "${ASYNC_PROFILER_ARCHIVE}" && -f "${ASYNC_PROFILER_ARCHIVE}" ]]; then
    mkdir -p "${REPO_ROOT}/tools"
    tar -xzf "${ASYNC_PROFILER_ARCHIVE}" -C "${REPO_ROOT}/tools"
    extracted_dir="$(find "${REPO_ROOT}/tools" -maxdepth 1 -type d -name 'async-profiler-*' | head -n 1 || true)"
    if [[ -n "${extracted_dir}" && -x "${extracted_dir}/bin/asprof" ]]; then
        rm -rf "${REPO_ROOT}/tools/async-profiler"
        mv "${extracted_dir}" "${REPO_ROOT}/tools/async-profiler"
        ASYNC_PROFILER_DIR="${REPO_ROOT}/tools/async-profiler"
    fi
fi

echo "============================================================"
echo "  ForL0 仓库一键部署"
echo "============================================================"
echo "  仓库路径:   ${REPO_ROOT}"
echo "  FLINK_HOME: ${FLINK_DIR}"
echo "  Docker:     ${DOCKER_BIN}"
echo ""

for required in "$BACKEND_JAR" "$NATIVE_LIB" "$WORDCOUNT_JAR" "$NEXMARK_JAR" "$CLIENT_JAR"; do
    if [[ ! -f "$required" ]]; then
        echo "✗ 缺少预编译产物: $required"
        exit 1
    fi
done

if [[ -z "$UNITTEST_JAR" || ! -f "$UNITTEST_JAR" ]]; then
    if [[ -d "${REPO_ROOT}/benchmark/unit-test" ]]; then
        echo "      ⚠ 未找到 unit-test benchmark JAR；apps/nexmark/wordcount/client_usecase 运行不依赖该产物"
    fi
fi

if [[ "$SKIP_DOCKER_LOAD" == "false" && "$START_DOCKER" == "true" ]]; then
    echo "[1/4] 检查 Docker 镜像..."
    IMAGE_FILE=""
    for candidate in \
        "${REPO_ROOT}/docker/images/eclipse-temurin-8-jre.tar.gz" \
        "${REPO_ROOT}/docker/images/eclipse-temurin-8-jre.tar"; do
        if [[ -f "$candidate" ]]; then
            IMAGE_FILE="$candidate"
            break
        fi
    done
    if ${DOCKER_BIN} image inspect eclipse-temurin:8-jre >/dev/null 2>&1; then
        echo "      ✓ Docker 镜像已存在"
    elif [[ -n "$IMAGE_FILE" ]]; then
        echo "      加载本地镜像 ${IMAGE_FILE}"
        ${DOCKER_BIN} load -i "$IMAGE_FILE"
    else
        echo "      ✗ 缺少本地镜像文件: docker/images/eclipse-temurin-8-jre.tar.gz 或 .tar"
        exit 1
    fi
else
    echo "[1/4] 跳过 Docker 镜像加载"
fi

echo ""
echo "[2/4] 检查 L0 环境..."
report_l0_status

echo ""
echo "[3/4] 安装预编译产物到 Flink..."
mkdir -p "${FLINK_DIR}/lib" "${FLINK_DIR}/native"
rm -f "${FLINK_DIR}/lib"/flink-statebackend-forl0-*.jar
rm -f "${FLINK_DIR}/lib"/flink-statebackend-forL0-*.jar
cp "$BACKEND_JAR" "${FLINK_DIR}/lib/flink-statebackend-forL0-1.0-SNAPSHOT.jar"
cp "$NEXMARK_JAR" "${FLINK_DIR}/lib/$(basename "$NEXMARK_JAR")"
cp "$NATIVE_LIB" "${FLINK_DIR}/native/libforl0_engine.so"

cat > "${REPO_ROOT}/docker/forl0-local.env" <<EOF
export FLINK_HOME="${FLINK_DIR}"
export FORL0_NATIVE_DIR="${FLINK_DIR}/native"
export WORDCOUNT_BENCHMARK_JAR="${WORDCOUNT_JAR}"
export UNITTEST_BENCHMARK_JAR="${UNITTEST_JAR}"
export NEXMARK_FLINK_JAR="${NEXMARK_JAR}"
export CLIENT_USECASE_JAR="${CLIENT_JAR}"
export REPO_ROOT="${REPO_ROOT}"
export DOCKER_BIN="${DOCKER_BIN}"
export ASYNC_PROFILER_HOME="${ASYNC_PROFILER_DIR}"
export FLINK_TASKMANAGER_CONTAINER="flink-taskmanager-1"
EOF

echo "      ✓ backend JAR 已安装到 ${FLINK_DIR}/lib/"
echo "      ✓ NexMark JAR 已安装到 ${FLINK_DIR}/lib/"
echo "      ✓ native 库已安装到 ${FLINK_DIR}/native/"
echo "      ✓ WordCount benchmark 已就绪"
echo "      ✓ Client usecase benchmark 已就绪"
if [[ -n "$UNITTEST_JAR" && -f "$UNITTEST_JAR" ]]; then
    echo "      ✓ unit-test benchmark 已就绪"
fi
echo "      ✓ 环境文件已写入 ${REPO_ROOT}/docker/forl0-local.env"
if [[ -n "${ASYNC_PROFILER_DIR}" && -x "${ASYNC_PROFILER_DIR}/bin/asprof" ]]; then
    echo "      ✓ async-profiler 已就绪: ${ASYNC_PROFILER_DIR}"
else
    echo "      ⚠ async-profiler 未就绪，--profile cpu 可能无法产出火焰图"
    echo "        可将 async-profiler-4.4-linux-*.tar.gz 放到 offline-packages/ 后重试"
fi

echo ""
echo "[4/4] 启动 Docker 集群..."
if [[ "$START_DOCKER" == "true" ]]; then
    export FLINK_HOME="${FLINK_DIR}"
    export FORL0_NATIVE_DIR="${FLINK_DIR}/native"
    export DOCKER_BIN
    bash "${REPO_ROOT}/docker/docker_run.sh" stop 2>/dev/null || true
    bash "${REPO_ROOT}/docker/docker_run.sh" start
else
    echo "      已跳过启动。手工执行："
    echo "      source ${REPO_ROOT}/docker/forl0-local.env"
    echo "      cd ${REPO_ROOT}/docker && ./docker_run.sh start"
fi

echo ""
echo "============================================================"
echo "  完成"
echo "============================================================"
echo "  查看状态: source ${REPO_ROOT}/docker/forl0-local.env && cd ${REPO_ROOT}/docker && ./docker_run.sh status"
echo "  查看日志: source ${REPO_ROOT}/docker/forl0-local.env && cd ${REPO_ROOT}/docker && ./docker_run.sh logs tm1"
echo "  全量运行 apps: cd ${REPO_ROOT}/docker && ./run_all_apps.sh"
echo "  只跑 WordCount: cd ${REPO_ROOT}/docker && ./run_all_apps.sh --test wordcount"
