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

report_l0_status() {
    local l0_device=""
    local l0_available=true
    local numa_found=false

    if [[ -e /dev/l0 ]]; then
        l0_device="/dev/l0"
    elif [[ -e /dev/hisi_l0 ]]; then
        l0_device="/dev/hisi_l0"
    else
        l0_available=false
    fi

    if [[ -f /usr/lib64/libl0mempool.so ]]; then
        echo "      ✓ libl0mempool.so 存在"
    else
        echo "      ✗ libl0mempool.so 不存在 (/usr/lib64/libl0mempool.so)"
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

BACKEND_JAR="$(pick_first_file \
    "${REPO_ROOT}/target/flink-statebackend-forL0-1.0-SNAPSHOT.jar" \
    "${REPO_ROOT}/docker/deploy/flink-statebackend-forL0-1.0-SNAPSHOT.jar" \
    "${REPO_ROOT}/docker/deploy/flink-statebackend-forl0-1.0-SNAPSHOT.jar")"

NATIVE_LIB="$(pick_first_file \
    "${REPO_ROOT}/src/main/resources/native/libforl0_engine.so" \
    "${REPO_ROOT}/src/main/native/libforl0_engine.so")"

WORDCOUNT_JAR="$(pick_first_file \
    "${REPO_ROOT}/benchmark/wordcount/target/wordcount-benchmark-1.0-SNAPSHOT.jar" \
    "${REPO_ROOT}/docker/deploy/wordcount-benchmark-1.0-SNAPSHOT.jar")"

echo "============================================================"
echo "  ForL0 仓库一键部署"
echo "============================================================"
echo "  仓库路径:   ${REPO_ROOT}"
echo "  FLINK_HOME: ${FLINK_DIR}"
echo ""

for required in "$BACKEND_JAR" "$NATIVE_LIB" "$WORDCOUNT_JAR"; do
    if [[ ! -f "$required" ]]; then
        echo "✗ 缺少预编译产物: $required"
        exit 1
    fi
done

if [[ "$SKIP_DOCKER_LOAD" == "false" ]]; then
    echo "[1/4] 检查 Docker 镜像..."
    IMAGE_FILE="${REPO_ROOT}/docker/images/eclipse-temurin-8-jre.tar"
    if docker image inspect eclipse-temurin:8-jre >/dev/null 2>&1; then
        echo "      ✓ Docker 镜像已存在"
    elif [[ -f "$IMAGE_FILE" ]]; then
        echo "      加载本地镜像 ${IMAGE_FILE}"
        docker load -i "$IMAGE_FILE"
    else
        echo "      ✗ 缺少本地镜像文件: ${IMAGE_FILE}"
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
cp "$NATIVE_LIB" "${FLINK_DIR}/native/libforl0_engine.so"

cat > "${REPO_ROOT}/docker/forl0-local.env" <<EOF
export FLINK_HOME="${FLINK_DIR}"
export FORL0_NATIVE_DIR="${FLINK_DIR}/native"
export WORDCOUNT_BENCHMARK_JAR="${WORDCOUNT_JAR}"
export REPO_ROOT="${REPO_ROOT}"
EOF

echo "      ✓ backend JAR 已安装到 ${FLINK_DIR}/lib/"
echo "      ✓ native 库已安装到 ${FLINK_DIR}/native/"
echo "      ✓ 环境文件已写入 ${REPO_ROOT}/docker/forl0-local.env"

echo ""
echo "[4/4] 启动 Docker 集群..."
if [[ "$START_DOCKER" == "true" ]]; then
    export FLINK_HOME="${FLINK_DIR}"
    export FORL0_NATIVE_DIR="${FLINK_DIR}/native"
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
echo "  提交 WordCount: ${FLINK_DIR}/bin/flink run -d ${WORDCOUNT_JAR}"
