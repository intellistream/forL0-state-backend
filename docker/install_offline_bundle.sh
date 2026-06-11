#!/usr/bin/env bash
################################################################################
#  ForL0 离线包一键安装脚本
#
#  用法:
#    ./docker/install_offline_bundle.sh --flink-home /path/to/flink
#    ./docker/install_offline_bundle.sh --flink-home /path/to/flink --start-docker
#    ./docker/install_offline_bundle.sh --flink-home /path/to/flink --install-dir ~/forl0-runtime
#
#  说明:
#    1. 脚本默认在解压后的离线包目录内执行
#    2. 不依赖网络，不会尝试下载任何内容
#    3. 如需启动 Docker 集群，目标机必须已提前准备好本地 Docker 镜像
################################################################################

set -euo pipefail
cd "$(dirname "$0")"

BUNDLE_ROOT="$(cd .. && pwd)"
INSTALL_DIR="${HOME}/forl0-runtime"
FLINK_DIR="${FLINK_HOME:-}"
START_DOCKER=false
COPY_PROFILER=false

usage() {
    cat <<'EOF'
用法:
  ./docker/install_offline_bundle.sh --flink-home /path/to/flink [选项]

选项:
  --flink-home PATH     Flink 安装目录，必填（也可通过 FLINK_HOME 提供）
  --install-dir PATH    安装输出目录，默认 ~/forl0-runtime
  --start-docker        安装完成后直接启动 docker/docker_run.sh start
  --copy-profiler       将 tools/async-profiler-4.4-linux-arm64 复制到安装目录
  -h, --help            显示帮助
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --flink-home)
            FLINK_DIR="$2"
            shift 2
            ;;
        --install-dir)
            INSTALL_DIR="$2"
            shift 2
            ;;
        --start-docker)
            START_DOCKER=true
            shift
            ;;
        --copy-profiler)
            COPY_PROFILER=true
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
    echo "✗ 必须提供 --flink-home 或预先设置 FLINK_HOME"
    usage
    exit 1
fi

BACKEND_JAR="${BUNDLE_ROOT}/artifacts/flink-statebackend-forL0-1.0-SNAPSHOT.jar"
NATIVE_LIB="${BUNDLE_ROOT}/artifacts/libforl0_engine.so"
WORDCOUNT_JAR="${BUNDLE_ROOT}/artifacts/wordcount-benchmark-1.0-SNAPSHOT.jar"
PROFILER_DIR="${BUNDLE_ROOT}/tools/async-profiler-4.4-linux-arm64"

for required in "$BACKEND_JAR" "$NATIVE_LIB" "$WORDCOUNT_JAR"; do
    if [[ ! -f "$required" ]]; then
        echo "✗ 离线包内容不完整，缺少: $required"
        exit 1
    fi
done

if [[ ! -d "$FLINK_DIR" ]]; then
    echo "✗ Flink 目录不存在: $FLINK_DIR"
    exit 1
fi

echo "============================================================"
echo "  ForL0 离线包一键安装"
echo "============================================================"
echo "  Bundle Root:  ${BUNDLE_ROOT}"
echo "  FLINK_HOME:   ${FLINK_DIR}"
echo "  Install Dir:  ${INSTALL_DIR}"
echo ""

mkdir -p "${FLINK_DIR}/lib" "${FLINK_DIR}/native"
mkdir -p "${INSTALL_DIR}/artifacts" "${INSTALL_DIR}/benchmark" "${INSTALL_DIR}/docker" "${INSTALL_DIR}/docs"

echo "[1/5] 安装 backend JAR 到 Flink lib/"
rm -f "${FLINK_DIR}/lib"/flink-statebackend-forl0-*.jar
rm -f "${FLINK_DIR}/lib"/flink-statebackend-forL0-*.jar
cp "$BACKEND_JAR" "${FLINK_DIR}/lib/"

echo "[2/5] 安装 native 库到 Flink native/"
cp "$NATIVE_LIB" "${FLINK_DIR}/native/"

echo "[3/5] 同步 benchmark、docker、docs 到安装目录"
cp "$WORDCOUNT_JAR" "${INSTALL_DIR}/artifacts/"
rm -rf "${INSTALL_DIR}/benchmark/config" "${INSTALL_DIR}/benchmark/scripts"
cp -r "${BUNDLE_ROOT}/benchmark/config" "${INSTALL_DIR}/benchmark/"
cp -r "${BUNDLE_ROOT}/benchmark/scripts" "${INSTALL_DIR}/benchmark/"
rm -rf "${INSTALL_DIR}/docker/conf"
cp -r "${BUNDLE_ROOT}/docker/conf" "${INSTALL_DIR}/docker/"
cp "${BUNDLE_ROOT}/docker/docker_run.sh" "${INSTALL_DIR}/docker/"
cp "${BUNDLE_ROOT}/docker/start.sh" "${INSTALL_DIR}/docker/"
cp "${BUNDLE_ROOT}/docker/stop.sh" "${INSTALL_DIR}/docker/"
cp "${BUNDLE_ROOT}/docker/restart.sh" "${INSTALL_DIR}/docker/"
cp "${BUNDLE_ROOT}/docker/install_offline_bundle.sh" "${INSTALL_DIR}/docker/"
cp -r "${BUNDLE_ROOT}/docs/." "${INSTALL_DIR}/docs/"

if [[ "$COPY_PROFILER" == "true" ]]; then
    echo "[4/5] 复制 async-profiler 到安装目录"
    rm -rf "${INSTALL_DIR}/tools/async-profiler-4.4-linux-arm64"
    mkdir -p "${INSTALL_DIR}/tools"
    cp -r "$PROFILER_DIR" "${INSTALL_DIR}/tools/"
else
    echo "[4/5] 跳过 profiler 复制（如需 flame graph，可加 --copy-profiler）"
fi

cat > "${INSTALL_DIR}/forl0-offline.env" <<EOF
export FLINK_HOME="${FLINK_DIR}"
export FORL0_BUNDLE_ROOT="${INSTALL_DIR}"
export FORL0_NATIVE_DIR="${FLINK_DIR}/native"
export WORDCOUNT_BENCHMARK_JAR="${INSTALL_DIR}/artifacts/wordcount-benchmark-1.0-SNAPSHOT.jar"
EOF

echo "[5/5] 写出环境文件 ${INSTALL_DIR}/forl0-offline.env"

chmod +x "${INSTALL_DIR}/docker/docker_run.sh" \
         "${INSTALL_DIR}/docker/start.sh" \
         "${INSTALL_DIR}/docker/stop.sh" \
         "${INSTALL_DIR}/docker/restart.sh" \
         "${INSTALL_DIR}/docker/install_offline_bundle.sh"

echo ""
echo "安装完成。"
echo ""
echo "后续使用方式："
echo "  source ${INSTALL_DIR}/forl0-offline.env"
echo "  cd ${INSTALL_DIR}/docker"
echo "  ./docker_run.sh status"

if [[ "$START_DOCKER" == "true" ]]; then
    echo ""
    echo "尝试启动 Docker 集群..."
    export FLINK_HOME="${FLINK_DIR}"
    export FORL0_NATIVE_DIR="${FLINK_DIR}/native"
    cd "${INSTALL_DIR}/docker"
    ./docker_run.sh start
fi