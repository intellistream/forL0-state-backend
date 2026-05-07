#!/usr/bin/env bash
################################################################################
#  准备离线部署文件 (在 macOS 上执行)
#  1. 保存 Docker 镜像
#  2. 复制编译好的 JAR
#
#  用法: ./save_docker_image.sh
################################################################################

set -euo pipefail
cd "$(dirname "$0")"

REPO_ROOT="$(cd .. && pwd)"
IMAGE="eclipse-temurin:8-jre"
OUTPUT_DIR="images"
OUTPUT_FILE="${OUTPUT_DIR}/eclipse-temurin-8-jre.tar"
DEPLOY_DIR="deploy"

echo "=== 准备离线部署文件 ==="
echo ""

# ---- 1. Docker 镜像 ----
# 鲲鹏服务器是 ARM64 (aarch64)，必须拉取 linux/arm64 镜像
PLATFORM="linux/arm64"

if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    echo "[1/2] 拉取镜像 ${IMAGE} (${PLATFORM}) ..."
    docker pull --platform "$PLATFORM" "$IMAGE"
else
    echo "[1/2] 镜像 ${IMAGE} 已存在"
fi

echo "      导出镜像到 ${OUTPUT_FILE} ..."
mkdir -p "$OUTPUT_DIR"
docker save "$IMAGE" -o "$OUTPUT_FILE"
SIZE=$(du -h "$OUTPUT_FILE" | cut -f1)
echo "      ✓ 镜像已保存 (${SIZE})"

# ---- 2. ForL0 JAR ----
echo ""
JAR_FILE="${REPO_ROOT}/target/flink-statebackend-forL0-1.0-SNAPSHOT.jar"
if [[ -f "$JAR_FILE" ]]; then
    mkdir -p "$DEPLOY_DIR"
    cp "$JAR_FILE" "$DEPLOY_DIR/"
    echo "[2/2] ✓ JAR 已复制到 ${DEPLOY_DIR}/"
else
    echo "[2/2] ✗ JAR 不存在，请先运行: mvn clean package -DskipTests"
    exit 1
fi

echo ""
echo "=== 完成 ==="
echo ""
echo "接下来提交到仓库:"
echo "  git lfs install"
echo "  git lfs track 'docker/images/*.tar'"
echo "  git add .gitattributes docker/images/ docker/deploy/"
echo "  git commit -m 'Add offline deployment files'"
echo "  git push"
