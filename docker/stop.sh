#!/usr/bin/env bash
################################################################################
#  停止 ForL0 Docker 测试集群
#  用法: ./stop.sh [--clean]
#    --clean  同时删除 checkpoint 数据卷
################################################################################

set -euo pipefail
cd "$(dirname "$0")"

# 兼容 Docker Compose V1/V2
if docker compose version >/dev/null 2>&1; then
    DOCKER_COMPOSE="docker compose"
elif docker-compose version >/dev/null 2>&1; then
    DOCKER_COMPOSE="docker-compose"
else
    echo "✗ docker compose 或 docker-compose 未找到"; exit 1
fi

echo "=== 停止 ForL0 Flink Docker 测试集群 ==="

if [[ "${1:-}" == "--clean" ]]; then
    echo "停止容器并清理数据卷..."
    $DOCKER_COMPOSE down -v
    echo "已清理所有容器和数据卷"
else
    $DOCKER_COMPOSE down
    echo "已停止所有容器（checkpoint 数据卷保留）"
    echo "使用 ./stop.sh --clean 清理数据卷"
fi
