#!/usr/bin/env bash
################################################################################
#  停止 ForL0 Docker 测试集群
#  用法: ./stop.sh [--clean]
#    --clean  同时删除 checkpoint 数据卷
################################################################################

set -euo pipefail
cd "$(dirname "$0")"

echo "=== 停止 ForL0 Flink Docker 测试集群 ==="

if [[ "${1:-}" == "--clean" ]]; then
    echo "停止容器并清理数据卷..."
    docker compose down -v
    echo "已清理所有容器和数据卷"
else
    docker compose down
    echo "已停止所有容器（checkpoint 数据卷保留）"
    echo "使用 ./stop.sh --clean 清理数据卷"
fi
