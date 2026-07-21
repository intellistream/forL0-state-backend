#!/usr/bin/env bash
################################################################################
#  启动 ForL0 Docker 测试集群
#  用法: ./start.sh
################################################################################

set -euo pipefail
cd "$(dirname "$0")"
source "./lib/l0_detector.sh"
forl0_detect_l0_environment || true
echo "=== L0 环境探测 ==="
forl0_print_l0_detection
if [[ -z "$FORL0_L0_DEVICE_PATH" || -z "$FORL0_L0_LIBRARY_PATH" ]]; then
    echo "✗ 未找到真实 L0 设备或运行库；请按上面的 override 提示设置后重试"
    exit 1
fi
export L0_DEVICE_HOST_PATH="$FORL0_L0_DEVICE_PATH"
export L0_MEMPOOL_LIB_HOST_PATH="$FORL0_L0_LIBRARY_PATH"
export NUMA_LIB_HOST_PATH="$FORL0_NUMA_LIBRARY_PATH"

# 兼容 Docker Compose V1/V2
if docker compose version >/dev/null 2>&1; then
    DOCKER_COMPOSE="docker compose"
elif docker-compose version >/dev/null 2>&1; then
    DOCKER_COMPOSE="docker-compose"
else
    echo "✗ docker compose 或 docker-compose 未找到"; exit 1
fi

echo "=== 启动 ForL0 Flink Docker 测试集群 ==="
echo "  配置目录: ${FORL0_FLINK_CONF_DIR:-./conf}"
echo "  JM:  1 × (2核 ${FORL0_JM_DOCKER_MEMORY:-8G})"
echo "  TM:  2 × (4核 ${FORL0_TM_DOCKER_MEMORY:-16G}, 4 slots)"
echo "  并行度: 8"
echo ""

# 拉取基础镜像（如果不存在）
if ! docker image inspect eclipse-temurin:8-jre >/dev/null 2>&1; then
    echo "[1/3] 拉取 eclipse-temurin:8-jre 镜像..."
    docker pull eclipse-temurin:8-jre
else
    echo "[1/3] 基础镜像已存在"
fi

# 启动集群
echo "[2/3] 启动容器..."
$DOCKER_COMPOSE up -d

# 等待 JM 就绪
echo "[3/3] 等待 JobManager 就绪..."
for i in $(seq 1 30); do
    if curl -sf http://localhost:8081/overview >/dev/null 2>&1; then
        echo ""
        echo "=== 集群启动成功 ==="

        # 等待 TM 注册
        sleep 3
        TM_COUNT=$(curl -sf http://localhost:8081/taskmanagers | grep -o '"id"' | wc -l)
        SLOTS=$(curl -sf http://localhost:8081/overview | grep -o '"slots-total":[0-9]*' | cut -d: -f2)

        echo "  TaskManager 数量: ${TM_COUNT}"
        echo "  总 Slot 数:       ${SLOTS:-pending}"
        echo "  Web UI:           http://localhost:8081"
        echo ""
        echo "提交作业示例:"
        echo "  \$FLINK_HOME/bin/flink run -m localhost:8081 -c <MainClass> <jar>"
        echo ""
        echo "查看日志:"
        echo "  $DOCKER_COMPOSE -f $(pwd)/docker-compose.yml logs -f"
        exit 0
    fi
    printf "."
    sleep 2
done

echo ""
echo "警告: JobManager 未在 60 秒内就绪，请检查日志:"
echo "  $DOCKER_COMPOSE logs jobmanager"
exit 1
