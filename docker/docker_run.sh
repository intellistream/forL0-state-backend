#!/usr/bin/env bash
################################################################################
#  用纯 docker run 启动 ForL0 Flink 集群（不依赖 docker-compose）
#
#  启动: ./docker_run.sh start
#  停止: ./docker_run.sh stop
#  日志: ./docker_run.sh logs [jm|tm1|tm2]
#  状态: ./docker_run.sh status
################################################################################

set -euo pipefail
cd "$(dirname "$0")"

REPO_ROOT="$(cd .. && pwd)"
FLINK_DIR="${FLINK_HOME:-/home/user/flink}"
NATIVE_DIR="${REPO_ROOT}/src/main/resources/native"
CONF_DIR="$(pwd)/conf"
NETWORK="flink-net"
IMAGE="eclipse-temurin:8-jre"

# L0 挂载参数 (仅当硬件存在时)
L0_OPTS=()
if [[ -e /dev/hisi_l0 ]]; then
    L0_OPTS+=(--device /dev/hisi_l0:/dev/hisi_l0)
fi
if [[ -f /usr/lib64/libl0mempool.so ]]; then
    L0_OPTS+=(-v /usr/lib64/libl0mempool.so:/usr/lib64/libl0mempool.so:ro)
fi

# 容器名
JM="flink-jobmanager"
TM1="flink-taskmanager-1"
TM2="flink-taskmanager-2"

usage() {
    echo "用法: $0 {start|stop|logs|status}"
    echo ""
    echo "  start   启动集群 (1 JM + 2 TM)"
    echo "  stop    停止并移除所有容器"
    echo "  logs    查看日志: $0 logs [jm|tm1|tm2]"
    echo "  status  查看容器状态"
    exit 1
}

do_start() {
    echo "=== 启动 ForL0 Flink Docker 集群 ==="
    echo "  FLINK_HOME: ${FLINK_DIR}"
    echo "  NATIVE:     ${NATIVE_DIR}"
    echo ""

    # 检查
    if [[ ! -d "$FLINK_DIR" ]]; then
        echo "✗ FLINK_HOME 不存在: ${FLINK_DIR}"
        echo "  请设置: export FLINK_HOME=/path/to/flink"
        exit 1
    fi
    if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
        echo "✗ Docker 镜像 ${IMAGE} 不存在，请先: docker load -i images/eclipse-temurin-8-jre.tar"
        exit 1
    fi

    # 创建网络
    docker network create "$NETWORK" 2>/dev/null || true

    # ---- JobManager ----
    echo "[1/3] 启动 JobManager..."
    docker run -d \
        --name "$JM" \
        --hostname jobmanager \
        --network "$NETWORK" \
        -p 8081:8081 \
        -v "${FLINK_DIR}/bin:/opt/flink/bin:ro" \
        -v "${FLINK_DIR}/lib:/opt/flink/lib:ro" \
        -v "${FLINK_DIR}/plugins:/opt/flink/plugins:ro" \
        -v "${FLINK_DIR}/opt:/opt/flink/opt:ro" \
        -v "${CONF_DIR}:/opt/flink/conf:ro" \
        -v "${NATIVE_DIR}:/opt/flink/native:ro" \
        -e FLINK_HOME=/opt/flink \
        "$IMAGE" \
        /opt/flink/bin/jobmanager.sh start-foreground

    # ---- TaskManager 1 ----
    echo "[2/3] 启动 TaskManager-1..."
    docker run -d \
        --name "$TM1" \
        --hostname taskmanager-1 \
        --network "$NETWORK" \
        -v "${FLINK_DIR}/bin:/opt/flink/bin:ro" \
        -v "${FLINK_DIR}/lib:/opt/flink/lib:ro" \
        -v "${FLINK_DIR}/plugins:/opt/flink/plugins:ro" \
        -v "${FLINK_DIR}/opt:/opt/flink/opt:ro" \
        -v "${CONF_DIR}:/opt/flink/conf:ro" \
        -v "${NATIVE_DIR}:/opt/flink/native:ro" \
        ${L0_OPTS[@]+"${L0_OPTS[@]}"} \
        -e FLINK_HOME=/opt/flink \
        --cpus 4 \
        --memory 16g \
        "$IMAGE" \
        /opt/flink/bin/taskmanager.sh start-foreground

    # ---- TaskManager 2 ----
    echo "[3/3] 启动 TaskManager-2..."
    docker run -d \
        --name "$TM2" \
        --hostname taskmanager-2 \
        --network "$NETWORK" \
        -v "${FLINK_DIR}/bin:/opt/flink/bin:ro" \
        -v "${FLINK_DIR}/lib:/opt/flink/lib:ro" \
        -v "${FLINK_DIR}/plugins:/opt/flink/plugins:ro" \
        -v "${FLINK_DIR}/opt:/opt/flink/opt:ro" \
        -v "${CONF_DIR}:/opt/flink/conf:ro" \
        -v "${NATIVE_DIR}:/opt/flink/native:ro" \
        ${L0_OPTS[@]+"${L0_OPTS[@]}"} \
        -e FLINK_HOME=/opt/flink \
        --cpus 4 \
        --memory 16g \
        "$IMAGE" \
        /opt/flink/bin/taskmanager.sh start-foreground

    # 等待就绪
    echo ""
    echo "等待 JobManager 就绪..."
    for i in $(seq 1 30); do
        if curl -sf http://localhost:8081/overview >/dev/null 2>&1; then
            sleep 3
            echo ""
            echo "=== 集群启动成功! ==="
            echo "  Web UI: http://localhost:8081"
            echo ""
            echo "  查看 L0 日志: $0 logs tm1 | grep -i L0"
            echo "  提交作业:     ${FLINK_DIR}/bin/flink run -m localhost:8081 -c <MainClass> <jar>"
            return 0
        fi
        sleep 2
        printf "."
    done
    echo ""
    echo "⚠ JobManager 未在 60 秒内就绪，查看日志: $0 logs jm"
    return 1
}

do_stop() {
    echo "=== 停止 ForL0 Flink Docker 集群 ==="
    docker rm -f "$TM2" "$TM1" "$JM" 2>/dev/null || true
    docker network rm "$NETWORK" 2>/dev/null || true
    echo "已停止"
}

do_logs() {
    case "${1:-all}" in
        jm)   docker logs -f "$JM" ;;
        tm1)  docker logs -f "$TM1" ;;
        tm2)  docker logs -f "$TM2" ;;
        all)  echo "=== JM ===" && docker logs --tail 20 "$JM" 2>&1; \
              echo "=== TM1 ===" && docker logs --tail 20 "$TM1" 2>&1; \
              echo "=== TM2 ===" && docker logs --tail 20 "$TM2" 2>&1 ;;
        *)    echo "用法: $0 logs [jm|tm1|tm2]" ;;
    esac
}

do_status() {
    echo "=== 容器状态 ==="
    docker ps -a --filter "name=flink-" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
}

# ---- Main ----
case "${1:-}" in
    start)  do_start ;;
    stop)   do_stop ;;
    logs)   do_logs "${2:-all}" ;;
    status) do_status ;;
    *)      usage ;;
esac
