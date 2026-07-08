#!/usr/bin/env bash
################################################################################
#  用纯 docker run 启动 BriskState Flink 集群（不依赖 docker-compose）
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
NATIVE_DIR="${FORL0_NATIVE_DIR:-}"
CONF_DIR="${FORL0_FLINK_CONF_DIR:-$(pwd)/conf}"
NETWORK="flink-net"
IMAGE="eclipse-temurin:8-jre"
DOCKER_BIN="${DOCKER_BIN:-}"
JM_DOCKER_MEMORY="${FORL0_JM_DOCKER_MEMORY:-8g}"
TM_DOCKER_MEMORY="${FORL0_TM_DOCKER_MEMORY:-16g}"

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

DOCKER_BIN="$(detect_docker_bin || true)"
if [[ -z "$DOCKER_BIN" ]]; then
    echo "✗ 当前用户无法访问 Docker。"
    echo "  请确认当前用户可直接执行 docker，或可执行 sudo -n docker。"
    exit 1
fi

if [[ -z "$NATIVE_DIR" ]]; then
    if [[ -f "${FLINK_DIR}/native/libforl0_engine.so" ]]; then
        NATIVE_DIR="${FLINK_DIR}/native"
    elif [[ -f "${REPO_ROOT}/src/main/resources/native/libforl0_engine.so" ]]; then
        NATIVE_DIR="${REPO_ROOT}/src/main/resources/native"
    elif [[ -f "${REPO_ROOT}/artifacts/libforl0_engine.so" ]]; then
        NATIVE_DIR="${REPO_ROOT}/artifacts"
    else
        NATIVE_DIR="${REPO_ROOT}/src/main/resources/native"
    fi
fi

# L0 挂载参数 (仅当硬件存在时)
L0_OPTS=()
NUMA_HOST_PATH="${NUMA_LIB_HOST_PATH:-}"
NUMA_CONTAINER_PATH="${NUMA_LIB_CONTAINER_PATH:-}"

if [[ -z "$NUMA_HOST_PATH" ]]; then
    for candidate in \
        /lib/aarch64-linux-gnu/libnuma.so.1 \
        /usr/lib/aarch64-linux-gnu/libnuma.so.1 \
        /lib64/libnuma.so.1 \
        /usr/lib64/libnuma.so.1; do
        if [[ -f "$candidate" ]]; then
            NUMA_HOST_PATH="$candidate"
            NUMA_CONTAINER_PATH="$candidate"
            break
        fi
    done
fi

if [[ -e /dev/hisi_l0 ]]; then
    L0_OPTS+=(--device /dev/hisi_l0:/dev/hisi_l0)
fi
if [[ -f /usr/lib64/libl0mempool.so ]]; then
    L0_OPTS+=(-v /usr/lib64/libl0mempool.so:/usr/lib/libl0mempool.so:ro)
    L0_OPTS+=(-v /usr/lib64/libl0mempool.so:/usr/lib64/libl0mempool.so:ro)
fi
if [[ -n "$NUMA_HOST_PATH" && -n "$NUMA_CONTAINER_PATH" ]]; then
    L0_OPTS+=(-v "${NUMA_HOST_PATH}:${NUMA_CONTAINER_PATH}:ro")
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
    echo "=== 启动 BriskState Flink Docker 集群 ==="
    echo "  FLINK_HOME: ${FLINK_DIR}"
    echo "  NATIVE:     ${NATIVE_DIR}"
    echo "  CONF:       ${CONF_DIR}"
    echo "  Docker:     ${DOCKER_BIN}"
    echo "  JM memory:  ${JM_DOCKER_MEMORY}"
    echo "  TM memory:  ${TM_DOCKER_MEMORY}"
    if [[ -n "$NUMA_HOST_PATH" && -n "$NUMA_CONTAINER_PATH" ]]; then
        echo "  libnuma:    ${NUMA_HOST_PATH} -> ${NUMA_CONTAINER_PATH}"
    else
        echo "  libnuma:    未检测到；可选原生依赖可能不可用"
    fi
    echo ""

    # 检查
    if [[ ! -d "$FLINK_DIR" ]]; then
        echo "✗ FLINK_HOME 不存在: ${FLINK_DIR}"
        echo "  请设置: export FLINK_HOME=/path/to/flink"
        exit 1
    fi
    if [[ ! -f "${NATIVE_DIR}/libforl0_engine.so" ]]; then
        echo "✗ 未找到 native 库: ${NATIVE_DIR}/libforl0_engine.so"
        echo "  可设置: export FORL0_NATIVE_DIR=/path/to/native-dir"
        exit 1
    fi
    if ! ${DOCKER_BIN} image inspect "$IMAGE" >/dev/null 2>&1; then
        echo "✗ Docker 镜像 ${IMAGE} 不存在，请先: docker load -i images/eclipse-temurin-8-jre.tar.gz"
        exit 1
    fi
    if [[ -z "$NUMA_HOST_PATH" ]]; then
        echo "⚠ 未找到 libnuma.so.1，继续启动，但部分可选原生依赖可能无法加载"
        echo "  如主机路径非常规，请设置: export NUMA_LIB_HOST_PATH=/path/to/libnuma.so.1"
        echo "                        export NUMA_LIB_CONTAINER_PATH=/lib/aarch64-linux-gnu/libnuma.so.1"
    fi

    # 创建网络
    ${DOCKER_BIN} network create "$NETWORK" 2>/dev/null || true

    # ---- JobManager ----
    echo "[1/3] 启动 JobManager..."
    ${DOCKER_BIN} run -d \
        --name "$JM" \
        --hostname jobmanager \
        --network "$NETWORK" \
        --privileged \
        -p 8081:8081 \
        -v "${FLINK_DIR}/bin:/opt/flink/bin:ro" \
        -v "${FLINK_DIR}/lib:/opt/flink/lib:ro" \
        -v "${FLINK_DIR}/plugins:/opt/flink/plugins:ro" \
        -v "${FLINK_DIR}/opt:/opt/flink/opt:ro" \
        -v "${CONF_DIR}:/opt/flink/conf:ro" \
        -v "${NATIVE_DIR}:/opt/flink/native:ro" \
        -e FLINK_HOME=/opt/flink \
        --memory "${JM_DOCKER_MEMORY}" \
        "$IMAGE" \
        /opt/flink/bin/jobmanager.sh start-foreground

    # 获取 JM 容器 IP（Docker 内置 DNS 在某些服务器上不可用，需要用 --add-host）
    JM_IP=$(${DOCKER_BIN} inspect "$JM" --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}')
    echo "      JobManager IP: ${JM_IP}"

    # ---- TaskManager 1 ----
    echo "[2/3] 启动 TaskManager-1..."
    ${DOCKER_BIN} run -d \
        --name "$TM1" \
        --hostname taskmanager-1 \
        --network "$NETWORK" \
        --privileged \
        --add-host jobmanager:${JM_IP} \
        -v "${FLINK_DIR}/bin:/opt/flink/bin:ro" \
        -v "${FLINK_DIR}/lib:/opt/flink/lib:ro" \
        -v "${FLINK_DIR}/plugins:/opt/flink/plugins:ro" \
        -v "${FLINK_DIR}/opt:/opt/flink/opt:ro" \
        -v "${CONF_DIR}:/opt/flink/conf:ro" \
        -v "${NATIVE_DIR}:/opt/flink/native:ro" \
        ${L0_OPTS[@]+"${L0_OPTS[@]}"} \
        -e FLINK_HOME=/opt/flink \
        -e LD_LIBRARY_PATH=/usr/lib:/usr/lib64:/usr/lib/aarch64-linux-gnu:/lib:/lib64:/lib/aarch64-linux-gnu \
        --memory "${TM_DOCKER_MEMORY}" \
        "$IMAGE" \
        /opt/flink/bin/taskmanager.sh start-foreground

    # ---- TaskManager 2 ----
    echo "[3/3] 启动 TaskManager-2..."
    ${DOCKER_BIN} run -d \
        --name "$TM2" \
        --hostname taskmanager-2 \
        --network "$NETWORK" \
        --privileged \
        --add-host jobmanager:${JM_IP} \
        -v "${FLINK_DIR}/bin:/opt/flink/bin:ro" \
        -v "${FLINK_DIR}/lib:/opt/flink/lib:ro" \
        -v "${FLINK_DIR}/plugins:/opt/flink/plugins:ro" \
        -v "${FLINK_DIR}/opt:/opt/flink/opt:ro" \
        -v "${CONF_DIR}:/opt/flink/conf:ro" \
        -v "${NATIVE_DIR}:/opt/flink/native:ro" \
        ${L0_OPTS[@]+"${L0_OPTS[@]}"} \
        -e FLINK_HOME=/opt/flink \
        -e LD_LIBRARY_PATH=/usr/lib:/usr/lib64:/usr/lib/aarch64-linux-gnu:/lib:/lib64:/lib/aarch64-linux-gnu \
        --memory "${TM_DOCKER_MEMORY}" \
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
            echo "  查看 TaskManager 日志: $0 logs tm1"
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
    echo "=== 停止 BriskState Flink Docker 集群 ==="
    ${DOCKER_BIN} rm -f "$TM2" "$TM1" "$JM" 2>/dev/null || true
    ${DOCKER_BIN} network rm "$NETWORK" 2>/dev/null || true
    echo "已停止"
}

do_logs() {
    case "${1:-all}" in
        jm)   ${DOCKER_BIN} logs -f "$JM" ;;
        tm1)  ${DOCKER_BIN} logs -f "$TM1" ;;
        tm2)  ${DOCKER_BIN} logs -f "$TM2" ;;
        all)  echo "=== JM ===" && ${DOCKER_BIN} logs --tail 20 "$JM" 2>&1; \
              echo "=== TM1 ===" && ${DOCKER_BIN} logs --tail 20 "$TM1" 2>&1; \
              echo "=== TM2 ===" && ${DOCKER_BIN} logs --tail 20 "$TM2" 2>&1 ;;
        *)    echo "用法: $0 logs [jm|tm1|tm2]" ;;
    esac
}

do_status() {
    echo "=== 容器状态 ==="
    ${DOCKER_BIN} ps -a --filter "name=flink-" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
}

# ---- Main ----
case "${1:-}" in
    start)  do_start ;;
    stop)   do_stop ;;
    logs)   do_logs "${2:-all}" ;;
    status) do_status ;;
    *)      usage ;;
esac
