#!/usr/bin/env bash
################################################################################
#  鲲鹏服务器一键部署脚本 (在服务器上执行)
#
#  前提:
#    1. 已 clone 仓库到服务器
#    2. Docker 镜像已保存在 docker/images/
#    3. FLINK_HOME 环境变量已设置，或 Flink 安装在 /home/user/flink
#    4. 服务器有 GCC/CMake 用于编译 native 库
#
#  用法: ./server_setup.sh [--skip-native] [--skip-docker-load]
################################################################################

set -euo pipefail
cd "$(dirname "$0")"

REPO_ROOT="$(cd .. && pwd)"

SKIP_NATIVE=false
SKIP_DOCKER_LOAD=false

for arg in "$@"; do
    case $arg in
        --skip-native) SKIP_NATIVE=true ;;
        --skip-docker-load) SKIP_DOCKER_LOAD=true ;;
    esac
done

echo "============================================================"
echo "  ForL0 State Backend - 鲲鹏服务器部署"
echo "============================================================"
echo "  仓库路径: ${REPO_ROOT}"
echo "  FLINK_HOME: ${FLINK_HOME:-/home/user/flink}"
echo ""

# ---- Step 1: 加载 Docker 镜像 ----
if [[ "$SKIP_DOCKER_LOAD" == "false" ]]; then
    IMAGE_FILE="images/eclipse-temurin-8-jre.tar"
    if [[ -f "$IMAGE_FILE" ]]; then
        if docker image inspect eclipse-temurin:8-jre >/dev/null 2>&1; then
            echo "[1/5] Docker 镜像已存在，跳过加载"
        else
            echo "[1/5] 加载 Docker 镜像..."
            docker load -i "$IMAGE_FILE"
            echo "      ✓ 镜像加载成功"
        fi
    else
        echo "[1/5] ✗ 镜像文件不存在: ${IMAGE_FILE}"
        echo "      请先在 macOS 上运行 ./save_docker_image.sh 并提交到仓库"
        exit 1
    fi
else
    echo "[1/5] 跳过 Docker 镜像加载"
fi

# ---- Step 2: 检查 L0 硬件 ----
echo ""
echo "[2/5] 检查 L0 硬件环境..."
L0_AVAILABLE=true

if [[ -f /usr/lib64/libl0mempool.so ]]; then
    echo "      ✓ libl0mempool.so 存在"
else
    echo "      ✗ libl0mempool.so 不存在 (/usr/lib64/libl0mempool.so)"
    L0_AVAILABLE=false
fi

NUMA_OK=false
for numa_candidate in \
    /lib/aarch64-linux-gnu/libnuma.so.1 \
    /usr/lib/aarch64-linux-gnu/libnuma.so.1 \
    /lib64/libnuma.so.1 \
    /usr/lib64/libnuma.so.1; do
    if [[ -f "$numa_candidate" ]]; then
        echo "      ✓ libnuma.so.1 存在 (${numa_candidate})"
        NUMA_OK=true
        break
    fi
done
if [[ "$NUMA_OK" == "false" ]]; then
    echo "      ✗ libnuma.so.1 不存在 (libl0mempool.so 的依赖将导致 dlopen 失败)"
    L0_AVAILABLE=false
fi

if [[ -e /dev/hisi_l0 ]]; then
    echo "      ✓ /dev/hisi_l0 设备存在"
else
    echo "      ✗ /dev/hisi_l0 设备不存在"
    L0_AVAILABLE=false
fi

if [[ "$L0_AVAILABLE" == "true" ]]; then
    echo "      ✓ L0 硬件可用，将使用 L0 Cache 加速"
else
    echo "      ⚠ L0 硬件不可用，将回退到 Heap 内存"
    echo "      (ForL0 StateBackend 仍可工作，只是不使用 L0 Cache)"
fi

# ---- Step 3: 编译 Native 库 ----
if [[ "$SKIP_NATIVE" == "false" ]]; then
    echo ""
    echo "[3/5] 编译 Native 库 (aarch64)..."
    NATIVE_DIR="${REPO_ROOT}/src/main/native"

    # 检查编译工具
    if ! command -v g++ &>/dev/null; then
        echo "      ✗ g++ 未安装，请先安装: yum install gcc-c++ 或 dnf install gcc-c++"
        exit 1
    fi

    # 自动检测 JAVA_HOME（如果未设置）
    if [[ -z "${JAVA_HOME:-}" ]]; then
        # 常见路径
        for jdk_path in /usr/local/java /usr/lib/jvm/java /usr/lib/jvm/java-1.8.0 /usr/lib/jvm/java-8; do
            if [[ -f "${jdk_path}/include/jni.h" ]]; then
                export JAVA_HOME="$jdk_path"
                echo "      自动检测 JAVA_HOME=${JAVA_HOME}"
                break
            fi
        done
        if [[ -z "${JAVA_HOME:-}" ]]; then
            echo "      ✗ JAVA_HOME 未设置且无法自动检测"
            echo "      请设置: export JAVA_HOME=/usr/local/java (或你的 JDK 路径)"
            exit 1
        fi
    fi
    echo "      JAVA_HOME=${JAVA_HOME}"

    cd "$NATIVE_DIR"

    if command -v cmake &>/dev/null; then
        BUILD_DIR="${NATIVE_DIR}/build"
        mkdir -p "$BUILD_DIR"
        cd "$BUILD_DIR"
        cmake .. -DCMAKE_BUILD_TYPE=Release -DFORL0_BUILD_TESTS=OFF
        make -j"$(nproc)"
        RESOURCE_NATIVE="${REPO_ROOT}/src/main/resources/native"
        mkdir -p "$RESOURCE_NATIVE"
        cp libforl0_engine.so "$RESOURCE_NATIVE/"
    else
        echo "      (cmake 未安装，使用 Makefile 编译)"
        make clean
        make JAVA_HOME="${JAVA_HOME}" -j"$(nproc)"
        make JAVA_HOME="${JAVA_HOME}" install
    fi

    echo "      ✓ libforl0_engine.so 编译完成并复制到 resources/native/"
    cd "${REPO_ROOT}/docker"
else
    echo ""
    echo "[3/5] 跳过 Native 库编译"
fi

# ---- Step 4: 安装 ForL0 JAR 到 Flink ----
echo ""
echo "[4/5] 安装 ForL0 JAR..."
FLINK_DIR="${FLINK_HOME:-/home/user/flink}"
JAR_NAME="flink-statebackend-forL0-1.0-SNAPSHOT.jar"
DEPLOY_JAR="${REPO_ROOT}/docker/deploy/${JAR_NAME}"
TARGET_JAR="${REPO_ROOT}/target/${JAR_NAME}"

if [[ ! -d "$FLINK_DIR" ]]; then
    echo "      ✗ FLINK_HOME 不存在: ${FLINK_DIR}"
    echo "      请设置 FLINK_HOME 环境变量指向 Flink 安装目录"
    exit 1
fi

# 优先使用 deploy/ 下的预编译 JAR，其次使用 target/ 下的
# 清理历史大小写不一致的旧包，避免 Flink lib/ 中同时存在多份 backend 实现。
rm -f "${FLINK_DIR}/lib/flink-statebackend-forl0-"*.jar
rm -f "${FLINK_DIR}/lib/flink-statebackend-forL0-"*.jar

if [[ -f "$DEPLOY_JAR" ]]; then
    cp "$DEPLOY_JAR" "${FLINK_DIR}/lib/"
    echo "      ✓ JAR (deploy/) 已复制到 ${FLINK_DIR}/lib/"
elif [[ -f "$TARGET_JAR" ]]; then
    cp "$TARGET_JAR" "${FLINK_DIR}/lib/"
    echo "      ✓ JAR (target/) 已复制到 ${FLINK_DIR}/lib/"
else
    echo "      ✗ JAR 不存在"
    echo "      请在 macOS 上运行 docker/save_docker_image.sh 并推送到仓库"
    exit 1
fi

# ---- Step 5: 启动 Docker 集群 ----
echo ""
echo "[5/5] 启动 Docker 集群..."
cd "${REPO_ROOT}/docker"

export FLINK_HOME="${FLINK_DIR}"
bash docker_run.sh stop 2>/dev/null || true
bash docker_run.sh start

# docker_run.sh start 已包含等待逻辑，这里显示最终状态
echo ""
echo "============================================================"
echo "  部署完成!"
echo "============================================================"
echo "  State Backend: ForL0StateBackend"
echo "  L0 Cache:      ${L0_AVAILABLE}"
echo "  Web UI:        http://localhost:8081"
echo ""
echo "  管理集群:"
echo "    docker/docker_run.sh status   # 查看状态"
echo "    docker/docker_run.sh logs tm1 # 查看 TM1 日志"
echo "    docker/docker_run.sh stop     # 停止集群"
echo ""
echo "  提交作业:"
echo "    ${FLINK_DIR}/bin/flink run -m localhost:8081 -c <MainClass> <jar>"
