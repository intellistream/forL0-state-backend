#!/usr/bin/env bash
################################################################################
#  ForL0 一键离线打包脚本
#
#  目标:
#    1. 编译核心 backend 与 native 库
#    2. 编译 benchmark / client usecase 产物
#    3. 收集部署所需文件到 docker/deploy 与 docker/images
#    4. 可选收集 async-profiler 离线包
#
#  用法:
#    ./docker/package_offline_bundle.sh
#    ./docker/package_offline_bundle.sh --skip-tests
#    ./docker/package_offline_bundle.sh --skip-docker-save
#    ./docker/package_offline_bundle.sh --output-dir /tmp/offline-artifacts
################################################################################

set -euo pipefail
cd "$(dirname "$0")"

REPO_ROOT="$(cd .. && pwd)"
OUTPUT_DIR="${REPO_ROOT}/offline-artifacts"
SKIP_TESTS=true
SKIP_DOCKER_SAVE=false
COPY_PROFILER=true
DOCKER_IMAGE="eclipse-temurin:8-jre"
ARCH=""
DOCKER_PLATFORM=""

detect_default_arch() {
    local machine
    machine="$(uname -m)"
    case "$machine" in
        aarch64|arm64)
            echo "arm64"
            ;;
        x86_64|amd64)
            echo "x64"
            ;;
        *)
            echo ""
            ;;
    esac
}

arch_to_platform() {
    case "$1" in
        arm64)
            echo "linux/arm64"
            ;;
        x64)
            echo "linux/amd64"
            ;;
        *)
            return 1
            ;;
    esac
}

ensure_docker_image_for_platform() {
    local image="$1"
    local platform="$2"
    if docker image inspect "$image" >/dev/null 2>&1; then
        return 0
    fi
    if sudo -n docker image inspect "$image" >/dev/null 2>&1; then
        return 0
    fi
    echo "      ⚠ 本机未找到镜像 ${image}，尝试按平台拉取 ${platform}"
    if docker pull --platform "$platform" "$image" >/dev/null 2>&1; then
        return 0
    fi
    if sudo -n docker pull --platform "$platform" "$image" >/dev/null 2>&1; then
        return 0
    fi
    return 1
}

usage() {
    cat <<'EOF'
用法:
  ./docker/package_offline_bundle.sh [选项]

选项:
  --output-dir PATH      产物输出目录，默认 offline-artifacts/
  --with-tests           编译时执行测试（默认跳过）
  --skip-tests           编译时跳过测试（默认）
  --skip-docker-save     不导出 docker/images/eclipse-temurin-8-jre.tar
  --no-profiler          不处理 async-profiler 离线包
    --arch NAME            目标架构: arm64 或 x64（默认按当前机器自动判断）
  -h, --help             显示帮助
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --with-tests)
            SKIP_TESTS=false
            shift
            ;;
        --skip-tests)
            SKIP_TESTS=true
            shift
            ;;
        --skip-docker-save)
            SKIP_DOCKER_SAVE=true
            shift
            ;;
        --no-profiler)
            COPY_PROFILER=false
            shift
            ;;
        --arch)
            ARCH="$2"
            shift 2
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

if [[ -z "$ARCH" ]]; then
    ARCH="$(detect_default_arch)"
fi

if [[ "$ARCH" != "arm64" && "$ARCH" != "x64" ]]; then
    echo "✗ 无法识别目标架构，请显式传入 --arch arm64 或 --arch x64"
    exit 1
fi

DOCKER_PLATFORM="$(arch_to_platform "$ARCH")"

MVN_ARGS=(clean package)
if [[ "$SKIP_TESTS" == "true" ]]; then
    MVN_ARGS+=(-DskipTests)
fi

mkdir -p "${REPO_ROOT}/docker/deploy" "${REPO_ROOT}/docker/images" "${REPO_ROOT}/offline-packages" "$OUTPUT_DIR"

echo "============================================================"
echo "  ForL0 一键离线打包"
echo "============================================================"
echo "  Repo:       ${REPO_ROOT}"
echo "  OutputDir:  ${OUTPUT_DIR}"
echo "  Arch:       ${ARCH} (${DOCKER_PLATFORM})"
echo ""

echo "[1/8] 编译 ForL0 backend"
(cd "$REPO_ROOT" && mvn "${MVN_ARGS[@]}")

echo "[2/8] 编译 native 库"
(cd "$REPO_ROOT/src/main/native" && make clean && make && make install)

echo "[3/8] 编译 WordCount benchmark"
(cd "$REPO_ROOT/benchmark/wordcount" && mvn "${MVN_ARGS[@]}")

if [[ -d "$REPO_ROOT/benchmark/unit-test" ]]; then
    echo "[4/8] 编译 unit-test benchmark"
    (cd "$REPO_ROOT/benchmark/unit-test" && mvn "${MVN_ARGS[@]}")
fi

if [[ -d "$REPO_ROOT/benchmark/nexmark-src" ]]; then
    echo "[5/8] 编译 NexMark"
    (cd "$REPO_ROOT/benchmark/nexmark-src" && mvn "${MVN_ARGS[@]}")
fi

if [[ -d "$REPO_ROOT/client_usecase/XX_6000c_Demo" ]]; then
    echo "[6/8] 编译 client_usecase"
    (cd "$REPO_ROOT/client_usecase/XX_6000c_Demo" && mvn "${MVN_ARGS[@]}")
fi

echo "[7/8] 收集离线部署产物到 docker/deploy"
cp -f "$REPO_ROOT/target/flink-statebackend-forL0-1.0-SNAPSHOT.jar" "$REPO_ROOT/docker/deploy/"

if [[ -f "$REPO_ROOT/src/main/resources/native/libforl0_engine.so" ]]; then
    cp -f "$REPO_ROOT/src/main/resources/native/libforl0_engine.so" "$REPO_ROOT/docker/deploy/libforl0_engine.so"
fi

if [[ -f "$REPO_ROOT/benchmark/wordcount/target/wordcount-benchmark-1.0-SNAPSHOT.jar" ]]; then
    cp -f "$REPO_ROOT/benchmark/wordcount/target/wordcount-benchmark-1.0-SNAPSHOT.jar" "$REPO_ROOT/docker/deploy/"
fi

if [[ -f "$REPO_ROOT/benchmark/unit-test/target/unit-test-benchmark-1.0-SNAPSHOT.jar" ]]; then
    cp -f "$REPO_ROOT/benchmark/unit-test/target/unit-test-benchmark-1.0-SNAPSHOT.jar" "$REPO_ROOT/docker/deploy/"
fi

if [[ -f "$REPO_ROOT/benchmark/nexmark-src/nexmark-flink/target/nexmark-flink-bin/nexmark-flink/lib/nexmark-flink-0.3-SNAPSHOT.jar" ]]; then
    cp -f "$REPO_ROOT/benchmark/nexmark-src/nexmark-flink/target/nexmark-flink-bin/nexmark-flink/lib/nexmark-flink-0.3-SNAPSHOT.jar" "$REPO_ROOT/docker/deploy/"
fi

if [[ -f "$REPO_ROOT/client_usecase/XX_6000c_Demo/target/flink-keyedcoprocessfunction-example-1.0-SNAPSHOT-jar-with-dependencies.jar" ]]; then
    cp -f "$REPO_ROOT/client_usecase/XX_6000c_Demo/target/flink-keyedcoprocessfunction-example-1.0-SNAPSHOT-jar-with-dependencies.jar" "$REPO_ROOT/docker/deploy/"
fi

if [[ "$COPY_PROFILER" == "true" ]]; then
    if [[ -n "${ASYNC_PROFILER_HOME:-}" && -x "${ASYNC_PROFILER_HOME}/bin/asprof" ]]; then
        profiler_name="$(basename "$ASYNC_PROFILER_HOME")"
        if [[ "$ARCH" == "arm64" && "$profiler_name" != *"arm64"* ]]; then
            echo "      ✗ ASYNC_PROFILER_HOME 与目标架构不匹配（期望 arm64）: ${ASYNC_PROFILER_HOME}"
            exit 1
        fi
        if [[ "$ARCH" == "x64" && "$profiler_name" != *"x64"* && "$profiler_name" != *"amd64"* ]]; then
            echo "      ✗ ASYNC_PROFILER_HOME 与目标架构不匹配（期望 x64）: ${ASYNC_PROFILER_HOME}"
            exit 1
        fi
        tar -czf "$REPO_ROOT/offline-packages/${profiler_name}.tar.gz" -C "$(dirname "$ASYNC_PROFILER_HOME")" "$profiler_name"
        echo "      ✓ async-profiler 已打包到 offline-packages/${profiler_name}.tar.gz"
    else
        echo "      ⚠ 未设置 ASYNC_PROFILER_HOME，跳过 async-profiler 打包"
    fi
fi

if [[ "$SKIP_DOCKER_SAVE" == "false" ]]; then
    echo "[8/8] 导出 Docker 镜像 ${DOCKER_IMAGE} (${DOCKER_PLATFORM})"
    if ensure_docker_image_for_platform "$DOCKER_IMAGE" "$DOCKER_PLATFORM"; then
        if docker image inspect "$DOCKER_IMAGE" >/dev/null 2>&1; then
            docker save "$DOCKER_IMAGE" -o "$REPO_ROOT/docker/images/eclipse-temurin-8-jre.tar"
        elif sudo -n docker image inspect "$DOCKER_IMAGE" >/dev/null 2>&1; then
            sudo -n docker save "$DOCKER_IMAGE" -o "$REPO_ROOT/docker/images/eclipse-temurin-8-jre.tar"
        fi
    else
        echo "      ⚠ 无法获取镜像 ${DOCKER_IMAGE} (${DOCKER_PLATFORM})，跳过导出"
    fi
else
    echo "[8/8] 跳过 Docker 镜像导出"
fi

cp -a "$REPO_ROOT/docker/deploy/." "$OUTPUT_DIR/"
cp -a "$REPO_ROOT/docker/images/." "$OUTPUT_DIR/" 2>/dev/null || true
cp -a "$REPO_ROOT/offline-packages/." "$OUTPUT_DIR/" 2>/dev/null || true

echo ""
echo "打包完成。"
echo "  部署产物目录: ${REPO_ROOT}/docker/deploy"
echo "  离线镜像目录: ${REPO_ROOT}/docker/images"
echo "  额外输出目录: ${OUTPUT_DIR}"
