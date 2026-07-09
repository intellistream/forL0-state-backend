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
#    ./docker/package_offline_bundle.sh --skip-python-wheels
################################################################################

set -euo pipefail
cd "$(dirname "$0")"

REPO_ROOT="$(cd .. && pwd)"
OUTPUT_DIR="${REPO_ROOT}/offline-artifacts"
SKIP_TESTS=true
SKIP_DOCKER_SAVE=false
COPY_PROFILER=true
DOWNLOAD_PYTHON_WHEELS=true
DOCKER_IMAGE="eclipse-temurin:8-jre"
ARCH=""
DOCKER_PLATFORM=""

detect_sha_cmd() {
    if command -v sha256sum >/dev/null 2>&1; then
        echo "sha256sum"
        return 0
    fi
    if command -v shasum >/dev/null 2>&1; then
        echo "shasum -a 256"
        return 0
    fi
    return 1
}

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
  --skip-docker-save     不导出 docker/images/eclipse-temurin-8-jre.tar.gz
  --skip-python-wheels   不下载 benchmark Python 依赖 wheels
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
        --skip-python-wheels)
            DOWNLOAD_PYTHON_WHEELS=false
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

echo "[1/10] 编译 ForL0 backend"
(cd "$REPO_ROOT" && mvn "${MVN_ARGS[@]}")

echo "[2/10] 编译 native 库"
(cd "$REPO_ROOT/src/main/native" && make clean && make && make install)

echo "[3/10] 编译 WordCount benchmark"
(cd "$REPO_ROOT/benchmark/wordcount" && mvn "${MVN_ARGS[@]}")

if [[ -d "$REPO_ROOT/benchmark/unit-test" ]]; then
    echo "[4/10] 编译 unit-test benchmark"
    (cd "$REPO_ROOT/benchmark/unit-test" && mvn "${MVN_ARGS[@]}")
fi

if [[ -d "$REPO_ROOT/benchmark/nexmark-src" ]]; then
    echo "[5/10] 编译 NexMark"
    (cd "$REPO_ROOT/benchmark/nexmark-src" && mvn "${MVN_ARGS[@]}")
fi

if [[ -d "$REPO_ROOT/client_usecase/XX_6000c_Demo" ]]; then
    echo "[6/10] 编译 client_usecase"
    (cd "$REPO_ROOT/client_usecase/XX_6000c_Demo" && mvn "${MVN_ARGS[@]}")
fi

if [[ -f "$REPO_ROOT/client_usecase/XX_6000c_Demo/target/flink-keyedcoprocessfunction-example-1.0-SNAPSHOT-jar-with-dependencies.jar" ]]; then
    cp -f "$REPO_ROOT/client_usecase/XX_6000c_Demo/target/flink-keyedcoprocessfunction-example-1.0-SNAPSHOT-jar-with-dependencies.jar" "$REPO_ROOT/docker/deploy/"
fi

if [[ -d "$REPO_ROOT/benchmark/client-drift" ]]; then
    if [[ -f "$REPO_ROOT/docker/deploy/flink-keyedcoprocessfunction-example-1.0-SNAPSHOT-jar-with-dependencies.jar" ]]; then
        echo "[6/10] 编译 client hotspot-drift benchmark"
        (cd "$REPO_ROOT/benchmark/client-drift" && mvn "${MVN_ARGS[@]}")
    else
        echo "[6/10] 跳过 client hotspot-drift benchmark（缺少客户 usecase jar）"
    fi
fi

echo "[7/10] 收集离线部署产物到 docker/deploy"
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

if [[ -f "$REPO_ROOT/benchmark/client-drift/target/client-drift-benchmark-1.0-SNAPSHOT.jar" ]]; then
    cp -f "$REPO_ROOT/benchmark/client-drift/target/client-drift-benchmark-1.0-SNAPSHOT.jar" "$REPO_ROOT/docker/deploy/"
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

if [[ "$DOWNLOAD_PYTHON_WHEELS" == "true" ]]; then
    echo "[8/10] 收集 benchmark Python 离线依赖"
    if [[ -d "$REPO_ROOT/benchmark/offline-packages" ]]; then
        cp -a "$REPO_ROOT/benchmark/offline-packages/." "$REPO_ROOT/offline-packages/" 2>/dev/null || true
    fi
    if command -v python3 >/dev/null 2>&1; then
        if python3 -m pip download --only-binary=:all: -r "$REPO_ROOT/benchmark/requirements.txt" -d "$REPO_ROOT/offline-packages"; then
            echo "      ✓ Python wheels 已保存到 offline-packages/"
        else
            echo "      ⚠ Python wheels 下载未完全成功；如目标机离线运行失败，请补齐 offline-packages/"
        fi
    else
        echo "      ⚠ 未找到 python3，跳过 Python wheels 下载"
    fi
else
    echo "[8/10] 跳过 benchmark Python 离线依赖收集"
fi

if [[ "$SKIP_DOCKER_SAVE" == "false" ]]; then
    echo "[9/10] 导出 Docker 镜像 ${DOCKER_IMAGE} (${DOCKER_PLATFORM})"
    if ensure_docker_image_for_platform "$DOCKER_IMAGE" "$DOCKER_PLATFORM"; then
        if docker image inspect "$DOCKER_IMAGE" >/dev/null 2>&1; then
            docker save "$DOCKER_IMAGE" | gzip -9 > "$REPO_ROOT/docker/images/eclipse-temurin-8-jre.tar.gz"
        elif sudo -n docker image inspect "$DOCKER_IMAGE" >/dev/null 2>&1; then
            sudo -n docker save "$DOCKER_IMAGE" | gzip -9 > "$REPO_ROOT/docker/images/eclipse-temurin-8-jre.tar.gz"
        fi
    else
        echo "      ⚠ 无法获取镜像 ${DOCKER_IMAGE} (${DOCKER_PLATFORM})，跳过导出"
    fi
else
    echo "[9/10] 跳过 Docker 镜像导出"
fi

cp -a "$REPO_ROOT/docker/deploy/." "$OUTPUT_DIR/"
cp -a "$REPO_ROOT/docker/images/." "$OUTPUT_DIR/" 2>/dev/null || true
cp -a "$REPO_ROOT/offline-packages/." "$OUTPUT_DIR/" 2>/dev/null || true

mkdir -p "$OUTPUT_DIR/artifacts" \
         "$OUTPUT_DIR/benchmark" \
         "$OUTPUT_DIR/docker" \
         "$OUTPUT_DIR/docker/images" \
         "$OUTPUT_DIR/docs"

cp -a "$REPO_ROOT/docker/deploy/." "$OUTPUT_DIR/artifacts/"
cp -a "$REPO_ROOT/docker/images/." "$OUTPUT_DIR/docker/images/" 2>/dev/null || true
cp -a "$REPO_ROOT/offline-packages" "$OUTPUT_DIR/benchmark/" 2>/dev/null || true
cp -a "$REPO_ROOT/benchmark/config" "$OUTPUT_DIR/benchmark/"
cp -a "$REPO_ROOT/benchmark/scripts" "$OUTPUT_DIR/benchmark/"
rm -rf "$OUTPUT_DIR/benchmark/scripts/__pycache__"
cp -f "$REPO_ROOT/benchmark/requirements.txt" "$OUTPUT_DIR/benchmark/"
cp -f "$REPO_ROOT/benchmark/README.md" "$OUTPUT_DIR/benchmark/"
if [[ -f "$REPO_ROOT/README.md" ]]; then
    cp -f "$REPO_ROOT/README.md" "$OUTPUT_DIR/"
fi
if [[ -f "$REPO_ROOT/forl0-offline-app.sh" ]]; then
    cp -f "$REPO_ROOT/forl0-offline-app.sh" "$OUTPUT_DIR/"
    chmod +x "$OUTPUT_DIR/forl0-offline-app.sh"
fi
cp -a "$REPO_ROOT/docker/conf" "$OUTPUT_DIR/docker/"
for script in docker_run.sh server_setup.sh run_all_apps.sh install_offline_bundle.sh start.sh stop.sh restart.sh; do
    if [[ -f "$REPO_ROOT/docker/$script" ]]; then
        cp -f "$REPO_ROOT/docker/$script" "$OUTPUT_DIR/docker/"
    fi
done
if [[ -d "$REPO_ROOT/交付文档" ]]; then
    cp -a "$REPO_ROOT/交付文档" "$OUTPUT_DIR/docs/"
elif [[ -d "$REPO_ROOT/docs" ]]; then
    cp -a "$REPO_ROOT/docs/." "$OUTPUT_DIR/docs/"
fi

echo "[10/10] 生成离线校验清单"
MANIFEST_FILE="$OUTPUT_DIR/offline_bundle_manifest.txt"
CHECKSUM_FILE="$OUTPUT_DIR/offline_bundle_sha256.txt"
{
    echo "ForL0 Offline Bundle Manifest"
    echo "GeneratedAt: $(date '+%Y-%m-%d %H:%M:%S %z')"
    echo "Arch: ${ARCH}"
    echo "DockerPlatform: ${DOCKER_PLATFORM}"
    echo ""
    (
        cd "$OUTPUT_DIR"
        find . -type f ! -name "offline_bundle_manifest.txt" ! -name "offline_bundle_sha256.txt" -printf '%P\n' | sort
    )
} > "$MANIFEST_FILE"

SHA_CMD="$(detect_sha_cmd || true)"
if [[ -n "$SHA_CMD" ]]; then
    (
        cd "$OUTPUT_DIR"
        find . -type f ! -name "offline_bundle_sha256.txt" -print0 | sort -z | xargs -0 $SHA_CMD
    ) > "$CHECKSUM_FILE"
    echo "      ✓ 清单: ${MANIFEST_FILE}"
    echo "      ✓ 校验: ${CHECKSUM_FILE}"
else
    echo "      ⚠ 未找到 sha256sum/shasum，跳过 SHA256 文件生成"
fi

echo ""
echo "打包完成。"
echo "  部署产物目录: ${REPO_ROOT}/docker/deploy"
echo "  离线镜像目录: ${REPO_ROOT}/docker/images"
echo "  额外输出目录: ${OUTPUT_DIR}"
