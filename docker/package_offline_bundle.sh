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
#    ./docker/package_offline_bundle.sh --arch arm64 --python-version 3.10
################################################################################

set -euo pipefail
cd "$(dirname "$0")"

REPO_ROOT="$(cd .. && pwd)"
OUTPUT_DIR="${REPO_ROOT}/offline-artifacts"
SKIP_TESTS=true
SKIP_BUILD=false
SKIP_DOCKER_SAVE=false
COPY_PROFILER=true
DOWNLOAD_PYTHON_WHEELS=true
DOWNLOAD_PORTABLE_PYTHON=true
DOWNLOAD_FLINK_DISTRIBUTION=true
DOCKER_IMAGE="eclipse-temurin:8-jre"
ARCH=""
DOCKER_PLATFORM=""
PYTHON_VERSION=""
PIP_PLATFORM=""
PORTABLE_PYTHON_FULL_VERSION=""
PORTABLE_PYTHON_RELEASE="20260718"
FLINK_VERSION="1.20.3"

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

arch_to_pip_platform() {
    case "$1" in
        arm64)
            echo "manylinux2014_aarch64"
            ;;
        x64)
            echo "manylinux2014_x86_64"
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

copy_deploy_artifacts() {
    local destination="$1"
    local artifact
    local -a artifact_names=(
        "flink-statebackend-forL0-1.0-SNAPSHOT.jar"
        "libforl0_engine.so"
        "wordcount-benchmark-1.0-SNAPSHOT.jar"
        "unit-test-benchmark-1.0-SNAPSHOT.jar"
        "nexmark-flink-0.3-SNAPSHOT.jar"
        "flink-keyedcoprocessfunction-example-1.0-SNAPSHOT-jar-with-dependencies.jar"
        "client-drift-benchmark-1.0-SNAPSHOT.jar"
    )
    mkdir -p "$destination"
    for artifact in "${artifact_names[@]}"; do
        if [[ -f "$REPO_ROOT/docker/deploy/$artifact" ]]; then
            cp -f "$REPO_ROOT/docker/deploy/$artifact" "$destination/"
        fi
    done
    for artifact in \
        "flink-statebackend-forL0-1.0-SNAPSHOT.jar" \
        "libforl0_engine.so" \
        "wordcount-benchmark-1.0-SNAPSHOT.jar"; do
        if [[ ! -f "$destination/$artifact" ]]; then
            echo "✗ 缺少必需部署产物: $artifact"
            exit 1
        fi
    done
}

usage() {
    cat <<'EOF'
用法:
  ./docker/package_offline_bundle.sh [选项]

选项:
  --output-dir PATH      产物输出目录，默认 offline-artifacts/
  --with-tests           编译时执行测试（默认跳过）
  --skip-tests           编译时跳过测试（默认）
  --skip-build           复用 docker/deploy 中已构建产物，只重新组包
  --skip-docker-save     不导出 docker/images/eclipse-temurin-8-jre.tar.gz
  --skip-python-wheels   不下载 benchmark Python 依赖 wheels
  --skip-portable-python 不下载可携带 CPython 运行时（目标机已有兼容 Python 时使用）
  --skip-flink           不下载完整 Flink 分发包（目标机已有完整 FLINK_HOME 时使用）
  --no-profiler          不处理 async-profiler 离线包
  --arch NAME            目标架构: arm64 或 x64（默认按当前机器自动判断）
  --python-version X.Y   目标 Linux 的 CPython 版本（默认使用本机 python3）
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
        --skip-build)
            SKIP_BUILD=true
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
        --skip-portable-python)
            DOWNLOAD_PORTABLE_PYTHON=false
            shift
            ;;
        --skip-flink)
            DOWNLOAD_FLINK_DISTRIBUTION=false
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
        --python-version)
            PYTHON_VERSION="$2"
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

if [[ "$OUTPUT_DIR" != /* ]]; then
    OUTPUT_DIR="${REPO_ROOT}/${OUTPUT_DIR#./}"
fi

if [[ -z "$ARCH" ]]; then
    ARCH="$(detect_default_arch)"
fi

if [[ "$ARCH" != "arm64" && "$ARCH" != "x64" ]]; then
    echo "✗ 无法识别目标架构，请显式传入 --arch arm64 或 --arch x64"
    exit 1
fi

DOCKER_PLATFORM="$(arch_to_platform "$ARCH")"
PIP_PLATFORM="$(arch_to_pip_platform "$ARCH")"

if [[ -z "$PYTHON_VERSION" ]]; then
    if command -v python3 >/dev/null 2>&1; then
        PYTHON_VERSION="$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
    else
        echo "✗ 未找到 python3；请安装 Python 3 或显式传入 --python-version X.Y"
        exit 1
    fi
fi
if [[ ! "$PYTHON_VERSION" =~ ^3\.[0-9]+$ ]]; then
    echo "✗ Python 版本格式无效: ${PYTHON_VERSION}（应为 3.10、3.11 等）"
    exit 1
fi
PYTHON_VERSION_DIGITS="$(printf '%s' "$PYTHON_VERSION" | tr -d '.')"
PYTHON_ABI="cp${PYTHON_VERSION_DIGITS}"
case "$PYTHON_VERSION" in
    3.10) PORTABLE_PYTHON_FULL_VERSION="3.10.20" ;;
    3.11) PORTABLE_PYTHON_FULL_VERSION="3.11.15" ;;
    3.12) PORTABLE_PYTHON_FULL_VERSION="3.12.13" ;;
    *)
        if [[ "$DOWNLOAD_PORTABLE_PYTHON" == "true" ]]; then
            echo "✗ 暂无 Python ${PYTHON_VERSION} 的固定可携带运行时；请使用 3.10/3.11/3.12 或 --skip-portable-python"
            exit 1
        fi
        ;;
esac

MVN_ARGS=(clean package)
if [[ "$SKIP_TESTS" == "true" ]]; then
    MVN_ARGS+=(-DskipTests)
fi

WHEEL_OUTPUT_DIR="${OUTPUT_DIR}/benchmark/offline-packages"
mkdir -p "${REPO_ROOT}/docker/deploy" "${REPO_ROOT}/docker/images" "$WHEEL_OUTPUT_DIR"

echo "============================================================"
echo "  ForL0 一键离线打包"
echo "============================================================"
echo "  Repo:       ${REPO_ROOT}"
echo "  OutputDir:  ${OUTPUT_DIR}"
echo "  Arch:       ${ARCH} (${DOCKER_PLATFORM})"
echo "  Python:     CPython ${PYTHON_VERSION} (${PIP_PLATFORM}, ${PYTHON_ABI})"
echo ""

if [[ "$SKIP_BUILD" == "true" ]]; then
    echo "[1-7/10] 复用 docker/deploy 中已构建产物"
else
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
        tar -czf "$WHEEL_OUTPUT_DIR/${profiler_name}.tar.gz" -C "$(dirname "$ASYNC_PROFILER_HOME")" "$profiler_name"
        echo "      ✓ async-profiler 已打包到 benchmark/offline-packages/${profiler_name}.tar.gz"
    else
        profiler_archive=""
        for candidate in \
            "$REPO_ROOT/benchmark/offline-packages/async-profiler-4.4-linux-${ARCH}.tar.gz" \
            "$REPO_ROOT/offline-packages/async-profiler-4.4-linux-${ARCH}.tar.gz"; do
            if [[ -f "$candidate" ]]; then
                profiler_archive="$candidate"
                break
            fi
        done
        if [[ -n "$profiler_archive" ]]; then
            cp -f "$profiler_archive" "$WHEEL_OUTPUT_DIR/"
            echo "      ✓ 已复用 $(basename "$profiler_archive")"
        else
            echo "      ⚠ 未设置 ASYNC_PROFILER_HOME 且没有匹配 ${ARCH} 的离线包，跳过"
        fi
    fi
fi

if [[ "$DOWNLOAD_PYTHON_WHEELS" == "true" ]]; then
    echo "[8/10] 收集 benchmark Python 离线依赖"
    if command -v python3 >/dev/null 2>&1; then
        if python3 -m pip download \
            --only-binary=:all: \
            --platform "$PIP_PLATFORM" \
            --implementation cp \
            --python-version "$PYTHON_VERSION_DIGITS" \
            --abi "$PYTHON_ABI" \
            -r "$REPO_ROOT/benchmark/requirements.txt" \
            -d "$WHEEL_OUTPUT_DIR"; then
            echo "      ✓ Linux ${ARCH} / CPython ${PYTHON_VERSION} wheels 已保存到 benchmark/offline-packages/"
        else
            echo "      ⚠ Python wheels 下载未完全成功；如目标机离线运行失败，请补齐 offline-packages/"
        fi
    else
        echo "      ⚠ 未找到 python3，跳过 Python wheels 下载"
    fi
else
    echo "[8/10] 跳过 benchmark Python 离线依赖收集"
fi

if [[ "$DOWNLOAD_PORTABLE_PYTHON" == "true" ]]; then
    echo "[8b/10] 收集可携带 CPython ${PORTABLE_PYTHON_FULL_VERSION} 运行时"
    case "$ARCH" in
        arm64) portable_target="aarch64-unknown-linux-gnu" ;;
        x64) portable_target="x86_64-unknown-linux-gnu" ;;
    esac
    portable_name="cpython-${PORTABLE_PYTHON_FULL_VERSION}+${PORTABLE_PYTHON_RELEASE}-${portable_target}-install_only_stripped.tar.gz"
    portable_url="https://github.com/astral-sh/python-build-standalone/releases/download/${PORTABLE_PYTHON_RELEASE}/${portable_name}"
    portable_tmp="$(mktemp -d /tmp/forl0-portable-python.XXXXXX)"
    if command -v curl >/dev/null 2>&1; then
        curl -fL --retry 5 --retry-all-errors -o "$portable_tmp/$portable_name" "$portable_url"
    elif command -v wget >/dev/null 2>&1; then
        wget -O "$portable_tmp/$portable_name" "$portable_url"
    else
        echo "✗ 下载可携带 Python 需要 curl 或 wget"
        exit 1
    fi
    mkdir -p "$OUTPUT_DIR/tools"
    tar -xzf "$portable_tmp/$portable_name" -C "$OUTPUT_DIR/tools"
    [[ -x "$OUTPUT_DIR/tools/python/bin/python3" ]] || {
        echo "✗ 可携带 Python 归档结构无效: $portable_name"
        exit 1
    }
    "$OUTPUT_DIR/tools/python/bin/python3" --version
    printf '%s\n' "$portable_url" > "$OUTPUT_DIR/tools/python-runtime-source.txt"
else
    echo "[8b/10] 跳过可携带 CPython 运行时"
fi

if [[ "$DOWNLOAD_FLINK_DISTRIBUTION" == "true" ]]; then
    echo "[8c/10] 收集完整 Apache Flink ${FLINK_VERSION} 分发包"
    flink_name="flink-${FLINK_VERSION}-bin-scala_2.12.tgz"
    flink_url="https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/${flink_name}"
    mkdir -p "$OUTPUT_DIR/tools/flink"
    if [[ ! -f "$OUTPUT_DIR/tools/flink/$flink_name" ]]; then
        if command -v curl >/dev/null 2>&1; then
            curl -fL --retry 5 --retry-all-errors -o "$OUTPUT_DIR/tools/flink/$flink_name" "$flink_url"
        elif command -v wget >/dev/null 2>&1; then
            wget -O "$OUTPUT_DIR/tools/flink/$flink_name" "$flink_url"
        else
            echo "✗ 下载 Flink 需要 curl 或 wget"
            exit 1
        fi
    fi
    tar -tzf "$OUTPUT_DIR/tools/flink/$flink_name" '*/bin/flink' >/dev/null
    printf '%s\n' "$flink_url" > "$OUTPUT_DIR/tools/flink/flink-runtime-source.txt"
    echo "      ✓ 已加入 $flink_name"
else
    echo "[8c/10] 跳过完整 Flink 分发包"
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

mkdir -p "$OUTPUT_DIR/artifacts" \
         "$OUTPUT_DIR/benchmark" \
         "$OUTPUT_DIR/docker" \
         "$OUTPUT_DIR/docker/images" \
         "$OUTPUT_DIR/docs"

copy_deploy_artifacts "$OUTPUT_DIR/artifacts"
if [[ -f "$REPO_ROOT/docker/images/eclipse-temurin-8-jre.tar.gz" ]]; then
    cp -f "$REPO_ROOT/docker/images/eclipse-temurin-8-jre.tar.gz" "$OUTPUT_DIR/docker/images/"
fi
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
if [[ -f "$REPO_ROOT/run-forl0-offline.sh" ]]; then
    cp -f "$REPO_ROOT/run-forl0-offline.sh" "$OUTPUT_DIR/"
    chmod +x "$OUTPUT_DIR/run-forl0-offline.sh"
fi
cp -a "$REPO_ROOT/docker/conf" "$OUTPUT_DIR/docker/"
for script in docker_run.sh server_setup.sh run_all_apps.sh install_offline_bundle.sh start.sh stop.sh restart.sh; do
    if [[ -f "$REPO_ROOT/docker/$script" ]]; then
        cp -f "$REPO_ROOT/docker/$script" "$OUTPUT_DIR/docker/"
    fi
done
if [[ -f "$REPO_ROOT/docker/docker-compose.yml" ]]; then
    cp -f "$REPO_ROOT/docker/docker-compose.yml" "$OUTPUT_DIR/docker/"
fi
if [[ -d "$REPO_ROOT/docker/lib" ]]; then
    cp -a "$REPO_ROOT/docker/lib" "$OUTPUT_DIR/docker/"
    chmod +x "$OUTPUT_DIR/docker/lib/l0_detector.sh"
fi
if [[ -f "$REPO_ROOT/docker/download_offline_python_wheels.ps1" ]]; then
    cp -f "$REPO_ROOT/docker/download_offline_python_wheels.ps1" "$OUTPUT_DIR/docker/"
fi
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
    echo "PythonVersion: ${PYTHON_VERSION}"
    echo "PipPlatform: ${PIP_PLATFORM}"
    echo "PythonABI: ${PYTHON_ABI}"
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
