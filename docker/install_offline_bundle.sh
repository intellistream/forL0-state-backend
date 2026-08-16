#!/usr/bin/env bash
################################################################################
#  ForL0 离线包一键安装脚本
#
#  用法:
#    ./docker/install_offline_bundle.sh --flink-home /path/to/flink
#    ./docker/install_offline_bundle.sh --flink-home /path/to/flink --start-docker
#    ./docker/install_offline_bundle.sh --flink-home /path/to/flink --install-dir ~/forl0-runtime
#
#  说明:
#    1. 脚本默认在解压后的离线包目录内执行
#    2. 不依赖网络，不会尝试下载任何内容
#    3. 如需启动 Docker 集群，目标机必须已提前准备好本地 Docker 镜像或离线包内包含 docker/images/*.tar.gz 或 *.tar
################################################################################

set -euo pipefail
cd "$(dirname "$0")"

BUNDLE_ROOT="$(cd .. && pwd)"
INSTALL_DIR="${HOME}/forl0-runtime"
FLINK_DIR="${FLINK_HOME:-${HOME}/flink_home}"
START_DOCKER=false
COPY_PROFILER=false

ensure_flink_distribution() {
    local flink_archive=""
    if [[ -x "${FLINK_DIR}/bin/flink" ]]; then
        echo "      ✓ 已发现完整 Flink: ${FLINK_DIR}/bin/flink"
        return 0
    fi

    flink_archive="$(find "${BUNDLE_ROOT}/tools/flink" -maxdepth 1 -type f \
        \( -name 'flink-*-bin-scala_*.tgz' -o -name 'flink-*-bin-scala_*.tar.gz' \) \
        -print -quit 2>/dev/null || true)"
    if [[ -z "$flink_archive" ]]; then
        echo "✗ FLINK_HOME 不完整，缺少可执行文件: ${FLINK_DIR}/bin/flink" >&2
        echo "  离线包中也没有完整 Flink 分发包: ${BUNDLE_ROOT}/tools/flink/" >&2
        return 1
    fi

    echo "      ⚠ FLINK_HOME 不完整，正在从离线包安装 Flink"
    echo "        archive: $flink_archive"
    echo "        target:  $FLINK_DIR"
    mkdir -p "$FLINK_DIR"
    tar -xzf "$flink_archive" -C "$FLINK_DIR" --strip-components=1
    chmod +x "$FLINK_DIR/bin/"*.sh "$FLINK_DIR/bin/flink" 2>/dev/null || true
    if [[ ! -x "$FLINK_DIR/bin/flink" ]]; then
        echo "✗ Flink 解压后仍缺少可执行文件: $FLINK_DIR/bin/flink" >&2
        return 1
    fi
    echo "      ✓ Flink 已安装: $FLINK_DIR"
}

usage() {
    cat <<'EOF'
用法:
  ./docker/install_offline_bundle.sh [选项]

选项:
  --flink-home PATH     Flink 安装目录（默认 $HOME/flink_home，也可通过 FLINK_HOME 提供）
  --install-dir PATH    安装输出目录，默认 ~/forl0-runtime
  --start-docker        安装完成后直接启动 docker/docker_run.sh start
  --copy-profiler       将 tools/async-profiler-4.4-linux-arm64 复制到安装目录
  -h, --help            显示帮助
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --flink-home)
            FLINK_DIR="$2"
            shift 2
            ;;
        --install-dir)
            INSTALL_DIR="$2"
            shift 2
            ;;
        --start-docker)
            START_DOCKER=true
            shift
            ;;
        --copy-profiler)
            COPY_PROFILER=true
            shift
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

pick_first_file() {
    for candidate in "$@"; do
        if [[ -f "$candidate" ]]; then
            echo "$candidate"
            return 0
        fi
    done
    return 1
}

pick_first_dir() {
    for candidate in "$@"; do
        if [[ -d "$candidate" ]]; then
            echo "$candidate"
            return 0
        fi
    done
    return 1
}

BACKEND_JAR="$(pick_first_file \
    "${BUNDLE_ROOT}/artifacts/flink-statebackend-forL0-1.0-SNAPSHOT.jar" \
    "${BUNDLE_ROOT}/flink-statebackend-forL0-1.0-SNAPSHOT.jar" \
    "${BUNDLE_ROOT}/flink-statebackend-forl0-1.0-SNAPSHOT.jar")"
NATIVE_LIB="$(pick_first_file \
    "${BUNDLE_ROOT}/artifacts/libforl0_engine.so" \
    "${BUNDLE_ROOT}/libforl0_engine.so")"
WORDCOUNT_JAR="$(pick_first_file \
    "${BUNDLE_ROOT}/artifacts/wordcount-benchmark-1.0-SNAPSHOT.jar" \
    "${BUNDLE_ROOT}/wordcount-benchmark-1.0-SNAPSHOT.jar")"
UNITTEST_JAR="$(pick_first_file \
    "${BUNDLE_ROOT}/artifacts/unit-test-benchmark-1.0-SNAPSHOT.jar" \
    "${BUNDLE_ROOT}/unit-test-benchmark-1.0-SNAPSHOT.jar" || true)"
NEXMARK_JAR="$(pick_first_file \
    "${BUNDLE_ROOT}/artifacts/nexmark-flink-0.3-SNAPSHOT.jar" \
    "${BUNDLE_ROOT}/nexmark-flink-0.3-SNAPSHOT.jar" || true)"
NEXMARK_DIST="$(pick_first_dir \
    "${BUNDLE_ROOT}/artifacts/nexmark-flink" \
    "${BUNDLE_ROOT}/docker/deploy/nexmark-flink" || true)"
PROFILER_ARCHIVE="$(pick_first_file \
    "${BUNDLE_ROOT}/benchmark/offline-packages/async-profiler-4.4-linux-arm64.tar.gz" \
    "${BUNDLE_ROOT}/benchmark/offline-packages/async-profiler-4.4-linux-x64.tar.gz" \
    "${BUNDLE_ROOT}/async-profiler-4.4-linux-arm64.tar.gz" \
    "${BUNDLE_ROOT}/async-profiler-4.4-linux-x64.tar.gz" || true)"

for required in "$BACKEND_JAR" "$NATIVE_LIB" "$WORDCOUNT_JAR" "$NEXMARK_JAR"; do
    if [[ ! -f "$required" ]]; then
        echo "✗ 离线包内容不完整，缺少: $required"
        exit 1
    fi
done
for required_dir in bin conf queries; do
    if [[ -z "$NEXMARK_DIST" || ! -d "${NEXMARK_DIST}/${required_dir}" ]]; then
        echo "✗ 离线包缺少完整 NexMark 分发目录: ${required_dir}"
        exit 1
    fi
done

ensure_flink_distribution

echo "============================================================"
echo "  ForL0 离线包一键安装"
echo "============================================================"
echo "  Bundle Root:  ${BUNDLE_ROOT}"
echo "  FLINK_HOME:   ${FLINK_DIR}"
echo "  Install Dir:  ${INSTALL_DIR}"
echo ""

mkdir -p "${FLINK_DIR}/lib" "${FLINK_DIR}/native"
mkdir -p "${INSTALL_DIR}/artifacts" "${INSTALL_DIR}/benchmark" "${INSTALL_DIR}/docker/deploy" "${INSTALL_DIR}/docs"

echo "[1/5] 安装 backend JAR 到 Flink lib/"
rm -f "${FLINK_DIR}/lib"/flink-statebackend-forl0-*.jar
rm -f "${FLINK_DIR}/lib"/flink-statebackend-forL0-*.jar
cp "$BACKEND_JAR" "${FLINK_DIR}/lib/"
if [[ -n "$NEXMARK_JAR" && -f "$NEXMARK_JAR" ]]; then
    cp "$NEXMARK_JAR" "${FLINK_DIR}/lib/"
fi

echo "[2/5] 安装 native 库到 Flink native/"
cp "$NATIVE_LIB" "${FLINK_DIR}/native/"

echo "[3/5] 同步 benchmark、docker、docs 到安装目录"
if [[ -d "${BUNDLE_ROOT}/artifacts" ]]; then
    cp -a "${BUNDLE_ROOT}/artifacts/." "${INSTALL_DIR}/artifacts/"
    cp -a "${BUNDLE_ROOT}/artifacts/." "${INSTALL_DIR}/docker/deploy/"
else
    cp "$WORDCOUNT_JAR" "${INSTALL_DIR}/artifacts/"
    cp "$WORDCOUNT_JAR" "${INSTALL_DIR}/docker/deploy/"
fi
[[ -n "$UNITTEST_JAR" && -f "$UNITTEST_JAR" ]] && cp "$UNITTEST_JAR" "${INSTALL_DIR}/artifacts/"
[[ -n "$NEXMARK_JAR" && -f "$NEXMARK_JAR" ]] && cp "$NEXMARK_JAR" "${INSTALL_DIR}/artifacts/"
[[ -n "$UNITTEST_JAR" && -f "$UNITTEST_JAR" ]] && cp "$UNITTEST_JAR" "${INSTALL_DIR}/docker/deploy/"
[[ -n "$NEXMARK_JAR" && -f "$NEXMARK_JAR" ]] && cp "$NEXMARK_JAR" "${INSTALL_DIR}/docker/deploy/"
rm -rf "${INSTALL_DIR}/docker/deploy/nexmark-flink"
cp -a "$NEXMARK_DIST" "${INSTALL_DIR}/docker/deploy/nexmark-flink"
rm -f "${INSTALL_DIR}/docker/deploy/nexmark-flink/lib/"nexmark-flink-*.jar
cp -f "$NEXMARK_JAR" \
    "${INSTALL_DIR}/docker/deploy/nexmark-flink/lib/$(basename "$NEXMARK_JAR")"
chmod +x "${INSTALL_DIR}/docker/deploy/nexmark-flink/bin/"*.sh 2>/dev/null || true

rm -rf "${INSTALL_DIR}/benchmark/config" "${INSTALL_DIR}/benchmark/scripts" \
       "${INSTALL_DIR}/benchmark/native" "${INSTALL_DIR}/benchmark/offline-packages"
if [[ -d "${BUNDLE_ROOT}/benchmark/config" ]]; then
    cp -r "${BUNDLE_ROOT}/benchmark/config" "${INSTALL_DIR}/benchmark/"
fi
if [[ -d "${BUNDLE_ROOT}/benchmark/scripts" ]]; then
    cp -r "${BUNDLE_ROOT}/benchmark/scripts" "${INSTALL_DIR}/benchmark/"
fi
if [[ -d "${BUNDLE_ROOT}/benchmark/native" ]]; then
    cp -r "${BUNDLE_ROOT}/benchmark/native" "${INSTALL_DIR}/benchmark/"
fi
if [[ -d "${BUNDLE_ROOT}/benchmark/offline-packages" ]]; then
    cp -r "${BUNDLE_ROOT}/benchmark/offline-packages" "${INSTALL_DIR}/benchmark/"
elif compgen -G "${BUNDLE_ROOT}/*.whl" >/dev/null || compgen -G "${BUNDLE_ROOT}/*.tar.gz" >/dev/null; then
    mkdir -p "${INSTALL_DIR}/benchmark/offline-packages"
    cp "${BUNDLE_ROOT}"/*.whl "${INSTALL_DIR}/benchmark/offline-packages/" 2>/dev/null || true
    cp "${BUNDLE_ROOT}"/async-profiler-*.tar.gz "${INSTALL_DIR}/benchmark/offline-packages/" 2>/dev/null || true
fi
if [[ -f "${BUNDLE_ROOT}/benchmark/requirements.txt" ]]; then
    cp "${BUNDLE_ROOT}/benchmark/requirements.txt" "${INSTALL_DIR}/benchmark/"
fi
if [[ -f "${BUNDLE_ROOT}/benchmark/README.md" ]]; then
    cp "${BUNDLE_ROOT}/benchmark/README.md" "${INSTALL_DIR}/benchmark/"
fi

rm -rf "${INSTALL_DIR}/docker/conf"
if [[ -d "${BUNDLE_ROOT}/docker/conf" ]]; then
    cp -r "${BUNDLE_ROOT}/docker/conf" "${INSTALL_DIR}/docker/"
fi
for script in docker_run.sh server_setup.sh run_all_apps.sh start.sh stop.sh restart.sh install_offline_bundle.sh; do
    if [[ -f "${BUNDLE_ROOT}/docker/${script}" ]]; then
        cp "${BUNDLE_ROOT}/docker/${script}" "${INSTALL_DIR}/docker/"
    fi
done
if [[ -f "${BUNDLE_ROOT}/docker/docker-compose.yml" ]]; then
    cp "${BUNDLE_ROOT}/docker/docker-compose.yml" "${INSTALL_DIR}/docker/"
fi
if [[ -d "${BUNDLE_ROOT}/docker/lib" ]]; then
    cp -r "${BUNDLE_ROOT}/docker/lib" "${INSTALL_DIR}/docker/"
    chmod +x "${INSTALL_DIR}/docker/lib/l0_detector.sh"
fi
if [[ -d "${BUNDLE_ROOT}/docker/images" ]]; then
    rm -rf "${INSTALL_DIR}/docker/images"
    cp -r "${BUNDLE_ROOT}/docker/images" "${INSTALL_DIR}/docker/"
fi
if [[ -d "${BUNDLE_ROOT}/docs" ]]; then
    cp -r "${BUNDLE_ROOT}/docs/." "${INSTALL_DIR}/docs/" 2>/dev/null || true
fi
for entry_point in run-forl0-offline.sh forl0-offline-app.sh reproduce-all \
                   reproduce-smoke stop-reproduce-all reproduce-l0-ablation; do
    if [[ -f "${BUNDLE_ROOT}/${entry_point}" ]]; then
        cp -f "${BUNDLE_ROOT}/${entry_point}" "${INSTALL_DIR}/"
        chmod +x "${INSTALL_DIR}/${entry_point}"
    fi
done
if [[ -x "${BUNDLE_ROOT}/tools/python/bin/python3" ]]; then
    echo "      ✓ 同步包内 CPython 运行时"
    mkdir -p "${INSTALL_DIR}/tools"
    rm -rf "${INSTALL_DIR}/tools/python"
    cp -a "${BUNDLE_ROOT}/tools/python" "${INSTALL_DIR}/tools/"
fi

if [[ "$COPY_PROFILER" == "true" ]]; then
    echo "[4/5] 复制 async-profiler 到安装目录"
    mkdir -p "${INSTALL_DIR}/tools"
    if [[ -n "$PROFILER_ARCHIVE" && -f "$PROFILER_ARCHIVE" ]]; then
        tar -xzf "$PROFILER_ARCHIVE" -C "${INSTALL_DIR}/tools"
    else
        echo "      ⚠ 离线包中未找到 async-profiler 压缩包"
    fi
else
    echo "[4/5] 跳过 profiler 复制（如需 flame graph，可加 --copy-profiler）"
fi

write_env_file() {
    local env_file="$1"
    cat > "$env_file" <<EOF
export FLINK_HOME="${FLINK_DIR}"
export FORL0_BUNDLE_ROOT="${INSTALL_DIR}"
export FORL0_NATIVE_DIR="${FLINK_DIR}/native"
export WORDCOUNT_BENCHMARK_JAR="${INSTALL_DIR}/artifacts/wordcount-benchmark-1.0-SNAPSHOT.jar"
export UNITTEST_BENCHMARK_JAR="${INSTALL_DIR}/artifacts/unit-test-benchmark-1.0-SNAPSHOT.jar"
export NEXMARK_FLINK_JAR="${INSTALL_DIR}/artifacts/nexmark-flink-0.3-SNAPSHOT.jar"
export NEXMARK_HOME="${INSTALL_DIR}/docker/deploy/nexmark-flink"
export REPO_ROOT="${INSTALL_DIR}"
export FORL0_OFFLINE=true
EOF
}

write_env_file "${INSTALL_DIR}/forl0-offline.env"
write_env_file "${INSTALL_DIR}/docker/forl0-local.env"

echo "[5/5] 写出环境文件 ${INSTALL_DIR}/forl0-offline.env 与 docker/forl0-local.env"

for script in docker_run.sh server_setup.sh run_all_apps.sh start.sh stop.sh restart.sh install_offline_bundle.sh; do
    if [[ -f "${INSTALL_DIR}/docker/${script}" ]]; then
        chmod +x "${INSTALL_DIR}/docker/${script}"
    fi
done

echo ""
echo "安装完成。"
echo ""
echo "后续使用方式："
echo "  source ${INSTALL_DIR}/forl0-offline.env"
echo "  cd ${INSTALL_DIR}/docker"
echo "  ./docker_run.sh status"

if [[ "$START_DOCKER" == "true" ]]; then
    echo ""
    echo "尝试启动 Docker 集群..."
    export FLINK_HOME="${FLINK_DIR}"
    export FORL0_NATIVE_DIR="${FLINK_DIR}/native"
    cd "${INSTALL_DIR}/docker"
    ./docker_run.sh start
fi
