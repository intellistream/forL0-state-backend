#!/usr/bin/env bash
###############################################################################
#  One-click launcher for full benchmark comparison
#
#  Handles ALL environment setup automatically so that on a fresh (possibly
#  offline) server you only need:
#
#      ./run_benchmark.sh                 # run everything
#      ./run_benchmark.sh --skip-profile  # skip flame-graph profiling
#      ./run_benchmark.sh --benchmarks wordcount   # only WordCount
#      ./run_benchmark.sh --help          # see all options
#
#  The script auto-bootstraps:
#    • Python 3 venv with required packages (pip or offline wheels)
#    • FLINK_HOME detection
#    • Flink cluster startup (Docker or standalone)
#    • ForL0 JAR deployment to Flink lib/
#    • Benchmark JAR builds (if missing)
###############################################################################
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BENCHMARK_ROOT="$PROJECT_ROOT/benchmark"
VENV_DIR="$BENCHMARK_ROOT/.venv-benchmark"
OFFLINE_PKGS="$BENCHMARK_ROOT/offline-packages"

# ── Colour helpers ────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[✓]${NC} $*"; }
warn()  { echo -e "${YELLOW}[!]${NC} $*"; }
error() { echo -e "${RED}[✗]${NC} $*"; }

# ── 1. Python 3 environment ──────────────────────────────────────────────────
#  Sets PYTHON_CMD to a working python3 binary (system or venv).
PYTHON_CMD=""
setup_python_env() {
    local PY=""
    for candidate in python3 python3.9 python3.10 python3.11 python3.12 python3.13; do
        if command -v "$candidate" &>/dev/null; then PY="$candidate"; break; fi
    done
    if [[ -z "$PY" ]]; then
        error "Python 3 not found. Please install Python 3.9+."; exit 1
    fi
    info "Using Python: $($PY --version) at $(which $PY)"

    local REQUIRED_PKGS="matplotlib numpy jinja2 pyyaml urllib3"

    # ── Check if system Python has all deps ──
    local system_ok=true
    for pkg in $REQUIRED_PKGS; do
        if ! "$PY" -c "import ${pkg}" &>/dev/null 2>&1; then
            system_ok=false; break
        fi
    done

    if $system_ok; then
        info "System Python has all required packages — skipping venv."
        PYTHON_CMD="$PY"
        return
    fi

    # ── Need venv ──
    if [[ ! -d "$VENV_DIR" ]] || [[ ! -f "$VENV_DIR/bin/python3" ]]; then
        warn "Creating Python venv (with --system-site-packages) at $VENV_DIR ..."
        "$PY" -m venv --system-site-packages "$VENV_DIR"
    fi

    # shellcheck disable=SC1091
    source "$VENV_DIR/bin/activate"
    PYTHON_CMD="python3"

    local missing=""
    for pkg in $REQUIRED_PKGS; do
        if ! python3 -c "import ${pkg}" &>/dev/null 2>&1; then
            missing="$missing $pkg"
        fi
    done

    if [[ -n "$missing" ]]; then
        info "Installing Python packages:$missing"
        if pip install --timeout 30 --quiet $missing 2>/dev/null; then
            info "Installed from PyPI."
        elif [[ -d "$OFFLINE_PKGS" ]] && ls "$OFFLINE_PKGS"/*.whl &>/dev/null 2>&1; then
            warn "PyPI unavailable — trying offline wheels ..."
            pip install --quiet --no-index --find-links "$OFFLINE_PKGS" $missing 2>/dev/null \
                && info "Installed from offline wheels." \
                || warn "Some packages failed to install from wheels. Continuing anyway..."
        else
            warn "Cannot install Python packages (no pip, no offline wheels)."
            warn "Some features may not work. Install manually: pip install$missing"
        fi
    fi
    info "Python environment ready."
}

# ── 2. FLINK_HOME auto-detection ─────────────────────────────────────────────
detect_flink_home() {
    if [[ -n "${FLINK_HOME:-}" ]] && [[ -d "$FLINK_HOME/bin" ]]; then
        info "FLINK_HOME = $FLINK_HOME (from environment)"
        return
    fi

    # Search common locations
    local candidates=(
        "$HOME/flink-1.20."*
        "$HOME/flink/flink-1.20."*
        /opt/flink*
        /usr/local/flink*
    )
    for c in "${candidates[@]}"; do
        if [[ -d "$c/bin" ]] && [[ -f "$c/bin/start-cluster.sh" ]]; then
            export FLINK_HOME="$c"
            info "Auto-detected FLINK_HOME = $FLINK_HOME"
            return
        fi
    done

    # Check if Docker Flink is the only option
    if command -v docker &>/dev/null && docker ps --format '{{.Names}}' 2>/dev/null | grep -qi jobmanager; then
        warn "No standalone Flink found, but Docker cluster is running."
        warn "Will use Docker cluster (FLINK_HOME not required for job submission)."
        return
    fi

    error "Flink installation not found. Set FLINK_HOME or install Flink."
    exit 1
}

# ── 3. Flink cluster ─────────────────────────────────────────────────────────
check_flink_cluster() {
    local REST_URL="${1:-http://localhost:8081}"
    if curl -sf "$REST_URL/overview" >/dev/null 2>&1; then
        info "Flink cluster running at $REST_URL"
        return 0
    fi
    return 1
}

ensure_flink_cluster() {
    local REST_URL
    REST_URL=$(python3 -c "
import sys; sys.path.insert(0,'$SCRIPT_DIR')
from utils.config import load_config
print(load_config().get('flink',{}).get('rest_url','http://localhost:8081'))
" 2>/dev/null || echo "http://localhost:8081")

    if check_flink_cluster "$REST_URL"; then return; fi

    # Try Docker first
    if command -v docker &>/dev/null; then
        local compose_file="$PROJECT_ROOT/docker/docker-compose.yml"
        if [[ -f "$compose_file" ]]; then
            warn "Starting Flink via Docker Compose ..."
            local DC=""
            if docker compose version &>/dev/null 2>&1; then DC="docker compose"
            elif docker-compose version &>/dev/null 2>&1; then DC="docker-compose"
            fi
            if [[ -n "$DC" ]]; then
                (cd "$PROJECT_ROOT/docker" && $DC up -d 2>&1) || true
                for i in $(seq 1 15); do
                    if check_flink_cluster "$REST_URL"; then return; fi
                    sleep 2
                done
            fi
        fi
    fi

    # Try standalone
    if [[ -n "${FLINK_HOME:-}" ]] && [[ -f "$FLINK_HOME/bin/start-cluster.sh" ]]; then
        warn "Starting standalone Flink cluster ..."
        "$FLINK_HOME/bin/start-cluster.sh" 2>&1 || true
        for i in $(seq 1 15); do
            if check_flink_cluster "$REST_URL"; then return; fi
            sleep 2
        done
    fi

    error "Could not start Flink cluster at $REST_URL"
    exit 1
}

# ── 4. Deploy JARs to Flink lib/ ────────────────────────────────────────────
#  Returns 0 if any JAR was updated (caller should restart cluster).
FORL0_JAR_UPDATED=false

deploy_forl0_jar() {
    [[ -z "${FLINK_HOME:-}" ]] && return
    local deploy_dir="$PROJECT_ROOT/docker/deploy"
    local target_jar="$PROJECT_ROOT/target/flink-statebackend-forL0-1.0-SNAPSHOT.jar"
    local flink_lib="$FLINK_HOME/lib"
    [[ -d "$flink_lib" ]] || return

    # Prefer freshly-built JAR over deploy/ copy
    local forl0_src=""
    if [[ -f "$target_jar" ]]; then
        forl0_src="$target_jar"
    else
        forl0_src=$(ls "$deploy_dir"/flink-statebackend-forL0-*.jar "$deploy_dir"/flink-statebackend-forl0-*.jar 2>/dev/null | head -1)
    fi

    if [[ -n "$forl0_src" ]] && [[ -f "$forl0_src" ]]; then
        local dest="$flink_lib/$(basename "$forl0_src")"
        if [[ ! -f "$dest" ]] || [[ "$forl0_src" -nt "$dest" ]]; then
            # Remove any old version with different casing
            rm -f "$flink_lib"/flink-statebackend-for[lL]0-*.jar
            cp "$forl0_src" "$dest"
            info "Deployed $(basename "$forl0_src") → $flink_lib/"
            FORL0_JAR_UPDATED=true
        fi
    fi

    # Deploy nexmark JAR
    for jar in "$deploy_dir"/nexmark-flink-*.jar; do
        [[ -f "$jar" ]] || continue
        local dest="$flink_lib/$(basename "$jar")"
        if [[ ! -f "$dest" ]] || [[ "$jar" -nt "$dest" ]]; then
            cp "$jar" "$dest"
            info "Deployed $(basename "$jar") → $flink_lib/"
        fi
    done

    # Deploy WordCount JAR
    for jar in "$deploy_dir"/wordcount-benchmark-*.jar; do
        [[ -f "$jar" ]] || continue
        local dest="$flink_lib/$(basename "$jar")"
        if [[ ! -f "$dest" ]]; then
            cp "$jar" "$dest"
            info "Deployed $(basename "$jar") → $flink_lib/"
        fi
    done
}

# ── 5. Auto-rebuild when source is newer than JAR ──────────────────────────
_needs_rebuild() {
    local jar="$1"
    shift
    local src_dirs=("$@")
    [[ ! -f "$jar" ]] && return 0  # JAR missing → needs build
    for d in "${src_dirs[@]}"; do
        if find "$d" \( -name '*.java' -o -name '*.cpp' -o -name '*.h' \) -newer "$jar" 2>/dev/null | head -1 | grep -q .; then
            return 0
        fi
    done
    return 1
}

rebuild_forl0() {
    local jar="$PROJECT_ROOT/target/flink-statebackend-forL0-1.0-SNAPSHOT.jar"
    local src_dirs=("$PROJECT_ROOT/src/main/java" "$PROJECT_ROOT/src/main/native")

    if ! _needs_rebuild "$jar" "${src_dirs[@]}"; then
        info "ForL0 JAR is up to date."
        return 0
    fi

    warn "ForL0 source changed — rebuilding ..."

    # 1. Build native library
    if command -v make &>/dev/null && [[ -f "$PROJECT_ROOT/src/main/native/Makefile" ]]; then
        info "  Building native library ..."
        (cd "$PROJECT_ROOT/src/main/native" && make clean && make -j"$(nproc)" && make install) || {
            error "  Native build failed!"; return 1
        }
    fi

    # 2. Maven package
    if command -v mvn &>/dev/null; then
        info "  Building ForL0 JAR ..."
        (cd "$PROJECT_ROOT" && mvn package -DskipTests -q) || {
            error "  Maven build failed!"; return 1
        }
        info "  ForL0 JAR rebuilt successfully."
        FORL0_JAR_UPDATED=true
    else
        warn "  Maven not found — cannot rebuild ForL0 JAR."
        return 1
    fi
}

rebuild_wordcount() {
    local jar="$BENCHMARK_ROOT/wordcount/target/wordcount-benchmark-1.0-SNAPSHOT.jar"
    local deploy_jar="$PROJECT_ROOT/docker/deploy/wordcount-benchmark-1.0-SNAPSHOT.jar"
    local src_dir="$BENCHMARK_ROOT/wordcount/src"

    if ! _needs_rebuild "$jar" "$src_dir"; then
        info "WordCount JAR is up to date."
        return 0
    fi

    warn "WordCount source changed — rebuilding ..."
    if command -v mvn &>/dev/null && [[ -f "$BENCHMARK_ROOT/wordcount/pom.xml" ]]; then
        (cd "$BENCHMARK_ROOT/wordcount" && mvn package -Plocal -DskipTests -q) && {
            info "  WordCount JAR rebuilt successfully."
            # Also update docker/deploy/ copy
            [[ -f "$jar" ]] && cp "$jar" "$deploy_jar"
            return 0
        } || error "  WordCount build failed!"
    fi

    # Fallback: copy pre-built from docker/deploy/
    if [[ -f "$deploy_jar" ]]; then
        mkdir -p "$(dirname "$jar")"
        cp "$deploy_jar" "$jar"
        info "  Copied pre-built WordCount JAR from docker/deploy/"
    fi
}

rebuild_nexmark() {
    local nm_home="$BENCHMARK_ROOT/nexmark-src/nexmark-flink"
    local jar="$nm_home/target/nexmark-flink-0.3-SNAPSHOT.jar"
    local deploy_jar="$PROJECT_ROOT/docker/deploy/nexmark-flink-0.3-SNAPSHOT.jar"
    local src_dir="$nm_home/src"

    if ! _needs_rebuild "$jar" "$src_dir"; then
        info "NexMark JAR is up to date."
        return 0
    fi

    warn "NexMark source changed — rebuilding ..."
    if command -v mvn &>/dev/null && [[ -f "$nm_home/pom.xml" ]]; then
        (cd "$nm_home" && mvn package -DskipTests -q) && {
            info "  NexMark JAR rebuilt successfully."
            # Update deploy copy
            [[ -f "$jar" ]] && cp "$jar" "$deploy_jar"
            # Copy to Flink lib
            if [[ -n "${FLINK_HOME:-}" ]] && [[ -d "$FLINK_HOME/lib" ]]; then
                cp "$jar" "$FLINK_HOME/lib/" 2>/dev/null
                FORL0_JAR_UPDATED=true  # triggers cluster restart
            fi
            return 0
        } || error "  NexMark build failed!"
    fi

    if [[ -f "$deploy_jar" ]]; then
        info "  Using pre-built NexMark JAR from docker/deploy/"
    fi
}

rebuild_all() {
    local any_failed=false
    rebuild_forl0 || any_failed=true
    rebuild_wordcount || any_failed=true
    rebuild_nexmark || any_failed=true

    if $any_failed; then
        warn "Some components failed to rebuild. Continuing with existing JARs."
    fi
}

# ── 6. Restart Flink cluster if JARs were updated ──────────────────────────
restart_flink_cluster() {
    if [[ "$FORL0_JAR_UPDATED" != "true" ]]; then
        return
    fi

    local REST_URL
    REST_URL=$($PYTHON_CMD -c "
import sys; sys.path.insert(0,'$SCRIPT_DIR')
from utils.config import load_config
print(load_config().get('flink',{}).get('rest_url','http://localhost:8081'))
" 2>/dev/null || echo "http://localhost:8081")

    warn "ForL0 JAR was updated — restarting Flink TaskManagers ..."

    # Cancel running jobs
    $PYTHON_CMD -c "
import sys, json, urllib.request
try:
    data = json.loads(urllib.request.urlopen('$REST_URL/jobs/overview', timeout=5).read())
    for j in data.get('jobs', []):
        if j.get('state') == 'RUNNING':
            urllib.request.urlopen(urllib.request.Request('$REST_URL/jobs/' + j['jid'] + '?mode=cancel', method='PATCH'), timeout=10)
            print(f'  Cancelled job {j[\"jid\"][:8]}...')
except Exception: pass
" 2>/dev/null
    sleep 2

    # Detect cluster type: Docker or standalone
    local IS_DOCKER=false
    local compose_file="$PROJECT_ROOT/docker/docker-compose.yml"
    if command -v docker &>/dev/null && [[ -f "$compose_file" ]]; then
        # Check if ANY Flink container exists (running or stopped)
        if docker ps -a --format '{{.Names}}' 2>/dev/null | grep -qi 'flink-\(task\|job\)'; then
            IS_DOCKER=true
        fi
    fi

    if $IS_DOCKER; then
        info "  Restarting Docker Flink cluster ..."
        local DC=""
        if docker compose version &>/dev/null 2>&1; then DC="docker compose"
        elif docker-compose version &>/dev/null 2>&1; then DC="docker-compose"; fi
        if [[ -n "$DC" ]]; then
            (cd "$PROJECT_ROOT/docker" && $DC restart 2>&1) || true
        fi
    elif [[ -n "${FLINK_HOME:-}" ]]; then
        info "  Restarting standalone Flink cluster ..."
        "$FLINK_HOME/bin/stop-cluster.sh" 2>/dev/null || true
        sleep 2
        "$FLINK_HOME/bin/start-cluster.sh" 2>/dev/null || true
    else
        warn "  Cannot determine cluster type for restart."
        return
    fi

    # Wait for cluster to come back
    for i in $(seq 1 20); do
        if curl -sf "$REST_URL/overview" >/dev/null 2>&1; then
            info "  Flink cluster restarted successfully."
            return
        fi
        sleep 2
    done
    warn "  Cluster restart may not have completed. Check manually."
}

# ── 7. Main ──────────────────────────────────────────────────────────────────
main() {
    echo ""
    echo "============================================================"
    echo "  ForL0 Benchmark — One-Click Launcher"
    echo "============================================================"
    echo ""

    setup_python_env
    detect_flink_home
    ensure_flink_cluster
    rebuild_all          # auto-rebuild if source changed
    deploy_forl0_jar     # deploy JARs to Flink lib/
    restart_flink_cluster  # restart if JARs were updated

    echo ""
    info "All prerequisites ready. Starting benchmarks ..."
    echo ""

    # Force unbuffered Python output
    export PYTHONUNBUFFERED=1
    exec "$PYTHON_CMD" -u "$SCRIPT_DIR/run_full_comparison.py" "$@"
}

main "$@"
