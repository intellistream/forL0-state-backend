#!/usr/bin/env bash
# Select a host Python interpreter compatible with the bundled pandas wheel.

if [ -z "${BASH_VERSION:-}" ]; then
    if command -v bash >/dev/null 2>&1; then
        exec bash "$0" "$@"
    fi
    echo "ERROR: the Python wheel detector requires bash" >&2
    exit 1
fi

forl0_normalize_arch() {
    case "$1" in
        aarch64|arm64) printf '%s\n' aarch64 ;;
        x86_64|amd64|x64) printf '%s\n' x86_64 ;;
        *) printf '%s\n' "$1" ;;
    esac
}

forl0_resolve_python_command() {
    local candidate="$1"
    if [[ "$candidate" == */* ]]; then
        [[ -x "$candidate" ]] || return 1
        readlink -f "$candidate" 2>/dev/null || printf '%s\n' "$candidate"
    else
        command -v "$candidate" 2>/dev/null
    fi
}

forl0_select_python_for_wheels() {
    local wheels_dir="$1"
    local pandas_wheel=""
    local wheel_name=""
    local host_arch=""
    local candidate=""
    local resolved=""
    local actual_version=""
    local -a candidates=()

    FORL0_WHEEL_PYTHON_ABI=""
    FORL0_WHEEL_PYTHON_VERSION=""
    FORL0_WHEEL_ARCH=""
    FORL0_SELECTED_PYTHON=""

    [[ -d "$wheels_dir" ]] || {
        echo "Python wheel detector: directory does not exist: $wheels_dir" >&2
        return 1
    }

    pandas_wheel="$(find "$wheels_dir" -maxdepth 1 -type f -name 'pandas-*.whl' -print -quit 2>/dev/null || true)"
    [[ -n "$pandas_wheel" ]] || {
        echo "Python wheel detector: pandas wheel is missing from $wheels_dir" >&2
        return 1
    }
    wheel_name="$(basename "$pandas_wheel")"

    if [[ "$wheel_name" =~ -cp([0-9])([0-9]+)- ]]; then
        FORL0_WHEEL_PYTHON_ABI="cp${BASH_REMATCH[1]}${BASH_REMATCH[2]}"
        FORL0_WHEEL_PYTHON_VERSION="${BASH_REMATCH[1]}.${BASH_REMATCH[2]}"
    else
        echo "Python wheel detector: cannot determine CPython ABI from $wheel_name" >&2
        return 1
    fi

    case "$wheel_name" in
        *aarch64*) FORL0_WHEEL_ARCH=aarch64 ;;
        *x86_64*) FORL0_WHEEL_ARCH=x86_64 ;;
        *)
            echo "Python wheel detector: cannot determine Linux architecture from $wheel_name" >&2
            return 1
            ;;
    esac

    host_arch="$(forl0_normalize_arch "$(uname -m)")"
    if [[ "$host_arch" != "$FORL0_WHEEL_ARCH" ]]; then
        echo "Python wheel detector: architecture mismatch" >&2
        echo "  host:   $host_arch" >&2
        echo "  wheels: $FORL0_WHEEL_ARCH ($wheel_name)" >&2
        return 1
    fi

    if [[ -n "${FORL0_PYTHON_BIN:-}" ]]; then
        candidates=("$FORL0_PYTHON_BIN")
    elif [[ -n "${FORL0_BUNDLED_PYTHON_BIN:-}" ]]; then
        candidates=("$FORL0_BUNDLED_PYTHON_BIN" "python${FORL0_WHEEL_PYTHON_VERSION}" python3)
    else
        candidates=("python${FORL0_WHEEL_PYTHON_VERSION}" python3)
    fi

    for candidate in "${candidates[@]}"; do
        resolved="$(forl0_resolve_python_command "$candidate" || true)"
        [[ -n "$resolved" ]] || continue
        actual_version="$("$resolved" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")' 2>/dev/null || true)"
        if [[ "$actual_version" == "$FORL0_WHEEL_PYTHON_VERSION" ]]; then
            FORL0_SELECTED_PYTHON="$resolved"
            export FORL0_WHEEL_PYTHON_ABI FORL0_WHEEL_PYTHON_VERSION FORL0_WHEEL_ARCH
            export FORL0_SELECTED_PYTHON
            return 0
        fi
        if [[ -n "${FORL0_PYTHON_BIN:-}" ]]; then
            echo "Python wheel detector: FORL0_PYTHON_BIN has version ${actual_version:-unknown}, but bundled wheels require ${FORL0_WHEEL_PYTHON_VERSION}" >&2
            return 1
        fi
    done

    echo "Python wheel detector: no compatible interpreter found" >&2
    echo "  required: Python ${FORL0_WHEEL_PYTHON_VERSION} (${FORL0_WHEEL_PYTHON_ABI}) on ${FORL0_WHEEL_ARCH}" >&2
    echo "  install/enable python${FORL0_WHEEL_PYTHON_VERSION} with its venv module, or set FORL0_PYTHON_BIN" >&2
    if [[ -n "${FORL0_BUNDLED_PYTHON_BIN:-}" ]]; then
        echo "  bundled candidate: $FORL0_BUNDLED_PYTHON_BIN" >&2
    fi
    if command -v python3 >/dev/null 2>&1; then
        echo "  current python3: $(python3 --version 2>&1) ($(command -v python3))" >&2
    fi
    return 1
}

forl0_print_python_wheel_detection() {
    echo "  wheel ABI:    ${FORL0_WHEEL_PYTHON_ABI:-unknown}"
    echo "  wheel arch:   ${FORL0_WHEEL_ARCH:-unknown}"
    echo "  Python:       ${FORL0_SELECTED_PYTHON:-not found} (${FORL0_WHEEL_PYTHON_VERSION:-unknown})"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    wheels_dir="${1:-benchmark/offline-packages}"
    echo "ForL0 offline Python wheel compatibility"
    if ! forl0_select_python_for_wheels "$wheels_dir"; then
        forl0_print_python_wheel_detection
        exit 1
    fi
    forl0_print_python_wheel_detection
fi
