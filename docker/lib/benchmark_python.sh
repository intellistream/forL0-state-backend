#!/usr/bin/env bash

forl0_benchmark_python_has_dependencies() {
    local python_bin="$1"
    [[ -x "$python_bin" ]] || return 1
    "$python_bin" - <<'PY' >/dev/null 2>&1
import yaml
import pandas
import numpy
import matplotlib
import seaborn
import jinja2
import requests
import tqdm
PY
}

forl0_benchmark_python_path() {
    local python_bin="$1"
    local python_dir

    # A venv's bin/python is normally a symlink to its base interpreter.  Do
    # not canonicalize the final component: executing the venv path is what
    # activates its site-packages, even though readlink -f points elsewhere.
    if [[ "$python_bin" != */* ]]; then
        command -v -- "$python_bin"
        return
    fi
    if [[ "$python_bin" != /* ]]; then
        python_bin="${PWD}/${python_bin}"
    fi
    python_dir="$(cd "$(dirname "$python_bin")" && pwd -P)"
    printf '%s/%s\n' "$python_dir" "$(basename "$python_bin")"
}

forl0_find_ready_benchmark_python() {
    local repository_root="$1"
    local explicit_python="${FORL0_BENCHMARK_PYTHON_BIN:-}"
    local fallback_root="${FORL0_BENCHMARK_PYTHON_ROOT:-}"
    local sibling_install_root="$(dirname "$repository_root")/forl0-runtime"
    local home_install_root="${HOME:-}/forl0-runtime"
    local root candidate seen_roots=""

    if [[ -n "$explicit_python" ]]; then
        if forl0_benchmark_python_has_dependencies "$explicit_python"; then
            forl0_benchmark_python_path "$explicit_python"
            return 0
        fi
        echo "ERROR: FORL0_BENCHMARK_PYTHON_BIN is not executable or lacks benchmark dependencies: $explicit_python" >&2
        return 2
    fi

    # The one-click installer defaults to ~/forl0-runtime. Environment exports
    # from the installer do not survive when a user later opens a new shell and
    # invokes ./reproduce-all directly, so also discover the standard install
    # location and a repository-sibling installation deterministically.
    for root in "$repository_root" "$fallback_root" \
            "$sibling_install_root" "$home_install_root"; do
        [[ -n "$root" && -d "$root" ]] || continue
        root="$(cd "$root" && pwd -P)"
        case ":$seen_roots:" in
            *":$root:"*) continue ;;
        esac
        seen_roots="${seen_roots:+${seen_roots}:}${root}"
        for candidate in \
            "$root"/.venv-benchmark-*/bin/python \
            "$root"/.venv-benchmark/bin/python; do
            [[ -x "$candidate" ]] || continue
            if forl0_benchmark_python_has_dependencies "$candidate"; then
                printf '%s\n' "$candidate"
                return 0
            fi
        done
    done
    return 1
}
