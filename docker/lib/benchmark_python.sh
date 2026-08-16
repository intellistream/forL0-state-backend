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

forl0_find_ready_benchmark_python() {
    local repository_root="$1"
    local explicit_python="${FORL0_BENCHMARK_PYTHON_BIN:-}"
    local fallback_root="${FORL0_BENCHMARK_PYTHON_ROOT:-}"
    local root candidate seen_roots=""

    if [[ -n "$explicit_python" ]]; then
        if forl0_benchmark_python_has_dependencies "$explicit_python"; then
            readlink -f "$explicit_python"
            return 0
        fi
        echo "ERROR: FORL0_BENCHMARK_PYTHON_BIN is not executable or lacks benchmark dependencies: $explicit_python" >&2
        return 2
    fi

    for root in "$repository_root" "$fallback_root"; do
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
                readlink -f "$candidate"
                return 0
            fi
        done
    done
    return 1
}
