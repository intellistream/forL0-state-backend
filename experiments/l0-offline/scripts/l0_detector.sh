#!/usr/bin/env bash
# Shared host-side detector for the L0 device and runtime libraries.
# This file is sourced by the launcher, setup scripts, and Docker runners.

# Avoid an opaque "bad substitution" when an operator runs this file with sh.
if [ -z "${BASH_VERSION:-}" ]; then
    if command -v bash >/dev/null 2>&1; then
        exec bash "$0" "$@"
    fi
    echo "ERROR: the L0 detector requires bash, but bash was not found" >&2
    exit 1
fi

forl0_canonical_file() {
    local path="$1"
    [[ -f "$path" && -r "$path" ]] || return 1
    if command -v readlink >/dev/null 2>&1; then
        readlink -f "$path" 2>/dev/null && return 0
    fi
    printf '%s\n' "$path"
}

forl0_rooted_path() {
    local path="$1"
    if [[ -n "${FORL0_SYSTEM_ROOT:-}" ]]; then
        printf '%s%s\n' "${FORL0_SYSTEM_ROOT%/}" "$path"
    else
        printf '%s\n' "$path"
    fi
}

forl0_library_record() {
    local path="$1"
    local source="$2"
    local canonical
    canonical="$(forl0_canonical_file "$path" || true)"
    [[ -n "$canonical" ]] || return 1
    printf '%s\t%s\n' "$canonical" "$source"
}

forl0_ldconfig_output() {
    if [[ -n "${FORL0_LDCONFIG_CACHE_FILE:-}" ]]; then
        cat "$FORL0_LDCONFIG_CACHE_FILE"
    elif [[ -z "${FORL0_SYSTEM_ROOT:-}" ]] && command -v ldconfig >/dev/null 2>&1; then
        ldconfig -p 2>/dev/null || true
    fi
}

forl0_detect_named_library() {
    local soname="$1"
    local explicit_path="$2"
    local search_roots="$3"
    local candidate=""
    local directory=""
    local multiarch="${FORL0_MULTIARCH:-}"
    local ldconfig_line=""
    local ldconfig_path=""
    local record=""
    local -a candidate_dirs=()
    local -a roots=()

    if [[ -n "$explicit_path" ]]; then
        if record="$(forl0_library_record "$explicit_path" "explicit override")"; then
            printf '%s\n' "$record"
            return 0
        fi
        echo "L0 detector: configured path is not a readable file: $explicit_path" >&2
        return 2
    fi

    if [[ -n "${LD_LIBRARY_PATH:-}" ]]; then
        IFS=':' read -r -a candidate_dirs <<< "$LD_LIBRARY_PATH"
        for directory in "${candidate_dirs[@]}"; do
            [[ -n "$directory" ]] || continue
            for candidate in "$directory/$soname" "$directory/$soname".*; do
                if record="$(forl0_library_record "$candidate" "LD_LIBRARY_PATH")"; then
                    printf '%s\n' "$record"
                    return 0
                fi
            done
        done
    fi

    while IFS= read -r ldconfig_line; do
        [[ "$ldconfig_line" == *"$soname"*"=>"* ]] || continue
        ldconfig_path="${ldconfig_line##*=> }"
        ldconfig_path="${ldconfig_path#${ldconfig_path%%[![:space:]]*}}"
        ldconfig_path="${ldconfig_path%${ldconfig_path##*[![:space:]]}}"
        if record="$(forl0_library_record "$ldconfig_path" "ldconfig cache")"; then
            printf '%s\n' "$record"
            return 0
        fi
    done < <(forl0_ldconfig_output)

    if [[ -z "$multiarch" && -z "${FORL0_SYSTEM_ROOT:-}" ]]; then
        if command -v gcc >/dev/null 2>&1; then
            multiarch="$(gcc -print-multiarch 2>/dev/null || true)"
        elif command -v dpkg-architecture >/dev/null 2>&1; then
            multiarch="$(dpkg-architecture -qDEB_HOST_MULTIARCH 2>/dev/null || true)"
        fi
    fi

    candidate_dirs=(
        /usr/lib64 /usr/lib /lib64 /lib
        /usr/local/lib64 /usr/local/lib
        /opt/l0/lib /opt/l0/lib64
        /opt/hisi/l0/lib /opt/hisi/l0/lib64
        /opt/huawei/l0/lib /opt/huawei/l0/lib64
        /usr/local/l0/lib /usr/local/l0/lib64
    )
    if [[ -n "$multiarch" ]]; then
        candidate_dirs=(
            "/usr/lib/$multiarch" "/lib/$multiarch"
            "/usr/local/lib/$multiarch"
            "${candidate_dirs[@]}"
        )
    fi

    for directory in "${candidate_dirs[@]}"; do
        directory="$(forl0_rooted_path "$directory")"
        for candidate in "$directory/$soname" "$directory/$soname".*; do
            if record="$(forl0_library_record "$candidate" "known library directory")"; then
                printf '%s\n' "$record"
                return 0
            fi
        done
    done

    if [[ -n "$search_roots" ]]; then
        IFS=':' read -r -a roots <<< "$search_roots"
    else
        roots=(/opt /usr/local "${HOME:-}")
    fi
    for directory in "${roots[@]}"; do
        [[ -d "$directory" ]] || continue
        candidate="$(find -L "$directory" -maxdepth 6 -type f \
            \( -name "$soname" -o -name "$soname.*" \) -print -quit 2>/dev/null || true)"
        if [[ -n "$candidate" ]] && record="$(forl0_library_record "$candidate" "bounded search under $directory")"; then
            printf '%s\n' "$record"
            return 0
        fi
    done

    return 1
}

forl0_detect_l0_environment() {
    local record=""

    FORL0_L0_DEVICE_PATH=""
    FORL0_L0_LIBRARY_PATH=""
    FORL0_L0_LIBRARY_SOURCE=""
    FORL0_NUMA_LIBRARY_PATH=""
    FORL0_NUMA_LIBRARY_SOURCE=""

    if [[ -n "${L0_DEVICE_HOST_PATH:-}" ]]; then
        if [[ -e "$L0_DEVICE_HOST_PATH" ]]; then
            FORL0_L0_DEVICE_PATH="$L0_DEVICE_HOST_PATH"
        else
            echo "L0 detector: configured device does not exist: $L0_DEVICE_HOST_PATH" >&2
        fi
    elif [[ -e "$(forl0_rooted_path /dev/hisi_l0)" ]]; then
        FORL0_L0_DEVICE_PATH="$(forl0_rooted_path /dev/hisi_l0)"
    elif [[ -e "$(forl0_rooted_path /dev/l0)" ]]; then
        FORL0_L0_DEVICE_PATH="$(forl0_rooted_path /dev/l0)"
    fi

    record="$(forl0_detect_named_library \
        libl0mempool.so \
        "${L0_MEMPOOL_LIB_HOST_PATH:-}" \
        "${FORL0_L0_SEARCH_ROOTS:-}" || true)"
    if [[ -n "$record" ]]; then
        IFS=$'\t' read -r FORL0_L0_LIBRARY_PATH FORL0_L0_LIBRARY_SOURCE <<< "$record"
    fi

    record="$(forl0_detect_named_library \
        libnuma.so.1 \
        "${NUMA_LIB_HOST_PATH:-}" \
        "${FORL0_NUMA_SEARCH_ROOTS:-}" || true)"
    if [[ -n "$record" ]]; then
        IFS=$'\t' read -r FORL0_NUMA_LIBRARY_PATH FORL0_NUMA_LIBRARY_SOURCE <<< "$record"
    fi

    export FORL0_L0_DEVICE_PATH FORL0_L0_LIBRARY_PATH FORL0_L0_LIBRARY_SOURCE
    export FORL0_NUMA_LIBRARY_PATH FORL0_NUMA_LIBRARY_SOURCE
    [[ -n "$FORL0_L0_DEVICE_PATH" && -n "$FORL0_L0_LIBRARY_PATH" ]]
}

forl0_print_l0_detection() {
    if [[ -n "${FORL0_L0_DEVICE_PATH:-}" ]]; then
        echo "  device:      OK (${FORL0_L0_DEVICE_PATH})"
    else
        echo "  device:      MISSING (/dev/hisi_l0 or /dev/l0; override: L0_DEVICE_HOST_PATH)"
    fi
    if [[ -n "${FORL0_L0_LIBRARY_PATH:-}" ]]; then
        echo "  libl0:       OK (${FORL0_L0_LIBRARY_PATH}; ${FORL0_L0_LIBRARY_SOURCE})"
    else
        echo "  libl0:       MISSING (override: L0_MEMPOOL_LIB_HOST_PATH; search: ldconfig, LD_LIBRARY_PATH, multiarch, /opt, /usr/local, HOME)"
    fi
    if [[ -n "${FORL0_NUMA_LIBRARY_PATH:-}" ]]; then
        echo "  libnuma:     OK (${FORL0_NUMA_LIBRARY_PATH}; ${FORL0_NUMA_LIBRARY_SOURCE})"
    else
        echo "  libnuma:     MISSING (override: NUMA_LIB_HOST_PATH)"
    fi
}

forl0_prepend_l0_library_path() {
    local library_path="${1:-${FORL0_L0_LIBRARY_PATH:-}}"
    local library_dir=""
    [[ -n "$library_path" ]] || return 0
    library_dir="$(dirname "$library_path")"
    case ":${LD_LIBRARY_PATH:-}:" in
        *":${library_dir}:"*) ;;
        *) export LD_LIBRARY_PATH="${library_dir}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}" ;;
    esac
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    echo "ForL0 L0 environment detection"
    if forl0_detect_l0_environment; then
        forl0_print_l0_detection
        exit 0
    fi
    forl0_print_l0_detection
    echo ""
    echo "If the runtime is installed in a private directory, set:"
    echo "  export L0_MEMPOOL_LIB_HOST_PATH=/path/to/libl0mempool.so"
    echo "  export L0_DEVICE_HOST_PATH=/dev/l0        # or /dev/hisi_l0"
    echo "Or add its directory to LD_LIBRARY_PATH and run this detector again."
    exit 1
fi
