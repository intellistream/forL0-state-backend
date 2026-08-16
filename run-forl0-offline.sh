#!/usr/bin/env bash
# One-command bootstrap after copying this script and the offline archive to a server.

# Re-enter Bash before parsing Bash-only syntax, avoiding an opaque
# "bad substitution" if the operator invokes this file with sh.
if [ -z "${BASH_VERSION:-}" ]; then
    if command -v bash >/dev/null 2>&1; then
        exec bash "$0" "$@"
    fi
    echo "ERROR: this bootstrap requires bash, but bash was not found" >&2
    exit 1
fi

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export FORL0_RUN_ID="${FORL0_RUN_ID:-manual_$(date '+%Y%m%d_%H%M%S')}"
DEFAULT_RESULTS_ROOT="${FORL0_RESULTS_DIR:-${SCRIPT_DIR}/benchmark/results/runs/${FORL0_RUN_ID}/formal}"
RUN_LOG="${FORL0_RUN_LOG:-${DEFAULT_RESULTS_ROOT}/.logs}"
mkdir -p "$(dirname "$RUN_LOG")"
if [[ "${FORL0_RUN_LOG_MODE:-overwrite}" == "append" ]]; then
    exec > >(tee -a "$RUN_LOG") 2>&1
else
    exec > >(tee "$RUN_LOG") 2>&1
fi

BUNDLE_NAME="forl0-offline-linux-arm64-py310-20260721"
ARCHIVE="${FORL0_OFFLINE_ARCHIVE:-${SCRIPT_DIR}/${BUNDLE_NAME}.tar.gz}"
EXTRACT_PARENT="${FORL0_EXTRACT_PARENT:-${SCRIPT_DIR}}"
BUNDLE_DIR="${EXTRACT_PARENT}/${BUNDLE_NAME}"
CHECKSUM_FILE="${ARCHIVE}.sha256"
RELEASE_TAG="${FORL0_RELEASE_TAG:-offline-arm64-py310-20260721-r7}"
RELEASE_BASE_URL="https://github.com/intellistream/forL0-state-backend/releases/download/${RELEASE_TAG}"
DOWNLOAD_AUTH_TOKEN=""
DEFAULT_RUN_ARGS=(--reproduce-ascend --keep-going)

# The root entry point is the "everything" path.  Keep explicit launcher modes
# working, but make a no-mode invocation run the complete numbered Ascend
# reproduction matrix instead of the smaller contract-app subset.
has_explicit_run_mode() {
    local arg
    for arg in "$@"; do
        case "$arg" in
            --smoke-only|--apps-only|--full|--pressure-only|--reproduce-ascend|--report-only|--workloads|--list-workloads)
                return 0
                ;;
        esac
    done
    return 1
}

discover_github_token() {
    local credential=""
    DOWNLOAD_AUTH_TOKEN="${GH_TOKEN:-${GITHUB_TOKEN:-}}"
    if [[ -z "$DOWNLOAD_AUTH_TOKEN" ]] && command -v gh >/dev/null 2>&1; then
        DOWNLOAD_AUTH_TOKEN="$(gh auth token 2>/dev/null || true)"
    fi
    if [[ -z "$DOWNLOAD_AUTH_TOKEN" ]] && command -v git >/dev/null 2>&1; then
        credential="$(printf 'protocol=https\nhost=github.com\n\n' | git credential fill 2>/dev/null || true)"
        DOWNLOAD_AUTH_TOKEN="$(printf '%s\n' "$credential" | sed -n 's/^password=//p' | head -n 1)"
    fi
}

resolve_private_asset_api_url() {
    local asset_name="$1"
    local release_json=""
    local current_url=""
    local line=""
    local api_url="https://api.github.com/repos/intellistream/forL0-state-backend/releases/tags/${RELEASE_TAG}"
    [[ -n "$DOWNLOAD_AUTH_TOKEN" ]] || return 1
    release_json="$(curl -fsSL \
        -H "Authorization: Bearer ${DOWNLOAD_AUTH_TOKEN}" \
        -H 'Accept: application/vnd.github+json' \
        "$api_url" 2>/dev/null || true)"
    [[ -n "$release_json" ]] || return 1
    while IFS= read -r line; do
        if [[ "$line" == *'"url": "https://api.github.com/repos/'*'/releases/assets/'* ]]; then
            current_url="${line#*\"url\": \"}"
            current_url="${current_url%%\"*}"
        elif [[ "$line" == *'"name": "'"$asset_name"'"'* && -n "$current_url" ]]; then
            printf '%s\n' "$current_url"
            return 0
        fi
    done <<< "$release_json"
    return 1
}

download_release_asset() {
    local asset_name="$1"
    local destination="$2"
    local url="${RELEASE_BASE_URL}/${asset_name}"
    local partial="${destination}.part"
    local private_api_url=""
    local -a auth_args=()
    echo "  download: $url"
    discover_github_token
    if [[ -n "$DOWNLOAD_AUTH_TOKEN" ]]; then
        private_api_url="$(resolve_private_asset_api_url "$asset_name" || true)"
        if [[ -n "$private_api_url" ]]; then
            url="$private_api_url"
            auth_args=(
                -H "Authorization: Bearer ${DOWNLOAD_AUTH_TOKEN}"
                -H 'Accept: application/octet-stream'
            )
        else
            auth_args=(-H "Authorization: Bearer ${DOWNLOAD_AUTH_TOKEN}")
        fi
    fi
    if command -v curl >/dev/null 2>&1; then
        if ! curl -fL --retry 8 --retry-all-errors --continue-at - \
            "${auth_args[@]}" -o "$partial" "$url"; then
            echo "ERROR: Release download failed. This repository may be private." >&2
            echo "  Authenticate with 'gh auth login', export GH_TOKEN, or keep the token used by git credential." >&2
            return 1
        fi
    elif command -v wget >/dev/null 2>&1; then
        local -a wget_auth_args=()
        if [[ -n "$DOWNLOAD_AUTH_TOKEN" ]]; then
            wget_auth_args=(--header="Authorization: Bearer ${DOWNLOAD_AUTH_TOKEN}")
        fi
        wget -c "${wget_auth_args[@]}" -O "$partial" "$url"
    else
        echo "ERROR: automatic Release download requires curl or wget" >&2
        return 1
    fi
    mv -f "$partial" "$destination"
}

verify_checksum_file() {
    local checksum_file="$1"
    local checksum_dir checksum_name
    checksum_dir="$(cd "$(dirname "$checksum_file")" && pwd)"
    checksum_name="$(basename "$checksum_file")"
    if command -v sha256sum >/dev/null 2>&1; then
        (cd "$checksum_dir" && sha256sum -c "$checksum_name")
    elif command -v shasum >/dev/null 2>&1; then
        (cd "$checksum_dir" && shasum -a 256 -c "$checksum_name")
    else
        echo "ERROR: sha256sum or shasum is required for offline verification" >&2
        exit 1
    fi
}

echo "============================================================"
echo "  ForL0 offline one-command bootstrap"
echo "============================================================"
echo "  Archive:     ${ARCHIVE}"
echo "  Project dir: ${SCRIPT_DIR}"
echo "  Run log:     ${RUN_LOG}"
echo "  Extract to:  ${BUNDLE_DIR}"
echo "  FLINK_HOME:  ${FLINK_HOME:-${HOME}/flink_home}"
echo ""

if [[ ! -f "$CHECKSUM_FILE" && "${FORL0_OFFLINE_ONLY:-false}" != "true" ]]; then
    echo "[0/4] Download Release checksum"
    download_release_asset "$(basename "$CHECKSUM_FILE")" "$CHECKSUM_FILE"
fi
if [[ ! -f "$ARCHIVE" && "${FORL0_OFFLINE_ONLY:-false}" != "true" ]]; then
    echo "[0/4] Download Release archive (resume is supported)"
    download_release_asset "$(basename "$ARCHIVE")" "$ARCHIVE"
fi

if [[ -f "$ARCHIVE" ]]; then
    if [[ ! -f "$CHECKSUM_FILE" ]]; then
        echo "ERROR: missing archive checksum: $CHECKSUM_FILE" >&2
        exit 1
    fi
    echo "[1/4] Verify archive SHA256"
    verify_checksum_file "$CHECKSUM_FILE"

    echo "[2/4] Extract offline bundle"
    mkdir -p "$EXTRACT_PARENT"
    tar -xzf "$ARCHIVE" -C "$EXTRACT_PARENT"
elif [[ -d "$BUNDLE_DIR" ]]; then
    echo "[1/4] Archive not present; reuse extracted bundle"
    echo "[2/4] Bundle already available"
else
    echo "ERROR: neither archive nor extracted bundle was found" >&2
    echo "  expected archive: $ARCHIVE" >&2
    echo "  expected bundle:  $BUNDLE_DIR" >&2
    exit 1
fi

[[ -f "$BUNDLE_DIR/offline_bundle_sha256.txt" ]] || {
    echo "ERROR: bundle checksum manifest is missing: $BUNDLE_DIR/offline_bundle_sha256.txt" >&2
    exit 1
}

echo "[3/4] Verify every file inside the bundle"
verify_checksum_file "$BUNDLE_DIR/offline_bundle_sha256.txt"

[[ -f "$BUNDLE_DIR/forl0-offline-app.sh" ]] || {
    echo "ERROR: bundle launcher is missing: $BUNDLE_DIR/forl0-offline-app.sh" >&2
    exit 1
}
chmod +x "$BUNDLE_DIR/forl0-offline-app.sh" "$BUNDLE_DIR/docker/"*.sh
if [[ -f "$BUNDLE_DIR/docker/lib/l0_detector.sh" ]]; then
    chmod +x "$BUNDLE_DIR/docker/lib/l0_detector.sh"
fi

echo "[4/4] Install and run ForL0 validation"
echo ""
export FLINK_HOME="${FLINK_HOME:-${HOME}/flink_home}"
if has_explicit_run_mode "$@"; then
    RUN_ARGS=("$@")
else
    echo "No run mode selected; running the complete Ascend reproduction matrix."
    echo "  workloads: W01-W02, N01-N14, C01-C08"
    echo "  failures:  keep going, then generate the final HTML report"
    RUN_ARGS=("${DEFAULT_RUN_ARGS[@]}" "$@")
fi
export FORL0_APP_ROOT="$BUNDLE_DIR"
export FORL0_CONTROL_ROOT="$SCRIPT_DIR"
exec bash "$SCRIPT_DIR/forl0-offline-app.sh" \
    --install-dir "${FORL0_INSTALL_DIR:-${HOME}/forl0-runtime}" \
    --results-dir "$DEFAULT_RESULTS_ROOT" \
    "${RUN_ARGS[@]}"
