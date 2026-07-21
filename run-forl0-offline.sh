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
BUNDLE_NAME="forl0-offline-linux-arm64-py310-20260721"
ARCHIVE="${FORL0_OFFLINE_ARCHIVE:-${SCRIPT_DIR}/${BUNDLE_NAME}.tar.gz}"
EXTRACT_PARENT="${FORL0_EXTRACT_PARENT:-${SCRIPT_DIR}}"
BUNDLE_DIR="${EXTRACT_PARENT}/${BUNDLE_NAME}"
CHECKSUM_FILE="${ARCHIVE}.sha256"

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
echo "  Extract to:  ${BUNDLE_DIR}"
echo "  FLINK_HOME:  ${FLINK_HOME:-${HOME}/flink_home}"
echo ""

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
exec bash "$BUNDLE_DIR/forl0-offline-app.sh" \
    --install-dir "${FORL0_INSTALL_DIR:-${HOME}/forl0-runtime}" \
    "$@"
