#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$REPO_ROOT/docker/lib/result_layout.sh"

TEST_TMP="$(mktemp -d /tmp/forl0-result-layout.XXXXXX)"
cleanup() {
    rm -rf "$TEST_TMP"
}
trap cleanup EXIT

RESULTS_BASE="$TEST_TMP/results"
mkdir -p "$RESULTS_BASE/latest" "$RESULTS_BASE/runs/old_failed"
printf 'old latest\n' > "$RESULTS_BASE/latest/old.txt"
printf 'old failed\n' > "$RESULTS_BASE/runs/old_failed/FAILED.txt"

campaign_root="$(forl0_prepare_campaign "$RESULTS_BASE" campaign42)"
[[ ! -e "$RESULTS_BASE/runs/old_failed" ]]
[[ -f "$RESULTS_BASE/latest/old.txt" ]]

mkdir -p "$campaign_root/smoke/raw" "$campaign_root/formal/nexmark_123"
printf '{"smoke": true}\n' > "$campaign_root/smoke/raw/client.json"
printf '{"formal": true}\n' > "$campaign_root/formal/nexmark_123/nexmark_results.json"
printf 'complete log\n' > "$campaign_root/.logs"
forl0_write_campaign_manifest "$campaign_root" campaign42 complete abc123 \
    2026-08-16T10:00:00+0800 2026-08-16T11:00:00+0800 false

latest="$(forl0_publish_flat_latest "$RESULTS_BASE" "$campaign_root")"
[[ "$latest" == "$RESULTS_BASE/latest" ]]
[[ ! -e "$campaign_root" ]]
[[ ! -e "$latest/old.txt" ]]
[[ -f "$latest/campaign.log" ]]
[[ -f "$latest/run_manifest.json" ]]
[[ -f "$latest/smoke__raw__client.json" ]]
[[ -f "$latest/formal__nexmark_123__nexmark_results.json" ]]
[[ -f "$latest/UPLOAD_MANIFEST.tsv" ]]
if find "$latest" -mindepth 1 -type d -print -quit | grep -q .; then
    echo "latest must not contain directories" >&2
    exit 1
fi
grep -q $'campaign.log\t.logs' "$latest/UPLOAD_MANIFEST.tsv"
grep -q $'formal__nexmark_123__nexmark_results.json\tformal/nexmark_123/nexmark_results.json' \
    "$latest/UPLOAD_MANIFEST.tsv"

collision_root="$(forl0_prepare_campaign "$RESULTS_BASE" collision42)"
mkdir -p "$collision_root/a"
printf 'first\n' > "$collision_root/a/b"
printf 'second\n' > "$collision_root/a__b"
if forl0_publish_flat_latest "$RESULTS_BASE" "$collision_root" >/dev/null 2>&1; then
    echo "flat filename collision was accepted" >&2
    exit 1
fi
[[ -d "$collision_root" ]]
[[ -f "$latest/run_manifest.json" ]]
if find "$RESULTS_BASE" -mindepth 1 -maxdepth 1 -name '.latest.tmp.*' -print -quit | grep -q .; then
    echo "failed publication left a temporary latest directory" >&2
    exit 1
fi

if forl0_validate_run_id '../escape' >/dev/null 2>&1; then
    echo "unsafe run ID was accepted" >&2
    exit 1
fi

echo "result layout tests passed"
