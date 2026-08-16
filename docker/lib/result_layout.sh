#!/usr/bin/env bash
# Run-scoped result staging and flat latest-result publication.

forl0_validate_run_id() {
    local run_id="${1:-}"
    [[ -n "$run_id" ]] || {
        echo "ERROR: benchmark run ID must not be empty" >&2
        return 1
    }
    [[ "$run_id" != "." && "$run_id" != ".." ]] || {
        echo "ERROR: unsafe benchmark run ID: $run_id" >&2
        return 1
    }
    [[ "$run_id" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || {
        echo "ERROR: benchmark run ID contains unsafe characters: $run_id" >&2
        return 1
    }
}

forl0_results_base() {
    local project_root="$1"
    local configured="${FORL0_RESULTS_BASE:-${project_root}/benchmark/results}"
    if [[ "$configured" != /* ]]; then
        configured="${project_root}/${configured}"
    fi
    if [[ -L "$configured" ]]; then
        echo "ERROR: results base must not be a symlink: $configured" >&2
        return 1
    fi
    mkdir -p "$configured"
    (cd "$configured" && pwd -P)
}

forl0_remove_direct_child() {
    local root="$1"
    local target="$2"
    local root_abs parent_abs target_name

    [[ -d "$root" && ! -L "$root" ]] || {
        echo "ERROR: cleanup root is not a real directory: $root" >&2
        return 1
    }
    [[ -e "$target" || -L "$target" ]] || return 0
    [[ ! -L "$target" ]] || {
        echo "ERROR: refusing to remove symlinked result entry: $target" >&2
        return 1
    }

    root_abs="$(cd "$root" && pwd -P)"
    parent_abs="$(cd "$(dirname "$target")" && pwd -P)"
    target_name="$(basename "$target")"
    [[ "$parent_abs" == "$root_abs" && -n "$target_name" && "$target_name" != "." && "$target_name" != ".." ]] || {
        echo "ERROR: refusing out-of-scope result cleanup: $target" >&2
        return 1
    }
    rm -rf -- "$target"
}

forl0_prune_staging_runs() {
    local results_base="$1"
    local runs_root="${results_base}/runs"
    local entry

    [[ ! -L "$runs_root" ]] || {
        echo "ERROR: runs root must not be a symlink: $runs_root" >&2
        return 1
    }
    mkdir -p "$runs_root"
    while IFS= read -r -d '' entry; do
        echo "[results] Removing superseded staging run: $entry" >&2
        forl0_remove_direct_child "$runs_root" "$entry"
    done < <(find "$runs_root" -mindepth 1 -maxdepth 1 -print0)
}

forl0_prepare_campaign() {
    local results_base="$1"
    local run_id="$2"
    local runs_root campaign_root

    forl0_validate_run_id "$run_id"
    forl0_prune_staging_runs "$results_base"
    runs_root="${results_base}/runs"
    campaign_root="${runs_root}/${run_id}"
    [[ ! -e "$campaign_root" && ! -L "$campaign_root" ]] || {
        echo "ERROR: campaign output already exists: $campaign_root" >&2
        return 1
    }
    mkdir -p "${campaign_root}/smoke" "${campaign_root}/formal"
    printf '%s\n' "$campaign_root"
}

forl0_write_campaign_manifest() {
    local campaign_root="$1"
    local run_id="$2"
    local status="$3"
    local control_revision="${4:-unavailable}"
    local started_at="${5:-unknown}"
    local finished_at="${6:-}"
    local dirty="${7:-unknown}"
    local dirty_json

    forl0_validate_run_id "$run_id"
    case "$status" in
        running|failed|complete|interrupted) ;;
        *)
            echo "ERROR: invalid campaign status: $status" >&2
            return 1
            ;;
    esac
    [[ "$control_revision" =~ ^[A-Za-z0-9._-]+$ ]] || control_revision="unavailable"
    [[ "$dirty" == "true" || "$dirty" == "false" || "$dirty" == "unknown" ]] || dirty="unknown"
    if [[ "$dirty" == "unknown" ]]; then
        dirty_json="null"
    else
        dirty_json="$dirty"
    fi

    {
        printf '{\n'
        printf '  "schema_version": 1,\n'
        printf '  "layout": "flat-latest-v1",\n'
        printf '  "evidence_label": "real-online",\n'
        printf '  "run_id": "%s",\n' "$run_id"
        printf '  "status": "%s",\n' "$status"
        printf '  "control_revision": "%s",\n' "$control_revision"
        printf '  "control_worktree_dirty": %s,\n' "$dirty_json"
        printf '  "started_at": "%s",\n' "$started_at"
        if [[ -n "$finished_at" ]]; then
            printf '  "finished_at": "%s",\n' "$finished_at"
        else
            printf '  "finished_at": null,\n'
        fi
        printf '  "entry_point": "./reproduce-all",\n'
        printf '  "smoke_results": "smoke",\n'
        printf '  "formal_results": "formal"\n'
        printf '}\n'
    } > "${campaign_root}/run_manifest.json"
}

forl0_mark_campaign_failed() {
    local campaign_root="$1"
    local status="$2"
    {
        printf 'ForL0 reproduce-all campaign failed\n'
        printf 'date=%s\n' "$(date '+%Y-%m-%d %H:%M:%S %z')"
        printf 'status=%s\n' "$status"
        printf 'log=%s\n' "${campaign_root}/.logs"
        printf 'note=partial outputs are diagnostic only and were not published as latest\n'
    } > "${campaign_root}/FAILED.txt"
}

forl0_publish_flat_latest() {
    local results_base="$1"
    local campaign_root="$2"
    local runs_root="${results_base}/runs"
    local latest="${results_base}/latest"
    local temporary="${results_base}/.latest.tmp.$$"
    local previous="${results_base}/.latest.previous.$$"
    local source relative flat_name destination
    local copied=0

    [[ -d "$campaign_root" && ! -L "$campaign_root" ]] || {
        echo "ERROR: campaign root is not a real directory: $campaign_root" >&2
        return 1
    }
    [[ "$(cd "$(dirname "$campaign_root")" && pwd -P)" == "$(cd "$runs_root" && pwd -P)" ]] || {
        echo "ERROR: campaign is outside the staging root: $campaign_root" >&2
        return 1
    }
    forl0_validate_run_id "$(basename "$campaign_root")"
    [[ ! -e "$temporary" && ! -L "$temporary" ]] || {
        echo "ERROR: temporary latest path already exists: $temporary" >&2
        return 1
    }
    [[ ! -e "$previous" && ! -L "$previous" ]] || {
        echo "ERROR: previous latest path already exists: $previous" >&2
        return 1
    }
    if [[ -L "$latest" || ( -e "$latest" && ! -d "$latest" ) ]]; then
        echo "ERROR: latest must be a real directory when present: $latest" >&2
        return 1
    fi

    mkdir -p "$temporary"
    printf 'flat_filename\tsource_path\n' > "${temporary}/UPLOAD_MANIFEST.tsv"
    while IFS= read -r -d '' source; do
        relative="${source#${campaign_root}/}"
        if [[ "$relative" == ".logs" ]]; then
            flat_name="campaign.log"
        else
            flat_name="${relative//\//__}"
        fi
        [[ -n "$flat_name" && "$flat_name" != "UPLOAD_MANIFEST.tsv" ]] || {
            echo "ERROR: reserved or empty flat result filename from: $relative" >&2
            forl0_remove_direct_child "$results_base" "$temporary"
            return 1
        }
        destination="${temporary}/${flat_name}"
        if [[ -e "$destination" || -L "$destination" ]]; then
            echo "ERROR: flat result filename collision: $flat_name" >&2
            forl0_remove_direct_child "$results_base" "$temporary"
            return 1
        fi
        cp -p -- "$source" "$destination"
        printf '%s\t%s\n' "$flat_name" "$relative" >> "${temporary}/UPLOAD_MANIFEST.tsv"
        copied=$((copied + 1))
    done < <(find "$campaign_root" -type f -print0 | sort -z)

    if (( copied == 0 )); then
        echo "ERROR: refusing to publish an empty campaign: $campaign_root" >&2
        forl0_remove_direct_child "$results_base" "$temporary"
        return 1
    fi
    if find "$temporary" -mindepth 1 -type d -print -quit | grep -q .; then
        echo "ERROR: flat latest staging unexpectedly contains a directory" >&2
        forl0_remove_direct_child "$results_base" "$temporary"
        return 1
    fi

    if [[ -d "$latest" ]]; then
        mv -- "$latest" "$previous"
    fi
    if ! mv -- "$temporary" "$latest"; then
        [[ ! -d "$previous" ]] || mv -- "$previous" "$latest"
        return 1
    fi
    [[ ! -d "$previous" ]] || forl0_remove_direct_child "$results_base" "$previous"
    forl0_remove_direct_child "$runs_root" "$campaign_root"
    printf '%s\n' "$latest"
}
