#!/usr/bin/env python3
"""Resumable ForL0 parameter search with real and simulation/model modes."""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
import os
import random
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]


TUNABLES = {
    "initial_table_capacity",
    "max_table_capacity",
    "main_table_load_factor_threshold",
    "l0_cache_size",
    "l0_memory_max_size",
}


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def size_mb(value: Any) -> float:
    text = str(value).strip().lower()
    factors = {"kb": 1 / 1024, "mb": 1, "gb": 1024}
    for suffix, factor in factors.items():
        if text.endswith(suffix):
            return float(text[:-len(suffix)]) * factor
    return float(text) / (1024 * 1024)


def candidate_id(index: int, params: dict[str, Any]) -> str:
    digest = hashlib.sha256(json.dumps(params, sort_keys=True).encode()).hexdigest()[:8]
    return f"trial_{index:03d}_{digest}"


def generate_candidates(space: dict[str, Any], limit: int) -> list[dict[str, Any]]:
    parameters = space["parameters"]
    names = list(parameters)
    combinations = [dict(zip(names, values)) for values in itertools.product(
        *(parameters[name] for name in names))]
    rng = random.Random(int(space.get("seed", 20260816)))
    rng.shuffle(combinations)
    # Always include the current safe reference point before sampled variants.
    reference = {
        "initial_table_capacity": 2048,
        "max_table_capacity": 2097152,
        "main_table_load_factor_threshold": 0.80,
        "l0_cache_size": "256mb",
        "l0_memory_max_size": "1280mb",
    }
    combinations = [reference] + [item for item in combinations if item != reference]
    return combinations[:limit]


def patch_config(node: Any, params: dict[str, Any]) -> None:
    if isinstance(node, dict):
        for key in list(node):
            if key in TUNABLES:
                node[key] = params[key]
            else:
                patch_config(node[key], params)
    elif isinstance(node, list):
        for item in node:
            patch_config(item, params)


def load_calibration_model(path: Path) -> tuple[dict[str, Any], str]:
    raw = path.read_bytes()
    model = json.loads(raw)
    if model.get("evidence_label") != "simulation/model":
        raise SystemExit("calibration model must be labelled simulation/model")
    if model.get("schema_version") != 2:
        raise SystemExit("calibration model schema_version 2 is required")
    if not model.get("rows") or not model.get("parallel_read_rows"):
        raise SystemExit("calibration model lacks dense or parallel L0 rows")
    return model, hashlib.sha256(raw).hexdigest()


def simulation_metrics(params: dict[str, Any], trial_index: int,
                       calibration: dict[str, Any]) -> dict[str, Any]:
    """Calibration-informed deterministic model, never measured workload evidence."""
    initial = float(params["initial_table_capacity"])
    maximum = float(params["max_table_capacity"])
    load = float(params["main_table_load_factor_threshold"])
    cache = size_mb(params["l0_cache_size"])
    native = size_mb(params["l0_memory_max_size"])

    dense = max(calibration["rows"], key=lambda row: row["working_set_bytes"])
    latency_speedup = max(
        0.25, dense["target_heap_random_read_ns"] / dense["target_l0_random_read_ns"])
    parallel_rows = calibration["parallel_read_rows"]
    preferred_parallel = min(parallel_rows, key=lambda row: abs(row["workers"] - 2))
    bandwidth_ratio = max(
        0.25, preferred_parallel["target_l0_gib_s"] /
        preferred_parallel["target_heap_gib_s"])
    pressure_rows = calibration.get("hotset_pressure_rows") or []
    if pressure_rows:
        pressure_first = pressure_rows[0]
        pressure_last = pressure_rows[-1]
        lookup_pressure = max(
            0.25, pressure_first["lookup_ns_per_op"] / pressure_last["lookup_ns_per_op"])
        update_pressure = max(
            0.25, pressure_first["update_ns_per_op"] / pressure_last["update_ns_per_op"])
        pressure_factor = min(1.25, max(
            0.5, math.sqrt(lookup_pressure * update_pressure)))
    else:
        pressure_factor = 1.0
    compute_ratio = max(0.25, float(calibration.get(
        "target_vs_local_hash_compute_ratio", 1.0)))

    startup_penalty = abs(math.log2(initial / 2048.0)) * 0.035 / min(2.0, compute_ratio)
    load_penalty = abs(load - 0.82) * 0.65
    measured_cache_weight = min(0.24, 0.055 * max(0.0, latency_speedup - 1.0))
    measured_bandwidth_weight = min(0.08, 0.04 * max(0.0, bandwidth_ratio - 1.0))
    cache_gain = ((measured_cache_weight + measured_bandwidth_weight)
                  * (1.0 - math.exp(-cache / 180.0)) * pressure_factor)
    memory_gain = (0.06 + min(0.05, 0.02 * max(0.0, compute_ratio - 1.0))) * min(
        1.0, max(0.0, (native - 700.0) / 500.0))
    capacity_gain = 0.035 * min(1.0, math.log2(maximum / 1048576.0 + 1.0))
    deterministic_noise = ((trial_index * 7919) % 101 - 50) / 10000.0
    objective = 1.0 + cache_gain + memory_gain + capacity_gain - startup_penalty - load_penalty + deterministic_noise
    stable = native >= 900 and maximum >= 1048576 and initial <= 8192
    workload_ids = ["W01", "W02"] + [f"N{i:02d}" for i in range(1, 15)] + [f"C{i:02d}" for i in range(1, 9)]
    workloads = {}
    for offset, workload_id in enumerate(workload_ids):
        is_baseline = int(workload_id[1:]) % 2 == 1
        base = 50_000.0 + offset * 13_777.0
        workloads[workload_id] = base if is_baseline else base * objective
    return {
        "status": "complete" if stable else "rejected",
        "stable": stable,
        "objective": objective if stable else 0.0,
        "predicted_throughput": workloads,
        "model_inputs": {
            "working_set_bytes": dense["working_set_bytes"],
            "target_l0_vs_heap_latency_speedup": latency_speedup,
            "target_l0_vs_heap_parallel_bandwidth_ratio": bandwidth_ratio,
            "hotset_pressure_factor": pressure_factor,
            "target_vs_local_hash_compute_ratio": compute_ratio,
        },
        "model_notes": (
            "Calibration-informed response model for candidate screening only; "
            "not real-online throughput evidence."),
    }


def extract_numeric_metrics(root: Path) -> list[float]:
    values: list[float] = []
    for path in root.rglob("*.json"):
        if path.name in {"trial_manifest.json", "parameters.json"}:
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        stack = [payload]
        while stack:
            value = stack.pop()
            if isinstance(value, dict):
                for key, child in value.items():
                    if key in ("throughput", "throughput_per_core") and isinstance(child, (int, float)) and child > 0:
                        values.append(float(child))
                    else:
                        stack.append(child)
            elif isinstance(value, list):
                stack.extend(value)
    return values


def run_real_trial(root: Path, trial_dir: Path, trial_id: str,
                   config_path: Path, workload_ids: list[str]) -> dict[str, Any]:
    workload_scores: dict[str, float] = {}
    workload_records = []
    complete_count = 0
    for workload_id in workload_ids:
        workload_dir = trial_dir / "workloads" / workload_id
        workload_dir.mkdir(parents=True, exist_ok=True)
        workload_manifest_path = workload_dir / "workload_manifest.json"
        if workload_manifest_path.is_file():
            try:
                previous = json.loads(workload_manifest_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                previous = {}
            if previous.get("status") == "complete" and isinstance(previous.get("score"), (int, float)):
                workload_scores[workload_id] = float(previous["score"])
                workload_records.append(previous)
                complete_count += 1
                print(f"[tuning] {trial_id} resume workload {workload_id}", flush=True)
                continue

        command = [
            "bash", str(root / "run-forl0-offline.sh"), "--workloads", workload_id,
            "--no-report", "--skip-docker-load",
        ]
        env = os.environ.copy()
        env.update({
            "FORL0_BENCHMARK_CONFIG": str(config_path),
            "FORL0_RESULTS_DIR": str(workload_dir / "results"),
            "FORL0_RUN_ID": f"{os.environ.get('FORL0_RUN_ID', 'tuning')}_{trial_id}_{workload_id}",
            "FORL0_RUN_STARTED_EPOCH": str(int(datetime.now().timestamp())),
            "FORL0_VARIANT": f"{trial_id}_{workload_id}",
            "FORL0_L0_STRICT_OVERRIDE": "true",
            "FORL0_RUN_LOG": "/dev/null",
        })
        log_path = workload_dir / "workload.log"
        record = {
            "schema_version": 1,
            "evidence_label": "real-online",
            "trial": trial_id,
            "workload_id": workload_id,
            "control_revision": os.environ.get("FORL0_CONTROL_REVISION", "unavailable"),
            "status": "running",
            "started_at": now(),
            "command": command,
        }
        write_json(workload_manifest_path, record)
        with log_path.open("w", encoding="utf-8", errors="replace") as log:
            completed = subprocess.run(command, cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT)
        values = extract_numeric_metrics(workload_dir / "results")
        score = (math.exp(sum(math.log(value) for value in values) / len(values))
                 if values else None)
        record.update({
            "status": "complete" if completed.returncode == 0 and score else "failed",
            "finished_at": now(),
            "returncode": completed.returncode,
            "metrics_found": len(values),
            "score": score,
            "log": log_path.name,
        })
        write_json(workload_manifest_path, record)
        workload_records.append(record)
        if record["status"] == "complete" and score:
            workload_scores[workload_id] = score
            complete_count += 1
        print(f"[tuning] {trial_id} workload {workload_id}: {record['status']}", flush=True)

    pair_ratios = []
    for prefix, count in (("W", 2), ("N", 14), ("C", 8)):
        for number in range(1, count + 1, 2):
            baseline_id = f"{prefix}{number:02d}"
            candidate_id_value = f"{prefix}{number + 1:02d}"
            baseline = workload_scores.get(baseline_id)
            candidate = workload_scores.get(candidate_id_value)
            if baseline and candidate:
                pair_ratios.append(candidate / baseline)
    objective = (math.exp(sum(math.log(ratio) for ratio in pair_ratios) / len(pair_ratios))
                 if len(pair_ratios) == len(workload_ids) // 2 else None)
    all_complete = complete_count == len(workload_ids) and objective is not None
    return {
        "status": "complete" if all_complete else "failed",
        "objective": objective,
        "objective_definition": "geometric mean of 12 ForL0/baseline paired workload ratios",
        "workload_ids": workload_ids,
        "workloads_complete": complete_count,
        "workloads_planned": len(workload_ids),
        "pair_ratios_complete": len(pair_ratios),
        "workloads": workload_records,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--mode", choices=("real", "simulate"), default="real")
    parser.add_argument("--calibration-model", type=Path)
    parser.add_argument("--max-trials", type=int)
    args = parser.parse_args()

    calibration_model: dict[str, Any] | None = None
    calibration_sha256: str | None = None
    if args.mode == "simulate":
        if args.calibration_model is None:
            parser.error("--calibration-model is required in simulate mode")
        calibration_model, calibration_sha256 = load_calibration_model(
            args.calibration_model.resolve())

    root = args.project_root.resolve()
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    space_path = root / "benchmark/config/tuning_space.yaml"
    base_path = root / "benchmark/config/benchmark.yaml"
    space = yaml.safe_load(space_path.read_text(encoding="utf-8"))
    total_combinations = math.prod(len(values) for values in space["parameters"].values())
    max_trials = args.max_trials if args.max_trials is not None else int(os.environ.get(
        "FORL0_TUNING_MAX_TRIALS", space.get("max_trials", total_combinations)))
    if max_trials <= 0:
        raise SystemExit("max trials must be positive")
    candidates = generate_candidates(space, max_trials)
    evidence = "simulation/model" if args.mode == "simulate" else "real-online"
    manifest_path = output / "tuning_manifest.json"
    if manifest_path.is_file():
        try:
            previous_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise SystemExit(f"cannot resume malformed tuning manifest: {exc}")
        previous_mode = previous_manifest.get("mode")
        if previous_mode and previous_mode != args.mode:
            raise SystemExit(
                f"refusing to mix tuning modes in {output}: existing={previous_mode}, requested={args.mode}"
            )
        previous_model = previous_manifest.get("calibration_model_sha256")
        if args.mode == "simulate":
            if not previous_model:
                raise SystemExit(
                    "refusing to resume legacy simulation without calibration model provenance; "
                    "choose a new FORL0_RUN_ID")
            if previous_model != calibration_sha256:
                raise SystemExit(
                    "refusing to resume simulation with a different calibration model: "
                    f"existing={previous_model}, requested={calibration_sha256}")
    manifest = {
        "schema_version": 1,
        "evidence_label": evidence,
        "status": "running",
        "mode": args.mode,
        "started_at": now(),
        "parameter_space": str(space_path.relative_to(root)),
        "base_config": str(base_path.relative_to(root)),
        "planned_trials": len(candidates),
        "total_parameter_combinations": total_combinations,
        "exhaustive": len(candidates) == total_combinations,
        "workload_ids": space["workload_ids"],
        "workload_count": len(space["workload_ids"]),
        "entry_point": "./reproduce-all --full" + (" --simulate" if args.mode == "simulate" else ""),
    }
    if args.mode == "simulate":
        manifest["calibration_model"] = str(args.calibration_model.resolve())
        manifest["calibration_model_sha256"] = calibration_sha256
        manifest["model_scope"] = calibration_model.get("model_scope")
    write_json(manifest_path, manifest)

    ranked = []
    for index, params in enumerate(candidates, 1):
        trial_id = candidate_id(index, params)
        trial_dir = output / trial_id
        trial_manifest = trial_dir / "trial_manifest.json"
        if trial_manifest.is_file():
            try:
                existing = json.loads(trial_manifest.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                existing = {}
            if existing.get("status") in ("complete", "rejected"):
                if existing.get("status") == "complete" and isinstance(existing.get("objective"), (int, float)):
                    ranked.append({"trial": trial_id, "objective": existing["objective"], "parameters": params})
                print(f"[tuning] resume: skip {trial_id} ({existing.get('status')})", flush=True)
                continue
        trial_dir.mkdir(parents=True, exist_ok=True)
        write_json(trial_dir / "parameters.json", params)
        running = {"schema_version": 1, "evidence_label": evidence, "trial": trial_id,
                   "status": "running", "started_at": now(), "parameters": params}
        write_json(trial_manifest, running)
        print(f"[tuning] {index}/{len(candidates)} {trial_id}: {params}", flush=True)
        if args.mode == "simulate":
            assert calibration_model is not None
            result = simulation_metrics(params, index, calibration_model)
            write_json(trial_dir / "simulation_result.json", {
                "evidence_label": evidence,
                "calibration_model_sha256": calibration_sha256,
                **result})
        else:
            config = yaml.safe_load(base_path.read_text(encoding="utf-8"))
            patch_config(config, params)
            config["_tuning"] = {"trial": trial_id, "parameters": params}
            config_path = trial_dir / "benchmark_config.yaml"
            config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
            result = run_real_trial(root, trial_dir, trial_id, config_path, space["workload_ids"])
        running.update(result)
        running["finished_at"] = now()
        write_json(trial_manifest, running)
        if result.get("status") == "complete" and isinstance(result.get("objective"), (int, float)):
            ranked.append({"trial": trial_id, "objective": result["objective"], "parameters": params})

    ranked.sort(key=lambda item: item["objective"], reverse=True)
    write_json(output / "ranking.json", {"evidence_label": evidence, "ranked_trials": ranked})
    if ranked:
        (output / "best_parameters.yaml").write_text(
            yaml.safe_dump(ranked[0]["parameters"], sort_keys=False), encoding="utf-8")
        best_config = output / ranked[0]["trial"] / "benchmark_config.yaml"
        if best_config.is_file():
            shutil.copy2(best_config, output / "best_benchmark_config.yaml")
    trial_statuses: list[str] = []
    for path in output.glob("trial_*/trial_manifest.json"):
        try:
            status = json.loads(path.read_text(encoding="utf-8")).get("status")
        except (OSError, json.JSONDecodeError):
            status = "invalid"
        trial_statuses.append(str(status))
    completed_trials = trial_statuses.count("complete")
    rejected_trials = trial_statuses.count("rejected")
    failed_trials = sum(
        status not in ("complete", "rejected") for status in trial_statuses)
    evaluated_trials = completed_trials + rejected_trials
    all_evaluated = evaluated_trials == len(candidates)
    manifest["status"] = "complete" if all_evaluated and failed_trials == 0 else "failed"
    manifest["finished_at"] = now()
    manifest["evaluated_trials"] = evaluated_trials
    manifest["completed_trials"] = completed_trials
    manifest["rejected_trials"] = rejected_trials
    manifest["failed_trials"] = failed_trials
    manifest["ranked_trials"] = len(ranked)
    write_json(manifest_path, manifest)
    print(f"[tuning] {manifest['status']}: {output}")
    return 0 if manifest["status"] == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())
