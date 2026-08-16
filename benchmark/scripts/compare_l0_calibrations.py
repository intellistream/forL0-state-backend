#!/usr/bin/env python3
"""Build a target/local cost table for high-fidelity offline modelling.

This is a calibration translator, not a fabricated benchmark result. It tells
the workload model how target L0 and target DRAM costs compare with local DRAM
at matching working-set sizes.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path


def curve_map(document: dict, name: str) -> dict[int, dict]:
    return {int(item["working_set_bytes"]): item for item in document.get(name) or []}


def worker_map(document: dict, name: str) -> dict[int, dict]:
    return {int(item["workers"]): item for item in document.get(name) or []}


def require_positive(value: object, description: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise SystemExit(f"{description} is not numeric") from exc
    if not math.isfinite(number) or number <= 0:
        raise SystemExit(f"{description} must be finite and positive")
    return number


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", required=True, type=Path)
    parser.add_argument("--local", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    target_bytes = args.target.read_bytes()
    local_bytes = args.local.read_bytes()
    target = json.loads(target_bytes)
    local = json.loads(local_bytes)
    if target.get("status") != "complete":
        raise SystemExit("target calibration is not complete")
    if target.get("evidence_label") != "real-hardware-calibration":
        raise SystemExit("target calibration is not labelled real-hardware-calibration")
    if local.get("status") not in ("heap-only", "complete"):
        raise SystemExit("local DRAM calibration is not complete")
    if not target.get("l0"):
        raise SystemExit("target calibration does not contain a real L0 curve")
    target_l0 = curve_map(target, "l0")
    target_heap = curve_map(target, "heap")
    local_heap = curve_map(local, "heap")
    target_l0_workers = worker_map(target, "l0_parallel_read_scaling")
    target_heap_workers = worker_map(target, "heap_parallel_read_scaling")
    local_heap_workers = worker_map(local, "heap_parallel_read_scaling")
    common = sorted(set(target_l0) & set(target_heap) & set(local_heap))
    if not common:
        raise SystemExit("calibrations have no matching working-set sizes")
    common_workers = sorted(
        set(target_l0_workers) & set(target_heap_workers) & set(local_heap_workers))
    if not common_workers:
        raise SystemExit("calibrations have no matching parallel worker counts")
    rows = []
    for size in common:
        tl0, theap, lheap = target_l0[size], target_heap[size], local_heap[size]
        tl0_latency = require_positive(
            tl0.get("random_read_ns"), f"target L0 latency at {size} bytes")
        theap_latency = require_positive(
            theap.get("random_read_ns"), f"target heap latency at {size} bytes")
        lheap_latency = require_positive(
            lheap.get("random_read_ns"), f"local heap latency at {size} bytes")
        tl0_read = require_positive(
            tl0.get("sequential_read_gib_s"), f"target L0 bandwidth at {size} bytes")
        theap_read = require_positive(
            theap.get("sequential_read_gib_s"), f"target heap bandwidth at {size} bytes")
        lheap_read = require_positive(
            lheap.get("sequential_read_gib_s"), f"local heap bandwidth at {size} bytes")
        rows.append({
            "working_set_bytes": size,
            "target_l0_random_read_ns": tl0_latency,
            "target_heap_random_read_ns": theap_latency,
            "local_heap_random_read_ns": lheap_latency,
            "target_l0_vs_target_heap_latency_ratio": tl0_latency / theap_latency,
            "target_heap_vs_local_heap_latency_ratio": theap_latency / lheap_latency,
            "target_l0_vs_local_heap_latency_ratio": tl0_latency / lheap_latency,
            "target_l0_read_gib_s": tl0_read,
            "target_heap_read_gib_s": theap_read,
            "local_heap_read_gib_s": lheap_read,
        })
    parallel_rows = []
    for workers in common_workers:
        tl0_read = require_positive(
            target_l0_workers[workers].get("sequential_read_gib_s"),
            f"target L0 parallel bandwidth with {workers} workers")
        theap_read = require_positive(
            target_heap_workers[workers].get("sequential_read_gib_s"),
            f"target heap parallel bandwidth with {workers} workers")
        lheap_read = require_positive(
            local_heap_workers[workers].get("sequential_read_gib_s"),
            f"local heap parallel bandwidth with {workers} workers")
        parallel_rows.append({
            "workers": workers,
            "target_l0_gib_s": tl0_read,
            "target_heap_gib_s": theap_read,
            "local_heap_gib_s": lheap_read,
            "target_l0_vs_local_heap_bandwidth_ratio": tl0_read / lheap_read,
        })
    hotset_rows = []
    for item in target.get("l0_hotset_pressure_curve") or []:
        requested_bytes = int(item["requested_active_bytes"])
        actual_bytes = int(item["actual_active_bytes"])
        sets = int(item["sets"])
        if requested_bytes <= 0 or actual_bytes <= 0 or sets <= 0:
            raise SystemExit("HotSet pressure sizes and set counts must be positive")
        hotset_rows.append({
            "requested_active_bytes": requested_bytes,
            "actual_active_bytes": actual_bytes,
            "sets": sets,
            "init_ns_per_set": require_positive(
                item.get("init_ns_per_set"), "HotSet initialization latency"),
            "lookup_ns_per_op": require_positive(
                item.get("lookup_ns_per_op"), "HotSet lookup latency"),
            "update_ns_per_op": require_positive(
                item.get("update_ns_per_op"), "HotSet update latency"),
        })
    hotset_rows.sort(key=lambda row: row["actual_active_bytes"])
    output = {
        "schema_version": 2,
        "evidence_label": "simulation/model",
        "model_scope": "memory-cost calibration; not a real workload measurement",
        "target_calibration": str(args.target.resolve()),
        "target_calibration_sha256": hashlib.sha256(target_bytes).hexdigest(),
        "local_calibration": str(args.local.resolve()),
        "local_calibration_sha256": hashlib.sha256(local_bytes).hexdigest(),
        "target_vs_local_hash_compute_ratio": (
            require_positive(target.get("cpu_hash_mix_mops_s"), "target hash rate")
            / require_positive(local.get("cpu_hash_mix_mops_s"), "local hash rate")
        ),
        "rows": rows,
        "parallel_read_rows": parallel_rows,
        "hotset_pressure_rows": hotset_rows,
    }
    output["coverage"] = {
        "dense_working_set_bytes": [row["working_set_bytes"] for row in rows],
        "parallel_workers": [row["workers"] for row in output["parallel_read_rows"]],
        "hotset_active_bytes": [
            row["actual_active_bytes"] for row in output["hotset_pressure_rows"]],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2) + "\n")
    print(f"L0 model calibration saved: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
