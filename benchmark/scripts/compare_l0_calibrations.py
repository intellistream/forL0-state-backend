#!/usr/bin/env python3
"""Build a target/local cost table for high-fidelity offline modelling.

This is a calibration translator, not a fabricated benchmark result. It tells
the workload model how target L0 and target DRAM costs compare with local DRAM
at matching working-set sizes.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def curve_map(document: dict, name: str) -> dict[int, dict]:
    return {int(item["working_set_bytes"]): item for item in document.get(name) or []}


def worker_map(document: dict, name: str) -> dict[int, dict]:
    return {int(item["workers"]): item for item in document.get(name) or []}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", required=True, type=Path)
    parser.add_argument("--local", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    target = json.loads(args.target.read_text())
    local = json.loads(args.local.read_text())
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
    rows = []
    for size in common:
        tl0, theap, lheap = target_l0[size], target_heap[size], local_heap[size]
        rows.append({
            "working_set_bytes": size,
            "target_l0_random_read_ns": tl0["random_read_ns"],
            "target_heap_random_read_ns": theap["random_read_ns"],
            "local_heap_random_read_ns": lheap["random_read_ns"],
            "target_l0_vs_target_heap_latency_ratio": (
                tl0["random_read_ns"] / theap["random_read_ns"]
            ),
            "target_heap_vs_local_heap_latency_ratio": (
                theap["random_read_ns"] / lheap["random_read_ns"]
            ),
            "target_l0_vs_local_heap_latency_ratio": (
                tl0["random_read_ns"] / lheap["random_read_ns"]
            ),
            "target_l0_read_gib_s": tl0["sequential_read_gib_s"],
            "target_heap_read_gib_s": theap["sequential_read_gib_s"],
            "local_heap_read_gib_s": lheap["sequential_read_gib_s"],
        })
    output = {
        "schema_version": 1,
        "evidence_label": "simulation/model",
        "model_scope": "memory-cost calibration; not a real workload measurement",
        "target_vs_local_hash_compute_ratio": (
            target["cpu_hash_mix_mops_s"] / local["cpu_hash_mix_mops_s"]
        ),
        "rows": rows,
        "parallel_read_rows": [
            {
                "workers": workers,
                "target_l0_gib_s": target_l0_workers[workers]["sequential_read_gib_s"],
                "target_heap_gib_s": target_heap_workers[workers]["sequential_read_gib_s"],
                "local_heap_gib_s": local_heap_workers[workers]["sequential_read_gib_s"],
                "target_l0_vs_local_heap_bandwidth_ratio": (
                    target_l0_workers[workers]["sequential_read_gib_s"]
                    / local_heap_workers[workers]["sequential_read_gib_s"]
                ),
            }
            for workers in sorted(
                set(target_l0_workers) & set(target_heap_workers) & set(local_heap_workers)
            )
        ],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2) + "\n")
    print(f"L0 model calibration saved: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
