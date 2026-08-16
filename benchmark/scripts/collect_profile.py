#!/usr/bin/env python3
"""Collect durable hardware and staged L0/DRAM calibration evidence."""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


L0_ALLOCATION_SIZES_MB = (1, 2, 4, 6)


def write_json(path: Path, payload: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def run_probe(binary: Path, library: str, output: Path, size: int,
              evidence_label: str, timeout: int,
              command_prefix: list[str] | None = None,
              tuner_capacity: int | None = None) -> dict[str, Any]:
    partial = output.with_suffix(".partial.json")
    stage_file = Path(str(partial) + ".stage")
    partial.unlink(missing_ok=True)
    stage_file.unlink(missing_ok=True)
    command = list(command_prefix or []) + [
        str(binary), library, str(partial), str(size), evidence_label]
    if tuner_capacity is not None:
        command.append(str(tuner_capacity))
    started = datetime.now(timezone.utc).isoformat()
    try:
        completed = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
        returncode = completed.returncode
        stdout = completed.stdout
        stderr = completed.stderr
    except subprocess.TimeoutExpired as exc:
        returncode = 124
        stdout = exc.stdout or ""
        stderr = (exc.stderr or "") + f"\nprobe timed out after {timeout}s"

    payload: dict[str, Any] = {}
    if partial.is_file() and partial.stat().st_size:
        try:
            payload = json.loads(partial.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = {}
    partial.unlink(missing_ok=True)
    last_stage = None
    if stage_file.is_file():
        try:
            last_stage = stage_file.read_text(encoding="utf-8").strip() or None
        except OSError:
            last_stage = None
    stage_file.unlink(missing_ok=True)
    if returncode != 0 or not payload:
        payload = {
            "schema_version": 1,
            "evidence_label": evidence_label,
            "status": "failed",
            "requested_bytes": size,
            "tuner_capacity_bytes": tuner_capacity if tuner_capacity is not None else size,
            "returncode": returncode,
            "signal": -returncode if returncode < 0 else None,
            "reason": "isolated calibration probe failed",
        }
    if last_stage:
        payload["last_stage"] = last_stage
        if payload.get("status") == "failed" and "failure_stage" not in payload:
            payload["failure_stage"] = last_stage
    payload["command"] = command
    payload["started_at"] = started
    payload["finished_at"] = datetime.now(timezone.utc).isoformat()
    payload["stdout"] = stdout[-4000:]
    payload["stderr"] = stderr[-4000:]
    write_json(output, payload)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--simulate", action="store_true",
                        help="Collect host DRAM calibration only and label it simulation/model.")
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument(
        "--minimum-l0-tuner-mb", type=int,
        default=int(os.environ.get("FORL0_PROFILE_MIN_TUNER_MB", "64")),
        help="Minimum production-shaped vendor tuner capacity (default: 64 MiB).")
    args = parser.parse_args()

    root = args.project_root.resolve()
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    evidence_label = "simulation/model" if args.simulate else "real-hardware-calibration"
    if args.minimum_l0_tuner_mb <= 0:
        parser.error("--minimum-l0-tuner-mb must be positive")
    git_revision = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "HEAD"], capture_output=True, text=True)
    git_dirty = subprocess.run(
        ["git", "-C", str(root), "status", "--porcelain"], capture_output=True, text=True)
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "evidence_label": evidence_label,
        "status": "running",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "host": platform.node(),
        "machine": platform.machine(),
        "profile_mode": "simulation" if args.simulate else "real",
        "control_revision": (git_revision.stdout.strip()
                             if git_revision.returncode == 0 else "unavailable"),
        "control_worktree_dirty": (bool(git_dirty.stdout.strip())
                                   if git_dirty.returncode == 0 else None),
        "python": sys.executable,
        "cluster_isolated_before_profile": (
            os.environ.get("FORL0_PROFILE_CLUSTER_CLEANED", "").lower() == "true"),
        "l0_probe_strategy": {
            "kind": "production-shaped-process-pool",
            "allocation_working_sets_mb": list(L0_ALLOCATION_SIZES_MB),
            "dense_measurement_limit_mb": 1,
            "extended_access_pattern": "hotset-sparse",
            "minimum_tuner_capacity_mb": args.minimum_l0_tuner_mb,
        },
        "probes": [],
    }
    write_json(output / "profile_manifest.json", manifest)

    snapshot_script = root / "benchmark/scripts/capture_hardware_snapshot.py"
    snapshot = output / "hardware_snapshot.json"
    snapshot_run = subprocess.run(
        [sys.executable, str(snapshot_script), "--project-root", str(root), "--output", str(snapshot)],
        capture_output=True, text=True)
    manifest["hardware_snapshot"] = {
        "path": snapshot.name,
        "returncode": snapshot_run.returncode,
        "stderr": snapshot_run.stderr[-4000:],
    }

    compiler = shutil.which("g++")
    source = root / "benchmark/native/l0_calibrate.cpp"
    binary = output / ".l0_calibrate"
    if not compiler or not source.is_file():
        manifest["status"] = "failed"
        manifest["reason"] = "g++ or calibration source unavailable"
        manifest["finished_at"] = datetime.now(timezone.utc).isoformat()
        write_json(output / "profile_manifest.json", manifest)
        return 1
    compile_run = subprocess.run(
        [compiler, "-O3", "-std=c++17", "-pthread", str(source), "-ldl", "-o", str(binary)],
        capture_output=True, text=True)
    if compile_run.returncode != 0:
        manifest["status"] = "failed"
        manifest["reason"] = "calibration helper compilation failed"
        manifest["compiler_stderr"] = compile_run.stderr[-8000:]
        manifest["finished_at"] = datetime.now(timezone.utc).isoformat()
        write_json(output / "profile_manifest.json", manifest)
        return 1

    # A heap reference is always collected. Real L0 allocations are isolated
    # and grow gradually, so one vendor-library crash cannot destroy the rest
    # of the profile bundle.
    heap = run_probe(binary, "-", output / "dram_calibration.json", 32 << 20,
                     evidence_label, args.timeout)
    manifest["probes"].append({"kind": "dram", "file": "dram_calibration.json",
                                "status": heap.get("status")})

    l0_library = os.environ.get("FORL0_L0_LIBRARY_PATH", "").strip()
    l0_device = os.environ.get("FORL0_L0_DEVICE_PATH", "").strip()
    successful_l0 = 0
    best_l0_payload: dict[str, Any] | None = None
    successful_sizes_mb: list[int] = []
    if not args.simulate and l0_library and l0_device:
        # Match production ownership without stress-touching the entire tuner
        # quota: dense latency/bandwidth stays bounded to the proven 1 MiB
        # region, while larger allocations use the production HotSet layout.
        for size_mb in L0_ALLOCATION_SIZES_MB:
            name = f"l0_calibration_{size_mb}mb.json"
            tuner_capacity = max(args.minimum_l0_tuner_mb, size_mb) << 20
            probe = run_probe(binary, l0_library, output / name, size_mb << 20,
                              evidence_label, args.timeout,
                              tuner_capacity=tuner_capacity)
            manifest["probes"].append({
                "kind": "l0",
                "allocation_mb": size_mb,
                "tuner_capacity_mb": tuner_capacity >> 20,
                "file": name,
                "status": probe.get("status"),
            })
            if probe.get("status") == "complete":
                successful_l0 += 1
                best_l0_payload = probe
                successful_sizes_mb.append(size_mb)
            # Once a safe allocation cannot be established, larger
            # requests are neither useful nor safe in this host context. This
            # also avoids turning a resource-busy init failure into a later
            # vendor-library crash.
            if probe.get("status") != "complete":
                break
    else:
        manifest["l0_reason"] = "simulation mode or L0 device/library not detected"

    # Reproduce the process-scoped production ownership model: one tuner per
    # TaskManager, concurrently. Pin instances to distinct NUMA nodes when the
    # host exposes usable CPU lists.
    expected_instances = max(1, int(os.environ.get("FORL0_EXPECTED_TASKMANAGERS", "2")))
    instance_results: list[dict[str, Any]] = []
    if successful_sizes_mb and not args.simulate:
        concurrent_size_mb = max(successful_sizes_mb)
        numa_cpus: list[str] = []
        for cpulist in sorted(Path("/sys/devices/system/node").glob("node*/cpulist")):
            try:
                first_range = cpulist.read_text(encoding="utf-8").strip().split(",", 1)[0]
                numa_cpus.append(first_range.split("-", 1)[0])
            except OSError:
                pass
        taskset = shutil.which("taskset")

        def instance_probe(instance: int) -> dict[str, Any]:
            prefix = ([taskset, "-c", numa_cpus[instance % len(numa_cpus)]]
                      if taskset and numa_cpus else [])
            name = f"l0_instance_{instance + 1}_{concurrent_size_mb}mb.json"
            payload = run_probe(
                binary, l0_library, output / name, concurrent_size_mb << 20,
                evidence_label, args.timeout, prefix,
                tuner_capacity=max(args.minimum_l0_tuner_mb, concurrent_size_mb) << 20)
            return {"instance": instance + 1, "file": name,
                    "numa_cpu": numa_cpus[instance % len(numa_cpus)] if numa_cpus else None,
                    "status": payload.get("status"), "signal": payload.get("signal")}

        with ThreadPoolExecutor(max_workers=expected_instances) as executor:
            instance_results = list(executor.map(instance_probe, range(expected_instances)))
        manifest["parallel_instance_probe"] = {
            "expected_instances": expected_instances,
            "allocation_mb_per_instance": concurrent_size_mb,
            "tuner_capacity_mb_per_instance": max(
                args.minimum_l0_tuner_mb, concurrent_size_mb),
            "instances": instance_results,
            "status": ("complete" if all(item["status"] == "complete" for item in instance_results)
                       else "failed"),
        }

    if best_l0_payload is not None:
        write_json(output / "l0_calibration.json", best_l0_payload)
    else:
        write_json(output / "l0_calibration.json", {
            "schema_version": 1,
            "evidence_label": evidence_label,
            "status": "unavailable" if not l0_library or not l0_device else "failed",
            "reason": manifest.get("l0_reason", "all isolated L0 probes failed"),
            "successful_probes": successful_l0,
        })

    binary.unlink(missing_ok=True)
    manifest["l0_successful_probes"] = successful_l0
    manifest["finished_at"] = datetime.now(timezone.utc).isoformat()
    if heap.get("status") not in ("heap-only", "complete"):
        manifest["status"] = "failed"
    elif not args.simulate and (not l0_library or not l0_device):
        manifest["status"] = "failed"
        manifest["reason"] = "real profile requires a detected L0 device and vendor library"
    elif (not args.simulate and l0_library and l0_device
          and successful_l0 != len(L0_ALLOCATION_SIZES_MB)):
        manifest["status"] = "failed"
        manifest["reason"] = (
            "staged L0 calibration incomplete: "
            f"{successful_l0}/{len(L0_ALLOCATION_SIZES_MB)} probes succeeded")
    elif instance_results and not all(item["status"] == "complete" for item in instance_results):
        manifest["status"] = "failed"
        manifest["reason"] = "single-instance L0 succeeded but concurrent TaskManager-shaped probes failed"
    else:
        manifest["status"] = "complete"
    write_json(output / "profile_manifest.json", manifest)
    print(output / "profile_manifest.json")
    return 0 if manifest["status"] == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())
