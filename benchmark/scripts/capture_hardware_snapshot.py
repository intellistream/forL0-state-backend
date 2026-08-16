#!/usr/bin/env python3
"""Capture a read-only, credential-free hardware context for an experiment.

The snapshot is evidence about the real host.  It intentionally does not dump
the process environment, network configuration, user database, or command-line
arguments, because results are commonly uploaded to a public repository.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import platform
import shutil
import stat
import subprocess
from pathlib import Path
from typing import Any


def read_text(path: Path, limit: int = 1_000_000) -> str | None:
    try:
        return path.read_text(errors="replace")[:limit].strip()
    except OSError:
        return None


def run(command: list[str], timeout: int = 10) -> dict[str, Any]:
    executable = shutil.which(command[0])
    if not executable:
        return {"available": False}
    try:
        proc = subprocess.run(
            [executable, *command[1:]],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout,
            check=False,
        )
        return {
            "available": True,
            "exit_code": proc.returncode,
            "output": proc.stdout.strip()[:1_000_000],
        }
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"available": True, "error": type(exc).__name__}


def file_record(path_text: str | None, include_hash: bool = False) -> dict[str, Any]:
    if not path_text:
        return {"present": False}
    path = Path(path_text)
    record: dict[str, Any] = {"path": str(path), "present": path.exists()}
    try:
        info = path.stat()
        record.update({
            "mode": stat.filemode(info.st_mode),
            "size_bytes": info.st_size,
            "device_major": os.major(info.st_rdev) if stat.S_ISCHR(info.st_mode) else None,
            "device_minor": os.minor(info.st_rdev) if stat.S_ISCHR(info.st_mode) else None,
        })
        if include_hash and path.is_file():
            digest = hashlib.sha256()
            with path.open("rb") as handle:
                for block in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(block)
            record["sha256"] = digest.hexdigest()
    except OSError as exc:
        record["stat_error"] = type(exc).__name__
    return record


def cache_topology() -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for directory in sorted(Path("/sys/devices/system/cpu/cpu0/cache").glob("index*")):
        item = {"index": directory.name}
        for name in (
            "level", "type", "size", "coherency_line_size",
            "ways_of_associativity", "number_of_sets", "shared_cpu_list",
        ):
            item[name] = read_text(directory / name)
        records.append(item)
    return records


def numa_topology() -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for directory in sorted(Path("/sys/devices/system/node").glob("node[0-9]*")):
        records.append({
            "node": directory.name,
            "cpulist": read_text(directory / "cpulist"),
            "distance": read_text(directory / "distance"),
            "meminfo": read_text(directory / "meminfo"),
        })
    return records


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--project-root", required=True, type=Path)
    args = parser.parse_args()

    device = os.environ.get("FORL0_L0_DEVICE_PATH")
    library = os.environ.get("FORL0_L0_LIBRARY_PATH")
    snapshot = {
        "schema_version": 1,
        "evidence_label": "real-hardware-context",
        "captured_at": dt.datetime.now(dt.timezone.utc).astimezone().isoformat(),
        "note": (
            "Topology and software identity support local modelling; this file "
            "does not make simulated performance equivalent to a real L0 run."
        ),
        "platform": {
            "machine": platform.machine(),
            "kernel": platform.release(),
            "system": platform.system(),
            "page_size_bytes": os.sysconf("SC_PAGE_SIZE"),
            "cpu_affinity": sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None,
            "os_release": read_text(Path("/etc/os-release")),
            "lscpu": run(["lscpu", "--json"]),
            "proc_cpuinfo": read_text(Path("/proc/cpuinfo")),
            "proc_meminfo": read_text(Path("/proc/meminfo")),
            "transparent_hugepage": read_text(
                Path("/sys/kernel/mm/transparent_hugepage/enabled")
            ),
            "clocksource": read_text(
                Path("/sys/devices/system/clocksource/clocksource0/current_clocksource")
            ),
            "cpu_frequency_governor": read_text(
                Path("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
            ),
            "cpu_cache": cache_topology(),
            "numa_nodes": numa_topology(),
            "numactl": run(["numactl", "--hardware"]),
            "cgroup": {
                "membership": read_text(Path("/proc/self/cgroup")),
                "cpuset_effective": read_text(Path("/sys/fs/cgroup/cpuset.cpus.effective")),
                "cpu_max": read_text(Path("/sys/fs/cgroup/cpu.max")),
                "memory_max": read_text(Path("/sys/fs/cgroup/memory.max")),
            },
        },
        "l0": {
            "detected": bool(device and library),
            "device": file_record(device),
            "library": file_record(library, include_hash=True),
            "library_source": os.environ.get("FORL0_L0_LIBRARY_SOURCE"),
            "library_dependencies": run(["ldd", library]) if library else {"available": False},
            "library_symbols": run(["nm", "-D", library]) if library else {"available": False},
        },
        "software": {
            "git": run(["git", "-C", str(args.project_root), "status", "--short", "--branch"]),
            "docker": run(["docker", "version", "--format", "{{json .}}"]),
            "java": run(["java", "-version"]),
            "gcc": run(["gcc", "--version"]),
            "python": platform.python_version(),
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + ".tmp")
    temporary.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False) + "\n")
    temporary.replace(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
