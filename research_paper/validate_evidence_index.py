#!/usr/bin/env python3
"""Validate the paper evidence index using only the Python standard library.

This is a restricted-token plus exact-sentence regression gate for the canonical
manuscript, not a general natural-language semantics proof. Unlisted paraphrases
outside the canonical paper remain a review limitation rather than a claim that
the validator recognizes every possible wording.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import statistics
import subprocess
import sys
from pathlib import Path


COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
VERIFIED_STATES = {"fresh_verified", "raw_verified"}
MECHANISM_STATES = {"implemented", "provisional", "absent"}
CANONICAL_PAPER_PATH = (
    "research_paper/TKDE-template/Conference-LaTeX-template_10-17-19/"
    "briskstate_tkde.tex"
)
CANONICAL_PROTECTED_SECTIONS = (
    ("full_manuscript", r"\documentclass", r"\end{document}"),
)
NATIVE_EVIDENCE_BINDING = (
    "correctness-native-130",
    "03558094e43cb12612daf9544c33f49741f562f4",
    "research_paper/evidence/native_tests_20260731.txt",
    "b675f48809fa7bf570521cf82372ea6ce75b46762d6efea5ff6666eecc79cb9e",
)
CANONICAL_MECHANISMS = {
    "native_swiss_table_simd": {
        "status": "implemented",
        "architecture_role": "native_state_table_with_group_probing",
        "symbols": (
            ("src/main/native/engine/swiss_table.h", "class SwissTable", "present"),
            ("src/main/native/platform/simd.h", "struct ProbeSeq", "present"),
        ),
        "tests": (
            (
                "src/main/native/test/swiss_table_test.cpp",
                "TEST(SwissTableTest, AutomaticGrowth)",
            ),
        ),
        "evidence": (
            (
                "src/main/native/platform/simd.h",
                "static constexpr size_t kGroupWidth = 16;",
            ),
        ),
        "evidence_claims": (NATIVE_EVIDENCE_BINDING,),
        "paper_claim_patterns": (r"Swiss[- ]table", r"SwissTable"),
    },
    "l0_hot_cache": {
        "status": "implemented",
        "architecture_role": "cache_above_state_table",
        "symbols": (
            ("src/main/native/engine/hot_cache.h", "class HotCacheManager", "present"),
            (
                "src/main/native/engine/allocator.h",
                "L0 is now a hot-key cache above StateTable",
                "present",
            ),
        ),
        "tests": (
            (
                "src/main/native/test/hot_cache_test.cpp",
                "TEST(HotCacheLL, PutThenGet)",
            ),
        ),
        "evidence": (
            (
                "src/main/native/engine/allocator.h",
                "L0 is now a hot-key cache above StateTable",
            ),
        ),
        "evidence_claims": (NATIVE_EVIDENCE_BINDING,),
        "paper_claim_patterns": (r"HotCache", r"hot-key cache"),
    },
    "cow_checkpoint": {
        "status": "implemented",
        "architecture_role": "snapshot_consistency_overlay",
        "symbols": (
            (
                "src/main/native/engine/state_engine.h",
                "uint64_t prepare_snapshot()",
                "present",
            ),
            (
                "src/main/native/checkpoint/checkpoint_writer.h",
                "snapshot-consistent iteration (COW)",
                "present",
            ),
        ),
        "tests": (
            (
                "src/main/native/test/checkpoint_round_trip_test.cpp",
                "TEST(CheckpointRoundTripTest, COWConsistency)",
            ),
            (
                "src/main/native/test/state_table_cow_test.cpp",
                "TEST(StateTableCOWTest, SnapshotSeesOldValues)",
            ),
        ),
        "evidence": (
            (
                "src/main/native/checkpoint/checkpoint_writer.h",
                "snapshot-consistent iteration (COW)",
            ),
        ),
        "evidence_claims": (NATIVE_EVIDENCE_BINDING,),
        "paper_claim_patterns": (r"copy-on-write", r"\bCoW\b"),
    },
    "split_allocation_layout": {
        "status": "provisional",
        "architecture_role": "test_only_allocator_extension_point",
        "symbols": (
            ("src/main/native/engine/allocator.h", "struct SplitResult", "present"),
            (
                "src/main/native/engine/swiss_table.h",
                "allocate_split(cb, 64, slots_bytes, sa)",
                "present",
            ),
        ),
        "tests": (
            (
                "src/main/native/test/swiss_table_test.cpp",
                "TEST(SwissTableSplitTest, GrowthTransitionUnifiedToSplit)",
            ),
        ),
        "evidence": (
            (
                "src/main/native/engine/allocator.h",
                "return SplitResult{p, static_cast<char*>(p) + ctrl_padded, false};",
            ),
        ),
        "evidence_claims": (NATIVE_EVIDENCE_BINDING,),
        "paper_claim_patterns": (r"split allocation", r"allocation split"),
    },
    "incremental_extendible_hash_split": {
        "status": "absent",
        "architecture_role": "absent_hash_growth_mechanism",
        "symbols": (
            (
                "src/main/native/engine/swiss_table.h",
                "incremental_hash_split",
                "absent",
            ),
        ),
        "tests": (
            (
                "research_paper/test_validate_evidence_index.py",
                "test_allocation_split_cannot_impersonate_hash_split",
            ),
        ),
        "evidence": (
            (
                "src/main/native/engine/swiss_table.h",
                "Move all FULL entries from old to new",
            ),
        ),
        "evidence_claims": (),
        "paper_claim_patterns": (
            r"incremental\s+(?:extendible[- ]hash\s+)?split",
            r"extendible\s+hash(?:ing)?",
            r"split.{0,100}(?:growing\s+)?hash[- ]table\s+buckets?.{0,100}incremental",
            r"(?:hash[- ]table\s+)?buckets?.{0,100}(?:split|grow).{0,100}director",
            r"director.{0,100}(?:split|grow).{0,100}(?:hash[- ]table\s+)?buckets?",
            r"(?:does\s+not\s+lack|never\s+lacks?|not\s+without).{0,100}incremental.{0,100}director(?:y|ies)?[- ]based.{0,100}bucket(?:\s+division|s)?",
        ),
    },
    "forl0_state_map": {
        "status": "absent",
        "architecture_role": "absent_directory_router",
        "symbols": (
            (
                "src/main/java/org/apache/flink/state/forl0/ForL0StateMap.java",
                "class ForL0StateMap",
                "absent",
            ),
        ),
        "tests": (
            (
                "research_paper/test_validate_evidence_index.py",
                "test_nonexistent_forl0_state_map_cannot_be_implemented",
            ),
        ),
        "evidence": (
            (
                "src/main/java/org/apache/flink/state/forl0/ForL0KeyedStateBackend.java",
                "class ForL0KeyedStateBackend",
            ),
        ),
        "evidence_claims": (),
        "paper_claim_patterns": (r"ForL0StateMap",),
    },
}
CANONICAL_PROVISIONAL_CLAIM_GUARDS = {
    "correctness-java-54-and-recovery": {
        "status": "blocked",
        "patterns": (
            r"\b184\b",
            r"(?:\b54\b.{0,80}(?:Java|JVM|test|recover)|(?:Java|JVM|test|recover).{0,80}\b54\b)",
        ),
    },
    "operator-jmh-13-of-14": {
        "status": "derived_missing_raw",
        "patterns": (r"\b13\s+(?:of|/)\s*14\b", r"\b132\s*\\?%", r"\b110\s*\\?%"),
    },
    "nexmark-runtime-comparison": {
        "status": "derived_missing_raw",
        "patterns": (
            r"(?:NexMark.{0,240}(?:outperform|improv|faster|shorten|speedup|stuck|stall|robust|stabil|avoid|gain)|(?:outperform|improv|faster|shorten|speedup|stuck|stall|robust|stabil|avoid|gain).{0,240}NexMark)",
        ),
    },
    "vtune-memory-bound": {
        "status": "documentary_only",
        "patterns": (
            r"\b48\.1\b",
            r"\b15\.3\b",
            r"\b30\.8\b",
            r"\b2\.2\b",
            r"VTune.{0,180}(?:improv|reduc|show|demonstrat|confirm)",
        ),
    },
    "ascend-july-backend-jni": {
        "status": "documentary_only",
        "patterns": (
            r"(?:July|Ascend).{0,180}(?:backend|JNI|native[- ]table)",
        ),
    },
    "real-l0-hardware-speedup": {
        "status": "gap",
        "patterns": (
            r"(?:real[- ]?L0|L0\s+hardware).{0,180}(?:speedup|improv|accelerat|faster|gain)",
            r"(?:speedup|improv|accelerat|faster|gain).{0,180}(?:real[- ]?L0|L0\s+hardware)",
        ),
    },
}
JAVA_FLINK_CONTEXT_RE = re.compile(
    r"\b(?:Flink|JVM|checkpoint|savepoint|restart|relaunch|restoration|restore|restored|"
    r"recovery|failover|snapshot|snapshots|snapshotted)\b",
    re.IGNORECASE,
)
DIRECTORY_MECHANISM_RE = re.compile(
    r"\b(?:bucket|buckets|growth|grow|growing|expand|expansion|partition|partitioning|"
    r"fission|redistribute|redistribution|split|splitting|division)\b",
    re.IGNORECASE,
)
CANONICAL_RESTRICTED_SENTENCE_ALLOWLIST: tuple[str, ...] = (
    r"\title{\briskstate: A Cache-Aware State Backend for Apache Flink}",
    "In Apache Flink, this path is exercised by point updates, aggregations, joins, "
    "timers, windows, and checkpointing logic, so state management often dominates "
    "end-to-end performance once working sets exceed cache capacity or heap pressure increases.",
    "The default choices available to many Flink deployments present a familiar trade-off.",
    "The implementation includes a Java class implementing Flink backend interfaces, "
    "specialized fast paths for primitive keys, and an optional native execution path "
    "for off-heap state management.",
    "Modern stateful dataflow systems such as MillWheel, Naiad, Flink, Structured Streaming, "
    "and the Dataflow model define execution semantics for continuous queries, event time, "
    "and out-of-order processing, but they generally abstract away the physical state layout "
    "that ultimately determines runtime cost.",
    "Within Flink, keyed state is partitioned by key-group for scalability and checkpointing.",
    "Existing work on Flink state management and migration has focused on consistency, "
    "operational continuity, and reconfiguration cost rather than on exposing or optimizing "
    "memory-locality effects inside the backend itself.",
    "Flink routes keyed state through key-groups, which already provide a natural granularity "
    "for scalability, checkpointing, and reconfiguration.",
    "Stateful stream-processing systems such as MillWheel, Naiad, Flink, and Structured "
    "Streaming define the execution model for continuous queries and event-time processing, "
    "but they do not prescribe one physical state layout within the backend.",
    "Work on Flink state management and migration has focused on fault tolerance, state "
    "redistribution, and correctness under reconfiguration rather than on the memory-locality "
    "properties of in-memory state structures.",
    "Systems such as Flink, MillWheel, and Structured Streaming established the semantics "
    "that make stateful stream processing practical.",
)
CANONICAL_ALLOWED_NEGATIVE_CLAUSES = {
    "mechanism_boundary": (
        "Incremental extendible-hash split is absent; split allocation is test-only, "
        "and production DefaultAllocator uses unified allocation."
    ),
    "correctness-java-54-and-recovery": (
        "Fresh JVM/Flink recovery evidence is missing; Flink State API semantics and "
        "checkpoint/savepoint recovery are not established by this revision."
    ),
    "operator-jmh-13-of-14": (
        "Fresh operator-level raw evidence is missing; operator-level performance gains "
        "are not established by this revision."
    ),
    "nexmark-runtime-comparison": (
        "Fresh NexMark raw evidence is missing; comparative runtime and completion outcomes "
        "are not established by this revision."
    ),
    "vtune-memory-bound": (
        "Fresh VTune raw profiler evidence is missing; microarchitectural improvements are "
        "not established by this revision."
    ),
    "ascend-july-backend-jni": (
        "Fresh Ascend backend/JNI evidence is missing; July backend/JNI performance claims "
        "are not established by this revision."
    ),
    "real-l0-hardware-speedup": (
        "Fresh real-L0 hardware evidence is missing; real-L0 performance gains are not "
        "established by this revision."
    ),
}


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant is forbidden: {value}")


def _reject_duplicate_json_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON key is forbidden: {key}")
        value[key] = item
    return value


def load_json(path: Path) -> object:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(
            handle,
            parse_constant=_reject_json_constant,
            object_pairs_hook=_reject_duplicate_json_keys,
        )


def repository_path(root: Path, value: str, errors: list[str], context: str) -> Path:
    path = (root / value).resolve()
    try:
        path.relative_to(root)
    except ValueError:
        errors.append(f"{context}: path escapes repository: {value}")
    return path


def _git(root: Path, *args: str) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        ["git", "-C", str(root), *args],
        capture_output=True,
    )


def _resolve_git_object(
    root: Path, revision: object, expected_type: str, errors: list[str], context: str
) -> str | None:
    if not isinstance(revision, str) or not COMMIT_RE.fullmatch(revision):
        errors.append(f"{context}: object id is not full 40-hex")
        return None
    resolved = _git(root, "rev-parse", "--verify", f"{revision}^{{{expected_type}}}")
    if resolved.returncode != 0:
        errors.append(f"{context}: Git {expected_type} object does not exist: {revision}")
        return None
    object_id = resolved.stdout.decode("ascii", errors="replace").strip()
    if object_id != revision and expected_type == "commit":
        errors.append(f"{context}: Git commit did not resolve exactly: {revision}")
        return None
    return object_id


def validate_repository_binding(root: Path, index: dict, errors: list[str]) -> None:
    repository = index.get("repository")
    if not isinstance(repository, dict):
        errors.append("repository must be an object")
        return
    required = {"head_commit", "source_tree_path", "source_tree", "paper_source", "notes"}
    if set(repository) != required:
        errors.append(
            "repository binding fields drifted: "
            f"missing={sorted(required - set(repository))!r} "
            f"extra={sorted(set(repository) - required)!r}"
        )
        return
    head = repository["head_commit"]
    if _resolve_git_object(root, head, "commit", errors, "repository.head_commit") is None:
        return
    source_tree_path = repository["source_tree_path"]
    if source_tree_path != "src/main":
        errors.append("repository.source_tree_path must be exactly 'src/main'")
        return
    declared_tree = repository["source_tree"]
    if not isinstance(declared_tree, str) or not COMMIT_RE.fullmatch(declared_tree):
        errors.append("repository.source_tree is not a full Git tree id")
        return
    committed_tree = _git(root, "rev-parse", f"{head}:{source_tree_path}")
    if committed_tree.returncode != 0:
        errors.append("repository source tree does not exist at head_commit")
        return
    actual_tree = committed_tree.stdout.decode("ascii", errors="replace").strip()
    if actual_tree != declared_tree:
        errors.append(
            "repository source_tree differs from head_commit: "
            f"declared={declared_tree} actual={actual_tree}"
        )
    tracked_diff = _git(root, "diff", "--quiet", head, "--", source_tree_path)
    if tracked_diff.returncode not in (0, 1):
        errors.append("cannot compare current source tree with repository.head_commit")
    elif tracked_diff.returncode == 1:
        errors.append("current source tree differs from repository.head_commit")
    source_status = _git(
        root, "status", "--porcelain", "--untracked-files=all", "--", source_tree_path
    )
    if source_status.returncode != 0:
        errors.append("cannot inspect current source-tree cleanliness")
    elif source_status.stdout.strip():
        errors.append("current source tree has tracked or untracked worktree changes")


def _positive_finite_number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        parsed = float(value)
    except OverflowError:
        return None
    return parsed if math.isfinite(parsed) and parsed > 0 else None


def _finite_number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        parsed = float(value)
    except OverflowError:
        return None
    return parsed if math.isfinite(parsed) else None


def validate_ratio_grid(root: Path, claim: dict, errors: list[str]) -> None:
    check = claim.get("quantitative_check")
    if not check:
        return
    if not isinstance(check, dict):
        errors.append(f"{claim['id']}: quantitative_check must be an object")
        return
    if check.get("type") != "wordcount_ratio_grid":
        errors.append(f"{claim['id']}: unknown quantitative check {check.get('type')!r}")
        return

    required = {"raw_path", "grid", "count", "all_greater_than_one", "mean", "max"}
    missing = required - set(check)
    if missing:
        errors.append(
            f"{claim['id']}: quantitative check missing fields: "
            f"{', '.join(sorted(missing))}"
        )
        return

    raw_path = repository_path(root, check["raw_path"], errors, claim["id"])
    if not raw_path.is_file():
        errors.append(f"{claim['id']}: quantitative raw file missing: {check['raw_path']}")
        return
    payload = load_json(raw_path)
    records = payload.get("records", []) if isinstance(payload, dict) else []
    grid = check.get("grid")
    expected_grid_keys = {
        "num_keys",
        "skew_factors",
        "benchmark",
        "total_records",
        "parallelism",
    }
    if not isinstance(grid, dict) or set(grid) != expected_grid_keys:
        errors.append(f"{claim['id']}: grid contract must declare exact workload axes")
        return
    num_keys = grid["num_keys"]
    skew_factors = grid["skew_factors"]
    if (
        not isinstance(grid["benchmark"], str)
        or not grid["benchmark"]
        or isinstance(grid["total_records"], bool)
        or not isinstance(grid["total_records"], int)
        or grid["total_records"] <= 0
        or isinstance(grid["parallelism"], bool)
        or not isinstance(grid["parallelism"], int)
        or grid["parallelism"] <= 0
    ):
        errors.append(f"{claim['id']}: grid workload contract is invalid")
        return
    if (
        not isinstance(num_keys, list)
        or not num_keys
        or any(isinstance(value, bool) or not isinstance(value, int) or value <= 0 for value in num_keys)
        or len(set(num_keys)) != len(num_keys)
    ):
        errors.append(f"{claim['id']}: grid num_keys must be unique positive integers")
        return
    if (
        not isinstance(skew_factors, list)
        or not skew_factors
        or any(_finite_number(value) is None for value in skew_factors)
        or len({_finite_number(value) for value in skew_factors}) != len(skew_factors)
    ):
        errors.append(f"{claim['id']}: grid skew_factors must be unique finite numbers")
        return
    expected_coordinates = {
        (num_key, _finite_number(skew)) for num_key in num_keys for skew in skew_factors
    }
    if check["count"] != len(expected_coordinates):
        errors.append(
            f"{claim['id']}: declared count does not equal complete grid size"
        )
    if not isinstance(records, list):
        errors.append(f"{claim['id']}: quantitative records must be an array")
        return
    ratios: list[float] = []
    observed_coordinates: set[tuple[int, float]] = set()
    for index, record in enumerate(records):
        if not isinstance(record, dict) or set(record) != {"num_keys", "skew_factor", "results"}:
            errors.append(f"{claim['id']}: record {index} has invalid schema")
            return
        coordinate = (record["num_keys"], _finite_number(record["skew_factor"])) if (
            isinstance(record["num_keys"], int)
            and not isinstance(record["num_keys"], bool)
            and _finite_number(record["skew_factor"]) is not None
        ) else None
        if coordinate is None or coordinate not in expected_coordinates:
            errors.append(f"{claim['id']}: record {index} has an out-of-grid coordinate")
            return
        if coordinate in observed_coordinates:
            errors.append(f"{claim['id']}: duplicate grid coordinate {coordinate!r}")
            return
        observed_coordinates.add(coordinate)
        results = record["results"]
        if not isinstance(results, dict) or set(results) != {"hashmap", "forl0"}:
            errors.append(f"{claim['id']}: record {index} must contain exactly two backends")
            return
        throughputs: dict[str, float] = {}
        for backend in ("hashmap", "forl0"):
            result = results[backend]
            if not isinstance(result, dict):
                errors.append(f"{claim['id']}: record {index} {backend} result is invalid")
                return
            if (
                result.get("backend") != backend
                or result.get("benchmark") != grid["benchmark"]
                or result.get("total_records") != grid["total_records"]
                or result.get("parallelism") != grid["parallelism"]
            ):
                errors.append(
                    f"{claim['id']}: record {index} {backend} workload contract differs"
                )
                return
            elapsed = _positive_finite_number(result.get("total_time_seconds"))
            throughput = _positive_finite_number(result.get("throughput"))
            per_core = _positive_finite_number(result.get("throughput_per_core"))
            if elapsed is None or throughput is None or per_core is None:
                errors.append(f"{claim['id']}: record {index} {backend} metrics are invalid")
                return
            expected_throughput = grid["total_records"] / elapsed
            expected_per_core = throughput / grid["parallelism"]
            if not math.isclose(throughput, expected_throughput, rel_tol=1e-12, abs_tol=0.0):
                errors.append(
                    f"{claim['id']}: record {index} {backend} throughput is not records/time"
                )
            if not math.isclose(per_core, expected_per_core, rel_tol=1e-12, abs_tol=0.0):
                errors.append(
                    f"{claim['id']}: record {index} {backend} per-core throughput is inconsistent"
                )
            throughputs[backend] = throughput
        ratios.append(throughputs["forl0"] / throughputs["hashmap"])

    missing_coordinates = expected_coordinates - observed_coordinates
    if missing_coordinates:
        errors.append(
            f"{claim['id']}: grid is incomplete; missing {len(missing_coordinates)} coordinate(s)"
        )

    try:
        tolerance = float(check.get("tolerance", 1e-9))
        expected_values = {"mean": float(check["mean"]), "max": float(check["max"])}
    except (TypeError, ValueError) as exc:
        errors.append(f"{claim['id']}: invalid quantitative numeric field: {exc}")
        return
    if not math.isfinite(tolerance) or tolerance < 0:
        errors.append(f"{claim['id']}: tolerance must be a finite non-negative number")
        return
    actual = {
        "count": len(ratios),
        "all_greater_than_one": all(value > 1.0 for value in ratios),
        "mean": statistics.mean(ratios) if ratios else math.nan,
        "max": max(ratios, default=math.nan),
    }
    for field in ("count", "all_greater_than_one"):
        if actual[field] != check[field]:
            errors.append(
                f"{claim['id']}: {field} expected {check[field]!r}, got {actual[field]!r}"
            )
    for field in ("mean", "max"):
        if not math.isclose(
            actual[field], expected_values[field], rel_tol=0.0, abs_tol=tolerance
        ):
            errors.append(
                f"{claim['id']}: {field} expected {expected_values[field]:.15g}, "
                f"got {actual[field]:.15g}"
            )


def _validate_literal_reference(
    root: Path,
    reference: object,
    errors: list[str],
    context: str,
    *,
    allow_absent: bool = False,
) -> None:
    if not isinstance(reference, dict):
        errors.append(f"{context}: reference must be an object")
        return
    required = {"path", "literal"}
    missing = required - set(reference)
    if missing:
        errors.append(f"{context}: missing fields: {', '.join(sorted(missing))}")
        return
    relative = reference["path"]
    literal = reference["literal"]
    if not isinstance(relative, str) or not relative:
        errors.append(f"{context}: path must be a non-empty string")
        return
    if not isinstance(literal, str) or not literal:
        errors.append(f"{context}: literal must be a non-empty string")
        return
    path = repository_path(root, relative, errors, context)
    expected = reference.get("expected", "present")
    if expected not in {"present", "absent"}:
        errors.append(f"{context}: expected must be 'present' or 'absent'")
        return
    if expected == "absent":
        if not allow_absent:
            errors.append(f"{context}: absent references are not allowed here")
            return
        if path.is_file() and literal in path.read_text(encoding="utf-8", errors="replace"):
            errors.append(f"{context}: absent symbol unexpectedly exists: {relative}: {literal}")
        return
    if not path.is_file():
        errors.append(f"{context}: source file missing: {relative}")
        return
    if literal not in path.read_text(encoding="utf-8", errors="replace"):
        errors.append(f"{context}: expected symbol missing: {relative}: {literal}")


def _protected_section(
    text: str, section: object, errors: list[str], context: str
) -> str:
    if not isinstance(section, dict):
        errors.append(f"{context}: protected section must be an object")
        return ""
    missing = {"name", "start", "end"} - set(section)
    if missing:
        errors.append(f"{context}: missing fields: {', '.join(sorted(missing))}")
        return ""
    start = section["start"]
    end = section["end"]
    if not all(isinstance(value, str) and value for value in (start, end)):
        errors.append(f"{context}: start and end must be non-empty strings")
        return ""
    if text.count(start) != 1 or text.count(end) < 1:
        errors.append(f"{context}: protected section markers are missing or ambiguous")
        return ""
    start_at = text.index(start)
    end_at = text.find(end, start_at + len(start))
    if end_at < 0:
        errors.append(f"{context}: protected section end precedes or misses its start")
        return ""
    return text[start_at : end_at + len(end)]


def _contract_references(
    references: object,
    errors: list[str],
    context: str,
    *,
    include_expected: bool,
) -> tuple[tuple[str, ...], ...]:
    if not isinstance(references, list):
        errors.append(f"{context}: must be an array")
        return ()
    values: list[tuple[str, ...]] = []
    expected_keys = {"path", "literal", "expected"} if include_expected else {"path", "literal"}
    for index, reference in enumerate(references):
        item_context = f"{context}[{index}]"
        if not isinstance(reference, dict) or set(reference) != expected_keys:
            errors.append(
                f"{item_context}: fields must be exactly {', '.join(sorted(expected_keys))}"
            )
            continue
        row = (reference["path"], reference["literal"])
        if include_expected:
            row += (reference["expected"],)
        if not all(isinstance(value, str) for value in row):
            errors.append(f"{item_context}: all values must be strings")
            continue
        values.append(row)
    return tuple(values)


def _contract_evidence_claims(
    bindings: object, errors: list[str], context: str
) -> tuple[tuple[str, str, str, str], ...]:
    if not isinstance(bindings, list):
        errors.append(f"{context}: must be an array")
        return ()
    expected_keys = {"claim_id", "code_commit", "artifact_path", "artifact_sha256"}
    values: list[tuple[str, str, str, str]] = []
    for index, binding in enumerate(bindings):
        item_context = f"{context}[{index}]"
        if not isinstance(binding, dict) or set(binding) != expected_keys:
            errors.append(
                f"{item_context}: fields must be exactly {', '.join(sorted(expected_keys))}"
            )
            continue
        row = (
            binding["claim_id"],
            binding["code_commit"],
            binding["artifact_path"],
            binding["artifact_sha256"],
        )
        if not all(isinstance(value, str) for value in row):
            errors.append(f"{item_context}: all values must be strings")
            continue
        values.append(row)
    return tuple(values)


def validate_mechanism_contract(
    root: Path,
    contract_path: Path,
    claims_by_id: dict[str, dict],
    errors: list[str],
) -> list[str]:
    contract_path = contract_path.resolve()
    try:
        contract_path.relative_to(root)
    except ValueError:
        errors.append(f"mechanism contract escapes repository: {contract_path}")
        return []
    if not contract_path.is_file():
        errors.append(f"mechanism contract missing: {contract_path.relative_to(root)}")
        return []
    try:
        contract = load_json(contract_path)
    except (OSError, json.JSONDecodeError) as exc:
        errors.append(f"mechanism contract cannot be loaded: {exc}")
        return []
    if not isinstance(contract, dict):
        errors.append("mechanism contract root must be an object")
        return []
    if contract.get("schema_version") != 1:
        errors.append("mechanism contract schema_version must be 1")
    if set(contract) != {"schema_version", "purpose", "paper_sources", "mechanisms"}:
        errors.append("mechanism contract top-level fields drifted from the canonical schema")

    mechanisms = contract.get("mechanisms")
    if not isinstance(mechanisms, list):
        errors.append("mechanism contract mechanisms must be an array")
        return []
    seen: set[str] = set()
    for position, mechanism in enumerate(mechanisms):
        context = f"mechanism[{position}]"
        if not isinstance(mechanism, dict):
            errors.append(f"{context}: must be an object")
            continue
        missing = {
            "id",
            "status",
            "architecture_role",
            "symbols",
            "tests",
            "evidence",
            "evidence_claims",
            "paper_claim_patterns",
        } - set(mechanism)
        expected_fields = {
            "id",
            "status",
            "architecture_role",
            "symbols",
            "tests",
            "evidence",
            "evidence_claims",
            "paper_claim_patterns",
        }
        if missing or set(mechanism) != expected_fields:
            errors.append(f"{context}: fields drifted from the canonical mechanism schema")
            continue
        mechanism_id = mechanism["id"]
        if not isinstance(mechanism_id, str) or not mechanism_id:
            errors.append(f"{context}: id must be a non-empty string")
            continue
        if mechanism_id in seen:
            errors.append(f"{mechanism_id}: duplicate mechanism id")
        seen.add(mechanism_id)
        canonical = CANONICAL_MECHANISMS.get(mechanism_id)
        if canonical is None:
            errors.append(f"{mechanism_id}: unknown audited mechanism id")
            continue
        status = mechanism["status"]
        if not isinstance(status, str) or status not in MECHANISM_STATES:
            errors.append(f"{mechanism_id}: unknown mechanism status {status!r}")
        if status != canonical["status"]:
            errors.append(
                f"{mechanism_id}: audited status must be {canonical['status']!r}, got {status!r}"
            )
        architecture_role = mechanism["architecture_role"]
        if architecture_role != canonical["architecture_role"]:
            errors.append(
                f"{mechanism_id}: architecture_role must be {canonical['architecture_role']!r}; "
                f"got {architecture_role!r}"
            )

        symbols = _contract_references(
            mechanism["symbols"], errors, f"{mechanism_id}.symbols", include_expected=True
        )
        tests = _contract_references(
            mechanism["tests"], errors, f"{mechanism_id}.tests", include_expected=False
        )
        evidence = _contract_references(
            mechanism["evidence"], errors, f"{mechanism_id}.evidence", include_expected=False
        )
        evidence_claims = _contract_evidence_claims(
            mechanism["evidence_claims"], errors, f"{mechanism_id}.evidence_claims"
        )
        for field, actual in (
            ("symbols", symbols),
            ("tests", tests),
            ("evidence", evidence),
            ("evidence_claims", evidence_claims),
        ):
            if actual != canonical[field]:
                errors.append(f"{mechanism_id}: canonical {field} binding drifted")

        symbol_references = mechanism["symbols"] if isinstance(mechanism["symbols"], list) else []
        for index, reference in enumerate(symbol_references):
            _validate_literal_reference(
                root,
                reference,
                errors,
                f"{mechanism_id}.symbols[{index}]",
                allow_absent=canonical["status"] == "absent",
            )
        for role in ("tests", "evidence"):
            role_references = mechanism[role] if isinstance(mechanism[role], list) else []
            for index, reference in enumerate(role_references):
                _validate_literal_reference(
                    root, reference, errors, f"{mechanism_id}.{role}[{index}]"
                )

        for claim_id, commit, artifact_path, digest in evidence_claims:
            claim = claims_by_id.get(claim_id)
            if claim is None:
                errors.append(f"{mechanism_id}: evidence claim missing: {claim_id}")
                continue
            if claim.get("evidence_state") not in VERIFIED_STATES:
                errors.append(f"{mechanism_id}: evidence claim is not verified: {claim_id}")
            if claim.get("code_commit") != commit:
                errors.append(f"{mechanism_id}: evidence claim commit drifted: {claim_id}")
            artifacts = claim.get("artifacts", [])
            if not any(
                isinstance(artifact, dict)
                and artifact.get("path") == artifact_path
                and artifact.get("sha256") == digest
                for artifact in artifacts
            ):
                errors.append(f"{mechanism_id}: evidence claim artifact binding drifted: {claim_id}")

        patterns = mechanism["paper_claim_patterns"]
        if not isinstance(patterns, list) or tuple(patterns) != canonical["paper_claim_patterns"]:
            errors.append(f"{mechanism_id}: canonical paper claim patterns drifted")

    if seen != set(CANONICAL_MECHANISMS):
        errors.append("mechanism contract audited ID set drifted")

    paper_sources = contract.get("paper_sources")
    canonical_paper_sources = [
        {
            "path": CANONICAL_PAPER_PATH,
            "protected_sections": [
                {"name": name, "start": start, "end": end}
                for name, start, end in CANONICAL_PROTECTED_SECTIONS
            ],
        }
    ]
    if paper_sources != canonical_paper_sources:
        errors.append("mechanism contract paper path or protected-section markers drifted")

    paper_path = repository_path(root, CANONICAL_PAPER_PATH, errors, "canonical paper")
    if not paper_path.is_file():
        errors.append(f"canonical paper source missing: {CANONICAL_PAPER_PATH}")
        return []
    text = paper_path.read_text(encoding="utf-8", errors="replace")
    protected_sections: list[str] = []
    for index, (name, start, end) in enumerate(CANONICAL_PROTECTED_SECTIONS):
        section = {"name": name, "start": start, "end": end}
        protected_sections.append(
            _protected_section(text, section, errors, f"canonical protected section[{index}]")
        )
    validate_mechanism_claim_guards(protected_sections, errors, require_boundaries=True)
    return protected_sections


def validate_mechanism_claim_guards(
    protected_sections: list[str],
    errors: list[str],
    require_boundaries: bool = False,
) -> None:
    protected_text = _mask_allowed_negative_clauses(
        _paper_claim_text(protected_sections),
        ("mechanism_boundary",),
        errors,
        require_boundaries,
    )
    for sentence in _paper_sentences(protected_text):
        if re.search(r"\bdirector(?:y|ies)\b", sentence, re.IGNORECASE) and (
            DIRECTORY_MECHANISM_RE.search(sentence)
        ):
            errors.append(
                "canonical paper: incremental_extendible_hash_split is absent but "
                f"directory mechanism sentence is present: {sentence[:180]!r}"
            )
    for mechanism_id, canonical in CANONICAL_MECHANISMS.items():
        if canonical["status"] not in {"absent", "provisional"}:
            continue
        for pattern in canonical["paper_claim_patterns"]:
            if re.search(pattern, protected_text, flags=re.IGNORECASE | re.DOTALL):
                errors.append(
                    f"canonical paper: {mechanism_id} is {canonical['status']} but is claimed "
                    f"by pattern {pattern!r}"
                )


def _paper_claim_text(protected_sections: list[str]) -> str:
    """Return manuscript prose without non-semantic LaTeX identifiers."""
    text = "\n".join(protected_sections)
    text = re.sub(
        r"\\begin\{tikzpicture\}.*?\\end\{tikzpicture\}",
        " ",
        text,
        flags=re.DOTALL,
    )
    text = re.sub(r"\\(?:cite|label|ref|includegraphics)(?:\[[^]]*\])?\{[^}]*\}", " ", text)
    text = re.sub(r"(?m)(?<!\\)%.*$", " ", text)
    return re.sub(r"[ \t]+([.,;:!?])", r"\1", text)


def _mask_allowed_negative_clauses(
    text: str,
    clause_ids: tuple[str, ...],
    errors: list[str],
    require_all: bool,
) -> str:
    """Mask only exact audited negative sentences; generic negation is never trusted."""
    normalized = text
    for clause_id in clause_ids:
        clause = CANONICAL_ALLOWED_NEGATIVE_CLAUSES[clause_id]
        exact = r"\s+".join(re.escape(part) for part in clause.split())
        pattern = re.compile(rf"(?<![A-Za-z0-9]){exact}(?=\s|$)")
        count = len(pattern.findall(normalized))
        if require_all and count != 1:
            errors.append(
                f"canonical paper: audited negative clause {clause_id!r} must occur exactly once"
            )
        normalized = pattern.sub(" ", normalized)
    return normalized


def _paper_sentences(text: str) -> list[str]:
    return [
        sentence.strip()
        for sentence in re.split(r"(?<=[.!?])(?:[ \t]+|\n+)|\n+|\\\\", text)
        if sentence.strip()
    ]


def _validate_java_flink_sentences(
    text: str, errors: list[str], require_allowlist: bool
) -> None:
    """Reject every restricted-evidence sentence except exact audited text."""
    for allowed in CANONICAL_RESTRICTED_SENTENCE_ALLOWLIST:
        exact = r"\s+".join(re.escape(part) for part in allowed.split())
        pattern = re.compile(rf"(?<![A-Za-z0-9]){exact}(?=\s|$)")
        count = len(pattern.findall(text))
        if require_allowlist and count != 1:
            errors.append(
                "canonical paper: restricted sentence allowlist entry must occur exactly once: "
                f"{allowed!r}"
            )
        text = pattern.sub(" ", text)
    for sentence in _paper_sentences(text):
        sentence = sentence.strip()
        if JAVA_FLINK_CONTEXT_RE.search(sentence):
            excerpt = sentence[:180]
            errors.append(
                "canonical paper: non-verified claim correctness-java-54-and-recovery "
                f"matched fail-closed sentence {excerpt!r}"
            )


def validate_provisional_claim_guards(
    claims_by_id: dict[str, dict],
    protected_sections: list[str],
    errors: list[str],
    require_boundaries: bool = False,
) -> None:
    nonverified_ids = {
        claim_id
        for claim_id, claim in claims_by_id.items()
        if claim.get("evidence_state") not in VERIFIED_STATES
    }
    guard_ids = set(CANONICAL_PROVISIONAL_CLAIM_GUARDS)
    if nonverified_ids != guard_ids:
        errors.append(
            "non-verified evidence claims are not exactly covered by canonical paper guards: "
            f"claims={sorted(nonverified_ids)!r}, guards={sorted(guard_ids)!r}"
        )
    protected_text = _mask_allowed_negative_clauses(
        _paper_claim_text(protected_sections),
        tuple(CANONICAL_PROVISIONAL_CLAIM_GUARDS),
        errors,
        require_boundaries,
    )
    _validate_java_flink_sentences(protected_text, errors, require_boundaries)
    for claim_id, guard in CANONICAL_PROVISIONAL_CLAIM_GUARDS.items():
        claim = claims_by_id.get(claim_id)
        if claim is None:
            errors.append(f"canonical provisional claim missing: {claim_id}")
            continue
        if claim.get("evidence_state") != guard["status"]:
            errors.append(
                f"{claim_id}: canonical provisional state must be {guard['status']!r}"
            )
        for pattern in guard["patterns"]:
            if re.search(pattern, protected_text, flags=re.IGNORECASE | re.DOTALL):
                errors.append(
                    f"canonical paper: non-verified claim {claim_id} matched {pattern!r}"
                )


def validate(index_path: Path) -> tuple[list[str], list[str], dict]:
    index_path = index_path.resolve()
    root = index_path.parent.parent.resolve()
    index = load_json(index_path)
    errors: list[str] = []
    warnings: list[str] = []

    if not isinstance(index, dict):
        return ["index root must be a JSON object"], warnings, {}
    if index.get("schema_version") != 1:
        errors.append("schema_version must be 1")
    validate_repository_binding(root, index, errors)

    policy = index.get("classification_policy", {})
    environment_classes = set(policy.get("environment_classes", {}))
    evidence_states = set(policy.get("evidence_states", {}))
    claims = index.get("claims")
    if not isinstance(claims, list):
        return errors + ["claims must be an array"], warnings, index
    claims_by_id = {
        claim["id"]: claim
        for claim in claims
        if isinstance(claim, dict) and isinstance(claim.get("id"), str)
    }

    seen_ids: set[str] = set()
    for position, claim in enumerate(claims):
        context = f"claim[{position}]"
        if not isinstance(claim, dict):
            errors.append(f"{context}: must be an object")
            continue
        missing = {
            "id",
            "scope",
            "claim",
            "paper_locations",
            "figures",
            "artifacts",
            "code_commit",
            "environment_class",
            "environment_evidence",
            "evidence_state",
            "paper_use",
        } - set(claim)
        if missing:
            errors.append(f"{context}: missing fields: {', '.join(sorted(missing))}")
            continue

        claim_id = claim["id"]
        if claim_id in seen_ids:
            errors.append(f"{claim_id}: duplicate id")
        seen_ids.add(claim_id)
        if claim["environment_class"] not in environment_classes:
            errors.append(f"{claim_id}: unknown environment_class {claim['environment_class']!r}")
        if claim["evidence_state"] not in evidence_states:
            errors.append(f"{claim_id}: unknown evidence_state {claim['evidence_state']!r}")
        if not COMMIT_RE.fullmatch(claim["code_commit"]):
            errors.append(f"{claim_id}: code_commit is not a full Git commit")
        elif _resolve_git_object(
            root, claim["code_commit"], "commit", errors, f"{claim_id}.code_commit"
        ) is not None and claim["evidence_state"] == "fresh_verified":
            indexed_head = index.get("repository", {}).get("head_commit")
            if claim["code_commit"] != indexed_head:
                errors.append(f"{claim_id}: fresh_verified code_commit differs from indexed head")

        for role in ("paper_locations", "figures"):
            if not isinstance(claim[role], list):
                errors.append(f"{claim_id}: {role} must be an array")
                continue
            for relative in claim[role]:
                path = repository_path(root, relative, errors, claim_id)
                if not path.is_file():
                    errors.append(f"{claim_id}: missing {role[:-1]}: {relative}")

        artifacts = claim["artifacts"]
        if not isinstance(artifacts, list):
            errors.append(f"{claim_id}: artifacts must be an array")
            artifacts = []

        raw_count = 0
        for artifact in artifacts:
            if not isinstance(artifact, dict):
                errors.append(f"{claim_id}: artifact must be an object")
                continue
            missing_artifact = {"role", "path", "sha256", "origin_commit"} - set(artifact)
            if missing_artifact:
                errors.append(
                    f"{claim_id}: artifact missing fields: {', '.join(sorted(missing_artifact))}"
                )
                continue
            if artifact["role"] == "raw":
                raw_count += 1
            path = repository_path(root, artifact["path"], errors, claim_id)
            if not path.is_file():
                errors.append(f"{claim_id}: missing artifact: {artifact['path']}")
                continue
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            if digest != artifact["sha256"]:
                errors.append(
                    f"{claim_id}: SHA-256 mismatch for {artifact['path']}: "
                    f"expected {artifact['sha256']}, got {digest}"
                )
            origin = artifact["origin_commit"]
            if origin is not None and not COMMIT_RE.fullmatch(origin):
                errors.append(f"{claim_id}: invalid artifact origin_commit for {artifact['path']}")
            elif origin is not None and _resolve_git_object(
                root, origin, "commit", errors, f"{claim_id}.artifact origin_commit"
            ) is not None:
                committed = _git(root, "show", f"{origin}:{artifact['path']}")
                if committed.returncode != 0:
                    errors.append(
                        f"{claim_id}: artifact is absent from origin_commit: {artifact['path']}"
                    )
                elif committed.stdout != path.read_bytes():
                    errors.append(
                        f"{claim_id}: artifact bytes differ from origin_commit: {artifact['path']}"
                    )

        if claim["evidence_state"] in VERIFIED_STATES and raw_count == 0:
            errors.append(f"{claim_id}: verified claim requires at least one raw artifact")

        environment = claim["environment_evidence"]
        if not isinstance(environment, dict):
            errors.append(f"{claim_id}: environment_evidence must be an object")
            environment = {}
        if claim["environment_class"] == "real_l0":
            if environment.get("device_present") is not True:
                errors.append(f"{claim_id}: real_l0 requires device_present=true")
            if environment.get("runtime_library_present") is not True:
                errors.append(f"{claim_id}: real_l0 requires runtime_library_present=true")
        if claim["scope"] == "real_l0_hardware" and claim["evidence_state"] != "gap":
            if claim["environment_class"] != "real_l0":
                errors.append(f"{claim_id}: non-gap real-L0 claim requires environment_class=real_l0")

        validate_ratio_grid(root, claim, errors)

    mechanism_contract_path = root / "research_paper" / "mechanism_contract.json"
    protected_sections = validate_mechanism_contract(
        root, mechanism_contract_path, claims_by_id, errors
    )
    validate_provisional_claim_guards(
        claims_by_id, protected_sections, errors, require_boundaries=True
    )

    real_l0_support = [
        claim
        for claim in claims
        if isinstance(claim, dict)
        and claim.get("environment_class") == "real_l0"
        and claim.get("evidence_state") != "gap"
    ]
    if not real_l0_support:
        warnings.append("No claim currently has qualifying real-L0 hardware evidence.")
    gaps = [
        claim.get("id", "<unknown>")
        for claim in claims
        if isinstance(claim, dict)
        and claim.get("evidence_state") in {"documentary_only", "derived_missing_raw", "blocked", "gap"}
    ]
    if gaps:
        warnings.append("Evidence gaps/provisional claims: " + ", ".join(gaps))
    return errors, warnings, index


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "index",
        nargs="?",
        type=Path,
        default=Path(__file__).with_name("evidence_index.json"),
    )
    args = parser.parse_args()
    errors, warnings, index = validate(args.index)
    for warning in warnings:
        print(f"WARNING: {warning}")
    for error in errors:
        print(f"ERROR: {error}", file=sys.stderr)
    if errors:
        print(f"FAIL: {len(errors)} validation error(s)", file=sys.stderr)
        return 1
    print(f"PASS: {len(index.get('claims', []))} claims validated")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
