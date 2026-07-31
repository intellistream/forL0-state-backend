#!/usr/bin/env python3
"""Validate the paper evidence index using only the Python standard library."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import statistics
import sys
from pathlib import Path


COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
VERIFIED_STATES = {"fresh_verified", "raw_verified"}


def load_json(path: Path) -> object:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def repository_path(root: Path, value: str, errors: list[str], context: str) -> Path:
    path = (root / value).resolve()
    try:
        path.relative_to(root)
    except ValueError:
        errors.append(f"{context}: path escapes repository: {value}")
    return path


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

    required = {"raw_path", "count", "all_greater_than_one", "mean", "max"}
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
    ratios: list[float] = []
    for index, record in enumerate(records):
        try:
            results = record["results"]
            ratios.append(results["forl0"]["throughput"] / results["hashmap"]["throughput"])
        except (KeyError, TypeError, ZeroDivisionError) as exc:
            errors.append(f"{claim['id']}: invalid record {index}: {exc}")
            return

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

    policy = index.get("classification_policy", {})
    environment_classes = set(policy.get("environment_classes", {}))
    evidence_states = set(policy.get("evidence_states", {}))
    claims = index.get("claims")
    if not isinstance(claims, list):
        return errors + ["claims must be an array"], warnings, index

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
