#!/usr/bin/env python3
"""Fail-closed tests for the ICDE evidence-index validator."""

from __future__ import annotations

import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from validate_evidence_index import (  # noqa: E402
    CANONICAL_PROTECTED_SECTIONS,
    load_json,
    validate,
    validate_mechanism_claim_guards,
    validate_mechanism_contract,
    validate_provisional_claim_guards,
    validate_ratio_grid,
)


class EvidenceIndexValidatorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.canonical = json.loads((HERE / "evidence_index.json").read_text())
        cls.canonical_contract = json.loads(
            (HERE / "mechanism_contract.json").read_text()
        )

    def validate_payload(self, payload: object) -> tuple[list[str], list[str], dict]:
        # Keep the temporary index under research_paper so repository-relative
        # artifact paths resolve exactly as they do for the canonical index.
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", dir=HERE, encoding="utf-8"
        ) as handle:
            json.dump(payload, handle)
            handle.flush()
            return validate(Path(handle.name))

    def validate_contract(self, payload: object) -> tuple[list[str], list[str], dict]:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", dir=HERE, encoding="utf-8"
        ) as handle:
            json.dump(payload, handle)
            handle.flush()
            errors: list[str] = []
            claims_by_id = {
                claim["id"]: claim for claim in self.canonical["claims"]
            }
            validate_mechanism_contract(
                HERE.parent, Path(handle.name), claims_by_id, errors
            )
            return errors, [], self.canonical

    @staticmethod
    def mechanism(payload: dict, mechanism_id: str) -> dict:
        return next(
            mechanism
            for mechanism in payload["mechanisms"]
            if mechanism["id"] == mechanism_id
        )

    def test_canonical_index_and_contract_pass(self) -> None:
        errors, warnings, _ = validate(HERE / "evidence_index.json")
        self.assertEqual([], errors)
        self.assertTrue(any("real-L0" in warning for warning in warnings))

    def test_all_evidence_json_rejects_duplicate_keys_and_nonfinite_values(self) -> None:
        mutations = (
            ('{"claim":{"scope":"backend","scope":"hardware"}}', "duplicate JSON key"),
            ('{"throughput":NaN}', "non-finite JSON constant"),
            ('{"throughput":Infinity}', "non-finite JSON constant"),
        )
        for raw, message in mutations:
            with self.subTest(raw=raw):
                with tempfile.NamedTemporaryFile(
                    mode="w", suffix=".json", dir=HERE, encoding="utf-8"
                ) as handle:
                    handle.write(raw)
                    handle.flush()
                    with self.assertRaisesRegex(ValueError, message):
                        load_json(Path(handle.name))

    def test_canonical_p1_result_and_causality_phrases_are_removed(self) -> None:
        paper = (
            HERE
            / "TKDE-template"
            / "Conference-LaTeX-template_10-17-19"
            / "briskstate_tkde.tex"
        ).read_text(encoding="utf-8")
        forbidden = (
            "observations drawn from the repository's benchmark and delivery artifacts",
            "state-intensive workloads degrade sharply",
            "operations become noticeably cheaper",
            "affects the cost of subsequent writes",
            "improves probe locality and reduces interference",
            "reducing the number of cache lines touched",
            "Intel and Kunpeng are used to expose behavior",
        )
        for phrase in forbidden:
            with self.subTest(phrase=phrase):
                self.assertNotIn(phrase, paper)
        self.assertIn("requires fresh matched evidence", paper)
        self.assertIn("remains an unverified hypothesis", paper)

    def test_non_array_artifacts_is_rejected_without_crashing(self) -> None:
        payload = copy.deepcopy(self.canonical)
        payload["claims"][0]["artifacts"] = {"role": "raw"}
        errors, _, _ = self.validate_payload(payload)
        self.assertIn(
            f"{payload['claims'][0]['id']}: artifacts must be an array", errors
        )

    def test_non_object_environment_is_rejected_without_crashing(self) -> None:
        payload = copy.deepcopy(self.canonical)
        payload["claims"][0]["environment_evidence"] = None
        errors, _, _ = self.validate_payload(payload)
        self.assertIn(
            f"{payload['claims'][0]['id']}: environment_evidence must be an object",
            errors,
        )

    def test_real_l0_requires_device_and_runtime_library(self) -> None:
        payload = copy.deepcopy(self.canonical)
        claim = payload["claims"][-1]
        claim["environment_class"] = "real_l0"
        claim["evidence_state"] = "raw_verified"
        claim["environment_evidence"] = {
            "device_present": False,
            "runtime_library_present": False,
        }
        errors, _, _ = self.validate_payload(payload)
        self.assertTrue(any("device_present=true" in error for error in errors))
        self.assertTrue(any("runtime_library_present=true" in error for error in errors))

    def test_incomplete_quantitative_check_is_rejected_without_crashing(self) -> None:
        payload = copy.deepcopy(self.canonical)
        claim = payload["claims"][0]
        claim["quantitative_check"] = {"type": "wordcount_ratio_grid"}
        errors, _, _ = self.validate_payload(payload)
        self.assertTrue(any("quantitative check missing fields" in error for error in errors))

    def test_repository_and_artifact_commits_must_be_real_bound_git_objects(self) -> None:
        payload = copy.deepcopy(self.canonical)
        payload["repository"]["head_commit"] = "f" * 40
        errors, _, _ = self.validate_payload(payload)
        self.assertTrue(any("Git commit object does not exist" in error for error in errors))

        payload = copy.deepcopy(self.canonical)
        payload["repository"]["source_tree"] = "f" * 40
        errors, _, _ = self.validate_payload(payload)
        self.assertTrue(any("source_tree differs" in error for error in errors))

        payload = copy.deepcopy(self.canonical)
        payload["claims"][0]["artifacts"][0]["origin_commit"] = payload["repository"][
            "head_commit"
        ]
        errors, _, _ = self.validate_payload(payload)
        self.assertTrue(any("artifact is absent from origin_commit" in error for error in errors))

    def validate_mutated_grid(self, mutate: object) -> list[str]:
        claim = copy.deepcopy(
            next(
                item
                for item in self.canonical["claims"]
                if item["id"] == "intel-wordcount-grid-36"
            )
        )
        raw = load_json(HERE.parent / claim["quantitative_check"]["raw_path"])
        assert isinstance(raw, dict)
        mutate(raw)
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", dir=HERE, encoding="utf-8"
        ) as handle:
            json.dump(raw, handle)
            handle.flush()
            claim["quantitative_check"]["raw_path"] = (
                f"research_paper/{Path(handle.name).name}"
            )
            errors: list[str] = []
            validate_ratio_grid(HERE.parent, claim, errors)
            return errors

    def test_wordcount_grid_rejects_duplicate_or_missing_coordinates(self) -> None:
        def duplicate(raw: dict) -> None:
            raw["records"][-1]["num_keys"] = raw["records"][0]["num_keys"]
            raw["records"][-1]["skew_factor"] = raw["records"][0]["skew_factor"]

        errors = self.validate_mutated_grid(duplicate)
        self.assertTrue(any("duplicate grid coordinate" in error for error in errors))

    def test_wordcount_grid_rejects_workload_mismatch_and_bad_arithmetic(self) -> None:
        def mismatch(raw: dict) -> None:
            raw["records"][0]["results"]["forl0"]["parallelism"] = 7

        errors = self.validate_mutated_grid(mismatch)
        self.assertTrue(any("workload contract differs" in error for error in errors))

        def arithmetic(raw: dict) -> None:
            raw["records"][0]["results"]["forl0"]["throughput"] *= 2

        errors = self.validate_mutated_grid(arithmetic)
        self.assertTrue(any("throughput is not records/time" in error for error in errors))

    def test_nonexistent_forl0_state_map_cannot_be_implemented(self) -> None:
        payload = copy.deepcopy(self.canonical_contract)
        mechanism = self.mechanism(payload, "forl0_state_map")
        mechanism["status"] = "implemented"
        mechanism["symbols"][0]["expected"] = "present"
        errors, _, _ = self.validate_contract(payload)
        self.assertTrue(
            any(
                "ForL0StateMap.java" in error and "source file missing" in error
                for error in errors
            )
        )

    def test_allocation_split_cannot_impersonate_hash_split(self) -> None:
        payload = copy.deepcopy(self.canonical_contract)
        mechanism = self.mechanism(payload, "split_allocation_layout")
        mechanism["architecture_role"] = "incremental_hash_split"
        errors, _, _ = self.validate_contract(payload)
        self.assertTrue(
            any(
                "split_allocation_layout" in error
                and "architecture_role must be 'test_only_allocator_extension_point'"
                in error
                for error in errors
            )
        )

    def test_hot_cache_cannot_impersonate_swiss_table_allocator(self) -> None:
        payload = copy.deepcopy(self.canonical_contract)
        mechanism = self.mechanism(payload, "l0_hot_cache")
        mechanism["architecture_role"] = "swiss_table_allocator"
        errors, _, _ = self.validate_contract(payload)
        self.assertTrue(
            any(
                "l0_hot_cache" in error
                and "architecture_role must be 'cache_above_state_table'" in error
                for error in errors
            )
        )

    def test_contract_cannot_redirect_main_paper(self) -> None:
        payload = copy.deepcopy(self.canonical_contract)
        payload["paper_sources"][0]["path"] = "README.md"
        errors, _, _ = self.validate_contract(payload)
        self.assertTrue(any("paper path" in error for error in errors))

    def test_contract_cannot_drift_protected_markers(self) -> None:
        payload = copy.deepcopy(self.canonical_contract)
        payload["paper_sources"][0]["protected_sections"][0]["start"] = "Abstract"
        errors, _, _ = self.validate_contract(payload)
        self.assertTrue(any("protected-section markers drifted" in error for error in errors))

    def test_contract_cannot_define_its_own_negative_wording(self) -> None:
        payload = copy.deepcopy(self.canonical_contract)
        payload["allowed_negative_clauses"] = ["Anything with not is allowed."]
        mechanism = self.mechanism(payload, "split_allocation_layout")
        mechanism["allowed_wording"] = ["split allocation"]
        errors, _, _ = self.validate_contract(payload)
        self.assertTrue(any("top-level fields drifted" in error for error in errors))
        self.assertTrue(
            any("fields drifted from the canonical mechanism schema" in error for error in errors)
        )

    def test_canonical_guard_scope_is_the_full_manuscript(self) -> None:
        self.assertEqual(
            (("full_manuscript", r"\documentclass", r"\end{document}"),),
            CANONICAL_PROTECTED_SECTIONS,
        )

    def test_contract_cannot_select_weaker_symbol_or_evidence(self) -> None:
        payload = copy.deepcopy(self.canonical_contract)
        mechanism = self.mechanism(payload, "native_swiss_table_simd")
        mechanism["symbols"][0]["literal"] = "SwissTable"
        mechanism["evidence_claims"][0]["artifact_sha256"] = "0" * 64
        errors, _, _ = self.validate_contract(payload)
        self.assertTrue(any("canonical symbols binding drifted" in error for error in errors))
        self.assertTrue(any("canonical evidence_claims binding drifted" in error for error in errors))

    def test_non_array_contract_references_fail_without_crashing(self) -> None:
        payload = copy.deepcopy(self.canonical_contract)
        mechanism = self.mechanism(payload, "native_swiss_table_simd")
        mechanism["symbols"] = {"path": "src/main/native/platform/simd.h"}
        mechanism["tests"] = None
        errors, _, _ = self.validate_contract(payload)
        self.assertTrue(any("symbols: must be an array" in error for error in errors))
        self.assertTrue(any("tests: must be an array" in error for error in errors))

    def test_absent_and_provisional_mechanism_synonyms_are_rejected(self) -> None:
        errors: list[str] = []
        validate_mechanism_claim_guards(
            ["We contribute Extendible Hashing and an allocation split."], errors
        )
        self.assertTrue(any("incremental_extendible_hash_split" in error for error in errors))
        self.assertTrue(any("split_allocation_layout" in error for error in errors))

    def test_incremental_directory_bucket_split_synonym_is_rejected(self) -> None:
        errors: list[str] = []
        validate_mechanism_claim_guards(
            ["We split growing hash-table buckets incrementally through a directory."],
            errors,
        )
        self.assertTrue(any("incremental_extendible_hash_split" in error for error in errors))

    def test_incremental_directory_bucket_double_negative_is_rejected(self) -> None:
        errors: list[str] = []
        validate_mechanism_claim_guards(
            ["The design does not lack incremental directory-based bucket division."],
            errors,
        )
        self.assertTrue(any("incremental_extendible_hash_split" in error for error in errors))

    def test_all_directory_growth_sentences_fail_closed(self) -> None:
        sentences = (
            "The design has no directory-based bucket expansion.",
            "We neither partition nor redistribute buckets through a directory.",
            "Directory growth does not split any bucket.",
        )
        for sentence in sentences:
            with self.subTest(sentence=sentence):
                errors: list[str] = []
                validate_mechanism_claim_guards([sentence], errors)
                self.assertTrue(
                    any("incremental_extendible_hash_split" in error for error in errors)
                )

    def test_exact_mechanism_boundary_is_allowed(self) -> None:
        errors: list[str] = []
        validate_mechanism_claim_guards(
            [
                "Incremental extendible-hash split is absent; split allocation is test-only, "
                "and production DefaultAllocator uses unified allocation."
            ],
            errors,
        )
        self.assertEqual([], errors)

    def test_provisional_claim_synonyms_are_rejected(self) -> None:
        claims_by_id = {claim["id"]: claim for claim in self.canonical["claims"]}
        errors: list[str] = []
        validate_provisional_claim_guards(
            claims_by_id,
            [
                "NexMark becomes faster and avoids stalled jobs. "
                "VTune demonstrates reduced memory stalls. "
                "Real L0 hardware gains are substantial."
            ],
            errors,
        )
        self.assertTrue(any("nexmark-runtime-comparison" in error for error in errors))
        self.assertTrue(any("vtune-memory-bound" in error for error in errors))
        self.assertTrue(any("real-l0-hardware-speedup" in error for error in errors))

    def test_provisional_claims_in_manuscript_body_are_rejected(self) -> None:
        claims_by_id = {claim["id"]: claim for claim in self.canonical["claims"]}
        errors: list[str] = []
        validate_provisional_claim_guards(
            claims_by_id,
            [
                r"\documentclass{article}\begin{document}"
                r"\section{Discussion} NexMark avoids stalled executions and "
                r"VTune confirms improved memory behavior.\end{document}"
            ],
            errors,
        )
        self.assertTrue(any("nexmark-runtime-comparison" in error for error in errors))
        self.assertTrue(any("vtune-memory-bound" in error for error in errors))

    def test_java_semantics_positive_and_double_negative_are_rejected(self) -> None:
        claims_by_id = {claim["id"]: claim for claim in self.canonical["claims"]}
        for sentence in (
            "The implementation preserves Flink State API semantics and "
            "checkpoint/savepoint recovery formats.",
            "The implementation does not fail to preserve Flink State API semantics "
            "and checkpoint recovery.",
        ):
            with self.subTest(sentence=sentence):
                errors: list[str] = []
                validate_provisional_claim_guards(claims_by_id, [sentence], errors)
                self.assertTrue(
                    any("correctness-java-54-and-recovery" in error for error in errors)
                )

    def test_java_integration_and_restart_synonyms_are_rejected(self) -> None:
        claims_by_id = {claim["id"]: claim for claim in self.canonical["claims"]}
        sentences = (
            "The backend maintains API behavior across job restart and state restoration.",
            "We provide a Flink-compatible keyed state backend.",
            "The design is integrated into Flink's keyed-state, namespace, and checkpoint model.",
            "The backend never loses API behavior after restart or restored snapshots.",
        )
        for sentence in sentences:
            with self.subTest(sentence=sentence):
                errors: list[str] = []
                validate_provisional_claim_guards(claims_by_id, [sentence], errors)
                self.assertTrue(
                    any("correctness-java-54-and-recovery" in error for error in errors)
                )

    def test_sentence_level_flink_checkpoint_assertions_are_rejected(self) -> None:
        claims_by_id = {claim["id"]: claim for claim in self.canonical["claims"]}
        sentences = (
            "The backend is integrated with Flink keyed state APIs.",
            "The keyed backend integrates with Flink keyed-state APIs.",
            "The backend reduces checkpoint interference without abandoning the checkpoint model.",
            "Checkpoint effects were observed in profiling and experiments.",
            "The design changes checkpoint behavior and runtime stability so configurations complete.",
        )
        for sentence in sentences:
            with self.subTest(sentence=sentence):
                errors: list[str] = []
                validate_provisional_claim_guards(claims_by_id, [sentence], errors)
                self.assertTrue(
                    any("correctness-java-54-and-recovery" in error for error in errors)
                )

    def test_non_allowlisted_java_negative_also_fails_closed(self) -> None:
        claims_by_id = {claim["id"]: claim for claim in self.canonical["claims"]}
        errors: list[str] = []
        validate_provisional_claim_guards(
            claims_by_id,
            ["Fresh Flink evidence is missing."],
            errors,
        )
        self.assertTrue(any("correctness-java-54-and-recovery" in error for error in errors))

    def test_relaunch_failover_and_snapshot_negatives_fail_closed(self) -> None:
        claims_by_id = {claim["id"]: claim for claim in self.canonical["claims"]}
        sentences = (
            "No failover behavior is claimed.",
            "The backend does not complete state restoration after relaunch.",
            "Snapshot behavior was not observed.",
        )
        for sentence in sentences:
            with self.subTest(sentence=sentence):
                errors: list[str] = []
                validate_provisional_claim_guards(claims_by_id, [sentence], errors)
                self.assertTrue(
                    any("correctness-java-54-and-recovery" in error for error in errors)
                )

    def test_exact_restricted_background_sentence_is_allowed_but_drift_is_not(self) -> None:
        claims_by_id = {claim["id"]: claim for claim in self.canonical["claims"]}
        exact = r"\title{\briskstate: A Cache-Aware State Backend for Apache Flink}"
        errors: list[str] = []
        validate_provisional_claim_guards(claims_by_id, [exact], errors)
        self.assertEqual([], errors)

        drift_errors: list[str] = []
        validate_provisional_claim_guards(
            claims_by_id,
            [r"\title{\briskstate: A Fast State Backend for Apache Flink}"],
            drift_errors,
        )
        self.assertTrue(
            any("correctness-java-54-and-recovery" in error for error in drift_errors)
        )

    def test_exact_java_and_real_l0_boundaries_are_allowed(self) -> None:
        claims_by_id = {claim["id"]: claim for claim in self.canonical["claims"]}
        errors: list[str] = []
        validate_provisional_claim_guards(
            claims_by_id,
            [
                "Fresh JVM/Flink recovery evidence is missing; Flink State API semantics "
                "and checkpoint/savepoint recovery are not established by this revision. "
                "Fresh real-L0 hardware evidence is missing; real-L0 performance gains "
                "are not established by this revision."
            ],
            errors,
        )
        self.assertEqual([], errors)

    def test_real_l0_positive_gain_is_rejected(self) -> None:
        claims_by_id = {claim["id"]: claim for claim in self.canonical["claims"]}
        errors: list[str] = []
        validate_provisional_claim_guards(
            claims_by_id,
            ["Real-L0 hardware delivers a large throughput gain."],
            errors,
        )
        self.assertTrue(any("real-l0-hardware-speedup" in error for error in errors))


if __name__ == "__main__":
    unittest.main()
