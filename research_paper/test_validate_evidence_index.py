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

from validate_evidence_index import validate  # noqa: E402


class EvidenceIndexValidatorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.canonical = json.loads((HERE / "evidence_index.json").read_text())

    def validate_payload(self, payload: object) -> tuple[list[str], list[str], dict]:
        # Keep the temporary index under research_paper so repository-relative
        # artifact paths resolve exactly as they do for the canonical index.
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", dir=HERE, encoding="utf-8"
        ) as handle:
            json.dump(payload, handle)
            handle.flush()
            return validate(Path(handle.name))

    def test_canonical_index_passes(self) -> None:
        errors, warnings, _ = validate(HERE / "evidence_index.json")
        self.assertEqual([], errors)
        self.assertTrue(any("real-L0" in warning for warning in warnings))

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


if __name__ == "__main__":
    unittest.main()
