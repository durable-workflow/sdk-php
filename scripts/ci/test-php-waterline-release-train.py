#!/usr/bin/env python3
"""Regression coverage for coordinated PHP SDK and Waterline releases."""

from __future__ import annotations

import copy
import importlib.util
import sys
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("php_waterline_release_train.py")
SPEC = importlib.util.spec_from_file_location("php_waterline_release_train", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"cannot load {SCRIPT}")
train = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = train
SPEC.loader.exec_module(train)


def plan() -> dict:
    return {
        "components": {
            "workflow": {"version": "2.0.0-rc.12", "commit": "a" * 40},
            "waterline": {"version": "2.0.0-rc.9", "commit": "b" * 40},
            "sdk-php": {"version": "2.0.0-rc.6", "commit": "c" * 40},
        },
    }


def manifest(candidate: dict) -> tuple[dict, bytes]:
    versions = train.versions(candidate)
    value = {
        "name": "durable-workflow/waterline",
        "require": {train.SDK_PACKAGE: versions["sdk-php"]},
        "require-dev": {train.WORKFLOW_PACKAGE: versions["workflow"]},
        "extra": {"durable-workflow": {"product-train": versions["waterline"]}},
    }
    return value, b"exact planned Waterline composer source"


class PhpWaterlineReleaseTrainTest(unittest.TestCase):
    def test_sdk_only_advance_stays_incomplete(self) -> None:
        baseline = plan()
        candidate = copy.deepcopy(baseline)
        candidate["components"]["sdk-php"]["version"] = "2.0.0-rc.7"
        composer, raw = manifest(candidate)

        with self.assertRaisesRegex(train.TrainError, "sequential Waterline"):
            train.validate(candidate, baseline, composer, raw)

    def test_paired_sequential_successor_is_publishable(self) -> None:
        baseline = plan()
        candidate = copy.deepcopy(baseline)
        candidate["components"]["sdk-php"]["version"] = "2.0.0-rc.7"
        candidate["components"]["waterline"]["version"] = "2.0.0-rc.10"
        composer, raw = manifest(candidate)

        evidence = train.validate(candidate, baseline, composer, raw)

        self.assertEqual("verified", evidence["outcome"])
        self.assertEqual("paired-sdk-waterline-successor", evidence["transition"])

    def test_cross_prerelease_compatibility_constraint_is_rejected(self) -> None:
        candidate = plan()
        composer, raw = manifest(candidate)
        composer["require"][train.SDK_PACKAGE] = "2.0.0-rc.7"

        with self.assertRaisesRegex(train.TrainError, "Composer-satisfiable"):
            train.validate(candidate, candidate, composer, raw)

    def test_historical_exact_pair_remains_recoverable(self) -> None:
        baseline = plan()
        historical = copy.deepcopy(baseline)
        historical["components"]["sdk-php"]["version"] = "2.0.0-rc.5"
        historical["components"]["waterline"]["version"] = "2.0.0-rc.8"
        composer, raw = manifest(historical)

        evidence = train.validate(historical, baseline, composer, raw)

        self.assertEqual("historical-compatible-pair", evidence["transition"])


if __name__ == "__main__":
    unittest.main()
