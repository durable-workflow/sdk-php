#!/usr/bin/env python3
"""Adversarial checks for regression-corpus policy enforcement."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

VALIDATOR = Path(__file__).with_name("validate-regression-corpus.py")
REPOSITORY_POLICY = VALIDATOR.parents[2] / "regression-corpus-policy.json"
REPOSITORY_ROOT = VALIDATOR.parents[2]
PHP_REPLAY_RUNNER = VALIDATOR.with_name("run-replay-regression-fixture.php")


def run(*arguments: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(arguments),
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
    )


class RegressionCorpusPolicyTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.repository_policy = json.loads(REPOSITORY_POLICY.read_text())
        (self.root / "src/Codec").mkdir(parents=True)
        (self.root / "src/Worker").mkdir(parents=True)
        (self.root / "resources/protocol").mkdir(parents=True)
        (self.root / "tests/fixtures/codec-regressions").mkdir(parents=True)
        (self.root / "vendor").mkdir()
        (self.root / "src/Codec/Example.php").write_text("<?php\nreturn 'base';\n")
        (self.root / "src/Worker/ReplayResult.php").write_text(
            "<?php\nfinal class ReplayResult { public array $commands = []; }\n"
        )
        (self.root / "src/Worker.php").write_text(
            """<?php
final class Worker
{
    private function completeHistory(): array
    {
        $history = [];

        return $history;
    }

    private function executeWorkflowTask(): void
    {
        $commands = $this->replayer->replay();
    }

    private function historyFromTask(array $task): array
    {
        return $task['history'] ?? [];
    }

    private function heartbeat(): void
    {
        $this->beats = 1;
    }
}
"""
        )
        self.replay_runner = self.root / "replay-runner.py"
        self.replay_runner.write_text(
            """import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--vendor-root")
parser.add_argument("--source-root", type=Path, required=True)
parser.add_argument("--fixture", type=Path, required=True)
args = parser.parse_args()
fixture = json.loads(args.fixture.read_text())
source = (args.source_root / "src/Worker.php").read_text()
identity = fixture["id"]
if identity == "candidate-failure":
    raise SystemExit(1)
if identity == "replay-defect":
    raise SystemExit(0 if "$history = ['changed'];" in source else 1)
if identity == "history-from-task-defect":
    raise SystemExit(0 if "return array_values($task['history'] ?? []);" in source else 1)
raise SystemExit(0)
"""
        )
        self.write_json(
            "tests/fixtures/codec-regressions/base.json",
            self.codec_fixture("base-codec-case", "0", "AA=="),
        )
        self.write_json(
            "tests/fixtures/previously-unselected.json",
            self.codec_fixture("previously-unselected-codec-case", "1", "Ag=="),
        )
        self.write_json(
            "resources/protocol/avro-value-v1-golden.json",
            {
                "schema": "durable_workflow.protocol.Value",
                "fingerprint": "e2a33dff55802237",
                "cases": [
                    {
                        "name": "long_7",
                        "kind": "long",
                        "value": "7",
                        "wire_base64": "wwHioz3/VYAiNwQO",
                    }
                ],
                "alternate_map_orders": [
                    {
                        "name": "map_orders",
                        "wire_base64": ["Aw==", "BA=="],
                    }
                ],
                "malformed_frames": [
                    {
                        "name": "empty_blob",
                        "error": "invalid_payload_framing",
                        "wire_base64": "",
                    }
                ],
            },
        )
        self.write_policy("src/Codec/*.php")
        self.git("init", "--quiet")
        self.git("add", "--all")
        self.git(
            "-c",
            "user.name=Regression Corpus Test",
            "-c",
            "user.email=regression-corpus@example.invalid",
            "commit",
            "--quiet",
            "--message=baseline",
        )
        self.base_ref = self.git("rev-parse", "HEAD").stdout.strip()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def git(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        result = run("git", *arguments, cwd=self.root)
        if result.returncode != 0:
            self.fail(
                f"git command failed: {arguments!r}\n{result.stdout}\n{result.stderr}"
            )
        return result

    def write_json(self, relative_path: str, value: dict[str, Any]) -> None:
        (self.root / relative_path).write_text(json.dumps(value, indent=2) + "\n")

    def codec_fixture(self, identity: str, value: str, wire: str) -> dict[str, Any]:
        return {
            "$schema": "https://example.invalid/evidence-schema.json",
            "fixture_schema": "durable-workflow.codec-regression/v1",
            "id": identity,
            "protocol": {
                "codec": "avro",
                "schema": "example.Value",
                "version": "1",
                "fingerprint": None,
            },
            "bindings": ["php"],
            "value": {"type": "long", "value": value},
            "framing": {"encoding": "base64", "wire_base64": wire},
            "failure_policy": {"operation": "round_trip", "error": None},
        }

    def replay_fixture(
        self,
        identity: str,
        bindings: list[str],
        *,
        name: str = "Ada",
        result: str = "hello Ada",
    ) -> dict[str, Any]:
        return {
            "$schema": "https://example.invalid/evidence-schema.json",
            "fixture_schema": "durable-workflow.replay-regression/v1",
            "id": identity,
            "protocol_version": "1",
            "bindings": bindings,
            "workflow": {
                "type": "golden.single-activity",
                "input": [name],
            },
            "history": [
                {
                    "event_type": "ActivityCompleted",
                    "payload": {"result": result},
                }
            ],
            "expected": {
                "command_sequence": [
                    {
                        "type": "complete_workflow",
                        "result": result,
                    }
                ]
            },
        }

    def write_policy(
        self,
        guard_glob: str,
        fixture_globs: tuple[str, ...] = ("tests/fixtures/codec-regressions/*.json",),
        fixture_selectors: tuple[tuple[str, str], ...] | None = None,
    ) -> None:
        fixture_selectors = fixture_selectors or tuple(
            (glob, "codec-regression-v1") for glob in fixture_globs
        )
        self.write_json(
            "regression-corpus-policy.json",
            {
                "$schema": "https://example.invalid/policy-schema.json",
                "schema": "durable-workflow.regression-corpus-policy/v1",
                "repository": "sdk-php",
                "binding": "php",
                "categories": {
                    "codec": {
                        "fixtures": [
                            {"glob": glob, "format": fixture_format}
                            for glob, fixture_format in fixture_selectors
                        ],
                        "guards": [{"glob": guard_glob}],
                    },
                    "replay": self.repository_policy["categories"]["replay"],
                },
            },
        )

    def validate(self) -> subprocess.CompletedProcess[str]:
        return run(
            sys.executable,
            str(VALIDATOR),
            "--root",
            str(self.root),
            "--base-ref",
            self.base_ref,
            "--php-executable",
            sys.executable,
            "--replay-runner",
            str(self.replay_runner),
            "--vendor-root",
            str(self.root / "vendor"),
            cwd=self.root,
        )

    def run_official_php_runner(
        self,
        source_root: Path,
        fixture: dict[str, Any],
    ) -> subprocess.CompletedProcess[str]:
        fixture_path = self.root / "official-runner-fixture.json"
        self.write_json(
            fixture_path.relative_to(self.root).as_posix(),
            fixture,
        )

        return run(
            "php",
            str(PHP_REPLAY_RUNNER),
            "--vendor-root",
            str(REPOSITORY_ROOT / "vendor"),
            "--source-root",
            str(source_root),
            "--fixture",
            str(fixture_path),
            cwd=REPOSITORY_ROOT,
        )

    def official_history_fixture(self, identity: str) -> dict[str, Any]:
        fixture = self.replay_fixture(identity, ["php"])
        fixture["history"][0]["payload"] = {
            "sequence": 1,
            "result": {
                "codec": "avro",
                "blob": "wwHioz3/VYAiNwoSaGVsbG8gQWRh",
            },
        }

        return fixture

    def test_codec_change_cannot_hide_behind_weakened_guard(self) -> None:
        (self.root / "src/Codec/Example.php").write_text("<?php\nreturn 'changed';\n")
        self.write_policy("src/nonmatching/*.php")

        result = self.validate()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn(
            "categories.codec.guards cannot remove or change a base selector",
            result.stderr,
        )

    def test_php_policy_rejects_unconsumed_replay_format(self) -> None:
        policy = json.loads(
            (self.root / "regression-corpus-policy.json").read_text()
        )
        policy["categories"]["replay"]["fixtures"].append(
            {
                "glob": "tests/fixtures/golden-history.json",
                "format": "golden-history-v1",
            }
        )
        self.write_json("regression-corpus-policy.json", policy)

        result = self.validate()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn("format has no official PHP consumer", result.stderr)

    def test_php_policy_rejects_nonportable_replay_selector(self) -> None:
        policy = json.loads(
            (self.root / "regression-corpus-policy.json").read_text()
        )
        policy["categories"]["replay"]["fixtures"].append(
            {
                "glob": "tests/fixtures/replay-regressions/**/*.json",
                "format": "replay-regression-v1",
            }
        )
        self.write_json("regression-corpus-policy.json", policy)

        result = self.validate()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn(
            "glob is not portable to the official PHP consumer",
            result.stderr,
        )

    def test_codec_change_cannot_claim_growth_from_expanded_fixture_selector(
        self,
    ) -> None:
        (self.root / "src/Codec/Example.php").write_text("<?php\nreturn 'changed';\n")
        self.write_policy(
            "src/Codec/*.php",
            (
                "tests/fixtures/codec-regressions/*.json",
                "tests/fixtures/previously-unselected.json",
            ),
        )

        result = self.validate()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn(
            "codec implementation changed but its corpus did not grow "
            "(base=2, current=2)",
            result.stderr,
        )

    def test_codec_change_cannot_claim_growth_from_nested_invalid_wire(self) -> None:
        (self.root / "src/Codec/Example.php").write_text("<?php\nreturn 'changed';\n")
        (self.root / "tests/fixtures/codec-regressions/nested").mkdir()
        fixture = self.codec_fixture("nested-invalid-wire", "7", "AQ==")
        fixture["protocol"] = {
            "codec": "avro",
            "schema": "durable_workflow.protocol.Value",
            "version": "1",
            "fingerprint": "e2a33dff55802237",
        }
        fixture["framing"]["encoding"] = "avro-single-object"
        self.write_json(
            "tests/fixtures/codec-regressions/nested/invalid-wire.json",
            fixture,
        )

        result = self.validate()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn(
            "codec implementation changed but its corpus did not grow "
            "(base=1, current=1)",
            result.stderr,
        )

    def test_codec_change_cannot_claim_growth_from_hidden_fixture(self) -> None:
        (self.root / "src/Codec/Example.php").write_text("<?php\nreturn 'changed';\n")
        self.write_json(
            "tests/fixtures/codec-regressions/.hidden.json",
            self.codec_fixture("hidden-codec-case", "7", "AQ=="),
        )

        result = self.validate()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn(
            "codec implementation changed but its corpus did not grow "
            "(base=1, current=1)",
            result.stderr,
        )

    def test_codec_change_cannot_claim_growth_by_rewrapping_golden_bytes(
        self,
    ) -> None:
        (self.root / "src/Codec/Example.php").write_text("<?php\nreturn 'changed';\n")
        fixture = self.codec_fixture(
            "duplicate-avro-long-seven",
            "7",
            "wwHioz3/VYAiNwQO",
        )
        fixture["protocol"] = {
            "codec": "avro",
            "schema": "durable_workflow.protocol.Value",
            "version": "1",
            "fingerprint": "e2a33dff55802237",
        }
        fixture["framing"]["encoding"] = "avro-single-object"
        self.write_json(
            "tests/fixtures/codec-regressions/duplicate-long-seven.json",
            fixture,
        )
        self.write_policy(
            "src/Codec/*.php",
            fixture_selectors=(
                (
                    "resources/protocol/avro-value-v1-golden.json",
                    "avro-value-golden-v1",
                ),
                (
                    "tests/fixtures/codec-regressions/*.json",
                    "codec-regression-v1",
                ),
            ),
        )

        result = self.validate()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn("duplicate semantic fixtures", result.stderr)
        self.assertIn("avro-value-v1-golden.json", result.stderr)
        self.assertIn("duplicate-long-seven.json", result.stderr)

    def test_codec_binding_metadata_cannot_disguise_duplicate_behavior(self) -> None:
        (self.root / "src/Codec/Example.php").write_text("<?php\nreturn 'changed';\n")
        fixture = self.codec_fixture("duplicate-with-other-bindings", "0", "AA==")
        fixture["bindings"] = ["rust", "php"]
        self.write_json(
            "tests/fixtures/codec-regressions/duplicate-bindings.json",
            fixture,
        )

        result = self.validate()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn("duplicate semantic fixtures", result.stderr)
        self.assertIn("base.json", result.stderr)
        self.assertIn("duplicate-bindings.json", result.stderr)

    def test_codec_schema_metadata_cannot_disguise_duplicate_behavior(self) -> None:
        (self.root / "src/Codec/Example.php").write_text("<?php\nreturn 'changed';\n")
        fixture = self.codec_fixture("duplicate-with-other-schema", "0", "AA==")
        fixture["protocol"]["schema"] = "metadata.only.Value"
        self.write_json(
            "tests/fixtures/codec-regressions/duplicate-schema.json",
            fixture,
        )

        result = self.validate()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn("duplicate semantic fixtures", result.stderr)
        self.assertIn("base.json", result.stderr)
        self.assertIn("duplicate-schema.json", result.stderr)

    def test_replay_binding_metadata_cannot_satisfy_corpus_growth(self) -> None:
        (self.root / "tests/fixtures/replay-regressions").mkdir(parents=True)
        self.write_json(
            "tests/fixtures/replay-regressions/base.json",
            self.replay_fixture("base-replay-case", ["php"]),
        )
        self.git("add", "--all")
        self.git(
            "-c",
            "user.name=Regression Corpus Test",
            "-c",
            "user.email=regression-corpus@example.invalid",
            "commit",
            "--quiet",
            "--message=add-replay-baseline",
        )
        self.base_ref = self.git("rev-parse", "HEAD").stdout.strip()

        worker = self.root / "src/Worker.php"
        worker.write_text(
            worker.read_text().replace("$history = [];", "$history = ['changed'];")
        )
        self.write_json(
            "tests/fixtures/replay-regressions/duplicate-bindings.json",
            self.replay_fixture("duplicate-with-other-bindings", ["rust", "php"]),
        )

        result = self.validate()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn("duplicate semantic fixtures", result.stderr)
        self.assertIn("base.json", result.stderr)
        self.assertIn("duplicate-bindings.json", result.stderr)

    def test_replay_protocol_metadata_cannot_satisfy_corpus_growth(self) -> None:
        (self.root / "tests/fixtures/replay-regressions").mkdir(parents=True)
        self.write_json(
            "tests/fixtures/replay-regressions/base.json",
            self.replay_fixture("base-replay-case", ["php"]),
        )
        self.git("add", "--all")
        self.git(
            "-c",
            "user.name=Regression Corpus Test",
            "-c",
            "user.email=regression-corpus@example.invalid",
            "commit",
            "--quiet",
            "--message=add-replay-baseline",
        )
        self.base_ref = self.git("rev-parse", "HEAD").stdout.strip()

        worker = self.root / "src/Worker.php"
        worker.write_text(
            worker.read_text().replace("$history = [];", "$history = ['changed'];")
        )
        fixture = self.replay_fixture("duplicate-with-other-protocol", ["php"])
        fixture["protocol_version"] = "2"
        self.write_json(
            "tests/fixtures/replay-regressions/duplicate-protocol.json",
            fixture,
        )

        result = self.validate()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn("duplicate semantic fixtures", result.stderr)
        self.assertIn("base.json", result.stderr)
        self.assertIn("duplicate-protocol.json", result.stderr)

    def test_replay_change_cannot_claim_growth_from_hidden_fixture(self) -> None:
        worker = self.root / "src/Worker.php"
        worker.write_text(
            worker.read_text().replace("$history = [];", "$history = ['changed'];")
        )
        (self.root / "tests/fixtures/replay-regressions").mkdir(parents=True)
        self.write_json(
            "tests/fixtures/replay-regressions/.hidden.json",
            self.replay_fixture("hidden-replay-case", ["php"]),
        )

        result = self.validate()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn(
            "replay implementation changed but its corpus did not grow "
            "(base=0, current=0)",
            result.stderr,
        )

    def test_replay_growth_fails_on_base_and_passes_candidate(self) -> None:
        worker = self.root / "src/Worker.php"
        worker.write_text(
            worker.read_text().replace("$history = [];", "$history = ['changed'];")
        )
        (self.root / "tests/fixtures/replay-regressions").mkdir(parents=True)
        self.write_json(
            "tests/fixtures/replay-regressions/replay-defect.json",
            self.replay_fixture("replay-defect", ["php"]),
        )

        result = self.validate()

        self.assertEqual(0, result.returncode, result.stderr)
        report = json.loads(result.stdout)
        self.assertTrue(report["counts"]["replay"]["related_change"])
        self.assertEqual(1, report["counts"]["replay"]["revision_verified"])
        self.assertEqual(
            {
                "base": "fail",
                "candidate": "pass",
                "consumer": "worker",
            },
            report["counts"]["replay"]["counterfactual"],
        )

    def test_official_php_runner_executes_supported_fixture(self) -> None:
        fixture = self.replay_fixture("official-runner-smoke", ["php"])
        fixture.pop("history")
        fixture["command_sequence"] = [
            {
                "type": "schedule_activity",
                "activity_type": "golden.greet",
                "arguments": ["Ada"],
            }
        ]
        fixture["expected"] = {"type": "schedule_activity"}
        result = self.run_official_php_runner(REPOSITORY_ROOT, fixture)

        self.assertEqual(0, result.returncode, result.stderr)

    def test_validator_records_official_worker_counterfactual(self) -> None:
        root = self.root / "official-worker-counterfactual"
        shutil.copytree(REPOSITORY_ROOT / "src", root / "src")
        (root / "scripts/ci").mkdir(parents=True)
        shutil.copy2(
            PHP_REPLAY_RUNNER,
            root / "scripts/ci/run-replay-regression-fixture.php",
        )
        (root / "tests/fixtures/replay-regressions").mkdir(parents=True)
        policy = {
            "$schema": "https://example.invalid/evidence-schema.json",
            "schema": "durable-workflow.regression-corpus-policy/v1",
            "repository": "sdk-php",
            "binding": "php",
            "categories": {
                "replay": {
                    "fixtures": [
                        {
                            "glob": "tests/fixtures/replay-regressions/*.json",
                            "format": "replay-regression-v1",
                        }
                    ],
                    "guards": [
                        {
                            "glob": "src/Worker.php",
                            "content_patterns": [
                                "function\\s+executeWorkflowTask\\s*\\(",
                            ],
                        }
                    ],
                }
            },
        }
        (root / "regression-corpus-policy.json").write_text(
            json.dumps(policy, indent=2) + "\n"
        )
        worker_path = root / "src/Worker.php"
        candidate_worker = worker_path.read_text()
        dispatch = "$this->replayer->replay($handler, $history, $input, $this->taskQueue, $task)"
        self.assertIn(dispatch, candidate_worker)
        worker_path.write_text(
            candidate_worker.replace(
                dispatch,
                "$this->replayer->replay($handler, [], $input, $this->taskQueue, $task)",
                1,
            )
        )
        for arguments in (
            ("init", "--quiet"),
            ("add", "--all"),
            (
                "-c",
                "user.name=Regression Corpus Test",
                "-c",
                "user.email=regression-corpus@example.invalid",
                "commit",
                "--quiet",
                "--message=defective-base",
            ),
        ):
            result = run("git", *arguments, cwd=root)
            self.assertEqual(0, result.returncode, result.stderr)
        base_ref = run("git", "rev-parse", "HEAD", cwd=root).stdout.strip()

        worker_path.write_text(candidate_worker)
        fixture = self.official_history_fixture("dispatch-history-counterfactual")
        (root / "tests/fixtures/replay-regressions/dispatch-history.json").write_text(
            json.dumps(fixture, indent=2) + "\n"
        )

        result = run(
            sys.executable,
            str(VALIDATOR),
            "--root",
            str(root),
            "--base-ref",
            base_ref,
            "--php-executable",
            "php",
            "--replay-runner",
            str(root / "scripts/ci/run-replay-regression-fixture.php"),
            "--vendor-root",
            str(REPOSITORY_ROOT / "vendor"),
            cwd=root,
        )

        self.assertEqual(0, result.returncode, result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(1, report["counts"]["replay"]["revision_verified"])
        self.assertEqual(
            {
                "base": "fail",
                "candidate": "pass",
                "consumer": "worker",
            },
            report["counts"]["replay"]["counterfactual"],
        )

    def test_official_php_runner_exercises_complete_history(self) -> None:
        source_root = self.root / "complete-history-source"
        shutil.copytree(REPOSITORY_ROOT / "src", source_root / "src")
        worker_path = source_root / "src/Worker.php"
        worker = worker_path.read_text()
        page_assembly = """            foreach (($page['history_events'] ?? []) as $event) {
                if (is_array($event)) {
                    $history[] = $event;
                }
            }
"""
        self.assertIn(page_assembly, worker)
        worker_path.write_text(worker.replace(page_assembly, "", 1))

        result = self.run_official_php_runner(
            source_root,
            self.official_history_fixture("complete-history-path"),
        )

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn("does not match", result.stderr)

    def test_official_php_runner_exercises_initial_history_extraction(self) -> None:
        source_root = self.root / "initial-history-source"
        shutil.copytree(REPOSITORY_ROOT / "src", source_root / "src")
        worker_path = source_root / "src/Worker.php"
        worker = worker_path.read_text()
        extraction = "$raw = $task['history_events'] ?? $task['history'] ?? [];"
        self.assertIn(extraction, worker)
        worker_path.write_text(worker.replace(extraction, "$raw = [];", 1))

        result = self.run_official_php_runner(
            source_root,
            self.official_history_fixture("initial-history-path"),
        )

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn("does not match", result.stderr)

    def test_official_php_runner_exercises_workflow_task_dispatch(self) -> None:
        source_root = self.root / "workflow-dispatch-source"
        shutil.copytree(REPOSITORY_ROOT / "src", source_root / "src")
        worker_path = source_root / "src/Worker.php"
        worker = worker_path.read_text()
        dispatch = "$this->replayer->replay($handler, $history, $input, $this->taskQueue, $task)"
        self.assertIn(dispatch, worker)
        worker_path.write_text(
            worker.replace(
                dispatch,
                "$this->replayer->replay($handler, [], $input, $this->taskQueue, $task)",
                1,
            )
        )

        result = self.run_official_php_runner(
            source_root,
            self.official_history_fixture("workflow-task-dispatch-path"),
        )

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn("does not match", result.stderr)

    def test_replay_growth_must_pass_on_candidate(self) -> None:
        worker = self.root / "src/Worker.php"
        worker.write_text(
            worker.read_text().replace("$history = [];", "$history = ['changed'];")
        )
        (self.root / "tests/fixtures/replay-regressions").mkdir(parents=True)
        self.write_json(
            "tests/fixtures/replay-regressions/candidate-failure.json",
            self.replay_fixture("candidate-failure", ["php"]),
        )

        result = self.validate()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn(
            "does not pass on the candidate through the official PHP binding",
            result.stderr,
        )

    def test_candidate_runner_cannot_manufacture_the_base_failure(self) -> None:
        worker = self.root / "src/Worker.php"
        worker.write_text(
            worker.read_text().replace("$history = [];", "$history = ['changed'];")
        )
        (self.root / "tests/fixtures/replay-regressions").mkdir(parents=True)
        self.write_json(
            "tests/fixtures/replay-regressions/unrelated.json",
            self.replay_fixture("unrelated-replay", ["php"]),
        )
        self.replay_runner.write_text(
            """import argparse
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--vendor-root")
parser.add_argument("--source-root", type=Path, required=True)
parser.add_argument("--fixture")
args = parser.parse_args()
source = (args.source_root / "src/Worker.php").read_text()
raise SystemExit(0 if "$history = ['changed'];" in source else 1)
"""
        )

        result = self.validate()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn(
            "official PHP replay runner must remain unchanged",
            result.stderr,
        )

    def test_replay_change_with_only_an_unrelated_fixture_is_rejected(self) -> None:
        worker = self.root / "src/Worker.php"
        worker.write_text(
            worker.read_text().replace("$history = [];", "$history = ['changed'];")
        )
        (self.root / "tests/fixtures/replay-regressions").mkdir(parents=True)
        self.write_json(
            "tests/fixtures/replay-regressions/unrelated.json",
            self.replay_fixture("unrelated-replay", ["php"]),
        )

        result = self.validate()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn(
            "new replay fixture tests/fixtures/replay-regressions/unrelated.json "
            "also passes on the defective base",
            result.stderr,
        )

    def test_unrelated_fixture_beside_replay_defect_is_rejected(self) -> None:
        worker = self.root / "src/Worker.php"
        worker.write_text(
            worker.read_text().replace("$history = [];", "$history = ['changed'];")
        )
        (self.root / "tests/fixtures/replay-regressions").mkdir(parents=True)
        self.write_json(
            "tests/fixtures/replay-regressions/a-replay-defect.json",
            self.replay_fixture("replay-defect", ["php"]),
        )
        self.write_json(
            "tests/fixtures/replay-regressions/unrelated.json",
            self.replay_fixture(
                "unrelated-replay",
                ["php"],
                name="Grace",
                result="hello Grace",
            ),
        )

        result = self.validate()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn(
            "new replay fixture tests/fixtures/replay-regressions/unrelated.json "
            "also passes on the defective base",
            result.stderr,
        )

    def test_history_assembly_change_requires_replay_corpus_growth(self) -> None:
        worker = self.root / "src/Worker.php"
        worker.write_text(
            worker.read_text().replace("$history = [];", "$history = ['changed'];")
        )

        result = self.validate()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn(
            "replay implementation changed but its corpus did not grow "
            "(base=0, current=0)",
            result.stderr,
        )

    def test_replay_dispatch_change_requires_replay_corpus_growth(self) -> None:
        worker = self.root / "src/Worker.php"
        worker.write_text(
            worker.read_text().replace(
                "$commands = $this->replayer->replay();",
                "$commands = $this->replayer->replay($history);",
            )
        )

        result = self.validate()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn(
            "replay implementation changed but its corpus did not grow "
            "(base=0, current=0)",
            result.stderr,
        )

    def test_history_from_task_change_requires_replay_corpus_growth(self) -> None:
        worker = self.root / "src/Worker.php"
        worker.write_text(
            worker.read_text().replace(
                "return $task['history'] ?? [];",
                "return array_values($task['history'] ?? []);",
            )
        )

        result = self.validate()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn(
            "replay implementation changed but its corpus did not grow "
            "(base=0, current=0)",
            result.stderr,
        )

    def test_history_from_task_fixture_is_proved_across_revisions(self) -> None:
        worker = self.root / "src/Worker.php"
        worker.write_text(
            worker.read_text().replace(
                "return $task['history'] ?? [];",
                "return array_values($task['history'] ?? []);",
            )
        )
        (self.root / "tests/fixtures/replay-regressions").mkdir(parents=True)
        self.write_json(
            "tests/fixtures/replay-regressions/history-from-task.json",
            self.replay_fixture("history-from-task-defect", ["php"]),
        )

        result = self.validate()

        self.assertEqual(0, result.returncode, result.stderr)
        report = json.loads(result.stdout)
        self.assertTrue(report["counts"]["replay"]["related_change"])
        self.assertEqual(1, report["counts"]["replay"]["revision_verified"])

    def test_replay_result_change_requires_replay_corpus_growth(self) -> None:
        replay_result = self.root / "src/Worker/ReplayResult.php"
        replay_result.write_text(
            "<?php\nfinal class ReplayResult { public iterable $commands = []; }\n"
        )

        result = self.validate()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn(
            "replay implementation changed but its corpus did not grow "
            "(base=0, current=0)",
            result.stderr,
        )

    def test_heartbeat_only_worker_change_is_replay_neutral(self) -> None:
        worker = self.root / "src/Worker.php"
        worker.write_text(
            worker.read_text().replace("$this->beats = 1;", "$this->beats = 2;")
        )

        result = self.validate()

        self.assertEqual(0, result.returncode, result.stderr)
        report = json.loads(result.stdout)
        self.assertFalse(report["counts"]["replay"]["related_change"])


if __name__ == "__main__":
    unittest.main()
