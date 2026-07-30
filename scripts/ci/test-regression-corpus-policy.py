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
PHP_CODEC_RUNNER = VALIDATOR.with_name("run-codec-regression-fixture.php")
PHP_REPLAY_RUNNER = VALIDATOR.with_name("run-replay-regression-fixture.php")
PHP_PAYLOAD_PROJECTOR = VALIDATOR.with_name("project-replay-payload.php")


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
        dependency_source = self.root / "vendor/apache/avro/lang/php/lib"
        dependency_source.mkdir(parents=True)
        (dependency_source / "dependency.marker").write_text("trusted-dependency\n")
        self.write_json(
            "composer.json",
            {
                "require": {
                    "apache/avro": "^1.12",
                },
            },
        )
        (self.root / "src/Codec/Example.php").write_text("<?php\nreturn 'base';\n")
        (self.root / "src/Client.php").write_text(
            """<?php
final class Client
{
    public function completeWorkflowTask(array $commands): array
    {
        if ($commands === []) {
            return ['status' => 'waiting'];
        }

        return ['commands' => $commands];
    }

    public function serviceStatus(): string
    {
        return 'ready';
    }
}
"""
        )
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
        (self.root / "vendor/autoload.php").write_text("<?php\n")
        self.codec_runner = self.root / "codec-runner.py"
        self.codec_runner.write_text(
            """import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--vendor-root")
parser.add_argument("--consumer-root")
parser.add_argument("--source-root", type=Path, required=True)
parser.add_argument("--fixture", type=Path, required=True)
parser.add_argument("--format")
args = parser.parse_args()
fixture = json.loads(args.fixture.read_text())
source = (args.source_root / "src/Codec/Example.php").read_text()
identity = fixture.get("id", "")
marker = (
    Path(args.vendor_root)
    / "apache/avro/lang/php/lib/dependency.marker"
).read_text()
if identity == "candidate-failure":
    outcome = "assertion-failure"
elif marker == "candidate-vendor-failure\\n":
    outcome = "pass" if "return 'changed';" in source else "assertion-failure"
elif identity == "codec-defect":
    outcome = "pass" if "return 'changed';" in source else "assertion-failure"
elif identity == "target-operational-failure":
    outcome = "pass" if "return 'changed';" in source else "operational-error"
else:
    outcome = "pass"
print(json.dumps({"outcome": outcome}))
raise SystemExit(
    {"pass": 0, "assertion-failure": 1, "operational-error": 2}[outcome]
)
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
        encoded_results = {
            "hello Ada": "wwHioz3/VYAiNwoSaGVsbG8gQWRh",
            "hello Grace": "wwHioz3/VYAiNwoWaGVsbG8gR3JhY2U=",
        }
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
                    "payload": {
                        "result": {
                            "codec": "avro",
                            "blob": encoded_results[result],
                        }
                    },
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
            "--codec-runner",
            str(self.codec_runner),
            "--replay-runner",
            str(self.replay_runner),
            "--vendor-root",
            str(self.root / "vendor"),
            "--payload-projector-executable",
            "php",
            "--payload-projector",
            str(PHP_PAYLOAD_PROJECTOR),
            "--payload-consumer-root",
            str(REPOSITORY_ROOT),
            "--payload-consumer-vendor-root",
            str(REPOSITORY_ROOT / "vendor"),
            cwd=self.root,
        )

    def validate_without_base(self) -> subprocess.CompletedProcess[str]:
        return run(
            sys.executable,
            str(VALIDATOR),
            "--root",
            str(self.root),
            "--php-executable",
            sys.executable,
            "--codec-runner",
            str(self.codec_runner),
            "--replay-runner",
            str(self.replay_runner),
            "--vendor-root",
            str(self.root / "vendor"),
            "--payload-projector-executable",
            "php",
            "--payload-projector",
            str(PHP_PAYLOAD_PROJECTOR),
            "--payload-consumer-root",
            str(REPOSITORY_ROOT),
            "--payload-consumer-vendor-root",
            str(REPOSITORY_ROOT / "vendor"),
            cwd=self.root,
        )

    def validate_with_payload_projector(
        self,
        executable: str,
        projector: Path,
    ) -> subprocess.CompletedProcess[str]:
        return run(
            sys.executable,
            str(VALIDATOR),
            "--root",
            str(self.root),
            "--php-executable",
            sys.executable,
            "--codec-runner",
            str(self.codec_runner),
            "--replay-runner",
            str(self.replay_runner),
            "--vendor-root",
            str(self.root / "vendor"),
            "--payload-projector-executable",
            executable,
            "--payload-projector",
            str(projector),
            "--payload-consumer-root",
            str(REPOSITORY_ROOT),
            "--payload-consumer-vendor-root",
            str(REPOSITORY_ROOT / "vendor"),
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

    def official_replay_fixture(
        self,
        identity: str,
        workflow_type: str,
        workflow_input: list[Any],
        expected: dict[str, Any],
        history: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        fixture = self.replay_fixture(identity, ["php"])
        fixture["workflow"] = {
            "type": workflow_type,
            "input": workflow_input,
        }
        fixture["expected"] = expected
        if history is None:
            fixture.pop("history")
            commands = expected.get("command_sequence")
            fixture["command_sequence"] = (
                commands if isinstance(commands, list) else [expected]
            )
        else:
            fixture["history"] = history

        return fixture

    def official_timer_fixture(self, identity: str) -> dict[str, Any]:
        return self.official_replay_fixture(
            identity,
            "golden.timer",
            [5],
            {
                "type": "start_timer",
                "delay_seconds": 5,
            },
        )

    def official_replay_repository(self, name: str) -> tuple[Path, str]:
        root = self.root / name
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
                "replay": self.repository_policy["categories"]["replay"],
            },
        }
        (root / "regression-corpus-policy.json").write_text(
            json.dumps(policy, indent=2) + "\n"
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
                "--message=baseline",
            ),
        ):
            result = run("git", *arguments, cwd=root)
            self.assertEqual(0, result.returncode, result.stderr)

        return root, run("git", "rev-parse", "HEAD", cwd=root).stdout.strip()

    def validate_official_replay_repository(
        self,
        root: Path,
        base_ref: str,
    ) -> subprocess.CompletedProcess[str]:
        return run(
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
            "--payload-projector-executable",
            "php",
            "--payload-projector",
            str(PHP_PAYLOAD_PROJECTOR),
            "--payload-consumer-root",
            str(root),
            "--payload-consumer-vendor-root",
            str(REPOSITORY_ROOT / "vendor"),
            cwd=root,
        )

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

    def test_noncanonical_base64_wire_is_rejected(self) -> None:
        self.write_json(
            "tests/fixtures/codec-regressions/noncanonical.json",
            self.codec_fixture("noncanonical-wire", "2", "AB=="),
        )

        result = self.validate()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn("is not canonical base64", result.stderr)

    def test_malformed_golden_wire_must_be_canonical_base64(self) -> None:
        path = self.root / "resources/protocol/avro-value-v1-golden.json"
        fixture = json.loads(path.read_text())
        fixture["malformed_frames"][0]["wire_base64"] = "%%%"
        self.write_json(
            "resources/protocol/avro-value-v1-golden.json",
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

        result = self.validate_without_base()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn("is not canonical base64", result.stderr)

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

    def test_codec_growth_fails_on_base_and_passes_candidate(self) -> None:
        (self.root / "src/Codec/Example.php").write_text("<?php\nreturn 'changed';\n")
        self.write_json(
            "tests/fixtures/codec-regressions/codec-defect.json",
            self.codec_fixture("codec-defect", "2", "Ag=="),
        )

        result = self.validate()

        self.assertEqual(0, result.returncode, result.stderr)
        report = json.loads(result.stdout)
        codec = report["counts"]["codec"]
        self.assertTrue(codec["related_change"])
        self.assertEqual(1, codec["revision_verified"])
        self.assertEqual(1, codec["target_failed"])
        self.assertEqual(1, codec["candidate_passed"])
        self.assertEqual(
            {
                "target": "assertion-failure",
                "candidate": "pass",
                "consumer": "codec",
            },
            codec["counterfactual"],
        )

    def test_official_php_codec_runner_executes_supported_fixture(self) -> None:
        result = run(
            "php",
            str(PHP_CODEC_RUNNER),
            "--vendor-root",
            str(REPOSITORY_ROOT / "vendor"),
            "--consumer-root",
            str(REPOSITORY_ROOT),
            "--source-root",
            str(REPOSITORY_ROOT),
            "--fixture",
            str(
                REPOSITORY_ROOT
                / "tests/fixtures/codec-regressions/avro-value-v1-long-zero.json"
            ),
            "--format",
            "codec-regression-v1",
            cwd=REPOSITORY_ROOT,
        )

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual({"outcome": "pass"}, json.loads(result.stdout))

    def test_official_php_codec_runner_reports_assertion_failures(self) -> None:
        source_root = self.root / "defective-codec-source"
        shutil.copytree(REPOSITORY_ROOT / "src", source_root / "src")
        codec_path = source_root / "src/Codec/AvroPayloadCodec.php"
        codec = codec_path.read_text()
        self.assertIn("e2a33dff55802237", codec)
        codec_path.write_text(codec.replace("e2a33dff55802237", "0000000000000000", 1))

        result = run(
            "php",
            str(PHP_CODEC_RUNNER),
            "--vendor-root",
            str(REPOSITORY_ROOT / "vendor"),
            "--consumer-root",
            str(REPOSITORY_ROOT),
            "--source-root",
            str(source_root),
            "--fixture",
            str(
                REPOSITORY_ROOT
                / "tests/fixtures/codec-regressions/avro-value-v1-long-zero.json"
            ),
            "--format",
            "codec-regression-v1",
            cwd=REPOSITORY_ROOT,
        )

        self.assertEqual(1, result.returncode, result.stdout)
        self.assertEqual({"outcome": "assertion-failure"}, json.loads(result.stdout))

    def test_codec_growth_must_pass_on_candidate(self) -> None:
        (self.root / "src/Codec/Example.php").write_text("<?php\nreturn 'changed';\n")
        self.write_json(
            "tests/fixtures/codec-regressions/candidate-failure.json",
            self.codec_fixture("candidate-failure", "2", "Ag=="),
        )

        result = self.validate()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn(
            "does not pass on the candidate through the official PHP binding",
            result.stderr,
        )

    def test_codec_growth_requires_deterministic_target_failure(self) -> None:
        (self.root / "src/Codec/Example.php").write_text("<?php\nreturn 'changed';\n")
        self.write_json(
            "tests/fixtures/codec-regressions/target-operational-failure.json",
            self.codec_fixture("target-operational-failure", "2", "Ag=="),
        )

        result = self.validate()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn(
            "did not establish a deterministic target-revision assertion failure",
            result.stderr,
        )

    def test_unrelated_fixture_beside_codec_defect_is_rejected(self) -> None:
        (self.root / "src/Codec/Example.php").write_text("<?php\nreturn 'changed';\n")
        self.write_json(
            "tests/fixtures/codec-regressions/a-codec-defect.json",
            self.codec_fixture("codec-defect", "2", "Ag=="),
        )
        self.write_json(
            "tests/fixtures/codec-regressions/unrelated.json",
            self.codec_fixture("unrelated-codec", "3", "Aw=="),
        )

        result = self.validate()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn(
            "new codec fixture tests/fixtures/codec-regressions/unrelated.json "
            "also passes on the defective base",
            result.stderr,
        )

    def test_candidate_only_codec_consumer_cannot_manufacture_base_failure(self) -> None:
        (self.root / "src/Codec/Example.php").write_text("<?php\nreturn 'changed';\n")
        self.write_json(
            "tests/fixtures/codec-regressions/unrelated.json",
            self.codec_fixture("unrelated-codec", "3", "Aw=="),
        )
        self.codec_runner.write_text(
            """import argparse
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--vendor-root")
parser.add_argument("--consumer-root")
parser.add_argument("--source-root", type=Path, required=True)
parser.add_argument("--fixture")
parser.add_argument("--format")
args = parser.parse_args()
source = (args.source_root / "src/Codec/Example.php").read_text()
raise SystemExit(0 if "return 'changed';" in source else 1)
"""
        )

        result = self.validate()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn(
            "new codec fixture tests/fixtures/codec-regressions/unrelated.json "
            "also passes on the defective base",
            result.stderr,
        )

    def test_candidate_generated_vendor_cannot_manufacture_base_failure(self) -> None:
        (self.root / "src/Codec/Example.php").write_text("<?php\nreturn 'changed';\n")
        self.write_json(
            "composer.json",
            {
                "require": {
                    "apache/avro": "^1.12",
                },
                "autoload": {
                    "files": ["candidate-autoload.php"],
                },
            },
        )
        (self.root / "vendor/apache/avro/lang/php/lib/dependency.marker").write_text(
            "candidate-vendor-failure\n"
        )
        self.write_json(
            "tests/fixtures/codec-regressions/unrelated.json",
            self.codec_fixture("unrelated-codec", "3", "Aw=="),
        )

        result = self.validate()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn(
            "codec implementation and counterfactual dependency definitions "
            "must change independently: composer.json",
            result.stderr,
        )

    def test_codec_change_cannot_replace_the_dependency_lock(self) -> None:
        (self.root / "src/Codec/Example.php").write_text("<?php\nreturn 'changed';\n")
        self.write_json(
            "composer.lock",
            {
                "packages": [
                    {
                        "name": "apache/avro",
                        "version": "candidate-only",
                    },
                ],
            },
        )
        self.write_json(
            "tests/fixtures/codec-regressions/unrelated.json",
            self.codec_fixture("unrelated-codec", "3", "Aw=="),
        )

        result = self.validate()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn(
            "codec implementation and counterfactual dependency definitions "
            "must change independently: composer.lock",
            result.stderr,
        )

    def test_compound_codec_file_cannot_count_unverified_fixtures(self) -> None:
        (self.root / "src/Codec/Example.php").write_text("<?php\nreturn 'changed';\n")
        self.write_json(
            "resources/protocol/new-golden.json",
            json.loads(
                (self.root / "resources/protocol/avro-value-v1-golden.json").read_text()
            ),
        )
        self.write_policy(
            "src/Codec/*.php",
            fixture_selectors=(
                (
                    "tests/fixtures/codec-regressions/*.json",
                    "codec-regression-v1",
                ),
                (
                    "resources/protocol/new-golden.json",
                    "avro-value-golden-v1",
                ),
            ),
        )

        result = self.validate()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn(
            "each new codec evidence file must contain exactly one independently "
            "verified fixture: resources/protocol/new-golden.json",
            result.stderr,
        )

    def test_official_php_codec_runner_classifies_execution_errors_as_operational(
        self,
    ) -> None:
        source_root = self.root / "missing-codec-source"
        (source_root / "src").mkdir(parents=True)
        result = run(
            "php",
            str(PHP_CODEC_RUNNER),
            "--vendor-root",
            str(REPOSITORY_ROOT / "vendor"),
            "--consumer-root",
            str(REPOSITORY_ROOT),
            "--source-root",
            str(source_root),
            "--fixture",
            str(
                REPOSITORY_ROOT
                / "tests/fixtures/codec-regressions/avro-value-v1-long-zero.json"
            ),
            "--format",
            "codec-regression-v1",
            cwd=REPOSITORY_ROOT,
        )

        self.assertEqual(2, result.returncode, result.stdout)
        self.assertEqual({"outcome": "operational-error"}, json.loads(result.stdout))

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

    def test_redundant_replay_command_assertions_are_duplicate_evidence(self) -> None:
        (self.root / "tests/fixtures/replay-regressions").mkdir(parents=True)
        baseline = self.replay_fixture("base-replay-case", ["php"])
        self.write_json(
            "tests/fixtures/replay-regressions/base.json",
            baseline,
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

        duplicate = self.replay_fixture("redundant-command-rewrap", ["php"])
        duplicate["command_sequence"] = duplicate["expected"]["command_sequence"]
        self.write_json(
            "tests/fixtures/replay-regressions/redundant-command-rewrap.json",
            duplicate,
        )

        result = self.validate()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn("duplicate semantic fixtures", result.stderr)
        self.assertIn("base.json", result.stderr)
        self.assertIn("redundant-command-rewrap.json", result.stderr)

    def test_golden_rewrap_with_redundant_commands_is_duplicate_evidence(
        self,
    ) -> None:
        (self.root / "tests/fixtures/replay-regressions").mkdir(parents=True)
        baseline = self.replay_fixture("golden-source", ["php"])
        self.write_json(
            "regression-corpus-policy.json",
            {
                "$schema": "https://example.invalid/policy-schema.json",
                "schema": "durable-workflow.regression-corpus-policy/v1",
                "repository": "sdk-php",
                "categories": {
                    "replay": {
                        "fixtures": [
                            {
                                "glob": "tests/fixtures/golden-history.json",
                                "format": "golden-history-v1",
                            },
                            {
                                "glob": "tests/fixtures/replay-regressions/*.json",
                                "format": "replay-regression-v1",
                            },
                        ],
                        "guards": [{"glob": "src/Worker.php"}],
                    }
                },
            },
        )
        self.write_json(
            "tests/fixtures/golden-history.json",
            {
                "fixture_schema": "durable-workflow.golden-history.v1",
                "source": {
                    "runtime": "sdk-php",
                    "version": "1.0.0",
                    "worker_protocol_version": "1",
                },
                "cases": [
                    {
                        "name": "single_activity",
                        "workflow_type": baseline["workflow"]["type"],
                        "start_input": baseline["workflow"]["input"],
                        "history": baseline["history"],
                        "expected": {
                            "command_type": "CompleteWorkflow",
                            "result": "hello Ada",
                        },
                    }
                ],
            },
        )
        duplicate = self.replay_fixture("redundant-command-rewrap", ["php"])
        duplicate["command_sequence"] = duplicate["expected"]["command_sequence"]
        self.write_json(
            "tests/fixtures/replay-regressions/redundant-command-rewrap.json",
            duplicate,
        )

        result = self.validate_without_base()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn("duplicate semantic fixtures", result.stderr)
        self.assertIn("golden-history.json", result.stderr)
        self.assertIn("redundant-command-rewrap.json", result.stderr)

        changed_commands = [
            {
                "type": "complete_workflow",
                "result": "hello Grace",
            }
        ]
        duplicate["command_sequence"] = changed_commands
        duplicate["expected"]["command_sequence"] = changed_commands
        self.write_json(
            "tests/fixtures/replay-regressions/redundant-command-rewrap.json",
            duplicate,
        )

        result = self.validate_without_base()

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual(2, json.loads(result.stdout)["counts"]["replay"]["current"])

    def test_numeric_history_sequence_aliases_are_duplicate_evidence(self) -> None:
        (self.root / "tests/fixtures/replay-regressions").mkdir(parents=True)
        baseline = self.replay_fixture("history-sequence-source", ["php"])
        baseline["history"][0]["payload"]["sequence"] = 1
        baseline["expected"] = {"type": "complete_workflow"}
        self.write_json(
            "tests/fixtures/replay-regressions/base.json",
            baseline,
        )

        variants = (
            ("sequence", "1.0"),
            ("sequence", "1e0"),
            ("workflow_sequence", "1"),
            ("workflow_sequence", "1.0"),
            ("workflow_sequence", "1e0"),
        )
        for field, sequence in variants:
            with self.subTest(field=field, sequence=sequence):
                duplicate = json.loads(json.dumps(baseline))
                duplicate["id"] = f"history-sequence-{field}-{sequence}"
                duplicate["history"][0]["payload"].pop("sequence")
                duplicate["history"][0]["payload"][field] = sequence
                self.write_json(
                    "tests/fixtures/replay-regressions/sequence-alias.json",
                    duplicate,
                )

                result = self.validate_without_base()

                self.assertNotEqual(0, result.returncode, result.stdout)
                self.assertIn("duplicate semantic fixtures", result.stderr)
                (
                    self.root / "tests/fixtures/replay-regressions/sequence-alias.json"
                ).unlink()

    def test_overflowing_history_sequence_is_duplicate_evidence(self) -> None:
        (self.root / "tests/fixtures/replay-regressions").mkdir(parents=True)
        baseline = self.official_history_fixture("history-sequence-zero")
        baseline["history"][0]["payload"]["sequence"] = 0
        duplicate = json.loads(json.dumps(baseline))
        duplicate["id"] = "history-sequence-overflow"
        duplicate["history"][0]["payload"]["sequence"] = "1e309"
        self.write_json(
            "tests/fixtures/replay-regressions/base.json",
            baseline,
        )
        self.write_json(
            "tests/fixtures/replay-regressions/overflow-rewrap.json",
            duplicate,
        )

        result = self.validate_without_base()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn("duplicate semantic fixtures", result.stderr)
        self.assertIn("base.json", result.stderr)
        self.assertIn("overflow-rewrap.json", result.stderr)

        for fixture in (baseline, duplicate):
            with self.subTest(identity=fixture["id"]):
                result = self.run_official_php_runner(REPOSITORY_ROOT, fixture)
                self.assertEqual(0, result.returncode, result.stderr)

    def test_null_history_envelope_codec_is_duplicate_evidence(self) -> None:
        (self.root / "tests/fixtures/replay-regressions").mkdir(parents=True)
        baseline = self.official_history_fixture("history-codec-default")
        baseline["history"][0]["payload"]["result"].pop("codec")
        duplicate = json.loads(json.dumps(baseline))
        duplicate["id"] = "history-codec-null"
        duplicate["history"][0]["payload"]["result"]["codec"] = None
        self.write_json(
            "tests/fixtures/replay-regressions/base.json",
            baseline,
        )
        self.write_json(
            "tests/fixtures/replay-regressions/null-codec-rewrap.json",
            duplicate,
        )

        result = self.validate_without_base()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn("duplicate semantic fixtures", result.stderr)
        self.assertIn("base.json", result.stderr)
        self.assertIn("null-codec-rewrap.json", result.stderr)

        for fixture in (baseline, duplicate):
            with self.subTest(identity=fixture["id"]):
                result = self.run_official_php_runner(REPOSITORY_ROOT, fixture)
                self.assertEqual(0, result.returncode, result.stderr)

    def test_raw_and_avro_null_history_results_are_duplicate_evidence(self) -> None:
        (self.root / "tests/fixtures/replay-regressions").mkdir(parents=True)
        baseline = self.official_history_fixture("history-raw-null")
        baseline["history"][0]["payload"]["result"] = None
        baseline["expected"]["command_sequence"][0]["result"] = None
        duplicate = json.loads(json.dumps(baseline))
        duplicate["id"] = "history-avro-null"
        duplicate["history"][0]["payload"]["result"] = {
            "codec": "avro",
            "blob": "wwHioz3/VYAiNwA=",
        }
        self.write_json(
            "tests/fixtures/replay-regressions/base.json",
            baseline,
        )
        self.write_json(
            "tests/fixtures/replay-regressions/avro-null-rewrap.json",
            duplicate,
        )

        result = self.validate_without_base()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn("duplicate semantic fixtures", result.stderr)
        self.assertIn("base.json", result.stderr)
        self.assertIn("avro-null-rewrap.json", result.stderr)

        for fixture in (baseline, duplicate):
            with self.subTest(identity=fixture["id"]):
                result = self.run_official_php_runner(REPOSITORY_ROOT, fixture)
                self.assertEqual(0, result.returncode, result.stderr)

    def test_absent_and_null_worker_update_names_are_duplicate_evidence(
        self,
    ) -> None:
        (self.root / "tests/fixtures/replay-regressions").mkdir(parents=True)
        baseline = self.replay_fixture("worker-update-name-absent", ["php"])
        baseline["workflow"] = {
            "type": "golden.worker-update",
            "input": [],
        }
        baseline["history"] = [
            {
                "event_type": "UpdateAccepted",
                "payload": {
                    "update_id": "update-1",
                    "arguments": {
                        "codec": "avro",
                        "blob": "wwHioz3/VYAiNwoSaGVsbG8gQWRh",
                    },
                },
            }
        ]
        baseline["expected"] = {
            "type": "complete_update",
            "update_id": "update-1",
            "result": {"updated": "hello Ada"},
        }
        duplicate = json.loads(json.dumps(baseline))
        duplicate["id"] = "worker-update-name-null"
        duplicate["history"][0]["payload"]["update_name"] = None
        self.write_json(
            "tests/fixtures/replay-regressions/base.json",
            baseline,
        )
        self.write_json(
            "tests/fixtures/replay-regressions/null-name-rewrap.json",
            duplicate,
        )

        result = self.validate_without_base()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn("duplicate semantic fixtures", result.stderr)
        self.assertIn("base.json", result.stderr)
        self.assertIn("null-name-rewrap.json", result.stderr)

        for fixture in (baseline, duplicate):
            with self.subTest(identity=fixture["id"]):
                result = self.run_official_php_runner(REPOSITORY_ROOT, fixture)
                self.assertEqual(0, result.returncode, result.stderr)

    def test_php_string_equivalent_worker_update_names_are_duplicate_evidence(
        self,
    ) -> None:
        (self.root / "tests/fixtures/replay-regressions").mkdir(parents=True)
        baseline = self.replay_fixture("worker-update-name-integer", ["php"])
        baseline["workflow"] = {
            "type": "golden.worker-update",
            "input": [],
        }
        baseline["history"] = [
            {
                "event_type": "UpdateAccepted",
                "payload": {
                    "update_id": "update-1",
                    "update_name": 123,
                    "arguments": {
                        "codec": "avro",
                        "blob": "wwHioz3/VYAiNwoSaGVsbG8gQWRh",
                    },
                },
            }
        ]
        baseline["expected"] = {
            "type": "fail_update",
            "update_id": "update-1",
            "message": (
                "No update handler is registered for "
                "golden.worker-update.123."
            ),
            "exception_type": "UnknownUpdate",
            "non_retryable": True,
        }
        duplicate = json.loads(json.dumps(baseline))
        duplicate["id"] = "worker-update-name-string"
        duplicate["history"][0]["payload"]["update_name"] = "123"
        self.write_json(
            "tests/fixtures/replay-regressions/base.json",
            baseline,
        )
        self.write_json(
            "tests/fixtures/replay-regressions/string-name-rewrap.json",
            duplicate,
        )

        result = self.validate_without_base()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn("duplicate semantic fixtures", result.stderr)
        self.assertIn("base.json", result.stderr)
        self.assertIn("string-name-rewrap.json", result.stderr)

        changed = json.loads(json.dumps(baseline))
        changed["id"] = "worker-update-name-distinct"
        changed["history"][0]["payload"]["update_name"] = 124
        changed["expected"]["message"] = (
            "No update handler is registered for golden.worker-update.124."
        )
        self.write_json(
            "tests/fixtures/replay-regressions/string-name-rewrap.json",
            changed,
        )

        result = self.validate_without_base()

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual(2, json.loads(result.stdout)["counts"]["replay"]["current"])

        for fixture in (baseline, duplicate, changed):
            with self.subTest(identity=fixture["id"]):
                result = self.run_official_php_runner(REPOSITORY_ROOT, fixture)
                self.assertEqual(0, result.returncode, result.stderr)

    def test_replay_payload_identity_fails_closed_without_official_consumer(
        self,
    ) -> None:
        (self.root / "tests/fixtures/replay-regressions").mkdir(parents=True)
        self.write_json(
            "tests/fixtures/replay-regressions/base.json",
            self.official_history_fixture("history-consumer-unavailable"),
        )

        result = self.validate_with_payload_projector(
            "php",
            self.root / "missing-payload-projector.php",
        )

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn(
            "official PHP replay payload consumer is unavailable",
            result.stderr,
        )

    def test_replay_payload_identity_fails_closed_on_consumer_disagreement(
        self,
    ) -> None:
        (self.root / "tests/fixtures/replay-regressions").mkdir(parents=True)
        self.write_json(
            "tests/fixtures/replay-regressions/base.json",
            self.official_history_fixture("history-consumer-inconsistent"),
        )
        inconsistent_projector = self.root / "inconsistent-projector.py"
        inconsistent_projector.write_text(
            """import json
import sys

json.load(sys.stdin)
print(json.dumps({
    "schema": "durable-workflow.php-replay-payload-projection/v1",
    "projections": [
        {"type": "null"},
        {"type": "bool", "value": False},
    ],
}))
"""
        )

        result = self.validate_with_payload_projector(
            sys.executable,
            inconsistent_projector,
        )

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn(
            "official PHP replay payload consumer returned inconsistent projections",
            result.stderr,
        )

    def test_consumer_ignored_history_metadata_is_duplicate_evidence(
        self,
    ) -> None:
        (self.root / "tests/fixtures/replay-regressions").mkdir(parents=True)
        baseline = self.replay_fixture("history-metadata-source", ["php"])
        baseline["history"][0]["payload"]["sequence"] = 1
        baseline["expected"] = {"type": "complete_workflow"}
        self.write_json(
            "tests/fixtures/replay-regressions/base.json",
            baseline,
        )
        duplicate = json.loads(json.dumps(baseline))
        duplicate["id"] = "history-metadata-rewrap"
        duplicate["history"][0]["recorded_at"] = "2030-01-01T00:00:00Z"
        duplicate["history"][0]["payload"]["corpus_note"] = "representation only"
        self.write_json(
            "tests/fixtures/replay-regressions/metadata-rewrap.json",
            duplicate,
        )

        result = self.validate_without_base()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn("duplicate semantic fixtures", result.stderr)
        self.assertIn("base.json", result.stderr)
        self.assertIn("metadata-rewrap.json", result.stderr)

    def test_changed_history_result_remains_distinct_evidence(self) -> None:
        (self.root / "tests/fixtures/replay-regressions").mkdir(parents=True)
        baseline = self.official_history_fixture("history-result-source")
        baseline["history"][0]["payload"]["sequence"] = 1
        baseline["expected"] = {"type": "complete_workflow"}
        self.write_json(
            "tests/fixtures/replay-regressions/base.json",
            baseline,
        )
        changed = json.loads(json.dumps(baseline))
        changed["id"] = "history-result-changed"
        changed["history"][0]["payload"]["result"]["blob"] = (
            "wwHioz3/VYAiNwoWaGVsbG8gR3JhY2U="
        )
        self.write_json(
            "tests/fixtures/replay-regressions/changed.json",
            changed,
        )

        result = self.validate_without_base()

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual(2, json.loads(result.stdout)["counts"]["replay"]["current"])

        baseline["expected"]["result"] = "hello Ada"
        changed["expected"]["result"] = "hello Grace"
        for fixture in (baseline, changed):
            with self.subTest(identity=fixture["id"]):
                result = self.run_official_php_runner(REPOSITORY_ROOT, fixture)
                self.assertEqual(0, result.returncode, result.stderr)

    def test_official_php_consumer_ignores_history_representation_details(
        self,
    ) -> None:
        baseline = self.official_history_fixture("history-consumer-source")
        baseline["history"][0]["payload"]["sequence"] = 1
        duplicate = json.loads(json.dumps(baseline))
        duplicate["id"] = "history-consumer-rewrap"
        duplicate["history"][0]["payload"].pop("sequence")
        duplicate["history"][0]["payload"]["workflow_sequence"] = "1e0"
        duplicate["history"][0]["payload"]["corpus_note"] = "representation only"
        duplicate["history"][0]["recorded_at"] = "2030-01-01T00:00:00Z"

        for fixture in (baseline, duplicate):
            with self.subTest(identity=fixture["id"]):
                result = self.run_official_php_runner(REPOSITORY_ROOT, fixture)
                self.assertEqual(0, result.returncode, result.stderr)

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

    def test_official_php_runner_executes_every_guarded_replay_semantic(
        self,
    ) -> None:
        encoded_value = {
            "codec": "avro",
            "blob": "wwHioz3/VYAiNwoSaGVsbG8gQWRh",
        }
        update_history = [
            {
                "event_type": "UpdateAccepted",
                "payload": {
                    "update_id": "update-1",
                    "update_name": "golden.update",
                    "arguments": encoded_value,
                },
            }
        ]
        fixtures = {
            "timer": self.official_timer_fixture("official-timer"),
            "child-workflow": self.official_replay_fixture(
                "official-child-workflow",
                "golden.child-workflow",
                ["golden.child"],
                {
                    "type": "start_child_workflow",
                    "workflow_type": "golden.child",
                    "arguments": ["golden-input"],
                },
            ),
            "side-effect": self.official_replay_fixture(
                "official-side-effect",
                "golden.side-effect",
                ["fixed-value"],
                {
                    "command_sequence": [
                        {
                            "type": "record_side_effect",
                            "result": "fixed-value",
                        },
                        {
                            "type": "complete_workflow",
                            "result": {"side_effect": "fixed-value"},
                        },
                    ]
                },
            ),
            "continue-as-new": self.official_replay_fixture(
                "official-continue-as-new",
                "golden.continue-as-new",
                ["next-input"],
                {
                    "type": "continue_as_new",
                    "arguments": ["next-input"],
                    "queue": "regression-corpus",
                },
            ),
            "search-attributes": self.official_replay_fixture(
                "official-search-attributes",
                "golden.search-attributes",
                ["processing"],
                {
                    "command_sequence": [
                        {
                            "type": "upsert_search_attributes",
                            "attributes": {"status": "processing"},
                        },
                        {
                            "type": "complete_workflow",
                            "result": "search-attributes-upserted",
                        },
                    ]
                },
            ),
            "signal": self.official_replay_fixture(
                "official-signal",
                "golden.signal",
                ["golden.signal"],
                {
                    "type": "complete_workflow",
                    "result": {"signals": [["hello Ada"]]},
                },
                [
                    {
                        "event_type": "SignalReceived",
                        "payload": {
                            "signal_name": "golden.signal",
                            "value": encoded_value,
                        },
                    }
                ],
            ),
            "update": self.official_replay_fixture(
                "official-update",
                "golden.update",
                ["golden.update"],
                {
                    "type": "complete_workflow",
                    "result": {"updates": [["hello Ada"]]},
                },
                update_history,
            ),
            "context-identity": self.official_replay_fixture(
                "official-context-identity",
                "golden.context-identity",
                [],
                {
                    "type": "complete_workflow",
                    "result": {
                        "workflow_id": "regression-workflow",
                        "run_id": "regression-inline",
                    },
                },
            ),
            "cancellation": self.official_replay_fixture(
                "official-cancellation",
                "golden.cancellation",
                [],
                {
                    "type": "fail_workflow",
                    "message": "Workflow cancellation was requested.",
                    "exception_type": "DurableWorkflow\\Exception\\WorkflowCancelled",
                },
            ),
            "worker-update": self.official_replay_fixture(
                "official-worker-update",
                "golden.worker-update",
                [],
                {
                    "type": "complete_update",
                    "update_id": "update-1",
                    "result": {"updated": "hello Ada"},
                },
                update_history,
            ),
        }

        for semantic, fixture in fixtures.items():
            with self.subTest(semantic=semantic):
                result = self.run_official_php_runner(REPOSITORY_ROOT, fixture)
                self.assertEqual(0, result.returncode, result.stderr)

    def test_timer_fixture_proves_non_activity_counterfactual(self) -> None:
        root, base_ref = self.official_replay_repository(
            "official-timer-counterfactual"
        )
        command_path = root / "src/Worker/WorkflowCommand.php"
        candidate_command = command_path.read_text()
        timer = "['delay_seconds' => max(0, $seconds)]"
        self.assertIn(timer, candidate_command)
        command_path.write_text(
            candidate_command.replace(
                timer,
                "['delay_seconds' => max(0, $seconds + 1)]",
                1,
            )
        )
        run("git", "add", "--all", cwd=root)
        result = run(
            "git",
            "-c",
            "user.name=Regression Corpus Test",
            "-c",
            "user.email=regression-corpus@example.invalid",
            "commit",
            "--quiet",
            "--message=defective-base",
            cwd=root,
        )
        self.assertEqual(0, result.returncode, result.stderr)
        base_ref = run("git", "rev-parse", "HEAD", cwd=root).stdout.strip()

        command_path.write_text(candidate_command)
        fixture_path = root / "tests/fixtures/replay-regressions/timer.json"
        fixture_path.write_text(
            json.dumps(
                self.official_timer_fixture("timer-counterfactual"),
                indent=2,
            )
            + "\n"
        )

        result = self.validate_official_replay_repository(root, base_ref)

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

    def test_non_activity_change_cannot_hide_behind_unrelated_fixture(self) -> None:
        root, base_ref = self.official_replay_repository(
            "official-unrelated-timer-change"
        )
        command_path = root / "src/Worker/WorkflowCommand.php"
        command = command_path.read_text()
        timer = "['delay_seconds' => max(0, $seconds)]"
        self.assertIn(timer, command)
        command_path.write_text(
            command.replace(
                timer,
                "['delay_seconds' => max(0, $seconds + 1)]",
                1,
            )
        )
        fixture = self.replay_fixture("unrelated-activity", ["php"])
        fixture.pop("history")
        fixture["command_sequence"] = [
            {
                "type": "schedule_activity",
                "activity_type": "golden.greet",
                "arguments": ["Ada"],
            }
        ]
        fixture["expected"] = {"type": "schedule_activity"}
        fixture_path = root / "tests/fixtures/replay-regressions/unrelated.json"
        fixture_path.write_text(json.dumps(fixture, indent=2) + "\n")

        result = self.validate_official_replay_repository(root, base_ref)

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn(
            "new replay fixture tests/fixtures/replay-regressions/unrelated.json "
            "also passes on the defective base",
            result.stderr,
        )

    def test_validator_records_official_worker_counterfactual(self) -> None:
        root, _ = self.official_replay_repository(
            "official-worker-counterfactual"
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
        run("git", "add", "--all", cwd=root)
        result = run(
            "git",
            "-c",
            "user.name=Regression Corpus Test",
            "-c",
            "user.email=regression-corpus@example.invalid",
            "commit",
            "--quiet",
            "--message=defective-base",
            cwd=root,
        )
        self.assertEqual(0, result.returncode, result.stderr)
        base_ref = run("git", "rev-parse", "HEAD", cwd=root).stdout.strip()

        worker_path.write_text(candidate_worker)
        fixture = self.official_history_fixture("dispatch-history-counterfactual")
        (root / "tests/fixtures/replay-regressions/dispatch-history.json").write_text(
            json.dumps(fixture, indent=2) + "\n"
        )

        result = self.validate_official_replay_repository(root, base_ref)

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

    def test_workflow_task_completion_change_requires_replay_corpus_growth(
        self,
    ) -> None:
        client = self.root / "src/Client.php"
        client.write_text(
            client.read_text().replace(
                "return ['status' => 'waiting'];",
                "return ['status' => 'ready'];",
            )
        )

        result = self.validate()

        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn(
            "replay implementation changed but its corpus did not grow "
            "(base=0, current=0)",
            result.stderr,
        )

    def test_unrelated_client_change_is_replay_neutral(self) -> None:
        client = self.root / "src/Client.php"
        client.write_text(
            client.read_text().replace(
                "return 'ready';",
                "return 'available';",
            )
        )

        result = self.validate()

        self.assertEqual(0, result.returncode, result.stderr)
        report = json.loads(result.stdout)
        self.assertFalse(report["counts"]["replay"]["related_change"])

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
