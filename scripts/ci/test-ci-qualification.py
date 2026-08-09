#!/usr/bin/env python3
"""Behavior and workflow contracts for portable PHP SDK qualification."""

from __future__ import annotations

import importlib.util
import os
import re
import subprocess
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CLASSIFIER = ROOT / "scripts/ci/classify-ci-qualification.py"
WORKFLOW = ROOT / ".github/workflows/ci.yml"

SPEC = importlib.util.spec_from_file_location("ci_qualification", CLASSIFIER)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("Unable to load CI qualification classifier")
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def workflow_job_source(source: str, name: str) -> str:
    marker = f"  {name}:\n"
    if marker not in source:
        raise AssertionError(f"workflow does not define the {name} job")
    job = source.split(marker, 1)[1]
    next_job = re.search(r"(?m)^  [a-z][a-z0-9-]*:\s*$", job)
    return job if next_job is None else job[: next_job.start()]


def workflow_step_script(job: str, name: str) -> str:
    marker = f"      - name: {name}\n"
    if marker not in job:
        raise AssertionError(f"job does not define the {name} step")
    step = job.split(marker, 1)[1]
    run_marker = "        run: |\n"
    if run_marker not in step:
        raise AssertionError(f"{name} is not a shell step")
    source = step.split(run_marker, 1)[1]
    lines = []
    for line in source.splitlines():
        if line and not line.startswith("          "):
            break
        lines.append(line[10:] if line else "")
    return "\n".join(lines)


class RouteClassificationTest(unittest.TestCase):
    def route(
        self,
        server_url: str,
        event_name: str,
        admission: str = "true",
    ) -> str:
        return MODULE.select_route(server_url, event_name, admission)[0]

    def test_github_pull_requests_use_bounded_focused_evidence(self) -> None:
        self.assertEqual(
            MODULE.FOCUSED,
            self.route("https://github.com", "pull_request"),
        )

    def test_github_target_and_manual_runs_use_complete_qualification(self) -> None:
        for event_name in ("push", "workflow_dispatch", "schedule", ""):
            with self.subTest(event_name=event_name):
                self.assertEqual(
                    MODULE.COMPLETE,
                    self.route("https://github.com", event_name),
                )

    def test_identified_alternate_ci_uses_focused_pr_and_target_sentinel(self) -> None:
        server_url = "https://ci.example.test"
        self.assertEqual(MODULE.FOCUSED, self.route(server_url, "pull_request"))
        self.assertEqual(MODULE.SENTINEL, self.route(server_url, "push"))

    def test_unidentified_environments_fail_safe_to_complete(self) -> None:
        cases = (
            ("", "pull_request", "true"),
            ("not-a-url", "pull_request", "true"),
            ("https://[", "pull_request", "true"),
            ("https://ci.example.test", "pull_request", ""),
            ("https://ci.example.test", "pull_request", "false"),
            ("https://ci.example.test/path", "pull_request", "true"),
            ("https://user@ci.example.test", "pull_request", "true"),
            ("https://ci.example.test", "workflow_dispatch", "true"),
        )
        for server_url, event_name, admission in cases:
            with self.subTest(
                server_url=server_url,
                event_name=event_name,
                admission=admission,
            ):
                self.assertEqual(
                    MODULE.COMPLETE,
                    self.route(server_url, event_name, admission),
                )


class ChangedPathClassificationTest(unittest.TestCase):
    def classify(self, paths: list[str]) -> tuple[tuple[str, ...], str]:
        return MODULE.classify_changed_files(paths)

    def test_known_surfaces_select_only_relevant_focused_evidence(self) -> None:
        cases = {
            "docs": (["docs/quickstart.md"], ("docs",)),
            "runtime": (["src/Client.php"], ("runtime",)),
            "release": (
                ["scripts/ci/component-release-recovery.py"],
                ("release",),
            ),
            "ci": ([".github/workflows/ci.yml"], ("ci",)),
        }
        for name, (paths, expected) in cases.items():
            with self.subTest(name=name):
                categories, reason = self.classify(paths)
                self.assertEqual(expected, categories)
                self.assertEqual("changed-paths-classified", reason)

    def test_mixed_changes_combine_relevant_categories(self) -> None:
        categories, _reason = self.classify(
            ["README.md", "src/Client.php", "scripts/ci/publish-planned-source.py"]
        )
        self.assertEqual(("docs", "release", "runtime"), categories)

    def test_missing_invalid_or_unknown_paths_select_every_focused_category(
        self,
    ) -> None:
        for paths in ([], ["../outside"], ["new-product-surface.txt"]):
            with self.subTest(paths=paths):
                categories, _reason = self.classify(paths)
                self.assertEqual(MODULE.ALL_FOCUSED_CATEGORIES, categories)


class WorkflowQualificationContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.source = WORKFLOW.read_text()

    def test_complete_matrix_remains_on_target_push_and_manual_dispatch(self) -> None:
        self.assertIn("  push:\n    branches: [main]", self.source)
        self.assertIn("  workflow_dispatch:", self.source)

        for name in (
            "regression-corpus",
            "test",
            "framework-compat",
            "analyse",
            "docs",
            "package-smoke",
            "release-recovery-boundary",
        ):
            with self.subTest(job=name):
                job = workflow_job_source(self.source, name)
                self.assertIn("    needs: qualification-route", job)
                self.assertIn(
                    "needs.qualification-route.outputs.route == 'complete'",
                    job,
                )

    def test_focused_gate_covers_structure_security_and_relevant_changes(self) -> None:
        focused = workflow_job_source(self.source, "focused-candidate")
        self.assertIn("needs.qualification-route.outputs.route == 'focused'", focused)
        self.assertIn("python scripts/ci/test-ci-qualification.py", focused)
        self.assertIn("python scripts/ci/test-workflow-trust-boundaries.py", focused)
        self.assertIn("scripts/check-public-boundary.sh", focused)
        self.assertIn("composer test", focused)
        self.assertIn("npm run test:docs-analytics-deployment", focused)
        self.assertNotIn("matrix:", focused)

    def test_aggregate_decision_requires_the_selected_route_to_pass(self) -> None:
        decision = workflow_job_source(self.source, "target-branch-qualification")
        self.assertIn('test "$FOCUSED_RESULT" = success', decision)
        self.assertIn('test "$TEST_RESULT" = success', decision)
        self.assertIn('test "$FRAMEWORK_RESULT" = success', decision)
        self.assertIn("before_elapsed_seconds_lower_bound=300", decision)
        self.assertIn("after_elapsed_seconds=$elapsed_seconds", decision)

    def test_aggregate_decision_fails_when_selected_evidence_fails(self) -> None:
        decision = workflow_job_source(self.source, "target-branch-qualification")
        script = workflow_step_script(
            decision,
            "Require selected candidate or target evidence",
        )
        skipped = {
            "FOCUSED_RESULT": "skipped",
            "TEST_RESULT": "skipped",
            "FRAMEWORK_RESULT": "skipped",
            "CORPUS_RESULT": "skipped",
            "ANALYSIS_RESULT": "skipped",
            "DOCS_RESULT": "skipped",
            "PACKAGE_RESULT": "skipped",
            "RECOVERY_RESULT": "skipped",
        }
        complete = {
            **skipped,
            "TEST_RESULT": "success",
            "FRAMEWORK_RESULT": "success",
            "CORPUS_RESULT": "success",
            "ANALYSIS_RESULT": "success",
            "DOCS_RESULT": "success",
            "PACKAGE_RESULT": "success",
            "RECOVERY_RESULT": "success",
        }
        cases = (
            ({**skipped, "ROUTE": "focused", "FOCUSED_RESULT": "success"}, 0),
            ({**skipped, "ROUTE": "focused", "FOCUSED_RESULT": "failure"}, 1),
            ({**complete, "ROUTE": "complete"}, 0),
            ({**complete, "ROUTE": "complete", "TEST_RESULT": "failure"}, 1),
            (
                {**complete, "ROUTE": "complete", "FRAMEWORK_RESULT": "failure"},
                1,
            ),
            ({**skipped, "ROUTE": "sentinel"}, 0),
        )
        for environment, expected_status in cases:
            with self.subTest(environment=environment):
                result = subprocess.run(
                    ["bash", "-eu", "-o", "pipefail", "-c", script],
                    env={**os.environ, **environment},
                    check=False,
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(expected_status, int(result.returncode != 0))


if __name__ == "__main__":
    unittest.main()
