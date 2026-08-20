#!/usr/bin/env python3
"""Behavior and workflow contracts for portable PHP SDK qualification."""

from __future__ import annotations

import importlib.util
import json
import os
import re
import subprocess
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CLASSIFIER = ROOT / "scripts/ci/classify-ci-qualification.py"
WORKFLOW = ROOT / ".github/workflows/ci.yml"
API_REFERENCE_WORKFLOW = ROOT / ".github/workflows/docs.yml"
EXTERNAL_LINK_WORKFLOW = ROOT / ".github/workflows/external-link-diagnostics.yml"
LARAVEL_ADOPTION_CONTRACT = ROOT / "docs/laravel-adoption-contract.json"

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
            "docs-prose": (["docs/quickstart.md"], ("docs",)),
            "portal-content": (
                ["docs/portal/frameworks/laravel.md"],
                ("docs", "docs-browser"),
            ),
            "docs-browser-template": (
                [".phpdoc/template/assets/api-reference.css"],
                ("docs", "docs-browser"),
            ),
            "docs-browser-check": (
                ["scripts/check-docs-analytics-browser.mjs"],
                ("docs", "docs-browser"),
            ),
            "api-reference-finalizer": (
                ["scripts/finalize-api-reference.php"],
                ("docs", "docs-browser"),
            ),
            "quickstart-deployment-check": (
                ["scripts/qualify-quickstart-contract-deployment.mjs"],
                ("docs",),
            ),
            "quickstart-release-availability": (
                ["scripts/qualify-quickstart-release-availability.mjs"],
                ("docs",),
            ),
            "docs-link-fixture": (
                ["scripts/ci/fixtures/docs-links/external-dns-failure.md"],
                ("docs",),
            ),
            "external-link-diagnostics": (
                [".github/workflows/external-link-diagnostics.yml"],
                ("docs",),
            ),
            "generated-reference-source": (
                ["src/Client.php"],
                ("docs", "runtime"),
            ),
            "runtime-test": (["tests/ClientTest.php"], ("runtime",)),
            "runtime-fixture-runner": (
                ["scripts/ci/run-replay-regression-fixture.php"],
                ("runtime",),
            ),
            "published-runtime-smoke": (
                [".github/workflows/service-mode-published-smoke.yml"],
                ("release", "runtime"),
            ),
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

    def test_api_reference_composer_input_selects_generated_docs_and_runtime_evidence(
        self,
    ) -> None:
        deployment = API_REFERENCE_WORKFLOW.read_text()
        self.assertIn("      - 'composer.json'", deployment)

        categories, reason = self.classify(["composer.json"])

        self.assertEqual(("docs", "runtime"), categories)
        self.assertEqual("changed-paths-classified", reason)

    def test_mixed_changes_combine_relevant_categories(self) -> None:
        categories, _reason = self.classify(
            ["README.md", "src/Client.php", "scripts/ci/publish-planned-source.py"]
        )
        self.assertEqual(
            ("docs", "release", "runtime"),
            categories,
        )

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
            "laravel-transition-compat",
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

    def test_laravel_jobs_match_the_published_adoption_matrix(self) -> None:
        contract = json.loads(LARAVEL_ADOPTION_CONTRACT.read_text())
        framework_job = workflow_job_source(self.source, "framework-compat")
        qualified = contract["framework"]["qualification_matrix"]

        self.assertEqual(len(qualified), framework_job.count("framework: Laravel"))
        for cell in qualified:
            package, constraint = cell["package"].split(":", 1)
            block = (
                "          - framework: Laravel\n"
                f"            version: '{cell['laravel']}'\n"
                f"            php: '{cell['php']}'\n"
                f"            package: '{package}:{constraint}'\n"
                f"            bootstrap: {cell['bootstrap']}\n"
                "            script: laravel.php"
            )
            with self.subTest(cell=cell):
                self.assertIn(block, framework_job)

    def test_fresh_laravel_cached_role_regression_is_executable(self) -> None:
        framework_job = workflow_job_source(self.source, "framework-compat")
        install = workflow_step_script(
            framework_job,
            "Install SDK into a fresh Laravel application for cached role isolation",
        )
        exercise = workflow_step_script(
            framework_job,
            "Exercise fresh Laravel cached role isolation",
        )

        self.assertIn("composer create-project laravel/laravel:^13.0", install)
        self.assertIn(
            'composer --working-dir="$application" require durable-workflow/sdk:@dev',
            install,
        )
        self.assertIn("bash tests/compat/laravel-fresh-config-cache.sh", exercise)
        shell_reproduction = (
            ROOT / "tests/compat/laravel-fresh-config-cache.sh"
        ).read_text()
        for marker in (
            "php artisan config:cache",
            'php "$role_launcher" "$application" durable-workflow:worker',
            'php "$role_launcher" "$application" durable-workflow:role-client',
            "DURABLE_WORKFLOW_PROCESS_ROLE=worker",
            "DURABLE_WORKFLOW_PROCESS_ROLE=client",
            "DURABLE_WORKFLOW_PROCESS_TOKEN=",
            "--assert-probes",
        ):
            self.assertIn(marker, shell_reproduction)
        self.assertNotIn("Symfony\\Component\\Process", shell_reproduction)
        role_launcher = (
            ROOT / "tests/compat/laravel-fresh-role-launch.php"
        ).read_text()
        for stage in ("shell-entry", "before-bootstrap"):
            self.assertIn(f"LaravelFreshRoleProbe::record('{stage}')", role_launcher)
        self.assertIn("LaravelFreshRoleProbe::record('after-bootstrap')", shell_reproduction)
        self.assertLess(
            framework_job.index(
                "- name: Install SDK into a fresh Laravel application for cached role isolation"
            ),
            framework_job.index("- name: Exercise fresh Laravel cached role isolation"),
        )

    def test_established_laravel_transitions_are_executable(self) -> None:
        transition_job = workflow_job_source(self.source, "laravel-transition-compat")
        self.assertIn("source_mode: embedded_v1", transition_job)
        self.assertIn("workflow: '^1.0'", transition_job)
        self.assertIn("source_mode: embedded_v2", transition_job)
        self.assertIn("workflow: '^2.0@RC'", transition_job)
        self.assertEqual(
            1,
            transition_job.count(
                "- name: Exercise the established Laravel application transition"
            ),
        )

    def test_focused_gate_covers_structure_security_and_relevant_changes(self) -> None:
        focused = workflow_job_source(self.source, "focused-candidate")
        self.assertIn("needs.qualification-route.outputs.route == 'focused'", focused)
        self.assertIn("python scripts/ci/test-ci-qualification.py", focused)
        self.assertIn("python scripts/ci/test-workflow-trust-boundaries.py", focused)
        self.assertIn("scripts/check-public-boundary.sh", focused)
        self.assertIn("composer test", focused)
        self.assertIn("npm run test:docs-analytics-deployment", focused)
        self.assertIn("npm run test:docs-browser-failures", focused)
        self.assertIn("--offline", focused)
        self.assertIn("npx playwright install chromium --with-deps", focused)
        self.assertIn("npm run check:docs-analytics-browser -- build/site", focused)
        self.assertIn("npm run check:docs-browser", focused)
        self.assertGreaterEqual(focused.count("'docs-browser'"), 3)
        self.assertNotIn("matrix:", focused)

    def test_source_qualification_uses_only_deterministic_link_checks(self) -> None:
        for source in (self.source, API_REFERENCE_WORKFLOW.read_text()):
            root_dirs = re.findall(r"--root-dir\s+(\S+)", source)
            self.assertGreater(len(root_dirs), 0)
            self.assertFalse(
                any(root.startswith(("/", "${{")) for root in root_dirs),
                "link roots must be portable relative paths",
            )

        workflows = (
            workflow_job_source(self.source, "focused-candidate"),
            workflow_job_source(self.source, "docs"),
            workflow_job_source(API_REFERENCE_WORKFLOW.read_text(), "build"),
        )
        for workflow in workflows:
            with self.subTest(workflow=workflow[:40]):
                self.assertIn(
                    "docker://lycheeverse/lychee@sha256:"
                    "e2d19e57cf6ab037026f20b8e449a1f30d9d7f81eef4194763aab2eab20bd28d",
                    workflow,
                )
                self.assertEqual(2, workflow.count("--offline"))
                self.assertEqual(1, workflow.count("--include-fragments"))
                root_dirs = re.findall(r"--root-dir\s+(\S+)", workflow)
                self.assertCountEqual([".", "build/site"], root_dirs)
                self.assertIn("build/site", workflow)
                self.assertNotIn("--base-url /github/workspace/build/api/", workflow)
                self.assertNotIn(
                    "--base-url ${{ github.workspace }}/build/api/", workflow
                )
                self.assertNotIn("--base-url file:", workflow)
                self.assertNotIn(
                    "--root-dir ${{ github.workspace }}/build/api",
                    workflow,
                )
                self.assertNotIn("--exclude-path build/api/graphs/classes.html", workflow)
                self.assertNotIn("build/api/**/*.html build/api/**/*.css", workflow)
                self.assertNotIn("--method get", workflow)
                self.assertIn("npm run test:docs-browser-failures", workflow)
                self.assertIn("npm run check:docs-analytics-browser -- build/site", workflow)

    def test_link_regression_fixtures_have_one_required_ci_owner(self) -> None:
        route = workflow_job_source(self.source, "qualification-route")
        workflow_sources = self.source + API_REFERENCE_WORKFLOW.read_text()
        fixtures = (
            "scripts/ci/fixtures/docs-links/external-dns-failure.md",
            "scripts/ci/fixtures/docs-links/broken-internal.md",
            "scripts/ci/fixtures/docs-links/malformed-url.md",
        )

        for fixture in fixtures:
            with self.subTest(fixture=fixture):
                self.assertIn(fixture, route)
                self.assertEqual(1, workflow_sources.count(fixture))

        self.assertIn("steps.broken-internal-link.outcome", route)
        self.assertIn("steps.malformed-url.outcome", route)
        self.assertEqual(2, route.count("continue-on-error: true"))
        self.assertEqual(3, route.count("--offline"))

    def test_external_reachability_is_scheduled_and_non_blocking(self) -> None:
        workflow = EXTERNAL_LINK_WORKFLOW.read_text()
        diagnostic = workflow_job_source(workflow, "diagnose")

        self.assertIn("  schedule:", workflow)
        self.assertNotIn("  push:", workflow)
        self.assertNotIn("  pull_request:", workflow)
        self.assertIn("        continue-on-error: true", diagnostic)
        self.assertIn("--method get", diagnostic)
        self.assertIn("--verbose", diagnostic)
        self.assertIn("--format json", diagnostic)
        self.assertIn("--output external-link-diagnostics.json", diagnostic)
        self.assertIn("--include ^https?://", diagnostic)
        self.assertNotIn("--offline", diagnostic)
        self.assertIn("'{method: \"GET\", lychee: .}'", diagnostic)
        self.assertIn("external-link-diagnostics-with-method.json", diagnostic)
        self.assertIn("actions/upload-artifact@", diagnostic)

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
