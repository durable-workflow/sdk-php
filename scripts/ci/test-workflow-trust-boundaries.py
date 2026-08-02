#!/usr/bin/env python3
"""Focused regressions for privileged native workflow dispatch boundaries."""

from __future__ import annotations

import unittest
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def workflow_source(name: str) -> str:
    return (REPOSITORY_ROOT / ".github" / "workflows" / name).read_text()


def job_source(source: str, name: str) -> str:
    lines = source.splitlines()
    start = lines.index(f"  {name}:") + 1
    end = next(
        (
            index
            for index, line in enumerate(lines[start:], start=start)
            if line.startswith("  ") and not line.startswith("   ")
        ),
        len(lines),
    )
    return "\n".join(lines[start:end])


def step_source(job: str, name: str) -> str:
    lines = job.splitlines()
    marker = f"      - name: {name}"
    start = lines.index(marker)
    end = next(
        (
            index
            for index, line in enumerate(lines[start + 1 :], start=start + 1)
            if line.startswith("      - ")
        ),
        len(lines),
    )
    return "\n".join(lines[start:end])


def job_condition(source: str) -> str:
    lines = source.splitlines()
    for index, line in enumerate(lines):
        if not line.startswith("    if:"):
            continue
        value = line.removeprefix("    if:").strip()
        if value not in {">", ">-", "|", "|-"}:
            return value

        continuation: list[str] = []
        for candidate in lines[index + 1 :]:
            if candidate.startswith("      "):
                continuation.append(candidate.strip())
                continue
            break
        return " ".join(continuation)
    raise AssertionError("privileged job has no job-level condition")


class PrivilegedWorkflowDispatchBoundaryTest(unittest.TestCase):
    def assert_main_only(self, job: str, expected: str) -> None:
        self.assertEqual(expected, job_condition(job))
        self.assertLess(job.index("    if:"), job.index("    steps:"))

    def test_release_publisher_rejects_caller_selected_refs_before_authority_use(self) -> None:
        source = workflow_source("release-plan-recovery.yml")
        self.assertIn("  workflow_dispatch:", source)
        publish = job_source(source, "publish")
        self.assert_main_only(
            publish,
            "github.ref == 'refs/heads/main' && "
            "needs.discover.outputs.action == 'publish'",
        )
        for privileged_marker in (
            "environment: release-plan-publication",
            "contents: write",
            "actions/download-artifact@",
            "secrets.RELEASE_PLAN_DEPLOY_KEY",
        ):
            self.assertIn(privileged_marker, publish)

    def test_completed_historical_plan_bypasses_current_train_qualification(self) -> None:
        discover = job_source(workflow_source("release-plan-recovery.yml"), "discover")
        for name in (
            "Resolve the planned Waterline source identity",
            "Check out the public release-train baseline",
            "Check out the exact planned Waterline source",
            "Require a compatible sequential Waterline train",
        ):
            step = step_source(discover, name)
            self.assertIn(
                "        if: steps.recovery.outputs.action == 'publish'",
                step,
            )

        guard = step_source(
            discover, "Require a compatible sequential Waterline train"
        )
        self.assertIn("scripts/ci/php_waterline_release_train.py", guard)

    def test_api_reference_deployer_rejects_branch_built_artifacts(self) -> None:
        source = workflow_source("docs.yml")
        self.assertIn("  workflow_dispatch:", source)
        deploy = job_source(source, "deploy")
        self.assert_main_only(
            deploy,
            "github.server_url == 'https://github.com' && "
            "github.ref == 'refs/heads/main'",
        )
        for privileged_marker in (
            "environment:",
            "id-token: write",
            "pages: write",
            "actions/deploy-pages@",
        ):
            self.assertIn(privileged_marker, deploy)


if __name__ == "__main__":
    unittest.main()
