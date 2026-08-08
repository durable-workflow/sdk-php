#!/usr/bin/env python3
"""Focused regressions for privileged native workflow dispatch boundaries."""

from __future__ import annotations

import os
import subprocess
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


def step_script(step: str) -> str:
    lines = step.splitlines()
    start = lines.index("        run: |") + 1
    script: list[str] = []
    for line in lines[start:]:
        if line and not line.startswith("          "):
            break
        script.append(line[10:] if line else "")
    return "\n".join(script)


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

    def test_published_smokes_reject_caller_selected_refs_before_secret_use(
        self,
    ) -> None:
        workflows = {
            "framework-bridges-published-smoke.yml": "framework-service-mode",
            "service-mode-published-smoke.yml": "source-free-service-mode",
        }
        for workflow, job in workflows.items():
            with self.subTest(workflow=workflow):
                source = workflow_source(workflow)
                self.assertIn("  workflow_dispatch:", source)
                smoke = job_source(source, job)
                self.assert_main_only(smoke, "github.ref == 'refs/heads/main'")
                for privileged_marker in (
                    "environment: published-service-smoke",
                    "secrets.DURABLE_WORKFLOW_SERVER_URL",
                    "secrets.DURABLE_WORKFLOW_NAMESPACE",
                    "secrets.DURABLE_WORKFLOW_CLIENT_TOKEN",
                    "secrets.DURABLE_WORKFLOW_WORKER_TOKEN",
                ):
                    self.assertIn(privileged_marker, smoke)
                    self.assertLess(
                        smoke.index("    if:"), smoke.index(privileged_marker)
                    )

    def test_published_smokes_accept_only_exact_release_versions(self) -> None:
        workflows = {
            "framework-bridges-published-smoke.yml": "framework-service-mode",
            "service-mode-published-smoke.yml": "source-free-service-mode",
        }
        exact_versions = (
            "0.1.16",
            "2.0.0",
            "2.0.0-alpha.1",
            "2.0.0-beta.21",
            "2.0.0-rc.9",
        )
        mutable_selectors = (
            "dev-main",
            "dev-feature#0123456789abcdef0123456789abcdef01234567",
            "2.x",
            "2.0.*",
            "^2.0",
            "~2.0.0",
            ">=2.0.0",
            "2.0.0 || 3.0.0",
            "2.0.0 as 3.0.0",
            "2.0.0@dev",
            "2.0.0-rc.9@RC",
            "https://github.com/durable-workflow/sdk-php.git",
        )

        for workflow, job in workflows.items():
            smoke = job_source(workflow_source(workflow), job)
            validation = step_source(smoke, "Validate the exact published SDK version")
            script = step_script(validation)
            self.assertLess(
                smoke.index(validation), smoke.index("shivammathur/setup-php@")
            )

            for version in exact_versions:
                with self.subTest(workflow=workflow, accepted=version):
                    result = subprocess.run(
                        ["bash", "-eu", "-o", "pipefail", "-c", script],
                        env={**os.environ, "SDK_VERSION": version},
                        check=False,
                        capture_output=True,
                        text=True,
                    )
                    self.assertEqual(0, result.returncode, result.stderr)

            for selector in mutable_selectors:
                with self.subTest(workflow=workflow, rejected=selector):
                    result = subprocess.run(
                        ["bash", "-eu", "-o", "pipefail", "-c", script],
                        env={**os.environ, "SDK_VERSION": selector},
                        check=False,
                        capture_output=True,
                        text=True,
                    )
                    self.assertNotEqual(0, result.returncode)

    def test_published_smokes_verify_the_installed_release_reference(self) -> None:
        workflows = {
            "framework-bridges-published-smoke.yml": (
                "framework-service-mode",
                "Create a fresh framework application from published artifacts",
                "Start the framework worker and complete a workflow",
            ),
            "service-mode-published-smoke.yml": (
                "source-free-service-mode",
                "Install only the published package",
                "Complete a class-oriented workflow against the published endpoint",
            ),
        }
        for workflow, (job, install_name, runtime_name) in workflows.items():
            with self.subTest(workflow=workflow):
                smoke = job_source(workflow_source(workflow), job)
                resolve = step_source(
                    smoke, "Resolve the immutable published SDK release"
                )
                install = step_source(smoke, install_name)
                verify = step_source(smoke, "Verify the installed SDK release identity")
                runtime = step_source(smoke, runtime_name)

                self.assertLess(smoke.index(resolve), smoke.index(install))
                self.assertLess(smoke.index(install), smoke.index(verify))
                self.assertLess(smoke.index(verify), smoke.index(runtime))
                for marker in (
                    'composer show durable-workflow/sdk "$SDK_VERSION" --all --format=json',
                    "($metadata['name'] ?? null) !== 'durable-workflow/sdk'",
                    "($metadata['versions'] ?? null) !== [$version]",
                    "preg_match('/\\A[0-9a-f]{40}\\z/i', $reference)",
                ):
                    self.assertIn(marker, resolve)
                for marker in (
                    "composer.lock",
                    "($package['name'] ?? null) === 'durable-workflow/sdk'",
                    "($packages[0]['version'] ?? null) !== $version",
                    "strtolower($reference) !== strtolower($expectedReference)",
                ):
                    self.assertIn(marker, verify)

    def test_published_smoke_credentials_are_runtime_step_scoped(self) -> None:
        workflows = {
            "framework-bridges-published-smoke.yml": (
                "framework-service-mode",
                "Start the framework worker and complete a workflow",
            ),
            "service-mode-published-smoke.yml": (
                "source-free-service-mode",
                "Complete a class-oriented workflow against the published endpoint",
            ),
        }
        secret_markers = (
            "secrets.DURABLE_WORKFLOW_SERVER_URL",
            "secrets.DURABLE_WORKFLOW_NAMESPACE",
            "secrets.DURABLE_WORKFLOW_CLIENT_TOKEN",
            "secrets.DURABLE_WORKFLOW_WORKER_TOKEN",
        )
        for workflow, (job, runtime_name) in workflows.items():
            with self.subTest(workflow=workflow):
                smoke = job_source(workflow_source(workflow), job)
                runtime = step_source(smoke, runtime_name)
                job_configuration = smoke[: smoke.index("    steps:")]
                self.assertNotIn("secrets.", job_configuration)
                runtime_offset = smoke.index(runtime)
                non_runtime = (
                    smoke[:runtime_offset]
                    + smoke[runtime_offset + len(runtime) :]
                )
                self.assertNotIn("secrets.", non_runtime)
                self.assertLess(
                    runtime.index("        env:"), runtime.index("        run:")
                )
                for marker in secret_markers:
                    self.assertEqual(1, smoke.count(marker))
                    self.assertIn(marker, runtime)

    def test_published_smokes_fail_closed_on_incomplete_or_shared_credentials(
        self,
    ) -> None:
        workflows = {
            "framework-bridges-published-smoke.yml": (
                "framework-service-mode",
                "Start the framework worker and complete a workflow",
                "DURABLE_WORKFLOW_ENDPOINT",
                "DURABLE_WORKFLOW_CONTROL_TOKEN",
            ),
            "service-mode-published-smoke.yml": (
                "source-free-service-mode",
                "Complete a class-oriented workflow against the published endpoint",
                "DURABLE_WORKFLOW_SERVER_URL",
                "DURABLE_WORKFLOW_CLIENT_TOKEN",
            ),
        }
        for workflow, (
            job,
            runtime_name,
            endpoint_name,
            control_token_name,
        ) in workflows.items():
            with self.subTest(workflow=workflow):
                smoke = job_source(workflow_source(workflow), job)
                runtime = step_source(smoke, runtime_name)
                guard = step_script(runtime).split("\n\n", 1)[0]
                environment = {
                    **os.environ,
                    endpoint_name: "https://runtime.example",
                    "DURABLE_WORKFLOW_NAMESPACE": "published-sdk-smoke",
                    control_token_name: "client-secret-value",
                    "DURABLE_WORKFLOW_WORKER_TOKEN": "worker-secret-value",
                }

                missing = subprocess.run(
                    ["bash", "-eu", "-o", "pipefail", "-c", guard],
                    env={
                        key: value
                        for key, value in environment.items()
                        if key != "DURABLE_WORKFLOW_NAMESPACE"
                    },
                    check=False,
                    capture_output=True,
                    text=True,
                )
                self.assertNotEqual(0, missing.returncode)
                self.assertIn("DURABLE_WORKFLOW_NAMESPACE", missing.stderr)
                self.assertNotIn("client-secret-value", missing.stderr)
                self.assertNotIn("worker-secret-value", missing.stderr)

                shared = subprocess.run(
                    ["bash", "-eu", "-o", "pipefail", "-c", guard],
                    env={
                        **environment,
                        "DURABLE_WORKFLOW_WORKER_TOKEN": "client-secret-value",
                    },
                    check=False,
                    capture_output=True,
                    text=True,
                )
                self.assertNotEqual(0, shared.returncode)
                self.assertIn(
                    "client and worker credentials must be distinct", shared.stderr
                )
                self.assertNotIn("client-secret-value", shared.stderr)

                complete = subprocess.run(
                    ["bash", "-eu", "-o", "pipefail", "-c", guard],
                    env=environment,
                    check=False,
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(0, complete.returncode, complete.stderr)

    def test_published_smokes_keep_role_credentials_on_their_own_operations(
        self,
    ) -> None:
        service = step_source(
            job_source(
                workflow_source("service-mode-published-smoke.yml"),
                "source-free-service-mode",
            ),
            "Complete a class-oriented workflow against the published endpoint",
        )
        framework_job = job_source(
            workflow_source("framework-bridges-published-smoke.yml"),
            "framework-service-mode",
        )
        framework = step_source(
            framework_job, "Start the framework worker and complete a workflow"
        )
        symfony_configuration = step_source(
            framework_job, "Configure Symfony Bundle and autowired handlers"
        )

        for runtime in (service, framework):
            self.assertIn("env -u DURABLE_WORKFLOW_WORKER_TOKEN php", runtime)
            self.assertNotIn("DURABLE_WORKFLOW_AUTH_TOKEN", runtime)

        self.assertIn(
            "controlToken: getenv('DURABLE_WORKFLOW_CLIENT_TOKEN') ?: null",
            service,
        )
        self.assertIn("env -u DURABLE_WORKFLOW_CLIENT_TOKEN php", service)
        self.assertIn(
            "$application->make(WorkflowClientInterface::class)", framework
        )
        self.assertIn("php artisan config:cache", framework)
        self.assertIn("-u DURABLE_WORKFLOW_TOKEN", framework)
        self.assertIn("-u DURABLE_WORKFLOW_CONTROL_TOKEN", framework)
        self.assertIn("-u DURABLE_WORKFLOW_WORKER_TOKEN", framework)
        self.assertIn("bootstrap/cache/config.php", framework)
        self.assertIn("$configuration['durable-workflow']['credentials']", framework)
        self.assertIn(
            "$kernel->getContainer()->get(WorkflowClientInterface::class)", framework
        )
        self.assertIn("env -u DURABLE_WORKFLOW_CONTROL_TOKEN php", framework)
        self.assertIn(
            "workerToken: getenv('DURABLE_WORKFLOW_WORKER_TOKEN') ?: null",
            service,
        )
        self.assertIn(
            "control_token: '%env(default::DURABLE_WORKFLOW_CONTROL_TOKEN)%'",
            symfony_configuration,
        )
        self.assertIn(
            "worker_token: '%env(default::DURABLE_WORKFLOW_WORKER_TOKEN)%'",
            symfony_configuration,
        )
        self.assertNotIn("DURABLE_WORKFLOW_TOKEN", symfony_configuration)

    def test_published_smokes_require_graceful_worker_shutdown(self) -> None:
        workflows = {
            "framework-bridges-published-smoke.yml": (
                "framework-service-mode",
                "Start the framework worker and complete a workflow",
                "env -u DURABLE_WORKFLOW_WORKER_TOKEN php durable-client.php",
            ),
            "service-mode-published-smoke.yml": (
                "source-free-service-mode",
                "Complete a class-oriented workflow against the published endpoint",
                "env -u DURABLE_WORKFLOW_WORKER_TOKEN php client.php",
            ),
        }
        for workflow, (job, runtime_name, client) in workflows.items():
            with self.subTest(workflow=workflow):
                runtime = step_script(
                    step_source(
                        job_source(workflow_source(workflow), job), runtime_name
                    )
                )
                self.assertIn(client, runtime)
                self.assertIn('wait "$worker_pid" || worker_status=$?', runtime)
                self.assertNotIn('wait "$worker_pid" 2>/dev/null || true', runtime)
                self.assertIn("worker\\.shutdown_failed", runtime)
                self.assertIn("HTTP[[:space:]]+403", runtime)
                self.assertIn("403[[:space:]]+Forbidden", runtime)
                self.assertIn('if [ "$worker_status" -ne 0 ]', runtime)
                self.assertNotIn(f"{client} || true", runtime)


if __name__ == "__main__":
    unittest.main()
