#!/usr/bin/env python3
"""Focused regressions for privileged native workflow dispatch boundaries."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
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


def workflow_dispatch_input_source(source: str, name: str) -> str:
    lines = source.splitlines()
    marker = f"      {name}:"
    start = lines.index(marker)
    end = next(
        (
            index
            for index, line in enumerate(lines[start + 1 :], start=start + 1)
            if line.startswith("      ") and not line.startswith("       ")
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
            "github.ref == 'refs/heads/main' && "
            "needs.build.outputs.release_published == 'true'",
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
            "framework-bridges-published-smoke.yml": (
                "framework-service-mode",
                (
                    "secrets.DURABLE_WORKFLOW_SERVER_URL",
                    "secrets.DURABLE_WORKFLOW_NAMESPACE",
                    "secrets.DURABLE_WORKFLOW_CLIENT_TOKEN",
                    "secrets.DURABLE_WORKFLOW_WORKER_TOKEN",
                ),
            ),
            "service-mode-published-smoke.yml": (
                "source-free-service-mode",
                (
                    "secrets.DURABLE_WORKFLOW_SERVER_URL",
                    "secrets.DURABLE_WORKFLOW_NAMESPACE",
                    "secrets.DURABLE_WORKFLOW_CLIENT_TOKEN",
                    "secrets.DURABLE_WORKFLOW_WORKER_TOKEN",
                ),
            ),
        }
        for workflow, (job, secret_markers) in workflows.items():
            with self.subTest(workflow=workflow):
                source = workflow_source(workflow)
                self.assertIn("  workflow_dispatch:", source)
                smoke = job_source(source, job)
                self.assert_main_only(smoke, "github.ref == 'refs/heads/main'")
                for privileged_marker in (
                    "environment: published-service-smoke",
                    *secret_markers,
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
                "Prepare the framework runtime qualification",
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

    def test_framework_smoke_requires_an_exact_python_sdk_product_version(
        self,
    ) -> None:
        source = workflow_source("framework-bridges-published-smoke.yml")
        declared_input = workflow_dispatch_input_source(
            source,
            "python_sdk_version",
        )
        smoke = job_source(source, "framework-service-mode")
        validation = step_source(
            smoke,
            "Validate the exact published Python SDK version",
        )
        script = step_script(validation)

        self.assertIn("        required: true", declared_input)
        self.assertIn("        type: string", declared_input)
        self.assertIn(
            "PYTHON_SDK_VERSION: ${{ inputs.python_sdk_version }}",
            validation,
        )
        self.assertLess(
            smoke.index(validation),
            smoke.index("actions/setup-python@"),
        )

        exact_versions = {
            "0.1.16": "0.1.16",
            "2.0.0": "2.0.0",
            "2.0.0-alpha.1": "2.0.0a1",
            "2.0.0-beta.21": "2.0.0b21",
            "2.0.0-rc.9": "2.0.0rc9",
        }
        mutable_or_non_product_versions = (
            "",
            "2.0.0rc9",
            "2.0.0-rc9",
            "2.0.*",
            "^2.0",
            "~2.0.0",
            ">=2.0.0",
            "latest",
        )

        for product_version, pep440_version in exact_versions.items():
            with (
                self.subTest(accepted=product_version),
                tempfile.TemporaryDirectory() as directory,
            ):
                github_env = Path(directory) / "github-env"
                result = subprocess.run(
                    ["bash", "-eu", "-o", "pipefail", "-c", script],
                    env={
                        **os.environ,
                        "GITHUB_ENV": str(github_env),
                        "PYTHON_SDK_VERSION": product_version,
                    },
                    check=False,
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(0, result.returncode, result.stderr)
                self.assertEqual(
                    f"PYTHON_SDK_PEP440_VERSION={pep440_version}\n",
                    github_env.read_text(),
                )

        for version in mutable_or_non_product_versions:
            with (
                self.subTest(rejected=version),
                tempfile.TemporaryDirectory() as directory,
            ):
                result = subprocess.run(
                    ["bash", "-eu", "-o", "pipefail", "-c", script],
                    env={
                        **os.environ,
                        "GITHUB_ENV": str(Path(directory) / "github-env"),
                        "PYTHON_SDK_VERSION": version,
                    },
                    check=False,
                    capture_output=True,
                    text=True,
                )
                self.assertNotEqual(0, result.returncode)

    def test_framework_smoke_installs_and_reports_the_exact_python_sdk(self) -> None:
        smoke = job_source(
            workflow_source("framework-bridges-published-smoke.yml"),
            "framework-service-mode",
        )
        install = step_source(
            smoke,
            "Install and verify the exact published Python activity worker",
        )

        self.assertIn(
            '"durable-workflow==${PYTHON_SDK_PEP440_VERSION}"',
            install,
        )
        self.assertNotIn("durable-workflow~=", install)
        self.assertIn('expected = os.environ["PYTHON_SDK_PEP440_VERSION"]', install)
        self.assertIn("if actual != expected:", install)
        self.assertIn("requested_python_sdk_version=", install)
        self.assertIn("installed_python_sdk_version=", install)
        self.assertIn('os.environ["GITHUB_STEP_SUMMARY"]', install)

    def test_service_mode_smoke_binds_the_declared_server_release(self) -> None:
        source = workflow_source("service-mode-published-smoke.yml")
        smoke = job_source(
            source,
            "source-free-service-mode",
        )
        validate = step_source(smoke, "Validate the exact qualified Server version")
        verify = step_source(smoke, "Verify the installed SDK release identity")
        runtime = step_source(
            smoke,
            "Complete a class-oriented workflow against the published endpoint",
        )

        self.assertIn("server_version:", source)
        self.assertIn("SERVER_VERSION: ${{ inputs.server_version }}", validate)
        self.assertIn("supported-server-versions", verify)
        self.assertIn("getenv('SERVER_VERSION')", verify)
        self.assertIn("QUALIFIED_SERVER_VERSION: ${{ inputs.server_version }}", runtime)
        self.assertIn("$client->clusterInfo()->version", runtime)

    def test_published_laravel_smoke_qualifies_both_runtime_destinations(self) -> None:
        journey = json.loads(
            (REPOSITORY_ROOT / "docs/laravel-adoption-contract.json").read_text()
        )["representative_journey"]
        source = workflow_source("framework-bridges-published-smoke.yml")
        smoke = job_source(source, "framework-service-mode")
        validate = step_source(smoke, "Validate the exact current Server version")
        verify = step_source(smoke, "Verify the installed SDK release identity")
        configure = step_source(
            smoke, "Configure Laravel through auto-discovery and vendor publish"
        )
        fake = step_source(
            smoke, "Prove the published Laravel fake without a runtime"
        )
        cloud_validation = step_source(smoke, "Validate managed Cloud configuration")
        standalone_runtime = step_source(smoke, "Start the exact standalone Server")
        runtime_driver = step_source(
            smoke, "Prepare the framework runtime qualification"
        )
        standalone_execution = step_source(
            smoke, "Complete the published workflow against standalone Server"
        )
        cloud_execution = step_source(
            smoke, "Complete the published workflow against managed Cloud"
        )
        runtime = runtime_driver + standalone_execution + cloud_execution

        self.assertIn("server_version:", source)
        self.assertIn("SERVER_VERSION: ${{ inputs.server_version }}", validate)
        self.assertIn("supported-server-versions", verify)
        self.assertIn("getenv('SERVER_VERSION')", verify)
        destinations = {
            "standalone-server": "job-local-server",
            "managed-cloud": "protected-cloud",
        }
        for destination, transport in destinations.items():
            self.assertIn(
                "          - framework: laravel\n"
                f"            runtime: {destination}\n"
                f"            transport: {transport}",
                smoke,
            )
            self.assertIn(
                "          - framework: symfony\n"
                f"            runtime: {destination}\n"
                f"            transport: {transport}",
                smoke,
            )
        for absent_alias in (
            "DURABLE_WORKFLOW_STANDALONE_SERVER_URL",
            "DURABLE_WORKFLOW_STANDALONE_SERVER_NAMESPACE",
            "DURABLE_WORKFLOW_STANDALONE_SERVER_CLIENT_TOKEN",
            "DURABLE_WORKFLOW_STANDALONE_SERVER_WORKER_TOKEN",
            "DURABLE_WORKFLOW_CLOUD_URL",
            "DURABLE_WORKFLOW_CLOUD_NAMESPACE",
            "DURABLE_WORKFLOW_CLOUD_CLIENT_TOKEN",
            "DURABLE_WORKFLOW_CLOUD_WORKER_TOKEN",
        ):
            self.assertNotIn(absent_alias, smoke)
        for protected_secret in (
            "secrets.DURABLE_WORKFLOW_SERVER_URL",
            "secrets.DURABLE_WORKFLOW_NAMESPACE",
            "secrets.DURABLE_WORKFLOW_CLIENT_TOKEN",
            "secrets.DURABLE_WORKFLOW_WORKER_TOKEN",
        ):
            self.assertIn(protected_secret, cloud_validation)
            self.assertIn(protected_secret, cloud_execution)

        for standalone_marker in (
            'server_image="durableworkflow/server:${SERVER_VERSION}"',
            "mysql:8.0.43",
            "redis:7-alpine",
            "--tmpfs /var/lib/mysql:",
            "--tmpfs /data:",
            '"$server_image" server-bootstrap',
            "DW_OPERATOR_TOKEN",
            "DW_WORKER_TOKEN",
            "DW_AUTH_BACKWARD_COMPATIBLE=false",
            '"$runtime_url/api/ready"',
            '($ready["status"] ?? null) === "ready"',
        ):
            self.assertIn(standalone_marker, standalone_runtime)
        self.assertNotIn("secrets.", standalone_runtime)
        self.assertNotIn("secrets.", standalone_execution)
        self.assertIn(
            "standalone-server:job-local-server|managed-cloud:protected-cloud",
            runtime_driver,
        )

        for marker in (
            "private PublishedGreetingPrefix $prefix",
            "private LaravelWorkflowClientInterface $workflows",
            "return $this->workflows->start(",
            "PublishedGreetingWorkflow::class",
            "php laravel-role-launch.php durable-workflow:worker",
        ):
            self.assertIn(marker, configure + runtime)
        self.assertIn("php artisan durable-workflow:published-fake", fake)
        self.assertIn("assertWorkflowStarted", configure)
        self.assertIn("assertResultRequested", configure)
        self.assertNotIn("secrets.", fake)
        self.assertLess(smoke.index(fake), smoke.index(runtime_driver))
        self.assertIn("QUALIFIED_SERVER_VERSION: ${{ inputs.server_version }}", runtime)
        self.assertIn("clusterInfo()->version", runtime)
        self.assertIn(
            "php laravel-role-launch.php durable-workflow:published-greeting",
            runtime,
        )
        for versioning_marker in (
            "getVersion('published-framework-greeting', 1, 1)",
            "getVersion('published-framework-greeting', 1, 2)",
            "VersionMarkerRecorded",
            "run_client_phase start",
            "run_client_phase finish",
            "worker-initial.log",
            "worker-upgraded.log",
            "count($markers) !== 1",
        ):
            self.assertIn(versioning_marker, configure + runtime)
        for concurrency_marker in (
            "childWorkflow('laravel.child-greeting'",
            "#[Activity('laravel.fail')]",
            "parallel_group_path",
            "count($scheduled) !== 9",
            "count($mixedGroups) !== 1",
            "count($failures) !== 1",
            "partially completed mixed group",
        ):
            self.assertIn(concurrency_marker, configure + runtime)
        self.assertNotIn("continue-on-error", runtime)
        self.assertIn(f"#[Workflow('{journey['workflow_type']}')]", configure)
        self.assertIn(f"#[Activity('{journey['activity_type']}')]", configure)
        self.assertIn(repr(journey["input"][0]), configure)
        self.assertIn(repr(journey["result"]), configure)

    def test_framework_runtime_transport_scripts_are_valid_bash(self) -> None:
        smoke = job_source(
            workflow_source("framework-bridges-published-smoke.yml"),
            "framework-service-mode",
        )
        standalone = step_script(step_source(smoke, "Start the exact standalone Server"))
        prepare = step_script(
            step_source(smoke, "Prepare the framework runtime qualification")
        )
        driver = prepare.split("<<'BASH'\n", 1)[1].rsplit("\nBASH\n", 1)[0]

        for name, script in (
            ("standalone transport", standalone),
            ("runtime driver", driver),
        ):
            with self.subTest(script=name):
                syntax = subprocess.run(
                    ["bash", "-n"],
                    input=script,
                    check=False,
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(0, syntax.returncode, syntax.stderr)

    def test_published_framework_release_signal_is_zero_argument_and_diagnostic(
        self,
    ) -> None:
        smoke = job_source(
            workflow_source("framework-bridges-published-smoke.yml"),
            "framework-service-mode",
        )

        self.assertEqual(2, smoke.count("#[Signal('published.release')]"))
        self.assertEqual(2, smoke.count("public function release(): void {}"))
        self.assertEqual(2, smoke.count("$handle->signal('published.release');"))
        self.assertNotIn("$handle->signal('published.release',", smoke)
        self.assertEqual(
            2,
            smoke.count(
                "catch (\\DurableWorkflow\\Exception\\SignalFailed $exception)"
            ),
        )
        self.assertEqual(2, smoke.count("'reason' => $exception->reason"))
        self.assertEqual(2, smoke.count("'details' => $exception->details"))

    def test_published_smoke_credentials_are_runtime_step_scoped(self) -> None:
        framework = job_source(
            workflow_source("framework-bridges-published-smoke.yml"),
            "framework-service-mode",
        )
        cloud_validation = step_source(
            framework, "Validate managed Cloud configuration"
        )
        cloud_runtime = step_source(
            framework, "Complete the published workflow against managed Cloud"
        )
        non_cloud_steps = framework.replace(cloud_validation, "").replace(
            cloud_runtime, ""
        )
        self.assertNotIn("secrets.", framework[: framework.index("    steps:")])
        self.assertNotIn("secrets.", non_cloud_steps)
        for step in (cloud_validation, cloud_runtime):
            self.assertLess(step.index("        env:"), step.index("        run:"))
        for marker in (
            "secrets.DURABLE_WORKFLOW_SERVER_URL",
            "secrets.DURABLE_WORKFLOW_NAMESPACE",
            "secrets.DURABLE_WORKFLOW_CLIENT_TOKEN",
            "secrets.DURABLE_WORKFLOW_WORKER_TOKEN",
        ):
            self.assertEqual(2, framework.count(marker))
            self.assertIn(marker, cloud_validation)
            self.assertIn(marker, cloud_runtime)

        service = job_source(
            workflow_source("service-mode-published-smoke.yml"),
            "source-free-service-mode",
        )
        runtime = step_source(
            service, "Complete a class-oriented workflow against the published endpoint"
        )
        non_runtime = service.replace(runtime, "")
        self.assertNotIn("secrets.", service[: service.index("    steps:")])
        self.assertNotIn("secrets.", non_runtime)
        self.assertLess(runtime.index("        env:"), runtime.index("        run:"))
        for marker in (
            "secrets.DURABLE_WORKFLOW_SERVER_URL",
            "secrets.DURABLE_WORKFLOW_NAMESPACE",
            "secrets.DURABLE_WORKFLOW_CLIENT_TOKEN",
            "secrets.DURABLE_WORKFLOW_WORKER_TOKEN",
        ):
            self.assertEqual(1, service.count(marker))
            self.assertIn(marker, runtime)

    def test_published_smokes_fail_closed_on_incomplete_or_shared_credentials(
        self,
    ) -> None:
        workflows = {
            "framework-bridges-published-smoke.yml": (
                "framework-service-mode",
                "Validate managed Cloud configuration",
                "DURABLE_WORKFLOW_SERVER_URL",
                "DURABLE_WORKFLOW_CLIENT_TOKEN",
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
            client_token_name,
        ) in workflows.items():
            with self.subTest(workflow=workflow):
                smoke = job_source(workflow_source(workflow), job)
                runtime = step_source(smoke, runtime_name)
                guard = step_script(runtime).split("\n\n", 1)[0]
                environment = {
                    **os.environ,
                    endpoint_name: "https://runtime.example",
                    "DURABLE_WORKFLOW_NAMESPACE": "published-sdk-smoke",
                    client_token_name: "client-secret-value",
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
            framework_job, "Prepare the framework runtime qualification"
        )
        laravel_configuration = step_source(
            framework_job, "Configure Laravel through auto-discovery and vendor publish"
        )
        symfony_configuration = step_source(
            framework_job, "Configure Symfony Bundle and autowired handlers"
        )
        quickstart_worker = (REPOSITORY_ROOT / "examples" / "worker.php").read_text()
        quickstart_client = (REPOSITORY_ROOT / "examples" / "client.php").read_text()

        for runtime in (service, framework):
            self.assertIn("env -u DURABLE_WORKFLOW_WORKER_TOKEN php", runtime)
            self.assertNotIn("DURABLE_WORKFLOW_AUTH_TOKEN", runtime)

        self.assertIn(
            "controlToken: quickstartEnvironment('DURABLE_WORKFLOW_CLIENT_TOKEN')",
            quickstart_client,
        )
        self.assertIn("env -u DURABLE_WORKFLOW_CLIENT_TOKEN php", service)
        self.assertIn(
            'example_dir="$consumer/vendor/durable-workflow/sdk/examples"',
            service,
        )
        self.assertIn('cp "$example_dir/$source" "$consumer/$source"', service)
        self.assertNotIn("tee worker.php", service)
        self.assertNotIn("tee client.php", service)
        self.assertIn(
            "private LaravelWorkflowClientInterface $workflows",
            laravel_configuration,
        )
        self.assertIn("$this->workflows->start(", laravel_configuration)
        self.assertNotIn("$this->workflows->startWorkflow(", laravel_configuration)
        self.assertIn(
            "app(\\App\\Actions\\StartPublishedGreeting::class)",
            laravel_configuration,
        )
        self.assertIn("php artisan config:cache", framework)
        self.assertIn("-u DURABLE_WORKFLOW_TOKEN", framework)
        self.assertIn("-u DURABLE_WORKFLOW_CLIENT_TOKEN", framework)
        self.assertIn("-u DURABLE_WORKFLOW_WORKER_TOKEN", framework)
        self.assertIn("bootstrap/cache/config.php", framework)
        self.assertIn("$configuration['durable-workflow']['credentials']", framework)
        self.assertIn("DURABLE_WORKFLOW_PROCESS_ROLE=worker", framework)
        self.assertIn("DURABLE_WORKFLOW_PROCESS_ROLE=client", framework)
        self.assertIn("DURABLE_WORKFLOW_PROCESS_TOKEN=", framework)
        self.assertIn("'stage' => 'dotenv-file'", framework)
        self.assertIn("'shell-entry'", laravel_configuration)
        self.assertIn("'before-bootstrap'", laravel_configuration)
        self.assertIn("'after-bootstrap'", laravel_configuration)
        self.assertIn("'installed_sdk_version'", framework)
        self.assertIn("'installed_sdk_source_reference'", framework)
        self.assertIn(
            "$kernel->getContainer()->get(WorkflowClientInterface::class)", framework
        )
        self.assertIn("env -u DURABLE_WORKFLOW_CLIENT_TOKEN php", framework)
        self.assertIn("Registered and polling:", framework)
        self.assertIn("workflows=[", framework)
        self.assertIn("laravel.greeting", framework)
        self.assertIn("laravel.child-greeting", framework)
        self.assertIn("activities=[", framework)
        self.assertIn("laravel.greet", framework)
        self.assertIn("laravel.fail", framework)
        self.assertIn(
            "workerToken: quickstartEnvironment('DURABLE_WORKFLOW_WORKER_TOKEN')",
            quickstart_worker,
        )
        self.assertNotIn("DURABLE_WORKFLOW_WORKER_TOKEN", quickstart_client)
        self.assertNotIn("DURABLE_WORKFLOW_CLIENT_TOKEN", quickstart_worker)
        self.assertIn(
            "control_token: '%env(default::DURABLE_WORKFLOW_CLIENT_TOKEN)%'",
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
                "Prepare the framework runtime qualification",
                "php laravel-role-launch.php durable-workflow:published-greeting",
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
