#!/usr/bin/env python3
"""Focused regressions for release recovery workflow source verification."""

from __future__ import annotations

import datetime as dt
import hashlib
import importlib.util
import io
import json
import sys
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest import mock

from cli_release_verifier_contract import CliRecoveryWorkflowSourceTest, CliReleaseAuthorityTest  # noqa: F401
from recovery_workflow_authority import (
    SCHEMA as AUTHORITY_SCHEMA,
)
from recovery_workflow_authority import (
    SOURCE_IDENTITY,
    authority_ref_url,
    authority_url,
    qualification_runs_url,
)

RECOVERY_SCRIPT = Path(__file__).with_name("component-release-recovery.py")
RUST_WORKFLOW_FIXTURE = Path(__file__).with_name("sdk-rust-release-plan-recovery.fixture.yml")

# This is the complete public sdk-rust workflow identified by the verifier's
# pinned digest, not a reduced semantic approximation of its shell commands.
CURRENT_RUST_RECOVERY_WORKFLOW = RUST_WORKFLOW_FIXTURE.read_text()

GENERIC_RECOVERY_WORKFLOW = r"""on:
  schedule:
  workflow_dispatch:
jobs:
  recover:
    steps:
      - run: |
          python recovery.py resolve --preparation-output release-preparation.json
          gh api --method POST "repos/$GITHUB_REPOSITORY/git/refs" \
            -f ref="refs/tags/$RELEASE_TAG" -f sha="$RELEASE_COMMIT"
          select-publication-run \
            --release-tag "$RELEASE_TAG" --release-commit "$RELEASE_COMMIT"
          gh run list --json databaseId,displayTitle,headBranch,headSha,status,conclusion
          gh workflow run release.yml --ref "$RELEASE_TAG" -f tag="$RELEASE_TAG"
"""


def load_recovery_module():
    spec = importlib.util.spec_from_file_location("component_release_recovery_test", RECOVERY_SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def github_http_error(status: int, body: bytes = b"error", **headers: str) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(
        "https://api.github.com/repos/durable-workflow/.github/releases",
        status,
        "request failed",
        headers,
        io.BytesIO(body),
    )


def load_recovery_for_retry_tests():
    loaded = globals().get("recovery")
    if loaded is not None:
        return loaded
    loader = globals().get("load_recovery_module")
    if not callable(loader):
        raise RuntimeError("release recovery module loader is unavailable")
    return loader()


AUTHORITY_COMMIT = "a" * 40


def continuity_resolution_qualification() -> dict[str, object]:
    return {
        "repository": "durable-workflow/.github",
        "workflow": ".github/workflows/beta-candidate.yml",
        "event": "push",
        "head_branch": "main",
        "head_sha": "9" * 40,
        "run_id": 987,
        "run_attempt": 2,
        "status": "completed",
        "conclusion": "success",
    }


def continuity_resolution_qualification_run() -> dict[str, object]:
    qualification = continuity_resolution_qualification()
    return {
        "id": qualification["run_id"],
        "run_attempt": qualification["run_attempt"],
        "repository": {"full_name": "durable-workflow/.github"},
        "head_repository": {"full_name": "durable-workflow/.github"},
        "path": ".github/workflows/beta-candidate.yml@main",
        "event": qualification["event"],
        "head_branch": qualification["head_branch"],
        "head_sha": qualification["head_sha"],
        "status": qualification["status"],
        "conclusion": qualification["conclusion"],
    }



def lifecycle_plan(module, channel: str = "alpha") -> dict[str, object]:
    prerelease = "alpha" if channel == "alpha" else "beta"
    return {
        "schema": module.SCHEMA,
        "plan": "component-recovery",
        "channel": channel,
        "foundation": {"tag": module.FOUNDATION_TAG, "commit": module.FOUNDATION_COMMIT},
        "components": {
            name: {
                "version": (
                    f"2.0.0-{prerelease}.{index + 1}"
                    if name in {"workflow", "waterline"}
                    else f"1.0.{index}"
                ),
                "commit": f"{index + 1:040x}",
            }
            for index, name in enumerate(module.COMPONENTS)
        },
        "beta_authorization": (
            {"tag": "beta-authorization/component-recovery", "commit": "f" * 40}
            if channel == "beta"
            else None
        ),
    }


def supersession_record(module, failed, successor, failed_commit: str) -> dict[str, object]:
    identity = failed["components"]["workflow"]
    observed_commit = "e" * 40
    environment_url = (
        "https://github.com/durable-workflow/.github/deployments/activity_log?"
        "environments_filter=release-plan-supersession"
    )
    protection = {
        "custom_branch_policies": [{"id": 22, "name": "main"}],
        "deployment_branch_policy": {
            "custom_branch_policies": True,
            "protected_branches": False,
        },
        "environment_id": 11,
        "environment_url": environment_url,
        "required_reviewer_rule_ids": [33],
    }
    return {
        "schema": "durable-workflow.release-plan-failure/v1",
        "outcome": "terminal-failure",
        "failed_plan": {
            "tag": f"release-plan/{failed['plan']}",
            "commit": failed_commit,
            "sha256": module.manifest_digest(failed),
        },
        "conflicts": [
            {
                "component": "workflow",
                "version": identity["version"],
                "planned_commit": identity["commit"],
                "observed_commit": observed_commit,
                "reason": "published-version-source-conflict",
                "github_release": {
                    "id": 44,
                    "url": "https://github.com/durable-workflow/workflow/releases/44",
                },
                "distribution": {
                    "kind": "composer",
                    "source_reference": observed_commit,
                    "dist_reference": observed_commit,
                },
            }
        ],
        "successor_plan": {
            "tag": f"release-plan/{successor['plan']}",
            "sha256": module.manifest_digest(successor),
        },
        "authorization": {
            "actor": "release-operator",
            "environment": "release-plan-supersession",
            "environment_approval": {
                "comment": "approved",
                "environments": [
                    {
                        "html_url": environment_url,
                        "id": 11,
                        "name": "release-plan-supersession",
                        "node_id": "environment-node",
                        "url": (
                            "https://api.github.com/repos/durable-workflow/.github/"
                            "environments/release-plan-supersession"
                        ),
                    }
                ],
                "run_attempt": 1,
                "run_id": 456,
                "state": "approved",
                "user": {
                    "html_url": "https://github.com/release-reviewer",
                    "id": 55,
                    "login": "release-reviewer",
                    "node_id": "reviewer-node",
                    "url": "https://api.github.com/users/release-reviewer",
                },
            },
            "environment_protection": protection,
            "repository": "durable-workflow/.github",
            "run_attempt": 1,
            "run_id": 456,
            "run_url": "https://github.com/durable-workflow/.github/actions/runs/456",
            "workflow_commit": "f" * 40,
            "workflow_ref": (
                "durable-workflow/.github/.github/workflows/"
                "release-plan-supersession.yml@refs/heads/main"
            ),
        },
    }


def captured_github_user(login: str, user_id: int, node_id: str) -> dict[str, object]:
    return {
        "login": login,
        "id": user_id,
        "node_id": node_id,
        "avatar_url": f"https://avatars.githubusercontent.com/u/{user_id}?v=4",
        "gravatar_id": "",
        "url": f"https://api.github.com/users/{login}",
        "html_url": f"https://github.com/{login}",
        "followers_url": f"https://api.github.com/users/{login}/followers",
        "following_url": f"https://api.github.com/users/{login}/following{{/other_user}}",
        "gists_url": f"https://api.github.com/users/{login}/gists{{/gist_id}}",
        "starred_url": f"https://api.github.com/users/{login}/starred{{/owner}}{{/repo}}",
        "subscriptions_url": f"https://api.github.com/users/{login}/subscriptions",
        "organizations_url": f"https://api.github.com/users/{login}/orgs",
        "repos_url": f"https://api.github.com/users/{login}/repos",
        "events_url": f"https://api.github.com/users/{login}/events{{/privacy}}",
        "received_events_url": f"https://api.github.com/users/{login}/received_events",
        "type": "User",
        "user_view_type": "public",
        "site_admin": False,
    }


def captured_supersession_github_responses(module) -> list[object]:
    environment_url = (
        "https://github.com/durable-workflow/.github/deployments/activity_log?"
        "environments_filter=release-plan-supersession"
    )
    environment_api_url = (
        "https://api.github.com/repos/durable-workflow/.github/"
        "environments/release-plan-supersession"
    )
    reviewer = captured_github_user("release-reviewer", 55, "reviewer-node")
    approval_environment = {
        "id": 11,
        "node_id": "environment-node",
        "name": "release-plan-supersession",
        "url": environment_api_url,
        "html_url": environment_url,
        "created_at": "2026-07-23T09:00:00Z",
        "updated_at": "2026-07-23T09:30:00Z",
        "can_admins_bypass": False,
        "protection_rules": [
            {
                "id": 33,
                "node_id": "reviewer-rule-node",
                "type": "required_reviewers",
                "prevent_self_review": True,
                "reviewers": [{"type": "User", "reviewer": reviewer}],
            }
        ],
        "deployment_branch_policy": {
            "custom_branch_policies": True,
            "protected_branches": False,
        },
    }
    return [
        approval_environment,
        {
            "total_count": 1,
            "branch_policies": [
                {
                    "id": 22,
                    "node_id": "branch-policy-node",
                    "name": "main",
                    "type": "branch",
                    "created_at": "2026-07-23T09:00:00Z",
                    "updated_at": "2026-07-23T09:30:00Z",
                }
            ],
        },
        {
            "id": 456,
            "name": "Release plan supersession",
            "run_attempt": 1,
            "event": "workflow_dispatch",
            "path": f"{module.SUPERSESSION_WORKFLOW}@main",
            "head_branch": "main",
            "head_sha": "f" * 40,
            "status": "completed",
            "conclusion": "success",
            "html_url": "https://github.com/durable-workflow/.github/actions/runs/456",
            "actor": captured_github_user("release-operator", 54, "operator-node"),
            "repository": {
                "id": 10,
                "node_id": "repository-node",
                "name": ".github",
                "full_name": "durable-workflow/.github",
                "private": False,
                "html_url": "https://github.com/durable-workflow/.github",
            },
        },
        [
            {
                "environments": [json.loads(json.dumps(approval_environment))],
                "user": reviewer,
                "state": "approved",
                "comment": "approved",
            }
        ],
    ]


def supersession_failure_fixture(module):
    failed = lifecycle_plan(module)
    failed["plan"] = "failed-plan"
    successor = json.loads(json.dumps(failed))
    successor["plan"] = "successor-plan"
    successor["components"]["workflow"]["version"] = "2.0.0-alpha.2"
    failed_commit = "a" * 40
    return (
        failed,
        successor,
        failed_commit,
        supersession_record(module, failed, successor, failed_commit),
    )


def qualification_run(
    status: str = "completed",
    conclusion: str | None = "success",
    *,
    head_sha: str = AUTHORITY_COMMIT,
    head_branch: str = "main",
    path: str = ".github/workflows/beta-candidate.yml",
) -> dict[str, object]:
    return {
        "id": 81,
        "run_attempt": 2,
        "name": "Beta candidate",
        "workflow_id": 37,
        "path": path,
        "event": "push",
        "head_branch": head_branch,
        "head_sha": head_sha,
        "status": status,
        "conclusion": conclusion,
        "url": "https://api.github.com/repos/durable-workflow/.github/actions/runs/81",
        "html_url": "https://github.com/durable-workflow/.github/actions/runs/81",
    }


class QualifiedAuthorityConsumerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.recovery = load_recovery_for_retry_tests()

    def authority(self) -> dict[str, object]:
        return {
            "schema": AUTHORITY_SCHEMA,
            "source": SOURCE_IDENTITY,
            "workflows": {
                name: {
                    "repository": component.repository,
                    "ref": f"refs/heads/{component.default_branch}",
                    "path": ".github/workflows/release-plan-recovery.yml",
                    "state": "active",
                    "sha256": "b" * 64,
                }
                for name, component in self.recovery.COMPONENTS.items()
            },
        }

    def client(self, runs: list[dict[str, object]]):
        authority_raw = json.dumps(self.authority()).encode("utf-8")

        class Client:
            def __init__(self) -> None:
                self.requests: list[tuple[str, str]] = []

            def json(self, url: str) -> dict[str, object]:
                self.requests.append(("json", url))
                if url == authority_ref_url():
                    return {"sha": AUTHORITY_COMMIT}
                if url == qualification_runs_url(AUTHORITY_COMMIT):
                    return {"total_count": len(runs), "workflow_runs": runs}
                raise AssertionError(f"peer source was read before authority qualification: {url}")

            def bytes(self, url: str, *, accept: str | None = None) -> bytes:
                self.requests.append(("bytes", url))
                if url != authority_url(AUTHORITY_COMMIT):
                    raise AssertionError(f"peer source was read before authority qualification: {url}")
                return authority_raw

        return Client(), authority_raw

    def test_green_qualification_binds_manifest_bytes_and_revision(self) -> None:
        client, authority_raw = self.client([qualification_run()])
        workflows, source = self.recovery.load_recovery_workflow_authority(client)

        self.assertEqual(set(self.recovery.COMPONENTS), set(workflows))
        self.assertEqual(AUTHORITY_COMMIT, source["commit"])
        self.assertEqual(hashlib.sha256(authority_raw).hexdigest(), source["sha256"])
        self.assertEqual(AUTHORITY_COMMIT, source["qualification"]["head_sha"])
        self.assertEqual(".github/workflows/beta-candidate.yml", source["qualification"]["path"])
        self.assertEqual("main", source["qualification"]["head_branch"])
        self.assertEqual(
            [
                ("json", authority_ref_url()),
                ("json", qualification_runs_url(AUTHORITY_COMMIT)),
                ("bytes", authority_url(AUTHORITY_COMMIT)),
            ],
            client.requests,
        )

    def test_non_green_fails_before_authority_or_peer_source_reads(self) -> None:
        cases = (
            ("pending", [qualification_run("in_progress", None)], "pending"),
            ("failed", [qualification_run("completed", "failure")], "failed"),
            ("cancelled", [qualification_run("completed", "cancelled")], "cancelled"),
            ("absent", [], "absent"),
            ("revision-mismatch", [qualification_run(head_sha="c" * 40)], "another commit"),
            (
                "wrong-workflow",
                [qualification_run(path=".github/workflows/source-qualification.yml")],
                "absent",
            ),
            ("wrong-ref", [qualification_run(head_branch="v2")], "absent"),
            (
                "wrong-path-ref",
                [qualification_run(path=".github/workflows/beta-candidate.yml@v2")],
                "absent",
            ),
        )
        for label, runs, message in cases:
            with self.subTest(state=label):
                client, _authority_raw = self.client(runs)
                with self.assertRaisesRegex(self.recovery.RecoveryError, message):
                    self.recovery.load_recovery_workflow_authority(client)
                self.assertEqual(
                    [
                        ("json", authority_ref_url()),
                        ("json", qualification_runs_url(AUTHORITY_COMMIT)),
                    ],
                    client.requests,
                )


class ContinuityGateTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.recovery = load_recovery_for_retry_tests()

    def test_scheduled_recovery_pauses_until_remote_resume(self) -> None:
        plan = {"plan": "workspace-unavailable-test"}
        with (
            mock.patch.object(
                self.recovery,
                "resolve_tag",
                side_effect=["a" * 40, None],
            ),
            mock.patch.object(self.recovery, "read_record", return_value=plan),
            mock.patch.object(self.recovery, "validate_plan"),
        ):
            paused = self.recovery.scheduled_continuity_pause(mock.Mock(), plan)

        self.assertEqual(
            "beta-continuity/workspace-unavailable-test/resumed",
            paused["resumed_tag"],
        )
        with (
            mock.patch.object(
                self.recovery,
                "resolve_tag",
                side_effect=["a" * 40, "b" * 40],
            ),
            mock.patch.object(self.recovery, "read_record", return_value=plan),
            mock.patch.object(self.recovery, "validate_plan"),
        ):
            self.assertIsNone(self.recovery.scheduled_continuity_pause(mock.Mock(), plan))


class PublicClientRetryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.recovery = load_recovery_for_retry_tests()

    def test_authenticated_requests_preserve_endpoint_api_versions(self) -> None:
        cases = (
            ({"X-GitHub-Api-Version": self.recovery.SUPERSESSION_API_VERSION}, self.recovery.SUPERSESSION_API_VERSION),
            ({}, "2022-11-28"),
        )
        for headers, expected_version in cases:
            with self.subTest(expected_version=expected_version):
                client = self.recovery.PublicClient(token="test-token")
                response = mock.Mock()
                with mock.patch.object(
                    self.recovery.urllib.request, "urlopen", return_value=response
                ) as open_url:
                    self.assertIs(
                        response,
                        client.request(
                            "https://api.github.com/repos/durable-workflow/.github/actions/runs/456",
                            headers=headers,
                        ),
                    )
                request = open_url.call_args.args[0]
                request_headers = {key.lower(): value for key, value in request.header_items()}
                self.assertEqual("Bearer test-token", request_headers["authorization"])
                self.assertEqual(expected_version, request_headers["x-github-api-version"])

    def test_retries_service_failures_connection_resets_and_timeouts(self) -> None:
        failures = (
            ("service", github_http_error(503, **{"Retry-After": "4"}), 4),
            ("connection-reset", urllib.error.URLError(ConnectionResetError("reset")), 1),
            ("timeout", urllib.error.URLError(TimeoutError("timed out")), 1),
        )

        for label, failure, expected_delay in failures:
            with self.subTest(label=label):
                sleeps: list[float] = []
                client = self.recovery.PublicClient(
                    max_attempts=2,
                    retry_base_seconds=1,
                    sleep=sleeps.append,
                )
                with mock.patch.object(
                    self.recovery.urllib.request,
                    "urlopen",
                    side_effect=[failure, io.BytesIO(b"[]")],
                ) as open_url:
                    self.assertEqual(
                        [],
                        client.json(
                            "https://api.github.com/repos/durable-workflow/.github/releases?per_page=100"
                        ),
                    )

                self.assertEqual([expected_delay], sleeps)
                self.assertEqual(2, open_url.call_count)

    def test_authentication_is_terminal_even_with_rate_limit_guidance(self) -> None:
        sleeps: list[float] = []
        client = self.recovery.PublicClient(max_attempts=3, sleep=sleeps.append)
        error = github_http_error(
            401,
            b"Bad credentials: API rate limit exceeded",
            **{"Retry-After": "20", "X-RateLimit-Remaining": "0"},
        )

        with (
            mock.patch.object(self.recovery.urllib.request, "urlopen", side_effect=error) as open_url,
            self.assertRaisesRegex(self.recovery.RecoveryError, r"public request failed \(401\)"),
        ):
            client.json("https://api.github.com/repos/durable-workflow/.github/releases?per_page=100")

        self.assertEqual([], sleeps)
        self.assertEqual(1, open_url.call_count)

    def test_authorization_requires_explicit_rate_limit_guidance(self) -> None:
        client = self.recovery.PublicClient(
            max_attempts=2,
            sleep=lambda _delay: self.fail("ordinary authorization failure was retried"),
        )
        with (
            mock.patch.object(
                self.recovery.urllib.request,
                "urlopen",
                side_effect=github_http_error(403, b"Resource not accessible"),
            ) as open_url,
            self.assertRaisesRegex(self.recovery.RecoveryError, r"public request failed \(403\)"),
        ):
            client.json("https://api.github.com/repos/durable-workflow/.github/releases?per_page=100")
        self.assertEqual(1, open_url.call_count)

        sleeps: list[float] = []
        client = self.recovery.PublicClient(max_attempts=2, retry_base_seconds=1, sleep=sleeps.append)
        with mock.patch.object(
            self.recovery.urllib.request,
            "urlopen",
            side_effect=[
                github_http_error(
                    403,
                    b"API rate limit exceeded",
                    **{"X-RateLimit-Remaining": "0"},
                ),
                io.BytesIO(b"[]"),
            ],
        ) as open_url:
            self.assertEqual(
                [],
                client.json("https://api.github.com/repos/durable-workflow/.github/releases?per_page=100"),
            )
        self.assertEqual([1], sleeps)
        self.assertEqual(2, open_url.call_count)

    def test_retry_exhaustion_has_a_distinct_infrastructure_classification(self) -> None:
        client = self.recovery.PublicClient(max_attempts=2, retry_base_seconds=1, sleep=lambda _delay: None)
        with (
            mock.patch.object(
                self.recovery.urllib.request,
                "urlopen",
                side_effect=[github_http_error(503), github_http_error(502)],
            ) as open_url,
            self.assertRaisesRegex(
                self.recovery.PublicInfrastructureError,
                r"classification=github-read-transient, endpoint_class=releases-api, "
                r"attempts=2, reason=retry-exhausted, status=502",
            ),
        ):
            client.json("https://api.github.com/repos/durable-workflow/.github/releases?per_page=100")
        self.assertEqual(2, open_url.call_count)


class ImmutablePlanDiscoveryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.recovery = load_recovery_for_retry_tests()

    def test_updated_older_release_cannot_override_newer_immutable_plan(self) -> None:
        older = lifecycle_plan(self.recovery)
        older["plan"] = "older-alpha"
        newer = lifecycle_plan(self.recovery, "beta")
        newer["plan"] = "newer-beta"
        tags = ["release-plan/older-alpha", "release-plan/newer-beta"]
        commits = {tags[0]: "a" * 40, tags[1]: "b" * 40}
        recorded = {
            "a" * 40: dt.datetime(2026, 7, 20, tzinfo=dt.UTC),
            "b" * 40: dt.datetime(2026, 7, 22, tzinfo=dt.UTC),
        }

        with (
            mock.patch.object(
                self.recovery,
                "list_release_plan_tags",
                # The older Release may now appear first, but Release order is not authority.
                return_value=tags,
            ),
            mock.patch.object(
                self.recovery,
                "resolve_tag",
                side_effect=lambda _client, _repository, tag: commits[tag],
            ),
            mock.patch.object(
                self.recovery,
                "read_plan_authority",
                side_effect=[(older, None), (newer, None), (older, None), (newer, None)],
            ),
            mock.patch.object(
                self.recovery,
                "direct_plan_lifecycle",
                side_effect=[
                    ("actionable", None),
                    ("completed", None),
                    ("actionable", None),
                    ("completed", None),
                ],
            ),
            mock.patch.object(
                self.recovery,
                "immutable_plan_recorded_at",
                side_effect=lambda _client, commit: recorded[commit],
            ),
            mock.patch.object(
                self.recovery,
                "accepted_continuity_supersession",
                return_value=None,
            ),
            self.assertRaisesRegex(
                self.recovery.RecoveryError,
                "no public release plan is available",
            ),
        ):
            self.recovery.select_implicit_plan_authority(mock.Mock())

    def test_scheduled_recovery_without_actionable_plan_is_a_truthful_no_op(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence = root / "release-recovery-evidence.json"
            github_output = root / "github-output"
            arguments = [
                "component-release-recovery.py",
                "resolve",
                "--component",
                "workflow",
                "--plan-output",
                str(root / "release-plan.json"),
                "--preparation-output",
                str(root / "release-preparation.json"),
                "--evidence",
                str(evidence),
                "--github-output",
                str(github_output),
                "--allow-empty",
            ]

            with (
                mock.patch.object(sys, "argv", arguments),
                mock.patch.object(
                    self.recovery,
                    "discover_plan",
                    side_effect=self.recovery.RecoveryError(
                        "no public release plan is available",
                        "plan-discovery",
                    ),
                ),
                mock.patch.object(self.recovery, "resolve_component") as recover_component,
            ):
                self.assertEqual(0, self.recovery.main())

            recover_component.assert_not_called()
            state = json.loads(evidence.read_text())
            self.assertEqual("plan-discovery", state["phase"])
            self.assertEqual("idle", state["outcome"])
            self.assertEqual("action=none\n", github_output.read_text())

    def test_explicit_completed_plan_is_not_recovered(self) -> None:
        candidate = lifecycle_plan(self.recovery, "beta")
        tag = f"release-plan/{candidate['plan']}"
        commit = "a" * 40
        authority = {
            "tag": tag,
            "commit": commit,
            "recorded_at": dt.datetime(2026, 7, 24, tzinfo=dt.UTC),
            "plan": candidate,
            "preparation": None,
            "lifecycle": "completed",
            "successor": None,
        }
        with (
            mock.patch.object(self.recovery, "classify_plan_authorities", return_value=[authority]),
            self.assertRaisesRegex(self.recovery.RecoveryError, "is completed and cannot be recovered"),
        ):
            self.recovery.select_explicit_plan_authority(mock.Mock(), tag, commit, candidate, None)

    def test_concurrent_terminal_supersession_retries_before_returning_action(self) -> None:
        older = lifecycle_plan(self.recovery)
        older["plan"] = "older-plan"
        successor = lifecycle_plan(self.recovery)
        successor["plan"] = "successor-plan"
        older_tag = "release-plan/older-plan"
        successor_tag = "release-plan/successor-plan"
        commits = {older_tag: "a" * 40, successor_tag: "b" * 40}
        plans = {older_tag: older, successor_tag: successor}
        recorded = {
            commits[older_tag]: dt.datetime(2026, 7, 20, tzinfo=dt.UTC),
            commits[successor_tag]: dt.datetime(2026, 7, 21, tzinfo=dt.UTC),
        }
        terminal_failure: dict[str, object] = {}
        registry_reads = 0

        def list_tags(_client: mock.Mock) -> list[str]:
            nonlocal registry_reads
            registry_reads += 1
            if registry_reads == 2:
                terminal_failure.update(
                    {"outcome": "terminal-failure", "successor": successor_tag}
                )
            return (
                [older_tag, successor_tag]
                if terminal_failure
                else [older_tag]
            )

        def lifecycle(
            _client: mock.Mock,
            tag: str,
            _commit: str,
            _plan: dict[str, object],
            _preparation: None,
        ) -> tuple[str, object | None]:
            if tag == older_tag and terminal_failure:
                return "superseded", {
                    "tag": successor_tag,
                    "sha256": self.recovery.manifest_digest(successor),
                    "plan": successor,
                }
            return "actionable", None

        with (
            mock.patch.object(
                self.recovery,
                "list_release_plan_tags",
                side_effect=list_tags,
            ),
            mock.patch.object(
                self.recovery,
                "resolve_tag",
                side_effect=lambda _client, _repository, tag: commits[tag],
            ),
            mock.patch.object(
                self.recovery,
                "read_plan_authority",
                side_effect=lambda _client, tag, _commit: (plans[tag], None),
            ),
            mock.patch.object(
                self.recovery,
                "direct_plan_lifecycle",
                side_effect=lifecycle,
            ),
            mock.patch.object(
                self.recovery,
                "immutable_plan_recorded_at",
                side_effect=lambda _client, commit: recorded[commit],
            ),
            mock.patch.object(
                self.recovery,
                "accepted_continuity_supersession",
                return_value=None,
            ),
        ):
            selected = self.recovery.select_implicit_plan_authority(mock.Mock())

        self.assertEqual(successor_tag, selected["tag"])
        self.assertEqual("actionable", selected["lifecycle"])
        self.assertEqual(4, registry_reads)

    def test_convergence_rechecks_nonselected_lifecycle_authority(self) -> None:
        older = {"tag": "release-plan/older", "lifecycle": "completed"}
        changed_older = {**older, "lifecycle": "superseded"}
        latest = {"tag": "release-plan/latest", "lifecycle": "actionable"}
        current_snapshot = [changed_older, latest]

        with mock.patch.object(
            self.recovery,
            "classify_implicit_plan_authority",
            side_effect=[
                (latest, [older, latest]),
                (latest, current_snapshot),
                (latest, current_snapshot),
                (latest, current_snapshot),
            ],
        ) as classify:
            selected = self.recovery.select_implicit_plan_authority(mock.Mock())

        self.assertEqual(4, classify.call_count)
        self.assertEqual(current_snapshot, selected["authority_snapshot"])

    def test_final_implicit_boundary_rejects_continuity_pause_activated_after_initial_read(
        self,
    ) -> None:
        candidate = lifecycle_plan(self.recovery)
        candidate_preparation = {
            "components": {
                "workflow": {
                    "release_notes": {
                        "release_date": "2026-07-23",
                        "sha256": "c" * 64,
                        "source": {},
                    }
                }
            }
        }
        component = self.recovery.COMPONENTS["workflow"]
        selected = {"tag": "release-plan/current", "lifecycle": "actionable"}
        authority = {"authority_snapshot": [selected]}
        continuity = mock.Mock(
            side_effect=[
                None,
                {
                    "accepted_tag": f"beta-continuity/{candidate['plan']}/accepted",
                    "accepted_commit": "b" * 40,
                    "resumed_tag": f"beta-continuity/{candidate['plan']}/resumed",
                },
            ]
        )
        publication_preflight = mock.Mock(
            side_effect=self.recovery.NotFound("not published")
        )

        with (
            mock.patch.object(self.recovery, "verify_plan_authority", return_value=({}, {})),
            mock.patch.object(self.recovery, "validate_release_preparation"),
            mock.patch.object(self.recovery, "resolve_tag", return_value=None),
            mock.patch.object(
                self.recovery,
                "classify_implicit_plan_authority",
                return_value=(selected, [selected]),
            ),
            mock.patch.object(
                self.recovery,
                "scheduled_continuity_pause",
                continuity,
            ),
            mock.patch.dict(
                self.recovery.VERIFIERS,
                {component.distribution: publication_preflight},
            ),
        ):
            self.assertIsNone(continuity(mock.Mock(), candidate))
            with self.assertRaisesRegex(
                self.recovery.RecoveryError,
                "continuity pause authority changed during component preflight",
            ):
                self.recovery.resolve_component(
                    mock.Mock(),
                    "workflow",
                    selected["tag"],
                    "a" * 40,
                    candidate,
                    candidate_preparation,
                    authority,
                )

        self.assertEqual(2, continuity.call_count)
        self.assertEqual(1, publication_preflight.call_count)

    def test_explicit_actionable_and_interrupted_plans_may_publish(self) -> None:
        candidate = lifecycle_plan(self.recovery)
        preparation = {
            "components": {
                "workflow": {
                    "release_notes": {
                        "release_date": "2026-07-23",
                        "sha256": "c" * 64,
                        "source": {},
                    }
                }
            }
        }
        tag = f"release-plan/{candidate['plan']}"
        commit = "a" * 40
        component = self.recovery.COMPONENTS["workflow"]

        for lifecycle, successor in (
            ("actionable", None),
            ("interrupted", f"beta-continuity/{candidate['plan']}/interrupted"),
        ):
            with self.subTest(lifecycle=lifecycle):
                authority = {
                    "selection": "explicit",
                    "tag": tag,
                    "commit": commit,
                    "recorded_at": dt.datetime(2026, 7, 23, tzinfo=dt.UTC),
                    "plan": candidate,
                    "preparation": preparation,
                    "lifecycle": lifecycle,
                    "successor": successor,
                }
                with (
                    mock.patch.object(
                        self.recovery, "verify_plan_authority", return_value=({}, {})
                    ),
                    mock.patch.object(self.recovery, "validate_release_preparation"),
                    mock.patch.object(self.recovery, "resolve_tag", return_value=None),
                    mock.patch.object(
                        self.recovery,
                        "classify_plan_authorities",
                        return_value=[
                            {
                                key: value
                                for key, value in authority.items()
                                if key != "selection"
                            }
                        ],
                    ),
                    mock.patch.dict(
                        self.recovery.VERIFIERS,
                        {
                            component.distribution: mock.Mock(
                                side_effect=self.recovery.NotFound("not published")
                            )
                        },
                    ),
                ):
                    _state, outputs = self.recovery.resolve_component(
                        mock.Mock(),
                        "workflow",
                        tag,
                        commit,
                        candidate,
                        preparation,
                        authority,
                    )

                self.assertEqual("publish", outputs["action"])

    def test_explicit_completed_plan_verifies_and_skips(self) -> None:
        candidate = lifecycle_plan(self.recovery)
        tag = f"release-plan/{candidate['plan']}"
        commit = "a" * 40
        identity = candidate["components"]["workflow"]
        public_evidence = {"version": identity["version"], "commit": identity["commit"]}
        authority = {
            "selection": "explicit",
            "tag": tag,
            "commit": commit,
            "recorded_at": dt.datetime(2026, 7, 23, tzinfo=dt.UTC),
            "plan": candidate,
            "preparation": None,
            "lifecycle": "completed",
            "successor": None,
        }

        with (
            mock.patch.object(
                self.recovery, "verify_plan_authority", return_value=({}, {})
            ),
            mock.patch.object(
                self.recovery, "resolve_tag", return_value=identity["commit"]
            ),
            mock.patch.object(
                self.recovery, "verify_component", return_value=public_evidence
            ) as verify,
            mock.patch.object(
                self.recovery,
                "classify_plan_authorities",
                return_value=[
                    {
                        key: value
                        for key, value in authority.items()
                        if key != "selection"
                    }
                ],
            ),
        ):
            state, outputs = self.recovery.resolve_component(
                mock.Mock(),
                "workflow",
                tag,
                commit,
                candidate,
                None,
                authority,
            )

        self.assertEqual("skip", outputs["action"])
        self.assertEqual("verified", state["outcome"])
        self.assertEqual(1, verify.call_count)

    def test_explicit_terminal_plan_is_rejected_before_preflight(self) -> None:
        failed = lifecycle_plan(self.recovery)
        failed["plan"] = "failed-plan"
        successor = json.loads(json.dumps(failed))
        successor["plan"] = "successor-plan"
        tag = f"release-plan/{failed['plan']}"
        commit = "a" * 40
        authority = {
            "tag": tag,
            "commit": commit,
            "recorded_at": dt.datetime(2026, 7, 23, tzinfo=dt.UTC),
            "plan": failed,
            "preparation": None,
            "lifecycle": "superseded",
            "successor": {
                "tag": f"release-plan/{successor['plan']}",
                "sha256": self.recovery.manifest_digest(successor),
                "plan": successor,
            },
        }

        with (
            mock.patch.object(
                self.recovery, "classify_plan_authorities", return_value=[authority]
            ),
            self.assertRaisesRegex(
                self.recovery.RecoveryError,
                "terminally superseded and cannot be recovered",
            ),
        ):
            self.recovery.select_explicit_plan_authority(
                mock.Mock(),
                tag,
                commit,
                failed,
                None,
            )

    def test_explicit_terminal_transition_during_preflight_cannot_publish(self) -> None:
        candidate = lifecycle_plan(self.recovery)
        preparation = {
            "components": {
                "workflow": {
                    "release_notes": {
                        "release_date": "2026-07-23",
                        "sha256": "c" * 64,
                        "source": {},
                    }
                }
            }
        }
        tag = f"release-plan/{candidate['plan']}"
        commit = "a" * 40
        component = self.recovery.COMPONENTS["workflow"]
        authority = {
            "selection": "explicit",
            "tag": tag,
            "commit": commit,
            "recorded_at": dt.datetime(2026, 7, 23, tzinfo=dt.UTC),
            "plan": candidate,
            "preparation": preparation,
            "lifecycle": "actionable",
            "successor": None,
        }
        superseded = {
            **authority,
            "lifecycle": "superseded",
            "successor": {
                "tag": "release-plan/successor",
                "sha256": "d" * 64,
                "plan": {"plan": "successor"},
            },
        }
        superseded.pop("selection")
        publication_preflight = mock.Mock(
            side_effect=self.recovery.NotFound("not published")
        )

        with (
            mock.patch.object(
                self.recovery, "verify_plan_authority", return_value=({}, {})
            ),
            mock.patch.object(self.recovery, "validate_release_preparation"),
            mock.patch.object(self.recovery, "resolve_tag", return_value=None),
            mock.patch.object(
                self.recovery, "classify_plan_authorities", return_value=[superseded]
            ),
            mock.patch.dict(
                self.recovery.VERIFIERS,
                {component.distribution: publication_preflight},
            ),
            self.assertRaisesRegex(
                self.recovery.RecoveryError,
                "became terminally superseded during component preflight",
            ),
        ):
            self.recovery.resolve_component(
                mock.Mock(),
                "workflow",
                tag,
                commit,
                candidate,
                preparation,
                authority,
            )

        self.assertEqual(1, publication_preflight.call_count)

    def test_supersession_during_mirror_validation_cannot_return_publish(self) -> None:
        older = lifecycle_plan(self.recovery)
        older["plan"] = "older-plan"
        successor = json.loads(json.dumps(older))
        successor["plan"] = "successor-plan"
        successor["components"]["workflow"]["version"] = "2.0.0-alpha.2"
        older_tag = "release-plan/older-plan"
        successor_tag = "release-plan/successor-plan"
        older_commit = "a" * 40
        successor_commit = "b" * 40
        preparation = {
            "components": {
                "workflow": {
                    "release_notes": {
                        "release_date": "2026-07-23",
                        "sha256": "c" * 64,
                        "source": {},
                    }
                }
            }
        }
        recorded_at = dt.datetime(2026, 7, 20, tzinfo=dt.UTC)
        older_authority = {
            "tag": older_tag,
            "commit": older_commit,
            "recorded_at": recorded_at,
            "plan": older,
            "preparation": preparation,
            "lifecycle": "actionable",
            "successor": None,
        }
        successor_authority = {
            "tag": successor_tag,
            "commit": successor_commit,
            "recorded_at": recorded_at + dt.timedelta(days=1),
            "plan": successor,
            "preparation": preparation,
            "lifecycle": "actionable",
            "successor": None,
        }
        superseded_older_authority = {
            **older_authority,
            "lifecycle": "superseded",
            "successor": {
                "tag": successor_tag,
                "sha256": self.recovery.manifest_digest(successor),
                "plan": successor,
            },
        }
        superseded = False
        classifications = 0
        mirror_validations = 0

        def classify(
            _client: mock.Mock,
        ) -> tuple[dict[str, object], list[dict[str, object]]]:
            nonlocal classifications
            classifications += 1
            if superseded:
                return successor_authority, [
                    superseded_older_authority,
                    successor_authority,
                ]
            return older_authority, [older_authority]

        def validate_mirrors(
            _client: mock.Mock,
            tag: str,
            _release: object,
            _plan: dict[str, object],
            _preparation: dict[str, object],
        ) -> None:
            nonlocal mirror_validations, superseded
            mirror_validations += 1
            self.assertEqual(older_tag, tag)
            if mirror_validations == 1:
                self.assertEqual(2, classifications)
                superseded = True

        def resolve_tag(
            _client: mock.Mock,
            repository: str,
            tag: str,
        ) -> str | None:
            if repository == self.recovery.CONTROL_REPOSITORY and tag == older_tag:
                return older_commit
            return None

        client = mock.Mock()
        client.json.return_value = {}
        with (
            mock.patch.object(
                self.recovery,
                "classify_implicit_plan_authority",
                side_effect=classify,
            ),
            mock.patch.object(
                self.recovery,
                "classify_plan_authorities",
                return_value=[
                    superseded_older_authority,
                    successor_authority,
                ],
            ),
            mock.patch.object(
                self.recovery,
                "validate_release_mirrors",
                side_effect=validate_mirrors,
            ),
            mock.patch.object(
                self.recovery,
                "read_plan_authority",
                return_value=(older, preparation),
            ),
            mock.patch.object(
                self.recovery,
                "verify_plan_authority",
                return_value=({}, {}),
            ),
            mock.patch.object(self.recovery, "validate_release_preparation"),
            mock.patch.object(
                self.recovery,
                "resolve_tag",
                side_effect=resolve_tag,
            ),
            mock.patch.dict(
                self.recovery.VERIFIERS,
                {
                    "composer": mock.Mock(
                        side_effect=self.recovery.NotFound("not published")
                    )
                },
            ),
        ):
            (
                tag,
                record_commit,
                plan,
                selected_preparation,
                implicit_authority,
            ) = self.recovery.discover_plan(client, None, "workflow")
            with self.assertRaisesRegex(
                self.recovery.RecoveryError,
                "authority changed during component preflight",
            ):
                self.recovery.resolve_component(
                    client,
                    "workflow",
                    tag,
                    record_commit,
                    plan,
                    selected_preparation,
                    implicit_authority,
                )

            with self.assertRaisesRegex(
                self.recovery.RecoveryError,
                "terminally superseded and cannot be recovered",
            ):
                self.recovery.discover_plan(client, older_tag, "workflow")

        self.assertTrue(superseded)
        self.assertEqual(3, classifications)
        self.assertEqual(1, mirror_validations)

    def test_interrupted_plan_rejects_multiple_continuity_successors(self) -> None:
        interrupted = lifecycle_plan(self.recovery)
        interrupted["plan"] = "interrupted-plan"
        first_successor = json.loads(json.dumps(interrupted))
        first_successor["plan"] = "first-successor"
        second_successor = json.loads(json.dumps(interrupted))
        second_successor["plan"] = "second-successor"
        tags = [
            f"release-plan/{interrupted['plan']}",
            f"release-plan/{first_successor['plan']}",
            f"release-plan/{second_successor['plan']}",
        ]
        plans_by_tag = dict(zip(tags, [interrupted, first_successor, second_successor], strict=True))
        commits = {
            tags[0]: "a" * 40,
            tags[1]: "b" * 40,
            tags[2]: "c" * 40,
        }
        interruption_tag = f"{self.recovery.CONTINUITY_TAG_PREFIX}{interrupted['plan']}/interrupted"
        interruption_commit = "d" * 40
        interruption_evidence = {"phase": "interrupted"}
        superseded_interruption = {
            "tag": interruption_tag,
            "commit": interruption_commit,
            "evidence_sha256": self.recovery.manifest_digest(interruption_evidence),
            "plan_sha256": self.recovery.manifest_digest(interrupted),
            "reason": self.recovery.CONTINUITY_SUPERSESSION_REASON,
        }
        orderings = [
            (
                tags,
                {
                    commits[tags[0]]: dt.datetime(2026, 7, 20, tzinfo=dt.UTC),
                    commits[tags[1]]: dt.datetime(2026, 7, 21, tzinfo=dt.UTC),
                    commits[tags[2]]: dt.datetime(2026, 7, 22, tzinfo=dt.UTC),
                },
            ),
            (
                list(reversed(tags)),
                {
                    commits[tags[0]]: dt.datetime(2026, 7, 20, tzinfo=dt.UTC),
                    commits[tags[1]]: dt.datetime(2026, 7, 22, tzinfo=dt.UTC),
                    commits[tags[2]]: dt.datetime(2026, 7, 21, tzinfo=dt.UTC),
                },
            ),
        ]

        for discovered_tags, recorded in orderings:
            with (
                self.subTest(tags=discovered_tags, recorded=recorded),
                mock.patch.object(
                    self.recovery,
                    "list_release_plan_tags",
                    return_value=discovered_tags,
                ),
                mock.patch.object(
                    self.recovery,
                    "resolve_tag",
                    side_effect=lambda _client, _repository, tag: (
                        interruption_commit if tag == interruption_tag else commits[tag]
                    ),
                ),
                mock.patch.object(
                    self.recovery,
                    "read_plan_authority",
                    side_effect=lambda _client, tag, _commit: (plans_by_tag[tag], None),
                ),
                mock.patch.object(
                    self.recovery,
                    "direct_plan_lifecycle",
                    side_effect=lambda _client, tag, *_args: (
                        ("interrupted", interruption_tag) if tag == tags[0] else ("completed", None)
                    ),
                ),
                mock.patch.object(
                    self.recovery,
                    "immutable_plan_recorded_at",
                    side_effect=lambda _client, commit, recorded=recorded: recorded[commit],
                ),
                mock.patch.object(
                    self.recovery,
                    "accepted_continuity_supersession",
                    side_effect=lambda _client, authority: (
                        None if authority["tag"] == tags[0] else superseded_interruption
                    ),
                ),
                mock.patch.object(
                    self.recovery,
                    "list_continuity_resolution_tags",
                    return_value=[],
                ),
                mock.patch.object(
                    self.recovery,
                    "read_record",
                    return_value=interruption_evidence,
                ),
                self.assertRaisesRegex(
                    self.recovery.RecoveryError,
                    "multiple continuity successors",
                ),
            ):
                self.recovery.select_implicit_plan_authority(mock.Mock())

    def test_continuity_successor_fork_accepts_exact_digest_bound_resolution(self) -> None:
        interrupted_plan = {"plan": "interrupted"}
        interrupted = {
            "tag": "release-plan/interrupted",
            "commit": "a" * 40,
            "plan": interrupted_plan,
        }
        interruption = {
            "tag": "beta-continuity/interrupted/interrupted",
            "commit": "b" * 40,
            "evidence_sha256": "c" * 64,
        }
        successors = []
        for index, name in enumerate(("first-successor", "second-successor"), start=1):
            successors.append(
                {
                    "tag": f"release-plan/{name}",
                    "supersession": {
                        **interruption,
                        "continuity_claim": {
                            "plan": {
                                "tag": f"release-plan/{name}",
                                "commit": str(index) * 40,
                                "sha256": str(index + 2) * 64,
                            },
                            "acceptance": {
                                "tag": f"beta-continuity/{name}/accepted",
                                "commit": str(index + 4) * 40,
                                "sha256": str(index + 6) * 64,
                            },
                        },
                    },
                }
            )
        claims = [successor["supersession"]["continuity_claim"] for successor in successors]
        resolution = {
            "schema": self.recovery.CONTINUITY_RESOLUTION_SCHEMA,
            "qualification": continuity_resolution_qualification(),
            "interruption": {
                "plan": {
                    "tag": interrupted["tag"],
                    "commit": interrupted["commit"],
                    "sha256": self.recovery.manifest_digest(interrupted_plan),
                },
                "evidence": {
                    "tag": interruption["tag"],
                    "commit": interruption["commit"],
                    "sha256": interruption["evidence_sha256"],
                },
            },
            "successor_claims": claims,
            "selected_successor": claims[1]["plan"],
        }
        resolution_tag = (
            f"{self.recovery.CONTINUITY_RESOLUTION_TAG_PREFIX}interrupted/"
            f"{self.recovery.manifest_digest(resolution)}"
        )
        client = mock.Mock()
        client.json.return_value = continuity_resolution_qualification_run()
        with (
            mock.patch.object(
                self.recovery,
                "list_continuity_resolution_tags",
                return_value=[resolution_tag],
            ),
            mock.patch.object(self.recovery, "resolve_tag", return_value="f" * 40),
            mock.patch.object(self.recovery, "read_record", return_value=resolution),
        ):
            selected = self.recovery.resolve_continuity_successor_fork(
                client,
                interrupted,
                successors,
            )
        self.assertEqual("release-plan/second-successor", selected)
        valid_run = continuity_resolution_qualification_run()
        failures = (
            (None, "qualification is absent"),
            ({**valid_run, "status": "in_progress", "conclusion": None}, "qualification is pending"),
            ({**valid_run, "conclusion": "failure"}, "qualification failed"),
            ({**valid_run, "conclusion": "cancelled"}, "qualification was cancelled"),
            ({**valid_run, "head_sha": "8" * 40}, "another source revision"),
            ({**valid_run, "path": ".github/workflows/untrusted.yml@main"}, "untrusted workflow"),
        )
        with (
            mock.patch.object(self.recovery, "list_continuity_resolution_tags", return_value=[resolution_tag]),
            mock.patch.object(self.recovery, "resolve_tag", return_value="f" * 40),
            mock.patch.object(self.recovery, "read_record", return_value=resolution),
        ):
            for run, message in failures:
                with self.subTest(qualification=message):
                    client.json.return_value = run
                    with self.assertRaisesRegex(self.recovery.RecoveryError, message):
                        self.recovery.resolve_continuity_successor_fork(client, interrupted, successors)

    def test_terminal_failure_successor_requires_exact_authorized_plan_identity(self) -> None:
        failed = lifecycle_plan(self.recovery)
        failed["plan"] = "failed-plan"
        authorized_successor = json.loads(json.dumps(failed))
        authorized_successor["plan"] = "successor-plan"
        authorized_successor["components"]["workflow"]["version"] = "2.0.0-alpha.2"
        recorded_successor = json.loads(json.dumps(authorized_successor))
        recorded_successor["components"]["workflow"]["commit"] = "e" * 40
        failed_tag = f"release-plan/{failed['plan']}"
        successor_tag = f"release-plan/{authorized_successor['plan']}"
        failed_commit = "a" * 40
        successor_commit = "b" * 40
        failure_commit = "c" * 40
        failure = supersession_record(
            self.recovery,
            failed,
            authorized_successor,
            failed_commit,
        )

        with (
            mock.patch.object(
                self.recovery,
                "resolve_tag",
                side_effect=[None, failure_commit],
            ),
            mock.patch.object(
                self.recovery,
                "read_record",
                side_effect=[failure, authorized_successor],
            ),
            mock.patch.object(self.recovery, "revalidate_supersession_authority"),
        ):
            lifecycle, successor_identity = self.recovery.direct_plan_lifecycle(
                mock.Mock(),
                failed_tag,
                failed_commit,
                failed,
                None,
            )

        self.assertEqual("superseded", lifecycle)
        self.assertEqual(
            {
                "tag": successor_tag,
                "sha256": self.recovery.manifest_digest(authorized_successor),
                "plan": authorized_successor,
            },
            successor_identity,
        )

        commits = {failed_tag: failed_commit, successor_tag: successor_commit}
        recorded = {
            failed_commit: dt.datetime(2026, 7, 20, tzinfo=dt.UTC),
            successor_commit: dt.datetime(2026, 7, 21, tzinfo=dt.UTC),
        }
        with (
            mock.patch.object(
                self.recovery,
                "list_release_plan_tags",
                return_value=[failed_tag, successor_tag],
            ),
            mock.patch.object(
                self.recovery,
                "resolve_tag",
                side_effect=lambda _client, _repository, tag: commits[tag],
            ),
            mock.patch.object(
                self.recovery,
                "read_plan_authority",
                side_effect=[(failed, None), (recorded_successor, None)],
            ),
            mock.patch.object(
                self.recovery,
                "direct_plan_lifecycle",
                side_effect=[
                    (lifecycle, successor_identity),
                    ("completed", None),
                ],
            ),
            mock.patch.object(
                self.recovery,
                "immutable_plan_recorded_at",
                side_effect=lambda _client, commit: recorded[commit],
            ),
            mock.patch.object(
                self.recovery,
                "accepted_continuity_supersession",
                return_value=None,
            ),
            self.assertRaisesRegex(
                self.recovery.RecoveryError,
                "conflicting successor identity",
            ),
        ):
            self.recovery.select_implicit_plan_authority(mock.Mock())

    def test_terminal_failure_accepts_captured_github_authority(self) -> None:
        failed, successor, failed_commit, failure = supersession_failure_fixture(self.recovery)
        failed_tag = f"release-plan/{failed['plan']}"
        client = mock.Mock()
        client.json.side_effect = captured_supersession_github_responses(self.recovery)

        with (
            mock.patch.object(
                self.recovery,
                "resolve_tag",
                side_effect=[None, "c" * 40],
            ),
            mock.patch.object(
                self.recovery,
                "read_record",
                side_effect=[failure, successor],
            ),
        ):
            lifecycle, successor_identity = self.recovery.direct_plan_lifecycle(
                client,
                failed_tag,
                failed_commit,
                failed,
                None,
            )

        self.assertEqual("superseded", lifecycle)
        self.assertEqual(successor, successor_identity["plan"])
        self.assertEqual(4, client.json.call_count)

    def test_supersession_authority_rejects_fabricated_environment_identity(self) -> None:
        _, _, _, failure = supersession_failure_fixture(self.recovery)
        authorization = failure["authorization"]
        authorization["environment_protection"]["environment_id"] = 99
        authorization["environment_approval"]["environments"][0]["id"] = 99
        client = mock.Mock()
        client.json.side_effect = captured_supersession_github_responses(self.recovery)

        with self.assertRaisesRegex(
            self.recovery.RecoveryError,
            "protected environment policy no longer matches GitHub",
        ):
            self.recovery.revalidate_supersession_authority(failure, client)

    def test_supersession_authority_rejects_fabricated_or_stale_run_identity(self) -> None:
        cases = {
            "fabricated run id": {
                "run_id": 999,
                "run_url": "https://github.com/durable-workflow/.github/actions/runs/999",
            },
            "stale run attempt": {"run_attempt": 2},
            "mismatched workflow revision": {"workflow_commit": "d" * 40},
        }
        for name, changes in cases.items():
            with self.subTest(name):
                _, _, _, failure = supersession_failure_fixture(self.recovery)
                authorization = failure["authorization"]
                authorization.update(changes)
                authorization["environment_approval"]["run_id"] = authorization["run_id"]
                authorization["environment_approval"]["run_attempt"] = authorization["run_attempt"]
                client = mock.Mock()
                client.json.side_effect = captured_supersession_github_responses(self.recovery)[2:]

                with self.assertRaisesRegex(
                    self.recovery.RecoveryError,
                    "workflow run evidence does not match GitHub",
                ):
                    self.recovery.protected_run_approval_evidence(
                        client,
                        authorization,
                        authorization["environment_protection"],
                        {(55, "release-reviewer")},
                    )

    def test_supersession_authority_rejects_mismatched_run_contract(self) -> None:
        cases = {
            "workflow": {"path": ".github/workflows/other.yml"},
            "branch": {"head_branch": "v2"},
            "conclusion": {"conclusion": "failure"},
            "actor": {"actor": captured_github_user("other-operator", 56, "other-operator-node")},
        }
        for name, changes in cases.items():
            with self.subTest(name):
                _, _, _, failure = supersession_failure_fixture(self.recovery)
                responses = captured_supersession_github_responses(self.recovery)
                responses[2].update(changes)
                client = mock.Mock()
                client.json.side_effect = responses[2:]

                with self.assertRaisesRegex(
                    self.recovery.RecoveryError,
                    "workflow run evidence does not match GitHub",
                ):
                    self.recovery.protected_run_approval_evidence(
                        client,
                        failure["authorization"],
                        failure["authorization"]["environment_protection"],
                        {(55, "release-reviewer")},
                    )

    def test_supersession_authority_rejects_approval_history_for_a_rerun_attempt(self) -> None:
        _, _, _, failure = supersession_failure_fixture(self.recovery)
        authorization = failure["authorization"]
        authorization["run_attempt"] = 2
        authorization["environment_approval"]["run_attempt"] = 2
        responses = captured_supersession_github_responses(self.recovery)
        responses[2]["run_attempt"] = 2
        client = mock.Mock()
        client.json.side_effect = responses

        with self.assertRaisesRegex(
            self.recovery.RecoveryError,
            "approval history cannot bind.*rerun attempt",
        ):
            self.recovery.revalidate_supersession_authority(failure, client)
        self.assertEqual(3, client.json.call_count)

    def test_supersession_authority_rejects_approver_outside_current_policy(self) -> None:
        _, _, _, failure = supersession_failure_fixture(self.recovery)
        responses = captured_supersession_github_responses(self.recovery)
        responses[0]["protection_rules"][0]["reviewers"][0]["reviewer"] = captured_github_user(
            "different-reviewer",
            77,
            "different-reviewer-node",
        )
        client = mock.Mock()
        client.json.side_effect = responses

        with self.assertRaisesRegex(
            self.recovery.RecoveryError,
            "approving user is not authorized by the current reviewer policy",
        ):
            self.recovery.revalidate_supersession_authority(failure, client)
        self.assertEqual(4, client.json.call_count)

    def test_supersession_authority_rejects_fabricated_reviewer_identity(self) -> None:
        _, _, _, failure = supersession_failure_fixture(self.recovery)
        failure["authorization"]["environment_approval"]["user"]["id"] = 99
        client = mock.Mock()
        client.json.side_effect = captured_supersession_github_responses(self.recovery)

        with self.assertRaisesRegex(
            self.recovery.RecoveryError,
            "approved deployment evidence no longer matches GitHub",
        ):
            self.recovery.revalidate_supersession_authority(failure, client)

    def test_supersession_authority_rejects_stale_or_mismatched_approval(self) -> None:
        cases = {
            "stale comment": (
                "record",
                "comment",
                "previous approval",
                "approved deployment evidence no longer matches GitHub",
            ),
            "mismatched environment": (
                "response",
                "environment_id",
                99,
                "approval names the wrong protected environment",
            ),
            "rejected decision": (
                "response",
                "state",
                "rejected",
                "must contain exactly one approved review",
            ),
            "missing decision": (
                "response",
                "history",
                [],
                "must contain exactly one approved review",
            ),
        }
        for name, (target, field, value, error) in cases.items():
            with self.subTest(name):
                _, _, _, failure = supersession_failure_fixture(self.recovery)
                responses = captured_supersession_github_responses(self.recovery)
                if target == "record":
                    failure["authorization"]["environment_approval"][field] = value
                elif field == "environment_id":
                    responses[3][0]["environments"][0]["id"] = value
                elif field == "history":
                    responses[3] = value
                else:
                    responses[3][0][field] = value
                client = mock.Mock()
                client.json.side_effect = responses

                with self.assertRaisesRegex(self.recovery.RecoveryError, error):
                    self.recovery.revalidate_supersession_authority(failure, client)

    def test_terminal_failure_rejects_incomplete_lifecycle_authority(self) -> None:
        failed = lifecycle_plan(self.recovery)
        failed["plan"] = "failed-plan"
        successor = json.loads(json.dumps(failed))
        successor["plan"] = "successor-plan"
        successor["components"]["workflow"]["version"] = "2.0.0-alpha.2"
        failed_tag = f"release-plan/{failed['plan']}"
        failed_commit = "a" * 40
        incomplete = {
            "schema": "durable-workflow.release-plan-failure/v1",
            "outcome": "terminal-failure",
            "failed_plan": {
                "tag": failed_tag,
                "commit": failed_commit,
                "sha256": self.recovery.manifest_digest(failed),
            },
            "successor_plan": {
                "tag": f"release-plan/{successor['plan']}",
                "sha256": self.recovery.manifest_digest(successor),
            },
        }

        with (
            mock.patch.object(
                self.recovery,
                "resolve_tag",
                side_effect=[None, "c" * 40],
            ),
            mock.patch.object(
                self.recovery,
                "read_record",
                side_effect=[incomplete, successor],
            ),
            self.assertRaisesRegex(
                self.recovery.RecoveryError,
                "record keys must be exactly",
            ),
        ):
            self.recovery.direct_plan_lifecycle(
                mock.Mock(),
                failed_tag,
                failed_commit,
                failed,
                None,
            )


class ReleasePreparationRecoveryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.recovery = load_recovery_module()

    def candidate(self) -> dict[str, object]:
        return {
            "plan": "missing-preparation",
            "channel": "alpha",
            "components": {"workflow": {"version": "2.0.0-alpha.1", "commit": "a" * 40}},
        }

    def test_discovery_rejects_missing_preparation_for_an_incomplete_release(self) -> None:
        candidate = self.candidate()
        tag = "release-plan/missing-preparation"
        record_commit = "b" * 40
        client = mock.Mock()
        client.json.return_value = {
            "tag_name": tag,
            "draft": False,
            "assets": [
                {
                    "name": "release-plan.json",
                    "browser_download_url": "https://example.invalid/release-plan.json",
                }
            ],
        }
        client.bytes.return_value = self.recovery.canonical_json(candidate)
        with (
            mock.patch.object(self.recovery, "validate_plan"),
            mock.patch.object(self.recovery, "resolve_tag", return_value=record_commit),
            mock.patch.object(
                self.recovery,
                "select_explicit_plan_authority",
                return_value={"selection": "explicit"},
            ),
            mock.patch.object(
                self.recovery,
                "read_record",
                side_effect=[candidate, self.recovery.NotFound("missing preparation")],
            ),
            mock.patch.object(
                self.recovery,
                "verify_component",
                side_effect=self.recovery.NotFound("release is incomplete"),
            ),
            self.assertRaisesRegex(self.recovery.RecoveryError, "only completed legacy releases"),
        ):
            self.recovery.discover_plan(client, tag, "workflow")

    def test_missing_preparation_cannot_resolve_to_publish(self) -> None:
        candidate = self.candidate()
        with (
            mock.patch.object(self.recovery, "verify_plan_authority", return_value=({}, {})),
            mock.patch.object(self.recovery, "resolve_tag", return_value=None),
            self.assertRaisesRegex(
                self.recovery.RecoveryError,
                "release preparation required before publishing workflow",
            ),
        ):
            self.recovery.resolve_component(
                mock.Mock(),
                "workflow",
                "release-plan/missing-preparation",
                "b" * 40,
                candidate,
                None,
            )

    def test_completed_legacy_release_still_resolves_to_skip(self) -> None:
        candidate = self.candidate()
        identity = candidate["components"]["workflow"]
        public_evidence = {"version": identity["version"], "commit": identity["commit"]}
        with (
            mock.patch.object(self.recovery, "verify_plan_authority", return_value=({}, {})),
            mock.patch.object(self.recovery, "resolve_tag", return_value=identity["commit"]),
            mock.patch.object(self.recovery, "verify_component", return_value=public_evidence),
        ):
            state, outputs = self.recovery.resolve_component(
                mock.Mock(),
                "workflow",
                "release-plan/missing-preparation",
                "b" * 40,
                candidate,
                None,
            )

        self.assertEqual("skip", outputs["action"])
        self.assertEqual("complete", state["phase"])
        self.assertNotIn("release_preparation", state)


class RecoveryWorkflowSourceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.recovery = load_recovery_module()

    def assert_rejected(self, source: str) -> None:
        with self.assertRaises(self.recovery.RecoveryError) as caught:
            self.recovery.verify_recovery_workflow_source(
                "sdk-rust",
                source,
                hashlib.sha256(CURRENT_RUST_RECOVERY_WORKFLOW.encode("utf-8")).hexdigest(),
            )
        self.assertEqual(caught.exception.phase, "default-branch-preflight")

    def test_accepts_only_the_current_protected_rust_workflow_identity(self) -> None:
        digest = hashlib.sha256(CURRENT_RUST_RECOVERY_WORKFLOW.encode("utf-8")).hexdigest()
        self.recovery.verify_recovery_workflow_source("sdk-rust", CURRENT_RUST_RECOVERY_WORKFLOW, digest)
        self.recovery.verify_recovery_workflow_source(
            "sdk-rust",
            CURRENT_RUST_RECOVERY_WORKFLOW.replace("\n", "\r\n"),
            digest,
        )

    def test_rejects_shell_semantic_bypasses_and_any_source_mutation(self) -> None:
        source = CURRENT_RUST_RECOVERY_WORKFLOW
        variants = {
            "one-byte mutation": source.replace("timeout-minutes: 30", "timeout-minutes: 31", 1),
            "one-line mutation": source + "\n",
            "readarray release tag mutation": source.replace(
                "          select_publication_run() {",
                "          readarray -t release_identity < <(printf '%s\\n' mutable)\n"
                '          RELEASE_TAG="${release_identity[0]}"\n\n'
                "          select_publication_run() {",
                1,
            ),
            "successful early exit": source.replace(
                "          python scripts/ci/publish-planned-tag.py \\",
                "          exit 0\n          python scripts/ci/publish-planned-tag.py \\",
                1,
            ),
            "shadowed gh command": source.replace(
                "          set -euo pipefail",
                "          set -euo pipefail\n          gh() { printf 'shadowed\\n'; }",
                1,
            ),
        }

        for label, variant in variants.items():
            with self.subTest(label):
                self.assertNotEqual(variant, source)
                self.assert_rejected(variant)

    def test_rejects_skipped_nonblocking_or_decoy_scoped_steps(self) -> None:
        source = CURRENT_RUST_RECOVERY_WORKFLOW
        tag_step = "      - name: Create or verify the exact planned source tag"
        publication_step = "      - name: Start or resume repository-owned publication"
        completion_step = "      - name: Verify crates.io source identity and the GitHub Release"
        exact_bindings = """          RELEASE_TAG: ${{ needs.discover.outputs.version }}
          RELEASE_COMMIT: ${{ needs.discover.outputs.commit }}"""
        decoy_step = f"""      - name: Unrelated release identity
        env:
{exact_bindings}
        run: echo "release identity is not consumed here"

"""
        mutable_tag_bindings = source.replace(
            exact_bindings,
            """          RELEASE_TAG: ${{ github.ref_name }}
          RELEASE_COMMIT: ${{ github.sha }}""",
            1,
        ).replace(tag_step, decoy_step + tag_step, 1)
        publication_env = """        env:
          GH_TOKEN: ${{ github.token }}
          PLAN_TAG: ${{ needs.discover.outputs.plan_tag }}
          RELEASE_TAG: ${{ needs.discover.outputs.version }}
          RELEASE_COMMIT: ${{ needs.discover.outputs.commit }}"""
        mutable_selector_bindings = source.replace(
            publication_env,
            """        env:
          GH_TOKEN: ${{ github.token }}
          PLAN_TAG: ${{ needs.discover.outputs.plan_tag }}
          RELEASE_TAG: ${{ github.ref_name }}
          RELEASE_COMMIT: ${{ github.sha }}""",
            1,
        ).replace(
            "      - name: Start or resume repository-owned publication",
            decoy_step + "      - name: Start or resume repository-owned publication",
            1,
        )
        variants = {
            "tag publication skipped": source.replace(
                tag_step,
                tag_step + "\n        if: ${{ false }}",
                1,
            ),
            "tag publication nonblocking even when false": source.replace(
                tag_step,
                tag_step + "\n        continue-on-error: false",
                1,
            ),
            "tag publication expression-enabled nonblocking": source.replace(
                tag_step,
                tag_step + "\n        continue-on-error: ${{ github.ref_name != '' }}",
                1,
            ),
            "tag publication uses a nonblocking shell": source.replace(
                tag_step,
                tag_step + "\n        shell: bash {0} || true",
                1,
            ),
            "publication selection skipped": source.replace(
                publication_step,
                publication_step + "\n        if: ${{ false }}",
                1,
            ),
            "completion verification skipped": source.replace(
                completion_step,
                completion_step + "\n        if: ${{ false }}",
                1,
            ),
            "completion verification nonblocking": source.replace(
                completion_step,
                completion_step + "\n        continue-on-error: true",
                1,
            ),
            "completion verification expression-enabled nonblocking": source.replace(
                completion_step,
                completion_step + "\n        continue-on-error: ${{ failure() }}",
                1,
            ),
            "tag bindings moved to an unrelated step": mutable_tag_bindings,
            "selector bindings moved to an unrelated step": mutable_selector_bindings,
            "checkout adds repository-token authority": source.replace(
                "          fetch-depth: 0",
                "          fetch-depth: 0\n          token: ${{ github.token }}",
                1,
            ),
            "run identity includes an unapproved field": source.replace(
                "databaseId,event,displayTitle,headBranch,headSha,status,conclusion",
                "databaseId,event,displayTitle,headBranch,headSha,status,conclusion,url",
                1,
            ),
        }

        for label, variant in variants.items():
            with self.subTest(label):
                self.assertNotEqual(variant, source)
                self.assert_rejected(variant)

    def test_rejects_weakened_or_mismatched_rust_publication_shapes(self) -> None:
        source = CURRENT_RUST_RECOVERY_WORKFLOW
        publisher = r"""          python scripts/ci/publish-planned-tag.py \
            --tag "$RELEASE_TAG" --commit "$RELEASE_COMMIT" --plan-tag "$PLAN_TAG" \
            --evidence release-tag-publication-evidence.json"""
        deferred_publisher = source.replace(publisher, "          echo tag-publication-deferred", 1).replace(
            "      - name: Verify crates.io source identity and the GitHub Release",
            "      - name: Deferred source tag publication\n"
            "        run: |\n"
            f"{publisher}\n\n"
            "      - name: Verify crates.io source identity and the GitHub Release",
            1,
        )
        repository_token_creation = source.replace(
            "          python scripts/ci/publish-planned-tag.py",
            "          gh api --method POST \"repos/$GITHUB_REPOSITORY/git/refs\"\n"
            "          python scripts/ci/publish-planned-tag.py",
            1,
        )
        misplaced_deploy_key = source.replace(
            "          ssh-key: ${{ secrets.RELEASE_PLAN_DEPLOY_KEY }}",
            "          env:\n"
            "            UNUSED_DEPLOY_KEY: ${{ secrets.RELEASE_PLAN_DEPLOY_KEY }}",
            1,
        )
        dormant_publisher = source.replace(
            publisher,
            "          publish_planned_tag() {\n"
            + "\n".join(f"  {line}" for line in publisher.splitlines())
            + "\n          }",
            1,
        )
        reassigned_tag = source.replace(
            "          python scripts/ci/publish-planned-tag.py",
            '          RELEASE_TAG="$GITHUB_REF_NAME"\n'
            "          python scripts/ci/publish-planned-tag.py",
            1,
        )
        nonblocking_verification = source.replace(
            "--attempts 6 --sleep 10 --evidence release-completion-evidence.json",
            "--attempts 6 --sleep 10 --evidence release-completion-evidence.json || true",
            1,
        )
        variants = {
            "missing protected environment": source.replace(
                "environment: release-plan-publication", "environment: unprotected", 1
            ),
            "missing deploy key": source.replace(
                "secrets.RELEASE_PLAN_DEPLOY_KEY", "secrets.UNPROTECTED_KEY", 1
            ),
            "deploy key only in unrelated env": misplaced_deploy_key,
            "tag publisher defined but not executed": dormant_publisher,
            "release tag reassigned before publication": reassigned_tag,
            "public verification made nonblocking": nonblocking_verification,
            "tag publication after dispatch": deferred_publisher,
            "mutable tag publisher argument": source.replace(
                '--tag "$RELEASE_TAG"', '--tag "$GITHUB_REF_NAME"', 1
            ),
            "mismatched tag publisher commit": source.replace(
                '--commit "$RELEASE_COMMIT"', '--commit "$GITHUB_SHA"', 1
            ),
            "mutable planned tag binding": source.replace(
                "needs.discover.outputs.version", "github.ref_name"
            ),
            "mutable planned commit binding": source.replace(
                "needs.discover.outputs.commit", "github.sha"
            ),
            "different selected workflow": source.replace(
                "gh run list --workflow release.yml", "gh run list --workflow nightly.yml", 1
            ),
            "different dispatched workflow": source.replace(
                "gh workflow run release.yml", "gh workflow run nightly.yml", 1
            ),
            "incomplete run identity": source.replace(
                "headBranch,headSha,status", "headBranch,status", 1
            ),
            "mismatched selector tag": source.replace(
                '--release-tag "$RELEASE_TAG"', '--release-tag "$GITHUB_REF_NAME"', 1
            ),
            "mismatched selector commit": source.replace(
                '--release-commit "$RELEASE_COMMIT"', '--release-commit "$GITHUB_SHA"', 1
            ),
            "mismatched dispatch tag": source.replace(
                '-f release_tag="$RELEASE_TAG"', '-f release_tag="$GITHUB_REF_NAME"', 1
            ),
            "missing completed release verification": source.replace(
                "--component sdk-rust --plan recovery-input/release-plan.json",
                "--component sdk-rust --plan mutable-release-plan.json",
                1,
            ),
            "broad contents permission": source.replace("contents: read", "contents: write", 1),
            "repository token tag creation": repository_token_creation,
        }

        for label, variant in variants.items():
            with self.subTest(label):
                self.assertNotEqual(variant, source)
                self.assert_rejected(variant)

    def test_other_components_keep_the_contents_api_contract(self) -> None:
        expected_sha256 = hashlib.sha256(GENERIC_RECOVERY_WORKFLOW.encode("utf-8")).hexdigest()
        self.recovery.verify_recovery_workflow_source(
            "server", GENERIC_RECOVERY_WORKFLOW, expected_sha256
        )

        protected_only = GENERIC_RECOVERY_WORKFLOW.replace(
            '-f ref="refs/tags/$RELEASE_TAG" -f sha="$RELEASE_COMMIT"',
            'python scripts/ci/publish-planned-tag.py --tag "$RELEASE_TAG" --commit "$RELEASE_COMMIT"',
        )
        with self.assertRaises(self.recovery.RecoveryError):
            self.recovery.verify_recovery_workflow_source("server", protected_only, expected_sha256)


if __name__ == "__main__":
    unittest.main()
