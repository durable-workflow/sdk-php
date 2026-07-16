#!/usr/bin/env python3
"""Exercise planned source publication across Git and HTTP boundaries."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
import unittest
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

REPOSITORY = "durable-workflow/sdk-php"
TAG = "0.1.9"
PLAN_TAG = "release-plan/source-recovery-boundary"
TOKEN = "fixture-actions-token-that-must-not-appear"
SCRIPT = Path(__file__).with_name("publish-planned-source.py")
SSH_REMOTE = f"git@github.com:{REPOSITORY}.git"


def run(*arguments: str, environment: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        list(arguments),
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"command failed: {arguments!r}\n{result.stdout}\n{result.stderr}")
    return result


class GitHubFixture:
    def __init__(self, remote: Path, planned_commit: str) -> None:
        self.remote = remote
        self.planned_commit = planned_commit
        self.release: dict[str, Any] | None = None
        self.permission_failure = False
        self.release_posts = 0
        self.release_payloads: list[dict[str, Any]] = []
        self.ref_visibility_misses = 0

    def resolve_tag(self) -> str | None:
        result = subprocess.run(
            ["git", "--git-dir", str(self.remote), "rev-parse", "--verify", f"refs/tags/{TAG}^{{commit}}"],
            text=True,
            capture_output=True,
            check=False,
        )
        return result.stdout.strip() if result.returncode == 0 else None

    def set_tag(self, commit: str) -> None:
        run("git", "--git-dir", str(self.remote), "update-ref", f"refs/tags/{TAG}", commit)

    def reject_pushes(self) -> None:
        hook = self.remote / "hooks" / "pre-receive"
        hook.write_text("#!/bin/sh\necho 'repository deploy key is read only' >&2\nexit 1\n")
        hook.chmod(0o755)


class Handler(BaseHTTPRequestHandler):
    server: "FixtureServer"

    def log_message(self, _format: str, *_args: object) -> None:
        pass

    def send_json(
        self,
        status: int,
        payload: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        expected_ref = f"/repos/{REPOSITORY}/git/ref/tags/{urllib.parse.quote(TAG, safe='')}"
        expected_release = f"/repos/{REPOSITORY}/releases/tags/{urllib.parse.quote(TAG, safe='')}"
        if self.path == expected_ref:
            commit = self.server.fixture.resolve_tag()
            if commit is not None and self.server.fixture.ref_visibility_misses > 0:
                self.server.fixture.ref_visibility_misses -= 1
                commit = None
            if commit is None:
                self.send_json(404, {"message": "Not Found"})
            else:
                self.send_json(
                    200,
                    {"ref": f"refs/tags/{TAG}", "object": {"type": "commit", "sha": commit}},
                )
            return
        if self.path == expected_release:
            if self.server.fixture.release is None:
                self.send_json(404, {"message": "Not Found"})
            else:
                self.send_json(200, self.server.fixture.release)
            return
        self.send_json(404, {"message": "Not Found"})

    def do_POST(self) -> None:
        if self.path != f"/repos/{REPOSITORY}/releases":
            self.send_json(404, {"message": "Not Found"})
            return
        self.server.fixture.release_posts += 1
        if self.headers.get("Authorization") != f"Bearer {TOKEN}":
            self.send_json(401, {"message": "Bad credentials"})
            return
        length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(length))
        self.server.fixture.release_payloads.append(payload)
        if self.server.fixture.permission_failure:
            self.send_json(
                403,
                {"message": "Resource not accessible by integration"},
                {
                    "X-Accepted-GitHub-Permissions": "contents=write; workflows=write",
                    "X-GitHub-Request-Id": "fixture-request-id",
                },
            )
            return
        if payload.get("tag_name") != TAG or "target_commitish" in payload:
            self.send_json(422, {"message": "incorrect release identity"})
            return
        if self.server.fixture.resolve_tag() != self.server.fixture.planned_commit:
            self.send_json(422, {"message": "source tag must exist before its release"})
            return
        self.server.fixture.release = {
            "id": 901,
            "tag_name": TAG,
            "draft": False,
            "html_url": f"https://github.com/{REPOSITORY}/releases/tag/{TAG}",
        }
        self.send_json(201, self.server.fixture.release)


class FixtureServer(ThreadingHTTPServer):
    def __init__(self, fixture: GitHubFixture) -> None:
        super().__init__(("127.0.0.1", 0), Handler)
        self.fixture = fixture


class PlannedSourceBoundaryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        temporary = Path(self.temporary.name)
        self.remote = temporary / "remote.git"
        self.seed = temporary / "seed"
        self.authority = temporary / "authority"
        self.git_config = temporary / "gitconfig"
        run("git", "init", "--bare", "--initial-branch=main", str(self.remote))
        run("git", "init", "--initial-branch=main", str(self.seed))
        run("git", "-C", str(self.seed), "config", "user.name", "Release boundary fixture")
        run("git", "-C", str(self.seed), "config", "user.email", "release-boundary@example.invalid")
        run("git", "-C", str(self.seed), "commit", "--allow-empty", "-m", "Planned source")
        self.planned_commit = run("git", "-C", str(self.seed), "rev-parse", "HEAD").stdout.strip()
        run("git", "-C", str(self.seed), "commit", "--allow-empty", "-m", "Newer repository head")
        self.head_commit = run("git", "-C", str(self.seed), "rev-parse", "HEAD").stdout.strip()
        run("git", "-C", str(self.seed), "remote", "add", "origin", str(self.remote))
        run("git", "-C", str(self.seed), "push", "--set-upstream", "origin", "main")
        run("git", "clone", str(self.remote), str(self.authority))
        run("git", "-C", str(self.authority), "remote", "set-url", "origin", SSH_REMOTE)
        run(
            "git",
            "config",
            "--file",
            str(self.git_config),
            f"url.file://{self.remote}.insteadOf",
            SSH_REMOTE,
        )

        self.fixture = GitHubFixture(self.remote, self.planned_commit)
        self.server = FixtureServer(self.fixture)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.evidence = temporary / "source-publication-evidence.json"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.temporary.cleanup()

    def run_publication(self) -> subprocess.CompletedProcess[str]:
        environment = {
            **os.environ,
            "GIT_CONFIG_GLOBAL": str(self.git_config),
            "GIT_CONFIG_NOSYSTEM": "1",
            "GITHUB_API_URL": f"http://127.0.0.1:{self.server.server_port}",
            "GITHUB_TOKEN": TOKEN,
        }
        return subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--repository",
                REPOSITORY,
                "--tag",
                TAG,
                "--commit",
                self.planned_commit,
                "--plan-tag",
                PLAN_TAG,
                "--git-directory",
                str(self.authority),
                "--evidence",
                str(self.evidence),
                "--ref-observation-attempts",
                "4",
                "--ref-observation-delay",
                "0",
            ],
            env=environment,
            text=True,
            capture_output=True,
            check=False,
        )

    def read_evidence(self) -> dict[str, Any]:
        return json.loads(self.evidence.read_bytes())

    def test_deploy_key_checkout_pushes_older_ancestor_and_rerun_only_verifies(self) -> None:
        run(
            "git",
            "--git-dir",
            str(self.remote),
            "merge-base",
            "--is-ancestor",
            self.planned_commit,
            self.head_commit,
        )
        first = self.run_publication()
        self.assertEqual(0, first.returncode, first.stderr)
        self.assertEqual(self.planned_commit, self.fixture.resolve_tag())
        self.assertEqual(1, self.fixture.release_posts)
        self.assertEqual("created", self.read_evidence()["source_tag_action"])

        second = self.run_publication()
        self.assertEqual(0, second.returncode, second.stderr)
        self.assertEqual(self.planned_commit, self.fixture.resolve_tag())
        self.assertEqual(1, self.fixture.release_posts)
        self.assertEqual("verified", self.read_evidence()["action"])

    def test_successful_push_tolerates_bounded_ref_visibility_lag(self) -> None:
        self.fixture.ref_visibility_misses = 2
        result = self.run_publication()
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual(self.planned_commit, self.fixture.resolve_tag())
        self.assertEqual("created", self.read_evidence()["source_tag_action"])

    def test_existing_tag_at_another_commit_is_refused(self) -> None:
        self.fixture.set_tag(self.head_commit)
        result = self.run_publication()
        self.assertNotEqual(0, result.returncode)
        self.assertEqual(0, self.fixture.release_posts)
        evidence = self.read_evidence()
        self.assertEqual("source-tag", evidence["phase"])
        self.assertIn(self.head_commit, evidence["reason"])

    def test_existing_exact_tag_resumes_missing_release(self) -> None:
        self.fixture.set_tag(self.planned_commit)
        result = self.run_publication()
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual(1, self.fixture.release_posts)
        self.assertNotIn("target_commitish", self.fixture.release_payloads[0])
        evidence = self.read_evidence()
        self.assertEqual("verified", evidence["source_tag_action"])
        self.assertEqual("created", evidence["github_release_action"])

    def test_read_only_deploy_key_failure_is_redacted_and_actionable(self) -> None:
        self.fixture.reject_pushes()
        result = self.run_publication()
        self.assertNotEqual(0, result.returncode)
        self.assertIsNone(self.fixture.resolve_tag())
        evidence = self.read_evidence()
        self.assertEqual("refs/tags/0.1.9", evidence["attempted_ref"])
        self.assertEqual(self.planned_commit, evidence["planned_commit"])
        self.assertEqual("github-permission", evidence["phase"])
        self.assertIn("git push", evidence["github_authority_operation"])
        self.assertIn("hook or ref policy", evidence["reason"])
        self.assertIn("deploy key", evidence["effective_github_permission_boundary"])
        self.assertIn("rerun", evidence["safe_recovery_action"])
        serialized = json.dumps(evidence) + result.stderr
        self.assertNotIn(TOKEN, serialized)
        self.assertNotIn("repository deploy key is read only", serialized)

    def test_release_api_permission_headers_are_retained_without_credentials(self) -> None:
        self.fixture.set_tag(self.planned_commit)
        self.fixture.permission_failure = True
        result = self.run_publication()
        self.assertNotEqual(0, result.returncode)
        evidence = self.read_evidence()
        self.assertEqual(403, evidence["github_http_status"])
        headers = evidence["github_response_permission_headers"]
        self.assertIn("workflows=write", headers["x-accepted-github-permissions"])
        self.assertEqual("fixture-request-id", headers["x-github-request-id"])
        self.assertNotIn(TOKEN, json.dumps(evidence) + result.stderr)


if __name__ == "__main__":
    unittest.main()
