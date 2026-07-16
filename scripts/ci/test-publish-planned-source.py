#!/usr/bin/env python3
"""Exercise planned source publication across an HTTP GitHub API boundary."""

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
COMMIT = "05fe99b44062b939e4c43acc00dae457eef87af2"
OTHER_COMMIT = "15fe99b44062b939e4c43acc00dae457eef87af2"
PLAN_TAG = "release-plan/source-recovery-boundary"
SCRIPT = Path(__file__).with_name("publish-planned-source.py")


class GitHubFixture:
    def __init__(self) -> None:
        self.commit: str | None = None
        self.release: dict[str, Any] | None = None
        self.permission_failure = False
        self.release_posts = 0


class Handler(BaseHTTPRequestHandler):
    server: "FixtureServer"

    def log_message(self, _format: str, *_args: object) -> None:
        pass

    def send_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        expected_ref = f"/repos/{REPOSITORY}/git/ref/tags/{urllib.parse.quote(TAG, safe='')}"
        expected_release = f"/repos/{REPOSITORY}/releases/tags/{urllib.parse.quote(TAG, safe='')}"
        if self.path == expected_ref:
            if self.server.fixture.commit is None:
                self.send_json(404, {"message": "Not Found"})
            else:
                self.send_json(
                    200,
                    {"ref": f"refs/tags/{TAG}", "object": {"type": "commit", "sha": self.server.fixture.commit}},
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
        if self.headers.get("Authorization") != "Bearer fixture-token-that-must-not-appear":
            self.send_json(401, {"message": "Bad credentials"})
            return
        length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(length))
        if self.server.fixture.permission_failure:
            self.send_json(403, {"message": "Resource not accessible by integration"})
            return
        if payload.get("tag_name") != TAG or payload.get("target_commitish") != COMMIT:
            self.send_json(422, {"message": "incorrect release identity"})
            return
        self.server.fixture.commit = COMMIT
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
        self.fixture = GitHubFixture()
        self.server = FixtureServer(self.fixture)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.temporary = tempfile.TemporaryDirectory()
        self.evidence = Path(self.temporary.name) / "source-publication-evidence.json"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.temporary.cleanup()

    def run_publication(self) -> subprocess.CompletedProcess[str]:
        environment = {
            **os.environ,
            "GITHUB_API_URL": f"http://127.0.0.1:{self.server.server_port}",
            "GITHUB_TOKEN": "fixture-token-that-must-not-appear",
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
                COMMIT,
                "--plan-tag",
                PLAN_TAG,
                "--evidence",
                str(self.evidence),
            ],
            env=environment,
            text=True,
            capture_output=True,
            check=False,
        )

    def read_evidence(self) -> dict[str, Any]:
        return json.loads(self.evidence.read_bytes())

    def test_release_endpoint_creates_tag_and_identical_rerun_only_verifies(self) -> None:
        first = self.run_publication()
        self.assertEqual(0, first.returncode, first.stderr)
        self.assertEqual(1, self.fixture.release_posts)
        self.assertEqual(COMMIT, self.read_evidence()["source_tag"]["commit"])

        second = self.run_publication()
        self.assertEqual(0, second.returncode, second.stderr)
        self.assertEqual(1, self.fixture.release_posts)
        self.assertEqual("verified", self.read_evidence()["action"])

    def test_existing_tag_at_another_commit_is_refused(self) -> None:
        self.fixture.commit = OTHER_COMMIT
        result = self.run_publication()
        self.assertNotEqual(0, result.returncode)
        self.assertEqual(0, self.fixture.release_posts)
        evidence = self.read_evidence()
        self.assertEqual("source-tag", evidence["phase"])
        self.assertIn(OTHER_COMMIT, evidence["reason"])

    def test_permission_failure_is_redacted_and_actionable(self) -> None:
        self.fixture.permission_failure = True
        result = self.run_publication()
        self.assertNotEqual(0, result.returncode)
        evidence = self.read_evidence()
        self.assertEqual("refs/tags/0.1.9", evidence["attempted_ref"])
        self.assertEqual(COMMIT, evidence["planned_commit"])
        self.assertEqual(403, evidence["github_http_status"])
        self.assertIn("contents=write", evidence["effective_github_permission_boundary"])
        self.assertIn("rerun", evidence["safe_recovery_action"])
        serialized = json.dumps(evidence) + result.stderr
        self.assertNotIn("fixture-token-that-must-not-appear", serialized)
        self.assertIn("Resource not accessible by integration", result.stderr)


if __name__ == "__main__":
    unittest.main()
