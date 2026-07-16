#!/usr/bin/env python3
"""Create or verify a release-plan source tag through GitHub Releases."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
PLAN_TAG_PATTERN = re.compile(r"^release-plan/[a-z0-9][a-z0-9._-]{0,55}$")
REPOSITORY_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
TAG_PATTERN = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z][0-9A-Za-z.-]*)?$")
PERMISSION_BOUNDARY = (
    "repository-scoped GitHub Actions GITHUB_TOKEN; workflow-declared permissions: "
    "contents=write, attestations=read, all other configurable permissions=none"
)
SCHEMA = "durable-workflow.source-release-publication/v1"


class PublicationError(RuntimeError):
    """The planned source release cannot be advanced safely."""

    def __init__(
        self,
        message: str,
        *,
        phase: str,
        operation: str | None = None,
        status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.phase = phase
        self.operation = operation
        self.status = status


class GitHubClient:
    def __init__(self, api_url: str, token: str) -> None:
        self.api_url = api_url.rstrip("/")
        self.token = token

    def request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> Any:
        operation = f"{method} {path}"
        body = json.dumps(payload).encode() if payload is not None else None
        request = urllib.request.Request(
            f"{self.api_url}{path}",
            data=body,
            method=method,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
                "User-Agent": "durable-workflow-source-release/1",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            try:
                response = json.loads(error.read(4096))
                message = response.get("message") if isinstance(response, dict) else None
            except (json.JSONDecodeError, UnicodeDecodeError):
                message = None
            raise PublicationError(
                str(message or f"GitHub API returned HTTP {error.code}"),
                phase="github-permission" if error.code in {401, 403} else "github-api",
                operation=operation,
                status=error.code,
            ) from error
        except urllib.error.URLError as error:
            raise PublicationError(
                f"GitHub API request failed: {error.reason}",
                phase="github-api",
                operation=operation,
            ) from error

    def optional(self, path: str) -> Any | None:
        try:
            return self.request("GET", path)
        except PublicationError as error:
            if error.status == 404:
                return None
            raise


def canonical_json(value: Any) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True, ensure_ascii=True) + "\n").encode()


def resolve_tag(client: GitHubClient, repository: str, tag: str) -> str | None:
    encoded = urllib.parse.quote(tag, safe="")
    target = client.optional(f"/repos/{repository}/git/ref/tags/{encoded}")
    if target is None:
        return None
    if not isinstance(target, dict):
        raise PublicationError("GitHub returned an invalid tag ref", phase="source-tag")
    target = target.get("object", {})
    seen: set[str] = set()
    while target.get("type") == "tag":
        sha = target.get("sha")
        if not isinstance(sha, str) or sha in seen:
            raise PublicationError("GitHub returned an invalid annotated tag chain", phase="source-tag")
        seen.add(sha)
        annotated = client.request("GET", f"/repos/{repository}/git/tags/{sha}")
        target = annotated.get("object", {}) if isinstance(annotated, dict) else {}
    sha = target.get("sha")
    if target.get("type") != "commit" or not isinstance(sha, str) or not COMMIT_PATTERN.fullmatch(sha):
        raise PublicationError("GitHub tag does not resolve to a commit", phase="source-tag")
    return sha


def get_release(client: GitHubClient, repository: str, tag: str) -> dict[str, Any] | None:
    encoded = urllib.parse.quote(tag, safe="")
    release = client.optional(f"/repos/{repository}/releases/tags/{encoded}")
    if release is None:
        return None
    if not isinstance(release, dict) or release.get("tag_name") != tag or release.get("draft"):
        raise PublicationError("GitHub Release is draft or has an unexpected tag identity", phase="github-release")
    return release


def safe_recovery(repository: str, plan_tag: str) -> str:
    return (
        f"Restore this repository's GitHub Actions contents:write authority, then rerun "
        f"{repository} Release plan recovery for {plan_tag}; do not move the tag, push it locally, "
        "or substitute an operator token"
    )


def evidence_base(repository: str, tag: str, commit: str, plan_tag: str) -> dict[str, Any]:
    return {
        "schema": SCHEMA,
        "repository": repository,
        "release_plan_tag": plan_tag,
        "attempted_ref": f"refs/tags/{tag}",
        "planned_commit": commit,
        "effective_github_permission_boundary": PERMISSION_BOUNDARY,
        "observed_at": dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    }


def publish_source(
    client: GitHubClient,
    repository: str,
    tag: str,
    commit: str,
    plan_tag: str,
) -> dict[str, Any]:
    source = resolve_tag(client, repository, tag)
    if source is not None and source != commit:
        raise PublicationError(
            f"existing source tag resolves to {source}, not the planned commit {commit}",
            phase="source-tag",
        )

    release = get_release(client, repository, tag)
    if source is None and release is not None:
        raise PublicationError("GitHub Release exists without its source tag", phase="github-release")

    action = "verified"
    if release is None:
        action = "created"
        release = client.request(
            "POST",
            f"/repos/{repository}/releases",
            {
                "tag_name": tag,
                "target_commitish": commit,
                "name": tag,
                "generate_release_notes": True,
                "draft": False,
                "prerelease": "-" in tag,
            },
        )

    source = resolve_tag(client, repository, tag)
    if source != commit:
        raise PublicationError(
            f"created source tag resolves to {source or 'no commit'}, not the planned commit {commit}",
            phase="source-tag",
        )
    release = get_release(client, repository, tag)
    if release is None:
        raise PublicationError("GitHub Release is absent after source publication", phase="github-release")

    return {
        "phase": "source-publication",
        "outcome": "verified",
        "action": action,
        "source_tag": {"ref": f"refs/tags/{tag}", "commit": source},
        "github_release": {"id": release.get("id"), "url": release.get("html_url")},
        "safe_recovery_action": "No action is required",
    }


def validate_arguments(repository: str, tag: str, commit: str, plan_tag: str) -> None:
    if not REPOSITORY_PATTERN.fullmatch(repository):
        raise PublicationError("repository must be an exact owner/name", phase="input")
    if not TAG_PATTERN.fullmatch(tag):
        raise PublicationError("release tag must be exact SemVer", phase="input")
    if not COMMIT_PATTERN.fullmatch(commit):
        raise PublicationError("planned commit must be a full lowercase Git commit", phase="input")
    if not PLAN_TAG_PATTERN.fullmatch(plan_tag):
        raise PublicationError("release plan tag has an invalid identity", phase="input")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--plan-tag", required=True)
    parser.add_argument("--evidence", required=True, type=Path)
    args = parser.parse_args()

    state = evidence_base(args.repository, args.tag, args.commit, args.plan_tag)
    try:
        validate_arguments(args.repository, args.tag, args.commit, args.plan_tag)
        token = os.environ.get("GITHUB_TOKEN")
        if not token:
            raise PublicationError("repository GitHub Actions token is unavailable", phase="github-permission")
        client = GitHubClient(os.environ.get("GITHUB_API_URL", "https://api.github.com"), token)
        state.update(publish_source(client, args.repository, args.tag, args.commit, args.plan_tag))
        args.evidence.write_bytes(canonical_json(state))
        print(f"verified refs/tags/{args.tag} at {args.commit} and its public GitHub Release")
        return 0
    except PublicationError as error:
        recovery = safe_recovery(args.repository, args.plan_tag)
        state.update(
            {
                "phase": error.phase,
                "outcome": "failed",
                "reason": str(error),
                "github_api_operation": error.operation,
                "github_http_status": error.status,
                "safe_recovery_action": recovery,
            }
        )
        args.evidence.write_bytes(canonical_json(state))
        print(
            f"source publication failed for refs/tags/{args.tag} at planned commit {args.commit}; "
            f"effective GitHub permission boundary: {PERMISSION_BOUNDARY}; reason: {error}; "
            f"safe recovery: {recovery}",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
