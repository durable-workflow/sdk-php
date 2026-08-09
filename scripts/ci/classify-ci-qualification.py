#!/usr/bin/env python3
"""Select the portable CI route and focused PHP SDK evidence categories."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path, PurePosixPath
from urllib.parse import urlparse


GITHUB_SERVER_URL = "https://github.com"
FOCUSED = "focused"
COMPLETE = "complete"
SENTINEL = "sentinel"

ALL_FOCUSED_CATEGORIES = ("docs", "docs-browser", "release", "runtime")
OBJECT_ID = re.compile(r"[0-9a-f]{40}(?:[0-9a-f]{24})?")


@dataclass(frozen=True)
class Qualification:
    route: str
    route_reason: str
    categories: tuple[str, ...]
    changed_path_reason: str
    changed_count: int


class ChangedPathIdentityError(RuntimeError):
    """Raised when an exact pull-request path set cannot be established."""


def is_identified_alternate_server(server_url: str) -> bool:
    try:
        parsed = urlparse(server_url)
        hostname = parsed.hostname
    except ValueError:
        return False
    return (
        parsed.scheme in {"http", "https"}
        and bool(hostname)
        and parsed.username is None
        and parsed.password is None
        and parsed.path in {"", "/"}
        and not parsed.params
        and not parsed.query
        and not parsed.fragment
    )


def select_route(
    server_url: str,
    event_name: str,
    alternate_ci_focused_admission: str,
) -> tuple[str, str]:
    if server_url == GITHUB_SERVER_URL:
        if event_name == "pull_request":
            return FOCUSED, "github-pull-request"
        return COMPLETE, "github-target-or-dispatch"

    alternate_is_identified = (
        is_identified_alternate_server(server_url)
        and alternate_ci_focused_admission == "true"
    )
    if alternate_is_identified and event_name == "pull_request":
        return FOCUSED, "alternate-ci-pull-request"
    if alternate_is_identified and event_name == "push":
        return SENTINEL, "alternate-ci-target-sentinel"

    return COMPLETE, "unidentified-environment-fail-safe"


def is_canonical_repo_path(path: str) -> bool:
    if not path or path.startswith("/") or "\\" in path:
        return False
    if any(character in path for character in "\0\r\n"):
        return False
    parts = PurePosixPath(path).parts
    return (
        bool(parts)
        and all(part not in {"", ".", ".."} for part in parts)
        and str(PurePosixPath(path)) == path
    )


def path_categories(path: str) -> set[str]:
    categories: set[str] = set()

    if path.startswith("src/"):
        categories.update(("docs", "docs-browser"))

    if (
        path
        in {
            "CHANGELOG.md",
            "CONTRIBUTING.md",
            "README.md",
            "package.json",
            "package-lock.json",
        }
        or path.startswith((".phpdoc/", "docs/"))
        or path.startswith("scripts/check-docs-")
        or path.startswith("scripts/qualify-docs-")
        or path == ".github/workflows/docs.yml"
    ):
        categories.add("docs")

    if (
        path
        in {
            ".github/workflows/docs.yml",
            "package.json",
            "package-lock.json",
            "scripts/check-docs-analytics-browser.mjs",
        }
        or path.startswith(".phpdoc/")
        or path.startswith("scripts/qualify-docs-")
    ):
        categories.add("docs-browser")

    if (
        path.startswith(("src/", "tests/", "examples/", "benchmarks/", "resources/"))
        or path
        in {
            "composer.json",
            "composer.lock",
            "phpstan.neon",
            "phpstan-framework.neon",
            "phpunit.xml.dist",
            "regression-corpus-policy.json",
        }
        or path.startswith("scripts/check-dependency-")
        or "regression-corpus" in path
    ):
        categories.add("runtime")

    if (
        path == ".github/workflows/release-plan-recovery.yml"
        or path.startswith("scripts/ci/component-release-")
        or path.startswith("scripts/ci/release-recovery-")
        or path.startswith("scripts/ci/release_recovery_")
        or path.startswith("scripts/ci/test-component-release-")
        or path.startswith("scripts/ci/test-publish-planned-")
        or path.startswith("scripts/ci/test-php-waterline-")
        or path.startswith("scripts/ci/publish-planned-")
        or path.startswith("scripts/ci/php_waterline_")
        or path.startswith("scripts/ci/recovery_workflow_")
    ):
        categories.add("release")

    if path in {
        ".github/workflows/ci.yml",
        ".github/workflows/public-boundary.yml",
        "scripts/check-public-boundary.sh",
        "scripts/ci/classify-ci-qualification.py",
        "scripts/ci/test-ci-qualification.py",
        "scripts/ci/test-workflow-trust-boundaries.py",
    }:
        categories.add("ci")

    return categories


def classify_changed_files(changed_files: Sequence[str]) -> tuple[tuple[str, ...], str]:
    paths = tuple(sorted(set(changed_files)))
    if not paths or any(not is_canonical_repo_path(path) for path in paths):
        return ALL_FOCUSED_CATEGORIES, "changed-path-identity-unavailable"

    categories: set[str] = set()
    for path in paths:
        selected = path_categories(path)
        if not selected:
            return ALL_FOCUSED_CATEGORIES, "unclassified-path-fail-safe"
        categories.update(selected)

    return tuple(sorted(categories)), "changed-paths-classified"


def changed_files_between(root: Path, base_ref: str, head_ref: str) -> tuple[str, ...]:
    if not OBJECT_ID.fullmatch(base_ref) or not OBJECT_ID.fullmatch(head_ref):
        raise ChangedPathIdentityError(
            "base and head revisions must be immutable object IDs"
        )

    try:
        for revision in (base_ref, head_ref):
            subprocess.run(
                ["git", "cat-file", "-e", f"{revision}^{{commit}}"],
                cwd=root,
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
        result = subprocess.run(
            [
                "git",
                "diff",
                "--name-only",
                "-z",
                "--no-renames",
                f"{base_ref}...{head_ref}",
                "--",
            ],
            cwd=root,
            check=True,
            capture_output=True,
        )
        return tuple(
            path.decode("utf-8") for path in result.stdout.split(b"\0") if path
        )
    except (OSError, subprocess.CalledProcessError, UnicodeDecodeError) as error:
        raise ChangedPathIdentityError(
            "unable to resolve the pull-request path set"
        ) from error


def qualify(
    *,
    root: Path,
    server_url: str,
    event_name: str,
    alternate_ci_focused_admission: str,
    base_ref: str,
    head_ref: str,
    changed_files: Sequence[str] | None = None,
) -> Qualification:
    route, route_reason = select_route(
        server_url,
        event_name,
        alternate_ci_focused_admission,
    )
    if route != FOCUSED:
        return Qualification(route, route_reason, (), "not-a-focused-route", 0)

    if changed_files is None:
        try:
            paths = changed_files_between(root, base_ref, head_ref)
        except ChangedPathIdentityError:
            categories = ALL_FOCUSED_CATEGORIES
            path_reason = "changed-path-identity-unavailable"
            changed_count = 0
        else:
            categories, path_reason = classify_changed_files(paths)
            changed_count = len(paths)
    else:
        categories, path_reason = classify_changed_files(changed_files)
        changed_count = len(set(changed_files))

    return Qualification(
        route,
        route_reason,
        categories,
        path_reason,
        changed_count,
    )


def write_github_output(path: Path, qualification: Qualification) -> None:
    with path.open("a", encoding="utf-8") as output:
        print(f"route={qualification.route}", file=output)
        print(f"route_reason={qualification.route_reason}", file=output)
        print(f"categories={','.join(qualification.categories)}", file=output)
        print(f"changed_path_reason={qualification.changed_path_reason}", file=output)
        print(f"changed_count={qualification.changed_count}", file=output)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--server-url", default="")
    parser.add_argument("--event-name", default="")
    parser.add_argument("--alternate-ci-focused-admission", default="")
    parser.add_argument("--base-ref", default="")
    parser.add_argument("--head-ref", default="")
    parser.add_argument("--changed-file", action="append", dest="changed_files")
    parser.add_argument("--github-output", type=Path)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    qualification = qualify(
        root=args.root.resolve(),
        server_url=args.server_url,
        event_name=args.event_name,
        alternate_ci_focused_admission=args.alternate_ci_focused_admission,
        base_ref=args.base_ref,
        head_ref=args.head_ref,
        changed_files=args.changed_files,
    )
    if args.github_output is not None:
        write_github_output(args.github_output, qualification)
    print(json.dumps(asdict(qualification), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
