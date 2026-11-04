#!/usr/bin/env python3
"""Select stable documentation only for a published, unchanged SDK source."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path


STABLE_VERSION = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+")
SOURCE_REVISION = re.compile(r"[0-9a-f]{40}")
SDK_PATHS = ("src", "examples", "resources", "config")
AUTOMATIC_PATHS = SDK_PATHS + ("composer.lock", "package.json", "package-lock.json")


class ChannelError(ValueError):
    """The requested stable source cannot be established."""


def run(*arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(arguments, check=False, capture_output=True, text=True, timeout=60)


def git(*arguments: str) -> str:
    result = run("git", *arguments)
    if result.returncode:
        raise ChannelError("Unable to verify the documentation source in Git.")
    return result.stdout.strip()


def published_stable_release(tag: str) -> bool:
    repository = os.environ.get("GITHUB_REPOSITORY", "")
    if not repository:
        return False
    result = run("gh", "release", "view", tag, "--repo", repository, "--json", "isDraft,isPrerelease")
    if result.returncode:
        return False
    try:
        release = json.loads(result.stdout)
    except json.JSONDecodeError:
        return False
    return isinstance(release, dict) and release.get("isDraft") is False and release.get("isPrerelease") is False


def same_published_product(tag_ref: str, *, explicit: bool) -> bool:
    paths = SDK_PATHS if explicit else AUTOMATIC_PATHS
    difference = run("git", "diff", "--quiet", tag_ref, "--", *paths)
    if difference.returncode not in (0, 1):
        raise ChannelError("Unable to compare the published SDK source.")
    if difference.returncode:
        return False
    if git("ls-files", "--others", "--exclude-standard", "--", *paths):
        return False
    published = json.loads(git("show", f"{tag_ref}:composer.json"))
    selected = json.loads(Path("composer.json").read_text())
    if explicit:
        # Retain the existing reviewed stable_source manifest comparison.
        for manifest in (published, selected):
            manifest.get("extra", {}).get("durable-workflow", {}).pop("product-train", None)
    # Preserve JSON value types and key order, as the previous strict PHP comparison did.
    return json.dumps(published, ensure_ascii=False) == json.dumps(selected, ensure_ascii=False)


def explicit_stable_source(stable_tag: str, stable_source: str, selected: str, version: str) -> bool:
    if version != stable_tag or not published_stable_release(stable_tag):
        raise ChannelError("stable_tag must match the manifest and a published stable release.")
    tag_ref = f"refs/tags/{stable_tag}"
    published = git("rev-parse", "--verify", f"{tag_ref}^{{commit}}")
    if not stable_source:
        if selected != published:
            raise ChannelError("The selected documentation source does not match stable_tag.")
    elif selected != stable_source or run("git", "merge-base", "--is-ancestor", selected, "origin/main").returncode:
        raise ChannelError("stable_source must select the exact main-branch source commit.")
    if not same_published_product(tag_ref, explicit=True):
        raise ChannelError("The selected source changes the published SDK product or manifest.")
    return True


def release_channel(stable_tag: str, stable_source: str) -> tuple[bool, str]:
    if stable_tag and not STABLE_VERSION.fullmatch(stable_tag):
        raise ChannelError("stable_tag must name an exact stable SDK version.")
    if stable_source and (not stable_tag or not SOURCE_REVISION.fullmatch(stable_source)):
        raise ChannelError("stable_source requires a stable_tag and an exact lowercase source commit.")
    selected = git("rev-parse", "HEAD")
    if not SOURCE_REVISION.fullmatch(selected):
        raise ChannelError("The selected documentation source must be an exact lowercase commit.")
    manifest = json.loads(Path("composer.json").read_text())
    version = manifest.get("extra", {}).get("durable-workflow", {}).get("product-train")
    if not isinstance(version, str):
        raise ChannelError("The selected SDK manifest has no product-train version.")

    if stable_tag:
        return explicit_stable_source(stable_tag, stable_source, selected, version), selected

    if not STABLE_VERSION.fullmatch(version):
        return False, selected
    tag_ref = f"refs/tags/{version}"
    if run("git", "rev-parse", "--verify", f"{tag_ref}^{{commit}}").returncode:
        return False, selected
    if not published_stable_release(version):
        return False, selected
    return same_published_product(tag_ref, explicit=False), selected


def main() -> int:
    try:
        stable, source = release_channel(os.environ.get("STABLE_TAG", ""), os.environ.get("STABLE_SOURCE", ""))
    except (ChannelError, json.JSONDecodeError, OSError, subprocess.TimeoutExpired) as error:
        print(f"Documentation channel refused: {error}", file=sys.stderr)
        return 1
    output = f"stable={'true' if stable else 'false'}\nsource_revision={source}\n"
    if os.environ.get("GITHUB_OUTPUT"):
        with Path(os.environ["GITHUB_OUTPUT"]).open("a") as destination:
            destination.write(output)
    print(output, end="")
    return 0


if __name__ == "__main__":
    sys.exit(main())
