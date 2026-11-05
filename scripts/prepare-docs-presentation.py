#!/usr/bin/env python3
"""Stage accepted presentation files over published docs without changing SDK code."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import sys

REVISION = re.compile(r"[0-9a-f]{40}")
PRESENTATION_FILES = frozenset({
    "docs/portal/index.md", "docs/portal/_includes/layout.njk",
    ".phpdoc/template/layout.html.twig",
    ".phpdoc/template/components/identity-header.html.twig",
    ".phpdoc/template/components/identity-footer.html.twig",
})
PACKET_FILE = "presentation.json"
PRESENTATION_TREES = ("docs/portal/assets/", ".phpdoc/template/assets/")


class PresentationError(ValueError):
    """The proposed presentation cannot be bound to accepted source."""


def git(root: Path, *args: str) -> bytes:
    return subprocess.check_output(["git", "-C", str(root), *args], timeout=30, stderr=subprocess.PIPE)


def copy_regular_tree(source: Path, target: Path) -> None:
    if source.is_symlink():
        raise PresentationError("Documentation input roots must be real directories.")
    for path in source.rglob("*"):
        if path.is_symlink() or not (path.is_dir() or path.is_file()):
            raise PresentationError("Documentation inputs must be regular files or directories.")
    shutil.copytree(source, target)


def presentation_blobs(root: Path, revision: str) -> list[tuple[str, bytes]]:
    entries = git(root, "ls-tree", "-rz", revision).split(b"\0")
    overlays = []
    total = 0
    for entry in filter(None, entries):
        metadata, name_bytes = entry.split(b"\t", 1)
        name = name_bytes.decode("utf-8")
        if name not in PRESENTATION_FILES and not name.startswith(PRESENTATION_TREES):
            continue
        mode, kind, blob = metadata.split(b" ")
        if mode != b"100644" or kind != b"blob" or ".." in PurePosixPath(name).parts:
            raise PresentationError("Presentation files must be regular non-executable blobs.")
        content = git(root, "cat-file", "blob", blob.decode("ascii"))
        total += len(content)
        if len(content) > 5_242_880 or total > 20_971_520:
            raise PresentationError("The presentation exceeds its source-only size bound.")
        overlays.append((name, content))
    if not PRESENTATION_FILES.issubset({name for name, _ in overlays}):
        raise PresentationError("The selected presentation is incomplete.")
    return overlays


def prepare(root: Path, revision: str, destination: Path, release_adapter: Path) -> dict:
    if not REVISION.fullmatch(revision):
        raise PresentationError("presentation_source must be an exact lowercase commit.")
    if git(root, "rev-parse", "--verify", revision + "^{commit}").decode().strip() != revision:
        raise PresentationError("The presentation commit differs.")
    if subprocess.run(["git", "-C", str(root), "merge-base", "--is-ancestor", revision, "origin/main"],
                      check=False, timeout=30, capture_output=True).returncode:
        raise PresentationError("The presentation must come from accepted main.")
    if git(root, "diff", "--name-only", "HEAD"):
        raise PresentationError("The selected SDK source must remain unchanged.")
    selected = git(root, "rev-parse", "HEAD").decode().strip()
    if destination.exists():
        raise PresentationError("The presentation staging directory must not exist.")
    overlays = presentation_blobs(root, revision)
    destination.mkdir(parents=True)
    copy_regular_tree(root / "docs/portal", destination / "portal")
    copy_regular_tree(root / ".phpdoc/template", destination / "template")
    for name, content in overlays:
        if name.startswith("docs/portal/"):
            path = destination / "portal" / name.removeprefix("docs/portal/")
        else:
            path = destination / "template" / name.removeprefix(".phpdoc/template/")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    # This workflow-owned adapter reads release facts from DOCS_SOURCE_DIRECTORY.
    shutil.copyfile(release_adapter, destination / "portal/_data/release.js")
    inventory = {path.relative_to(destination).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
                 for path in sorted(destination.rglob("*")) if path.is_file()}
    packet = {"schema": "durable-workflow.sdk-php.docs-presentation/v1",
              "sdk_source_revision": selected, "presentation_source_revision": revision, "files": inventory}
    (destination / PACKET_FILE).write_text(json.dumps(packet, sort_keys=True) + "\n")
    return packet


def verify(destination: Path, selected: str, revision: str) -> None:
    packet = json.loads((destination / PACKET_FILE).read_text())
    actual = {path.relative_to(destination).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
              for path in sorted(destination.rglob("*")) if path.is_file() and path != destination / PACKET_FILE}
    if packet.get("sdk_source_revision") != selected or packet.get("presentation_source_revision") != revision \
            or packet.get("files") != actual or any(path.is_symlink() for path in destination.rglob("*")):
        raise PresentationError("The staged presentation changed before artifact binding.")


def main() -> int:
    try:
        if len(sys.argv) != 3:
            raise PresentationError("Expected an exact presentation commit and a fresh staging directory.")
        root = Path.cwd()
        destination = Path(sys.argv[2]).resolve()
        adapter = Path(__file__).resolve().parent.parent / "docs/portal/_data/release.js"
        packet = prepare(root, sys.argv[1], destination, adapter)
        values = {"DOCS_PORTAL_DIRECTORY": str(destination / "portal"),
                  "DOCS_TEMPLATE_DIRECTORY": str(destination / "template"),
                  "DOCS_PRESENTATION_DIRECTORY": str(destination),
                  "DOCS_PRESENTATION_REVISION": packet["presentation_source_revision"]}
        if any("\n" in value or "\r" in value for value in values.values()):
            raise PresentationError("Staging paths must be single-line values.")
        if os.environ.get("GITHUB_ENV"):
            with Path(os.environ["GITHUB_ENV"]).open("a") as stream:
                stream.write("".join(f"{key}={value}\n" for key, value in values.items()))
        print(json.dumps({key: packet[key] for key in ("sdk_source_revision", "presentation_source_revision")}))
        return 0
    except (OSError, ValueError, subprocess.SubprocessError):
        print("Documentation presentation refused; no SDK source override is allowed.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
