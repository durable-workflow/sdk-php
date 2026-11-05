#!/usr/bin/env python3
"""Bind a rendered site to its verified, unchanged selected Git source."""

import importlib.util
import os
import json
import re
import subprocess
import sys
from pathlib import Path


def main() -> int:
    if len(sys.argv) != 2 or not re.fullmatch(r"[0-9a-f]{40}", sys.argv[1]):
        print("Expected the exact selected source commit.", file=sys.stderr)
        return 1
    selected = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    if selected != sys.argv[1] or subprocess.run(["git", "diff", "--exit-code", "HEAD"], check=False).returncode:
        print("The selected documentation source changed before artifact binding.", file=sys.stderr)
        return 1
    presentation = os.environ.get("DOCS_PRESENTATION_REVISION", selected)
    if not re.fullmatch(r"[0-9a-f]{40}", presentation):
        print("The presentation revision is invalid.", file=sys.stderr)
        return 1
    if os.environ.get("DOCS_PRESENTATION_DIRECTORY"):
        spec = importlib.util.spec_from_file_location("presentation", Path(__file__).with_name("prepare-docs-presentation.py"))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        module.verify(Path(os.environ["DOCS_PRESENTATION_DIRECTORY"]), selected, presentation)
    audit = {
        "schema": "durable-workflow.sdk-php.docs-deployment/v2",
        "source_revision": selected, "sdk_source_revision": selected,
        "presentation_source_revision": presentation,
    }
    tag = os.environ.get("STABLE_TAG", "")
    if tag:
        if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", tag):
            print("The published SDK tag is invalid.", file=sys.stderr)
            return 1
        audit["published_sdk_source_revision"] = subprocess.check_output(
            ["git", "rev-parse", "--verify", f"refs/tags/{tag}^{{commit}}"], text=True,
        ).strip()
        audit["published_sdk_version"] = tag
    workflow = os.environ.get("GITHUB_WORKFLOW_SHA", "")
    if workflow:
        if not re.fullmatch(r"[0-9a-f]{40}", workflow):
            return 1
        audit["workflow_revision"] = workflow
    Path("build/site/deployment-audit.json").write_text(json.dumps(audit) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
