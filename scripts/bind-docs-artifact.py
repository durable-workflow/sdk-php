#!/usr/bin/env python3
"""Bind a rendered site to its verified, unchanged selected Git source."""

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
    Path("build/site/deployment-audit.json").write_text(json.dumps({
        "schema": "durable-workflow.sdk-php.docs-deployment/v1", "source_revision": selected,
    }) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
