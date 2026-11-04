#!/usr/bin/env python3
"""Check out exact workflow tooling without changing the selected source."""

import re
import subprocess
import sys


def main() -> int:
    if len(sys.argv) != 3:
        print("Expected an exact workflow commit and a separate tooling directory.", file=sys.stderr)
        return 1
    revision = re.fullmatch(r"[0-9a-f]{40}", sys.argv[1])
    destination = re.fullmatch(r"/[\w ./-]+", sys.argv[2], re.ASCII)
    if revision is None or destination is None:
        print("Expected an exact workflow commit and an absolute tooling directory.", file=sys.stderr)
        return 1
    return subprocess.run(
        ["git", "worktree", "add", "--detach", "--", destination.group(0), revision.group(0)], check=False,
    ).returncode


if __name__ == "__main__":
    sys.exit(main())
