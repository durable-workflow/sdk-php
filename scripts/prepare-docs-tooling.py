#!/usr/bin/env python3
"""Check out exact workflow tooling without changing the selected source."""

import re
import subprocess
import sys


def main() -> int:
    if len(sys.argv) != 3 or not re.fullmatch(r"[0-9a-f]{40}", sys.argv[1]):
        print("Expected an exact workflow commit and a separate tooling directory.", file=sys.stderr)
        return 1
    return subprocess.run(
        ["git", "worktree", "add", "--detach", sys.argv[2], sys.argv[1]], check=False,
    ).returncode


if __name__ == "__main__":
    sys.exit(main())
