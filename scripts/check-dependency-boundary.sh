#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
composer --working-dir="$repo_root" show --no-dev --format=json \
  | php "$repo_root/scripts/check-dependency-boundary.php" "$repo_root/composer.json"
