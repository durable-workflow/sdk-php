#!/usr/bin/env bash
set -euo pipefail

release_tag="${RELEASE_TAG:-${1:-}}"
if [[ ! "$release_tag" =~ ^[0-9]+\.[0-9]+\.[0-9]+([-+][0-9A-Za-z.-]+)?$ ]]; then
    printf 'RELEASE_TAG must identify an exact SDK SemVer release: %s\n' "${release_tag:-<empty>}" >&2
    exit 2
fi

for command in composer php; do
    if ! command -v "$command" >/dev/null 2>&1; then
        printf 'required command not found: %s\n' "$command" >&2
        exit 2
    fi
done

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
consumer="$(mktemp -d)"
trap 'rm -rf "$consumer"' EXIT

composer --working-dir="$consumer" init --name=durable-workflow/sdk-release-identity --no-interaction >/dev/null
composer --working-dir="$consumer" config repositories.sdk \
    "{\"type\":\"path\",\"url\":\"$repo_root\",\"options\":{\"symlink\":false,\"versions\":{\"durable-workflow/sdk\":\"$release_tag\"}}}"
composer --working-dir="$consumer" require \
    "durable-workflow/sdk:$release_tag" \
    --no-interaction --no-progress --prefer-dist

php "$repo_root/scripts/check-registration-identity.php" \
    "$consumer/vendor/autoload.php" \
    "$release_tag"
