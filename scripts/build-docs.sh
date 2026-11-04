#!/usr/bin/env bash
set -euo pipefail

# Run workflow-owned tools and dependencies against the selected source inputs.
tooling_directory=$(cd "$(dirname "$0")/.." && pwd)
export DOCS_SOURCE_DIRECTORY=${DOCS_SOURCE_DIRECTORY:-$PWD}
cd "$DOCS_SOURCE_DIRECTORY"
"$tooling_directory/vendor/bin/phpdoc" \
  -d "$PWD/src" -t "$PWD/build/api" --cache-folder "$PWD/build/phpdoc-cache" \
  --title='Durable Workflow PHP SDK — API Reference' --template="$PWD/.phpdoc/template"
php "$tooling_directory/scripts/finalize-api-reference.php" build/api
"$tooling_directory/node_modules/.bin/eleventy" --config="$tooling_directory/.eleventy.cjs"

# Substitute only the generated runtime; keep the selected checkout unchanged.
if [ "${DOCS_REQUIRE_ANALYTICS_TOKEN:-}" = true ]; then
  if [[ ! "${CLOUDFLARE_WEB_ANALYTICS_TOKEN:-}" =~ ^[a-f0-9]{32}$ ]]; then
    echo 'CLOUDFLARE_WEB_ANALYTICS_TOKEN must be the canonical 32-character site token' >&2
    exit 1
  fi
  python - <<'PY'
import os
from pathlib import Path
runtime = Path("build/site/analytics/analytics.js")
runtime.write_text(runtime.read_text().replace("__CLOUDFLARE_WEB_ANALYTICS_TOKEN__", os.environ["CLOUDFLARE_WEB_ANALYTICS_TOKEN"]))
PY
fi
