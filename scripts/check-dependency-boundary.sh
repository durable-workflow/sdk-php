#!/usr/bin/env bash
set -euo pipefail

manifest="$(composer show --no-dev --format=json)"

if grep -Eqi '"name"[[:space:]]*:[[:space:]]*"(laravel/|illuminate/|symfony/|durable-workflow/(workflow|server))"' <<<"$manifest"; then
  echo "Production dependency graph crosses the framework-neutral SDK boundary." >&2
  exit 1
fi

php -r '
$manifest = json_decode(file_get_contents("composer.json"), true, flags: JSON_THROW_ON_ERROR);
foreach (array_keys($manifest["require"] ?? []) as $requirement) {
    if (preg_match("~^(laravel/|illuminate/|symfony/|durable-workflow/(workflow|server)$)~i", $requirement)) {
        fwrite(STDERR, "composer.json declares forbidden production dependency {$requirement}.\n");
        exit(1);
    }
}
'
