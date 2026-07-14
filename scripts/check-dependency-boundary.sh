#!/usr/bin/env bash
set -euo pipefail

manifest="$(composer show --format=json)"

if grep -Eqi '"name"[[:space:]]*:[[:space:]]*"(laravel/|illuminate/|durable-workflow/(workflow|server))"' <<<"$manifest"; then
  echo "Production dependency graph crosses the framework-neutral SDK boundary." >&2
  exit 1
fi

if grep -Eqi '"(laravel/|illuminate/|durable-workflow/(workflow|server))"[[:space:]]*:' composer.json; then
  echo "composer.json declares a forbidden production dependency." >&2
  exit 1
fi
