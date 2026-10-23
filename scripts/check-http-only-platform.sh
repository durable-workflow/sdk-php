#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
consumer="$(mktemp -d)"
trap 'rm -rf "$consumer"' EXIT

php -r '
$root = $argv[1];
$manifest = json_decode(file_get_contents($root."/composer.json"), true, flags: JSON_THROW_ON_ERROR);
$version = $manifest["extra"]["durable-workflow"]["product-train"];
$consumer = [
    "name" => "durable-workflow/http-only-platform-check",
    "repositories" => [["type" => "path", "url" => $root, "options" => [
        "symlink" => true, "versions" => ["durable-workflow/sdk" => $version],
    ]]],
    "require" => ["durable-workflow/sdk" => $version, "guzzlehttp/guzzle" => "^7.8", "psr/http-message" => "^1.1"],
    "config" => ["allow-plugins" => new stdClass(), "platform" => [
        "php" => "8.1.0", "ext-curl" => false, "ext-openssl" => false,
    ]],
];
file_put_contents($argv[2]."/composer.json", json_encode($consumer, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR)."\n");
' "$repo_root" "$consumer"
composer --working-dir="$consumer" update --no-dev --no-interaction --prefer-dist --no-progress
composer --working-dir="$consumer" audit --no-dev
SDK_CONTROL_ACCEPTANCE_AUTOLOAD="$consumer/vendor/autoload.php" \
    php "$repo_root/vendor/bin/phpunit" "$repo_root/tests/ControlRequestTimeoutTest.php" --filter testHttpOnlyConsumerNeedsNoTlsFunctions
