#!/usr/bin/env bash
# Installed-lifecycle qualification: a normally installed (Packagist) PHP SDK
# against an exact Server image pulled by digest, in disposable local
# containers only. Usage: run.sh <output-dir>
# Optional: SDK_VERSION, SERVER_IMAGE. Nothing here talks to Cloud.
set -euo pipefail

OUT="$(mkdir -p "$1" && cd "$1" && pwd)"
HERE="$(cd "$(dirname "$0")" && pwd)"
SDK_VERSION="${SDK_VERSION:-3.6.1}"
SERVER_IMAGE="${SERVER_IMAGE:-ghcr.io/durable-workflow/server@sha256:485295819c7739d3e937a2c89b8593532de0d9c2fbc8f2e7aaddb0b45fe4b319}"
COMPOSER_IMAGE="composer:2.10.3@sha256:da624c2c705d54e2f5a9bb77a5dc6c7d091711433ebefd7035672e84c2010fee"
PHP_IMAGE="php:8.4-cli@sha256:7e7413ed31b8c42b31c939cc83b3758b1cd618f7bf3803f5a0c39f98d4adcbee"
RUN="sdkphp-installed-$(date +%s)-$$"
NET="$RUN-net"
TOKEN="fixture-$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"
QUEUE="installed-lifecycle"

cleanup() {
    docker logs "$RUN-worker" > "$OUT/worker.log" 2>&1 || true
    docker logs "$RUN-server" > "$OUT/server.log" 2>&1 || true
    docker rm -f "$RUN-worker" "$RUN-server" > /dev/null 2>&1 || true
    docker network rm "$NET" > /dev/null 2>&1 || true
}
trap cleanup EXIT

date -u +%Y-%m-%dT%H:%M:%SZ > "$OUT/started_at"
git -C "$HERE" rev-parse HEAD > "$OUT/runner_commit"
git -C "$HERE" status --porcelain -- . > "$OUT/runner_dirty"

# Server identity: the pulled index and its per-platform manifests.
docker pull -q "$SERVER_IMAGE" > /dev/null
docker buildx imagetools inspect "$SERVER_IMAGE" > "$OUT/server-index.txt"
docker image inspect "$SERVER_IMAGE" --format '{{json .}}' > "$OUT/server-image.json"

# Normal registry installation of the SDK into a fresh consumer project.
mkdir -p "$OUT/consumer"
cat > "$OUT/consumer/composer.json" <<JSON
{
    "require": {"durable-workflow/sdk": "$SDK_VERSION"},
    "config": {"platform": {"php": "8.4.27"}, "allow-plugins": {"php-http/discovery": false}}
}
JSON
docker run --rm --name "$RUN-install" -e COMPOSER_HOME=/tmp/composer-home -v "$OUT/consumer:/consumer" -w /consumer \
    "$COMPOSER_IMAGE" sh -c 'composer install --no-interaction --no-progress --no-scripts \
        && composer show durable-workflow/sdk --format=json > /consumer/sdk-show.json \
        && find /tmp/composer-home/cache/files/durable-workflow/sdk -type f -exec sha256sum {} \; > /consumer/sdk-dist.sha256 \
        && composer audit --locked --no-interaction > /consumer/audit.txt 2>&1; true' > "$OUT/install.log" 2>&1

docker network create "$NET" > /dev/null
docker run -d --name "$RUN-server" --network "$NET" --network-alias server -e DW_AUTH_TOKEN="$TOKEN" \
    "$SERVER_IMAGE" > /dev/null
docker run --rm --name "$RUN-wait" --network "$NET" "$PHP_IMAGE" php -r '
    $until = time() + 60;
    while (time() < $until) {
        $body = @file_get_contents("http://server:8080/api/health");
        if ($body !== false) { echo $body, "\n"; exit(0); }
        usleep(500000);
    }
    exit(1);' > "$OUT/health.json"

common=(--network "$NET" -e DW_URL=http://server:8080 -e DW_TOKEN="$TOKEN" -e DW_TASK_QUEUE="$QUEUE"
    -v "$OUT/consumer:/consumer:ro" -v "$HERE:/harness:ro")
docker run -d --name "$RUN-worker" "${common[@]}" "$PHP_IMAGE" php /harness/worker.php > /dev/null
sleep 3
docker run --rm --name "$RUN-client" "${common[@]}" "$PHP_IMAGE" php /harness/client.php | tee "$OUT/results.jsonl"

# Optional (OTEL=1): add the OpenTelemetry SDK to the same consumer and check
# exported SDK metrics against the live Server.
if [ "${OTEL:-0}" = 1 ]; then
    docker run --rm --name "$RUN-otel-install" -e COMPOSER_HOME=/tmp/composer-home -v "$OUT/consumer:/consumer" -w /consumer \
        "$COMPOSER_IMAGE" sh -c 'composer require --no-interaction --no-progress --no-scripts open-telemetry/sdk:^1.15 \
            && composer show open-telemetry/sdk --format=json > /consumer/otel-sdk-show.json' >> "$OUT/install.log" 2>&1
    docker run --rm --name "$RUN-otel" "${common[@]}" "$PHP_IMAGE" php /harness/otel-metrics.php | tee -a "$OUT/results.jsonl"
fi

date -u +%Y-%m-%dT%H:%M:%SZ > "$OUT/ended_at"
