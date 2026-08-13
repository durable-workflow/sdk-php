---
layout: layout.njk
title: Your first workflow
description: Run the package-owned Durable Workflow PHP quickstart against Cloud or a self-hosted Server and read its durable result.
lead: Start a runtime, install the published SDK, run its shipped worker and client, and check one durable result. Each stage ends with something you can observe.
next:
  label: Client setup
  url: /getting-started/client-setup/
---
## What you need

- PHP 8.1 or newer with `ext-json`, Composer 2, and two terminal windows.
- Docker for the local Server route, or a provisioned Cloud namespace runtime.
- `ext-pcntl` in the worker process so `SIGINT` and `SIGTERM` drain cleanly.

## 1. Install the published SDK

```bash
mkdir durable-php-quickstart
cd durable-php-quickstart
composer init --name=acme/durable-php-quickstart --no-interaction
{{ release.composerCommand }}
```

<div class="outcome"><strong>Checkpoint</strong><p><code>composer show durable-workflow/sdk</code> reports a <code>{{ release.sdkChannel }}</code> release.</p></div>

## 2. Start or select the runtime

Choose one path and keep its values in both terminals.

### Self-hosted Server on your machine

Bootstrap a durable volume once, then start the Server version qualified with this SDK:

```bash
{{ release.serverImageCommand | safe }}
export DW_AUTH_TOKEN=dev-token

docker volume create durable-workflow-php-quickstart
docker run --rm \
  -v durable-workflow-php-quickstart:/app/database \
  -e DW_AUTH_DRIVER=token \
  -e DW_AUTH_TOKEN="$DW_AUTH_TOKEN" \
  "$DW_SERVER_IMAGE" server-bootstrap

docker run -d --name durable-workflow-php-server \
  -p 8080:8080 \
  -v durable-workflow-php-quickstart:/app/database \
  -e DW_AUTH_DRIVER=token \
  -e DW_AUTH_TOKEN="$DW_AUTH_TOKEN" \
  "$DW_SERVER_IMAGE"

until curl -sf http://localhost:8080/api/ready >/dev/null; do sleep 1; done
export DURABLE_WORKFLOW_RUNTIME_URL='{{ release.quickstart.runtime_targets.server.client_input }}'
export DURABLE_WORKFLOW_NAMESPACE='{{ release.quickstart.runtime_targets.server.namespace_input }}'
export DURABLE_WORKFLOW_CLIENT_TOKEN="$DW_AUTH_TOKEN"
export DURABLE_WORKFLOW_WORKER_TOKEN="$DW_AUTH_TOKEN"
```

<div class="outcome"><strong>Checkpoint</strong><p>The readiness loop exits successfully. Pass the bare Server origin to the SDK; it appends its own <code>/api</code> segment.</p></div>

### Durable Workflow Cloud

Use the complete namespace runtime URI and namespace returned by provisioning. Do not replace the runtime URI with the Cloud control-plane URL or trim its path prefix.

```bash
export DURABLE_WORKFLOW_RUNTIME_URL='{{ release.quickstart.runtime_targets.cloud.client_input }}'
export DURABLE_WORKFLOW_NAMESPACE='{{ release.quickstart.runtime_targets.cloud.namespace_input }}'

read -rsp 'Client credential: ' DURABLE_WORKFLOW_CLIENT_TOKEN; echo
export DURABLE_WORKFLOW_CLIENT_TOKEN
read -rsp 'Worker credential: ' DURABLE_WORKFLOW_WORKER_TOKEN; echo
export DURABLE_WORKFLOW_WORKER_TOKEN
```

The prompts do not echo credentials. Keep them in process environments or a secret manager, never in source or diagnostics.

## 3. Choose an isolated task queue

```bash
export DURABLE_WORKFLOW_TASK_QUEUE="php-quickstart-$(php -r 'echo bin2hex(random_bytes(8));')"
```

<div class="outcome"><strong>Checkpoint</strong><p><code>printf '%s\n' "$DURABLE_WORKFLOW_TASK_QUEUE"</code> prints a non-empty, unique queue name.</p></div>

## 4. Copy the shipped files

These are the executable files published under the package's `examples/` directory. Save each beside the new project's `vendor/` directory.

<details class="code-disclosure" open><summary><strong>bootstrap.php</strong> — locate Composer without a checkout-specific path</summary>

{% sourceFile "bootstrap", "php", "bootstrap.php" %}

</details>

<details class="code-disclosure"><summary><strong>worker.php</strong> — discover the attributed workflow and activity before polling</summary>

{% sourceFile "worker", "php", "worker.php" %}

</details>

<details class="code-disclosure"><summary><strong>client.php</strong> — start a unique workflow and wait for its result</summary>

{% sourceFile "client", "php", "client.php" %}

</details>

## 5. Run the worker, client, and result read

In terminal one, expose only the worker role credential:

```bash
env -u DURABLE_WORKFLOW_CLIENT_TOKEN php worker.php
```

In terminal two, reuse the runtime, namespace, and task queue but expose only the client role credential:

```bash
env -u DURABLE_WORKFLOW_WORKER_TOKEN php client.php
```

<div class="outcome"><strong>Expected result</strong><p>The client prints a fresh <code>php-quickstart-…</code> workflow ID and <code>"result":{"greeting":"{{ release.quickstart.expected_result.result.greeting }}"}</code>. Stop the worker with <kbd>Ctrl</kbd>+<kbd>C</kbd>; the managed worker drains before returning.</p></div>

## What just became durable?

The runtime owns the workflow ID, run history, pending tasks, and terminal result. The PHP worker owns deterministic orchestration and activity code. If the process exits after an activity result is committed, another compatible worker can replay the history and continue without repeating that committed activity.

Next, configure [role-specific clients](/getting-started/client-setup/) or explore [workflows and activities](/build/workflows-activities/).
