---
layout: layout.njk
title: Deployment
description: Deploy Durable Workflow PHP clients and workers to Cloud or self-hosted Server with safe process, queue, and version boundaries.
lead: Deploy the web client, worker process, and runtime as separate roles. Keep task queue routing and protocol compatibility observable during every rollout.
previous:
  label: Symfony
  url: /frameworks/symfony/
next:
  label: Authentication
  url: /operate/authentication/
---
## Cloud

Cloud provisioning returns a namespace runtime URL and role credentials. Configure each process from secrets rather than baking credentials into the image:

```bash
DURABLE_WORKFLOW_RUNTIME_URL=https://provisioned-runtime.example/api/runtime/v1/namespaces/runtime-id
DURABLE_WORKFLOW_NAMESPACE=orders-production
DURABLE_WORKFLOW_CLIENT_TOKEN=...
DURABLE_WORKFLOW_WORKER_TOKEN=...
```

Deploy web/API processes with the client role. Deploy worker processes with the worker role and only the application dependencies required by their task queues. Treat the provisioned runtime URL as opaque; retain its path prefix and let the SDK append `/api`.

## Self-hosted Server

Use the Server image qualified by this SDK release:

```bash
{{ release.serverImageCommand | safe }}
docker pull "$DW_SERVER_IMAGE"
```

Production Server deployment also needs durable database storage, explicit authentication, bootstrap/migrations, readiness checks, queue-processing roles, TLS at the ingress boundary, backups, and retention policy. Follow the [self-hosted Server guide](https://durable-workflow.com/docs/2.0/polyglot/server/) rather than copying the single-container development topology into production.

## Package the PHP worker

A minimal production image should:

1. install Composer dependencies with `--no-dev --prefer-dist --classmap-authoritative`;
2. include `pcntl` for managed signal handling;
3. run one foreground worker command;
4. expose no HTTP port unless the container also owns a separate health process;
5. identify the application release through `buildId`.

Do not use the SDK repository's `main` branch as a production Composer source. Install `{{ release.composerPackage }}` to follow the supported stable 2.0 line, then commit the resolved version in `composer.lock`.

## Roll out compatible workers

Start new workers before draining the old pool. Keep old and new builds registered only when both can replay the histories routed to that queue. If a change is not replay-compatible, use a new task queue or a worker build-routing plan rather than hoping restarts choose the right code.

## Readiness and shutdown

Runtime readiness, worker admission, and process liveness are different signals:

- Server/Cloud readiness proves the runtime can use its configured backing services.
- Worker visibility proves a build is registered and fresh on the task queue.
- Process liveness proves only that PHP has not exited.

On termination, stop accepting tasks, allow the active synchronous task to settle, then let the process exit. See [graceful shutdown](/operate/graceful-shutdown/).

<div class="note"><strong>Compatibility floor</strong><p>This portal follows the <code>{{ release.sdkChannel }}</code> SDK and <code>{{ release.serverChannel }}</code> Server lines with worker protocol <code>{{ release.workerProtocolVersion }}</code>. Confirm the live runtime's discovery response during deployment.</p></div>
