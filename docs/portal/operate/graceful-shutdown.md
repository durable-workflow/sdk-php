---
layout: layout.njk
title: Graceful shutdown
description: Drain Durable Workflow PHP workers safely under signals, supervisors, containers, and rolling deployments.
lead: A graceful stop prevents new polls, lets the active synchronous task settle, and preserves task completion or failure reporting before the process exits.
previous:
  label: Authentication
  url: /operate/authentication/
next:
  label: Troubleshooting
  url: /operate/troubleshooting/
---
## Use the managed signal path

`Worker::run()` installs `SIGINT` and `SIGTERM` handlers when `pcntl` is available. The handler requests shutdown; it does not kill the active PHP call stack.

```php
$worker = new Worker($client, 'orders', buildId: getenv('APP_RELEASE') ?: null);
$worker->registerWorkflow('orders.process', $workflow);
$worker->registerActivity('orders.reserve', $activity);
$worker->run();
```

Confirm `pcntl` is present in the production CLI image:

```bash
php -r 'exit(extension_loaded("pcntl") ? 0 : 1);'
```

## Match the supervisor grace period

Set the platform's termination grace period longer than the maximum time one synchronous activity can run without reaching a safe boundary. On Kubernetes, combine `terminationGracePeriodSeconds` with a pre-stop drain only if the platform can address the worker instance reliably.

Long activities should heartbeat progress and split work into idempotent chunks. A PHP signal cannot safely interrupt an arbitrary library call and then claim its side effect did not happen.

## Own the loop when a framework requires it

For a custom Console or supervisor integration, call `tick()` with a bounded poll timeout and invoke `requestShutdown()` from the framework's signal event:

```php
while (!$application->isStopping()) {
    $worker->tick(pollTimeoutSeconds: 1);
}

$worker->requestShutdown();
```

Keep the poll timeout short enough for the process to notice shutdown while still avoiding a busy loop.

## Rolling deployment sequence

1. Start the new compatible worker pool.
2. Verify registration freshness and expected build ID on every task queue.
3. Send `SIGTERM` to the old pool.
4. Wait for active tasks and registrations to drain.
5. Force-stop only after the documented grace window.

If a poll returns a typed terminal status such as draining, stopped, or stale registration, the managed worker stops its loop instead of treating that response as idle work.

<div class="outcome"><strong>Shutdown check</strong><p>Send <code>SIGTERM</code> while the worker is idle and during a bounded test activity. In both cases it should stop within the supervisor window without leaving an unreported completed side effect.</p></div>
