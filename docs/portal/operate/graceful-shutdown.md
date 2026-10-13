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

## Integrate a framework command

Run the managed loop inside your Console command. The supplied
[Laravel](/frameworks/laravel/) and [Symfony](/frameworks/symfony/) worker commands
already use this lifecycle:

```php
$worker->run(pollTimeoutSeconds: 1);
```

The process supervisor delivers `SIGTERM`; the managed signal path requests
shutdown and `run()` deregisters in its cleanup. A framework callback may call
`requestShutdown()` when it executes during the managed loop. That method sets
the shutdown flag; deregistration occurs when `run()` exits, rather than at the
instant the callback runs.

`tick()` alone does not register a worker, initialize its heartbeat lifecycle or
deregister it. A framework-owned `while` loop around `tick()` therefore does not
provide the managed lifecycle, and `requestShutdown()` does not supply the
missing setup or cleanup. Keep a finite poll timeout with `run()` so an idle
worker can notice shutdown promptly.

## Deregistration retries

With a Server that advertises `worker_deregistration_fencing`, the worker captures
its registration token and uses it when deregistering. Temporary connection or
database contention can be retried within one ten-second budget covering requests
and backoff. A completed receipt is replayed without removing a replacement worker.
Any remaining cancellation deadline also limits this budget.

Retries require bounded transport I/O. The default Guzzle transport supports it
when `ext-curl` is installed. Custom unbounded transports make one fenced attempt
and emit `worker.shutdown_retry_unavailable`. Older Servers retain one legacy
deregistration attempt. Persistent failures emit `worker.shutdown_failed` and
remain failures. An earlier worker error retains precedence.

## Rolling deployment sequence

1. Start the new compatible worker pool.
2. Verify registration freshness and expected build ID on every task queue.
3. Send `SIGTERM` to the old pool.
4. Wait for active tasks and registrations to drain.
5. Force-stop only after the documented grace window.

If a poll returns a typed terminal status such as draining, stopped, or stale registration, the managed worker stops its loop instead of treating that response as idle work.

<div class="outcome"><strong>Shutdown check</strong><p>Send <code>SIGTERM</code> while the worker is idle and during a bounded test activity. In both cases it should stop within the supervisor window without leaving an unreported completed side effect.</p></div>
