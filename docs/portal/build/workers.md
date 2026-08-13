---
layout: layout.njk
title: Workers
description: Register, run, supervise, scale, and observe managed Durable Workflow PHP workers.
lead: A worker is a long-running PHP process that advertises type names, polls one task queue, replays workflows, runs activities, and heartbeats its registration.
previous:
  label: Workflows & activities
  url: /build/workflows-activities/
next:
  label: Signals, queries & updates
  url: /build/messages/
---
## Register every handler before polling

Choose one registration style for each handler. Attribute discovery is the
preferred class-oriented path; direct registration is useful when an
application already produces callables.

<div class="choice-grid registration-options">
  <div class="choice">
    <h3>Discover attributed classes</h3>
    <p><code>register()</code> discovers every <code>#[Workflow]</code> and <code>#[Activity]</code> method in the supplied services.</p>
    <pre><code class="language-php">final class OrderWorkflow
{
    #[Workflow('orders.process')]
    public function run(WorkflowContext $context): array
    {
        $reservation = $context->activity('orders.reserve');

        return ['reservation' => $reservation];
    }
}

final class OrderActivities
{
    #[Activity('orders.reserve')]
    public function reserve(ActivityContext $context): array
    {
        // ...
    }
}

$worker = Worker::create($client, 'orders')
    -&gt;register(OrderWorkflow::class, OrderActivities::class);</code></pre>
  </div>
  <div class="choice">
    <h3>Register direct callables</h3>
    <p>Name each protocol type explicitly when handlers are generated or already available as callables.</p>
    <pre><code class="language-php">$worker = Worker::create($client, 'orders')
    -&gt;registerWorkflow(
        'orders.process',
        $orderWorkflow,
    )
    -&gt;registerActivity(
        'orders.reserve',
        $reserveInventory,
    );</code></pre>
  </div>
</div>

`register()` validates all supplied services before the worker contacts the
runtime. Passing an un-attributed class raises `InvalidWorkerDefinition` before
registration or polling begins. Add a Durable Workflow handler attribute, or
use the direct callable API instead.

Add the other handler types to the same worker before starting its loop:

```php
$worker = new Worker(
    client: $client,
    taskQueue: 'orders',
    buildId: getenv('APP_RELEASE') ?: null,
);

$worker
    ->declareSignal('orders.process', 'approve')
    ->registerQuery('orders.process', 'status', $statusQuery)
    ->registerUpdate('orders.process', 'change-address', $addressUpdate);

$worker->run();
```

Workflow and activity type names are protocol identities. Keep them stable even when PHP class names or namespaces change. Duplicate registrations fail during bootstrap rather than becoming order-dependent.

## One task queue is one routing boundary

Scale a queue by starting more workers with the same registered types and a compatible build ID. Split queues when work needs different dependencies, resource limits, deployment cadence, or trust boundaries.

Do not send a workflow to a queue that has no compatible workflow handler. Inspect `listWorkers()` and `describeTaskQueue()` before assuming a deployment received traffic.

## Let the managed loop own the protocol

`Worker::run()` registers, negotiates heartbeat cadence, polls workflow/activity/query tasks, renews workflow-task leases, and reports terminal outcomes. Typed transient poll pressure is retried with capped backoff while registration heartbeats and shutdown checks remain responsive.

Use `tick()` only when a framework supervisor must own the outer loop:

```php
while (!$shutdownRequested) {
    $worker->tick(pollTimeoutSeconds: 1);
}

$worker->requestShutdown();
```

The lower-level `poll*TaskResponse()` APIs are for adapters that intentionally own lease, attempt, completion, and failure semantics. Application workers should prefer the managed loop.

## Supervise the process

Run one process per container or a bounded number under systemd, Supervisor, Kubernetes, Laravel process management, or Symfony Process. A production supervisor should:

- restart unexpected exits with backoff;
- deliver `SIGTERM` and allow a drain window;
- report the worker build ID and task queue;
- cap memory and recycle processes deliberately;
- keep client and worker credentials separate.

<div class="warning"><strong>Do not daemonize in a web request</strong><p>Workers need a real process lifecycle. Start them through your platform's process supervisor, not a controller, queue callback, or request shutdown hook.</p></div>

## Observe worker health

Use Server or Cloud visibility to verify registration freshness, build compatibility, and queue depth. Treat “the process exists” and “the worker is admitted and polling” as different health checks.
