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

Handler registration configures this PHP object. `run()` separately registers
the worker with the runtime before its first poll.

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

An activity option such as `['queue' => 'payments-v2']` selects the queue of a newly scheduled activity execution. The runtime stores that resolved queue. Pending activities, their retries and repaired tasks keep it. Changing a worker's `taskQueue` does not move recorded work, and existing workflow runs keep their own workflow queue. Retain compatible workers on old queues while those dependencies remain. Follow the [recorded-queue transition and diagnostics procedure](https://durable-workflow.com/docs/polyglot/task-matching-dispatch/#recorded-activity-queues).

## Let the managed loop own the protocol

`Worker::run()` registers, negotiates heartbeat cadence, polls workflow/activity/query tasks, renews workflow-task leases, and reports terminal outcomes. Typed transient poll pressure is retried with capped backoff while registration heartbeats and shutdown checks remain responsive.

### Large payloads on slower connections

Cooperative workers supervise external payload downloads while bytes arrive and
while the connection waits. Worker presence and workflow lease renewal use short
control requests. Activity status checks observe ownership and cancellation
without recording application heartbeats. Query leases and original cancellation
cleanup deadlines continue to limit loading.

The default transport allows 30 seconds for each supervised payload download.
Select a larger finite transfer timeout when your connection needs it:

```php
$client = new Client(
    baseUri: $serverUrl,
    transport: new Psr18Transport(payloadTransferTimeoutSeconds: 180),
);
```

The transfer timeout accepts 1 through 900 seconds. It does not extend a run,
activity, query, explicit request, or cancellation cleanup budget. Control RPCs
retain their bounded timeouts. A partial, unauthorized, or invalid body is
discarded before handler dispatch. Custom transports can implement the optional
`BoundedProgressPayloadTransport` interface to provide the same supervised
streaming support.

After a transient registration-heartbeat failure, the managed loop refreshes
that existing registration before acquiring another task. An already acquired
task continues without waiting for that retry. Server fencing, stopped and
draining responses remain authoritative, and the worker does not register again
to bypass them.

### Query execution authority

Query loading, preparation and result submission share the original query
deadline. On protocol 1.20 runtimes that advertise `query_task_authority`, a
bounded transport also observes the original claim without renewing its lease.
The worker checks ownership before entering the handler and discards a late
result. Registration heartbeats do not extend query authority.

Require this mode before registration when your application needs it:

```php
$worker = Worker::create(
    $client,
    'orders',
    requireQueryTaskAuthority: true,
);
```

This option refuses an unsupported runtime, protocol or transport before
registration. The default `Psr18Transport` supports bounded requests with Guzzle
and cURL. Custom transports must provide bounded requests and supervised payload
progress to bound external loading as well.

Without the option, protocol 1.19 and unbounded transports keep their compatible
mode. The worker enforces timing actually supplied in the delivery and reports
`worker.query_authority_unavailable` when live observation is unavailable. A
legacy delivery without timing cannot establish the original caller deadline.
Protocol 1.19 never calls the status endpoint.

Synchronous PHP handlers already running cannot be safely interrupted. Keep query
handlers short and avoid side effects. The worker checks authority again before
submitting their result. Transport timeouts use whole seconds. If the remaining
budget cannot cover bounded control I/O, the worker abandons that claim without
extending its deadline. Direct `Client` completion and failure calls retain the
Server's receipt and terminal-state semantics.

Keep `run()` inside your CLI command or supervised process:

```php
$worker->run(pollTimeoutSeconds: 1);
```

The supervisor starts the process and delivers shutdown signals. The managed
loop owns registration, heartbeat state and deregistration. Laravel and Symfony
applications can use their supplied [Laravel](/frameworks/laravel/) or
[Symfony](/frameworks/symfony/) worker commands.

`tick()` performs one polling iteration. It does not establish or close that
worker lifecycle. A fresh worker using `tick()` directly has no managed
registration; calling a low-level client registration method does not initialize
the Worker's heartbeat or cleanup state. Use `run()` for application workers.

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
