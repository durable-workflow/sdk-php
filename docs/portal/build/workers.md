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

An untyped outage does not stop the Worker either. That covers a connection that is refused, reset or timed out; a 502/503/504 with no protocol body; a 503 `storage_unavailable` or `backend_unavailable`, for example while the Server's database warms up; and any 5xx or 429 with `retryable: true`. The Worker retries using the schedule all SDKs share: 0.25 s doubling to a 4 s cap, with full random jitter. This applies to polls, and to any request that fails a tick, such as a completion; the Server re-delivers a claimed task when its lease ends. Authentication and authorization failures, other protocol refusals, and explicit `retryable: false` stay fatal. They are reported as a `worker.fatal_failure` diagnostic before `run()` rethrows.

**Startup and long outages.** A Worker started while the Server or its load balancer is unavailable keeps retrying registration on the same schedule instead of exiting. Registration is safe to repeat: the Server accepts the same worker ID registering again. An outage that lasts 60 s is reported as a `worker.outage_persisting` error, then every 5 minutes until a request succeeds.

**Cluster info.** Reads of `GET /api/cluster/info` are read-only, so any outage of them is retried: a refused, reset or timed-out connection, or any 5xx or 429 unless it says `retryable: false`. At startup, a Worker that needs discovery waits for it as it waits for registration. While running, the transport keeps the last payload policy it discovered and tries discovery again later. If it has none yet, the request that needed it is not sent and is retried. A lasting outage is reported as `worker.outage_persisting`. Authentication and authorization refusals stay fatal.

**A lowered payload limit.** If the Server refuses a completion for its size (`413 payload_too_large`), for example because its inline limit was lowered after the Worker read it, the Worker reads the payload policy again, encodes the same outcome under it (externalising large values when storage is configured) and sends it once more. The result is never recomputed. An activity result that is still too large fails the attempt as a non-retryable `ActivityResultTooLarge`; the Worker keeps running.

**Completions.** A completion that meets an outage is retried with the same attempt fence on that schedule. With activity lease maintenance, the retries stop when the claim's grant ends; without it, they stop after a bounded number of attempts. Retrying is safe: the Server records one outcome per attempt. A retry of a completion the Server already committed is answered `409 stale_attempt` with `recorded: false` and the attempt `completed`, and the Worker treats that as its own committed completion. A result that cannot be published before the retries end is logged as `worker.outcome_unpublished` and left to the Server, which re-delivers the attempt when its lease ends. It is never reported as an activity failure.

**Across SDKs.** PHP and Rust Workers stop on a 401 or 403 after logging it as an error. Python Workers keep retrying polls through a 401 or 403 so that credentials can be rotated without a restart, and log every refusal at error level.

### Activity continuations

Activity continuations are an explicit constructor opt-in:

```php
$worker = new Worker(
    client: $client,
    taskQueue: 'orders',
    enableActivityContinuation: true,
);

$worker->register(OrderWorkflow::class, OrderActivities::class);
$worker->run();
```

The default is `false`. A supporting native Server advertises
`activity_workflow_continuation`. After a successful activity, the worker may
replay the run's bounded history with that result and propose the next workflow
commands on its completion. The Server validates the authoritative history and
original authority, then commits the result and eligible commands together.
An exact completion retry recovers the original decision.

The initial contract supports sequential remote activities, timers, workflow
completion and version markers. Local activities, parallel groups, selections,
cancellation scopes, message cursors/waits and update/cancellation state retain
ordinary execution. If workflow code reads business time at or after the
speculative result, the worker abandons speculation because only the Server can
assign that event's time. It does not repeat the activity callback.

Missing or older capability, unsupported replay state and a documented Server
decline preserve ordinary completion and polling. Backend faults and unknown
completion outcomes follow completion error/recovery rules. They do not become
eligibility declines. Drain or disable opted-in workers before downgrading a
producer or introducing older serving nodes.

Version 1 raised batch wall time by 24–49% under load.
[Version 1 benchmark](https://github.com/durable-workflow/server/issues/1078#issuecomment-6827690149)

Version 2 reduced median wall time by 26–28% and cut database commits per
workflow roughly in half in the measured 200-workflow batches. Sequential
workflow latency also fell in that experiment.
[Version 2 benchmark](https://github.com/durable-workflow/server/issues/1181#issuecomment-7234206731)

The option remains off by default. Verify that every serving node advertises
v2 before enabling it on a queue.

### Activity continuation version 2

A native Server that implements version 2 advertises it inside the same
capability, as `supported_versions` containing `2` and a `v2` member. The
worker uses version 2 when all of these hold:

- `enableActivityContinuation` is on;
- the `v2` member is well formed, with `version` exactly `2` and the expected
  `complete_endpoint`;
- the registration negotiated sticky history suffixes;
- no worker interceptors are configured.

A malformed `v2` member turns off version 2 only. Version 1 stays the fallback
when version 2 is not advertised, or after a Server refuses it with
`unsupported_continuation_version`. That refusal holds until the worker
registers again.

Version 2 speculates from the worker's sticky cache and never asks the Server
for history:

1. A workflow task completion with a cached boundary asks for
   `continuation_tail`. The Server answers with the events it wrote after that
   boundary. With a tail requested, the carried activity poll leases only an
   activity this completion created.
2. The worker appends the tail only when it continues the cache exactly. A tail
   that does not continue the cache evicts the run's entry. An omitted tail
   (over a Server budget, or `tail_unavailable` on a retry) keeps the old
   boundary, and so does an empty tail at that boundary. Each appended tail
   renews the entry's TTL, so a long chain of accepted steps keeps its cache.
3. After an activity succeeds, the worker sends version 2 only when the cache
   starts with `WorkflowStarted`, is contiguous, and ends in this attempt's
   `ActivityStarted`. It also needs a reserved workflow poll slot, and the
   replay with the result must pass the version 1 checks. Otherwise it sends
   the ordinary completion, which costs nothing extra.
4. An accepted answer returns the next tail and may carry the next activity,
   which the worker runs in the same tick. After 16 such activities in one
   tick, the request carries no activity poll, so the tick ends and query and
   workflow polls get a turn.

Answers follow the
[contract](https://github.com/durable-workflow/server/blob/main/docs/contracts/activity-workflow-continuation-v2.md):

- **Decline.** The result is recorded. The worker processes the carried
  workflow poll answer. It evicts the cache entry for
  `cache_identity_mismatch` and `run_not_open`, and keeps it for every other
  reason.
- **Refusal** (a 4xx, or 500 `invalid_continuation_receipt` or
  `history_invariant_violated`). Nothing was written, so the worker sends the
  ordinary activity completion with the same carried workflow poll. The two
  500 reasons also evict the cache entry and log an error. 409 `stale_attempt`
  and `conflicting_completion` keep the ordinary stale-attempt and duplicate
  handling and are not retried.
- **Unknown outcome** (another 5xx, or a transport failure). The worker resends
  the identical request on the completion retry schedule, never an ordinary
  completion. A retry that outlasts its attempts is logged as
  `worker.outcome_unpublished`, and the Server re-delivers the attempt when
  its lease ends.

Diagnostics: `worker.activity_continuation` (with `version`, `outcome`,
`reason` and `tail`), `worker.activity_continuation_skipped`,
`worker.activity_continuation_refused`,
`worker.activity_continuation_tail_rejected` and
`worker.activity_continuation_tail_refused`.

### Poll timeouts and early disconnects

When the Server advertises `task_delivery`, the managed worker negotiates
offer/accept delivery. Receiving an offer does not reserve a task or start an
activity attempt. The SDK accepts the selected offer before running a handler.
If that response is lost, it retries the same packet and poll identity until
the Server confirms the claim or refuses it. The refusal can be expiry, a
rolled-back acceptance (unclaimable, ended lease, or conflicting offer), or
loss of registration authority. A retryable read-key outage on one replica
keeps the packet for a later replay. Without a definitive answer, the worker
releases the packet 30 seconds after its deadline and starts a new poll; a
claim that did commit expires and is redelivered.
After acceptance begins, an older node returning an error cannot switch that
poll to legacy delivery. If a node returns `404` or `405` for the offer route
before any packet is selected, the worker uses legacy polling for the rest of
that registration. A temporary `503 task_offer_issuance_unavailable` is
retried with backoff. A lease carried by a task completion is recovered
through the legacy poll route, where the Server bound it.
Servers without this capability continue using the existing poll protocol.
During acceptance, stop/drain hints and malformed replies retain that packet.
Only the protocol's closed HTTP `409` refusal shapes can release it or end
the original registration; a later replacement registration is preserved.

With the default cURL transport, supporting Servers send progress during idle
worker polls. Progress keeps an idle connection active while the complete
request remains bounded to the requested wait plus five seconds. Ordinary polls
honor a shorter injected Guzzle timeout. Cooperative polls retain their explicit
request budget. Older Servers continue returning ordinary JSON. Opaque PSR-18
clients retain ordinary JSON and their configured policies.

Keep proxy timeouts longer than the configured poll wait. A worker warning
named `worker.poll_ended_early` reports the observed duration and configured
wait when a connection failure or gateway cutoff ends a poll early. Inspect
the proxy and client timeout settings named in its remediation. Configure the
worker's PSR logger or diagnostic listener to collect these warnings.

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

### Recover from a stale registration

If the worker process stalls longer than the Server's registration-freshness
window (a host pause, a long garbage-collection stop or a network partition),
the Server fences its registration. `run()` then drops every task it held,
registers again and keeps polling. The Server redelivers the dropped tasks.
Repeated recoveries back off up to 60 seconds. Each one logs
`worker.reregistered` and increments `durable_workflow_worker_reregistrations`.
Construct the worker with `stopOnStaleRegistration: true` to stop instead and
let your supervisor restart it.

## Observe worker health

Use Server or Cloud visibility to verify registration freshness, build compatibility, and queue depth. Treat “the process exists” and “the worker is admitted and polling” as different health checks.
