---
layout: layout.njk
title: Workflows & activities
description: Author deterministic, straight-line PHP workflows and isolate side effects in retryable Durable Workflow activities.
lead: Workflow code decides what should happen. Activity code performs the fallible outside work. The Server records enough history to replay the decision code safely after a crash.
previous:
  label: Client setup
  url: /getting-started/client-setup/
next:
  label: Workers
  url: /build/workers/
---
## Keep workflow code deterministic

A workflow handler runs as ordinary straight-line PHP inside an isolated Fiber. Calls on `WorkflowContext` pause internally when durable work is pending; on replay, the SDK walks committed history and returns recorded values directly from those calls.

Use `WorkflowContext::waitCondition()` when progress depends on workflow state rather than an external activity. Its deterministic predicate is re-evaluated when committed signals or updates produce another workflow task. A stable key identifies the wait across replay, and the optional timeout returns `false` instead of requiring an application timer loop.

```php
$approved = $context->waitCondition(
    static fn (): bool => $context->signals('approve') !== [],
    key: 'order-approved',
    timeout: 300,
);

if (!$approved) {
    return ['status' => 'timed_out'];
}
```

```php
$worker->registerWorkflow(
    'orders.process',
    static function (WorkflowContext $context, array $order): array {
        $reservation = $context->activity(
            'inventory.reserve',
            [$order['id']],
            ['start_to_close_timeout_seconds' => 20],
        );

        $context->sleep(5);

        return ['order' => $order['id'], 'reservation' => $reservation];
    },
);
```

The sequence, type, and details of context operations are durable. Changing them while old histories are still running can raise `NonDeterministicWorkflow`. Deploy replay-compatible code or drain incompatible histories before removing an old branch.

## Fan out durable work and join it once

`all()` (also available as `parallel()`) captures one durable operation from each closure, schedules every leaf in one workflow-task completion, then suspends the Fiber until the group can join. The returned array always follows declaration order, even when the Server records completions in another order:

```php
[$profile, [$recommendations, $delay]] = $context->all([
    static fn () => $context->activity('customers.profile', [$customerId]),
    static fn () => $context->parallel([
        $context->deferChildWorkflow('recommendations.build', [$customerId]),
        $context->deferTimer(1),
    ]),
]);
```

Use either a closure containing one `activity()`, `childWorkflow()`, or `sleep()` call, or the corresponding `deferActivity()`, `deferChildWorkflow()`, or `deferTimer()` value. The normal API never requires protocol command arrays or `yield`. Mixed and nested groups retain their input shape, and one outer group may contain at most `WorkflowContext::MAX_PARALLEL_OPERATIONS` (1,000) durable leaves. An empty top-level group returns `[]`; an empty nested barrier fails before transport because no history identity could prove that nested shape during replay.

The barrier is fail-fast. The first failure in durable history order is thrown into the workflow as `ActivityFailed`, `ChildWorkflowFailed`, or `WorkflowCancelled`; a tie uses declaration order. Every sibling was already scheduled, so fail-fast does not cancel it. A late or duplicate terminal event cannot reorder results or replace the first recorded terminal outcome. There is deliberately no collect-all barrier: model expected per-member errors as activity or child results when the workflow needs every outcome as data.

On retry or worker restart, replay validates the exact group ID, base sequence, size, leaf index, and nested path. Pending or completed members keep their original sequence and are not scheduled again. Changing a dynamic fan-out count, order, operation kind, or nesting while an open run can reach the group raises `NonDeterministicWorkflow` before any replacement commands leave the worker.

## Evolve workflow code with version markers

Give each long-lived code change a stable ID and keep the old branch while histories can still reach it. `getVersion()` records the maximum supported version for a new run and returns that same decision after redelivery, worker restart, or cold replay. Including `-1` in the first range lets histories that passed this point before the marker existed stay on the legacy branch:

```php
$version = $context->getVersion('inventory-reservation', -1, 1);

$reservation = $version === -1
    ? $context->activity('inventory.reserve-legacy', [$orderId])
    : $context->activity('inventory.reserve', [$orderId]);
```

A later deployment can raise the maximum while keeping the recorded versions in range. Existing runs keep their decision; only runs that have not recorded this change ID select the new maximum.

For a boolean rollout, use `patched('require-reservation-review')`. It returns `false` for legacy histories and `true` for new histories. After the legacy branch is no longer supported, replace the conditional call with `deprecatePatch()` for the same ID before eventually removing the marker. Do not reuse a change ID for an unrelated rollout or switch one ID between `getVersion()` and the patch helpers.

## Put side effects in activities

```php
$worker->registerActivity(
    'inventory.reserve',
    static function (ActivityContext $context, string $orderId): array {
        $context->heartbeat(['order_id' => $orderId, 'stage' => 'starting']);

        return reserveInventory($orderId);
    },
);
```

Activities may call databases, HTTP services, queues, and the filesystem. An activity attempt can run again after a retryable failure, so use an idempotency key tied to the workflow, activity, or business identity at the downstream boundary.

## Compensate completed work with a saga

Create a saga inside the workflow Fiber and register each compensation only
after its forward activity returns. `run()` catches a later failure, schedules
the registered activities in reverse order, then preserves the original
failure as the workflow outcome:

```php
use DurableWorkflow\Worker\Saga;
use DurableWorkflow\Worker\WorkflowContext;

return $context->saga()->run(
    static function (Saga $saga) use ($context, $tripId): array {
        $flight = $context->activity('trip.reserve-flight', [$tripId]);
        $saga->addCompensation('trip.cancel-flight', [$flight]);

        $hotel = $context->activity('trip.reserve-hotel', [$tripId]);
        $saga->addCompensation('trip.cancel-hotel', [$hotel]);

        $context->activity('trip.charge', [$tripId]);

        return compact('flight', 'hotel');
    },
);
```

Compensations use the same arguments, queue, timeout, and retry-policy options
as `WorkflowContext::activity()`. They can therefore be implemented by a PHP,
Python, or Rust worker registered on the selected queue; there is no
PHP-specific Server command.

The activity history is the durable compensation record. On replay the
workflow recreates the same registrations, consumes any compensation activity
that already completed, and resumes at the next reverse-order step. Keep the
forward decision order and each compensation type stable while a run is open.

`Saga::run()` compensates `ActivityFailed`, cancellation thrown by
`throwIfCancellationRequested()`, and other escaping failures. Successful
compensation rethrows the original failure. A failed compensation throws
`SagaCompensationFailed`, whose typed fields retain the forward failure,
compensation failure, compensation activity type, and registration order.
Service-mode compensation is always sequential LIFO and stops at that first
terminal compensation failure, so an earlier registered compensation is not
scheduled after the failure. Nested sagas own separate registration lists: an
inner saga compensates first; if its failure escapes, the outer saga then
compensates its own completed work.

For an embedded-style workflow that intentionally completes with a
`compensated` result, keep the explicit catch and call `compensate($failure)`
before returning that result. Repeated calls after a successful pass are
no-ops:

```php
$saga = $context->saga();

try {
    // Run forward activities and call $saga->addCompensation(...) after each success.
} catch (\DurableWorkflow\Exception\ActivityFailed $failure) {
    $saga->compensate($failure);

    return ['status' => 'compensated', 'reason' => $failure->getMessage()];
}
```

`ActivityContext::heartbeat()` records progress and throws `ActivityCancelled` when the Server requests cancellation. Heartbeat before and during long calls that can be safely divided into chunks.

## Use durable commands for workflow time

The context provides these replay-aware commands:

| Command | Use it for |
| --- | --- |
| `activity()` | Retryable external work. |
| `saga()` | Reverse-order compensation through ordinary activity history. |
| `sleep()` | A durable timer that survives worker downtime. |
| `childWorkflow()` | A separately identified durable execution. |
| `all()`, `parallel()` | An ordered, fail-fast join over deferred activities, child workflows, and timers. |
| `sideEffect()` | A small nondeterministic value recorded once in history. |
| `getVersion()`, `patched()`, `deprecatePatch()` | A durable workflow-code evolution decision. |
| `upsertSearchAttributes()` | Operator-visible indexed state. |
| `continueAsNew()` | A fresh run with bounded history. |

Do not use PHP's process sleep, random-number functions, current wall-clock time, or mutable global state directly to make workflow decisions. `sideEffect()` is for small values whose result belongs in history, not for network or database work.

## Bound long histories

Use `continueAsNew()` for recurring or event-heavy workflows. Pass only the state needed by the next run:

```php
$context->continueAsNew(
    arguments: [['cursor' => $nextCursor]],
    workflowType: 'reports.rollup',
    taskQueue: 'reports',
);
```

Clients can keep using ordinary `WorkflowHandle` methods across the run boundary. Use selected-run methods only when crossing that boundary would be a correctness bug.

<div class="outcome"><strong>Design check</strong><p>If repeating a line after a process crash would be unsafe, that line belongs behind an idempotent activity or another durable command.</p></div>
