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

For repeated ordered application input, open an
[inbound Message Stream](/build/message-streams/) from `WorkflowContext`. Its
durable cursor resumes after replay, worker replacement, and continue-as-new.

Use `WorkflowContext::waitCondition()` when progress depends on workflow state rather than an external activity. Its deterministic predicate is re-evaluated when committed signals or updates produce another workflow task. A stable key identifies the wait across replay, and the optional timeout returns `false` instead of requiring an application timer loop.

Choose a distinct stable key for each logical wait, and preserve its predicate
and timeout across replay. An already true condition, or a false condition with
`timeout: 0`, returns without opening a new server wait. Those immediate returns
do not consume a later recorded operation. A recorded wait result or canonical
cancellation decision still governs the original call.

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
            ['start_to_close_timeout' => 20],
        );

        $context->sleep(5);

        return ['order' => $order['id'], 'reservation' => $reservation];
    },
);
```

Remote activity options are `connection`, `queue`, `retry_policy`,
`start_to_close_timeout`, `schedule_to_start_timeout`, `schedule_to_close_timeout`,
`heartbeat_timeout`, `worker_session` and `cancellation_policy`. Unknown option
or retry-policy field names fail locally with `InvalidArgumentException`.
See [failures and retries](/build/failures-retries/) for the retry policy fields.

The sequence, type, and details of context operations are durable. Changing them while old histories are still running can raise `NonDeterministicWorkflow`. Deploy replay-compatible code or drain incompatible histories before removing an old branch.

## Keep invocation state local

Create execution-local variables and temporary mutable objects inside the
workflow method. Workflow replay reconstructs durable values through `WorkflowContext`.
Apply the same rule to queries and updates.

Attributed workflow handlers also receive a new instance for every task replay,
query and update. Create nested execution-local objects inside the constructor
or factory. Injected services retain their configured identities and must not
hold execution-local mutable state. Static properties and objects captured
outside a factory remain shared.

Constructors and factories run repeatedly. Keep them free of external side
effects; perform those effects in activities.

### Migrate workflow registration from PHP SDK 2

Register the workflow class when the SDK or container can construct it. Replace
prebuilt workflow objects with an explicit construction recipe when dependencies
need custom wiring:

```php
use DurableWorkflow\Worker\WorkflowFactory;

$worker->register(new WorkflowFactory(
    static fn (): OrderWorkflow => new OrderWorkflow($sharedClient),
));
```

PSR-11 workflow bindings must return a new workflow on every resolution. Keep
collaborators such as API clients shared where the application requires it.
Laravel workflow bindings must be transient. Symfony's autoconfigured workflow
services become non-shared while their collaborators keep their configured
lifetimes. Activities and the low-level callable API retain their existing
lifetimes. Reused factory instances fail registration; a factory that becomes
invalid after startup produces a recoverable replay error.

The upgrade does not rewrite committed history. Preserve the workflow's recorded
command sequence and verify representative existing histories before deployment.

## Call a registered service

Use `serviceOperation()` inside a workflow to invoke a Server service endpoint.
The call pauses until Server records its result. Worker restarts replay the same
call identity and result without submitting another operation.

```php
use DurableWorkflow\Exception\ServiceOperationFailed;
use DurableWorkflow\Model\ServiceOperationOptions;
use DurableWorkflow\Worker\WorkflowContext;

function invoice(WorkflowContext $context, int $invoiceId): mixed
{
    try {
        $call = $context->serviceOperation('billing', 'invoices', 'create',
            ['invoice_id' => $invoiceId],
            new ServiceOperationOptions(waitTimeoutSeconds: 30, targetNamespace: 'billing'));

        return $call->responsePayload;
    } catch (ServiceOperationFailed $failure) {
        return ['call_id' => $failure->serviceCallId,
            'error' => $failure->failureType, 'cancelled' => $failure->cancelled];
    }
}
```

Register this handler with `registerWorkflow()` as usual. The endpoint, service
and operation must already exist, and the worker credential must have permission
to call them. `targetNamespace` selects the target namespace and serializes to
the same `namespace` field as array options. Omit it to use the caller's namespace.
Empty targets are invalid. Server assigns caller identity from the leased run;
target selection does not grant caller authority. For direct client service calls,
construct the `Client` in the target namespace instead of using this workflow option.

The default options are `mode_override=sync` and `wait_for=completed`. Use
`wait_for=accepted` or `mode_override=async` to receive the original admission
instead. Its `serviceCallId` identifies the operation and its `serviceCall` map
contains the recorded diagnostics. A later completion cannot replace that
original asynchronous result during replay.

`wait_timeout_seconds` is the Server wait budget, not a sleep in the SDK. Replay
does not start a fresh budget. Keep the service names, request, routing and wait
options stable for open runs, or guard changes with workflow version markers.
Service operations currently run sequentially outside cancellation scopes and
operation groups. The worker reports `service_operations_unavailable` when the
connected runtime does not advertise the command.

With cooperative cancellation enabled, a completion-waiting call can deliver
cancellation at its original service-call boundary. The runtime must advertise
`nexus_cooperative_cancellation` for the `workflow_run` binding. The caller
keeps its original cleanup deadline while its owned target finishes cleanup.
Admission-only calls do not gain this waiting boundary, and returning an
existing workflow does not grant cancellation ownership over it.

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

For a boolean rollout, use `patched('require-reservation-review')`. It returns `false` for legacy histories and `true` for new histories. After the legacy branch is no longer supported, replace the conditional call with `deprecatePatch()` for the same ID before eventually removing the marker. Compatible numeric and patch helpers share the same frozen version for a change ID. Every call must support that version, and `patched()` returns whether it equals `1`. Do not reuse a change ID for an unrelated rollout.

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

Workers that opt into [cooperative cancellation](https://durable-workflow.com/docs/2.0/polyglot/cancellation/)
supervise callbacks and observe cancellation independently of application
heartbeats. Prepared local activities use supervised callback processes.

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
