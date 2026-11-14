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

Use `WorkflowContext::waitCondition()` when progress depends on workflow state rather than an external activity. Its deterministic predicate is re-evaluated when committed signals or updates produce another workflow task. A stable key identifies the wait across replay, and the optional timeout returns `false` instead of requiring an application timer loop. The timeout runs from the first time the wait opens: a signal or update that leaves the predicate false does not restart it. When the Server advertises `condition_wait_occurrence_deadline`, the Server also keeps that first open's wall-clock deadline for every reopen; see the [upgrade guide](/operate/upgrading/#condition-wait-occurrence-deadlines) for the rollout order.

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

### Durations

`sleep()` and `deferTimer()`, the `timeout` of `waitCondition()` and
`deferCondition()`, the remote and local activity timeout options, the child
workflow `execution_timeout_seconds` and `run_timeout_seconds` options, and the
`serviceOperation()` `wait_timeout_seconds` option take a duration in any of
these forms:

- `int` or `float` seconds, such as `1800`;
- a string such as `'30 minutes'`, `'1h 30m'`, `'1.5 hours'` or
  `'1 hour and 30 minutes'`;
- a `DateInterval` or `CarbonInterval`, such as `new DateInterval('PT30M')` or
  `CarbonInterval::minutes(30)`.

```php
use Carbon\CarbonInterval;

$verified = $context->waitCondition(
    static fn (): bool => $context->signals('verify') !== [],
    key: 'email-verified',
    timeout: '30 minutes',
);

$context->sleep(CarbonInterval::hours(2));

$receipt = $context->activity('payments.capture', [$orderId], [
    'start_to_close_timeout' => '30 seconds',
    'schedule_to_close_timeout' => '10 minutes',
]);
```

The SDK converts each duration to whole seconds once, rounding up, so
`'30 minutes'`, `1800`, `new DateInterval('PT30M')` and
`CarbonInterval::minutes(30)` record identical history. Strings convert exactly
as the embedded Laravel package's `timer()` and `await()` do, with
`ceil(CarbonInterval::fromString($duration)->totalSeconds)`. Intervals are
measured by their year, month, day, hour, minute, second and microsecond fields
with `CarbonInterval::instance()->totalSeconds`.

Carbon converts with its cascade factors, not the calendar: by default a day is
24 hours, a week 7 days, a month 4 weeks and a year 12 months. Those factors do
not depend on the current date, time zone or daylight-saving changes. Changing
the factors with
`CarbonInterval::setCascadeFactors()` changes the result, in the SDK and in the
embedded package, so leave them at their defaults in worker processes.

Conversion retains the installed Carbon major's floating-point behavior. Very
large accepted durations with millisecond or microsecond parts can produce
different whole-second results across Carbon majors. For example,
`'5555555555555 seconds 32037 milliseconds'` converts to `5555555555588`
seconds on Carbon 2 and `5555555555556` on Carbon 3. Each matches the embedded
package on that same Carbon major.

Regression samples span base durations from 10 billion seconds through the
reported value above, with cases around 100 billion and one trillion seconds.
These sample points do not establish a universal precision bound. Even a
one-microsecond part can disappear before
rounding at large magnitudes. Keep the Carbon major consistent when workers
replay runs using such duration strings; explicit integer seconds avoid this
string-conversion difference.

A `CarbonInterval` from `diff()` is measured by its fields too. For example, the
interval from January 1 to February 10, 01:00 is 1 month, 9 days and 1 hour, so
it converts to 37 days and 1 hour. On Carbon 3, the embedded package measures
the 40-day span between the interval's dates instead. Pass
`$start->diffInSeconds($end)` when you need the real span.

These are the string units, matched case-insensitively:

| Unit | Spellings |
|---|---|
| Microseconds | `microsecond`, `microseconds`, `micro`, `µs` |
| Milliseconds | `millisecond`, `milliseconds`, `milli`, `ms` |
| Seconds | `second`, `seconds`, `s` |
| Minutes | `minute`, `minutes`, `m` |
| Hours | `hour`, `hours`, `h` |
| Days | `day`, `days`, `d` |
| Weeks | `week`, `weeks`, `w` |
| Months | `month`, `months`, `mo`, `mos` |
| Quarters | `quarter`, `quarters` |
| Years | `year`, `years`, `y`, `yr`, `yrs` |
| Decades | `decade`, `decades` |
| Centuries | `century`, `centuries` |
| Millennia | `millennium`, `millennia` |

Parts may be separated by spaces or `and`, such as `'1 hour and 30 minutes'`,
and may use a decimal fraction, such as `'1.5 hours'`. A fraction of a month,
quarter, year, decade, century or millennium is rejected, because Carbon would
truncate it. Unknown units, extra words, commas, negative parts, parts with
more than 15 digits before the decimal point, durations of 2^53 seconds or more
(about 310 million years as Carbon counts them, with 336-day years), and
ISO 8601 strings throw
`InvalidArgumentException` naming the parameter and the accepted forms. Pass
ISO 8601 durations as `new DateInterval('PT30M')`. Negative intervals and
relative `DateInterval::createFromDateString()` values such as `'next weekday'`
or `'last day of next month'` are rejected too.

A numeric string such as `'30'` or `'1.5'` converts as PHP coerced it before
string durations were accepted, in a file without `strict_types`. Timers and
condition timeouts round it up to whole seconds, with negative values becoming
zero. Activity and child-workflow options pass it to the Server unchanged. In a
file with `declare(strict_types=1)`, such a string used to throw a `TypeError`;
it is now accepted with the same conversion.

A zero duration is valid for timers, condition waits and `wait_timeout_seconds`.
Activity and child-workflow timeouts need at least one second.

Keep a timer's duration stable across deployments. Changing `'30 minutes'` to
`'31 minutes'` while a run is waiting on that timer raises `NonDeterministicWorkflow`
on replay, just as changing `1800` to `1860` would.

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
`heartbeat_timeout`, `worker_session` and `cancellation_policy`. The four
timeouts accept the [duration forms](#durations) above. Unknown option
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
use `$client->withNamespace('billing')` instead of this workflow option.

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

## Start a child and keep its identity

`childWorkflow()` returns the child's result, so the parent waits until the
child finishes. When the parent needs the child's identity first, for example
to record it or notify someone, start the child with `startChildWorkflow()`:

```php
use DurableWorkflow\Worker\ParentClosePolicy;

$child = $context->startChildWorkflow('billing.reconcile', [$accountId], [
    'parent_close_policy' => ParentClosePolicy::Abandon,
]);

$identity = $child->identity();
$context->activity('notify.reconcile-started', [$identity->workflowId]);

$summary = $child->await();
```

The turn that calls `startChildWorkflow()` ends after the start command. The
runtime commits the child's start and gives the parent a new turn while the
child keeps running, so the handle returns only for a committed child.
`identity()` reads canonical history:

| Property | Meaning |
| --- | --- |
| `callId` | The parent's call for this child. It stays the same through child continue-as-new and retries. |
| `workflowId` | The child workflow instance ID. |
| `initialRunId` | The first run of the child. A later continued or retried run does not change it. |

`await()` returns the child's result or throws `ChildWorkflowFailed`. It
follows the same child call through continue-as-new and retries, and sends no
command of its own. Replay and a replacement worker rebuild the same handle
from history without starting another child.

Parent-close and cancellation policies, timeouts and retries work as they do
for `childWorkflow()`. The handle supports only `identity()` and `await()`:
it cannot signal, query, update, cancel or terminate the child. Await it in the
workflow execution that started it. `startChildWorkflow()` cannot run inside an
`all()` or `parallel()` group; use `deferChildWorkflow()` there.

This needs a runtime that advertises `child_workflow_start_turn`. Native Server
3.0.x and PHP Server 2.x do not. On those, the worker abandons the task with
`child_workflow_start_turn_unavailable` before it submits anything, so no child
starts. A cancellation-scope checkpoint keeps the parent's claim, so the
runtime refuses a start there with `child_start_turn_not_at_checkpoint`.

### Run the service-mode child example

The complete [worker](https://github.com/durable-workflow/sdk-php/blob/main/examples/child-workflow-worker.php)
and [client](https://github.com/durable-workflow/sdk-php/blob/main/examples/child-workflow-client.php)
use the quickstart's [bootstrap](https://github.com/durable-workflow/sdk-php/blob/main/examples/bootstrap.php).
Copy those three files beside the `vendor/` directory of your Composer project.
Use an SDK release containing `startChildWorkflow()` and a Server advertising
`child_workflow_start_turn`. The same runtime URL and namespace must be available
to both processes. Set `DURABLE_WORKFLOW_RUNTIME_URL`,
`DURABLE_WORKFLOW_NAMESPACE`, and `DURABLE_WORKFLOW_TASK_QUEUE` in both terminals.
Set `DURABLE_WORKFLOW_WORKER_TOKEN` in the worker terminal and
`DURABLE_WORKFLOW_CLIENT_TOKEN` in the client terminal, using their respective
role credentials.

Start the worker, then run a client command in the second terminal:

```bash
php child-workflow-worker.php
```

```bash
php child-workflow-client.php await
php child-workflow-client.php await reject
php child-workflow-client.php abandon
php child-workflow-client.php cancel
php child-workflow-client.php terminate
```

Each parent first demonstrates a sequential result. It then starts a child with a committed handle and records
the identity in an activity. The activity may be retried, so an application
replacing its sample output with an external write should make that write
idempotent by `call_id`.

`await` returns the typed greeting. `await reject` catches `ChildWorkflowFailed`
and returns its recorded failure. The other modes close the parent while its
child remains open: `abandon` allows it to complete, `cancel` applies legacy
terminal cancellation, and `terminate` terminates it. The client prints the
parent result and the child's observed status. Cancellation through the handle
is unsupported. Cooperative cleanup uses the separate opt-in cancellation APIs
and `ParentClosePolicy::RequestCancellation`, as described in the cancellation
guide. Initial run IDs remain snapshots through retry and continue-as-new;
instance-oriented client operations target the child's current run unless the
caller explicitly selects a run.

`deferChildWorkflow()` prepares an operation without starting it or providing a
child identity. Join it at an `all()` or `parallel()` barrier on a runtime that
supports child members in those groups, as shown in the concurrency examples
above. Native Server 3.2 supports a flat `all()` (or `parallel()`) barrier
whose members are all deferred children. It refuses other child group shapes
with `development_parallel_group_unqualified`: `select()`, nested or mixed
groups, groups inside a cancellation scope, and committed-start handles inside
a barrier. Earlier Native Server releases refuse child members in groups.
The runnable example uses sequential children and committed start handles; it
does not demonstrate a deferred group.

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

### Inspect open-run version usage

Before raising a minimum or removing a legacy branch, inspect the open runs for
that exact workflow type and change ID. `workflowVersionUsage()` requires the
Server's `workflow_version_usage` capability and throws
`ServerCapabilityUnavailable` when it is absent. Discovery and the metadata read
use the client's control credential, namespace and configured control-request
timeout. They do not use a worker credential or fetch application payloads.

```php
$page = $client->workflowVersionUsage(
    workflowType: 'shipment',
    changeId: 'carrier-api',
    minVersion: 2,
    state: 'legacy',
    limit: 50,
);

// Counts include every state, even though this page lists only legacy runs.
$unknown = $page->counts->unknown;
foreach ($page->runs as $run) {
    printf("%s %s %s\n", $run->instanceId, $run->runId, $run->state);
}

// Repeat the exact namespace/type/change/minimum/state query to continue.
$nextCursor = $page->nextCursor;
```

`minVersion` is the proposed minimum supported signed 32-bit version, including
negative legacy sentinels; it is not the largest deployed version. The response
distinguishes `legacy`, `current` and `unknown`. Missing markers, unavailable or
invalid history remain unknown with their original reason. A current build or
definition is not evidence that a run recorded the current branch.

Each page is a live observation over a fixed upper run-ID bound, not a durable
fleet snapshot. Runs can close or record decisions between pages, so counts can
change. Pass cursors unchanged with the exact same filters; invalid cursors are
errors, not a reason to restart silently. Unknowns remain relevant to rollout
decisions. Fence or drain admissions and restart the traversal before making a
removal decision; this API returns no `safe_to_remove` authorization. The
[shared v1 contract](https://github.com/durable-workflow/workflow/blob/c93cadad8a0104b6e7e5f38e022c3b06c26c86e6/docs/version-usage.md)
defines the classification and live paging limits.

A `waitCondition()` predicate is part of replay identity too. New waits use a
`dwphp2:sha256:` fingerprint of the callable's tokens. Whitespace and comments
outside literals, file locations and neighboring call-site arguments are
excluded. Changing the predicate's tokens still raises
`NonDeterministicWorkflow`, even for a wait that has already been satisfied.
The key and timeout remain separate replay checks; change them behind a version
marker when an open run can reach the call.

Reflection can leave several same-line closures indistinguishable. Those
closures share a combined candidate fingerprint, so changing either can affect
both. Formatting that resolves that ambiguity can also change the candidate
set. Put predicates on separate lines before starting runs when they need
independent identities. If the callable cannot be isolated, or PHP lacks the
tokenizer, the worker retains the v1 source-line fingerprint.
Every worker that replays v2 waits needs PHP's tokenizer enabled.

Waits first opened by older SDKs retain their exact v1 source-line check.
Re-indenting those lines, changing a comment or editing neighboring code on
the same lines can still fail legacy replay. A physical reopen echoes the
first recorded fingerprint, so upgrading does not change an existing wait's
identity. Upgrade the queue's workers together: older workers reject v2
fingerprints, and cannot safely take over runs that already contain them.

Deploy workflow code atomically and restart workers. The fingerprint reads the deployed source file. If files are replaced in place under any long-running worker that still has the old code loaded, the loaded code and the file on disk can disagree, and replay reports a mismatch until the worker restarts. `opcache.validate_timestamps=0` widens this window. Ship the PHP sources with the worker image; the fingerprint cannot be computed without them.

The [upgrade guide](/operate/upgrading/) covers the queue-wide deployment and
rollback boundary when both legacy and v2 condition histories exist.

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

## Inspect retry budget and the owning workflow

The `ActivityContext::activityExecutionId` property contains the
logical `activity_execution_id` received with the claim. Ordinary and
cooperatively supervised remote callbacks expose that same execution identity
across retry attempts, while `activityAttemptId` identifies each attempt.
Use the logical ID in a downstream idempotency key when the destination honors
that key, including your application scope where needed.

```php
function activityIdempotencyKey(
    \DurableWorkflow\Worker\ActivityContext $context,
    string $businessKey,
): string {
    return $context->activityExecutionId ?? $businessKey;
}
```

Older claims can omit the field. Missing, null, empty or non-string values
produce null, so supply a business key when it is unavailable. Prepared local
callbacks expose the execution ID from their validated Server admission.
Inline local and Server-free callbacks without that admission report null.
Session activity callbacks expose the field when their actual claim includes
it; omission also produces null. The property is read-only and performs no
additional runtime request.

PHP SDK 3.6.0 introduced retry metadata on `ActivityContext`. Use 3.6.1 or later
for the original owning workflow and run IDs from public activity claims.
`$context->retryInfo` retains the original attempt number and retry policy,
including unknown fields. `maxAttempts()` returns a positive configured limit
when available. Use `isUnlimited()` to distinguish an explicit
`max_attempts: null` policy from an unknown limit. `policyProvided` distinguishes
an omitted snapshot from a received legacy null; `policy()` preserves either
received value without filling in defaults.

`isLastConfiguredAttempt()` returns `true` at or beyond a bounded final attempt,
`false` for a remaining budget or explicit unlimited policy, and `null` when
the policy or attempt is unknown. Keep the unknown case separate:

```php
function logConfiguredFinalAttempt(
    \DurableWorkflow\Worker\ActivityContext $context,
    \Psr\Log\LoggerInterface $logger,
): void {
    if ($context->retryInfo->isLastConfiguredAttempt() === true) {
        $logger->warning('Activity reached its final configured attempt', [
            'attempt' => $context->retryInfo->attemptNumber,
            'max_attempts' => $context->retryInfo->maxAttempts(),
        ]);
    }
}
```

A remaining budget does not promise another execution. Non-retryable failures,
cancellation, workflow closure and elapsed deadlines can end execution earlier.
Local activities and Server-free activity tests have no received Server retry
snapshot and report an unknown policy.

`workflowId` and `runId` contain the received owning workflow instance and run
IDs. Local activities retain the parent workflow task's IDs when available.
Older claims and Server-free tests can leave either ID null. To signal the
original owning run, use both IDs and an application event identity that stays
stable across activity retries:

```php
function signalOwningRun(
    \DurableWorkflow\Client $client,
    \DurableWorkflow\Worker\ActivityContext $context,
    string $orderId,
): array {
    if ($context->workflowId === null || $context->runId === null) {
        throw new \RuntimeException('Owning workflow IDs are unavailable; supply explicit IDs for this older claim.');
    }

    $eventIdentity = json_encode([$context->workflowId, $context->runId, $orderId], JSON_THROW_ON_ERROR);

    return $client->signalWorkflow(
        $context->workflowId,
        'order_activity_completed',
        [$orderId],
        runId: $context->runId,
        requestId: 'order-activity-completed:'.hash('sha256', $eventIdentity),
    );
}
```

The client must use the same runtime and namespace with permission to signal.
Keep the request ID stable to identify retries of the same business event.
The published PHP Server 2.11.3 can still record another signal with the same
key. Signal receipt retention remains tracked in [shared issue 216](https://github.com/durable-workflow/.github/issues/216).
Make the receiving handler idempotent for the business event and reconcile an
ambiguous response before retrying. A stable key alone does not establish
once-only signal admission or external effects. Do not substitute a task or
attempt ID when an owning workflow identity is unavailable.

## Use durable commands for workflow time

The context provides these replay-aware commands:

| Command | Use it for |
| --- | --- |
| `activity()` | Retryable external work. |
| `saga()` | Reverse-order compensation through ordinary activity history. |
| `sleep()` | A durable timer that survives worker downtime. |
| `childWorkflow()` | A separately identified durable execution. |
| `startChildWorkflow()` | A started child whose identity the parent reads before awaiting its result. |
| `all()`, `parallel()` | An ordered, fail-fast join over deferred activities, child workflows, and timers. |
| `sideEffect()` | A small nondeterministic value recorded once in history. |
| `getVersion()`, `patched()`, `deprecatePatch()` | A durable workflow-code evolution decision. |
| `upsertSearchAttributes()` | Operator-visible indexed state. |
| `continueAsNew()` | A fresh run with bounded history. |

Do not use PHP's process sleep, random-number functions, current wall-clock time, or mutable global state directly to make workflow decisions. `sideEffect()` is for small values whose result belongs in history, not for network or database work.

### Read the schedule fire that started a run

A run started by a schedule can read which fire started it. Use the occurrence
time to choose the window to process, including for backfills, instead of the
current time:

```php
$origin = $context->scheduleOrigin();          // null when no schedule started the run
$windowEnd = $origin?->occurrenceTime;         // the tick or backfilled occurrence
```

The value comes from the run's history, so it is the same on every replay.

## Declare search-attribute types

Pass a type map when the workflow must preserve search-attribute type identity
across replay and redrive:

```php
$context->upsertSearchAttributes(
    ['Stage' => 'approved', 'Attempts' => 2],
    ['Stage' => 'keyword', 'Attempts' => 'int'],
);
```

Declare every non-deleted attribute. Canonical types are `string`, `keyword`,
`keyword_list`, `int`, `float`, `bool`, and `datetime`. Use `null` to delete a
key. The runtime also checks declarations against the namespace's registered
types. Search metadata uses the existing `typed_search_attributes` worker
capability and protocol contract.

An explicitly typed call refuses a changed value or a changed recorded type.
Malformed recorded type maps also refuse replay. Histories written before type
metadata existed still compare values, since their type identity is unknown.
Existing calls that omit the type map keep their value-only behavior. That
legacy path does not establish typed replay equivalence; signal-wait redrive
qualification is also separate.

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
