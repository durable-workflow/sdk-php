---
layout: layout.njk
title: Signals, queries & updates
description: Communicate with running Durable Workflow PHP executions using durable signals, replayed queries, and tracked updates.
lead: Choose a message by its consistency contract—fire-and-record signals, read-only queries, or tracked updates that return an applied result.
previous:
  label: Workers
  url: /build/workers/
next:
  label: Message Streams
  url: /build/message-streams/
---
## Choose the message contract

| Message | Durable effect | Caller waits for | Handler context |
| --- | --- | --- | --- |
| Signal | Appended to workflow history | Admission | Workflow replay consumes `signals()` |
| Query | No history mutation | Read result | Query handler reads committed history |
| Update | Accepted and applied as tracked history | Accepted or completed | Update handler reads committed history |

## Declare and consume a signal

Declaration advertises the signal name and argument shape. The signature callback is reflected for registration metadata and is never invoked:

```php
$worker->declareSignal(
    'orders.process',
    'approve',
    static fn (string $reviewer): mixed => null,
);
```

Workflow replay reads the committed signal arguments:

```php
foreach ($context->signals('approve') as [$reviewer]) {
    $approvedBy = $reviewer;
}
```

Send it through a workflow handle:

```php
$client->workflowHandle('order-1001')->signal('approve', ['Ada']);
```

Signals are appropriate when the caller only needs acknowledgement that the runtime accepted the event.

Use [inbound Message Streams](/build/message-streams/) instead when one workflow
instance needs repeated ordered input with stable message identities and a
replay-safe consumption cursor.

## Register a read-only query

```php
$worker->registerQuery(
    'orders.process',
    'status',
    static function (QueryContext $context): array {
        return [
            'workflow_id' => $context->workflowId,
            'activities_completed' => count($context->events('ActivityCompleted')),
        ];
    },
);

$status = $handle->query('status');
```

A query observes committed history. It must not perform an activity or mutate workflow state.

## Apply a tracked update

```php
$worker->registerUpdate(
    'orders.process',
    'change-address',
    static fn (QueryContext $context, array $address): array => [
        'accepted' => true,
        'address' => $address,
    ],
);

$result = $handle->update(
    'change-address',
    [['city' => 'London']],
    waitFor: 'completed',
    requestId: 'order-1001-address-v2',
);
```

Reuse a stable request ID when a caller may retry the same update after losing its response. Use `waitFor: 'accepted'` when another process will observe completion later.

A request ID names one command. On a Server that advertises `command_request_keys` (native 3.9.1 and later), retrying a signal or update with the same request ID and the same input returns the original receipt, and reusing it with a different name, arguments or run target throws `SignalFailed` or `UpdateFailed` whose `isRequestIdConflict()` is true. `conflict()` names what differs and `retainedCommandId()` the original command. Nothing is written, and the SDK doesn't resend it. Use a new request ID for new input.

If a `waitFor: 'completed'` wait ends before the update finishes, `update()` throws `UpdateStillPending` instead of returning a value. Call `update()` again with `$pending->requestId` to keep waiting. The Server returns the same update and doesn't apply it twice.

Use `Client::updateOutcome($workflowId, $pending->updateId)` to read the same update without another admission. This typed read defaults to `waitFor: 'completed'`: it throws `UpdateStillPending` while work is pending, then returns the result, including a completed `null`.

A pending read preserves the stable update ID. The producer may omit the original request key on this GET path; `$pending->requestId` is then an empty string for an unknown key. Continue with `updateOutcome()` and that update ID. Never submit another update using an empty, inferred or internal hashed key.

## Follow run transitions deliberately

Ordinary handle methods target the current run. `signalSelectedRun()`, `querySelectedRun()`, and `updateSelectedRun()` preserve the selected run guard. Choose selected-run methods for administrative tools whose answer must refer to one immutable run.

<div class="note"><strong>Message names are public contracts</strong><p>Version a message shape additively. Keep old workers able to decode histories that were already admitted.</p></div>
