---
layout: layout.njk
title: Signals, queries & updates
description: Communicate with running Durable Workflow PHP executions using durable signals, replayed queries, and tracked updates.
lead: Choose a message by its consistency contract—fire-and-record signals, read-only queries, or tracked updates that return an applied result.
previous:
  label: Workers
  url: /build/workers/
next:
  label: Failures & retries
  url: /build/failures-retries/
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

## Follow run transitions deliberately

Ordinary handle methods target the current run. `signalSelectedRun()`, `querySelectedRun()`, and `updateSelectedRun()` preserve the selected run guard. Choose selected-run methods for administrative tools whose answer must refer to one immutable run.

<div class="note"><strong>Message names are public contracts</strong><p>Version a message shape additively. Keep old workers able to decode histories that were already admitted.</p></div>
