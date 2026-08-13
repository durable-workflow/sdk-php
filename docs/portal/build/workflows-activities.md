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

`ActivityContext::heartbeat()` records progress and throws `ActivityCancelled` when the Server requests cancellation. Heartbeat before and during long calls that can be safely divided into chunks.

## Use durable commands for workflow time

The context provides these replay-aware commands:

| Command | Use it for |
| --- | --- |
| `activity()` | Retryable external work. |
| `sleep()` | A durable timer that survives worker downtime. |
| `childWorkflow()` | A separately identified durable execution. |
| `sideEffect()` | A small nondeterministic value recorded once in history. |
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
