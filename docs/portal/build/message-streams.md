---
layout: layout.njk
title: Inbound Message Streams
description: Append repeated input to a Durable Workflow PHP service and consume it in deterministic, ordered batches.
lead: Give every logical application event a stable identity, then let workflow history own its ordered consumption cursor across replay and worker replacement.
previous:
  label: Signals, queries & updates
  url: /build/messages/
next:
  label: Failures & retries
  url: /build/failures-retries/
---
## Choose repeated input deliberately

Inbound Message Streams carry application input to a stable workflow instance.
They fit order events, human replies, device readings, and other inputs that can
arrive repeatedly and must be processed in Server-assigned order.

| Contract | Direction and scope | Delivery model |
| --- | --- | --- |
| Inbound Message Stream | Application to workflow instance | Repeated input with stable message identity, ordered positions, and a durable cursor |
| Workflow Stream | Workflow output associated with one run | Run-scoped output with offset-based subscribers and an explicit lifecycle |
| Signal | Application to workflow | One-shot input recorded in history and read through `signals()` |

Use a signal for a one-time event when admission is enough. Use a tracked update
when the caller needs an applied result. Use an inbound Message Stream when the
workflow must consume a repeated series deterministically. Do not substitute a
Workflow Stream: its output lifecycle and run scope solve a different problem.

## Consume one message or a bounded batch

Start the `orders.message-inbox` workflow with input `[1]` when it should consume
one message through `receiveOne()`. Pass a value from `2` through `20` when it
should consume the currently available ordered batch through `receive()`. A
batch receiver waits until at least one message is available, then returns no
more than its bound; it does not wait for the batch to fill.

This shipped example connects the worker with only its worker credential:

<!-- docs-example id="php.message-stream.worker.portal" -->
```php
<?php

declare(strict_types=1);

require __DIR__.'/bootstrap.php';

use DurableWorkflow\Attribute\Workflow;
use DurableWorkflow\Client;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\MessageStreamMessage;
use DurableWorkflow\Worker\WorkflowContext;

final class OrderMessageInboxWorkflow
{
    /** @return list<array{message_id: string, position: int, event: mixed}> */
    #[Workflow('orders.message-inbox')]
    public function run(WorkflowContext $context, int $batchSize = 1): array
    {
        $stream = $context->messageStream('order-events');
        $messages = $batchSize === 1
            ? [$stream->receiveOne()]
            : $stream->receive(maxItems: min(max($batchSize, 2), 20));

        return array_map(
            static fn (MessageStreamMessage $message): array => [
                'message_id' => $message->messageId,
                'position' => $message->position,
                'event' => $message->arguments[0],
            ],
            $messages,
        );
    }
}

$client = new Client(
    quickstartEnvironment('DURABLE_WORKFLOW_RUNTIME_URL'),
    namespace: quickstartEnvironment('DURABLE_WORKFLOW_NAMESPACE'),
    workerToken: quickstartEnvironment('DURABLE_WORKFLOW_WORKER_TOKEN'),
);

Worker::create($client, quickstartEnvironment('DURABLE_WORKFLOW_TASK_QUEUE'))
    ->register(OrderMessageInboxWorkflow::class)
    ->run();
```

The returned `MessageStreamMessage` exposes its stable `messageId`, ordered
`position`, and decoded positional `arguments`. Keep the stream name and payload
shape backward-compatible while active workflow histories can still reach this
code.

## Append from an application service

Start an `orders.message-inbox` instance using the normal
[`startWorkflow()` path](/getting-started/client-setup/#start-once-then-retain-the-handle),
then append from a command, controller, listener, job, or service. Persist a
business-event identity before the call and reuse it after any timeout whose
outcome is unknown. Do not generate a new random ID for a retry.

The application example connects with only the client credential:

<!-- docs-example id="php.message-stream.client.portal" -->
```php
<?php

declare(strict_types=1);

require __DIR__.'/bootstrap.php';

use DurableWorkflow\Client;

$orderId = quickstartEnvironment('ORDER_ID');
$eventId = quickstartEnvironment('ORDER_EVENT_ID');
$messageId = 'order-event:'.$eventId;

$client = new Client(
    quickstartEnvironment('DURABLE_WORKFLOW_RUNTIME_URL'),
    namespace: quickstartEnvironment('DURABLE_WORKFLOW_NAMESPACE'),
    controlToken: quickstartEnvironment('DURABLE_WORKFLOW_CLIENT_TOKEN'),
);

$outcome = $client
    ->workflowHandle('order:'.$orderId)
    ->appendMessage(
        'order-events',
        $messageId,
        [[
            'event_id' => $eventId,
            'kind' => 'item-added',
            'sku' => quickstartEnvironment('ORDER_SKU'),
            'quantity' => (int) quickstartEnvironment('ORDER_QUANTITY'),
        ]],
    );

echo json_encode($outcome, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES).PHP_EOL;
```

In Laravel or Symfony, inject `WorkflowClientInterface` into the application
service and call the same `workflowHandle()->appendMessage()` path. Container
resolution remains credential-lazy in Laravel; the append is the first real
application-client operation and therefore requires the client role. Keep the
worker role credential out of application processes.

## Reuse identities safely

The first accepted append receives the next ordered stream position. Repeating
the same message ID and identical payload returns the original position with a
duplicate outcome and does not deliver another item. Reusing that ID with a
different payload is rejected as `message_identity_conflict`; choose whether to
repair the upstream event record or submit a genuinely new event with a new
stable identity.

The SDK records cursor advancement in workflow-task completion. Replay resumes
from the recorded position, and a replacement worker reconstructs the same
state from history instead of relying on process memory. Application code uses
the public append and receive APIs; the SDK and runtime own their internal
transport and wait mechanics.

## Keep the instance across run transitions

Inbound Message Streams are instance-scoped. Continue-as-new transfers the
consumed cursor and pending input to the successor run, so the application keeps
addressing the same workflow ID and cannot accidentally re-consume an
acknowledged position. Workflow Streams remain run-scoped output and therefore
do not replace this inbound handoff contract.

For managed Cloud, use the complete provisioned runtime URL and its separate
client and worker credentials; Cloud operates the runtime storage and service.
For self-hosted Server, your team also owns Server deployment, database
durability, authentication, backups, and upgrades. In both cases the PHP
application owns stable message IDs and payload compatibility, the worker owns
deterministic consumption code, and the runtime owns ordering, waits, and cursor
durability. See [deployment ownership](/operate/deployment/) for the complete
process split.
