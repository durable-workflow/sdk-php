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
