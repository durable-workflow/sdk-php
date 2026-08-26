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
