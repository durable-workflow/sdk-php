<?php

declare(strict_types=1);

require_once $argv[1];
[$script, $autoload, $url, $operation, $seconds] = $argv;
$client = new DurableWorkflow\Client($url, requestTimeoutSeconds: (float) $seconds);
$started = hrtime(true);
try {
    match ($operation) {
        'list' => $client->listWorkflows(),
        'start' => $client->startWorkflow('test.timeout', 'test-timeout', 'test-timeout'),
        'signal' => $client->signalWorkflow('test-timeout', 'test.signal'),
        'update' => $client->updateWorkflow('test-timeout', 'test.update'),
        default => throw new InvalidArgumentException('Unknown control fixture operation.'),
    };
    echo json_encode(['outcome' => 'response', 'elapsed' => (hrtime(true) - $started) / 1e9], JSON_THROW_ON_ERROR)."\n";
} catch (DurableWorkflow\Exception\ServerException $error) {
    echo json_encode(['outcome' => 'error', 'type' => $error::class, 'status' => $error->status,
        'reason' => $error->reason, 'elapsed' => (hrtime(true) - $started) / 1e9], JSON_THROW_ON_ERROR)."\n";
}
