<?php

declare(strict_types=1);

// Diagnostic for Server #325: records acceptance/refusal of one deferred child.
// Usage: php probe-deferred-child-group.php AUTOLOAD DISPOSABLE_SERVER_URL
// Uses the same disposable role tokens as qualify-child-start-handle.php.

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;

[$script, $autoload, $url] = $argv;
require_once $autoload;
$suffix = bin2hex(random_bytes(6));
$namespace = 'php-child-group-'.$suffix;
$queue = 'child-group-'.$suffix;
$admin = new Client($url, controlToken: 'claimed-task-admin');
$admin->createNamespace($namespace);
$control = new Client($url, namespace: $namespace, controlToken: 'claimed-task-client');
$worker = new Client($url, namespace: $namespace, workerToken: 'claimed-task-worker');
$parent = $control->startWorkflow('probe.child-group.parent', 'parent-'.$suffix, $queue);
$workerId = $queue.'-worker';
$registration = $worker->registerWorker($workerId, $queue, ['probe.child-group.parent'], []);
try {
    $task = $worker->pollWorkflowTask($workerId, $queue, 1);
    if (!is_array($task)) {
        throw new LogicException('Probe did not receive its initial parent claim.');
    }
    $workflow = static fn (WorkflowContext $context): array => $context->all([
        $context->deferChildWorkflow('probe.child-group.child', ['child input']),
    ]);
    $commands = (new Replayer($worker->payloadCodec()))->replay($workflow, $task['history_events'] ?? [], [], $queue)->commands;
    try {
        $response = $worker->completeWorkflowTask($task['task_id'], $task['lease_owner'], $task['workflow_task_attempt'], $commands);
        $outcome = ['accepted' => true, 'receipt' => $response];
    } catch (ServerException $error) {
        $outcome = ['accepted' => false, 'status' => $error->status, 'reason' => $error->reason];
    }
    $page = $control->workflowHistory($parent->workflowId, (string) $parent->selectedRunId, 200);
    echo json_encode([
        'observed_at' => gmdate(DATE_ATOM), 'namespace' => $namespace,
        'workflow_id' => $parent->workflowId, 'run_id' => $parent->selectedRunId,
        'commands' => $commands, 'outcome' => $outcome,
        'history' => array_column($page['events'] ?? $page['history_events'] ?? [], 'event_type'),
    ], JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT).PHP_EOL;
} finally {
    $worker->deregisterWorkerRegistration($workerId, $registration['registration_token'] ?? null);
}
