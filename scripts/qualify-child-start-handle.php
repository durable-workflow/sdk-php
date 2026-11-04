<?php

declare(strict_types=1);

// End-to-end check of WorkflowContext::startChildWorkflow() against a runtime
// that advertises child_workflow_start_turn (Server child-start-turn contract).
// Usage: php qualify-child-start-handle.php AUTOLOAD SERVER_URL
// The Server must accept the operator/worker/admin tokens below (a disposable
// token-auth runtime, as in docker-compose.claimed-task-recovery.yml). The
// worker runs in a child process because Worker::run() owns registration.

use DurableWorkflow\Client;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ParentClosePolicy;
use DurableWorkflow\Worker\WorkflowContext;

[$script, $autoload, $url] = $argv;
require_once $autoload;

if (($argv[3] ?? null) === '--worker') {
    [$namespace, $queue] = array_slice($argv, 4);
    $worker = new Worker(new Client($url, namespace: $namespace, workerToken: 'claimed-task-worker'), $queue, workerId: $queue);
    $worker->registerWorkflow('qualification.child-handle.parent', static function (WorkflowContext $context, string $account) use ($queue): array {
        $child = $context->startChildWorkflow('qualification.child-handle.child', [$account], [
            'queue' => $queue, 'parent_close_policy' => ParentClosePolicy::Abandon,
        ]);
        $identity = $child->identity();
        // Durable work while the child keeps running, before awaiting it.
        $context->sleep(1);

        return ['identity' => [$identity->callId, $identity->workflowId, $identity->initialRunId], 'result' => $child->await()];
    });
    $worker->registerWorkflow('qualification.child-handle.child', static function (WorkflowContext $context, string $account): string {
        $context->sleep(3);

        return 'reconciled '.$account;
    });
    try {
        $worker->run(1);
    } catch (Throwable $error) {
        fwrite(STDERR, 'worker stopped: '.$error::class.': '.$error->getMessage()."\n");
        exit(1);
    }
    exit(0);
}

function check(bool $condition, string $message): void
{
    if (!$condition) {
        fwrite(STDERR, "FAIL: {$message}\n");
        exit(1);
    }
    echo "ok: {$message}\n";
}

$admin = new Client($url, controlToken: 'claimed-task-admin');
check(($admin->clusterInfo()->capabilities['child_workflow_start_turn'] ?? null) === true,
    'cluster info advertises child_workflow_start_turn');
$suffix = bin2hex(random_bytes(6));
$namespace = 'php-child-handle-'.$suffix;
$queue = 'child-handle-'.$suffix;
$admin->createNamespace($namespace);
$client = new Client($url, namespace: $namespace, controlToken: 'claimed-task-client');
$process = proc_open([PHP_BINARY, $script, $autoload, $url, '--worker', $namespace, $queue], [1 => STDOUT, 2 => STDERR], $pipes);

try {
    $parent = $client->startWorkflow('qualification.child-handle.parent', 'parent-'.$suffix, $queue, ['acct-1']);
    $deadline = microtime(true) + 90;
    $execution = null;
    while (microtime(true) < $deadline) {
        usleep(500_000);
        $execution = $client->describeWorkflow($parent->workflowId);
        if ($execution->status !== 'running' && $execution->status !== 'waiting' && $execution->status !== 'pending') {
            break;
        }
    }
    $history = $client->workflowHistory($parent->workflowId, (string) $parent->selectedRunId, 200);
    $events = $history['events'] ?? $history['history_events'] ?? [];
    if ($execution?->status !== 'completed') {
        foreach ($events as $event) {
            fwrite(STDERR, ($event['recorded_at'] ?? '').' '.$event['event_type']."\n");
        }
    }
    check($execution?->status === 'completed', 'the parent completes (status: '.($execution?->status ?? 'none').')');

    $types = array_column($events, 'event_type');
    $scheduled = array_values(array_filter($events, static fn (array $event): bool => $event['event_type'] === 'ChildWorkflowScheduled'));
    check(count($scheduled) === 1, 'exactly one child start was committed');
    $recorded = [$scheduled[0]['payload']['child_call_id'], $scheduled[0]['payload']['child_workflow_instance_id'], $scheduled[0]['payload']['child_workflow_run_id']];
    $workedAt = array_search('TimerScheduled', $types, true);
    $resolvedAt = array_search('ChildRunCompleted', $types, true);
    check($workedAt !== false && $resolvedAt !== false && $workedAt < $resolvedAt,
        'the parent did durable work between the committed start and the child result');
    $result = $parent->result(5);
    check($result === ['identity' => $recorded, 'result' => 'reconciled acct-1'],
        'the parent returns the committed identity and the awaited child result');
    echo json_encode(['namespace' => $namespace, 'identity' => $recorded, 'history' => $types], JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT)."\n";
} finally {
    proc_terminate($process);
    proc_close($process);
}
