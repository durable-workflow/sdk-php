<?php

declare(strict_types=1);

// Check source compatibility against a runtime without child_workflow_start_turn.
// Usage: php qualify-child-start-compatibility.php AUTOLOAD SERVER_URL
// Use only a disposable token-auth runtime with the fixture credentials from
// docker-compose.claimed-task-recovery.yml. No production runtime is suitable.

use DurableWorkflow\Client;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\WorkflowContext;

[$script, $autoload, $url] = $argv;
require $autoload;

if (($argv[3] ?? null) === '--worker') {
    [$namespace, $queue, $mode, $receipt] = array_slice($argv, 4);
    $worker = null;
    $worker = new Worker(
        new Client($url, namespace: $namespace, workerToken: 'claimed-task-worker'),
        $queue,
        workerId: $queue,
        diagnosticListener: static function (string $event, array $context) use (&$worker, $receipt): void {
            if ($event === 'worker.handler_failed') {
                $exception = $context['exception'];
                file_put_contents($receipt, json_encode([
                    'unsupported' => str_contains($exception->getMessage(), 'child_workflow_start_turn_unavailable'),
                    'exception_type' => $exception::class,
                ], JSON_THROW_ON_ERROR));
                $worker->requestShutdown();
            }
        },
    );
    $worker->registerWorkflow('compat.parent', static function (WorkflowContext $context, string $value) use ($mode): mixed {
        if ($mode === 'gate') {
            return $context->startChildWorkflow('compat.child', [$value])->await();
        }

        return $context->childWorkflow('compat.child', [$value]);
    });
    $worker->registerWorkflow('compat.child', static fn (WorkflowContext $context, string $value): array => [$value, PHP_INT_MAX, null]);
    $worker->run(1);
    exit(0);
}

function check(bool $condition, string $message): void
{
    if (!$condition) {
        throw new RuntimeException($message);
    }
    echo "ok: {$message}\n";
}

$startedAt = gmdate(DATE_ATOM);
$admin = new Client($url, controlToken: 'claimed-task-admin');
$capability = $admin->clusterInfo()->capabilities['child_workflow_start_turn'] ?? null;
check($capability !== true, 'published Server does not advertise the child start turn');
$results = [];
foreach (['gate', 'legacy'] as $mode) {
    $suffix = bin2hex(random_bytes(6));
    $namespace = 'php-child-compat-'.$suffix;
    $queue = 'child-compat-'.$suffix;
    $admin->createNamespace($namespace);
    $client = new Client($url, namespace: $namespace, controlToken: 'claimed-task-client');
    $parent = $client->startWorkflow('compat.parent', 'parent-'.$suffix, $queue, ['typed PHP result']);
    $receipt = sys_get_temp_dir().'/child-compat-'.$suffix.'.json';
    $process = proc_open([PHP_BINARY, $script, $autoload, $url, '--worker', $namespace, $queue, $mode, $receipt], [1 => STDOUT, 2 => STDERR], $pipes);
    check(is_resource($process), 'managed worker process starts');
    try {
        $deadline = microtime(true) + 45;
        do {
            usleep(100_000);
            $execution = $client->describeWorkflow($parent->workflowId);
            if (($mode === 'gate' && is_file($receipt)) || ($mode === 'legacy' && $execution->status === 'completed')) {
                break;
            }
        } while (microtime(true) < $deadline);
        if ($mode === 'gate') {
            check(is_file($receipt), 'the worker reports the unsupported start turn');
            $refusal = json_decode((string) file_get_contents($receipt), true, flags: JSON_THROW_ON_ERROR);
            check($refusal['unsupported'] === true, 'the refusal names child_workflow_start_turn_unavailable');
            $workerDeadline = microtime(true) + 5;
            while (proc_get_status($process)['running'] && microtime(true) < $workerDeadline) {
                usleep(100_000);
            }
        } else {
            check($execution->status === 'completed', 'legacy childWorkflow completes on the published Server');
            check($parent->result(5) === ['typed PHP result', PHP_INT_MAX, null], 'legacy childWorkflow preserves its typed result');
        }
        $history = $client->workflowHistory($parent->workflowId, (string) $parent->selectedRunId, 200);
        $events = $history['events'] ?? $history['history_events'] ?? [];
        $types = array_column($events, 'event_type');
        $childStarts = count(array_filter($types, static fn (string $type): bool => $type === 'ChildWorkflowScheduled'));
        check($childStarts === ($mode === 'gate' ? 0 : 1), $mode === 'gate' ? 'unsupported start committed no child' : 'legacy call committed exactly one child');
        $results[$mode] = ['child_starts' => $childStarts, 'history' => $types];
    } finally {
        if (proc_get_status($process)['running']) {
            proc_terminate($process);
        }
        proc_close($process);
        if (is_file($receipt)) {
            unlink($receipt);
        }
    }
}
echo json_encode(['started_at' => $startedAt, 'finished_at' => gmdate(DATE_ATOM), 'php' => PHP_VERSION, 'cases' => $results], JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT)."\n";

