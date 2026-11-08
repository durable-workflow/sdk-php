<?php

declare(strict_types=1);

// Installed native condition-wait qualification for #269 (cases C1-C5).
// Usage: php qualify-condition-wait-native.php AUTOLOAD SERVER_URL
// Use only a disposable token-auth runtime with the synthetic fixture
// credentials claimed-task-admin / -client / -worker. No production runtime is
// suitable. History assertions are SDK-owned; the physical database check is
// Server-owned and runs separately on the run IDs this script prints.

use DurableWorkflow\Client;
use DurableWorkflow\Qualification\ConditionWaitNativeEvidence as Evidence;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\WorkflowContext;

if (count($argv) < 3) {
    fwrite(STDERR, "Usage: php qualify-condition-wait-native.php AUTOLOAD SERVER_URL\n");
    exit(64);
}
[$script, $autoload, $url] = $argv;
require_once $autoload;
require_once __DIR__.'/Support/ConditionWaitNativeEvidence.php';

final class ConditionWaitNativeFailure extends RuntimeException
{
}

if (($argv[3] ?? null) === '--worker') {
    [$namespace, $queue] = array_slice($argv, 4);
    $worker = new Worker(new Client($url, namespace: $namespace, workerToken: 'claimed-task-worker'), $queue, workerId: $queue);
    $worker->registerActivity('cwn.pause', static function (ActivityContext $context, int $milliseconds): int {
        usleep($milliseconds * 1000);
        return $milliseconds;
    });
    // C1: a false wake must keep the first open's deadline.
    $worker->registerWorkflow('cwn.false_wake', static function (WorkflowContext $context, int $timeout): array {
        $satisfied = $context->waitCondition(static fn (): bool => in_array(['right'], $context->signals('go'), true), key: 'go', timeout: $timeout);
        return ['satisfied' => $satisfied];
    });
    // C2: an unrelated signal during the activity queues a second workflow task.
    $worker->registerWorkflow('cwn.duplicate', static function (WorkflowContext $context): array {
        $context->activity('cwn.pause', [2000]);
        $satisfied = $context->waitCondition(static fn (): bool => in_array(['right'], $context->signals('go'), true), key: 'go', timeout: 60);
        return ['satisfied' => $satisfied];
    });
    // C3: close a run that has an open wait (terminate, cancel), or continue as new after a timeout.
    $worker->registerWorkflow('cwn.close', static function (WorkflowContext $context, string $mode): string {
        if ($mode === 'done') {
            return 'done';
        }
        $context->waitCondition(static fn (): bool => $context->signals('never') !== [], key: 'k', timeout: $mode === 'continue' ? 2 : 300);
        if ($mode === 'continue') {
            $context->continueAsNew(['done']);
        }
        return $mode;
    });
    // C4: after wait A's consumed terminal, newly authored wait B keeps its own budget.
    $worker->registerWorkflow('cwn.own_budget', static function (WorkflowContext $context): array {
        $first = $context->waitCondition(static fn (): bool => $context->signals('first') !== [], key: 'first', timeout: 30);
        $second = $context->waitCondition(static fn (): bool => in_array(['right'], $context->signals('second'), true), key: 'second', timeout: 20);
        return [$first, $second];
    });
    // C5: the sample-app decision-turn shape across continue-as-new.
    $worker->registerWorkflow('cwn.turns', static function (WorkflowContext $context, int $first, int $last, bool $continue): array {
        for ($turn = $first; $turn <= $last; ++$turn) {
            $current = $turn;
            $decided = $context->waitCondition(static fn (): bool => in_array([$current], $context->signals('decision'), true),
                key: 'decision-'.$current, timeout: 30);
            if (!$decided) {
                return ['timed_out' => $current];
            }
        }
        if ($continue) {
            $context->continueAsNew([$last + 1, $last + 1, false]);
        }
        return ['decided' => $last];
    });
    // Native refuses signals the command contract does not declare.
    Evidence::declareSignals($worker);
    $worker->run(1);
    exit(0);
}

function cwnCheck(bool $condition, string $message): void
{
    if (!$condition) {
        throw new ConditionWaitNativeFailure($message);
    }
    echo "ok: {$message}\n";
}

function cwnWait(callable $condition, string $description, float $seconds = 30.0): mixed
{
    $deadline = microtime(true) + $seconds;
    do {
        $result = $condition();
        if ($result) {
            return $result;
        }
        usleep(100_000);
    } while (microtime(true) < $deadline);

    throw new ConditionWaitNativeFailure('Timed out: '.$description);
}

/** @return list<array<string, mixed>> */
function cwnHistory(Client $client, string $workflowId, string $runId): array
{
    $page = $client->workflowHistory($workflowId, $runId, 500);
    cwnCheck(empty($page['next_page_token']), 'history fits one complete page');

    return $page['events'] ?? $page['history_events'] ?? [];
}

function cwnOpens(Client $client, string $workflowId, string $runId): int
{
    return count(array_filter(cwnHistory($client, $workflowId, $runId), static fn (array $e): bool => ($e['event_type'] ?? null) === 'ConditionWaitOpened'));
}

function cwnStatus(Client $client, string $workflowId, ?string $runId, array $statuses): bool
{
    return in_array($client->describeWorkflow($workflowId, $runId)->status, $statuses, true);
}

/** @return array<string, mixed> */
function cwnRun(string $workflowId, string $runId, array $history): array
{
    return ['workflow_id' => $workflowId, 'run_id' => $runId, 'events' => array_column($history, 'event_type')];
}

$startedAt = gmdate(DATE_ATOM);
$admin = new Client($url, controlToken: 'claimed-task-admin');
cwnCheck(($admin->clusterInfo()->capabilities['condition_wait_idempotent_reopen'] ?? null) === true,
    'runtime advertises condition_wait_idempotent_reopen');
$suffix = bin2hex(random_bytes(6));
$namespace = 'php-cwn-'.$suffix;
$queue = 'cwn-'.$suffix;
$admin->createNamespace($namespace);
$client = new Client($url, namespace: $namespace, controlToken: 'claimed-task-client');
$process = proc_open([PHP_BINARY, $script, $autoload, $url, '--worker', $namespace, $queue], [1 => STDERR, 2 => STDERR], $pipes);
cwnCheck(is_resource($process), 'managed worker process starts');
$cases = [];
$runIds = [];
$c1RunIds = [];
try {
    // C1
    $handle = $client->startWorkflow('cwn.false_wake', 'c1-'.$suffix, $queue, [20]);
    $run = (string) $handle->selectedRunId;
    cwnWait(static fn (): bool => cwnOpens($client, $handle->workflowId, $run) >= 1, 'C1 first open');
    usleep(5_000_000);
    $client->signalWorkflow($handle->workflowId, 'go', ['wrong']);
    cwnWait(static fn (): bool => cwnStatus($client, $handle->workflowId, $run, ['completed']), 'C1 completes after the first deadline', 40);
    cwnCheck($handle->result(5) === ['satisfied' => false], 'C1 times out without a matching signal');
    $history = cwnHistory($client, $handle->workflowId, $run);
    $cases['C1'] = ['runs' => [cwnRun($handle->workflowId, $run, $history)], 'evidence' => Evidence::falseWake(Evidence::events($history), 20)];
    cwnCheck(true, 'C1 false wake keeps the first-open deadline');
    $runIds[] = $c1RunIds[] = $run;

    // C2
    $handle = $client->startWorkflow('cwn.duplicate', 'c2-'.$suffix, $queue);
    $run = (string) $handle->selectedRunId;
    usleep(500_000);
    $client->signalWorkflow($handle->workflowId, 'noise', ['during-activity']);
    cwnWait(static fn (): bool => cwnOpens($client, $handle->workflowId, $run) >= 1, 'C2 open after the activity');
    $client->signalWorkflow($handle->workflowId, 'go', ['right']);
    cwnWait(static fn (): bool => cwnStatus($client, $handle->workflowId, $run, ['completed']), 'C2 completes');
    $history = cwnHistory($client, $handle->workflowId, $run);
    $cases['C2'] = ['runs' => [cwnRun($handle->workflowId, $run, $history)], 'evidence' => Evidence::settledRun(Evidence::events($history))];
    cwnCheck(true, 'C2 duplicate queued task opens no overlapping wait');
    $runIds[] = $run;

    // C3
    foreach (['terminate' => 'terminated', 'cancel' => 'cancelled'] as $close => $status) {
        $handle = $client->startWorkflow('cwn.close', 'c3-'.$close.'-'.$suffix, $queue, [$close]);
        $run = (string) $handle->selectedRunId;
        cwnWait(static fn (): bool => cwnOpens($client, $handle->workflowId, $run) >= 1, 'C3 '.$close.' open');
        $close === 'terminate'
            ? $client->terminateWorkflow($handle->workflowId, 'condition-wait qualification', $run)
            : $client->cancelWorkflow($handle->workflowId, 'condition-wait qualification', $run);
        cwnWait(static fn (): bool => cwnStatus($client, $handle->workflowId, $run, [$status]), 'C3 '.$close.' closes');
        $history = cwnHistory($client, $handle->workflowId, $run);
        $cases['C3-'.$close] = ['runs' => [cwnRun($handle->workflowId, $run, $history)],
            'evidence' => Evidence::closedWithOpenWait(Evidence::events($history), $close)];
        cwnCheck(true, 'C3 '.$close.' cancels the open condition timer');
        $runIds[] = $run;
    }
    $handle = $client->startWorkflow('cwn.close', 'c3-continue-'.$suffix, $queue, ['continue']);
    $first = (string) $handle->selectedRunId;
    $next = cwnWait(static function () use ($client, $handle, $first): ?string {
        $execution = $client->describeWorkflow($handle->workflowId);
        return $execution->runId !== null && $execution->runId !== $first && $execution->status === 'completed' ? $execution->runId : null;
    }, 'C3 continue-as-new successor completes');
    $history = cwnHistory($client, $handle->workflowId, $first);
    $successor = cwnHistory($client, $handle->workflowId, $next);
    cwnCheck(Evidence::waits(Evidence::events($successor)) === [], 'C3 successor run has no inherited wait');
    $cases['C3-continue'] = ['runs' => [cwnRun($handle->workflowId, $first, $history), cwnRun($handle->workflowId, $next, $successor)],
        'evidence' => Evidence::closedWithOpenWait(Evidence::events($history), 'continue')];
    cwnCheck(true, 'C3 continue-as-new after a fired timeout');
    array_push($runIds, $first, $next);

    // C4
    $handle = $client->startWorkflow('cwn.own_budget', 'c4-'.$suffix, $queue);
    $run = (string) $handle->selectedRunId;
    cwnWait(static fn (): bool => cwnOpens($client, $handle->workflowId, $run) >= 1, 'C4 wait A opens');
    $client->signalWorkflow($handle->workflowId, 'first', ['done']);
    cwnWait(static fn (): bool => cwnOpens($client, $handle->workflowId, $run) >= 2, 'C4 wait B opens');
    usleep(5_000_000);
    $client->signalWorkflow($handle->workflowId, 'second', ['wrong']);
    cwnWait(static fn (): bool => cwnOpens($client, $handle->workflowId, $run) >= 3, 'C4 wait B reopens');
    $client->signalWorkflow($handle->workflowId, 'second', ['right']);
    cwnWait(static fn (): bool => cwnStatus($client, $handle->workflowId, $run, ['completed']), 'C4 completes');
    $history = cwnHistory($client, $handle->workflowId, $run);
    $cases['C4'] = ['runs' => [cwnRun($handle->workflowId, $run, $history)],
        'evidence' => Evidence::ownBudget(Evidence::events($history), 'first', 'second', 20)];
    cwnCheck(true, 'C4 newly authored wait keeps its own budget');
    $runIds[] = $run;

    // C5
    $handle = $client->startWorkflow('cwn.turns', 'c5-'.$suffix, $queue, [1, 2, true]);
    $runs = [(string) $handle->selectedRunId];
    $turnsByRun = [[1, 2], [3]];
    foreach ($turnsByRun as $index => $turns) {
        if ($index > 0) {
            $previous = $runs[$index - 1];
            $runs[] = cwnWait(static function () use ($client, $handle, $previous): ?string {
                $execution = $client->describeWorkflow($handle->workflowId);
                return $execution->runId !== null && $execution->runId !== $previous ? $execution->runId : null;
            }, 'C5 continue-as-new successor starts');
        }
        $run = $runs[$index];
        foreach ($turns as $position => $turn) {
            cwnWait(static fn (): bool => cwnOpens($client, $handle->workflowId, $run) >= 2 * $position + 1, 'C5 turn '.$turn.' opens');
            $client->signalWorkflow($handle->workflowId, 'decision', [999]);
            cwnWait(static fn (): bool => cwnOpens($client, $handle->workflowId, $run) >= 2 * $position + 2, 'C5 turn '.$turn.' reopens');
            $client->signalWorkflow($handle->workflowId, 'decision', [$turn]);
        }
    }
    cwnWait(static fn (): bool => cwnStatus($client, $handle->workflowId, $runs[1], ['completed']), 'C5 completes');
    $evidence = [];
    $recorded = [];
    foreach ($runs as $index => $run) {
        $history = cwnHistory($client, $handle->workflowId, $run);
        $evidence[] = Evidence::settledRun(Evidence::events($history), $index === 0 ? 'WorkflowContinuedAsNew' : 'WorkflowCompleted');
        $recorded[] = cwnRun($handle->workflowId, $run, $history);
    }
    $cases['C5'] = ['runs' => $recorded, 'evidence' => $evidence];
    cwnCheck(true, 'C5 decision turns settle across continue-as-new');
    array_push($runIds, ...$runs);
} finally {
    if (proc_get_status($process)['running']) {
        proc_terminate($process);
    }
    proc_close($process);
}

echo json_encode(['started_at' => $startedAt, 'finished_at' => gmdate(DATE_ATOM), 'php' => PHP_VERSION, 'namespace' => $namespace,
    'run_ids' => $runIds, 'c1_run_ids' => $c1RunIds, 'c1_timeout' => 20, 'cases' => $cases],
    JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT).PHP_EOL;
