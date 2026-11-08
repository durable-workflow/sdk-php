<?php

declare(strict_types=1);

// Installed native condition-wait qualification for #269 (cases C1-C5).
// Usage: php qualify-condition-wait-native.php AUTOLOAD SERVER_URL
// Use only a disposable token-auth runtime with the synthetic fixture
// credentials claimed-task-admin / -client / -worker. No production runtime is
// suitable. History assertions are SDK-owned; the physical database check is
// Server-owned and runs separately on the run IDs this script prints.

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
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

/**
 * Send one signal. A refusal reports only the Server's reason and the shape of
 * its validation errors (labels and messages), never tokens or payloads.
 *
 * @param list<mixed> $arguments
 */
function cwnSignal(Client $client, string $workflowId, string $name, array $arguments): void
{
    try {
        $client->signalWorkflow($workflowId, $name, $arguments);
    } catch (ServerException $error) {
        $errors = $error->details['validation_errors'] ?? null;
        $shape = is_array($errors) ? array_map(static fn (mixed $messages): array => array_values(array_filter(
            is_array($messages) ? $messages : [], 'is_string')), $errors) : null;
        $admission = $error->details['signal_admission'] ?? null;
        $admission = is_array($admission) ? array_filter($admission, 'is_scalar') : (is_scalar($admission) ? $admission : null);
        throw new ConditionWaitNativeFailure(substr(sprintf('signal %s refused: status %d, reason %s, validation_errors %s, signal_admission %s',
            $name, $error->status, $error->reason ?? 'none', json_encode($shape, JSON_UNESCAPED_SLASHES),
            json_encode($admission, JSON_UNESCAPED_SLASHES)), 0, 600), 0, $error);
    }
}

/**
 * An initial run must carry the worker's durable command contract, so its
 * signals are validated. Continue-as-new successors carry none (Native
 * continuations), so this applies to initial runs only.
 */
function cwnContract(Client $client, string $workflowId, string $runId, string $workflowType, string $case): array
{
    $contract = $client->workflowObservation($workflowId, $runId, historyPageSize: 1)['command_contract'] ?? null;
    try {
        $checked = Evidence::durableContract($contract, $workflowType);
    } catch (RuntimeException $error) {
        throw new ConditionWaitNativeFailure(substr(sprintf('%s initial run %s: source %s', $case, $error->getMessage(),
            json_encode(is_array($contract) ? ($contract['source'] ?? null) : null)), 0, 300), 0, $error);
    }
    cwnCheck(true, $case.' initial run carries the durable command contract');

    return $checked;
}

/** A continue-as-new successor's contract source, recorded as an observation only. */
function cwnSuccessorSource(Client $client, string $workflowId, string $runId): mixed
{
    $source = $client->workflowObservation($workflowId, $runId, historyPageSize: 1)['command_contract']['source'] ?? null;

    return is_scalar($source) || $source === null ? $source : 'non_scalar';
}

/**
 * Run one history check after checkpointing its evidence. Before the
 * assertion, the case, run identity and a bounded payload-free timeline of the
 * already-fetched history (event offsets and integer timing fields only, with
 * the first event's absolute time; at most 4 KiB) go to stderr, so they are
 * kept whether the check passes or fails. The assertion itself is unchanged.
 *
 * @param list<array<string, mixed>> $history
 * @param \Closure(list<array{type: string, at: float, payload: array<string, mixed>}>): array<mixed> $check
 * @return array<mixed>
 */
function cwnEvidence(string $case, string $workflowId, string $runId, array $history, \Closure $check): array
{
    $events = Evidence::events($history);
    fwrite(STDERR, substr('checkpoint '.json_encode(['case' => $case, 'workflow_id' => $workflowId, 'run_id' => $runId,
        'origin' => isset($events[0]) ? sprintf('%.6F', $events[0]['at']) : null, 'timeline' => Evidence::timeline($events)],
        JSON_UNESCAPED_SLASHES), 0, 4096).PHP_EOL);
    try {
        return $check($events);
    } catch (RuntimeException $error) {
        fwrite(STDERR, substr(sprintf('%s evidence refused: %s', $case, $error->getMessage()), 0, 300).PHP_EOL);
        throw $error;
    }
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
    // Start runs only after the worker is active. Each case's initial run is then
    // checked to retain the worker's durable command contract (cwnContract).
    cwnWait(static fn (): bool => count($client->listWorkers($queue, 'active')['workers'] ?? []) >= 1, 'managed worker is active');
    cwnCheck(true, 'managed worker is active before the first run starts');
    // C1
    $handle = $client->startWorkflow('cwn.false_wake', 'c1-'.$suffix, $queue, [20]);
    $run = (string) $handle->selectedRunId;
    $contract = cwnContract($client, $handle->workflowId, $run, 'cwn.false_wake', 'C1');
    cwnWait(static fn (): bool => cwnOpens($client, $handle->workflowId, $run) >= 1, 'C1 first open');
    usleep(5_000_000);
    cwnSignal($client, $handle->workflowId, 'go', ['wrong']);
    cwnWait(static fn (): bool => cwnStatus($client, $handle->workflowId, $run, ['completed']), 'C1 completes after the first deadline', 40);
    cwnCheck($handle->result(5) === ['satisfied' => false], 'C1 times out without a matching signal');
    $history = cwnHistory($client, $handle->workflowId, $run);
    $cases['C1'] = ['command_contract' => $contract, 'runs' => [cwnRun($handle->workflowId, $run, $history)], 'evidence' => cwnEvidence('C1', $handle->workflowId, $run, $history, static fn (array $events): array => Evidence::falseWake($events, 20))];
    cwnCheck(true, 'C1 false wake keeps the first-open deadline');
    $runIds[] = $c1RunIds[] = $run;

    // C2
    $handle = $client->startWorkflow('cwn.duplicate', 'c2-'.$suffix, $queue);
    $run = (string) $handle->selectedRunId;
    $contract = cwnContract($client, $handle->workflowId, $run, 'cwn.duplicate', 'C2');
    usleep(500_000);
    cwnSignal($client, $handle->workflowId, 'noise', ['during-activity']);
    cwnWait(static fn (): bool => cwnOpens($client, $handle->workflowId, $run) >= 1, 'C2 open after the activity');
    cwnSignal($client, $handle->workflowId, 'go', ['right']);
    cwnWait(static fn (): bool => cwnStatus($client, $handle->workflowId, $run, ['completed']), 'C2 completes');
    $history = cwnHistory($client, $handle->workflowId, $run);
    $cases['C2'] = ['command_contract' => $contract, 'runs' => [cwnRun($handle->workflowId, $run, $history)], 'evidence' => cwnEvidence('C2', $handle->workflowId, $run, $history, static fn (array $events): array => Evidence::settledRun($events))];
    cwnCheck(true, 'C2 duplicate queued task opens no overlapping wait');
    $runIds[] = $run;

    // C3
    foreach (['terminate' => 'terminated', 'cancel' => 'cancelled'] as $close => $status) {
        $handle = $client->startWorkflow('cwn.close', 'c3-'.$close.'-'.$suffix, $queue, [$close]);
        $run = (string) $handle->selectedRunId;
        $contract = cwnContract($client, $handle->workflowId, $run, 'cwn.close', 'C3 '.$close);
        cwnWait(static fn (): bool => cwnOpens($client, $handle->workflowId, $run) >= 1, 'C3 '.$close.' open');
        $close === 'terminate'
            ? $client->terminateWorkflow($handle->workflowId, 'condition-wait qualification', $run)
            : $client->cancelWorkflow($handle->workflowId, 'condition-wait qualification', $run);
        cwnWait(static fn (): bool => cwnStatus($client, $handle->workflowId, $run, [$status]), 'C3 '.$close.' closes');
        $history = cwnHistory($client, $handle->workflowId, $run);
        $cases['C3-'.$close] = ['command_contract' => $contract, 'runs' => [cwnRun($handle->workflowId, $run, $history)],
            'evidence' => cwnEvidence('C3 '.$close, $handle->workflowId, $run, $history, static fn (array $events): array => Evidence::closedWithOpenWait($events, $close))];
        cwnCheck(true, 'C3 '.$close.' cancels the open condition timer');
        $runIds[] = $run;
    }
    $handle = $client->startWorkflow('cwn.close', 'c3-continue-'.$suffix, $queue, ['continue']);
    $first = (string) $handle->selectedRunId;
    $contract = cwnContract($client, $handle->workflowId, $first, 'cwn.close', 'C3 continue');
    $next = cwnWait(static function () use ($client, $handle, $first): ?string {
        $execution = $client->describeWorkflow($handle->workflowId);
        return $execution->runId !== null && $execution->runId !== $first && $execution->status === 'completed' ? $execution->runId : null;
    }, 'C3 continue-as-new successor completes');
    $history = cwnHistory($client, $handle->workflowId, $first);
    $successor = cwnHistory($client, $handle->workflowId, $next);
    cwnCheck(cwnEvidence('C3 successor', $handle->workflowId, (string) $next, $successor, static fn (array $events): array => Evidence::waits($events)) === [], 'C3 successor run has no inherited wait');
    $cases['C3-continue'] = ['command_contract' => $contract,
        'successor_contract_sources' => [cwnSuccessorSource($client, $handle->workflowId, $next)], 'runs' => [cwnRun($handle->workflowId, $first, $history), cwnRun($handle->workflowId, $next, $successor)],
        'evidence' => cwnEvidence('C3 continue', $handle->workflowId, $first, $history, static fn (array $events): array => Evidence::closedWithOpenWait($events, 'continue'))];
    cwnCheck(true, 'C3 continue-as-new after a fired timeout');
    array_push($runIds, $first, $next);

    // C4
    $handle = $client->startWorkflow('cwn.own_budget', 'c4-'.$suffix, $queue);
    $run = (string) $handle->selectedRunId;
    $contract = cwnContract($client, $handle->workflowId, $run, 'cwn.own_budget', 'C4');
    cwnWait(static fn (): bool => cwnOpens($client, $handle->workflowId, $run) >= 1, 'C4 wait A opens');
    cwnSignal($client, $handle->workflowId, 'first', ['done']);
    cwnWait(static fn (): bool => cwnOpens($client, $handle->workflowId, $run) >= 2, 'C4 wait B opens');
    usleep(5_000_000);
    cwnSignal($client, $handle->workflowId, 'second', ['wrong']);
    cwnWait(static fn (): bool => cwnOpens($client, $handle->workflowId, $run) >= 3, 'C4 wait B reopens');
    cwnSignal($client, $handle->workflowId, 'second', ['right']);
    cwnWait(static fn (): bool => cwnStatus($client, $handle->workflowId, $run, ['completed']), 'C4 completes');
    $history = cwnHistory($client, $handle->workflowId, $run);
    $cases['C4'] = ['command_contract' => $contract, 'runs' => [cwnRun($handle->workflowId, $run, $history)],
        'evidence' => cwnEvidence('C4', $handle->workflowId, $run, $history, static fn (array $events): array => Evidence::ownBudget($events, 'first', 'second', 20))];
    cwnCheck(true, 'C4 newly authored wait keeps its own budget');
    $runIds[] = $run;

    // C5
    $handle = $client->startWorkflow('cwn.turns', 'c5-'.$suffix, $queue, [1, 2, true]);
    $runs = [(string) $handle->selectedRunId];
    $contract = cwnContract($client, $handle->workflowId, $runs[0], 'cwn.turns', 'C5');
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
            cwnSignal($client, $handle->workflowId, 'decision', [999]);
            cwnWait(static fn (): bool => cwnOpens($client, $handle->workflowId, $run) >= 2 * $position + 2, 'C5 turn '.$turn.' reopens');
            cwnSignal($client, $handle->workflowId, 'decision', [$turn]);
        }
    }
    cwnWait(static fn (): bool => cwnStatus($client, $handle->workflowId, $runs[1], ['completed']), 'C5 completes');
    $evidence = [];
    $recorded = [];
    foreach ($runs as $index => $run) {
        $history = cwnHistory($client, $handle->workflowId, $run);
        $evidence[] = cwnEvidence('C5 run '.$index, $handle->workflowId, $run, $history, static fn (array $events): array => Evidence::settledRun($events,
            $index === 0 ? 'WorkflowContinuedAsNew' : 'WorkflowCompleted'));
        $recorded[] = cwnRun($handle->workflowId, $run, $history);
    }
    $cases['C5'] = ['command_contract' => $contract,
        'successor_contract_sources' => [cwnSuccessorSource($client, $handle->workflowId, $runs[1])], 'runs' => $recorded, 'evidence' => $evidence];
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
