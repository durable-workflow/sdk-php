<?php

declare(strict_types=1);

// Installed anchored condition-wait qualification for #269 (Native occurrence deadline).
// Usage: php qualify-condition-wait-anchored.php AUTOLOAD SERVER_URL
// Use only a disposable token-auth runtime with the synthetic fixture credentials
// claimed-task-admin / -client / -worker. The runtime must advertise
// condition_wait_occurrence_deadline; otherwise the script refuses (exit 2) before
// starting anything. Each case runs on its own fresh namespace queue with exactly one
// worker process, whose @internal delivered-history observer writes the witness the
// anchored assertions bind to. Recorded identities come from the Native history
// export of the run this script started (fresh runs only).
// Output: stdout carries only the final JSON document. Progress ("ok: ...") and one
// bounded refusal line (at most 300 bytes) go to stderr; the worker processes' own
// output is routed to stderr and is not capped here. Exit 0 pass, 1 refused or failed,
// 2 capability absent, 64 usage.

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Qualification\ConditionWaitAnchoredEvidence as Anchored;
use DurableWorkflow\Qualification\ConditionWaitNativeEvidence as Evidence;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\WorkflowContext;

if (count($argv) < 3) {
    fwrite(STDERR, "Usage: php qualify-condition-wait-anchored.php AUTOLOAD SERVER_URL\n");
    exit(64);
}
[$script, $autoload, $url] = $argv;
require_once $autoload;
require_once __DIR__.'/Support/ConditionWaitNativeEvidence.php';
require_once __DIR__.'/Support/ConditionWaitAnchoredEvidence.php';

final class ConditionWaitAnchoredFailure extends RuntimeException
{
}

if (($argv[3] ?? null) === '--worker') {
    [$namespace, $queue, $witnessPath] = array_slice($argv, 4);
    $witness = [];
    $record = Anchored::observer($witness);
    $worker = new Worker(new Client($url, namespace: $namespace, workerToken: 'claimed-task-worker'), $queue, workerId: $queue);
    // Every report is appended and flushed atomically; the Worker ignores observer failures.
    $worker->observeDeliveredWorkflowHistory(static function (string $kind, string $taskId, int $attempt, array $items) use ($record, &$witness, $witnessPath): void {
        $record($kind, $taskId, $attempt, $items);
        file_put_contents($witnessPath.'.tmp', json_encode($witness, JSON_THROW_ON_ERROR));
        rename($witnessPath.'.tmp', $witnessPath);
    });
    // The legacy qualifier's workflow types, so their declared signal contracts apply unchanged.
    $worker->registerWorkflow('cwn.false_wake', static function (WorkflowContext $context, int $timeout): array {
        $satisfied = $context->waitCondition(static fn (): bool => in_array(['right'], $context->signals('go'), true), key: 'go', timeout: $timeout);
        return ['satisfied' => $satisfied];
    });
    $worker->registerWorkflow('cwn.own_budget', static function (WorkflowContext $context): array {
        $first = $context->waitCondition(static fn (): bool => $context->signals('first') !== [], key: 'first', timeout: 30);
        $second = $context->waitCondition(static fn (): bool => in_array(['right'], $context->signals('second'), true), key: 'second', timeout: 20);
        return [$first, $second];
    });
    Evidence::declareSignals($worker);
    $worker->run(1);
    exit(0);
}

function cwaCheck(bool $condition, string $message): void
{
    if (!$condition) {
        throw new ConditionWaitAnchoredFailure($message);
    }
    fwrite(STDERR, "ok: {$message}\n");
}

function cwaWait(callable $condition, string $description, float $seconds = 30.0): mixed
{
    $deadline = microtime(true) + $seconds;
    do {
        $result = $condition();
        if ($result) {
            return $result;
        }
        usleep(100_000);
    } while (microtime(true) < $deadline);

    throw new ConditionWaitAnchoredFailure('Timed out: '.$description);
}

function cwaOpens(Client $client, string $workflowId, string $runId): int
{
    $page = $client->workflowHistory($workflowId, $runId, 500);

    return count(array_filter($page['events'] ?? $page['history_events'] ?? [],
        static fn (array $event): bool => ($event['event_type'] ?? null) === 'ConditionWaitOpened'));
}

function cwaCompleted(Client $client, string $workflowId, string $runId): bool
{
    return $client->describeWorkflow($workflowId, $runId)->status === 'completed';
}

/** @param list<mixed> $arguments */
function cwaSignal(Client $client, string $workflowId, string $name, array $arguments): void
{
    try {
        $client->signalWorkflow($workflowId, $name, $arguments);
    } catch (ServerException $error) {
        throw new ConditionWaitAnchoredFailure(substr(sprintf('signal %s refused: status %d, reason %s', $name, $error->status,
            $error->reason ?? 'none'), 0, 300), 0, $error);
    }
}

/**
 * One case on its own queue with one worker process: run the driver, then bind the
 * run's Native history export (fresh run, started here) and the worker's witness to
 * the anchored assertions in replay-input-only mode.
 *
 * @param \Closure(Client, string, string): array{0: string, 1: string} $drive (client, suffix, queue) returns [workflow ID, run ID]
 * @param \Closure(list<array<string, mixed>>, list<array<string, mixed>>): array<string, mixed> $assert
 * @return array<string, mixed>
 */
function cwaCase(string $case, Client $admin, string $url, string $script, string $autoload, string $root, \Closure $drive, \Closure $assert): array
{
    $suffix = bin2hex(random_bytes(6));
    $namespace = 'php-cwa-'.$suffix;
    $queue = 'cwa-'.$suffix;
    $witnessPath = $root.'/witness-'.$case.'.json';
    $admin->createNamespace($namespace);
    $client = new Client($url, namespace: $namespace, controlToken: 'claimed-task-client');
    $process = proc_open([PHP_BINARY, $script, $autoload, $url, '--worker', $namespace, $queue, $witnessPath], [1 => STDERR, 2 => STDERR], $pipes);
    cwaCheck(is_resource($process), $case.' worker process starts');
    try {
        cwaWait(static fn (): bool => count($client->listWorkers($queue, 'active')['workers'] ?? []) === 1, $case.' exactly one worker is active');
        [$workflowId, $runId] = $drive($client, $suffix, $queue);
        // Let the final completion's report reach the witness before the worker stops.
        usleep(1_000_000);
    } finally {
        if (proc_get_status($process)['running']) {
            proc_terminate($process);
        }
        proc_close($process);
    }
    $witness = json_decode((string) file_get_contents($witnessPath), true, flags: JSON_THROW_ON_ERROR);
    cwaCheck(is_array($witness) && array_is_list($witness), $case.' witness recorded');
    $export = $client->exportWorkflowHistory($workflowId, $runId);
    $encoded = json_encode($export, JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT).PHP_EOL;
    cwaCheck(file_put_contents($root.'/export-'.$case.'.json', $encoded) === strlen($encoded), $case.' raw export retained');
    $history = Anchored::freshNativeHistory($export, $runId);
    cwaCheck(true, $case.' recorded history is the Native export of the fresh run');
    try {
        $evidence = $assert($history, $witness);
    } catch (RuntimeException $error) {
        fwrite(STDERR, substr(sprintf('%s anchored evidence refused: %s', $case, $error->getMessage()), 0, 300).PHP_EOL);
        throw $error;
    }
    cwaCheck(true, $case.' anchored evidence holds');

    return ['namespace' => $namespace, 'queue' => $queue, 'workflow_id' => $workflowId, 'run_id' => $runId,
        'witness_entries' => count($witness), 'history_events' => count($history), 'evidence' => $evidence];
}

$startedAt = gmdate(DATE_ATOM);
$admin = new Client($url, controlToken: 'claimed-task-admin');
if (($admin->clusterInfo()->capabilities['condition_wait_occurrence_deadline'] ?? null) !== true) {
    fwrite(STDERR, "refused: the runtime does not advertise condition_wait_occurrence_deadline\n");
    exit(2);
}
cwaCheck(true, 'runtime advertises condition_wait_occurrence_deadline');
try {
$root = sys_get_temp_dir().'/cwa-'.bin2hex(random_bytes(6));
cwaCheck(mkdir($root, 0700), 'private witness directory');
$cases = [];

// C1: one false wake about 5 s after the first open keeps A; the worker authors the remaining time.
$cases['C1'] = cwaCase('C1', $admin, $url, $script, $autoload, $root, static function (Client $client, string $suffix, string $queue): array {
    $handle = $client->startWorkflow('cwn.false_wake', 'c1-'.$suffix, $queue, [20]);
    $run = (string) $handle->selectedRunId;
    cwaWait(static fn (): bool => cwaOpens($client, $handle->workflowId, $run) >= 1, 'C1 first open');
    usleep(5_000_000);
    cwaSignal($client, $handle->workflowId, 'go', ['wrong']);
    cwaWait(static fn (): bool => cwaCompleted($client, $handle->workflowId, $run), 'C1 completes after A', 40);
    cwaCheck($handle->result(5) === ['satisfied' => false], 'C1 times out without a matching signal');

    return [$handle->workflowId, $run];
}, static fn (array $history, array $witness): array => Anchored::falseWake($history, $witness, 'go', 20, 1, true));

// C1-repeat: two false wakes keep the same A; each reopen authors its own remaining time.
$cases['C1-repeat'] = cwaCase('C1-repeat', $admin, $url, $script, $autoload, $root, static function (Client $client, string $suffix, string $queue): array {
    $handle = $client->startWorkflow('cwn.false_wake', 'c1r-'.$suffix, $queue, [20]);
    $run = (string) $handle->selectedRunId;
    cwaWait(static fn (): bool => cwaOpens($client, $handle->workflowId, $run) >= 1, 'C1-repeat first open');
    usleep(5_000_000);
    cwaSignal($client, $handle->workflowId, 'go', ['wrong']);
    cwaWait(static fn (): bool => cwaOpens($client, $handle->workflowId, $run) >= 2, 'C1-repeat first reopen');
    usleep(5_000_000);
    cwaSignal($client, $handle->workflowId, 'go', ['wrong']);
    cwaWait(static fn (): bool => cwaCompleted($client, $handle->workflowId, $run), 'C1-repeat completes after A', 40);

    return [$handle->workflowId, $run];
}, static fn (array $history, array $witness): array => Anchored::falseWake($history, $witness, 'go', 20, 2, true));

// C4: after wait A is satisfied, newly authored wait B is a fresh chain with its own A and budget.
$cases['C4'] = cwaCase('C4', $admin, $url, $script, $autoload, $root, static function (Client $client, string $suffix, string $queue): array {
    $handle = $client->startWorkflow('cwn.own_budget', 'c4-'.$suffix, $queue);
    $run = (string) $handle->selectedRunId;
    cwaWait(static fn (): bool => cwaOpens($client, $handle->workflowId, $run) >= 1, 'C4 wait A opens');
    cwaSignal($client, $handle->workflowId, 'first', ['done']);
    cwaWait(static fn (): bool => cwaOpens($client, $handle->workflowId, $run) >= 2, 'C4 wait B opens');
    usleep(5_000_000);
    cwaSignal($client, $handle->workflowId, 'second', ['wrong']);
    cwaWait(static fn (): bool => cwaOpens($client, $handle->workflowId, $run) >= 3, 'C4 wait B reopens');
    cwaSignal($client, $handle->workflowId, 'second', ['right']);
    cwaWait(static fn (): bool => cwaCompleted($client, $handle->workflowId, $run), 'C4 completes');

    return [$handle->workflowId, $run];
}, static fn (array $history, array $witness): array => Anchored::ownBudget($history, $witness, 'first', 30, 'second', 20, true));

} catch (Throwable $error) {
    // One bounded refusal line instead of an unhandled fatal stack; nothing is printed on stdout.
    fwrite(STDERR, substr('refused: '.$error::class.': '.$error->getMessage().' (evidence '.$root.')', 0, 300).PHP_EOL);
    exit(1);
}

echo json_encode(['started_at' => $startedAt, 'finished_at' => gmdate(DATE_ATOM), 'php' => PHP_VERSION, 'mode' => 'anchored', 'evidence_directory' => $root,
    'run_ids' => array_values(array_column($cases, 'run_id')), 'cases' => $cases], JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT).PHP_EOL;
