<?php

declare(strict_types=1);

// Source runner only. See deferred-child-all.md for the published-tuple gates.
// Usage: php qualify-deferred-child-all.php AUTOLOAD DISPOSABLE_SERVER_URL [CASE]

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Qualification\DeferredChildAllScenario as Scenario;
use DurableWorkflow\Qualification\DeferredChildAllTurnTransport;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Worker;

if (count($argv) < 3) {
    fwrite(STDERR, 'Usage: php qualify-deferred-child-all.php AUTOLOAD DISPOSABLE_SERVER_URL [CASE]'.PHP_EOL);
    exit(64);
}
[$script, $autoload, $url] = $argv;
require_once $autoload;
require_once __DIR__.'/Support/DeferredChildAllScenario.php';
require_once __DIR__.'/Support/DeferredChildAllTurnTransport.php';
umask(0077);

if (($argv[3] ?? null) === '--worker') {
    [$case, $namespace, $queue, $queuesJson, $workerId, $receipt, $initialRun] = array_slice($argv, 4);
    $scenario = new Scenario($case, json_decode($queuesJson, true, flags: JSON_THROW_ON_ERROR));
    $worker = null;
    $transport = new DeferredChildAllTurnTransport(new Psr18Transport(), $receipt, static function () use (&$worker): void {
        /** @var Worker|null $worker Assigned after transport construction, before any request. */
        $worker?->requestShutdown();
    });
    $client = new Client($url, namespace: $namespace, workerToken: 'claimed-task-worker', transport: $transport);
    $worker = new Worker($client, $queue, workerId: $workerId);
    $worker->registerWorkflow($scenario->parentType(), $scenario->parent());
    $worker->registerWorkflow($scenario->childType(), $scenario->child($initialRun !== '' ? $initialRun : null));
    $worker->run(1);
    exit(0);
}

$deadline = microtime(true) + 600;

function childAllBudget(): void
{
    global $deadline;
    Scenario::check(microtime(true) < $deadline, 'Deferred-child adapter exceeded its absolute time budget.');
}

/**
 * @param resource $process
 * @phpstan-impure
 */
function childAllProcessRunning($process): bool
{
    return proc_get_status($process)['running'];
}

/** @return list<array<string, mixed>> */
function childAllHistory(Client $client, string $workflow, string $run): array
{
    childAllBudget();
    $page = $client->workflowHistory($workflow, $run, 200);
    Scenario::check(empty($page['next_page_token']), 'History exceeds the bounded complete page.');

    return $page['events'] ?? $page['history_events'] ?? [];
}

/**
 * Exactly one worker exists at a time. Each turn has a fresh process/cache and
 * exits after its completion acknowledgement, including an empty command turn.
 *
 * @return array<string, mixed>
 */
function childAllTurn(Scenario $scenario, string $namespace, string $queue, string $initialRun = ''): array
{
    global $script, $autoload, $url, $deadline;
    childAllBudget();
    $receipt = tempnam(sys_get_temp_dir(), 'php-child-all-turn-');
    $log = tempnam(sys_get_temp_dir(), 'php-child-all-log-');
    $process = null;
    try {
        Scenario::check(is_string($receipt) && is_string($log), 'Could not allocate private worker receipts.');
        $process = proc_open([
            PHP_BINARY, $script, $autoload, $url, '--worker', $scenario->case, $namespace, $queue,
            json_encode($scenario->queues, JSON_THROW_ON_ERROR), $queue.'-'.bin2hex(random_bytes(4)), $receipt, $initialRun,
        ], [0 => ['file', '/dev/null', 'r'], 1 => ['file', $log, 'a'], 2 => ['file', $log, 'a']], $pipes);
        Scenario::check(is_resource($process), 'Could not start the managed worker.');
        $turnDeadline = min($deadline, microtime(true) + 35);
        do {
            $status = proc_get_status($process);
            if (!$status['running']) {
                Scenario::check($status['exitcode'] === 0, 'Managed worker exited unsuccessfully.');
                $bytes = file_get_contents($receipt);
                Scenario::check(is_string($bytes) && $bytes !== '', 'Managed worker exited without a recorded turn.');

                return json_decode($bytes, true, flags: JSON_THROW_ON_ERROR);
            }
            usleep(50_000);
        } while (microtime(true) < $turnDeadline);
        throw new RuntimeException('Managed worker exceeded its turn deadline.');
    } finally {
        if (is_resource($process)) {
            if (childAllProcessRunning($process)) {
                proc_terminate($process);
                $stop = microtime(true) + 2;
                while (childAllProcessRunning($process) && microtime(true) < $stop) {
                    usleep(50_000);
                }
                if (childAllProcessRunning($process)) {
                    proc_terminate($process, 9);
                }
            }
            proc_close($process);
        }
        if (is_string($receipt)) {
            @unlink($receipt);
        }
        if (is_string($log)) {
            @unlink($log);
        }
    }
}

/** A public poll observes readiness without running or retaining a parent claim. */
function childAllNoParentTask(Client $worker, Scenario $scenario, string $queue): void
{
    childAllBudget();
    $id = $queue.'-observer-'.bin2hex(random_bytes(4));
    $registration = $worker->registerWorker($id, $queue, [$scenario->parentType()], []);
    try {
        $response = $worker->pollWorkflowTaskResponse($id, $queue, 0);
        Scenario::check(array_key_exists('task', $response) && $response['task'] === null, 'Unexpected extra or premature parent task.');
    } finally {
        $worker->deregisterWorkerRegistration($id, $registration['registration_token'] ?? null);
    }
}

/**
 * @param array<string, mixed> $receipt
 * @param list<array{string, string}> $also
 */
function childAllDuplicate(Client $worker, Client $control, array $receipt, string $workflow, string $run, array $also = []): void
{
    $targets = [[$workflow, $run], ...$also];
    $before = array_map(static fn (array $target): array => childAllHistory($control, ...$target), $targets);
    $body = $receipt['body'];
    try {
        $response = $worker->completeWorkflowTask($receipt['task']['task_id'], $body['lease_owner'], $body['workflow_task_attempt'], $body['commands']);
        Scenario::check(($response['recorded'] ?? null) === false && ($response['reason'] ?? null) === 'already_completed', 'Duplicate completion was not an explicit no-op.');
    } catch (ServerException $error) {
        Scenario::check($error->status === 409 && $error->reason === 'task_not_leased', 'Duplicate completion was not fenced.');
    }
    foreach ($targets as $index => $target) {
        Scenario::check(childAllHistory($control, ...$target) === $before[$index], 'Duplicate completion changed canonical history.');
    }
}

/**
 * @param array<string, mixed> $identity
 * @return array<string, mixed>
 */
function childAllMember(Scenario $scenario, Client $worker, Client $control, string $namespace, int $index, array $identity, string $parent, string $parentRun): array
{
    $receipt = childAllTurn($scenario, $namespace, $scenario->queues[$index], $identity['child_workflow_run_id']);
    $task = $receipt['task'];
    Scenario::check(($task['workflow_id'] ?? null) === $identity['child_workflow_instance_id']
        && ($task['task_queue'] ?? null) === $scenario->queues[$index], 'Managed child received a different member or queue.');
    $arguments = $control->payloadCodec()->decodeEnvelope($task['arguments']);
    Scenario::check(($arguments[0] ?? null) === ['index' => $index, 'value' => $scenario->fixture['input']], 'Child arguments lost their exact values or types.');
    childAllDuplicate($worker, $control, $receipt, $task['workflow_id'], $task['run_id'], [[$parent, $parentRun]]);

    return $receipt;
}

/** @return array<string, mixed> */
function childAllCase(string $case): array
{
    global $url;
    $sizes = ['single' => 1, 'out_of_order' => 3, 'failed' => 2, 'cancelled' => 2, 'terminated' => 2, 'retry' => 2, 'continue' => 2];
    Scenario::check(isset($sizes[$case]), 'Unknown deferred-child case.');
    $suffix = bin2hex(random_bytes(6));
    $namespace = 'php-child-all-'.$suffix;
    $parentQueue = 'parent-'.$suffix;
    $queues = array_map(static fn (int $index): string => 'child-'.$suffix.'-'.$index, range(0, $sizes[$case] - 1));
    $scenario = new Scenario($case, $queues);
    $admin = new Client($url, controlToken: 'claimed-task-admin');
    $admin->createNamespace($namespace);
    $control = new Client($url, namespace: $namespace, controlToken: 'claimed-task-client');
    $worker = new Client($url, namespace: $namespace, workerToken: 'claimed-task-worker');
    $parent = $control->startWorkflow($scenario->parentType(), 'parent-'.$suffix, $parentQueue, [$scenario->fixture['input']]);
    $run = (string) $parent->selectedRunId;
    $start = childAllTurn($scenario, $namespace, $parentQueue);
    $prefix = childAllHistory($control, $parent->workflowId, $run);
    $commands = $start['body']['commands'];
    $identities = $scenario->identities($prefix, $commands);
    $scenario->assertStable($prefix, $commands, $identities);
    childAllDuplicate($worker, $control, $start, $parent->workflowId, $run);
    childAllNoParentTask($worker, $scenario, $parentQueue);
    $closed = null;
    $memberRuns = [];
    foreach ($scenario->order() as $offset => $index) {
        childAllBudget();
        $identity = $identities[$index];
        if ($index === 0 && in_array($case, ['cancelled', 'terminated'], true)) {
            if ($case === 'cancelled') {
                $control->cancelWorkflow($identity['child_workflow_instance_id'], 'child barrier closed λ', $identity['child_workflow_run_id']);
            } else {
                $control->terminateWorkflow($identity['child_workflow_instance_id'], 'child barrier closed λ', $identity['child_workflow_run_id']);
            }
        } else {
            $receipt = childAllMember($scenario, $worker, $control, $namespace, $index, $identity, $parent->workflowId, $run);
            Scenario::check($receipt['task']['run_id'] === $identity['child_workflow_run_id'], 'Initial child task used another run.');
            $memberRuns[$index] = [$receipt['task']['run_id']];
            if ($index === 0 && in_array($case, ['retry', 'continue'], true)) {
                childAllNoParentTask($worker, $scenario, $parentQueue);
                $replacement = childAllMember($scenario, $worker, $control, $namespace, $index, $identity, $parent->workflowId, $run);
                Scenario::check($replacement['task']['run_id'] !== $identity['child_workflow_run_id'], 'Child chain did not create a distinct second run.');
                $memberRuns[$index][] = $replacement['task']['run_id'];
            }
        }
        $history = childAllHistory($control, $parent->workflowId, $run);
        $scenario->assertStable($history, $commands, $identities);
        if ($closed !== null) {
            Scenario::check($history === $closed, 'A late abandoned sibling mutated the closed parent.');
            childAllNoParentTask($worker, $scenario, $parentQueue);
            continue;
        }
        $ready = $scenario->failsEarly() || $offset === $scenario->size - 1;
        if (!$ready) {
            childAllNoParentTask($worker, $scenario, $parentQueue);
            continue;
        }
        $finish = childAllTurn($scenario, $namespace, $parentQueue);
        $finishCommands = $finish['body']['commands'];
        Scenario::check(count($finishCommands) === 1 && $finishCommands[0]['type'] === 'complete_workflow', 'Cold parent did not finish with one result.');
        $result = $control->payloadCodec()->decodeEnvelope($finishCommands[0]['result']);
        Scenario::check($result === $scenario->expected() && $parent->result(5) === $scenario->expected(), 'Cold parent result changed declaration order, values or failure type.');
        childAllDuplicate($worker, $control, $finish, $parent->workflowId, $run);
        $closed = childAllHistory($control, $parent->workflowId, $run);
        $scenario->assertStable($closed, $commands, $identities);
        childAllNoParentTask($worker, $scenario, $parentQueue);
    }
    Scenario::check($closed !== null, 'Parent never reached its expected terminal result.');
    if (in_array($case, ['retry', 'continue'], true)) {
        $starts = array_values(array_filter($closed, static fn (array $event): bool => $event['event_type'] === 'ChildRunStarted' && $event['payload']['sequence'] === 1));
        Scenario::check(array_column(array_column($starts, 'payload'), 'child_workflow_run_id') === $memberRuns[0], 'Canonical child chain differs from the two managed runs.');
    }

    return ['case' => $case, 'namespace' => $namespace, 'workflow_id' => $parent->workflowId, 'run_id' => $run,
        'initial_children' => $identities, 'child_queues' => $queues, 'member_runs' => $memberRuns,
        'completion_order' => $scenario->order(), 'result' => $scenario->expected(),
        'events' => array_column($closed, 'event_type'), 'parent_completion_turns' => 2];
}

$started = gmdate(DATE_ATOM);
$cases = isset($argv[3]) ? [$argv[3]] : ['single', 'out_of_order', 'failed', 'cancelled', 'terminated', 'retry', 'continue'];
$results = [];
foreach ($cases as $case) {
    $results[] = childAllCase($case);
}
echo json_encode(['fixture_source' => Scenario::SOURCE, 'fixture_sha256' => Scenario::FIXTURE_SHA256,
    'started_at' => $started, 'finished_at' => gmdate(DATE_ATOM), 'scenarios' => $results], JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT).PHP_EOL;
