<?php

declare(strict_types=1);

// Usage: php qualify-child-start-lifecycle.php AUTOLOAD DISPOSABLE_SERVER_URL [--policies]
// Uses only the fixture role tokens from qualify-child-start-handle.php.
// Each replacement is a fresh managed Worker process, with no shared replay cache.

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ChildWorkflowFailed;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Transport\BoundedTransport;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\WorkflowContext;

[$script, $autoload, $url] = $argv;
require_once $autoload;

final class ChildLifecycleFailure extends RuntimeException
{
}

final class QualificationChildRejected extends RuntimeException
{
}

/** Observe completion bodies without logging authentication headers. */
final class ChildLifecycleTransport implements BoundedTransport
{
    private readonly Psr18Transport $http;

    public function __construct(private readonly string $receipt, private readonly Closure $started)
    {
        $this->http = new Psr18Transport();
    }

    public function supportsBoundedRequests(): bool
    {
        return $this->http->supportsBoundedRequests();
    }

    public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
    {
        return $this->observe($uri, $body, $this->http->send($method, $uri, $headers, $body));
    }

    public function sendBounded(string $method, string $uri, array $headers, ?array $body, int $timeoutSeconds): ?array
    {
        return $this->observe($uri, $body, $this->http->sendBounded($method, $uri, $headers, $body, $timeoutSeconds));
    }

    private function observe(string $uri, ?array $body, ?array $response): ?array
    {
        if (preg_match('~/workflow-tasks/([^/]+)/complete$~', $uri, $match) === 1
            && ($body['commands'][0]['await'] ?? null) === 'started') {
            file_put_contents($this->receipt, json_encode([
                'task_id' => rawurldecode($match[1]), 'body' => $body, 'response' => $response,
            ], JSON_THROW_ON_ERROR));
            ($this->started)();
        }

        return $response;
    }
}

function lifecycleCheck(bool $condition, string $message): void
{
    if (!$condition) {
        throw new ChildLifecycleFailure($message);
    }
    echo 'ok: '.$message.PHP_EOL;
}

/** @return list<mixed> */
function lifecycleValue(): array
{
    return ['typed child ✓', PHP_INT_MAX, null, true, -0.0];
}

if (($argv[3] ?? null) === '--worker') {
    [$namespace, $queue, $workerId, $stopAfterStart, $receipt, $cooperative] = array_slice($argv, 4);
    $worker = null;
    $transport = new ChildLifecycleTransport($receipt, static function () use (&$worker, $stopAfterStart): void {
        if ($stopAfterStart === 'yes') {
            $worker->requestShutdown();
        }
    });
    $client = new Client($url, namespace: $namespace, workerToken: 'claimed-task-worker', transport: $transport);
    $worker = new Worker($client, $queue, workerId: $workerId, enableCooperativeCancellation: $cooperative === 'yes');
    $worker->registerWorkflow('qualification.lifecycle.parent', static function (
        WorkflowContext $context, string $mode, string $childQueue, array $options,
    ): array {
        $child = $context->startChildWorkflow('qualification.lifecycle.child', [$mode, 0], ['queue' => $childQueue, ...$options]);
        $identity = $child->identity();
        $snapshot = [$identity->callId, $identity->workflowId, $identity->initialRunId];
        if ($mode === 'close') {
            return ['identity' => $snapshot];
        }
        $context->sideEffect(static fn (): array => $snapshot);
        try {
            return ['identity' => $snapshot, 'result' => $child->await()];
        } catch (ChildWorkflowFailed $failure) {
            return ['identity' => $snapshot, 'failure' => [
                'class' => $failure::class, 'workflow_type' => $failure->workflowType,
                'failure_type' => $failure->failureType, 'message' => $failure->getMessage(),
            ]];
        }
    });
    $worker->registerWorkflow('qualification.lifecycle.child', static function (
        WorkflowContext $context, string $mode, int $stage,
    ): array {
        if ($mode === 'failure' || $mode === 'retry') {
            throw new QualificationChildRejected('Qualification child rejected.');
        }
        if ($mode === 'continue' && $stage === 0) {
            $context->continueAsNew([$mode, 1]);
        }
        if ($mode === 'cancel') {
            $context->sleep(60);
        }

        return lifecycleValue();
    });
    $worker->run(1);
    exit(0);
}

/** @return list<array<string, mixed>> */
function lifecycleHistory(Client $client, string $workflow, string $run): array
{
    $page = $client->workflowHistory($workflow, $run, 200);
    lifecycleCheck(empty($page['next_page_token']), 'qualification history fits one complete page');

    return $page['events'] ?? $page['history_events'] ?? [];
}

function lifecycleWait(callable $condition, string $description, int $seconds = 35): mixed
{
    $deadline = microtime(true) + $seconds;
    do {
        $result = $condition();
        if ($result) {
            return $result;
        }
        usleep(100_000);
    } while (microtime(true) < $deadline);

    throw new ChildLifecycleFailure('Timed out: '.$description);
}

/** @return resource */
function lifecycleWorker(string $namespace, string $queue, string $workerId, string $receipt, bool $once = false, bool $cooperative = false)
{
    global $script, $autoload, $url;
    $process = proc_open([
        PHP_BINARY, $script, $autoload, $url, '--worker', $namespace, $queue, $workerId,
        $once ? 'yes' : 'no', $receipt, $cooperative ? 'yes' : 'no',
    ], [1 => STDOUT, 2 => STDERR], $pipes);
    if (!is_resource($process)) {
        throw new ChildLifecycleFailure('Could not start a managed worker.');
    }

    return $process;
}

/** @param resource $process */
function lifecycleStop($process): void
{
    if (proc_get_status($process)['running']) {
        proc_terminate($process);
        $deadline = microtime(true) + 5;
        while (proc_get_status($process)['running'] && microtime(true) < $deadline) {
            usleep(100_000);
        }
        if (proc_get_status($process)['running']) {
            proc_terminate($process, 9);
        }
    }
    proc_close($process);
}

/** @return array<string, mixed> */
function lifecycleDuplicate(Client $client, array $start, string $queue): array
{
    $body = $start['body'];
    try {
        $repeat = $client->completeWorkflowTask($start['task_id'], $body['lease_owner'], $body['workflow_task_attempt'], $body['commands']);
        lifecycleCheck(($repeat['recorded'] ?? null) === false && ($repeat['reason'] ?? null) === 'already_completed', 'duplicate start completion is an explicit no-op');
        $duplicateReceipt = 'already_completed';
    } catch (ServerException $error) {
        lifecycleCheck($error->status === 409 && $error->reason === 'task_not_leased', 'duplicate completion from the stopped worker is fenced');
        $duplicateReceipt = '409/task_not_leased';
    }

    $workerId = $queue.'-redelivery';
    $registration = $client->registerWorker($workerId, $queue, ['qualification.lifecycle.parent'], []);
    try {
        $requestId = $queue.'-same-poll';
        $first = $client->pollWorkflowTaskResponse($workerId, $queue, 1, $requestId);
        $second = $client->pollWorkflowTaskResponse($workerId, $queue, 1, $requestId);
        $task = $first['task'] ?? null;
        lifecycleCheck(is_array($task), 'a fresh parent turn is available before child execution');
        foreach (['task_id', 'lease_owner', 'workflow_task_attempt'] as $field) {
            lifecycleCheck($task[$field] === ($second['task'][$field] ?? null), 'duplicate parent poll preserves '.$field);
        }
    } finally {
        $released = $client->deregisterWorkerRegistration($workerId, $registration['registration_token'] ?? null);
    }
    lifecycleCheck($released['recovered_workflow_task_count'] === 1, 'the delivery-control worker releases its one claim for replacement');
    try {
        $client->completeWorkflowTask($task['task_id'], $task['lease_owner'], $task['workflow_task_attempt'], [[
            'type' => 'complete_workflow', 'result' => $client->payloadCodec()->envelope('stale worker'),
        ]]);
        throw new ChildLifecycleFailure('A released claim completed the parent.');
    } catch (ServerException $error) {
        lifecycleCheck($error->status === 409 && $error->reason === 'task_not_leased', 'the released parent attempt is fenced');
    }

    return ['duplicate_completion' => $duplicateReceipt, 'duplicate_delivery' => 'same_claim', 'released_attempt' => 'task_not_leased'];
}

$startedAt = gmdate(DATE_ATOM);
$admin = new Client($url, controlToken: 'claimed-task-admin');
lifecycleCheck(($admin->clusterInfo()->capabilities['child_workflow_start_turn'] ?? null) === true, 'runtime advertises the committed-start turn');
$results = [];
$cases = [
    ['result', ['parent_close_policy' => 'abandon']],
    ['failure', []],
    ['continue', []],
    ['retry', ['retry_policy' => ['max_attempts' => 2, 'backoff_seconds' => [0]]]],
];
foreach (['abandon', 'request_cancel', 'terminate', 'request_cancellation'] as $policy) {
    $cases[] = ['close', ['parent_close_policy' => $policy]];
}
foreach (['abandon', 'try_cancel', 'wait_cancellation_completed'] as $policy) {
    $cases[] = ['cancel', ['parent_close_policy' => 'abandon', 'cancellation_policy' => $policy]];
}
if (($argv[3] ?? null) === '--policies') {
    $cases = array_filter($cases, static fn (array $case): bool => in_array($case[0], ['close', 'cancel'], true));
}

foreach ($cases as [$mode, $options]) {
    $suffix = bin2hex(random_bytes(6));
    $namespace = 'php-child-lifecycle-'.$suffix;
    $parentQueue = 'parent-'.$suffix;
    $childQueue = 'child-'.$suffix;
    $receipt = sys_get_temp_dir().'/child-lifecycle-'.$suffix.'.json';
    $admin->createNamespace($namespace);
    $client = new Client($url, namespace: $namespace, controlToken: 'claimed-task-client');
    $workerClient = new Client($url, namespace: $namespace, workerToken: 'claimed-task-worker');
    $cooperative = $mode === 'cancel' || ($options['parent_close_policy'] ?? null) === 'request_cancellation';
    $parent = $client->startWorkflow('qualification.lifecycle.parent', 'parent-'.$suffix, $parentQueue, [$mode, $childQueue, $options]);
    $processes = [];
    $identity = null;
    try {
        $first = lifecycleWorker($namespace, $parentQueue, $parentQueue.'-original', $receipt, once: true, cooperative: $cooperative);
        $processes[] = $first;
        lifecycleWait(static fn (): bool => is_file($receipt) && !proc_get_status($first)['running'], 'original worker commits the start and exits');
        $start = json_decode((string) file_get_contents($receipt), true, flags: JSON_THROW_ON_ERROR);
        $before = lifecycleHistory($client, $parent->workflowId, (string) $parent->selectedRunId);
        $scheduled = array_values(array_filter($before, static fn (array $event): bool => $event['event_type'] === 'ChildWorkflowScheduled'));
        lifecycleCheck(count($scheduled) === 1, 'exactly one child was started before replacement');
        $payload = $scheduled[0]['payload'];
        $identity = [$payload['child_call_id'], $payload['child_workflow_instance_id'], $payload['child_workflow_run_id']];
        $controls = $mode === 'result' ? lifecycleDuplicate($workerClient, $start, $parentQueue) : [];

        $other = 'php-child-isolation-'.$suffix;
        $admin->createNamespace($other);
        try {
            (new Client($url, namespace: $other, controlToken: 'claimed-task-client'))->describeWorkflow($identity[1]);
            throw new ChildLifecycleFailure('A child was visible in another namespace.');
        } catch (ServerException $error) {
            lifecycleCheck($error->status === 404 && $error->reason === 'instance_not_found', 'the child identity remains namespace-scoped ('.$error->status.'/'.$error->reason.')');
        }

        $processes[] = lifecycleWorker($namespace, $parentQueue, $parentQueue.'-replacement', $receipt, cooperative: $cooperative);
        if ($mode !== 'close') {
            lifecycleWait(static function () use ($client, $parent): bool {
                $page = $client->workflowHistory($parent->workflowId, (string) $parent->selectedRunId, 200);

                return in_array('SideEffectRecorded', array_column($page['events'] ?? $page['history_events'] ?? [], 'event_type'), true);
            }, 'replacement recovers identity and records its snapshot');
        }

        if ($mode === 'cancel') {
            $request = $parent->requestCancellation('Qualification parent cancellation.', 20);
            $repeat = $parent->requestCancellation('Repeated request must not reset the budget.', 30);
            lifecycleCheck($repeat['duplicate'] === true, 'a repeated cancellation is recognized');
            foreach (['request_id', 'cleanup_deadline_at'] as $field) {
                lifecycleCheck($request['cancellation_request'][$field] === $repeat['cancellation_request'][$field], 'repeated cancellation preserves '.$field);
            }
        }
        if ($mode !== 'close') {
            $processes[] = lifecycleWorker($namespace, $childQueue, $childQueue.'-worker', $receipt, cooperative: $cooperative);
        }
        $execution = lifecycleWait(static function () use ($client, $parent) {
            $value = $client->describeWorkflow($parent->workflowId, (string) $parent->selectedRunId);

            return in_array($value->status, ['completed', 'cancelled', 'terminated', 'failed'], true) ? $value : null;
        }, 'parent reaches its expected terminal state');
        lifecycleCheck($execution->status === ($mode === 'cancel' ? 'cancelled' : 'completed'), 'parent terminal state for '.$mode);
        if ($mode === 'close' && in_array($options['parent_close_policy'], ['abandon', 'request_cancellation'], true)) {
            $processes[] = lifecycleWorker($namespace, $childQueue, $childQueue.'-worker', $receipt, cooperative: $cooperative);
        }
        $history = lifecycleHistory($client, $parent->workflowId, (string) $parent->selectedRunId);
        $types = array_column($history, 'event_type');
        lifecycleCheck(count(array_filter($types, static fn (string $type): bool => $type === 'ChildWorkflowScheduled')) === 1, 'replacement and duplicate delivery did not start another child');
        if ($mode !== 'cancel') {
            $result = $parent->result(5);
            lifecycleCheck($result['identity'] === $identity, 'initial child identity is unchanged at parent completion');
            if (in_array($mode, ['result', 'continue'], true)) {
                lifecycleCheck(serialize($result['result']) === serialize(lifecycleValue()), 'the awaited value preserves its PHP types and negative zero');
            }
            if ($mode === 'failure' || $mode === 'retry') {
                lifecycleCheck($result['failure'] === [
                    'class' => ChildWorkflowFailed::class, 'workflow_type' => 'qualification.lifecycle.child',
                    'failure_type' => 'ChildRunFailed', 'message' => 'Qualification child rejected.',
                ], 'the parent receives the typed child failure');
            }
        }
        $starts = array_values(array_filter($history, static fn (array $event): bool => $event['event_type'] === 'ChildRunStarted'));
        if ($mode === 'continue' || $mode === 'retry') {
            lifecycleCheck(count($starts) === 2 && $starts[1]['payload']['child_workflow_run_id'] !== $identity[2], 'the child chain has a distinct second run');
            lifecycleCheck($starts[1]['payload']['child_call_id'] === $identity[0], 'the second run belongs to the same child call');
        }
        if ($mode === 'close' || $mode === 'cancel') {
            $expected = $mode === 'close' ? match ($options['parent_close_policy']) {
                'abandon' => 'completed', 'terminate' => 'terminated', default => 'cancelled',
            } : ($options['cancellation_policy'] === 'abandon' ? 'open' : 'cancelled');
            $child = lifecycleWait(static function () use ($client, $identity, $expected) {
                $value = $client->describeWorkflow($identity[1]);

                return ($expected === 'open' ? in_array($value->status, ['pending', 'running', 'waiting'], true) : $value->status === $expected) ? $value : null;
            }, 'child lifecycle policy '.$expected);
            lifecycleCheck($expected === 'open' || $child->status === $expected, 'child policy result '.$child->status);
            if (($mode === 'close' && $options['parent_close_policy'] === 'request_cancellation')
                || ($mode === 'cancel' && $options['cancellation_policy'] !== 'abandon')) {
                $childHistory = lifecycleHistory($client, $identity[1], $identity[2]);
                $parentRequests = array_values(array_filter($history, static fn (array $event): bool => in_array($event['event_type'], ['CooperativeCancellationRequested', 'ParentCloseCancellationRequested'], true)));
                $childRequests = array_values(array_filter($childHistory, static fn (array $event): bool => $event['event_type'] === 'CooperativeCancellationRequested'));
                lifecycleCheck(count($parentRequests) === 1 && count($childRequests) === 1, 'one canonical cancellation root reaches the child');
                $root = $parentRequests[0]['payload']['cancellation'];
                $descendant = $childRequests[0]['payload']['cancellation'];
                foreach (['root_request_id', 'cleanup_deadline_at'] as $field) {
                    lifecycleCheck($root[$field] === $descendant[$field], 'child cancellation preserves '.$field);
                }
                $controls['cancellation_deadline'] = 'preserved';
            }
        }
        $results[] = ['mode' => $mode, 'options' => $options, 'identity' => $identity, 'history' => $types, 'controls' => $controls];
    } catch (Throwable $error) {
        fwrite(STDERR, json_encode([
            'mode' => $mode, 'options' => $options, 'namespace' => $namespace,
            'parent_id' => $parent->workflowId, 'identity' => $identity ?? null,
        ], JSON_THROW_ON_ERROR).PHP_EOL);
        throw $error;
    } finally {
        foreach (array_reverse($processes) as $process) {
            lifecycleStop($process);
        }
        @unlink($receipt);
    }
}
echo json_encode(['started_at' => $startedAt, 'finished_at' => gmdate(DATE_ATOM), 'scenarios' => $results], JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT).PHP_EOL;
