<?php

declare(strict_types=1);

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ActivityFailed;
use DurableWorkflow\Exception\WorkflowFailed;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\WorkflowContext;

[$script, $autoload, $url] = $argv;
require_once $autoload;

final class FailureDeliveryQualificationException extends RuntimeException {}

const FAILURE_DELIVERY_MARKER = '[truncated failure report;';

function requireDelivery(bool $condition, string $message): void
{
    if (!$condition) {
        throw new FailureDeliveryQualificationException($message);
    }
}

/** Drive the ordinary worker while waiting for the selected run, with a finite fixture budget. */
function finishFailureWorkflow(Client $control, Worker $worker, DurableWorkflow\WorkflowHandle $handle, ?Worker $activities = null): mixed
{
    $deadline = microtime(true) + 30;
    do {
        $execution = $control->describeWorkflow($handle->workflowId, $handle->selectedRunId);
        if ($execution->isTerminal === true || in_array($execution->status,
            ['completed', 'failed', 'cancelled', 'terminated', 'timed_out'], true)) {
            return $handle->resultOfSelectedRun(1, 0.05);
        }
        $worker->tick(0);
        $activities?->tick(0);
        usleep(10000);
    } while (microtime(true) < $deadline);
    throw new FailureDeliveryQualificationException('The selected run did not finish within its fixture budget.');
}

$http = new class implements Psr\Http\Client\ClientInterface {
    public array $reports = [];
    public array $claims = [];
    private GuzzleHttp\Client $client;
    public function __construct() { $this->client = new GuzzleHttp\Client(['http_errors' => false, 'timeout' => 15]); }
    public function sendRequest(Psr\Http\Message\RequestInterface $request): Psr\Http\Message\ResponseInterface
    {
        $response = $this->client->sendRequest($request);
        $this->observe($request, $response);
        return $response;
    }
    public function observe(Psr\Http\Message\RequestInterface $request, Psr\Http\Message\ResponseInterface $response): void
    {
        $wire = (string) $request->getBody();
        $data = $wire === '' ? [] : json_decode($wire, true, flags: JSON_THROW_ON_ERROR);
        if ($response->getStatusCode() >= 400) {
            fwrite(STDERR, json_encode(['path' => $request->getUri()->getPath(), 'status' => $response->getStatusCode(),
                'response' => array_diff_key(json_decode((string) $response->getBody(), true), ['server_capabilities' => true])], JSON_THROW_ON_ERROR)."\n");
        }
        $path = $request->getUri()->getPath();
        if (str_ends_with($path, '/fail') || isset($data['commands'][0]['exception_type'])) {
            $failure = $data['failure'] ?? $data['commands'][0];
            $this->reports[] = ['path' => $path, 'encoded_bytes' => strlen($wire), 'http_status' => $response->getStatusCode(),
                'lease_owner' => $data['lease_owner'] ?? null, 'activity_attempt_id' => $data['activity_attempt_id'] ?? null,
                'workflow_task_attempt' => $data['workflow_task_attempt'] ?? null,
                'type' => isset($data['failure']) ? ($failure['type'] ?? null) : ($failure['exception_type'] ?? null),
                'message_bytes' => strlen($failure['message']),
                'marked' => str_contains($failure['message'], FAILURE_DELIVERY_MARKER),
                'stack_bytes' => strlen($failure['stack_trace'] ?? $failure['exception']['stack_trace'] ?? ''),
                'message_sha256' => hash('sha256', $failure['message']), 'non_retryable' => $failure['non_retryable'] ?? null];
        }
        if (str_ends_with($path, '/poll')) {
            $reply = json_decode((string) $response->getBody(), true, flags: JSON_THROW_ON_ERROR);
            $task = $reply['task'] ?? null;
            if (is_array($task)) {
                $this->claims[] = array_intersect_key($task, array_flip([
                    'task_id', 'lease_owner', 'workflow_task_attempt', 'activity_attempt_id', 'attempt_id', 'workflow_id', 'run_id',
                ]));
            }
        }
    }
};
$namespace = 'php-failure-'.bin2hex(random_bytes(6));
$admin = new Client($url, controlToken: 'claimed-task-admin');
$admin->createNamespace($namespace);
$control = new Client($url, namespace: $namespace, controlToken: 'claimed-task-client');
$transport = new DurableWorkflow\Transport\Psr18Transport(client: $http);
$client = new Client($url, namespace: $namespace, transport: $transport, workerToken: 'claimed-task-worker');
$queue = $namespace;
$workerId = $namespace.'-workflow';
$activityWorkerId = $namespace.'-activity';
$worker = new Worker($client, $queue, workerId: $workerId, stickyCacheCapacity: 1);
$workflowHandler = static function (WorkflowContext $context, string $kind): array {
    if ($kind === 'terminal') {
        throw new FailureDeliveryQualificationException(str_repeat('t', 3 * 1024 * 1024));
    }
    if ($kind === 'retry') {
        return ['activity_value' => $context->activity('qualification.failure.activity', [],
            ['retry_policy' => ['max_attempts' => 2, 'backoff_seconds' => [1]]])];
    }
    if ($kind === 'workflow-task') {
        return ['fresh_turn' => true];
    }
    try {
        if ($kind === 'local-failure') {
            $context->localActivity('qualification.failure.local', [], ['retry_policy' => ['max_attempts' => 1]]);
        } else {
        $context->activity($kind === 'cooperative-worker' ? 'qualification.failure.managed' : 'qualification.failure.activity',
            [], ['retry_policy' => ['max_attempts' => 1]]);
        }
        throw new FailureDeliveryQualificationException('The activity unexpectedly succeeded.');
    } catch (ActivityFailed $failure) {
        return ['type' => $failure->failureType, 'marked' => str_contains($failure->getMessage(), FAILURE_DELIVERY_MARKER),
            'message_bytes' => strlen($failure->getMessage()), 'message_sha256' => hash('sha256', $failure->getMessage()),
            'non_retryable' => $failure->nonRetryable];
    }
};
$worker->registerWorkflow('qualification.failure', $workflowHandler);
$worker->registerActivity('qualification.failure.local', static function (DurableWorkflow\Worker\ActivityContext $_context): never {
    requireDelivery($_context->activityType === 'qualification.failure.local', 'Wrong local callback context.');
    throw new FailureDeliveryQualificationException(str_repeat('雪', 1024 * 1024));
});
$client->registerWorker($workerId, $queue, ['qualification.failure'], [], ['sticky_execution', 'typed_search_attributes', 'local_activities'],
    workflowCommandContracts: $worker->contracts()['workflow_commands']);
$client->registerWorker($activityWorkerId, $queue, [], ['qualification.failure.activity']);
$result = ['sdk_version' => Composer\InstalledVersions::getPrettyVersion('durable-workflow/sdk'),
    'sdk_reference' => Composer\InstalledVersions::getReference('durable-workflow/sdk'),
    'server_version' => $admin->clusterInfo()->version, 'namespace' => $namespace, 'scenarios' => []];
register_shutdown_function(static function () use (&$result, $http): void {
    if (!isset($result['pass'])) {
        $result['reports'] = $http->reports;
        $result['claims'] = $http->claims;
        $result['incomplete'] = true;
        $result['last_error'] = error_get_last();
        echo json_encode($result, JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE)."\n";
    }
});
$large = str_repeat('m', 3 * 1024 * 1024);
foreach (['message', 'stack', 'combined', 'escaped', 'details', 'lower-limit', 'small'] as $kind) {
    $handle = $control->startWorkflow('qualification.failure', $namespace.'-'.$kind, $queue, [$kind], runTimeoutSeconds: 120);
    $worker->tick(0);
    $task = $client->pollActivityTask($activityWorkerId, $queue, 0);
    requireDelivery(is_array($task), 'The activity was not leased for '.$kind);
    $message = in_array($kind, ['message', 'combined', 'lower-limit'], true) ? $large : 'original message';
    $stack = in_array($kind, ['stack', 'combined', 'lower-limit'], true) ? $large : null;
    if ($kind === 'escaped') {
        $message = $stack = str_repeat("\0\"\\雪🦀", 300000);
    }
    $details = $kind === 'details' ? ['nested' => ['cause' => $large]] : ['whole' => 1.0, 'zero' => -0.0];
    $type = 'OriginalFailure.'.$kind;
    $reporter = $kind === 'lower-limit' ? $client->withFailureReportLimit(4096) : $client;
    $reporter->failActivityTask($task['task_id'], $task['activity_attempt_id'] ?? $task['attempt_id'], $task['lease_owner'],
        $message, $type, true, $details, $stack);
    $worker->tick(0);
    $output = finishFailureWorkflow($control, $worker, $handle);
    requireDelivery($output['type'] === $type && $output['marked'] === ($kind !== 'small'), 'Original workflow failure mismatch.');
    requireDelivery($output['non_retryable'] === true, 'The non-retryable decision changed.');
    $history = $control->workflowHistory($handle->workflowId, $handle->selectedRunId, 100);
    $events = $history['events'] ?? $history['history_events'] ?? [];
    $counts = array_count_values(array_column($events, 'event_type'));
    requireDelivery(($counts['ActivityFailed'] ?? 0) === 1 && ($counts['WorkflowCompleted'] ?? 0) === 1, 'Failure history missing.');
    $result['scenarios'][$kind] = ['workflow_id' => $handle->workflowId, 'run_id' => $handle->selectedRunId,
        'task_id' => $task['task_id'], 'attempt_id' => $task['activity_attempt_id'] ?? $task['attempt_id'],
        'lease_owner' => $task['lease_owner'], 'output' => $output, 'event_counts' => $counts];
}
$handle = $control->startWorkflow('qualification.failure', $namespace.'-local', $queue, ['local-failure'], runTimeoutSeconds: 120);
$worker->tick(0);
$output = finishFailureWorkflow($control, $worker, $handle);
$history = $control->workflowHistory($handle->workflowId, $handle->selectedRunId, 100);
$events = $history['events'] ?? $history['history_events'] ?? [];
$failed = array_values(array_filter($events, static fn (array $event): bool => $event['event_type'] === 'ActivityFailed'));
requireDelivery(count($failed) === 1, 'Local failure was not persisted once.');
$message = $failed[0]['payload']['message'] ?? '';
requireDelivery($output['message_sha256'] === hash('sha256', $message), 'Initial local branch saw a different failure from persisted history.');
requireDelivery($output['type'] === FailureDeliveryQualificationException::class && $output['marked'] === true,
    'Original local failure type or marker changed.');
$cold = (new DurableWorkflow\Worker\Replayer($client->payloadCodec()))->replay($workflowHandler, $events, ['local-failure'], $queue,
    localActivityExecutor: static function (): never { throw new FailureDeliveryQualificationException('Cold replay repeated a local callback.'); });
requireDelivery($cold->terminalFailure === null && count($cold->commands) === 1, 'Local cold replay did not complete normally.');
requireDelivery($client->payloadCodec()->decodeEnvelope($cold->commands[0]['result']) === $output,
    'Cold workflow branch saw a different local failure.');
$result['scenarios']['local-failure'] = ['workflow_id' => $handle->workflowId, 'run_id' => $handle->selectedRunId,
    'output' => $output, 'initial_equals_persisted_equals_cold' => true];
$stack = GuzzleHttp\HandlerStack::create();
$stack->push(static function (callable $next) use ($http): Closure {
    return static function (Psr\Http\Message\RequestInterface $request, array $options) use ($next, $http) {
        return $next($request, $options)->then(static function (Psr\Http\Message\ResponseInterface $response) use ($request, $http) {
            $http->observe($request, $response);
            return $response;
        });
    };
});
$managedId = $namespace.'-cooperative';
$managedClient = new Client($url, namespace: $namespace, workerToken: 'claimed-task-worker',
    transport: new DurableWorkflow\Transport\Psr18Transport(client: new GuzzleHttp\Client(['handler' => $stack, 'http_errors' => false])));
$managedWorker = new Worker($managedClient, $queue, workerId: $managedId, enableCooperativeCancellation: true);
$managedWorker->registerActivity('qualification.failure.managed', static function (DurableWorkflow\Worker\ActivityContext $_context): never {
    requireDelivery($_context->activityType === 'qualification.failure.managed', 'Wrong isolated callback context.');
    throw new FailureDeliveryQualificationException(str_repeat('m', 3 * 1024 * 1024));
});
$managedClient->registerWorker($managedId, $queue, [], ['qualification.failure.managed'], ['cooperative_cancellation']);
$handle = $control->startWorkflow('qualification.failure', $namespace.'-cooperative-worker', $queue, ['cooperative-worker'], runTimeoutSeconds: 120);
$worker->tick(0);
$managedWorker->tick(0);
$worker->tick(0);
$output = finishFailureWorkflow($control, $worker, $handle, $managedWorker);
requireDelivery($output['type'] === FailureDeliveryQualificationException::class && $output['marked'] === true,
    'The original workflow did not receive its isolated callback failure.');
requireDelivery($output['non_retryable'] === false, 'The callback retry classification changed.');
$history = $control->workflowHistory($handle->workflowId, $handle->selectedRunId, 100);
$events = $history['events'] ?? $history['history_events'] ?? [];
$failed = array_values(array_filter($events, static fn (array $event): bool => $event['event_type'] === 'ActivityFailed'));
$failureWire = json_encode($failed, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES);
requireDelivery(str_contains($failureWire, '3145728') && str_contains($failureWire, 'qualify-failure-report-delivery.php'),
    'Original callback trace or original-size diagnostics missing from history.');
$result['scenarios']['cooperative-worker'] = ['workflow_id' => $handle->workflowId, 'run_id' => $handle->selectedRunId,
    'output' => $output, 'original_callback_trace_and_sizes_in_history' => true];
$handle = $control->startWorkflow('qualification.failure', $namespace.'-retry', $queue, ['retry'], runTimeoutSeconds: 120);
$worker->tick(0);
$original = $client->pollActivityTask($activityWorkerId, $queue, 0);
requireDelivery(is_array($original), 'Retry control original attempt was not leased.');
$retryStarted = hrtime(true);
$client->failActivityTask($original['task_id'], $original['activity_attempt_id'] ?? $original['attempt_id'],
    $original['lease_owner'], $large, 'OriginalRetryableFailure', false);
$deadline = microtime(true) + 15;
do {
    $retry = $client->pollActivityTask($activityWorkerId, $queue, 0);
    if ($retry !== null) { break; }
    usleep(50000);
} while (microtime(true) < $deadline);
requireDelivery(is_array($retry), 'Authored retry was not made available.');
$originalAttempt = $original['activity_attempt_id'] ?? $original['attempt_id'];
$retryAttempt = $retry['activity_attempt_id'] ?? $retry['attempt_id'];
requireDelivery($retryAttempt !== $originalAttempt, 'The failed attempt was reused as publication authority.');
$client->completeActivityTask($retry['task_id'], $retryAttempt, $retry['lease_owner'], 42);
$worker->tick(0);
$output = finishFailureWorkflow($control, $worker, $handle);
requireDelivery($output === ['activity_value' => 42], 'The original workflow did not receive the legitimate retry result.');
$history = $control->workflowHistory($handle->workflowId, $handle->selectedRunId, 100);
$events = $history['events'] ?? $history['history_events'] ?? [];
$scheduled = array_values(array_filter($events, static fn (array $event): bool => $event['event_type'] === 'ActivityScheduled'));
$result['scenarios']['retry'] = ['workflow_id' => $handle->workflowId, 'run_id' => $handle->selectedRunId,
    'original_task_id' => $original['task_id'], 'original_attempt_id' => $originalAttempt,
    'retry_task_id' => $retry['task_id'], 'retry_attempt_id' => $retryAttempt,
    'retry_elapsed_seconds' => (hrtime(true) - $retryStarted) / 1e9,
    'scheduled_payload' => $scheduled[0]['payload'] ?? null, 'output' => $output];
$handle = $control->startWorkflow('qualification.failure', $namespace.'-workflow-task', $queue, ['workflow-task'], runTimeoutSeconds: 120);
$task = $client->pollWorkflowTask($workerId, $queue, 0);
requireDelivery(is_array($task), 'Original workflow task not leased.');
$ack = $client->failWorkflowTask($task['task_id'], $task['lease_owner'], $task['workflow_task_attempt'], $large,
    'OriginalWorkflowTaskFailure', stackTrace: $large);
requireDelivery(($ack['recorded'] ?? null) === true, 'Workflow-task failure not recorded.');
$worker->tick(0);
$output = finishFailureWorkflow($control, $worker, $handle);
requireDelivery($output === ['fresh_turn' => true], 'The workflow did not receive its fresh legitimate turn.');
$history = $control->workflowHistory($handle->workflowId, $handle->selectedRunId, 100);
$events = $history['events'] ?? $history['history_events'] ?? [];

$failureEvents = array_values(array_filter($events, static fn (array $event): bool => $event['event_type'] === 'WorkflowTaskFailed'));
// The published runtime stores task failure separately and emits no history event.
requireDelivery(($ack['next_task_id'] ?? null) !== $task['task_id'], 'Failed claim identity was reused as retry authority.');
$result['scenarios']['workflow-task'] = ['workflow_id' => $handle->workflowId, 'run_id' => $handle->selectedRunId,
    'original_task_id' => $task['task_id'], 'lease_owner' => $task['lease_owner'], 'attempt' => $task['workflow_task_attempt'],
    'ack' => $ack, 'history_failure_events' => count($failureEvents), 'fresh_turn_result' => $output,
    'history_visibility' => 'Published runtime emits no WorkflowTaskFailed history event; parent225 runtime follow-through remains.'];
$handle = $control->startWorkflow('qualification.failure', $namespace.'-terminal', $queue, ['terminal'], runTimeoutSeconds: 120);
$worker->tick(0);
try {
    finishFailureWorkflow($control, $worker, $handle);
    throw new FailureDeliveryQualificationException('Terminal workflow unexpectedly succeeded.');
} catch (WorkflowFailed $failure) {
    requireDelivery($failure->failureType === FailureDeliveryQualificationException::class, 'Workflow failure type changed.');
    requireDelivery(str_contains($failure->getMessage(), FAILURE_DELIVERY_MARKER), 'Terminal workflow marker missing.');
    $result['scenarios']['terminal'] = ['workflow_id' => $handle->workflowId, 'run_id' => $handle->selectedRunId,
        'failure_type' => $failure->failureType, 'marked_message' => true, 'message_bytes' => strlen($failure->getMessage())];
}
foreach ($http->reports as $report) {
    requireDelivery($report['encoded_bytes'] <= 1048576 && $report['http_status'] === 200, 'Encoded/reporting HTTP boundary failed.');
}
$result['reports'] = $http->reports;
$result['claims'] = $http->claims;
$result['pass'] = true;
echo json_encode($result, JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE)."\n";
