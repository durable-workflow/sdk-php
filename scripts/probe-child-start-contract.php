<?php

declare(strict_types=1);

use Composer\InstalledVersions;
use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Worker\ParentClosePolicy;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;

// Use only the disposable token-auth runtime from claimed-task recovery's topology.
[$script, $autoload, $url] = $argv;
require_once $autoload;

/** @return list<array<string, mixed>> */
function childProbeHistory(Client $client, string $workflowId, string $runId): array
{
    $page = $client->workflowHistory($workflowId, $runId, 100);
    if (($page['next_page_token'] ?? null) !== null) {
        throw new RuntimeException('The bounded probe did not load complete canonical history.');
    }

    return $page['events'] ?? $page['history_events'] ?? [];
}

/** @param list<array<string, mixed>> $observations
 * @return array<string, mixed>|null
 */
function childProbePoll(Client $worker, string $workerId, string $queue, string $phase, array &$observations): ?array
{
    $worker->heartbeatWorker($workerId, ['workflow_available' => 1, 'activity_available' => 0, 'session_available' => 0]);
    for ($attempt = 0; $attempt < 3; ++$attempt) {
        $reply = $worker->pollWorkflowTaskResponse($workerId, $queue, 1);
        $task = $reply['task'] ?? null;
        $observations[] = ['phase' => $phase, 'poll_status' => $reply['poll_status'] ?? null,
            'reason' => $reply['reason'] ?? null, 'task_id' => $task['task_id'] ?? null];
        if (is_array($task)) {
            return $task;
        }
    }

    return null;
}

$admin = new Client($url, controlToken: 'claimed-task-admin');
$suffix = bin2hex(random_bytes(6));
$namespace = 'php-child-start-'.$suffix;
$parentQueue = 'parent-'.$suffix;
$childQueue = 'child-'.$suffix;
$parentWorker = 'parent-worker-'.$suffix;
$childWorker = 'child-worker-'.$suffix;
$admin->createNamespace($namespace);
$client = new Client($url, namespace: $namespace, controlToken: 'claimed-task-client');
$worker = new Client($url, namespace: $namespace, workerToken: 'claimed-task-worker');
$registered = [];
$polls = [];
$result = ['outcome' => 'observed', 'sdk_version' => InstalledVersions::getPrettyVersion('durable-workflow/sdk'),
    'sdk_reference' => InstalledVersions::getReference('durable-workflow/sdk'),
    'server_version' => $admin->clusterInfo()->version, 'namespace' => $namespace];
try {
    $registration = $worker->registerWorker($parentWorker, $parentQueue, ['qualification.child-start.parent'], [], capabilities: []);
    $registered[$parentWorker] = $registration['registration_token'] ?? null;
    $registration = $worker->registerWorker($childWorker, $childQueue, ['qualification.child-start.child'], [], capabilities: []);
    $registered[$childWorker] = $registration['registration_token'] ?? null;
    $handle = $client->startWorkflow('qualification.child-start.parent', 'parent-'.$suffix, $parentQueue);
    $result += ['parent_workflow_id' => $handle->workflowId, 'parent_run_id' => $handle->selectedRunId];
    $parentTask = childProbePoll($worker, $parentWorker, $parentQueue, 'initial_parent', $polls);
    if ($parentTask === null) {
        throw new RuntimeException('The parent initial task was unavailable.');
    }
    $runId = (string) $handle->selectedRunId;
    $value = ['max' => PHP_INT_MAX, 'text' => '雪☃', 'negative_zero' => -0.0];
    $handler = static fn (WorkflowContext $context): mixed => $context->childWorkflow(
        'qualification.child-start.child', [$value],
        ['queue' => $childQueue, 'parent_close_policy' => ParentClosePolicy::Abandon],
    );
    $initial = (new Replayer($client->payloadCodec()))->replay($handler,
        childProbeHistory($client, $handle->workflowId, $runId), [], $parentQueue, $parentTask);
    if ($initial->terminalFailure !== null || count($initial->commands) !== 1
        || $initial->commands[0]['type'] !== 'start_child_workflow') {
        throw new RuntimeException('The normal Replayer did not author one child start.');
    }
    $claim = ['task_id' => $parentTask['task_id'], 'lease_owner' => $parentTask['lease_owner'],
        'attempt' => $parentTask['workflow_task_attempt']];
    $result['original_claim'] = $claim;
    $ack = $worker->completeWorkflowTask($claim['task_id'], $claim['lease_owner'], $claim['attempt'], $initial->commands);
    $beforeDuplicate = childProbeHistory($client, $handle->workflowId, $runId);
    try {
        $duplicate = ['response' => $worker->completeWorkflowTask($claim['task_id'], $claim['lease_owner'],
            $claim['attempt'], $initial->commands)];
    } catch (ServerException $exception) {
        $duplicate = ['status' => $exception->status, 'reason' => $exception->reason];
    }
    $history = childProbeHistory($client, $handle->workflowId, $runId);
    $scheduled = array_values(array_filter($history, static fn (array $event): bool => $event['event_type'] === 'ChildWorkflowScheduled'));
    $started = array_values(array_filter($history, static fn (array $event): bool => $event['event_type'] === 'ChildRunStarted'));
    if (count($scheduled) !== 1 || count($started) !== 1 || $beforeDuplicate !== $history) {
        throw new RuntimeException('Duplicate completion changed canonical child scheduling history.');
    }
    $result += ['scheduled_event' => $scheduled[0], 'started_event' => $started[0], 'duplicate_completion' => $duplicate];
    $followup = childProbePoll($worker, $parentWorker, $parentQueue, 'before_child_completion', $polls);
    $childTask = childProbePoll($worker, $childWorker, $childQueue, 'initial_child', $polls);
    $cold = [];
    for ($attempt = 0; $attempt < 2; ++$attempt) {
        $replay = (new Replayer($client->payloadCodec()))->replay($handler, $history, [], $parentQueue, $parentTask);
        $cold[] = ['commands' => $replay->commands, 'terminal_failure' => $replay->terminalFailure?->getMessage()];
    }
    // Child resolution below is the positive queue control. Do not manufacture
    // a parent turn with a timer, signal, visibility query or stale claim.
    $progressTask = $followup;
    if ($childTask === null) {
        throw new RuntimeException('The child task was unavailable.');
    }
    if ($progressTask !== null) {
        $progressReplay = (new Replayer($client->payloadCodec()))->replay($handler,
            childProbeHistory($client, $handle->workflowId, $runId), [], $parentQueue, $progressTask);
        if ($progressReplay->commands !== [] || $progressReplay->terminalFailure !== null) {
            throw new RuntimeException('The pending sequential child unexpectedly resumed.');
        }
        $worker->completeWorkflowTask($progressTask['task_id'], $progressTask['lease_owner'],
            $progressTask['workflow_task_attempt'], $progressReplay->commands);
    }
    $childHandler = static fn (WorkflowContext $context, array $argument): mixed => $context->continueAsNew([$argument]);
    $continuation = (new Replayer($client->payloadCodec()))->replay($childHandler,
        $childTask['history_events'] ?? $childTask['history'] ?? [], [$value], $childQueue, $childTask);
    $worker->completeWorkflowTask($childTask['task_id'], $childTask['lease_owner'],
        $childTask['workflow_task_attempt'], $continuation->commands);
    $continuedHistory = childProbeHistory($client, $handle->workflowId, $runId);
    $continuedStarts = array_values(array_filter($continuedHistory,
        static fn (array $event): bool => $event['event_type'] === 'ChildRunStarted'));
    if (count($continuedStarts) !== 2) {
        throw new RuntimeException('Child continuation did not retain two canonical start observations.');
    }
    $childNextTask = childProbePoll($worker, $childWorker, $childQueue, 'continued_child', $polls);
    if ($childNextTask === null || $childNextTask['workflow_id'] !== $childTask['workflow_id']
        || $childNextTask['run_id'] === $childTask['run_id']) {
        throw new RuntimeException('Child continuation changed the instance or failed to create a new run.');
    }
    $continued = $client->describeWorkflow($childNextTask['workflow_id'], $childNextTask['run_id']);
    if ($continued->input !== [$value]) {
        throw new RuntimeException('Child continuation changed its typed input.');
    }
    $completedChild = (new Replayer($client->payloadCodec()))->replay(
        static fn (WorkflowContext $context, array $argument): array => $argument,
        $childNextTask['history_events'] ?? $childNextTask['history'] ?? [], $continued->input,
        $childQueue, $childNextTask,
    );
    $worker->completeWorkflowTask($childNextTask['task_id'], $childNextTask['lease_owner'],
        $childNextTask['workflow_task_attempt'], $completedChild->commands);
    $parentFinalTask = childProbePoll($worker, $parentWorker, $parentQueue, 'resolved_parent', $polls);
    if ($parentFinalTask === null) {
        throw new RuntimeException('Child completion did not wake its original parent.');
    }
    $resolvedHistory = childProbeHistory($client, $handle->workflowId, $runId);
    $resolved = (new Replayer($client->payloadCodec()))->replay($handler, $resolvedHistory, [], $parentQueue, $parentFinalTask);
    if ($resolved->terminalFailure !== null || count($resolved->commands) !== 1
        || $resolved->commands[0]['type'] !== 'complete_workflow') {
        throw new RuntimeException('The cold parent did not recover the continued child result.');
    }
    $output = $client->payloadCodec()->decodeEnvelope($resolved->commands[0]['result']);
    if ($output !== $value || bin2hex(pack('E', $output['negative_zero'])) !== '8000000000000000') {
        throw new RuntimeException('The typed child result or negative-zero bits changed.');
    }
    $worker->completeWorkflowTask($parentFinalTask['task_id'], $parentFinalTask['lease_owner'],
        $parentFinalTask['workflow_task_attempt'], $resolved->commands);
    $result += ['parent_workflow_id' => $handle->workflowId, 'parent_run_id' => $runId,
        'original_claim' => $claim, 'start_command' => $initial->commands[0],
        'completion_ack' => array_intersect_key($ack, array_flip(['task_id', 'outcome', 'run_status', 'created_task_ids'])),
        'duplicate_completion' => $duplicate, 'scheduled_event' => $scheduled[0], 'started_event' => $started[0],
        'parent_followup_before_child_completion' => $followup === null ? null : [
            'task_id' => $followup['task_id'], 'lease_owner' => $followup['lease_owner'],
            'attempt' => $followup['workflow_task_attempt']],
        'child_task' => $childTask === null ? null : ['task_id' => $childTask['task_id'],
            'workflow_id' => $childTask['workflow_id'], 'run_id' => $childTask['run_id']],
        'cold_pending_replays' => $cold, 'event_counts_before_child_completion' => array_count_values(array_column($history, 'event_type')),
        'continued_child_start' => $continuedStarts[1], 'continued_child_task_id' => $childNextTask['task_id'],
        'parent_final_task_id' => $parentFinalTask['task_id'], 'typed_result_preserved' => true,
        'event_counts_after_child_completion' => array_count_values(array_column($resolvedHistory, 'event_type'))];
} catch (Throwable $exception) {
    $result['outcome'] = 'probe_error';
    $result['error'] = ['type' => $exception::class, 'message' => $exception->getMessage()];
} finally {
    foreach ($registered as $workerId => $registrationToken) {
        try {
            $worker->deregisterWorkerRegistration($workerId, $registrationToken);
        } catch (Throwable $exception) {
            $result['outcome'] = 'probe_error';
            $result['cleanup_errors'][] = ['worker_id' => $workerId, 'message' => $exception->getMessage()];
        }
    }
}
$result['poll_observations'] = $polls;
echo json_encode($result, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION
    | JSON_THROW_ON_ERROR)."\n";
exit($result['outcome'] === 'probe_error' ? 1 : 0);
