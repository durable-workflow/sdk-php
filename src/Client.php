<?php

declare(strict_types=1);

namespace DurableWorkflow;

use DurableWorkflow\Auth\Authentication;
use DurableWorkflow\Auth\TokenAuthentication;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Codec\PayloadCodec;
use DurableWorkflow\Exception\QueryFailed;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\SignalFailed;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Exception\UpdateFailed;
use DurableWorkflow\Exception\WorkflowCancelled;
use DurableWorkflow\Exception\WorkflowFailed;
use DurableWorkflow\Exception\WorkflowTerminated;
use DurableWorkflow\Exception\WorkflowTimedOut;
use DurableWorkflow\Model\ClusterInfo;
use DurableWorkflow\Model\NamespaceDescription;
use DurableWorkflow\Model\ScheduleAction;
use DurableWorkflow\Model\ScheduleDescription;
use DurableWorkflow\Model\SchedulePage;
use DurableWorkflow\Model\ScheduleSpec;
use DurableWorkflow\Model\SearchAttributeCollection;
use DurableWorkflow\Model\SearchAttributeDefinition;
use DurableWorkflow\Model\ServiceOperationDescription;
use DurableWorkflow\Model\ServiceOperationOptions;
use DurableWorkflow\Model\WorkflowExecution;
use DurableWorkflow\Model\WorkflowPage;
use DurableWorkflow\Model\WorkflowRun;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Transport\Transport;
use InvalidArgumentException;

/** Synchronous control-plane and worker-plane client for the standalone server. */
final class Client
{
    private const WORKFLOW_TASK_WAITING_FOR_HISTORY_MESSAGE = 'Workflow task waiting for scheduled history.';
    private const WORKFLOW_TASK_WAITING_FOR_HISTORY_TYPE = 'WorkflowTaskWaitingForHistory';

    private readonly string $baseUri;
    private readonly ?Authentication $authentication;
    private readonly Transport $transport;
    private readonly PayloadCodec $codec;

    public function __construct(
        string $baseUri,
        ?Authentication $authentication = null,
        public readonly string $namespace = 'default',
        ?Transport $transport = null,
        ?PayloadCodec $codec = null,
        ?string $token = null,
        ?string $controlToken = null,
        ?string $workerToken = null,
    ) {
        if (trim($baseUri) === '') {
            throw new InvalidArgumentException('The Durable Workflow server URI cannot be empty.');
        }
        if ($authentication !== null && ($token !== null || $controlToken !== null || $workerToken !== null)) {
            throw new InvalidArgumentException('Pass either an Authentication implementation or token arguments, not both.');
        }
        if (trim($this->namespace) === '') {
            throw new InvalidArgumentException('The Durable Workflow namespace cannot be empty.');
        }
        $this->baseUri = rtrim($baseUri, '/');
        $this->authentication = $authentication
            ?? (($token !== null || $controlToken !== null || $workerToken !== null)
                ? new TokenAuthentication($token, $controlToken, $workerToken)
                : null);
        $this->transport = $transport ?? new Psr18Transport();
        $this->codec = $codec ?? new AvroPayloadCodec();
    }

    public function payloadCodec(): PayloadCodec
    {
        return $this->codec;
    }

    /** Return a new client with the same transport, authentication, and codec for another namespace. */
    public function withNamespace(string $namespace): self
    {
        return new self(
            $this->baseUri,
            $this->authentication,
            $namespace,
            $this->transport,
            $this->codec,
        );
    }

    /** @return array<string, mixed> */
    public function health(): array
    {
        return $this->control('GET', '/health');
    }

    public function clusterInfo(bool $includeDiagnostics = false): ClusterInfo
    {
        $path = $includeDiagnostics ? '/cluster/info?include=diagnostics' : '/cluster/info';

        return ClusterInfo::fromArray($this->control('GET', $path));
    }

    /**
     * @param list<mixed> $input
     * @param array<string, mixed>|null $memo
     * @param array<string, mixed>|null $searchAttributes
     */
    public function startWorkflow(
        string $workflowType,
        string $workflowId,
        string $taskQueue,
        array $input = [],
        int $executionTimeoutSeconds = 3600,
        int $runTimeoutSeconds = 600,
        ?string $duplicatePolicy = null,
        ?array $memo = null,
        ?array $searchAttributes = null,
        ?int $priority = null,
        ?string $fairnessKey = null,
        ?int $fairnessWeight = null,
        ?string $buildId = null,
    ): WorkflowHandle {
        $body = $this->withoutNulls([
            'workflow_id' => $workflowId,
            'workflow_type' => $workflowType,
            'task_queue' => $taskQueue,
            'input' => $this->codec->envelope($input),
            'execution_timeout_seconds' => $executionTimeoutSeconds,
            'run_timeout_seconds' => $runTimeoutSeconds,
            'duplicate_policy' => $duplicatePolicy,
            'memo' => $memo,
            'search_attributes' => $searchAttributes,
            'priority' => $priority,
            'fairness_key' => $fairnessKey,
            'fairness_weight' => $fairnessWeight,
            'build_id' => $buildId,
        ]);
        $response = $this->control('POST', '/workflows', $body);

        return new WorkflowHandle(
            $this,
            (string) ($response['workflow_id'] ?? $workflowId),
            isset($response['run_id']) ? (string) $response['run_id'] : null,
            (string) ($response['workflow_type'] ?? $workflowType),
        );
    }

    public function workflowHandle(string $workflowId, ?string $selectedRunId = null): WorkflowHandle
    {
        return new WorkflowHandle($this, $workflowId, $selectedRunId);
    }

    public function describeWorkflow(string $workflowId, ?string $runId = null): WorkflowExecution
    {
        $path = '/workflows/'.$this->segment($workflowId);
        if ($runId !== null) {
            $path .= '/runs/'.$this->segment($runId);
        }
        $response = $this->control('GET', $path);

        return WorkflowExecution::fromArray(
            $response,
            $workflowId,
            $runId,
            $this->decodedResponsePayload($response, 'input'),
            $this->decodedResponsePayload($response, 'output'),
        );
    }

    public function listWorkflows(
        ?string $workflowType = null,
        ?string $status = null,
        ?string $query = null,
        ?int $pageSize = null,
        ?string $nextPageToken = null,
    ): WorkflowPage {
        $path = $this->pathWithQuery('/workflows', [
            'workflow_type' => $workflowType,
            'status' => $status,
            'query' => $query,
            'page_size' => $pageSize,
            'next_page_token' => $nextPageToken,
        ]);
        $response = $this->control('GET', $path);
        $executions = [];
        foreach (($response['workflows'] ?? []) as $value) {
            if (is_array($value)) {
                $executions[] = WorkflowExecution::fromArray($value);
            }
        }

        return new WorkflowPage(
            $executions,
            isset($response['next_page_token']) ? (string) $response['next_page_token'] : null,
            isset($response['workflow_count']) ? (int) $response['workflow_count'] : count($executions),
            $response,
        );
    }

    /** @return list<WorkflowRun> */
    public function listWorkflowRuns(string $workflowId): array
    {
        $response = $this->control('GET', '/workflows/'.$this->segment($workflowId).'/runs');
        $runs = [];
        foreach (($response['runs'] ?? []) as $value) {
            if (!is_array($value)) {
                continue;
            }
            $runs[] = new WorkflowRun(
                (string) ($value['workflow_id'] ?? $workflowId),
                (string) ($value['run_id'] ?? ''),
                (string) ($value['workflow_type'] ?? ''),
                isset($value['status']) ? (string) $value['status'] : null,
                (bool) ($value['is_current_run'] ?? false),
                $value,
            );
        }

        return $runs;
    }

    /** @return array<string, mixed> */
    public function workflowHistory(string $workflowId, string $runId): array
    {
        return $this->control(
            'GET',
            '/workflows/'.$this->segment($workflowId).'/runs/'.$this->segment($runId).'/history',
        );
    }

    /** @return array<string, mixed> */
    public function exportWorkflowHistory(string $workflowId, string $runId): array
    {
        return $this->control(
            'GET',
            '/workflows/'.$this->segment($workflowId).'/runs/'.$this->segment($runId).'/history/export',
        );
    }

    /**
     * @param list<mixed> $arguments
     * @return array<string, mixed>
     */
    public function signalWorkflow(
        string $workflowId,
        string $signalName,
        array $arguments = [],
        ?string $runId = null,
    ): array {
        $path = $this->workflowOperationPath($workflowId, $runId, 'signal/'.$this->segment($signalName));

        return $this->control('POST', $path, ['input' => $this->codec->envelope($arguments)], 'signal');
    }

    /** @param list<mixed> $arguments */
    public function queryWorkflow(
        string $workflowId,
        string $queryName,
        array $arguments = [],
        ?string $runId = null,
    ): mixed {
        $path = $this->workflowOperationPath($workflowId, $runId, 'query/'.$this->segment($queryName));
        $response = $this->control('POST', $path, ['input' => $this->codec->envelope($arguments)], 'query');

        return $this->resultFromResponse($response);
    }

    /** @param list<mixed> $arguments */
    public function updateWorkflow(
        string $workflowId,
        string $updateName,
        array $arguments = [],
        string $waitFor = 'completed',
        ?int $waitTimeoutSeconds = null,
        ?string $requestId = null,
        ?string $runId = null,
    ): mixed {
        $path = $this->workflowOperationPath($workflowId, $runId, 'update/'.$this->segment($updateName));
        $response = $this->control('POST', $path, $this->withoutNulls([
            'input' => $this->codec->envelope($arguments),
            'wait_for' => $waitFor,
            'wait_timeout_seconds' => $waitTimeoutSeconds,
            'request_id' => $requestId,
        ]), 'update');

        return $this->resultFromResponse($response);
    }

    /** @return array<string, mixed> */
    public function cancelWorkflow(string $workflowId, ?string $reason = null, ?string $runId = null): array
    {
        return $this->control(
            'POST',
            $this->workflowOperationPath($workflowId, $runId, 'cancel'),
            $this->withoutNulls(['reason' => $reason]),
        );
    }

    /** @return array<string, mixed> */
    public function terminateWorkflow(string $workflowId, ?string $reason = null, ?string $runId = null): array
    {
        return $this->control(
            'POST',
            $this->workflowOperationPath($workflowId, $runId, 'terminate'),
            $this->withoutNulls(['reason' => $reason]),
        );
    }

    public function workflowResult(
        string $workflowId,
        ?string $selectedRunId = null,
        float $timeoutSeconds = 30.0,
        float $pollIntervalSeconds = 0.5,
        bool $followCurrentRun = true,
    ): mixed {
        $deadline = microtime(true) + $timeoutSeconds;
        do {
            $execution = $this->describeWorkflow($workflowId, $followCurrentRun ? null : $selectedRunId);
            $status = strtolower((string) $execution->status);
            if (in_array($status, ['completed', 'failed', 'cancelled', 'canceled', 'terminated', 'timed_out', 'timeout'], true)) {
                $runId = $followCurrentRun ? $execution->runId : ($selectedRunId ?? $execution->runId);
                if ($runId === null) {
                    throw new WorkflowFailed("Workflow {$workflowId} reached a terminal state without a run ID.");
                }

                return $this->terminalResult($workflowId, $runId, $status, $execution);
            }
            if (microtime(true) >= $deadline) {
                throw new WorkflowTimedOut("Workflow {$workflowId} was not terminal after {$timeoutSeconds} seconds.");
            }
            usleep(max(1, (int) ($pollIntervalSeconds * 1_000_000)));
        } while (true);
    }

    public function startServiceOperation(
        string $endpointName,
        string $serviceName,
        string $operationName,
        mixed $arguments = null,
        ?ServiceOperationOptions $options = null,
    ): ServiceOperationHandle {
        $body = $this->serviceOperationBody($arguments, $options);
        $body['mode_override'] = 'async';
        $body['wait_for'] = 'accepted';
        $description = $this->requestServiceOperation(
            $endpointName,
            $serviceName,
            $operationName,
            $body,
        );

        if ($description->serviceCallId === '') {
            throw new ServerException(
                'The server accepted a service operation without returning a service call ID.',
                200,
                'invalid_service_operation_response',
                $description->raw,
            );
        }

        return new ServiceOperationHandle(
            $this,
            $endpointName,
            $serviceName,
            $operationName,
            $description->serviceCallId,
            $description,
        );
    }

    public function executeServiceOperation(
        string $endpointName,
        string $serviceName,
        string $operationName,
        mixed $arguments = null,
        ?ServiceOperationOptions $options = null,
    ): ServiceOperationDescription {
        $body = $this->serviceOperationBody($arguments, $options);
        $body['wait_for'] ??= 'completed';

        return $this->requestServiceOperation($endpointName, $serviceName, $operationName, $body);
    }

    public function describeServiceOperation(
        string $endpointName,
        string $serviceName,
        string $operationName,
        string $serviceCallId,
    ): ServiceOperationDescription {
        $response = $this->control(
            'GET',
            $this->serviceOperationPath(
                $endpointName,
                $serviceName,
                $operationName,
                'service-calls/'.$this->segment($serviceCallId),
            ),
        );

        return ServiceOperationDescription::fromArray($response, $endpointName, $serviceName, $operationName);
    }

    public function cancelServiceOperation(
        string $endpointName,
        string $serviceName,
        string $operationName,
        string $serviceCallId,
        ?string $reason = null,
    ): ServiceOperationDescription {
        $response = $this->control(
            'POST',
            $this->serviceOperationPath(
                $endpointName,
                $serviceName,
                $operationName,
                'service-calls/'.$this->segment($serviceCallId).'/cancel',
            ),
            $this->withoutNulls(['reason' => $reason]),
        );

        return ServiceOperationDescription::fromArray($response, $endpointName, $serviceName, $operationName);
    }

    /**
     * @param array<string, mixed>|null $memo
     * @param array<string, mixed>|null $searchAttributes
     */
    public function createSchedule(
        ScheduleSpec $spec,
        ScheduleAction $action,
        ?string $scheduleId = null,
        ?string $overlapPolicy = null,
        ?int $jitterSeconds = null,
        ?int $maxRuns = null,
        ?array $memo = null,
        ?array $searchAttributes = null,
        bool $paused = false,
        ?string $note = null,
    ): ScheduleHandle {
        $response = $this->control('POST', '/schedules', $this->withoutNulls([
            'schedule_id' => $scheduleId,
            'spec' => $spec->toArray(),
            'action' => $action->toArray($this->codec),
            'overlap_policy' => $overlapPolicy,
            'jitter_seconds' => $jitterSeconds,
            'max_runs' => $maxRuns,
            'memo' => $memo,
            'search_attributes' => $searchAttributes,
            'paused' => $paused ?: null,
            'note' => $note,
        ]));

        return new ScheduleHandle($this, (string) ($response['schedule_id'] ?? $scheduleId ?? ''));
    }

    public function scheduleHandle(string $scheduleId): ScheduleHandle
    {
        return new ScheduleHandle($this, $scheduleId);
    }

    public function describeSchedule(string $scheduleId): ScheduleDescription
    {
        return ScheduleDescription::fromArray(
            $this->control('GET', '/schedules/'.$this->segment($scheduleId)),
            $scheduleId,
        );
    }

    /**
     * Return one server-filtered schedule page.
     *
     * Continuation tokens are opaque and must be reused with the same namespace and filters.
     */
    public function listSchedules(
        ?string $status = null,
        ?string $workflowType = null,
        ?string $query = null,
        ?int $pageSize = null,
        ?string $nextPageToken = null,
    ): SchedulePage
    {
        $response = $this->control('GET', $this->pathWithQuery('/schedules', [
            'status' => $status,
            'workflow_type' => $workflowType,
            'query' => $query,
            'page_size' => $pageSize,
            'next_page_token' => $nextPageToken,
        ]));
        $schedules = [];
        foreach (($response['schedules'] ?? []) as $value) {
            if (is_array($value)) {
                $schedules[] = ScheduleDescription::fromArray($value);
            }
        }

        return new SchedulePage(
            $schedules,
            isset($response['next_page_token']) ? (string) $response['next_page_token'] : null,
            $response,
        );
    }

    /** @param array<string, mixed> $changes */
    public function updateSchedule(string $scheduleId, array $changes): void
    {
        $this->control('PUT', '/schedules/'.$this->segment($scheduleId), $changes);
    }

    public function pauseSchedule(string $scheduleId, ?string $note = null): void
    {
        $this->control('POST', '/schedules/'.$this->segment($scheduleId).'/pause', $this->withoutNulls(['note' => $note]));
    }

    public function resumeSchedule(string $scheduleId, ?string $note = null): void
    {
        $this->control('POST', '/schedules/'.$this->segment($scheduleId).'/resume', $this->withoutNulls(['note' => $note]));
    }

    /** @return array<string, mixed> */
    public function triggerSchedule(string $scheduleId, ?string $overlapPolicy = null): array
    {
        return $this->control(
            'POST',
            '/schedules/'.$this->segment($scheduleId).'/trigger',
            $this->withoutNulls(['overlap_policy' => $overlapPolicy]),
        );
    }

    /** @return array<string, mixed> */
    public function backfillSchedule(
        string $scheduleId,
        string $startTime,
        string $endTime,
        ?string $overlapPolicy = null,
    ): array {
        return $this->control('POST', '/schedules/'.$this->segment($scheduleId).'/backfill', $this->withoutNulls([
            'start_time' => $startTime,
            'end_time' => $endTime,
            'overlap_policy' => $overlapPolicy,
        ]));
    }

    public function deleteSchedule(string $scheduleId): void
    {
        $this->control('DELETE', '/schedules/'.$this->segment($scheduleId));
    }

    /** @return array<string, mixed> */
    public function scheduleHistory(string $scheduleId, ?int $limit = null, ?int $afterSequence = null): array
    {
        $query = http_build_query($this->withoutNulls([
            'limit' => $limit,
            'after_sequence' => $afterSequence,
        ]), '', '&', PHP_QUERY_RFC3986);
        $path = '/schedules/'.$this->segment($scheduleId).'/history';

        return $this->control('GET', $query === '' ? $path : $path.'?'.$query);
    }

    /** @return list<NamespaceDescription> */
    public function listNamespaces(): array
    {
        $response = $this->control('GET', '/namespaces');
        $result = [];
        foreach (($response['namespaces'] ?? []) as $value) {
            if (is_array($value)) {
                $result[] = NamespaceDescription::fromArray($value);
            }
        }

        return $result;
    }

    public function describeNamespace(string $name): NamespaceDescription
    {
        return NamespaceDescription::fromArray($this->control('GET', '/namespaces/'.$this->segment($name)));
    }

    public function createNamespace(string $name, ?string $description = null, int $retentionDays = 30): NamespaceDescription
    {
        return NamespaceDescription::fromArray($this->control('POST', '/namespaces', [
            'name' => $name,
            'description' => $description,
            'retention_days' => $retentionDays,
        ]));
    }

    public function updateNamespace(
        string $name,
        ?string $description = null,
        ?int $retentionDays = null,
    ): NamespaceDescription {
        return NamespaceDescription::fromArray($this->control(
            'PUT',
            '/namespaces/'.$this->segment($name),
            $this->withoutNulls(['description' => $description, 'retention_days' => $retentionDays]),
        ));
    }

    public function deleteNamespace(string $name): NamespaceDescription
    {
        return NamespaceDescription::fromArray($this->control('DELETE', '/namespaces/'.$this->segment($name)));
    }

    /** @param array<string, mixed>|null $config */
    public function setNamespaceExternalStorage(
        string $name,
        string $driver,
        bool $enabled = true,
        ?int $thresholdBytes = null,
        ?array $config = null,
    ): NamespaceDescription {
        if ($thresholdBytes !== null && $thresholdBytes < 1) {
            throw new InvalidArgumentException('Namespace external storage threshold must be at least one byte.');
        }

        return NamespaceDescription::fromArray($this->control(
            'PUT',
            '/namespaces/'.$this->segment($name).'/external-storage',
            $this->withoutNulls([
                'driver' => $driver,
                'enabled' => $enabled,
                'threshold_bytes' => $thresholdBytes,
                'config' => $config,
            ]),
        ));
    }

    public function listSearchAttributes(): SearchAttributeCollection
    {
        return SearchAttributeCollection::fromArray($this->control('GET', '/search-attributes'));
    }

    public function createSearchAttribute(string $name, string $type): SearchAttributeDefinition
    {
        return SearchAttributeDefinition::fromArray($this->control('POST', '/search-attributes', [
            'name' => $name,
            'type' => $type,
        ]), $name);
    }

    public function deleteSearchAttribute(string $name): SearchAttributeDefinition
    {
        return SearchAttributeDefinition::fromArray(
            $this->control('DELETE', '/search-attributes/'.$this->segment($name)),
            $name,
        );
    }

    /**
     * @param list<string> $workflowTypes
     * @param list<string> $activityTypes
     * @param list<string> $capabilities
     * @return array<string, mixed>
     */
    public function registerWorker(
        string $workerId,
        string $taskQueue,
        array $workflowTypes,
        array $activityTypes,
        array $capabilities = ['query_tasks', 'workflow_updates'],
        int $maxConcurrentWorkflowTasks = 1,
        int $maxConcurrentActivityTasks = 1,
        ?string $buildId = null,
    ): array {
        return $this->worker('POST', '/worker/register', $this->withoutNulls([
            'worker_id' => $workerId,
            'task_queue' => $taskQueue,
            'runtime' => 'php',
            'sdk_version' => 'durable-workflow-php/'.Version::SDK,
            'supported_workflow_types' => $workflowTypes,
            'supported_activity_types' => $activityTypes,
            'capabilities' => $capabilities,
            'max_concurrent_workflow_tasks' => $maxConcurrentWorkflowTasks,
            'max_concurrent_activity_tasks' => $maxConcurrentActivityTasks,
            'build_id' => $buildId,
        ]));
    }

    /**
     * @param array<string, int> $taskSlots
     * @return array<string, mixed>
     */
    public function heartbeatWorker(string $workerId, array $taskSlots = []): array
    {
        return $this->worker('POST', '/worker/heartbeat', $this->withoutNulls([
            'worker_id' => $workerId,
            'task_slots' => $taskSlots ?: null,
            'process_metrics' => [
                'process_id' => getmypid(),
                'process_uptime_seconds' => 0,
            ],
        ]));
    }

    public function deregisterWorker(string $workerId): void
    {
        $this->control('DELETE', '/workers/'.$this->segment($workerId));
    }

    /** @return array<string, mixed>|null */
    public function pollWorkflowTask(string $workerId, string $taskQueue, int $timeoutSeconds = 5): ?array
    {
        $response = $this->worker('POST', '/worker/workflow-tasks/poll', [
            'worker_id' => $workerId,
            'task_queue' => $taskQueue,
            'poll_request_id' => $this->requestId('php-workflow-poll'),
            'timeout_seconds' => max(0, min(60, $timeoutSeconds)),
        ]);

        return isset($response['task']) && is_array($response['task']) ? $response['task'] : null;
    }

    /** @return array<string, mixed> */
    public function workflowTaskHistory(
        string $taskId,
        string $leaseOwner,
        int $attempt,
        string $nextPageToken,
    ): array {
        return $this->worker('POST', '/worker/workflow-tasks/'.$this->segment($taskId).'/history', [
            'lease_owner' => $leaseOwner,
            'workflow_task_attempt' => $attempt,
            'next_history_page_token' => $nextPageToken,
        ]);
    }

    /**
     * @param list<array<string, mixed>> $commands
     * @return array<string, mixed>
     */
    public function completeWorkflowTask(string $taskId, string $leaseOwner, int $attempt, array $commands): array
    {
        if ($commands === []) {
            return $this->failWorkflowTask(
                $taskId,
                $leaseOwner,
                $attempt,
                self::WORKFLOW_TASK_WAITING_FOR_HISTORY_MESSAGE,
                self::WORKFLOW_TASK_WAITING_FOR_HISTORY_TYPE,
            );
        }

        return $this->worker('POST', '/worker/workflow-tasks/'.$this->segment($taskId).'/complete', [
            'lease_owner' => $leaseOwner,
            'workflow_task_attempt' => $attempt,
            'commands' => $commands,
        ]);
    }

    /** @return array<string, mixed> */
    public function failWorkflowTask(
        string $taskId,
        string $leaseOwner,
        int $attempt,
        string $message,
        string $failureType = 'PhpWorkflowTaskFailure',
    ): array {
        return $this->worker('POST', '/worker/workflow-tasks/'.$this->segment($taskId).'/fail', [
            'lease_owner' => $leaseOwner,
            'workflow_task_attempt' => $attempt,
            'failure' => ['message' => $message, 'type' => $failureType],
        ]);
    }

    /** @return array<string, mixed> */
    public function heartbeatWorkflowTask(string $taskId, string $leaseOwner, int $attempt): array
    {
        return $this->worker('POST', '/worker/workflow-tasks/'.$this->segment($taskId).'/heartbeat', [
            'lease_owner' => $leaseOwner,
            'workflow_task_attempt' => $attempt,
        ]);
    }

    /** @return array<string, mixed>|null */
    public function pollActivityTask(string $workerId, string $taskQueue, int $timeoutSeconds = 5): ?array
    {
        $response = $this->worker('POST', '/worker/activity-tasks/poll', [
            'worker_id' => $workerId,
            'task_queue' => $taskQueue,
            'poll_request_id' => $this->requestId('php-activity-poll'),
            'timeout_seconds' => max(0, min(60, $timeoutSeconds)),
        ]);

        return isset($response['task']) && is_array($response['task']) ? $response['task'] : null;
    }

    /** @return array<string, mixed> */
    public function completeActivityTask(
        string $taskId,
        string $activityAttemptId,
        string $leaseOwner,
        mixed $result,
    ): array {
        return $this->worker('POST', '/worker/activity-tasks/'.$this->segment($taskId).'/complete', [
            'activity_attempt_id' => $activityAttemptId,
            'lease_owner' => $leaseOwner,
            'result' => $this->codec->envelope($result),
        ]);
    }

    /**
     * @param array<string, mixed>|null $details
     * @return array<string, mixed>
     */
    public function failActivityTask(
        string $taskId,
        string $activityAttemptId,
        string $leaseOwner,
        string $message,
        string $failureType = 'PhpActivityFailure',
        bool $nonRetryable = false,
        ?array $details = null,
    ): array {
        return $this->worker('POST', '/worker/activity-tasks/'.$this->segment($taskId).'/fail', [
            'activity_attempt_id' => $activityAttemptId,
            'lease_owner' => $leaseOwner,
            'failure' => $this->withoutNulls([
                'message' => $message,
                'type' => $failureType,
                'non_retryable' => $nonRetryable,
                'details' => $details !== null ? $this->codec->envelope($details) : null,
            ]),
        ]);
    }

    /**
     * @param array<string, mixed> $details
     * @return array<string, mixed>
     */
    public function heartbeatActivityTask(
        string $taskId,
        string $activityAttemptId,
        string $leaseOwner,
        array $details = [],
    ): array {
        return $this->worker('POST', '/worker/activity-tasks/'.$this->segment($taskId).'/heartbeat', [
            'activity_attempt_id' => $activityAttemptId,
            'lease_owner' => $leaseOwner,
            'details' => $details,
        ]);
    }

    /** @return array<string, mixed>|null */
    public function pollQueryTask(string $workerId, string $taskQueue, int $timeoutSeconds = 5): ?array
    {
        $response = $this->worker('POST', '/worker/query-tasks/poll', [
            'worker_id' => $workerId,
            'task_queue' => $taskQueue,
            'poll_request_id' => $this->requestId('php-query-poll'),
            'timeout_seconds' => max(0, min(60, $timeoutSeconds)),
        ]);

        return isset($response['task']) && is_array($response['task']) ? $response['task'] : null;
    }

    /** @return array<string, mixed> */
    public function completeQueryTask(string $taskId, string $leaseOwner, int $attempt, mixed $result): array
    {
        return $this->worker('POST', '/worker/query-tasks/'.$this->segment($taskId).'/complete', [
            'lease_owner' => $leaseOwner,
            'query_task_attempt' => $attempt,
            'result' => $result,
            'result_envelope' => $this->codec->envelope($result),
        ]);
    }

    /** @return array<string, mixed> */
    public function failQueryTask(
        string $taskId,
        string $leaseOwner,
        int $attempt,
        string $message,
        string $reason = 'query_rejected',
    ): array {
        return $this->worker('POST', '/worker/query-tasks/'.$this->segment($taskId).'/fail', [
            'lease_owner' => $leaseOwner,
            'query_task_attempt' => $attempt,
            'failure' => ['message' => $message, 'reason' => $reason, 'type' => 'PhpQueryFailure'],
        ]);
    }

    /**
     * @param array<string, mixed>|null $body
     * @return array<string, mixed>
     */
    private function control(string $method, string $path, ?array $body = null, ?string $operation = null): array
    {
        return $this->request($method, $path, false, $body, $operation);
    }

    /**
     * @param array<string, mixed>|null $body
     * @return array<string, mixed>
     */
    private function worker(string $method, string $path, ?array $body = null): array
    {
        return $this->request($method, $path, true, $body);
    }

    /**
     * @param array<string, mixed>|null $body
     * @return array<string, mixed>
     */
    private function request(
        string $method,
        string $path,
        bool $worker,
        ?array $body = null,
        ?string $operation = null,
    ): array {
        $headers = [
            'Accept' => 'application/json',
            'Content-Type' => 'application/json',
            'X-Namespace' => $this->namespace,
            $worker ? 'X-Durable-Workflow-Protocol-Version' : 'X-Durable-Workflow-Control-Plane-Version'
                => $worker ? Version::WORKER_PROTOCOL : Version::CONTROL_PLANE_PROTOCOL,
        ];
        if ($this->authentication !== null) {
            $headers = array_merge($headers, $this->authentication->headers($worker));
        }

        try {
            $response = $this->transport->send($method, $this->baseUri.'/api'.$path, $headers, $body);

            return is_array($response) && !array_is_list($response) ? $response : [];
        } catch (TransportException $exception) {
            $details = $exception->response;
            $reason = is_array($details) && isset($details['reason']) ? (string) $details['reason'] : null;
            $status = $exception->status ?? 0;
            $message = $exception->getMessage();
            if ($operation === 'query') {
                throw new QueryFailed($message, $status, $reason, $details, $exception);
            }
            if ($operation === 'update') {
                throw new UpdateFailed($message, $status, $reason, $details, $exception);
            }
            if ($operation === 'signal') {
                throw new SignalFailed($message, $status, $reason, $details, $exception);
            }

            throw new ServerException($message, $status, $reason, $details, $exception);
        }
    }

    /** @param array<string, mixed> $response */
    private function resultFromResponse(array $response): mixed
    {
        if (isset($response['result_envelope']) && is_array($response['result_envelope'])) {
            return $this->codec->decodeEnvelope($response['result_envelope']);
        }
        if (isset($response['result']) && is_array($response['result']) && isset($response['result']['codec'])) {
            return $this->codec->decodeEnvelope($response['result']);
        }

        return $response['result'] ?? $response;
    }

    private function terminalResult(
        string $workflowId,
        string $runId,
        string $status,
        WorkflowExecution $execution,
    ): mixed {
        $history = $this->workflowHistory($workflowId, $runId);
        $events = $history['events'] ?? $history['history_events'] ?? [];
        if (is_array($events)) {
            foreach (array_reverse($events) as $event) {
                if (!is_array($event)) {
                    continue;
                }
                $type = (string) ($event['event_type'] ?? $event['type'] ?? '');
                $payload = isset($event['payload']) && is_array($event['payload']) ? $event['payload'] : [];
                if ($type === 'WorkflowCompleted') {
                    $output = $payload['output'] ?? $payload['result'] ?? null;

                    return (is_array($output) && (isset($output['blob']) || isset($output['codec']))) || is_string($output)
                        ? $this->codec->decodeEnvelope($output)
                        : $output;
                }
                if ($type === 'WorkflowFailed') {
                    throw new WorkflowFailed(
                        (string) ($payload['message'] ?? 'Workflow failed.'),
                        isset($payload['exception_type']) ? (string) $payload['exception_type'] : null,
                        $payload,
                    );
                }
                if ($type === 'WorkflowCancelled') {
                    throw new WorkflowCancelled((string) ($payload['reason'] ?? 'Workflow was cancelled.'));
                }
                if ($type === 'WorkflowTerminated') {
                    throw new WorkflowTerminated((string) ($payload['reason'] ?? 'Workflow was terminated.'));
                }
            }
        }

        return match ($status) {
            'completed' => $execution->output,
            'cancelled', 'canceled' => throw new WorkflowCancelled('Workflow was cancelled.'),
            'terminated' => throw new WorkflowTerminated('Workflow was terminated.'),
            'timed_out', 'timeout' => throw new WorkflowTimedOut('Workflow execution timed out.'),
            default => throw new WorkflowFailed('Workflow failed.'),
        };
    }

    /** @param array<string, mixed> $response */
    private function decodedResponsePayload(array $response, string $field): mixed
    {
        $envelope = $response[$field.'_envelope'] ?? null;
        if (is_array($envelope) || is_string($envelope)) {
            return $this->codec->decodeEnvelope($envelope);
        }

        return $response[$field] ?? null;
    }

    /** @return array<string, mixed> */
    private function serviceOperationBody(mixed $arguments, ?ServiceOperationOptions $options): array
    {
        $body = $options?->toArray() ?? [];
        if ($arguments !== null) {
            $body['arguments'] = $this->codec->encode($arguments);
            $body['payload_codec'] = $this->codec->name();
        }

        return $body;
    }

    /** @param array<string, mixed> $body */
    private function requestServiceOperation(
        string $endpointName,
        string $serviceName,
        string $operationName,
        array $body,
    ): ServiceOperationDescription {
        $response = $this->control(
            'POST',
            $this->serviceOperationPath($endpointName, $serviceName, $operationName, 'execute'),
            $body,
        );

        return ServiceOperationDescription::fromArray($response, $endpointName, $serviceName, $operationName);
    }

    private function serviceOperationPath(
        string $endpointName,
        string $serviceName,
        string $operationName,
        string $suffix,
    ): string {
        return '/service-endpoints/'.$this->segment($endpointName)
            .'/services/'.$this->segment($serviceName)
            .'/operations/'.$this->segment($operationName)
            .'/'.$suffix;
    }

    /** @param array<string, bool|int|float|string|null> $query */
    private function pathWithQuery(string $path, array $query): string
    {
        $encoded = http_build_query($this->withoutNulls($query), '', '&', PHP_QUERY_RFC3986);

        return $encoded === '' ? $path : $path.'?'.$encoded;
    }

    private function workflowOperationPath(string $workflowId, ?string $runId, string $operation): string
    {
        $path = '/workflows/'.$this->segment($workflowId);
        if ($runId !== null) {
            $path .= '/runs/'.$this->segment($runId);
        }

        return $path.'/'.$operation;
    }

    private function segment(string $value): string
    {
        return rawurlencode($value);
    }

    private function requestId(string $prefix): string
    {
        return $prefix.'-'.bin2hex(random_bytes(16));
    }

    /**
     * @param array<string, mixed> $values
     * @return array<string, mixed>
     */
    private function withoutNulls(array $values): array
    {
        return array_filter($values, static fn (mixed $value): bool => $value !== null);
    }
}
