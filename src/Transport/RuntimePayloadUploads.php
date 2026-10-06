<?php

declare(strict_types=1);

namespace DurableWorkflow\Transport;

use DurableWorkflow\Exception\ExternalPayloadException;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Version;

/** Namespace-scoped discovery and request-local, content-addressed uploads. */
final class RuntimePayloadUploads
{
    private const COMPLETION_SCHEMA = 'durable-workflow.v2.payload-completion-context.v1';
    private const PREPARED_COMPLETION_SCHEMA = 'durable-workflow.v2.payload-completion-context.v2';
    private const COMPLETION_HEADER = 'X-Durable-Workflow-Payload-Completion';
    /** @var array<string, list<string>> */
    public const COMMAND_FIELDS = [
        'complete_workflow' => ['result'],
        'schedule_activity' => ['arguments'],
        'start_child_workflow' => ['arguments'],
        'continue_as_new' => ['arguments'],
        'complete_update' => ['result'],
        'record_side_effect' => ['result'],
        'record_local_activity' => ['arguments', 'result'],
        'prepare_local_activity' => ['arguments'],
        'start_service_operation' => ['request_payload'],
        'upsert_memo' => ['entries'],
    ];

    /** @var array<int, array{expires: int, policy: array<string, mixed>}> */
    private array $policies = [];

    public function __construct(private readonly Transport $transport, private readonly string $baseUri)
    {
    }

    /**
     * @param array<string, mixed> $body
     * @param array<string, string> $headers
     * @return array<string, mixed>
     */
    public function request(array $body, string $method, string $path, bool $worker, array $headers, ?RequestBudget $budget = null): array
    {
        if (!$this->transport instanceof PayloadUploadTransport || !in_array(strtoupper($method), ['POST', 'PUT', 'PATCH'], true)) {
            return $body;
        }
        $slots = $this->paths($body, explode('?', $path)[0], $worker);
        $payloads = [];
        foreach ($slots as $segments) {
            $value = $this->get($body, $segments);
            if (is_array($value) && array_key_exists('external_payload', $value)) {
                if (count($value) !== 2 || ($value['codec'] ?? null) !== 'avro') {
                    throw new ExternalPayloadException('Invalid runtime payload envelope.', 422, 'external_payload_unsupported');
                }
                RuntimePayloads::validateReference($value['external_payload']);
            }
            $blob = is_string($value) ? $value : (is_array($value) && count($value) === 2
                && ($value['codec'] ?? null) === 'avro' ? ($value['blob'] ?? null) : null);
            if (is_string($blob)) {
                $payloads[] = ['path' => $segments, 'blob' => $blob];
            }
        }
        if ($payloads === []) {
            return $body;
        }

        try {
            $policy = $this->policy($headers, $worker, $budget);
        } catch (TransportException $exception) {
            $discovery = $exception->response;
            if ($worker
                && preg_match('~\A/worker/activity-tasks/([^/]+)/complete\z~', $path, $matches) === 1
                && $exception->status === 503
                && is_array($discovery) && !array_is_list($discovery)
                && ($discovery['reason'] ?? null) === 'backend_unavailable'
                && ($discovery['operation'] ?? null) === 'cluster_info'
                && ($discovery['retryable'] ?? null) === true
                && is_string($body['activity_attempt_id'] ?? null)
                && $body['activity_attempt_id'] !== ''
                && is_string($body['lease_owner'] ?? null)
                && $body['lease_owner'] !== '') {
                // Discovery failed before any upload or task completion was
                // sent. Preserve the completion bytes and its lease fence.
                $retryAfter = $discovery['retry_after_seconds'] ?? null;
                throw new ExternalPayloadException(
                    'Runtime payload policy discovery is temporarily unavailable.',
                    503,
                    'payload_discovery_unavailable',
                    [
                        'reason' => 'payload_discovery_unavailable',
                        'operation' => 'complete_activity_task',
                        'request_admitted' => false,
                        'retryable' => true,
                        'task_id' => rawurldecode($matches[1]),
                        'activity_attempt_id' => $body['activity_attempt_id'],
                        'lease_owner' => $body['lease_owner'],
                        'retry_after_seconds' => is_int($retryAfter) && $retryAfter > 0 ? $retryAfter : 1,
                    ],
                    $exception,
                );
            }

            throw $exception;
        }
        // Plan the entire request before uploading anything. Several individually
        // inline values can still exceed the ordinary JSON request limit.
        $selected = [];
        usort($payloads, static fn (array $a, array $b): int => strlen($b['blob']) <=> strlen($a['blob']));
        foreach ($payloads as $index => $payload) {
            if (strlen($payload['blob']) > $policy['max_bytes']) {
                throw new ExternalPayloadException('Encoded payload exceeds the runtime upload limit.', 413, 'external_payload_oversized');
            }
            if (strlen($payload['blob']) > $policy['threshold_bytes']) {
                $selected[$index] = $this->placeholder($body, $payload);
            }
        }
        foreach ($payloads as $index => $payload) {
            if ($this->jsonSize($body) <= $policy['request_bytes']) {
                break;
            }
            if (!isset($selected[$index])) {
                $selected[$index] = $this->placeholder($body, $payload);
            }
        }
        if ($this->jsonSize($body) > $policy['request_bytes']) {
            throw new ExternalPayloadException('Request metadata exceeds the ordinary API limit even with external payloads.', 413, 'payload_too_large');
        }
        if ($selected !== [] && $policy['status'] !== 'available') {
            throw new ExternalPayloadException('Namespace runtime external payload storage is not available.', 503, 'external_payload_unavailable');
        }

        $uploaded = [];
        $upload = function (array $uploadHeaders, string $blob) use ($policy, $budget): array {
            $uri = $this->baseUri.'/api/external-payloads/v1';
            if ($budget === null) {
                return $this->transport->uploadPayload($uri, $uploadHeaders, $blob, $policy['timeout_seconds']);
            }
            if (!$this->transport instanceof BoundedPayloadUploadTransport) {
                throw new ExternalPayloadException('The cooperative transport must bound payload uploads.', 415, 'external_payload_unsupported');
            }
            $response = $this->transport->uploadPayloadBounded($uri, $uploadHeaders, $blob, min($budget->remainingSeconds(), $policy['timeout_seconds']));
            $budget->remainingSeconds();

            return $response;
        };
        foreach ($selected as $index => $expected) {
            $blob = $payloads[$index]['blob'];
            $key = $expected['sha256'].':'.$expected['size_bytes'];
            if (!isset($uploaded[$key])) {
                try {
                    $uploadHeaders = array_replace($headers, [
                        'Content-Type' => 'application/octet-stream',
                        'Accept' => 'application/json',
                        'X-Durable-Workflow-Payload-Codec' => 'avro',
                        'X-Durable-Workflow-Payload-Size' => (string) strlen($blob),
                        'X-Durable-Workflow-Payload-SHA256' => $expected['sha256'],
                    ]);
                    try {
                        $response = $upload($uploadHeaders, $blob);
                    } catch (TransportException $refusal) {
                        $context = $worker && ($policy['completion_context'] ?? false)
                            ? $this->completionContext($body, $path, $payloads[$index]['path'],
                                ($policy['prepared_completion_context'] ?? false)
                                && ($headers['X-Durable-Workflow-Protocol-Version'] ?? null) === Version::COOPERATIVE_CANCELLATION_MINIMUM_WORKER_PROTOCOL,
                                ($policy['scope_completion_context'] ?? false)
                                && ($headers['X-Durable-Workflow-Protocol-Version'] ?? null) === Version::COOPERATIVE_CANCELLATION_MINIMUM_WORKER_PROTOCOL) : null;
                        if ($context === null || $refusal->status !== 503
                            || ($refusal->response['reason'] ?? null) !== 'storage_pressure'
                            || ($refusal->response['storage_state'] ?? null) !== 'draining'
                            || ($refusal->response['request_admitted'] ?? null) !== false) {
                            throw $refusal;
                        }
                        // One capability-negotiated retry; never turn other pressure
                        // errors into retries or repeat application activity code.
                        $uploadHeaders[self::COMPLETION_HEADER] = $context;
                        $response = $upload($uploadHeaders, $blob);
                    }
                } catch (TransportException $exception) {
                    $reason = $exception->response['reason'] ?? null;
                    $reason = is_string($reason) ? $reason : match ($exception->status) {
                        401, 403 => 'external_payload_unauthorized',
                        413 => 'external_payload_oversized',
                        415, 422 => 'external_payload_unsupported',
                        default => 'external_payload_unavailable',
                    };
                    throw new ExternalPayloadException('Runtime external payload upload failed.', $exception->status ?? 0,
                        $reason, $exception->response, $exception);
                }
                if (($response['schema'] ?? null) !== 'durable-workflow.v2.runtime-external-payload-upload.v1'
                    || ($response['transport_version'] ?? null) !== 1) {
                    throw new ExternalPayloadException('Unsupported runtime upload response.', 422, 'external_payload_unsupported');
                }
                $reference = RuntimePayloads::validateReference($response['reference'] ?? null);
                if ($reference['size_bytes'] !== strlen($blob) || !hash_equals($expected['sha256'], $reference['sha256'])) {
                    throw new ExternalPayloadException('Runtime upload reference differs from the submitted bytes.', 422, 'external_payload_integrity_mismatch');
                }
                $uploaded[$key] = $reference;
            }
            $this->replace($body, $payloads[$index]['path'], $uploaded[$key]);
        }

        return $body;
    }

    /** @param array<string, string> $headers
     * @return array{threshold_bytes: int, max_bytes: int, request_bytes: int, timeout_seconds: int, status: string, completion_context?: bool, prepared_completion_context?: bool, scope_completion_context?: bool}
     */
    private function policy(array $headers, bool $worker, ?RequestBudget $budget = null): array
    {
        $key = (int) $worker;
        if (isset($this->policies[$key]) && $this->policies[$key]['expires'] > time()) {
            return $this->policies[$key]['policy'];
        }
        // Discovery accepts either runtime role. Never require a client key in a worker.
        unset($headers['X-Durable-Workflow-Protocol-Version']);
        $headers['X-Durable-Workflow-Control-Plane-Version'] = Version::CONTROL_PLANE_PROTOCOL;
        if ($budget === null) {
            $info = $this->transport->send('GET', $this->baseUri.'/api/cluster/info', $headers);
        } else {
            if (!$this->transport instanceof BoundedTransport || !$this->transport->supportsBoundedRequests()) {
                throw new ExternalPayloadException('The cooperative transport must bound payload discovery.', 415, 'external_payload_unsupported');
            }
            $info = $this->transport->sendBounded('GET', $this->baseUri.'/api/cluster/info', $headers, null, $budget->remainingSeconds());
            $budget->remainingSeconds();
        }
        $storage = $info['namespace']['external_payload_storage'] ?? [];
        $manifest = $storage['transport'] ?? null;
        $requestLimit = $info['limits']['max_payload_bytes'] ?? 2097152;
        if (!is_int($requestLimit) || $requestLimit < 1) {
            throw new ExternalPayloadException('Invalid ordinary request limit in runtime discovery.', 422, 'external_payload_unsupported');
        }
        if ($manifest === null) {
            $policy = ['threshold_bytes' => $requestLimit, 'max_bytes' => $requestLimit,
                'request_bytes' => $requestLimit, 'timeout_seconds' => 30, 'status' => 'unsupported'];
        } else {
            $threshold = $storage['threshold_bytes'] ?? null;
            $max = $manifest['limits']['max_payload_bytes'] ?? null;
            $timeout = $manifest['limits']['request_timeout_seconds'] ?? null;
            if (!is_array($manifest) || ($manifest['schema'] ?? null) !== 'durable-workflow.v2.runtime-external-payload-transport.v1'
                || ($manifest['version'] ?? null) !== 1 || ($manifest['reference_schema'] ?? null) !== RuntimePayloads::SCHEMA
                || ($manifest['mode'] ?? null) !== 'authenticated_namespace_runtime'
                || ($manifest['upload']['method'] ?? null) !== 'POST' || ($manifest['upload']['path'] ?? null) !== '/api/external-payloads/v1'
                || ($manifest['fetch']['method'] ?? null) !== 'GET' || ($manifest['fetch']['path_template'] ?? null) !== '/api/external-payloads/v1/{referenceId}'
                || !is_int($threshold) || $threshold < 1 || !is_int($max) || $max < $threshold
                || !is_int($timeout) || $timeout < 1) {
                throw new ExternalPayloadException('Unsupported namespace runtime payload transport discovery.', 422, 'external_payload_unsupported');
            }
            $policy = ['threshold_bytes' => $threshold, 'max_bytes' => $max, 'request_bytes' => $requestLimit,
                'timeout_seconds' => $timeout, 'status' => is_string($storage['status'] ?? null) ? $storage['status'] : 'unavailable',
                'completion_context' => ($manifest['upload']['completion_context']['schema'] ?? null) === self::COMPLETION_SCHEMA
                    && ($manifest['upload']['completion_context']['header'] ?? null) === self::COMPLETION_HEADER,
                'prepared_completion_context' => ($manifest['upload']['completion_context']['prepared_schema'] ?? null) === self::PREPARED_COMPLETION_SCHEMA,
                'scope_completion_context' => ($manifest['upload']['completion_context']['scope_schema'] ?? null) === self::PREPARED_COMPLETION_SCHEMA];
        }
        $this->policies[$key] = ['expires' => time() + 60, 'policy' => $policy];

        return $policy;
    }

    /** @param array<string, mixed> $body
     * @return list<list<int|string>>
     */
    private function paths(array $body, string $path, bool $worker): array
    {
        $paths = [];
        if ($worker) {
            if (preg_match('~\A/worker/workflow-tasks/[^/]+/(?:complete|local-activities/checkpoint(?:-group)?|cancellation-scopes/checkpoint)\z~', $path)) {
                foreach ($body['commands'] ?? [] as $index => $command) {
                    if (!is_array($command)) {
                        continue;
                    }
                    foreach (self::COMMAND_FIELDS[$command['type'] ?? ''] ?? [] as $field) {
                        $paths[] = ['commands', $index, $field];
                    }
                    if (($command['type'] ?? null) === 'fail_workflow') {
                        $paths[] = ['commands', $index, 'exception', 'details'];
                    }
                    foreach ($command['workflow_stream']['items'] ?? [] as $item => $_) {
                        $paths[] = ['commands', $index, 'workflow_stream', 'items', $item, 'payload'];
                    }
                }
            } elseif (preg_match('~\A/worker/workflow-tasks/[^/]+/local-activities/(?:prepare|recover)\z~', $path)) {
                $paths[] = ['descriptor', 'arguments'];
            } elseif (preg_match('~\A/worker/workflow-tasks/[^/]+/local-activities/[^/]+/outcome\z~', $path)) {
                $paths[] = ['report', 'result'];
            } elseif (preg_match('~\A/worker/activity-tasks/[^/]+/(complete|fail)\z~', $path, $match)) {
                $paths[] = $match[1] === 'complete' ? ['result'] : ['failure', 'details'];
            } elseif (preg_match('~\A/worker/query-tasks/[^/]+/complete\z~', $path)) {
                $paths[] = ['result_envelope'];
            }
        } else {
            if (in_array($path, ['/workflows', '/activities'], true)
                || preg_match('~\A/workflows/[^/]+(?:/runs/[^/]+)?/(?:signal|query|update)/[^/]+\z~', $path)
                || preg_match('~\A/workflows/[^/]+/message-streams/[^/]+/messages\z~', $path)) {
                $paths[] = ['input'];
            } elseif (preg_match('~\A/schedules(?:/[^/]+)?\z~', $path)) {
                $paths[] = ['action', 'input'];
            } elseif (preg_match('~\A/service-endpoints/[^/]+/services/[^/]+/operations/[^/]+/execute\z~', $path)) {
                $paths[] = ['arguments'];
            } elseif (preg_match('~\A/workflows/[^/]+/runs/[^/]+/streams/[^/]+/items\z~', $path)) {
                foreach ($body['items'] ?? [] as $index => $_) {
                    $paths[] = ['items', $index, 'payload'];
                }
            }
        }

        return $paths;
    }

    /** @param array<string, mixed> $body
     * @param list<int|string> $slot
     */
    private function completionContext(array $body, string $path, array $slot, bool $prepared, bool $scope): ?string
    {
        $path = explode('?', $path)[0];
        if ($scope && preg_match('~\A/worker/workflow-tasks/([^/]+)/cancellation-scopes/checkpoint\z~', $path, $match)) {
            $attempt = $body['workflow_task_attempt'] ?? null;
            $owner = $body['lease_owner'] ?? null;
            $task = rawurldecode($match[1]);
            $checkpoint = $body['checkpoint_id'] ?? null;
            if (!self::contextIdentifier($owner) || !self::contextIdentifier($task) || !self::contextIdentifier($checkpoint)
                || !is_int($attempt) || $attempt < 1) {
                return null;
            }
            $context = json_encode(['schema' => self::PREPARED_COMPLETION_SCHEMA, 'kind' => 'workflow',
                'task_id' => $task, 'attempt' => $attempt, 'lease_owner' => $owner,
                'operation' => 'cancellation_scope_checkpoint', 'slot' => $slot, 'checkpoint_id' => $checkpoint], JSON_THROW_ON_ERROR);

            return strlen($context) <= 4096 ? $context : null;
        }
        if ($prepared && preg_match('~\A/worker/workflow-tasks/([^/]+)/local-activities/(?:(checkpoint(?:-group)?|prepare|recover)|([^/]+)/outcome)\z~', $path, $match)) {
            $attempt = $body['workflow_task_attempt'] ?? null;
            $owner = $body['lease_owner'] ?? null;
            $task = rawurldecode($match[1]);
            $operation = ($match[2] ?? '') !== '' ? $match[2] : 'outcome';
            $identity = match ($operation) {
                'checkpoint', 'checkpoint-group' => ['checkpoint_id' => $body['checkpoint_id'] ?? null],
                'outcome' => ['activity_attempt_id' => rawurldecode($match[3])],
                default => ['sequence' => $body['sequence'] ?? null],
            };
            $value = reset($identity);
            if (!self::contextIdentifier($owner) || !self::contextIdentifier($task)
                || !is_int($attempt) || $attempt < 1
                || ($operation === 'prepare' || $operation === 'recover'
                    ? !is_int($value) || $value < 1 : !self::contextIdentifier($value))) {
                return null;
            }
            $context = json_encode(['schema' => self::PREPARED_COMPLETION_SCHEMA, 'kind' => 'workflow',
                'task_id' => $task, 'attempt' => $attempt, 'lease_owner' => $owner,
                'operation' => $operation === 'checkpoint-group' ? 'local_activity_group_checkpoint' : 'local_activity_'.$operation,
                'slot' => $slot, ...$identity], JSON_THROW_ON_ERROR);

            return strlen($context) <= 4096 ? $context : null;
        }
        if (!preg_match('~\A/worker/(activity|workflow|query)-tasks/([^/]+)/(complete|fail)\z~', explode('?', $path)[0], $match)) {
            return null;
        }
        $kind = $match[1];
        $attempt = $body[$kind === 'activity' ? 'activity_attempt_id' : $kind.'_task_attempt'] ?? null;
        $owner = $body['lease_owner'] ?? null;
        if (!is_string($owner) || $owner === '' || ($kind === 'activity'
            ? !is_string($attempt) || $attempt === '' : !is_int($attempt) || $attempt < 1)) {
            return null;
        }
        $context = json_encode(['schema' => self::COMPLETION_SCHEMA, 'kind' => $kind,
            'task_id' => rawurldecode($match[2]), 'attempt' => $attempt, 'lease_owner' => $owner,
            'operation' => $match[3], 'slot' => $slot], JSON_THROW_ON_ERROR);

        return strlen($context) <= 4096 ? $context : null;
    }

    private static function contextIdentifier(mixed $value): bool
    {
        return is_string($value) && $value !== '' && strlen($value) <= 255
            && preg_match('/[\x00-\x1f\x7f]/', $value) === 0;
    }

    /** @param array<string, mixed> $body
     * @param array{path: list<int|string>, blob: string} $payload
     * @return array{schema: string, reference_id: string, codec: string, size_bytes: int, sha256: string}
     */
    private function placeholder(array &$body, array $payload): array
    {
        $reference = ['schema' => RuntimePayloads::SCHEMA, 'reference_id' => 'ep_00000000000000000000000000',
            'codec' => 'avro', 'size_bytes' => strlen($payload['blob']), 'sha256' => hash('sha256', $payload['blob'])];
        $this->replace($body, $payload['path'], $reference);

        return $reference;
    }

    /** @param array<string, mixed> $body
     * @param list<int|string> $segments
     * @param array<string, mixed> $reference
     */
    private function replace(array &$body, array $segments, array $reference): void
    {
        $value = &$body;
        $last = array_pop($segments);
        foreach ($segments as $key) {
            $value = &$value[$key];
        }
        if ($last === 'payload') {
            unset($value['payload']);
            $value['payload_reference'] = $reference;
            $value['payload_codec'] = 'avro';
        } else {
            $value[$last] = ['codec' => 'avro', 'external_payload' => $reference];
            if ($last === 'result_envelope') {
                $value['result'] = null;
            }
        }
    }

    /** @param list<int|string> $segments */
    private function get(mixed $value, array $segments): mixed
    {
        foreach ($segments as $key) {
            if (!is_array($value) || !array_key_exists($key, $value)) {
                return null;
            }
            $value = $value[$key];
        }

        return $value;
    }

    private function jsonSize(mixed $value): int
    {
        return strlen(json_encode($value, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE));
    }
}
