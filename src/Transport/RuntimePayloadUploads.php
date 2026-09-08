<?php

declare(strict_types=1);

namespace DurableWorkflow\Transport;

use DurableWorkflow\Exception\ExternalPayloadException;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Version;

/** Namespace-scoped discovery and request-local, content-addressed uploads. */
final class RuntimePayloadUploads
{
    /** @var array<string, list<string>> */
    public const COMMAND_FIELDS = [
        'complete_workflow' => ['result'],
        'schedule_activity' => ['arguments'],
        'start_child_workflow' => ['arguments'],
        'continue_as_new' => ['arguments'],
        'complete_update' => ['result'],
        'record_side_effect' => ['result'],
        'record_local_activity' => ['arguments', 'result'],
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
    public function request(array $body, string $method, string $path, bool $worker, array $headers): array
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

        $policy = $this->policy($headers, $worker);
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
        foreach ($selected as $index => $expected) {
            $blob = $payloads[$index]['blob'];
            $key = $expected['sha256'].':'.$expected['size_bytes'];
            if (!isset($uploaded[$key])) {
                try {
                    $response = $this->transport->uploadPayload($this->baseUri.'/api/external-payloads/v1', array_replace($headers, [
                        'Content-Type' => 'application/octet-stream',
                        'Accept' => 'application/json',
                        'X-Durable-Workflow-Payload-Codec' => 'avro',
                        'X-Durable-Workflow-Payload-Size' => (string) strlen($blob),
                        'X-Durable-Workflow-Payload-SHA256' => $expected['sha256'],
                    ]), $blob, $policy['timeout_seconds']);
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
     * @return array{threshold_bytes: int, max_bytes: int, request_bytes: int, timeout_seconds: int, status: string}
     */
    private function policy(array $headers, bool $worker): array
    {
        $key = (int) $worker;
        if (isset($this->policies[$key]) && $this->policies[$key]['expires'] > time()) {
            return $this->policies[$key]['policy'];
        }
        // Discovery accepts either runtime role. Never require a client key in a worker.
        unset($headers['X-Durable-Workflow-Protocol-Version']);
        $headers['X-Durable-Workflow-Control-Plane-Version'] = Version::CONTROL_PLANE_PROTOCOL;
        $info = $this->transport->send('GET', $this->baseUri.'/api/cluster/info', $headers);
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
                'timeout_seconds' => $timeout, 'status' => is_string($storage['status'] ?? null) ? $storage['status'] : 'unavailable'];
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
            if (preg_match('~\A/worker/workflow-tasks/[^/]+/complete\z~', $path)) {
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
