<?php

declare(strict_types=1);

namespace DurableWorkflow\Transport;

use DurableWorkflow\Exception\ExternalPayloadException;

/** Resolve only protocol envelope fields, never arbitrary application projections. */
final class RuntimePayloads
{
    private const SCHEMA = 'durable-workflow.v2.runtime-external-payload-reference.v1';

    /** @var array<string, string> */
    private array $fetched = [];
    private int $bytes = 0;

    /** @param array<string, string> $headers */
    public function __construct(
        private readonly Transport $transport,
        private readonly string $baseUri,
        private readonly array $headers,
        private readonly int $limit,
    ) {
    }

    /**
     * @param array<string, mixed> $response
     * @return array<string, mixed>
     */
    public function response(array $response, string $requestPath, bool $worker): array
    {
        $path = explode('?', $requestPath)[0];
        $paths = [];
        if ($worker) {
            foreach (['arguments', 'workflow_arguments', 'query_arguments', 'signal_arguments', 'update_arguments'] as $field) {
                $paths[] = ['task', $field];
            }
            $paths = [...$paths, ...$this->historyPaths(['task', 'history_events']), ...$this->historyPaths(['history_events']),
                ...$this->exportPaths(['task', 'history_export'])];
        } else {
            foreach (['input_envelope', 'output_envelope', 'result_envelope'] as $field) {
                $paths[] = [$field];
            }
            if (preg_match('~\A/activities/[^/]+\z~', $path)) {
                $paths[] = ['result'];
            }
            if (str_starts_with($path, '/schedules')) {
                $paths[] = ['action', 'input'];
                $paths[] = ['schedules', '*', 'action', 'input'];
            }
            if (str_ends_with($path, '/history')) {
                $paths = [...$paths, ...$this->historyPaths(['events'])];
            }
            if (str_ends_with($path, '/export')) {
                $paths = [...$paths, ...$this->exportPaths([])];
            }
        }
        foreach ($paths as $segments) {
            $response = $this->at($response, $segments);
        }

        return $response;
    }

    /**
     * @param list<string> $prefix
     * @return list<list<string>>
     */
    private function historyPaths(array $prefix): array
    {
        $paths = [];
        foreach ([['arguments'], ['result'], ['output'], ['activity', 'arguments'], ['activity', 'result'],
            ['command', 'payload'], ['exception', 'details']] as $field) {
            $paths[] = [...$prefix, '*', 'payload', ...$field];
        }

        return $paths;
    }

    /**
     * @param list<string> $prefix
     * @return list<list<string>>
     */
    private function exportPaths(array $prefix): array
    {
        $paths = $this->historyPaths([...$prefix, 'history_events']);
        foreach ([['activities', '*', 'arguments'], ['activities', '*', 'result'], ['commands', '*', 'payload'],
            ['payloads', 'arguments', 'data'], ['payloads', 'output', 'data'], ['signals', '*', 'arguments'],
            ['timeline', '*', 'command', 'payload'], ['updates', '*', 'arguments'], ['updates', '*', 'result']] as $field) {
            $paths[] = [...$prefix, ...$field];
        }

        return $paths;
    }

    /** @param list<string> $segments */
    private function at(mixed $value, array $segments): mixed
    {
        if ($segments === []) {
            return $this->envelope($value);
        }
        if (!is_array($value)) {
            return $value;
        }
        $key = array_shift($segments);
        if ($key === '*') {
            foreach ($value as $index => $item) {
                $value[$index] = $this->at($item, $segments);
            }
        } elseif (array_key_exists($key, $value)) {
            $value[$key] = $this->at($value[$key], $segments);
        }

        return $value;
    }

    private function envelope(mixed $value): mixed
    {
        if (!is_array($value) || !array_key_exists('external_payload', $value)) {
            return $value;
        }
        $reference = $value['external_payload'];
        if (count($value) !== 2 || ($value['codec'] ?? null) !== 'avro' || !is_array($reference)
            || count($reference) !== 5 || ($reference['schema'] ?? null) !== self::SCHEMA
            || ($reference['codec'] ?? null) !== 'avro'
            || !is_string($reference['reference_id'] ?? null) || preg_match('/\Aep_[0-9A-HJKMNP-TV-Z]{26}\z/', $reference['reference_id']) !== 1
            || !is_string($reference['sha256'] ?? null) || preg_match('/\A[a-f0-9]{64}\z/', $reference['sha256']) !== 1
            || !is_int($reference['size_bytes'] ?? null) || $reference['size_bytes'] < 0) {
            throw new ExternalPayloadException('Invalid runtime external payload reference.', 422, 'external_payload_unsupported');
        }
        $key = $reference['reference_id'].':'.$reference['sha256'].':'.$reference['size_bytes'];
        if (!array_key_exists($key, $this->fetched)) {
            if ($reference['size_bytes'] > $this->limit - $this->bytes) {
                throw new ExternalPayloadException('External payloads exceed the configured per-response byte limit.', 413, 'external_payload_oversized');
            }
            if (!$this->transport instanceof PayloadTransport) {
                throw new ExternalPayloadException('The configured transport must implement PayloadTransport to read external payloads.', 415, 'external_payload_unsupported');
            }
            $headers = array_replace($this->headers, [
                'Accept' => 'application/octet-stream',
                'X-Durable-Workflow-Payload-Codec' => 'avro',
                'X-Durable-Workflow-Payload-Size' => (string) $reference['size_bytes'],
                'X-Durable-Workflow-Payload-SHA256' => $reference['sha256'],
            ]);
            // IDs cannot contain a URL or path. Use only the already authenticated runtime.
            $blob = $this->transport->fetchPayload($this->baseUri.'/api/external-payloads/v1/'.$reference['reference_id'], $headers, $reference['size_bytes']);
            if (strlen($blob) !== $reference['size_bytes'] || !hash_equals($reference['sha256'], hash('sha256', $blob))) {
                throw new ExternalPayloadException('Runtime payload bytes differ from the authenticated reference.', 422, 'external_payload_integrity_mismatch');
            }
            $this->bytes += strlen($blob);
            $this->fetched[$key] = $blob;
        }

        return ['codec' => 'avro', 'blob' => $this->fetched[$key]];
    }
}
