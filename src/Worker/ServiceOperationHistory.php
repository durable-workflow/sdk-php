<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Codec\PayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Exception\ServiceOperationFailed;
use DurableWorkflow\Model\ServiceOperationResult;

/** @internal One original admission and terminal delivery for a durable call. */
final class ServiceOperationHistory
{
    /** @var array<string, mixed>|null */
    private ?array $admission = null;
    /** @var array<string, mixed>|null */
    private ?array $terminal = null;
    private ?string $terminalType = null;
    private int $admissionOrder = PHP_INT_MAX;
    private int $terminalOrder = PHP_INT_MAX;

    public function __construct(private readonly PayloadCodec $codec, private readonly int $sequence)
    {
    }

    /** @param array<string, mixed> $payload */
    public function record(string $type, array $payload, int $order): void
    {
        $nested = is_array($payload['service_call'] ?? null) ? $payload['service_call'] : [];
        $outerId = self::string($payload['service_call_id'] ?? null);
        $nestedId = self::string($nested['service_call_id'] ?? null) ?? self::string($nested['id'] ?? null);
        if ($outerId !== null && $nestedId !== null && $outerId !== $nestedId) {
            $this->reject('Service history contains conflicting call identities.');
        }
        $previous = $this->admission ?? $this->terminal;
        if ($previous !== null) {
            $original = self::surface($previous);
            $incoming = self::surface($payload);
            foreach (['service_call_id', 'id', 'caller_workflow_instance_id', 'caller_workflow_run_id',
                'idempotency_key', 'deadline_policy'] as $field) {
                if (isset($original[$field], $incoming[$field]) && $original[$field] !== $incoming[$field]) {
                    $this->reject("Recorded service {$field} changed.");
                }
            }
            $originalId = self::callId($previous);
            $incomingId = self::callId($payload);
            if ($originalId !== $incomingId) {
                $this->reject('Recorded service call identity changed.');
            }
        }
        if ($type === 'ServiceCallStarted') {
            if ($this->admission !== null && $this->admission !== $payload) {
                $this->reject('Recorded service admission changed.');
            }
            if ($this->admission === null && $this->terminal !== null) {
                $this->reject('Service admission was appended after its terminal delivery.');
            }
            if (self::callId($payload) === null) {
                $this->reject('Recorded service admission is missing its call identity.');
            }
            $this->admission ??= $payload;
            $this->admissionOrder = min($this->admissionOrder, $order);
            return;
        }
        if ($this->terminal !== null && ($this->terminalType !== $type || $this->terminal !== $payload)) {
            $this->reject('Recorded service terminal delivery changed.');
        }
        $this->terminal ??= $payload;
        $this->terminalType ??= $type;
        $this->terminalOrder = min($this->terminalOrder, $order);
    }

    public function assertIntent(WorkflowCommand $command): void
    {
        foreach ([$this->admission, $this->terminal] as $payload) {
            if ($payload === null) {
                continue;
            }
            $surface = self::surface($payload);
            foreach (['endpoint_name', 'service_name', 'operation_name', 'wait_for', 'idempotency_key'] as $field) {
                if (isset($surface[$field]) && $surface[$field] !== ($command->attributes[$field] ?? null)) {
                    $this->reject("Authored service {$field} changed during replay.");
                }
            }
            foreach (['wait_timeout_seconds', 'target_workflow_instance_id', 'target_workflow_run_id'] as $field) {
                if (isset($command->attributes[$field], $surface[$field])
                    && $surface[$field] !== $command->attributes[$field]) {
                    $this->reject("Authored service {$field} changed during replay.");
                }
            }
            if (isset($command->attributes['namespace'], $surface['namespace'])
                && $command->attributes['namespace'] !== $surface['namespace']) {
                $this->reject('Authored service namespace changed during replay.');
            }
            if (isset($surface['operation_mode'])
                && $surface['operation_mode'] !== ($command->attributes['mode_override'] ?? null)) {
                $this->reject('Authored service mode changed during replay.');
            }
            if (array_key_exists('request_payload', $payload) && $payload['request_payload'] !== null
                && $this->codec->encode($this->decode($payload['request_payload']))
                    !== $this->codec->encode($command->attributes['request_payload_value'] ?? null)) {
                $this->reject('Authored service request changed during replay.');
            }
        }
    }

    /** @return array{resolved: bool, value: ?ServiceOperationResult, failure: ?ServiceOperationFailed, resolution_order: int} */
    public function delivery(WorkflowCommand $command): array
    {
        $accepted = ($command->attributes['wait_for'] ?? null) === 'accepted'
            || ($command->attributes['mode_override'] ?? null) === 'async';
        $payload = $accepted && $this->admission !== null ? $this->admission : $this->terminal;
        $order = $accepted && $this->admission !== null ? $this->admissionOrder : $this->terminalOrder;
        if ($payload === null) {
            return ['resolved' => false, 'value' => null, 'failure' => null, 'resolution_order' => PHP_INT_MAX];
        }
        $type = $accepted && $this->admission !== null ? 'ServiceCallStarted' : $this->terminalType;
        $surface = self::surface($payload);
        $id = self::callId($payload);
        if (in_array($type, ['ServiceCallFailed', 'ServiceCallCancelled'], true)) {
            $cancelled = $type === 'ServiceCallCancelled';
            $failureType = self::string($surface['exception_type'] ?? null)
                ?? self::string($surface['caller_observed_error_type'] ?? null)
                ?? self::string($surface['failure_type'] ?? null)
                ?? self::string($surface['outcome_reason'] ?? null)
                ?? ($cancelled ? 'service_call_cancelled' : 'service_operation_failed');
            $message = self::string($surface['message'] ?? null)
                ?? self::string($surface['failure_message'] ?? null)
                ?? ($cancelled ? 'Service operation cancelled.' : 'Service operation failed.');
            return ['resolved' => true, 'value' => null, 'resolution_order' => $order,
                'failure' => new ServiceOperationFailed($message, $id, $failureType, $cancelled, $surface)];
        }
        if ($id === null) {
            $this->reject('Recorded service result is missing its call identity.');
        }
        $response = $payload['response_payload'] ?? $surface['result_envelope'] ?? null;
        $value = $response !== null ? $this->decode($response) : ($surface['result'] ?? null);
        return ['resolved' => true, 'failure' => null, 'resolution_order' => $order,
            'value' => new ServiceOperationResult($id, $type === 'ServiceCallCompleted' ? 'completed' : 'started',
                self::string($surface['outcome'] ?? null), $value, $surface)];
    }

    /**
     * @param array<string, mixed> $payload
     * @return array<string, mixed>
     */
    private static function surface(array $payload): array
    {
        return array_replace(is_array($payload['service_call'] ?? null) ? $payload['service_call'] : [], $payload);
    }

    /** @param array<string, mixed> $payload */
    private static function callId(array $payload): ?string
    {
        $surface = self::surface($payload);
        return self::string($surface['service_call_id'] ?? null) ?? self::string($surface['id'] ?? null);
    }

    private static function string(mixed $value): ?string
    {
        return is_string($value) && $value !== '' ? $value : null;
    }

    private function decode(mixed $payload): mixed
    {
        if (is_array($payload) && (isset($payload['external_payload'])
            || ($payload['schema'] ?? null) === \DurableWorkflow\Transport\RuntimePayloads::SCHEMA)) {
            throw new WorkflowClaimAborted('service_operation_external_payload_unresolved: load history through the bounded SDK transport.');
        }
        if (is_string($payload) || (is_array($payload) && (isset($payload['codec']) || isset($payload['blob'])))) {
            return $this->codec->decodeEnvelope($payload);
        }
        return $payload;
    }

    private function reject(string $message): never
    {
        throw new NonDeterministicWorkflow($message, $this->sequence, reason: 'service_operation_history_conflict');
    }
}
