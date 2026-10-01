<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use InvalidArgumentException;

/** The durable authored call at which the Server committed cancellation delivery. */
final class CancellationDelivery
{
    public const CALL_KINDS = [
        'activity', 'local_activity', 'timer', 'condition', 'signal', 'child', 'parallel', 'selection_handle',
    ];

    private function __construct(
        public readonly string $requestId,
        public readonly int $sequence,
        public readonly string $callKind,
        public readonly int $sequenceSpan,
        public readonly ?int $operationSequence,
        public readonly int $operationSequenceSpan,
    ) {
    }

    /** @param array<string, mixed> $payload */
    public static function fromPayload(array $payload): self
    {
        $requestId = $payload['workflow_command_id'] ?? null;
        if (!is_string($requestId) || trim($requestId) === '') {
            throw new InvalidArgumentException('Delivery must name the original cancellation request.');
        }
        $sequence = self::positive($payload['sequence'] ?? null, 'sequence');
        $kind = $payload['call_kind'] ?? null;
        $span = self::positive(array_key_exists('sequence_span', $payload) ? $payload['sequence_span'] : 1, 'sequence_span', 1000);
        $operation = $payload['operation_sequence'] ?? null;
        $operationSpan = self::positive(
            array_key_exists('operation_sequence_span', $payload) ? $payload['operation_sequence_span'] : 1,
            'operation_sequence_span', 1000,
        );
        if (!is_string($kind) || !in_array($kind, self::CALL_KINDS, true)
            || ($kind !== 'parallel' && $span !== 1)) {
            throw new InvalidArgumentException('Delivery kind and span must describe a durable call.');
        }
        if ($sequence > PHP_INT_MAX - $span) {
            throw new InvalidArgumentException('Delivery sequence range overflows.');
        }
        if ($kind === 'selection_handle') {
            $operation = self::positive($operation, 'operation_sequence');
            if ($operation >= $sequence || $operationSpan > $sequence - $operation) {
                throw new InvalidArgumentException('A selection handle must name an earlier authored operation.');
            }
        } elseif ($operation !== null || $operationSpan !== 1) {
            throw new InvalidArgumentException('Only selection handles may carry an operation range.');
        }

        return new self($requestId, $sequence, $kind, $span, $operation, $operationSpan);
    }

    public function interrupts(int $sequence): bool
    {
        $base = $this->operationSequence ?? $this->sequence;
        $span = $this->operationSequence === null ? $this->sequenceSpan : $this->operationSequenceSpan;

        return $sequence >= $base && $sequence < $base + $span;
    }

    private static function positive(mixed $value, string $field, int $maximum = PHP_INT_MAX): int
    {
        if (!is_int($value) || $value < 1 || $value > $maximum) {
            throw new InvalidArgumentException("Delivery {$field} must be a positive integer within {$maximum}.");
        }

        return $value;
    }
}
