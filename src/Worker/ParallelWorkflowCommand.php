<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use LogicException;

/** @internal A deterministic tree of operations suspended by one all/parallel barrier. */
class ParallelWorkflowCommand
{
    /** @var list<DeferredWorkflowOperation|self> */
    public readonly array $operations;

    /** @var list<int|string> */
    public readonly array $keys;

    /** @param iterable<mixed, mixed> $operations */
    public function __construct(iterable $operations, public readonly string $mode = 'all')
    {
        if (!in_array($mode, ['all', 'select'], true)) {
            throw new LogicException("Unsupported durable group mode {$mode}.");
        }

        $normalized = [];
        $keys = [];
        $seenSelectionKeys = [];
        foreach ($operations as $key => $operation) {
            if (!is_int($key) && !is_string($key)) {
                throw new LogicException(sprintf(
                    'WorkflowContext::select() member keys must be integers or strings; received %s.',
                    get_debug_type($key),
                ));
            }
            if ($mode === 'select' && ((is_string($key) && $key === '') || (is_int($key) && $key < 0))) {
                throw new LogicException(
                    'WorkflowContext::select() member keys must be non-empty strings or non-negative integers.',
                );
            }
            if ($mode === 'select' && array_key_exists($key, $seenSelectionKeys)) {
                throw new LogicException(sprintf(
                    'WorkflowContext::select() member key %s is duplicated.',
                    (string) $key,
                ));
            }
            if (!$operation instanceof DeferredWorkflowOperation && !$operation instanceof self) {
                throw new LogicException(sprintf(
                    'WorkflowContext::%s() accepts deferred workflow operations or nested all()/parallel() barriers; received %s.',
                    $mode === 'select' ? 'select' : 'all',
                    get_debug_type($operation),
                ));
            }
            if ($mode === 'select' && $operation instanceof self && $operation->mode === 'select') {
                throw new LogicException('WorkflowContext::select() does not support nested selection groups.');
            }
            $normalized[] = $operation;
            $keys[] = $key;
            $seenSelectionKeys[$key] = true;
        }

        if ($mode === 'select' && $normalized === []) {
            throw new LogicException('WorkflowContext::select() requires at least one durable operation.');
        }

        $this->operations = $normalized;
        $this->keys = $keys;
    }

    public function leafCount(): int
    {
        $count = 0;
        foreach ($this->operations as $operation) {
            $count += $operation instanceof self ? $operation->leafCount() : 1;
        }

        return $count;
    }

    /**
     * @return list<array{
     *     operation: DeferredWorkflowOperation,
     *     offset: int,
     *     result_path: list<int>,
     *     group_path: list<array{
     *         parallel_group_id: string,
     *         parallel_group_kind: string,
     *         parallel_group_base_sequence: int,
     *         parallel_group_size: int,
     *         parallel_group_index: int
     *     }>
     * }>
     */
    public function leafDescriptors(int $baseSequence): array
    {
        return $this->descriptors($this->operations, $baseSequence);
    }

    /** @param list<mixed> $flatResults
     *  @return list<mixed>
     */
    public function nestedResults(array $flatResults): array
    {
        $offset = 0;

        return $this->consumeResults($flatResults, $offset);
    }

    /** @param list<mixed> $flatResults
     *  @return list<mixed>
     */
    private function consumeResults(array $flatResults, int &$offset): array
    {
        $results = [];
        foreach ($this->operations as $operation) {
            if ($operation instanceof self) {
                $results[] = $operation->consumeResults($flatResults, $offset);
                continue;
            }

            $results[] = $flatResults[$offset] ?? null;
            ++$offset;
        }

        return $results;
    }

    /**
     * @param list<DeferredWorkflowOperation|self> $operations
     * @return list<array{
     *     operation: DeferredWorkflowOperation,
     *     offset: int,
     *     result_path: list<int>,
     *     group_path: list<array{
     *         parallel_group_id: string,
     *         parallel_group_kind: string,
     *         parallel_group_base_sequence: int,
     *         parallel_group_size: int,
     *         parallel_group_index: int
     *     }>
     * }>
     */
    private function descriptors(array $operations, int $baseSequence): array
    {
        $descriptors = [];
        $cursor = 0;
        $size = self::countLeaves($operations);
        $kind = self::kind($operations) ?? 'activity';

        foreach ($operations as $index => $operation) {
            $memberSize = $operation instanceof self ? $operation->leafCount() : 1;
            if ($operation instanceof DeferredWorkflowOperation) {
                $descriptors[] = [
                    'operation' => $operation,
                    'offset' => $cursor,
                    'result_path' => [$index],
                    'group_path' => [$this->groupEntry(
                        $baseSequence,
                        $size,
                        $cursor,
                        $kind,
                        $this->keys[$index] ?? $index,
                        $index,
                        $baseSequence + $cursor,
                        $memberSize,
                        self::operationKind($operation),
                    )],
                ];
                ++$cursor;
                continue;
            }

            foreach ($operation->descriptors($operation->operations, $baseSequence + $cursor) as $descriptor) {
                $descriptors[] = [
                    'operation' => $descriptor['operation'],
                    'offset' => $cursor + $descriptor['offset'],
                    'result_path' => array_merge([$index], $descriptor['result_path']),
                    'group_path' => array_merge([
                        $this->groupEntry(
                            $baseSequence,
                            $size,
                            $cursor + $descriptor['offset'],
                            $kind,
                            $this->keys[$index] ?? $index,
                            $index,
                            $baseSequence + $cursor,
                            $memberSize,
                            'group',
                        ),
                    ], $descriptor['group_path']),
                ];
            }
            $cursor += $operation->leafCount();
        }

        return $descriptors;
    }

    /** @param list<DeferredWorkflowOperation|self> $operations */
    private static function countLeaves(array $operations): int
    {
        $count = 0;
        foreach ($operations as $operation) {
            $count += $operation instanceof self ? self::countLeaves($operation->operations) : 1;
        }

        return $count;
    }

    /** @param list<DeferredWorkflowOperation|self> $operations */
    private static function kind(array $operations): ?string
    {
        $kind = null;
        foreach ($operations as $operation) {
            $operationKind = $operation instanceof self
                ? self::kind($operation->operations)
                : match ($operation->command->historyShape) {
                    'activity' => 'activity',
                    'child_workflow' => 'child',
                    'timer' => 'timer',
                    'condition_wait' => 'condition',
                    default => throw new LogicException(sprintf(
                        'WorkflowContext::all() does not support deferred %s operations.',
                        $operation->command->historyShape,
                    )),
                };

            if ($operationKind === null) {
                continue;
            }

            if ($kind === null) {
                $kind = $operationKind;
            } elseif ($kind !== $operationKind) {
                return 'mixed';
            }
        }

        return $kind;
    }

    /**
     * @return array{
     *     parallel_group_id: string,
     *     parallel_group_kind: string,
     *     parallel_group_base_sequence: int,
     *     parallel_group_size: int,
     *     parallel_group_index: int
     * }
     */
    private function groupEntry(
        int $baseSequence,
        int $size,
        int $index,
        string $kind,
        int|string $memberKey,
        int $memberIndex,
        int $memberBaseSequence,
        int $memberSize,
        string $memberKind,
    ): array
    {
        $prefix = $this->mode === 'select' ? 'select-calls' : match ($kind) {
            'activity' => 'parallel-activities',
            'child' => 'parallel-children',
            'timer' => 'parallel-timers',
            default => 'parallel-calls',
        };

        return array_filter([
            'parallel_group_id' => sprintf('%s:%d:%d', $prefix, $baseSequence, $size),
            'parallel_group_kind' => $kind,
            'parallel_group_mode' => $this->mode === 'select' ? 'select' : null,
            'parallel_group_base_sequence' => $baseSequence,
            'parallel_group_size' => $size,
            'parallel_group_index' => $index,
            'selection_member_key' => $this->mode === 'select' ? $memberKey : null,
            'selection_member_index' => $this->mode === 'select' ? $memberIndex : null,
            'selection_member_base_sequence' => $this->mode === 'select' ? $memberBaseSequence : null,
            'selection_member_size' => $this->mode === 'select' ? $memberSize : null,
            'selection_member_kind' => $this->mode === 'select' ? $memberKind : null,
        ], static fn (mixed $value): bool => $value !== null);
    }

    /**
     * @param array<string, mixed> $winner
     * @param array<int, mixed> $flatResults
     * @param array<int, \Throwable> $flatFailures
     * @param array<int, string> $operationIdentities
     */
    public function selectionResult(
        int $baseSequence,
        array $winner,
        array $flatResults,
        array $flatFailures,
        array $operationIdentities = [],
    ): SelectionResult
    {
        $winnerIndex = is_int($winner['member_index'] ?? null) ? $winner['member_index'] : 0;
        $handles = [];
        $cursor = 0;
        foreach ($this->operations as $index => $operation) {
            $size = $operation instanceof self ? $operation->leafCount() : 1;
            $key = $this->keys[$index] ?? $index;
            $kind = self::operationKind($operation);
            $identity = $index === $winnerIndex && is_string($winner['operation_identity'] ?? null)
                ? $winner['operation_identity']
                : ($kind === 'group'
                    ? sprintf('group:%d:%d', $baseSequence + $cursor, $size)
                    : ($operationIdentities[$baseSequence + $cursor] ?? null));
            if (!is_string($identity) || $identity === '') {
                throw new LogicException(sprintf(
                    'Selection member [%s] is missing its durable scheduled or open operation identity.',
                    (string) $key,
                ));
            }
            $handles[$key] = new DurableOperationHandle(
                $key,
                $index,
                $kind,
                $identity,
                $baseSequence + $cursor,
                $size,
                sprintf('select-calls:%d:%d', $baseSequence, $this->leafCount()),
                $operation,
            );
            $cursor += $size;
        }

        $winnerKey = $this->keys[$winnerIndex] ?? $winnerIndex;
        $winnerHandle = $handles[$winnerKey];
        $winnerValue = $this->memberValue($winnerIndex, $flatResults);
        $winnerFailure = $this->memberFailure($baseSequence, $winnerIndex, $winner, $flatFailures);

        return new SelectionResult(
            $winnerKey,
            $winnerIndex,
            $winnerHandle->kind,
            $winnerHandle->identity,
            $winnerValue,
            $winnerFailure,
            $winnerHandle,
            $handles,
        );
    }

    /** @param array<int, mixed> $flatResults */
    private function memberValue(int $memberIndex, array $flatResults): mixed
    {
        $cursor = 0;
        foreach ($this->operations as $index => $operation) {
            $size = $operation instanceof self ? $operation->leafCount() : 1;
            if ($index === $memberIndex) {
                if (!$operation instanceof self) {
                    return $flatResults[$cursor] ?? null;
                }

                return $operation->nestedResults(array_slice($flatResults, $cursor, $size));
            }
            $cursor += $size;
        }

        return null;
    }

    /** @param array<string, mixed> $winner
     *  @param array<int, \Throwable> $flatFailures
     */
    private function memberFailure(
        int $baseSequence,
        int $memberIndex,
        array $winner,
        array $flatFailures,
    ): ?\Throwable
    {
        $resolutionSequence = is_int($winner['_resolution_sequence'] ?? null)
            ? $winner['_resolution_sequence']
            : null;
        if (($winner['outcome'] ?? null) === 'failed' && $resolutionSequence !== null) {
            $failure = $flatFailures[$resolutionSequence - $baseSequence] ?? null;

            return $failure instanceof \Throwable ? $failure : null;
        }

        $cursor = 0;
        foreach ($this->operations as $index => $operation) {
            $size = $operation instanceof self ? $operation->leafCount() : 1;
            if ($index === $memberIndex) {
                for ($offset = 0; $offset < $size; ++$offset) {
                    if (($flatFailures[$cursor + $offset] ?? null) instanceof \Throwable) {
                        return $flatFailures[$cursor + $offset];
                    }
                }

                return null;
            }
            $cursor += $size;
        }

        return null;
    }

    private static function operationKind(DeferredWorkflowOperation|self $operation): string
    {
        if ($operation instanceof self) {
            return 'group';
        }

        return match ($operation->command->historyShape) {
            'activity' => 'activity',
            'child_workflow' => 'child',
            'timer' => 'timer',
            'condition_wait' => 'condition',
            default => $operation->command->historyShape,
        };
    }
}
