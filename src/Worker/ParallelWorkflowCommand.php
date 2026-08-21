<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use LogicException;

/** @internal A deterministic tree of operations suspended by one all/parallel barrier. */
final class ParallelWorkflowCommand
{
    /** @var list<DeferredWorkflowOperation|self> */
    public readonly array $operations;

    /** @param iterable<int, mixed> $operations */
    public function __construct(iterable $operations)
    {
        $normalized = [];
        foreach ($operations as $operation) {
            if (!$operation instanceof DeferredWorkflowOperation && !$operation instanceof self) {
                throw new LogicException(sprintf(
                    'WorkflowContext::all() accepts deferred workflow operations or nested barriers; received %s.',
                    get_debug_type($operation),
                ));
            }
            $normalized[] = $operation;
        }

        $this->operations = $normalized;
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
        return self::descriptors($this->operations, $baseSequence);
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
    private static function descriptors(array $operations, int $baseSequence): array
    {
        $descriptors = [];
        $cursor = 0;
        $size = self::countLeaves($operations);
        $kind = self::kind($operations) ?? 'activity';

        foreach ($operations as $index => $operation) {
            if ($operation instanceof DeferredWorkflowOperation) {
                $descriptors[] = [
                    'operation' => $operation,
                    'offset' => $cursor,
                    'result_path' => [$index],
                    'group_path' => [self::groupEntry($baseSequence, $size, $cursor, $kind)],
                ];
                ++$cursor;
                continue;
            }

            foreach (self::descriptors($operation->operations, $baseSequence + $cursor) as $descriptor) {
                $descriptors[] = [
                    'operation' => $descriptor['operation'],
                    'offset' => $cursor + $descriptor['offset'],
                    'result_path' => array_merge([$index], $descriptor['result_path']),
                    'group_path' => array_merge([
                        self::groupEntry($baseSequence, $size, $cursor + $descriptor['offset'], $kind),
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
    private static function groupEntry(int $baseSequence, int $size, int $index, string $kind): array
    {
        $prefix = match ($kind) {
            'activity' => 'parallel-activities',
            'child' => 'parallel-children',
            'timer' => 'parallel-timers',
            default => 'parallel-calls',
        };

        return [
            'parallel_group_id' => sprintf('%s:%d:%d', $prefix, $baseSequence, $size),
            'parallel_group_kind' => $kind,
            'parallel_group_base_sequence' => $baseSequence,
            'parallel_group_size' => $size,
            'parallel_group_index' => $index,
        ];
    }
}
