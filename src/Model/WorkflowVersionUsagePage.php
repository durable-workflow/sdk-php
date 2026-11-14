<?php

declare(strict_types=1);

namespace DurableWorkflow\Model;

use UnexpectedValueException;

/** One live v1 observation; never a durable snapshot or permission to remove branches. */
final class WorkflowVersionUsagePage
{
    public const SCHEMA_VERSION = 1;
    public const CONSISTENCY = 'live';

    /**
     * @param list<WorkflowVersionUsageRun> $runs
     * @param array<string, mixed> $raw
     */
    private function __construct(
        public readonly ?string $namespace,
        public readonly string $workflowType,
        public readonly string $changeId,
        public readonly int $minVersion,
        public readonly ?string $state,
        public readonly ?string $throughRunId,
        public readonly WorkflowVersionUsageCounts $counts,
        public readonly array $runs,
        public readonly int $limit,
        public readonly int $returnedCount,
        public readonly bool $hasMore,
        public readonly ?string $nextCursor,
        public readonly array $raw,
    ) {
    }

    /**
     * @param array<string, mixed> $value
     * @param string|null $requestCursor The opaque cursor supplied for this exact page request.
     */
    public static function fromArray(array $value, ?string $requestCursor = null): self
    {
        self::requireShape($value);
        self::requireTextFields($value);
        $counts = WorkflowVersionUsageCounts::fromArray($value['counts']);
        if ($value['returned_count'] !== count($value['runs']) || $value['returned_count'] > $value['limit']
            || $value['returned_count'] > $counts->total
            || ($value['has_more'] !== ($value['next_cursor'] !== null))
            || ($value['has_more'] && $value['returned_count'] === 0)
            || ($value['through_run_id'] === null && $counts->total !== 0)) {
            throw new UnexpectedValueException('Workflow version-usage page boundaries do not reconcile.');
        }
        $remaining = ['legacy' => $counts->legacy, 'current' => $counts->current, 'unknown' => $counts->unknown];
        $selectedCount = $value['state'] === null ? $counts->total : $remaining[$value['state']];
        if ($value['has_more'] && $selectedCount <= $value['returned_count']) {
            throw new UnexpectedValueException('Workflow version-usage continuation has no remaining matching runs.');
        }
        self::requireCursorProgress($value, $selectedCount, $requestCursor);

        return new self($value['namespace'], $value['workflow_type'], $value['change_id'], $value['min_version'],
            $value['state'], $value['through_run_id'], $counts, self::readRuns($value, $remaining), $value['limit'], $value['returned_count'],
            $value['has_more'], $value['next_cursor'], $value);
    }

    /** @param array<string, mixed> $value */
    private static function requireCursorProgress(array $value, int $selectedCount, ?string $requestCursor): void
    {
        if ($requestCursor === null && !$value['has_more'] && $value['returned_count'] !== $selectedCount) {
            throw new UnexpectedValueException('The first terminal version-usage page must contain the complete selected cohort.');
        }
        if ($value['has_more'] && $value['returned_count'] !== $value['limit']) {
            throw new UnexpectedValueException('A version-usage continuation page must fill its requested limit.');
        }
        if ($requestCursor !== null && $value['next_cursor'] === $requestCursor) {
            throw new UnexpectedValueException('A version-usage continuation cursor must advance.');
        }
    }

    /** @param array<string, mixed> $value */
    private static function requireShape(array $value): void
    {
        $keys = array_keys($value);
        sort($keys);
        if ($keys !== ['change_id', 'consistency', 'counts', 'has_more', 'limit', 'min_version', 'namespace',
            'next_cursor', 'returned_count', 'runs', 'schema_version', 'state', 'through_run_id', 'workflow_type']
            || $value['schema_version'] !== 1 || $value['consistency'] !== 'live'
            || !is_int($value['min_version']) || $value['min_version'] < -2147483648 || $value['min_version'] > 2147483647
            || !in_array($value['state'], [null, 'legacy', 'current', 'unknown'], true)
            || !is_int($value['limit']) || $value['limit'] < 1 || $value['limit'] > 200
            || !is_int($value['returned_count']) || $value['returned_count'] < 0
            || !is_bool($value['has_more']) || !is_array($value['counts'])
            || !is_array($value['runs']) || !array_is_list($value['runs'])) {
            throw new UnexpectedValueException('Invalid workflow version-usage v1 response.');
        }
    }

    /** @param array<string, mixed> $value */
    private static function requireTextFields(array $value): void
    {
        foreach (['namespace', 'workflow_type', 'change_id', 'through_run_id', 'next_cursor'] as $field) {
            if ($value[$field] === null && in_array($field, ['namespace', 'through_run_id', 'next_cursor'], true)) {
                continue;
            }
            if (!is_string($value[$field]) || $value[$field] === '' || preg_match('//u', $value[$field]) !== 1) {
                throw new UnexpectedValueException('Invalid workflow version-usage response identity or cursor.');
            }
        }
    }

    /**
     * @param array<string, mixed> $value
     * @param array<string, int> $remaining
     * @return list<WorkflowVersionUsageRun>
     */
    private static function readRuns(array $value, array $remaining): array
    {
        $runs = [];
        $previous = null;
        foreach ($value['runs'] as $row) {
            if (!is_array($row)) {
                throw new UnexpectedValueException('Invalid workflow version-usage run.');
            }
            $run = WorkflowVersionUsageRun::fromArray($row, $value['min_version']);
            if (--$remaining[$run->state] < 0) {
                throw new UnexpectedValueException('Workflow version-usage page exceeds its all-state counts.');
            }
            if (($previous !== null && strcmp($previous, $run->runId) >= 0)
                || $value['through_run_id'] === null || strcmp($run->runId, $value['through_run_id']) > 0
                || ($value['state'] !== null && $run->state !== $value['state'])) {
                throw new UnexpectedValueException('Workflow version-usage runs violate their order, upper bound or filter.');
            }
            $runs[] = $run;
            $previous = $run->runId;
        }

        return $runs;
    }
}
