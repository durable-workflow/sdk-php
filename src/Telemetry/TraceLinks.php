<?php

declare(strict_types=1);

namespace DurableWorkflow\Telemetry;

use Throwable;

/** Bounded secondary observations from an already authorized original claim. */
final class TraceLinks
{
    /** @return list<array<string, mixed>> */
    public static function normalize(mixed $value, string $taskId, string $runId, string $namespace, ?string &$diagnostic = null): array
    {
        $diagnostic = $value === null ? null : 'discarded';
        if (!TraceCarrier::identity($taskId) || !TraceCarrier::identity($runId) || !TraceCarrier::identity($namespace)
            || !is_array($value) || count($value) !== 6
            || ($value['schema'] ?? null) !== 'durable-workflow.trace-links' || ($value['version'] ?? null) !== 1
            || ($value['task_id'] ?? null) !== $taskId || ($value['run_id'] ?? null) !== $runId
            || ($value['namespace'] ?? null) !== $namespace
            || array_diff(array_keys($value), ['schema', 'version', 'task_id', 'run_id', 'namespace', 'links']) !== []
            || !is_array($value['links'] ?? null) || !array_is_list($value['links']) || count($value['links']) > 32) {
            return [];
        }
        $result = [];
        $filtered = false;
        foreach ($value['links'] as $entry) {
            $entry = self::entry($entry, $contextDiagnostic);
            if ($entry === null) {
                return [];
            }
            $result[] = $entry;
            $filtered = $filtered || $contextDiagnostic !== null;
        }
        try {
            if (strlen(json_encode($value, JSON_THROW_ON_ERROR)) > 4096) {
                return [];
            }
            $diagnostic = $filtered ? 'filtered' : null;

            return $result;
        } catch (Throwable) {
            return [];
        }
    }

    /** @return array<string, mixed>|null */
    private static function entry(mixed $value, ?string &$diagnostic): ?array
    {
        $diagnostic = 'discarded';
        if (!is_array($value) || count($value) > 10) {
            return null;
        }
        $shape = match ($value['kind'] ?? null) {
            'command' => [['command_id'], []],
            'task_outcome' => [['task_id', 'event_id', 'sequence', 'event_type'],
                ['activity_execution_id', 'activity_attempt_id', 'timer_id']],
            'child_outcome' => [['workflow_link_id', 'child_run_id', 'event_id', 'sequence', 'event_type'], ['child_task_id']],
            default => null,
        };
        if ($shape === null) {
            return null;
        }
        [$required, $optional] = $shape;
        if (array_diff($required, array_keys($value)) !== []
            || array_diff(array_keys($value), [...$required, ...$optional, 'kind', 'context']) !== []) {
            return null;
        }
        foreach ([...$required, ...array_intersect($optional, array_keys($value))] as $key) {
            if ($key === 'sequence') {
                if (!is_int($value[$key]) || $value[$key] < 1) {
                    return null;
                }
            } elseif ($key !== 'child_task_id' || $value[$key] !== null) {
                if (!TraceCarrier::identity($value[$key])) {
                    return null;
                }
            }
        }
        if (!is_array($value['context'] ?? null) || !is_array($value['context']['carrier'] ?? null)
            || count($value['context']['carrier']) > 2
            || array_diff(array_keys($value['context']['carrier']), ['traceparent', 'tracestate']) !== []) {
            return null;
        }
        $context = TraceCarrier::normalize($value['context'], diagnostic: $diagnostic);

        return $context === null ? null : array_replace($value, ['context' => $context]);
    }
}
