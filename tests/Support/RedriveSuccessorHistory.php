<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests\Support;

use DateTimeImmutable;

/**
 * Checks that a fixture history is one Workflow 2.19 redrive can write.
 *
 * Core's DefaultWorkflowControlPlane::redrive() records, in one transaction,
 * a WorkflowStarted carrying the successor identity and the redrive envelope,
 * then the reused prefix from FailedRunRedrivePlan::successorEvents(). Rows
 * the successor records itself come after that prefix, at or after its start,
 * and belong to the successor run.
 */
final class RedriveSuccessorHistory
{
    private const IDENTITY = ['workflow_instance_id', 'workflow_run_id', 'workflow_command_id', 'workflow_link_id', 'continued_from_run_id'];

    /**
     * @param list<array<string, mixed>> $history
     * @param array{workflow_id: string, run_id: string}|null $task
     * @return string|null the first violated rule, or null for a valid successor
     */
    public static function violation(array $history, ?array $task = null): ?string
    {
        $started = $history[0] ?? null;
        if (!is_array($started) || ($started['event_type'] ?? null) !== 'WorkflowStarted' || ($started['sequence'] ?? null) !== 1) {
            return 'history must open with WorkflowStarted at sequence 1';
        }
        $start = is_array($started['payload'] ?? null) ? $started['payload'] : [];
        foreach (self::IDENTITY as $field) {
            if (!is_string($start[$field] ?? null) || $start[$field] === '') {
                return "WorkflowStarted {$field} must be a non-empty string";
            }
        }
        $instance = $start['workflow_instance_id'];
        $run = $start['workflow_run_id'];
        $source = $start['continued_from_run_id'];
        if ($source === $run) {
            return 'continued_from_run_id must name a different run than workflow_run_id';
        }
        if (($start['recovery_kind'] ?? null) !== 'redrive') {
            return 'recovery_kind must be redrive';
        }
        $startedAt = self::time($started['recorded_at'] ?? null);
        $replayedStartedAt = self::time($start['replayed_started_at'] ?? null);
        if ($startedAt === null || $replayedStartedAt === null || $replayedStartedAt >= $startedAt) {
            return 'replayed_started_at must be a valid time before the successor start';
        }
        if ($task !== null && ($task['workflow_id'] !== $instance || $task['run_id'] !== $run)) {
            return 'workflow task must belong to the successor instance and run';
        }

        $inPrefix = true;
        $lastReusedStep = 0;
        $previousAt = $startedAt;
        foreach (array_slice($history, 1) as $offset => $event) {
            $payload = is_array($event['payload'] ?? null) ? $event['payload'] : [];
            $type = (string) ($event['event_type'] ?? '');
            if (($event['sequence'] ?? null) !== $offset + 2) {
                return "history order: {$type} must be history sequence ".($offset + 2);
            }
            $recordedAt = self::time($event['recorded_at'] ?? $event['timestamp'] ?? null);
            if ($recordedAt === null || $recordedAt < $previousAt) {
                return "history order: {$type} must not predate the event before it";
            }
            $previousAt = $recordedAt;
            if (array_key_exists('reused_from_run_id', $payload)) {
                if (!$inPrefix) {
                    return "reused {$type} must belong to the prefix directly after WorkflowStarted";
                }
                if ($payload['reused_from_run_id'] !== $source) {
                    return "reused {$type} reused_from_run_id must match continued_from_run_id";
                }
                if ($recordedAt != $startedAt) {
                    return "reused {$type} is recorded with the successor start";
                }
                $sourceAt = self::time($payload['reused_recorded_at'] ?? null);
                if ($sourceAt === null || $sourceAt < $replayedStartedAt || $sourceAt >= $startedAt) {
                    return "reused {$type} reused_recorded_at must fall in the source run";
                }
                if (array_key_exists('workflow_run_id', $payload) && $payload['workflow_run_id'] !== $source) {
                    return "reused {$type} keeps the source run id";
                }
                if (array_key_exists('workflow_instance_id', $payload) && $payload['workflow_instance_id'] !== $instance) {
                    return "reused {$type} must belong to the successor instance";
                }
                // SignalReceived has no workflow step of its own; it belongs to its wait.
                if (is_int($payload['sequence'] ?? null)) {
                    $lastReusedStep = max($lastReusedStep, $payload['sequence']);
                }
                continue;
            }
            $inPrefix = false;
            if (array_key_exists('workflow_run_id', $payload) && $payload['workflow_run_id'] !== $run) {
                return "successor {$type} must bind to the successor run";
            }
            if (array_key_exists('workflow_instance_id', $payload) && $payload['workflow_instance_id'] !== $instance) {
                return "successor {$type} must bind to the successor instance";
            }
            $cancellation = is_array($payload['cancellation'] ?? null) ? $payload['cancellation'] : null;
            if ($cancellation !== null) {
                if (($cancellation['root_workflow_instance_id'] ?? null) !== $instance || ($cancellation['root_workflow_run_id'] ?? null) !== $run) {
                    return "successor {$type} cancellation must root at the successor";
                }
                foreach ($cancellation['lineage'] ?? [] as $link) {
                    if (($link['workflow_instance_id'] ?? null) !== $instance || ($link['workflow_run_id'] ?? null) !== $run) {
                        return "successor {$type} cancellation lineage must name the successor";
                    }
                }
            }
            if (is_int($payload['sequence'] ?? null) && $payload['sequence'] <= $lastReusedStep) {
                return "successor {$type} must not reuse a reused step sequence";
            }
        }
        if ($lastReusedStep === 0) {
            return 'history has no reused workflow step';
        }
        if (($start['resume_step_sequence'] ?? null) !== $lastReusedStep + 1) {
            return 'resume_step_sequence must be the last reused workflow step + 1 ('.($lastReusedStep + 1).')';
        }

        return null;
    }

    private static function time(mixed $value): ?DateTimeImmutable
    {
        if (!is_string($value)
            || preg_match('/\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})\z/', $value) !== 1) {
            return null;
        }

        return new DateTimeImmutable($value);
    }
}
