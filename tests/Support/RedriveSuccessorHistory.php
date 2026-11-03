<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests\Support;

use DateTimeImmutable;

/**
 * Checks that a replay fixture is a history Workflow 2.19 redrive can write.
 *
 * Core's DefaultWorkflowControlPlane::redrive() records, in one transaction,
 * a WorkflowStarted carrying the successor identity and the redrive envelope,
 * then the reused prefix from FailedRunRedrivePlan::successorEvents(). Every
 * reused event carries reused_from_run_id and reused_recorded_at, except the
 * TimerScheduled that successorEvents() copies, without them and under a new
 * timer_id, directly before a reused TimerFired of the same timer. Rows the
 * successor records itself come after that prefix, at or after its start, and
 * belong to the successor run.
 */
final class RedriveSuccessorHistory
{
    private const IDENTITY = [
        'workflow_type', 'workflow_instance_id', 'workflow_run_id', 'workflow_command_id', 'workflow_link_id', 'continued_from_run_id',
    ];

    private const REUSE_KEYS = ['reused_from_run_id', 'reused_recorded_at'];

    /** Reused events that do not occupy a workflow step of their own. */
    private const STEPLESS = ['SignalReceived'];

    /**
     * Lifecycle events of one durable step, and the payload field that names
     * that step. Only events of the same lifecycle may repeat a sequence.
     * Memo, search-attribute, side-effect and version-marker events are one
     * event per step.
     */
    private const LIFECYCLES = [
        'SignalWaitOpened' => ['signal_wait', 'signal_wait_id'],
        'SignalApplied' => ['signal_wait', 'signal_wait_id'],
        'ActivityScheduled' => ['activity', 'activity_execution_id'],
        'ActivityStarted' => ['activity', 'activity_execution_id'],
        'ActivityCompleted' => ['activity', 'activity_execution_id'],
        'TimerScheduled' => ['timer', 'timer_id'],
        'TimerFired' => ['timer', 'timer_id'],
        'ConditionWaitOpened' => ['condition_wait', 'condition_wait_id'],
        'ConditionWaitSatisfied' => ['condition_wait', 'condition_wait_id'],
    ];

    /**
     * @param array<string, mixed> $fixture a replay-regression fixture with history, workflow and workflow_tasks
     * @param list<string> $reusedEventTypes the reused prefix, in order, exactly as redrive copies it
     * @return string|null the first violated rule, or null for a valid successor
     */
    public static function violation(array $fixture, array $reusedEventTypes): ?string
    {
        $history = is_array($fixture['history'] ?? null) ? $fixture['history'] : [];
        $task = $fixture['workflow_tasks'][0] ?? null;
        $workflowType = $fixture['workflow']['type'] ?? null;
        if ($reusedEventTypes === []) {
            return 'a redrive successor needs at least one expected reused event type';
        }
        $started = $history[0] ?? null;
        if (!is_array($started) || ($started['event_type'] ?? null) !== 'WorkflowStarted' || ($started['sequence'] ?? null) !== 1) {
            return 'history must open with WorkflowStarted at sequence 1';
        }
        $start = is_array($started['payload'] ?? null) ? $started['payload'] : [];
        foreach (self::IDENTITY as $field) {
            if (!self::nonBlank($start[$field] ?? null)) {
                return "WorkflowStarted {$field} must be a non-blank string";
            }
        }
        if ($start['workflow_type'] !== $workflowType) {
            return 'WorkflowStarted workflow_type must be the fixture workflow type';
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
        // Core writes both deadlines into the envelope, null when unbounded.
        foreach (['execution_deadline_at' => $replayedStartedAt, 'run_deadline_at' => $startedAt] as $field => $after) {
            if (!array_key_exists($field, $start)) {
                return "WorkflowStarted must carry {$field}";
            }
            if ($start[$field] !== null) {
                $deadline = self::time($start[$field]);
                if ($deadline === null || $deadline <= $after) {
                    return "{$field} must be null or a valid time after the ".($field === 'run_deadline_at' ? 'successor' : 'workflow').' start';
                }
            }
        }
        if (!is_array($task) || ($task['workflow_id'] ?? null) !== $instance || ($task['run_id'] ?? null) !== $run) {
            return 'workflow task must belong to the successor instance and run';
        }

        $lastReusedStep = 0;
        $reusedSteps = [];
        $stepOwner = null;
        $previousAt = $startedAt;
        $prefixLength = count($reusedEventTypes);
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
            if ($offset < $prefixLength) {
                if ($type !== $reusedEventTypes[$offset]) {
                    return "reused prefix position {$offset} must be {$reusedEventTypes[$offset]}, not {$type}";
                }
                if ($type === 'TimerScheduled') {
                    // Core copies a fired timer's schedule under a new timer_id
                    // and without reuse keys; the reused TimerFired that must
                    // follow it carries them.
                    $fired = $history[$offset + 2] ?? null;
                    $firedPayload = is_array($fired['payload'] ?? null) ? $fired['payload'] : [];
                    if (($reusedEventTypes[$offset + 1] ?? null) !== 'TimerFired'
                        || ($fired['event_type'] ?? null) !== 'TimerFired'
                        || !self::nonBlank($payload['timer_id'] ?? null)
                        || ($firedPayload['timer_id'] ?? null) !== $payload['timer_id']
                        || ($firedPayload['sequence'] ?? null) !== ($payload['sequence'] ?? null)) {
                        return 'copied TimerScheduled must be followed by the reused TimerFired of the same timer_id and step';
                    }
                    foreach (self::REUSE_KEYS as $key) {
                        if (array_key_exists($key, $payload)) {
                            return "copied TimerScheduled must not carry {$key}";
                        }
                    }
                } else {
                    foreach (self::REUSE_KEYS as $key) {
                        if (!self::nonBlank($payload[$key] ?? null)) {
                            return "reused {$type} must carry a non-blank {$key}";
                        }
                    }
                    if ($payload['reused_from_run_id'] !== $source) {
                        return "reused {$type} reused_from_run_id must match continued_from_run_id";
                    }
                }
                if ($recordedAt != $startedAt) {
                    return "reused {$type} is recorded with the successor start";
                }
                if ($type !== 'TimerScheduled') {
                    $sourceAt = self::time($payload['reused_recorded_at']);
                    if ($sourceAt === null || $sourceAt < $replayedStartedAt || $sourceAt >= $startedAt) {
                        return "reused {$type} reused_recorded_at must fall in the source run";
                    }
                }
                if (array_key_exists('workflow_run_id', $payload) && $payload['workflow_run_id'] !== $source) {
                    return "reused {$type} keeps the source run id";
                }
                if (array_key_exists('workflow_instance_id', $payload) && $payload['workflow_instance_id'] !== $instance) {
                    return "reused {$type} must belong to the successor instance";
                }
                // SignalReceived has no workflow step of its own; it belongs to its wait.
                if (!in_array($type, self::STEPLESS, true)) {
                    $step = $payload['sequence'] ?? null;
                    if (!is_int($step) || $step < 1) {
                        return "reused {$type} must name a positive workflow step";
                    }
                    if ($step < $lastReusedStep) {
                        return "reused workflow steps must not descend: {$type} step {$step} follows step {$lastReusedStep}";
                    }
                    [$lifecycle, $identityField] = self::LIFECYCLES[$type] ?? [$type, null];
                    $owner = [$lifecycle, $identityField === null ? null : ($payload[$identityField] ?? null)];
                    if ($step === $lastReusedStep
                        && ($identityField === null || $owner[1] === null || $owner !== $stepOwner)) {
                        return "reused workflow step {$step} is repeated by {$type}, which is not part of the same durable step";
                    }
                    $stepOwner = $owner;
                    $lastReusedStep = $step;
                    $reusedSteps[$step] = true;
                }
                continue;
            }
            foreach (self::REUSE_KEYS as $key) {
                if (array_key_exists($key, $payload)) {
                    return "successor {$type} must not carry {$key} outside the reused prefix";
                }
            }
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
        if (count($history) - 1 < $prefixLength) {
            return 'history ends before the expected reused prefix';
        }
        if ($lastReusedStep === 0) {
            return 'history has no reused workflow step';
        }
        if (array_keys($reusedSteps) !== range(1, $lastReusedStep)) {
            return 'reused workflow steps must be exactly 1..'.$lastReusedStep.' with no gap';
        }
        if (($start['resume_step_sequence'] ?? null) !== $lastReusedStep + 1) {
            return 'resume_step_sequence must be the last reused workflow step + 1 ('.($lastReusedStep + 1).')';
        }

        return null;
    }

    private static function nonBlank(mixed $value): bool
    {
        return is_string($value) && trim($value) !== '';
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
