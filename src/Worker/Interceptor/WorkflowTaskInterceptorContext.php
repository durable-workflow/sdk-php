<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker\Interceptor;

/** Immutable context passed to workflow-task interceptor hooks. */
final class WorkflowTaskInterceptorContext
{
    /**
     * Event types that a workflow task records for a workflow command. Their
     * presence in the delivered history means the workflow code replays a
     * committed prefix before it reaches new execution.
     */
    private const COMMAND_EVENT_PREFIXES = [
        'Activity', 'Timer', 'ChildWorkflow', 'ChildRun', 'ServiceCall', 'ServiceOperation',
        'ExternalSignal', 'ConditionWait', 'SignalWait', 'CancellationScope',
    ];
    private const COMMAND_EVENTS = [
        'SideEffectRecorded', 'VersionMarkerRecorded', 'MemoUpserted', 'SearchAttributesUpserted',
        'SignalApplied', 'UpdateApplied', 'UpdateCompleted',
    ];

    /**
     * @param array<string, mixed> $task the workflow task as delivered by the Server
     * @param bool $replaying true when the history already contains commands recorded
     *                        by earlier workflow tasks, so the workflow code re-runs that
     *                        committed prefix before any new execution
     */
    public function __construct(
        public readonly string $workerId,
        public readonly string $taskQueue,
        public readonly ?string $namespace,
        public readonly array $task,
        public readonly string $taskId,
        public readonly string $workflowType,
        public readonly ?string $workflowId,
        public readonly ?string $runId,
        public readonly int $workflowTaskAttempt,
        public readonly int $historyLength,
        public readonly bool $replaying,
        public readonly ?string $updateId = null,
    ) {
    }

    /**
     * @internal built by the Worker
     *
     * @param array<string, mixed> $task
     * @param list<array<string, mixed>> $history
     */
    public static function forTask(string $workerId, string $taskQueue, ?string $namespace, array $task, array $history): self
    {
        $updateId = isset($task['workflow_update_id']) ? (string) $task['workflow_update_id'] : null;

        return new self(
            $workerId,
            $taskQueue,
            $namespace,
            $task,
            (string) ($task['task_id'] ?? ''),
            (string) ($task['workflow_type'] ?? ''),
            isset($task['workflow_id']) ? (string) $task['workflow_id'] : null,
            isset($task['run_id']) ? (string) $task['run_id'] : null,
            (int) ($task['workflow_task_attempt'] ?? 1),
            count($history),
            self::replaysCommittedPrefix($history),
            $updateId === '' ? null : $updateId,
        );
    }

    /** @param list<array<string, mixed>> $history */
    public static function replaysCommittedPrefix(array $history): bool
    {
        foreach ($history as $event) {
            $type = (string) ($event['event_type'] ?? $event['type'] ?? '');
            if (in_array($type, self::COMMAND_EVENTS, true)) {
                return true;
            }
            foreach (self::COMMAND_EVENT_PREFIXES as $prefix) {
                if (str_starts_with($type, $prefix)) {
                    return true;
                }
            }
        }

        return false;
    }
}
