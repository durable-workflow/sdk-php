<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Codec\AvroMapValue;
use DurableWorkflow\Codec\PayloadCodec;
use DurableWorkflow\Exception\ActivityFailed;
use DurableWorkflow\Exception\ChildWorkflowFailed;
use DurableWorkflow\Exception\DurableOperationCancelled;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Exception\WorkflowCancelled;
use Fiber;
use LogicException;
use Throwable;

/** Re-executes a straight-line workflow Fiber against committed, sequence-ordered history. */
final class Replayer
{
    private const MIN_VERSION = -2_147_483_648;

    private const MAX_VERSION = 2_147_483_647;

    public function __construct(private readonly PayloadCodec $codec)
    {
    }

    /**
     * @param callable(WorkflowContext, mixed ...$input): mixed $handler
     * @param list<array<string, mixed>> $history
     * @param list<mixed> $input
     * @param array<string, mixed> $task
     */
    public function replay(
        callable $handler,
        array $history,
        array $input,
        string $taskQueue,
        array $task = [],
    ): ReplayResult {
        $steps = $this->recordedSteps($history);
        $stepsBySequence = [];
        foreach ($steps as $step) {
            $stepsBySequence[$step['sequence']] = $step;
        }
        $selectionResolutions = $this->selectionResolutions($history);
        $selectionCancellations = $this->selectionCancellations($history);
        $selectionOperationIdentities = $this->selectionOperationIdentities($history);
        $completedHistory = $this->hasCompletedHistory($history);
        $context = null;
        $execution = new Fiber(function () use ($handler, $history, $input, $task, &$context): mixed {
            $current = Fiber::getCurrent();
            if ($current === null) {
                throw new LogicException('Workflow execution did not start inside its Fiber.');
            }
            $context = new WorkflowContext(
                (string) ($task['workflow_id'] ?? ''),
                (string) ($task['run_id'] ?? ''),
                $history,
                $this->codec,
                (bool) ($task['cancel_requested'] ?? false),
                $current,
                isset($task['workflow_command_id']) && (string) $task['workflow_command_id'] !== ''
                    ? (string) $task['workflow_command_id']
                    : (isset($task['task_id']) ? (string) $task['task_id'] : null),
            );

            return $handler($context, ...$input);
        });

        $stepCursor = 0;
        $commands = [];
        /** @var array<string, array{version: int, family: string, sequence: ?int}> $versionDecisions */
        $versionDecisions = [];
        $suspended = $execution->start();
        $nextSequence = 1;

        while (!$execution->isTerminated()) {
            if ($suspended instanceof ParallelWorkflowCommand) {
                $commandsBeforeGroup = count($commands);
                $baseSequence = isset($steps[$stepCursor])
                    ? $steps[$stepCursor]['sequence']
                    : $nextSequence;
                $descriptors = $suspended->leafDescriptors($baseSequence);
                $results = [];
                $pending = false;
                $matched = 0;
                $failure = null;
                $failures = [];
                $missingMember = false;

                foreach ($descriptors as $offset => $descriptor) {
                    $command = $descriptor['operation']->command;
                    $path = $descriptor['group_path'];
                    $step = $steps[$stepCursor + $offset] ?? null;
                    if ($step === null) {
                        $missingMember = true;
                        $metadata = $path[array_key_last($path)] ?? [];
                        $commands[] = $command->withAttributes([
                            ...$metadata,
                            'parallel_group_path' => $path,
                        ])->toWire($this->codec, $taskQueue);
                        continue;
                    }

                    ++$matched;
                    $this->assertCommandMatchesStep($command, $step);
                    $this->assertParallelPathMatches($step, $path);
                    if ($command->type === 'open_condition_wait') {
                        $this->assertConditionWaitCompatible($step, $command);
                    }
                    $nextSequence = max($nextSequence, $step['sequence'] + 1);

                    if (!$step['resolved']) {
                        if ($command->type === 'open_condition_wait') {
                            if ($command->conditionSatisfied()) {
                                $results[$offset] = true;
                                continue;
                            }

                            $metadata = $path[array_key_last($path)] ?? [];
                            $commands[] = $command->withAttributes([
                                ...$metadata,
                                'parallel_group_path' => $path,
                            ])->toWire($this->codec, $taskQueue);
                        }
                        $pending = true;
                        continue;
                    }
                    if ($step['failure'] instanceof Throwable) {
                        $failures[$offset] = $step['failure'];
                        if ($failure === null
                            || $step['resolution_order'] < $failure['resolution_order']
                            || ($step['resolution_order'] === $failure['resolution_order'] && $offset < $failure['offset'])) {
                            $failure = [
                                'exception' => $step['failure'],
                                'resolution_order' => $step['resolution_order'],
                                'offset' => $offset,
                            ];
                        }
                        continue;
                    }

                    $results[$offset] = $step['value'];
                }

                $stepCursor += $matched;
                $nextSequence = max($nextSequence, $baseSequence + count($descriptors));
                if ($missingMember && $failure !== null) {
                    throw new NonDeterministicWorkflow(
                        'Parallel history contains a failure before every declared member was durably scheduled.',
                        $baseSequence,
                        'all parallel members scheduled',
                        'failed group with missing members',
                        'parallel_group_partially_scheduled',
                    );
                }
                if ($suspended->mode === 'select') {
                    $groupId = $descriptors[0]['group_path'][0]['parallel_group_id'] ?? null;
                    $winner = is_string($groupId) ? ($selectionResolutions[$groupId] ?? null) : null;
                    if (is_array($winner)) {
                        $winner = $this->validatedSelectionResolution(
                            $suspended,
                            $baseSequence,
                            $winner,
                            $stepsBySequence,
                            $history,
                            $selectionOperationIdentities,
                        );
                        ksort($results);
                        ksort($failures);
                        $selection = $suspended->selectionResult(
                            $baseSequence,
                            $winner,
                            $results,
                            $failures,
                            $selectionOperationIdentities,
                        );
                        $this->validateSelectionCancellationsForHandles(
                            $selection->handles,
                            $selectionCancellations,
                        );
                        $suspended = $execution->resume($selection);
                        continue;
                    }
                    return $this->result($commands, $context);
                }
                if ($failure !== null) {
                    $suspended = $execution->throw($failure['exception']);
                    continue;
                }
                if ($pending || count($commands) > $commandsBeforeGroup) {
                    return $this->result($commands, $context);
                }

                ksort($results);
                $suspended = $execution->resume($suspended->nestedResults(array_values($results)));
                continue;
            }

            if ($suspended instanceof DurableOperationHandle) {
                $resolution = $this->durableHandleResolution(
                    $suspended,
                    $stepsBySequence,
                    $selectionCancellations,
                );
                if (!$resolution['resolved']) {
                    return $this->result($commands, $context);
                }
                $suspended = $resolution['failure'] instanceof Throwable
                    ? $execution->throw($resolution['failure'])
                    : $execution->resume($resolution['value']);
                continue;
            }

            if ($suspended instanceof CancelDurableOperationCommand) {
                if ($this->selectionCancellationForHandle($suspended->handle, $selectionCancellations) === null) {
                    $commands[] = $suspended->toWire();
                }
                $suspended = $execution->resume(null);
                continue;
            }

            if (!$suspended instanceof WorkflowCommand) {
                throw new NonDeterministicWorkflow('Workflow suspended with an unsupported value instead of WorkflowCommand.');
            }
            if ($suspended->type === 'continue_as_new') {
                $this->assertNoRemainingSteps($steps, $stepCursor, 'continue_as_new');
                $commands[] = $suspended->toWire($this->codec, $taskQueue);

                return $this->result($commands, $context);
            }

            if ($suspended->type === 'record_version_marker') {
                $changeId = (string) $suspended->attributes['change_id'];
                $decision = $versionDecisions[$changeId] ?? null;
                if ($decision !== null) {
                    $this->assertVersionDecisionCompatible($changeId, $decision, $suspended);
                    $suspended = $execution->resume($suspended->versionResult($decision['version']));
                    continue;
                }

                $step = $steps[$stepCursor] ?? null;
                if ($step !== null) {
                    if ($step['shape'] !== 'version_marker') {
                        $version = -1;
                        $this->assertVersionSupported(
                            $changeId,
                            $version,
                            (int) $suspended->attributes['min_supported'],
                            (int) $suspended->attributes['max_supported'],
                            $step['sequence'],
                        );
                        $versionDecisions[$changeId] = [
                            'version' => $version,
                            'family' => $this->versionFamily($suspended),
                            'sequence' => $step['sequence'],
                        ];
                        $suspended = $execution->resume($suspended->versionResult($version));
                        continue;
                    }
                    if ($step['detail'] !== $changeId) {
                        throw new NonDeterministicWorkflow(
                            "Recorded version marker {$step['detail']} does not match change ID {$changeId}.",
                            $step['sequence'],
                            $step['detail'],
                            $changeId,
                            'version_change_id_mismatch',
                        );
                    }
                    $version = (int) $step['value'];
                    $this->assertVersionSupported(
                        $changeId,
                        $version,
                        (int) $suspended->attributes['min_supported'],
                        (int) $suspended->attributes['max_supported'],
                        $step['sequence'],
                    );
                    ++$stepCursor;
                    $nextSequence = max($nextSequence, $step['sequence'] + 1);
                    $versionDecisions[$changeId] = [
                        'version' => $version,
                        'family' => $this->versionFamily($suspended),
                        'sequence' => $step['sequence'],
                    ];
                    $suspended = $execution->resume($suspended->versionResult($version));
                    continue;
                }

                if ($completedHistory) {
                    $version = -1;
                    $this->assertVersionSupported(
                        $changeId,
                        $version,
                        (int) $suspended->attributes['min_supported'],
                        (int) $suspended->attributes['max_supported'],
                        null,
                    );
                    $versionDecisions[$changeId] = [
                        'version' => $version,
                        'family' => $this->versionFamily($suspended),
                        'sequence' => null,
                    ];
                    $suspended = $execution->resume($suspended->versionResult($version));
                    continue;
                }

                $version = (int) $suspended->attributes['max_supported'];
                $commands[] = $suspended->toWire($this->codec, $taskQueue);
                ++$nextSequence;
                $versionDecisions[$changeId] = [
                    'version' => $version,
                    'family' => $this->versionFamily($suspended),
                    'sequence' => null,
                ];
                $suspended = $execution->resume($suspended->versionResult($version));
                continue;
            }

            $step = $steps[$stepCursor] ?? null;
            if ($step !== null) {
                $this->assertCommandMatchesStep($suspended, $step);
                if ($step['parallel_path'] !== []) {
                    throw new NonDeterministicWorkflow(
                        'Recorded parallel-group history was replaced by a sequential workflow operation.',
                        $step['sequence'],
                        'parallel barrier',
                        $suspended->historyShape,
                        'parallel_group_missing_from_workflow',
                    );
                }
                ++$stepCursor;
                $nextSequence = max($nextSequence, $step['sequence'] + 1);
                if ($suspended->type === 'open_condition_wait') {
                    $this->assertConditionWaitCompatible($step, $suspended);
                    if ($step['resolved']) {
                        $suspended = $execution->resume($step['value']);
                        continue;
                    }
                    if ($suspended->conditionSatisfied()) {
                        $suspended = $execution->resume(true);
                        continue;
                    }

                    $commands[] = $suspended->toWire($this->codec, $taskQueue);

                    return $this->result($commands, $context);
                }
                if ($step['resolved'] === false) {
                    return $this->result($commands, $context);
                }
                $suspended = $step['failure'] instanceof Throwable
                    ? $execution->throw($step['failure'])
                    : $execution->resume($step['value']);
                continue;
            }

            if ($suspended->type === 'record_side_effect') {
                $suspended = $suspended->resolveSideEffect();
            }
            if ($suspended->type === 'open_condition_wait') {
                if ($suspended->conditionSatisfied()) {
                    $suspended = $execution->resume(true);
                    continue;
                }
                if (($suspended->attributes['timeout_seconds'] ?? null) === 0) {
                    $suspended = $execution->resume(false);
                    continue;
                }
            }
            $commands[] = $suspended->toWire($this->codec, $taskQueue);
            ++$nextSequence;
            if (in_array($suspended->type, ['record_side_effect', 'upsert_memo', 'upsert_search_attributes'], true)) {
                $suspended = $execution->resume($suspended->localResult);
                continue;
            }

            return $this->result($commands, $context);
        }

        $this->assertNoRemainingSteps($steps, $stepCursor, 'complete_workflow');
        $result = $execution->getReturn();
        if ($result instanceof \Generator) {
            throw new LogicException(
                'Workflow handlers must call WorkflowContext operations directly; Generator results are not supported.',
            );
        }
        $commands[] = $this->completeCommand($result);

        return $this->result($commands, $context);
    }

    /** @param list<array<string, mixed>> $commands */
    private function result(array $commands, ?WorkflowContext $context): ReplayResult
    {
        return new ReplayResult(
            $commands,
            $context?->messageStreamCursorAcknowledgements() ?? [],
            $context?->messageStreamPendingWaits() ?? [],
        );
    }

    /**
     * @param list<array<string, mixed>> $history
     * @return list<array{
     *     sequence: int,
     *     shape: string,
     *     detail: ?string,
     *     resolved: bool,
     *     value: mixed,
     *     failure: ?Throwable,
     *     condition_key: ?string,
     *     condition_definition_fingerprint: ?string,
     *     timeout_seconds: ?int,
     *     parallel_path: list<array<string, mixed>>,
     *     resolution_order: int
     * }>
     */
    private function recordedSteps(array $history): array
    {
        $steps = [];
        $conditionStepsByWaitId = [];
        $versionMarkerSequences = [];
        $versionMarkerChangeIds = [];
        $memoSequences = [];
        $fallbackSequence = 1_000_000;
        foreach ($history as $resolutionOrder => $event) {
            $type = (string) ($event['event_type'] ?? $event['type'] ?? '');
            $payload = isset($event['payload']) && is_array($event['payload']) ? $event['payload'] : [];
            $sequence = match ($type) {
                'VersionMarkerRecorded' => $this->versionMarkerSequence($payload),
                'MemoUpserted' => $this->memoSequence($payload),
                default => $this->sequence($payload) ?? $fallbackSequence++,
            };
            $key = (string) $sequence;

            if ($type !== 'VersionMarkerRecorded'
                && isset($versionMarkerSequences[$key])
                && $this->historyEventShape($type, $payload) !== null) {
                throw new NonDeterministicWorkflow(
                    "Workflow sequence {$sequence} contains a version marker and another durable command.",
                    $sequence,
                    'one durable command shape',
                    'version marker and '.$this->historyEventShape($type, $payload),
                    'durable_command_sequence_collision',
                );
            }

            if (in_array($type, ['ActivityScheduled', 'ActivityStarted'], true)) {
                $steps[$key] ??= $this->step(
                    $sequence,
                    'activity',
                    $this->payloadDetail($payload, 'activity'),
                    parallelPath: $this->parallelPath($payload, $sequence),
                );
            } elseif ($type === 'ActivityCompleted') {
                if (($steps[$key]['resolved'] ?? false) === true) {
                    continue;
                }
                $steps[$key] = $this->resolvedStep(
                    $sequence,
                    'activity',
                    $this->decodeResult($payload),
                    detail: $this->payloadDetail($payload, 'activity') ?? ($steps[$key]['detail'] ?? null),
                    parallelPath: $this->resolutionParallelPath($payload, $steps[$key] ?? null, $sequence),
                    resolutionOrder: $resolutionOrder,
                );
            } elseif (in_array($type, ['ActivityFailed', 'ActivityTimedOut', 'ActivityCancelled'], true)) {
                if (($steps[$key]['resolved'] ?? false) === true) {
                    continue;
                }
                $failure = new ActivityFailed(
                    (string) ($payload['message'] ?? $payload['closed_reason'] ?? 'Activity failed.'),
                    isset($payload['activity_type']) ? (string) $payload['activity_type'] : null,
                    isset($payload['exception_type']) ? (string) $payload['exception_type'] : null,
                    (bool) ($payload['non_retryable'] ?? false),
                    $payload,
                );
                $steps[$key] = $this->resolvedStep(
                    $sequence,
                    'activity',
                    null,
                    $failure,
                    $this->payloadDetail($payload, 'activity') ?? ($steps[$key]['detail'] ?? null),
                    parallelPath: $this->resolutionParallelPath($payload, $steps[$key] ?? null, $sequence),
                    resolutionOrder: $resolutionOrder,
                );
            } elseif ($type === 'TimerScheduled') {
                if (!in_array($payload['timer_kind'] ?? null, ['condition_timeout', 'signal_timeout'], true)) {
                    $steps[$key] ??= $this->step(
                        $sequence,
                        'timer',
                        $this->payloadDetail($payload, 'timer'),
                        parallelPath: $this->parallelPath($payload, $sequence),
                    );
                }
            } elseif ($type === 'TimerFired') {
                if (($payload['timer_kind'] ?? null) === 'condition_timeout') {
                    $conditionStepKey = $this->conditionStepKey($payload, $conditionStepsByWaitId);
                    if ($conditionStepKey !== null
                        && isset($steps[$conditionStepKey])
                        && $steps[$conditionStepKey]['resolved'] === false) {
                        $steps[$conditionStepKey] = $this->resolvedStep(
                            $steps[$conditionStepKey]['sequence'],
                            'condition_wait',
                            false,
                            detail: $steps[$conditionStepKey]['detail'],
                            conditionKey: $steps[$conditionStepKey]['condition_key'],
                            conditionDefinitionFingerprint: $steps[$conditionStepKey]['condition_definition_fingerprint'],
                            timeoutSeconds: $steps[$conditionStepKey]['timeout_seconds'],
                            parallelPath: $this->resolutionParallelPath(
                                $payload,
                                $steps[$conditionStepKey],
                                $steps[$conditionStepKey]['sequence'],
                            ),
                            resolutionOrder: $resolutionOrder,
                        );
                    }
                } elseif (($payload['timer_kind'] ?? null) !== 'signal_timeout') {
                    if (($steps[$key]['resolved'] ?? false) === true) {
                        continue;
                    }
                    $steps[$key] = $this->resolvedStep(
                        $sequence,
                        'timer',
                        null,
                        detail: $this->payloadDetail($payload, 'timer') ?? ($steps[$key]['detail'] ?? null),
                        parallelPath: $this->resolutionParallelPath($payload, $steps[$key] ?? null, $sequence),
                        resolutionOrder: $resolutionOrder,
                    );
                }
            } elseif ($type === 'TimerCancelled'
                && !in_array($payload['timer_kind'] ?? null, ['condition_timeout', 'signal_timeout'], true)) {
                if (($steps[$key]['resolved'] ?? false) === true) {
                    continue;
                }
                $steps[$key] = $this->resolvedStep(
                    $sequence,
                    'timer',
                    null,
                    new WorkflowCancelled((string) ($payload['message'] ?? 'Durable timer was cancelled.')),
                    $this->payloadDetail($payload, 'timer') ?? ($steps[$key]['detail'] ?? null),
                    parallelPath: $this->resolutionParallelPath($payload, $steps[$key] ?? null, $sequence),
                    resolutionOrder: $resolutionOrder,
                );
            } elseif (in_array($type, ['ChildWorkflowScheduled', 'ChildRunStarted'], true)) {
                $steps[$key] ??= $this->step(
                    $sequence,
                    'child_workflow',
                    $this->payloadDetail($payload, 'child_workflow'),
                    parallelPath: $this->parallelPath($payload, $sequence),
                );
            } elseif ($type === 'ChildRunCompleted') {
                if (($steps[$key]['resolved'] ?? false) === true) {
                    continue;
                }
                $steps[$key] = $this->resolvedStep(
                    $sequence,
                    'child_workflow',
                    $this->decodeResult($payload),
                    detail: $this->payloadDetail($payload, 'child_workflow') ?? ($steps[$key]['detail'] ?? null),
                    parallelPath: $this->resolutionParallelPath($payload, $steps[$key] ?? null, $sequence),
                    resolutionOrder: $resolutionOrder,
                );
            } elseif (in_array($type, ['ChildRunFailed', 'ChildRunCancelled', 'ChildRunTerminated'], true)) {
                if (($steps[$key]['resolved'] ?? false) === true) {
                    continue;
                }
                $steps[$key] = $this->resolvedStep(
                    $sequence,
                    'child_workflow',
                    null,
                    new ChildWorkflowFailed(
                        (string) ($payload['message'] ?? 'Child workflow failed.'),
                        isset($payload['child_workflow_type']) ? (string) $payload['child_workflow_type'] : null,
                        $type,
                        $payload,
                    ),
                    $this->payloadDetail($payload, 'child_workflow') ?? ($steps[$key]['detail'] ?? null),
                    parallelPath: $this->resolutionParallelPath($payload, $steps[$key] ?? null, $sequence),
                    resolutionOrder: $resolutionOrder,
                );
            } elseif ($type === 'SideEffectRecorded') {
                $steps[$key] = $this->resolvedStep(
                    $sequence,
                    'side_effect',
                    $this->decodeResult($payload),
                    resolutionOrder: $resolutionOrder,
                );
            } elseif ($type === 'VersionMarkerRecorded') {
                if (isset($versionMarkerSequences[$key])) {
                    throw new NonDeterministicWorkflow(
                        "Workflow sequence {$sequence} contains more than one VersionMarkerRecorded event.",
                        $sequence,
                        'one VersionMarkerRecorded event',
                        'multiple VersionMarkerRecorded events',
                        'duplicate_version_marker_record',
                    );
                }
                if (isset($steps[$key])) {
                    throw new NonDeterministicWorkflow(
                        "Workflow sequence {$sequence} contains a version marker and another durable command.",
                        $sequence,
                        'one durable command shape',
                        $steps[$key]['shape'].' and version marker',
                        'durable_command_sequence_collision',
                    );
                }
                [$changeId, $version] = $this->versionMarker($payload, $sequence);
                if (isset($versionMarkerChangeIds[$changeId])) {
                    throw new NonDeterministicWorkflow(
                        "Version marker {$changeId} is recorded more than once in workflow history.",
                        $sequence,
                        'one marker for the change ID',
                        "markers at sequences {$versionMarkerChangeIds[$changeId]} and {$sequence}",
                        'duplicate_version_marker',
                    );
                }
                $versionMarkerSequences[$key] = true;
                $versionMarkerChangeIds[$changeId] = $sequence;
                $steps[$key] = $this->resolvedStep(
                    $sequence,
                    'version_marker',
                    $version,
                    detail: $changeId,
                    resolutionOrder: $resolutionOrder,
                );
            } elseif ($type === 'MemoUpserted') {
                if (isset($memoSequences[$key])) {
                    throw new NonDeterministicWorkflow(
                        "Workflow sequence {$sequence} contains more than one MemoUpserted event.",
                        $sequence,
                        'one MemoUpserted event',
                        'multiple MemoUpserted events',
                        'duplicate_memo_upsert_record',
                    );
                }
                if (isset($steps[$key])) {
                    throw new NonDeterministicWorkflow(
                        "Workflow sequence {$sequence} contains a memo upsert and another durable command.",
                        $sequence,
                        'one durable command shape',
                        $steps[$key]['shape'].' and memo upsert',
                        'durable_command_sequence_collision',
                    );
                }
                if (!isset($payload['entries']) || !is_array($payload['entries'])) {
                    throw new NonDeterministicWorkflow(
                        "MemoUpserted history at sequence {$sequence} is missing replay identity entries.",
                        $sequence,
                        'memo entries object',
                        'missing entries',
                        'memo_entries_missing',
                    );
                }
                if (!isset($payload['merged']) || !is_array($payload['merged'])) {
                    throw new NonDeterministicWorkflow(
                        "MemoUpserted history at sequence {$sequence} is missing its merged projection.",
                        $sequence,
                        'merged memo projection',
                        'missing merged',
                        'memo_merged_projection_missing',
                    );
                }
                $entries = $this->decodeMemoHistoryMap(
                    $payload['entries'],
                    $sequence,
                    'entries',
                    true,
                );
                $this->decodeMemoHistoryMap($payload['merged'], $sequence, 'merged', false);
                $memoSequences[$key] = true;
                $steps[$key] = $this->resolvedStep(
                    $sequence,
                    'memo',
                    null,
                    detail: $this->codec->encode($entries),
                    resolutionOrder: $resolutionOrder,
                );
            } elseif ($type === 'SearchAttributesUpserted') {
                $steps[$key] = $this->resolvedStep(
                    $sequence,
                    'search_attributes',
                    null,
                    resolutionOrder: $resolutionOrder,
                );
            } elseif ($type === 'ConditionWaitOpened') {
                $conditionKey = $this->stringValue($payload['condition_key'] ?? null);
                $conditionDefinitionFingerprint = $this->stringValue(
                    $payload['condition_definition_fingerprint'] ?? null,
                );
                $timeoutSeconds = $this->intValue($payload['timeout_seconds'] ?? null);
                $steps[$key] ??= $this->step(
                    $sequence,
                    'condition_wait',
                    $this->conditionDetail($conditionKey, $conditionDefinitionFingerprint),
                    $conditionKey,
                    $conditionDefinitionFingerprint,
                    $timeoutSeconds,
                    $this->parallelPath($payload, $sequence),
                );
                $conditionWaitId = $this->stringValue($payload['condition_wait_id'] ?? null);
                if ($conditionWaitId !== null) {
                    $conditionStepsByWaitId[$conditionWaitId] = $key;
                }
            } elseif (in_array($type, ['ConditionWaitSatisfied', 'ConditionWaitTimedOut'], true)) {
                $conditionStepKey = $this->conditionStepKey($payload, $conditionStepsByWaitId);
                if ($conditionStepKey !== null
                    && isset($steps[$conditionStepKey])
                    && $steps[$conditionStepKey]['resolved'] === false) {
                    $steps[$conditionStepKey] = $this->resolvedStep(
                        $steps[$conditionStepKey]['sequence'],
                        'condition_wait',
                        $type === 'ConditionWaitSatisfied',
                        detail: $steps[$conditionStepKey]['detail'],
                        conditionKey: $steps[$conditionStepKey]['condition_key'],
                        conditionDefinitionFingerprint: $steps[$conditionStepKey]['condition_definition_fingerprint'],
                        timeoutSeconds: $steps[$conditionStepKey]['timeout_seconds'],
                        parallelPath: $this->resolutionParallelPath(
                            $payload,
                            $steps[$conditionStepKey],
                            $steps[$conditionStepKey]['sequence'],
                        ),
                        resolutionOrder: $resolutionOrder,
                    );
                }
            }
        }
        ksort($steps, SORT_NUMERIC);

        return $this->collapseConditionReopens(array_values($steps));
    }

    /** @param list<array<string, mixed>> $history
     *  @return array<string, array<string, mixed>>
     */
    private function selectionResolutions(array $history): array
    {
        $resolutions = [];
        foreach ($history as $event) {
            if (($event['event_type'] ?? $event['type'] ?? null) !== 'SelectionResolved') {
                continue;
            }
            $payload = is_array($event['payload'] ?? null) ? $event['payload'] : [];
            $groupId = $payload['selection_group_id'] ?? null;
            $memberIndex = $payload['member_index'] ?? null;
            $memberBase = $payload['member_base_sequence'] ?? null;
            $identity = $payload['operation_identity'] ?? null;
            if (!is_string($groupId) || !preg_match('/\Aselect-calls:[1-9][0-9]*:[1-9][0-9]*\z/', $groupId)
                || !is_int($memberIndex) || $memberIndex < 0
                || !is_int($memberBase) || $memberBase < 1
                || !is_string($identity) || $identity === '') {
                throw new NonDeterministicWorkflow(
                    'SelectionResolved history contains invalid durable winner identity.',
                    $memberBase,
                    'stable selection group, member, and operation identity',
                    json_encode($payload, JSON_THROW_ON_ERROR),
                    'selection_resolution_metadata_invalid',
                );
            }
            if (isset($resolutions[$groupId])) {
                throw new NonDeterministicWorkflow(
                    "Selection group {$groupId} contains more than one committed winner.",
                    $memberBase,
                    'exactly one SelectionResolved marker',
                    'duplicate SelectionResolved markers',
                    'duplicate_selection_resolution',
                );
            }
            $resolutions[$groupId] = $payload;
        }

        return $resolutions;
    }

    /** @param list<array<string, mixed>> $history
     *  @return array<string, array<string, mixed>>
     */
    private function selectionCancellations(array $history): array
    {
        $cancellations = [];
        foreach ($history as $event) {
            if (($event['event_type'] ?? $event['type'] ?? null) !== 'SelectionOperationCancelled') {
                continue;
            }
            $payload = is_array($event['payload'] ?? null) ? $event['payload'] : [];
            $groupId = $payload['selection_group_id'] ?? null;
            $memberBase = $payload['member_base_sequence'] ?? null;
            if (!is_string($groupId)
                || !preg_match('/\Aselect-calls:[1-9][0-9]*:[1-9][0-9]*\z/', $groupId)
                || !is_int($memberBase) || $memberBase < 1) {
                throw new NonDeterministicWorkflow(
                    'Selection cancellation history contains invalid durable identity.',
                    is_int($memberBase) ? $memberBase : null,
                    'SelectionOperationCancelled with stable group and member base',
                    json_encode($payload, JSON_THROW_ON_ERROR),
                    'selection_cancellation_invalid',
                );
            }
            $key = $groupId.':'.$memberBase;
            if (isset($cancellations[$key]) && $cancellations[$key] !== $payload) {
                throw new NonDeterministicWorkflow(
                    'Selection cancellation history contains conflicting markers.',
                    $memberBase,
                    'one stable SelectionOperationCancelled marker',
                    json_encode($payload, JSON_THROW_ON_ERROR),
                    'selection_cancellation_conflict',
                );
            }
            $cancellations[$key] = $payload;
        }

        return $cancellations;
    }

    /**
     * @param list<array<string, mixed>> $history
     * @return array<int, string>
     */
    private function selectionOperationIdentities(array $history): array
    {
        $identities = [];
        $priorities = [];

        foreach ($history as $event) {
            $type = (string) ($event['event_type'] ?? $event['type'] ?? '');
            $payload = is_array($event['payload'] ?? null) ? $event['payload'] : [];
            $sequence = $this->sequence($payload);
            $descriptor = match ($type) {
                'ActivityScheduled' => ['activity_execution_id', 10],
                'ChildWorkflowScheduled' => ['child_workflow_run_id', 10],
                'TimerScheduled' => ['timer_id', 1],
                'SignalWaitOpened' => ['signal_wait_id', 20],
                'ConditionWaitOpened' => ['condition_wait_id', 20],
                default => null,
            };
            if ($sequence === null || $descriptor === null) {
                continue;
            }

            [$field, $priority] = $descriptor;
            $identity = $this->stringValue($payload[$field] ?? null);
            if ($identity !== null && $identity !== '' && $priority > ($priorities[$sequence] ?? -1)) {
                $identities[$sequence] = $identity;
                $priorities[$sequence] = $priority;
            }
        }

        return $identities;
    }

    /**
     * @param array<string, mixed> $marker
     * @param array<int, array<string, mixed>> $stepsBySequence
     * @param list<array<string, mixed>> $history
     * @param array<int, string> $operationIdentities
     * @return array<string, mixed>
     */
    private function validatedSelectionResolution(
        ParallelWorkflowCommand $group,
        int $baseSequence,
        array $marker,
        array $stepsBySequence,
        array $history,
        array $operationIdentities,
    ): array {
        $groupSize = $group->leafCount();
        $expectedGroupId = "select-calls:{$baseSequence}:{$groupSize}";
        foreach ([
            'selection_group_id' => $expectedGroupId,
            'selection_group_base_sequence' => $baseSequence,
            'selection_group_size' => $groupSize,
        ] as $field => $expected) {
            if (($marker[$field] ?? null) !== $expected) {
                $this->throwSelectionMarkerMismatch(
                    $baseSequence,
                    $marker,
                    "field {$field} does not match the authored selection group",
                );
            }
        }

        $memberIndex = $marker['member_index'] ?? null;
        if (!is_int($memberIndex) || !array_key_exists($memberIndex, $group->operations)) {
            $this->throwSelectionMarkerMismatch(
                $baseSequence,
                $marker,
                'member_index does not name an authored selection member',
            );
        }

        $cursor = 0;
        foreach ($group->operations as $index => $operation) {
            $memberSize = $operation instanceof ParallelWorkflowCommand ? $operation->leafCount() : 1;
            if ($index !== $memberIndex) {
                $cursor += $memberSize;
                continue;
            }

            $memberBase = $baseSequence + $cursor;
            $memberKind = $operation instanceof ParallelWorkflowCommand
                ? 'group'
                : match ($operation->command->historyShape) {
                    'activity' => 'activity',
                    'child_workflow' => 'child',
                    'timer' => 'timer',
                    'condition_wait' => 'condition',
                    default => $operation->command->historyShape,
                };
            $identity = $memberKind === 'group'
                ? "group:{$memberBase}:{$memberSize}"
                : ($operationIdentities[$memberBase] ?? null);
            foreach ([
                'member_key' => $group->keys[$index],
                'member_base_sequence' => $memberBase,
                'member_size' => $memberSize,
                'operation_kind' => $memberKind,
                'operation_identity' => $identity,
            ] as $field => $expected) {
                if ($expected === null || ($marker[$field] ?? null) !== $expected) {
                    $this->throwSelectionMarkerMismatch(
                        $memberBase,
                        $marker,
                        "field {$field} does not match the authored durable member",
                    );
                }
            }

            $outcome = $marker['outcome'] ?? null;
            if (!in_array($outcome, ['completed', 'failed'], true)) {
                $this->throwSelectionMarkerMismatch($memberBase, $marker, 'outcome must be completed or failed');
            }
            $resolutionId = $marker['resolution_event_id'] ?? null;
            $resolutionType = $marker['resolution_event_type'] ?? null;
            if (!is_string($resolutionId) || $resolutionId === '' || !is_string($resolutionType)) {
                $this->throwSelectionMarkerMismatch(
                    $memberBase,
                    $marker,
                    'resolution_event_id/type must identify the terminal durable event',
                );
            }

            $failureTypes = [
                'ActivityFailed',
                'ActivityCancelled',
                'ActivityTimedOut',
                'ChildRunFailed',
                'ChildRunCancelled',
                'ChildRunTerminated',
            ];
            $successTypes = [
                'ActivityCompleted',
                'ChildRunCompleted',
                'TimerFired',
                'SignalApplied',
                'ConditionWaitSatisfied',
                'ConditionWaitTimedOut',
            ];
            $terminalTypes = $outcome === 'failed' ? $failureTypes : $successTypes;
            $candidates = [];
            foreach ($history as $order => $event) {
                $type = (string) ($event['event_type'] ?? $event['type'] ?? '');
                $payload = is_array($event['payload'] ?? null) ? $event['payload'] : [];
                $sequence = $this->sequence($payload);
                if (!in_array($type, $terminalTypes, true)
                    || $sequence === null
                    || $sequence < $memberBase
                    || $sequence >= $memberBase + $memberSize) {
                    continue;
                }
                $eventId = $event['id'] ?? $event['event_id'] ?? null;
                if (!is_string($eventId) || $eventId === '') {
                    $this->throwSelectionMarkerMismatch(
                        $memberBase,
                        $marker,
                        'terminal selection history is missing its durable event id',
                    );
                }
                $candidates[] = [
                    'id' => $eventId,
                    'type' => $type,
                    'sequence' => $sequence,
                    'order' => $order,
                ];
            }
            $resolution = $outcome === 'failed' ? ($candidates[0] ?? null) : ($candidates[array_key_last($candidates)] ?? null);
            if (!is_array($resolution)
                || $resolution['id'] !== $resolutionId
                || $resolution['type'] !== $resolutionType) {
                $this->throwSelectionMarkerMismatch(
                    $memberBase,
                    $marker,
                    'resolution event is not the event that made the authored member terminal',
                );
            }

            if ($outcome === 'completed') {
                for ($sequence = $memberBase; $sequence < $memberBase + $memberSize; ++$sequence) {
                    $step = $stepsBySequence[$sequence] ?? null;
                    if (!is_array($step) || $step['resolved'] !== true || $step['failure'] instanceof Throwable) {
                        $this->throwSelectionMarkerMismatch(
                            $memberBase,
                            $marker,
                            'completed nested winner does not have a fully completed durable barrier',
                        );
                    }
                }
            } else {
                $step = $stepsBySequence[$resolution['sequence']] ?? null;
                if (!is_array($step) || !$step['failure'] instanceof Throwable) {
                    $this->throwSelectionMarkerMismatch(
                        $memberBase,
                        $marker,
                        'failed winner does not reference its exact durable failure',
                    );
                }
            }

            return [...$marker, '_resolution_sequence' => $resolution['sequence']];
        }

        $this->throwSelectionMarkerMismatch($baseSequence, $marker, 'winner is outside the authored selection group');
    }

    /** @param array<string, mixed> $marker */
    private function throwSelectionMarkerMismatch(int $sequence, array $marker, string $detail): never
    {
        throw new NonDeterministicWorkflow(
            "SelectionResolved {$detail}.",
            $sequence,
            'SelectionResolved matching the authored member and terminal event',
            json_encode($marker, JSON_THROW_ON_ERROR),
            'selection_resolution_member_mismatch',
        );
    }

    /**
     * @param array<int, array<string, mixed>> $stepsBySequence
     * @param array<string, array<string, mixed>> $selectionCancellations
     * @return array{resolved: bool, value: mixed, failure: ?Throwable}
     */
    private function durableHandleResolution(
        DurableOperationHandle $handle,
        array $stepsBySequence,
        array $selectionCancellations,
    ): array {
        if ($this->selectionCancellationForHandle($handle, $selectionCancellations) !== null) {
            return [
                'resolved' => true,
                'value' => null,
                'failure' => new DurableOperationCancelled(
                    $handle->selectionGroupId,
                    $handle->key,
                    $handle->index,
                    $handle->kind,
                    $handle->identity,
                ),
            ];
        }

        if ($handle->operation instanceof DeferredWorkflowOperation) {
            $step = $stepsBySequence[$handle->baseSequence] ?? null;

            return is_array($step) && $step['resolved'] === true
                ? ['resolved' => true, 'value' => $step['value'], 'failure' => $step['failure']]
                : ['resolved' => false, 'value' => null, 'failure' => null];
        }

        $results = [];
        $pending = false;
        $failures = [];
        foreach ($handle->operation->leafDescriptors($handle->baseSequence) as $descriptor) {
            $step = $stepsBySequence[$handle->baseSequence + $descriptor['offset']] ?? null;
            if (!is_array($step) || $step['resolved'] !== true) {
                $pending = true;
                continue;
            }
            if ($step['failure'] instanceof Throwable) {
                $failures[] = $step;
                continue;
            }
            $results[$descriptor['offset']] = $step['value'];
        }
        if ($failures !== []) {
            usort($failures, static fn (array $left, array $right): int =>
                $left['resolution_order'] <=> $right['resolution_order']);

            return ['resolved' => true, 'value' => null, 'failure' => $failures[0]['failure']];
        }
        if ($pending) {
            return ['resolved' => false, 'value' => null, 'failure' => null];
        }
        ksort($results);

        return [
            'resolved' => true,
            'value' => $handle->operation->nestedResults(array_values($results)),
            'failure' => null,
        ];
    }

    /**
     * @param array<string, array<string, mixed>> $selectionCancellations
     * @return array<string, mixed>|null
     */
    private function selectionCancellationForHandle(
        DurableOperationHandle $handle,
        array $selectionCancellations,
    ): ?array {
        $payload = $selectionCancellations[$handle->selectionGroupId.':'.$handle->baseSequence] ?? null;
        if (!is_array($payload)) {
            return null;
        }

        foreach ([
            'selection_group_id' => $handle->selectionGroupId,
            'member_key' => $handle->key,
            'member_index' => $handle->index,
            'member_base_sequence' => $handle->baseSequence,
            'member_size' => $handle->size,
            'operation_kind' => $handle->kind,
            'operation_identity' => $handle->identity,
        ] as $field => $expected) {
            if (($payload[$field] ?? null) === $expected) {
                continue;
            }

            throw new NonDeterministicWorkflow(
                "Selection cancellation field {$field} does not match the authored durable member.",
                $handle->baseSequence,
                'SelectionOperationCancelled matching the authored selection handle',
                json_encode($payload, JSON_THROW_ON_ERROR),
                'selection_cancellation_member_mismatch',
            );
        }

        return $payload;
    }

    /**
     * @param array<int|string, DurableOperationHandle> $handles
     * @param array<string, array<string, mixed>> $selectionCancellations
     */
    private function validateSelectionCancellationsForHandles(
        array $handles,
        array $selectionCancellations,
    ): void {
        $first = reset($handles);
        if (!$first instanceof DurableOperationHandle) {
            return;
        }
        $byBase = [];
        foreach ($handles as $handle) {
            $byBase[$handle->baseSequence] = $handle;
        }
        foreach ($selectionCancellations as $payload) {
            if (($payload['selection_group_id'] ?? null) !== $first->selectionGroupId) {
                continue;
            }
            $memberBase = $payload['member_base_sequence'] ?? null;
            $handle = is_int($memberBase) ? ($byBase[$memberBase] ?? null) : null;
            if (!$handle instanceof DurableOperationHandle) {
                throw new NonDeterministicWorkflow(
                    'Selection cancellation member base does not name an authored durable member.',
                    is_int($memberBase) ? $memberBase : null,
                    'SelectionOperationCancelled matching an authored selection handle',
                    json_encode($payload, JSON_THROW_ON_ERROR),
                    'selection_cancellation_member_mismatch',
                );
            }
            $this->selectionCancellationForHandle($handle, $selectionCancellations);
        }
    }

    /**
     * Decode the inline Avro map envelope returned by Server history.
     *
     * External memo command references are resolved by the runtime before the
     * MemoUpserted event is recorded, so replay always consumes inline bytes.
     *
     * @return array<string, mixed>
     */
    private function decodeMemoHistoryMap(
        mixed $envelope,
        int $sequence,
        string $field,
        bool $requireEntries,
    ): array {
        try {
            if (!is_array($envelope)) {
                throw new LogicException('expected an Avro payload envelope object');
            }

            $keys = array_keys($envelope);
            sort($keys);
            if ($keys !== ['blob', 'codec']) {
                throw new LogicException('expected exactly the public {codec, blob} payload envelope');
            }

            $decoded = $this->codec->decodeEnvelope($envelope);
            if ($decoded instanceof AvroMapValue) {
                $entries = [];
                foreach ($decoded->pairs as [$key, $value]) {
                    $entries[$key] = $value;
                }
                $decoded = $entries;
            }
            if (!is_array($decoded)) {
                throw new LogicException('the Avro payload must decode to a string-keyed map');
            }
            if ($decoded === []) {
                if (!$requireEntries) {
                    return [];
                }

                throw new LogicException('the memo entries map must not be empty');
            }
            if (array_is_list($decoded)) {
                throw new LogicException('the Avro payload must decode to a string-keyed map');
            }

            return WorkflowCommand::canonicalMemoEntries($decoded);
        } catch (NonDeterministicWorkflow $exception) {
            throw $exception;
        } catch (Throwable $exception) {
            throw new NonDeterministicWorkflow(
                "MemoUpserted history at sequence {$sequence} has an invalid {$field} envelope: {$exception->getMessage()}",
                $sequence,
                "Avro memo {$field} payload envelope",
                get_debug_type($envelope),
                $field === 'entries' ? 'memo_entries_invalid' : 'memo_merged_projection_invalid',
            );
        }
    }

    /** @param list<array<string, mixed>> $history */
    private function hasCompletedHistory(array $history): bool
    {
        foreach ($history as $event) {
            if (($event['event_type'] ?? $event['type'] ?? null) === 'WorkflowCompleted') {
                return true;
            }
        }

        return false;
    }

    /**
     * @param list<array{
     *     sequence: int,
     *     shape: string,
     *     detail: ?string,
     *     resolved: bool,
     *     value: mixed,
     *     failure: ?Throwable,
     *     condition_key: ?string,
     *     condition_definition_fingerprint: ?string,
     *     timeout_seconds: ?int,
     *     parallel_path: list<array<string, mixed>>,
     *     resolution_order: int
     * }> $steps
     */
    private function assertNoRemainingSteps(array $steps, int $stepCursor, string $terminalCommand): void
    {
        if ($stepCursor >= count($steps)) {
            return;
        }

        $step = $steps[$stepCursor];
        throw new NonDeterministicWorkflow(
            "Workflow reached {$terminalCommand} before consuming its recorded durable history.",
            $step['sequence'],
            $step['shape'],
            $terminalCommand,
        );
    }

    /**
     * @param list<array<string, mixed>> $parallelPath
     * @return array{
     *     sequence: int,
     *     shape: string,
     *     detail: ?string,
     *     resolved: bool,
     *     value: mixed,
     *     failure: ?Throwable,
     *     condition_key: ?string,
     *     condition_definition_fingerprint: ?string,
     *     timeout_seconds: ?int,
     *     parallel_path: list<array<string, mixed>>,
     *     resolution_order: int
     * }
     */
    private function step(
        int $sequence,
        string $shape,
        ?string $detail = null,
        ?string $conditionKey = null,
        ?string $conditionDefinitionFingerprint = null,
        ?int $timeoutSeconds = null,
        array $parallelPath = [],
        int $resolutionOrder = PHP_INT_MAX,
    ): array {
        return [
            'sequence' => $sequence,
            'shape' => $shape,
            'detail' => $detail,
            'resolved' => false,
            'value' => null,
            'failure' => null,
            'condition_key' => $conditionKey,
            'condition_definition_fingerprint' => $conditionDefinitionFingerprint,
            'timeout_seconds' => $timeoutSeconds,
            'parallel_path' => $parallelPath,
            'resolution_order' => $resolutionOrder,
        ];
    }

    /**
     * @param list<array<string, mixed>> $parallelPath
     * @return array{
     *     sequence: int,
     *     shape: string,
     *     detail: ?string,
     *     resolved: bool,
     *     value: mixed,
     *     failure: ?Throwable,
     *     condition_key: ?string,
     *     condition_definition_fingerprint: ?string,
     *     timeout_seconds: ?int,
     *     parallel_path: list<array<string, mixed>>,
     *     resolution_order: int
     * }
     */
    private function resolvedStep(
        int $sequence,
        string $shape,
        mixed $value,
        ?Throwable $failure = null,
        ?string $detail = null,
        ?string $conditionKey = null,
        ?string $conditionDefinitionFingerprint = null,
        ?int $timeoutSeconds = null,
        array $parallelPath = [],
        int $resolutionOrder = PHP_INT_MAX,
    ): array
    {
        return [
            'sequence' => $sequence,
            'shape' => $shape,
            'detail' => $detail,
            'resolved' => true,
            'value' => $value,
            'failure' => $failure,
            'condition_key' => $conditionKey,
            'condition_definition_fingerprint' => $conditionDefinitionFingerprint,
            'timeout_seconds' => $timeoutSeconds,
            'parallel_path' => $parallelPath,
            'resolution_order' => $resolutionOrder,
        ];
    }

    /** @param array<string, mixed> $step */
    private function assertCommandMatchesStep(WorkflowCommand $command, array $step): void
    {
        if ($step['shape'] !== $command->historyShape) {
            throw new NonDeterministicWorkflow(
                "History contains {$step['shape']} but workflow scheduled {$command->historyShape}.",
                $step['sequence'],
                $step['shape'],
                $command->historyShape,
            );
        }

        $actualDetail = $this->commandDetail($command);
        if ($command->type !== 'open_condition_wait'
            && $step['detail'] !== null
            && $actualDetail !== null
            && $step['detail'] !== $actualDetail) {
            throw new NonDeterministicWorkflow(
                "Recorded {$step['shape']} detail changed from {$step['detail']} to {$actualDetail}.",
                $step['sequence'],
                $step['detail'],
                $actualDetail,
            );
        }
    }

    /**
     * @param array<string, mixed> $step
     * @param list<array<string, mixed>> $expectedPath
     */
    private function assertParallelPathMatches(array $step, array $expectedPath): void
    {
        if ($step['parallel_path'] === []) {
            throw new NonDeterministicWorkflow(
                'Recorded parallel member is missing its durable group path.',
                $step['sequence'],
                json_encode($expectedPath, JSON_THROW_ON_ERROR),
                '<missing>',
                'parallel_group_metadata_missing',
            );
        }
        if ($step['parallel_path'] !== $expectedPath) {
            throw new NonDeterministicWorkflow(
                'Recorded parallel-group identity or path changed during replay.',
                $step['sequence'],
                json_encode($step['parallel_path'], JSON_THROW_ON_ERROR),
                json_encode($expectedPath, JSON_THROW_ON_ERROR),
                'parallel_group_shape_mismatch',
            );
        }
    }

    /**
     * @param array<string, mixed> $payload
     * @return list<array<string, mixed>>
     */
    private function parallelPath(array $payload, int $sequence): array
    {
        $fields = [
            'parallel_group_id',
            'parallel_group_kind',
            'parallel_group_base_sequence',
            'parallel_group_size',
            'parallel_group_index',
            'parallel_group_mode',
            'selection_member_key',
            'selection_member_index',
            'selection_member_base_sequence',
            'selection_member_size',
            'selection_member_kind',
        ];
        $hasMetadata = array_key_exists('parallel_group_path', $payload);
        foreach ($fields as $field) {
            $hasMetadata = $hasMetadata || array_key_exists($field, $payload);
        }
        if (!$hasMetadata) {
            return [];
        }

        $topLevel = $this->parallelEntry($payload, $sequence);
        $rawPath = $payload['parallel_group_path'] ?? null;
        if ($rawPath === null) {
            return [$topLevel];
        }
        if (!is_array($rawPath) || !array_is_list($rawPath) || $rawPath === []) {
            throw new NonDeterministicWorkflow(
                'Parallel-group history contains an invalid group path.',
                $sequence,
                'non-empty parallel_group_path list',
                get_debug_type($rawPath),
                'parallel_group_metadata_invalid',
            );
        }

        $path = [];
        foreach ($rawPath as $entry) {
            if (!is_array($entry)) {
                throw new NonDeterministicWorkflow(
                    'Parallel-group history contains a non-object path entry.',
                    $sequence,
                    'parallel group metadata object',
                    get_debug_type($entry),
                    'parallel_group_metadata_invalid',
                );
            }
            $path[] = $this->parallelEntry($entry, $sequence);
        }

        if ($path[array_key_last($path)] !== $topLevel) {
            throw new NonDeterministicWorkflow(
                'Parallel-group history top-level fields do not match the innermost path entry.',
                $sequence,
                json_encode($path[array_key_last($path)], JSON_THROW_ON_ERROR),
                json_encode($topLevel, JSON_THROW_ON_ERROR),
                'parallel_group_metadata_invalid',
            );
        }

        return $path;
    }

    /**
     * @param array<string, mixed> $payload
     * @return array<string, mixed>
     */
    private function parallelEntry(array $payload, int $sequence): array
    {
        $id = $payload['parallel_group_id'] ?? null;
        $kind = $payload['parallel_group_kind'] ?? null;
        $base = $payload['parallel_group_base_sequence'] ?? null;
        $size = $payload['parallel_group_size'] ?? null;
        $index = $payload['parallel_group_index'] ?? null;
        $mode = $payload['parallel_group_mode'] ?? (is_string($id) && str_starts_with($id, 'select-calls:')
            ? 'select'
            : 'all');
        if (!is_string($id) || $id === ''
            || !is_string($kind) || !in_array($kind, ['activity', 'child', 'timer', 'condition', 'mixed'], true)
            || !is_string($mode) || !in_array($mode, ['all', 'select'], true)
            || !is_int($base) || $base < 1
            || !is_int($size) || $size < 1 || $size > WorkflowContext::MAX_PARALLEL_OPERATIONS
            || !is_int($index) || $index < 0 || $index >= $size
            || $sequence !== $base + $index) {
            throw new NonDeterministicWorkflow(
                'Parallel-group history contains invalid identity, bounds, or path fields.',
                $sequence,
                'valid group, base-sequence, size, index, and path identity',
                json_encode($payload, JSON_THROW_ON_ERROR),
                'parallel_group_metadata_invalid',
            );
        }

        $prefix = $mode === 'select' ? 'select-calls' : match ($kind) {
            'activity' => 'parallel-activities',
            'child' => 'parallel-children',
            'timer' => 'parallel-timers',
            default => 'parallel-calls',
        };
        $expectedId = sprintf('%s:%d:%d', $prefix, $base, $size);
        if ($id !== $expectedId) {
            throw new NonDeterministicWorkflow(
                'Parallel-group history contains an incompatible stable group ID.',
                $sequence,
                $expectedId,
                $id,
                'parallel_group_metadata_invalid',
            );
        }

        $entry = array_filter([
            'parallel_group_id' => $id,
            'parallel_group_kind' => $kind,
            'parallel_group_mode' => $mode === 'select' ? 'select' : null,
            'parallel_group_base_sequence' => $base,
            'parallel_group_size' => $size,
            'parallel_group_index' => $index,
        ], static fn (mixed $value): bool => $value !== null);

        if ($mode === 'select') {
            $memberKey = $payload['selection_member_key'] ?? null;
            $memberIndex = $payload['selection_member_index'] ?? null;
            $memberBase = $payload['selection_member_base_sequence'] ?? null;
            $memberSize = $payload['selection_member_size'] ?? null;
            $memberKind = $payload['selection_member_kind'] ?? null;
            if (!((is_string($memberKey) && $memberKey !== '') || (is_int($memberKey) && $memberKey >= 0))
                || !is_int($memberIndex) || $memberIndex < 0
                || !is_int($memberBase) || $memberBase < $base
                || !is_int($memberSize) || $memberSize < 1
                || !is_string($memberKind)
                || !in_array($memberKind, ['activity', 'child', 'timer', 'signal', 'condition', 'group'], true)
                || $sequence < $memberBase || $sequence >= $memberBase + $memberSize) {
                throw new NonDeterministicWorkflow(
                    'Selection-group history contains invalid durable member identity.',
                    $sequence,
                    'stable member key, index, base sequence, and size',
                    json_encode($payload, JSON_THROW_ON_ERROR),
                    'selection_group_metadata_invalid',
                );
            }
            $entry = [
                ...$entry,
                'selection_member_key' => $memberKey,
                'selection_member_index' => $memberIndex,
                'selection_member_base_sequence' => $memberBase,
                'selection_member_size' => $memberSize,
                'selection_member_kind' => $memberKind,
            ];
        }

        return $entry;
    }

    /**
     * @param array<string, mixed> $payload
     * @param array<string, mixed>|null $previous
     * @return list<array<string, mixed>>
     */
    private function resolutionParallelPath(array $payload, ?array $previous, int $sequence): array
    {
        $current = $this->parallelPath($payload, $sequence);
        $recorded = is_array($previous['parallel_path'] ?? null) ? $previous['parallel_path'] : [];
        if ($current !== [] && $recorded !== [] && $current !== $recorded) {
            throw new NonDeterministicWorkflow(
                'Parallel-group metadata changed between scheduling and resolution history.',
                $sequence,
                json_encode($recorded, JSON_THROW_ON_ERROR),
                json_encode($current, JSON_THROW_ON_ERROR),
                'parallel_group_history_conflict',
            );
        }

        return $current !== [] ? $current : $recorded;
    }

    private function commandDetail(WorkflowCommand $command): ?string
    {
        $value = match ($command->historyShape) {
            'activity' => $command->attributes['activity_type'] ?? null,
            'timer' => $command->attributes['delay_seconds'] ?? null,
            'child_workflow' => $command->attributes['workflow_type'] ?? null,
            'version_marker' => $command->attributes['change_id'] ?? null,
            'memo' => $this->codec->encode(
                WorkflowCommand::canonicalMemoEntries($command->attributes['entries'] ?? []),
            ),
            'condition_wait' => $this->conditionDetail(
                $this->stringValue($command->attributes['condition_key'] ?? null),
                $this->stringValue($command->attributes['condition_definition_fingerprint'] ?? null),
            ),
            default => null,
        };

        return $value === null ? null : (string) $value;
    }

    /** @param array<string, mixed> $payload */
    private function payloadDetail(array $payload, string $shape): ?string
    {
        $value = match ($shape) {
            'activity' => $payload['activity_type'] ?? $payload['activity_name'] ?? null,
            'timer' => $payload['delay_seconds'] ?? null,
            'child_workflow' => $payload['child_workflow_type'] ?? $payload['workflow_type'] ?? null,
            'version_marker' => $payload['change_id'] ?? null,
            'condition_wait' => $this->conditionDetail(
                $this->stringValue($payload['condition_key'] ?? null),
                $this->stringValue($payload['condition_definition_fingerprint'] ?? null),
            ),
            default => null,
        };

        return $value === null ? null : (string) $value;
    }

    /** @param array<string, mixed> $payload */
    private function decodeResult(array $payload): mixed
    {
        $raw = $payload['result'] ?? $payload['output'] ?? null;
        if (is_array($raw) || is_string($raw)) {
            return $this->codec->decodeEnvelope($raw);
        }

        return $raw;
    }

    /** @param array<string, mixed> $payload */
    private function sequence(array $payload): ?int
    {
        $value = $payload['sequence'] ?? $payload['workflow_sequence'] ?? null;

        return is_numeric($value) ? (int) $value : null;
    }

    /** @param array<string, mixed> $payload */
    private function memoSequence(array $payload): int
    {
        $sequence = $payload['sequence'] ?? $payload['workflow_sequence'] ?? null;
        if ($sequence === null) {
            throw new NonDeterministicWorkflow(
                'MemoUpserted history is missing its replay sequence.',
                null,
                'positive integer sequence',
                'missing sequence',
                'memo_sequence_missing',
            );
        }
        if (!is_int($sequence) || $sequence <= 0) {
            throw new NonDeterministicWorkflow(
                'MemoUpserted history replay sequence must be a positive integer.',
                is_int($sequence) ? $sequence : null,
                'positive integer sequence',
                is_scalar($sequence) ? (string) $sequence : get_debug_type($sequence),
                'memo_sequence_invalid',
            );
        }

        return $sequence;
    }

    /** @param array<string, mixed> $payload */
    private function versionMarkerSequence(array $payload): int
    {
        $sequence = $payload['sequence'] ?? null;
        if (!is_int($sequence) || $sequence <= 0) {
            throw new NonDeterministicWorkflow(
                'Version-marker history is missing a positive integer workflow sequence.',
                is_int($sequence) ? $sequence : null,
                'positive integer sequence',
                is_scalar($sequence) ? (string) $sequence : get_debug_type($sequence),
                'version_marker_sequence_invalid',
            );
        }

        return $sequence;
    }

    /**
     * @param array<string, mixed> $payload
     * @return array{string, int}
     */
    private function versionMarker(array $payload, int $sequence): array
    {
        $changeId = $payload['change_id'] ?? null;
        if (!is_string($changeId) || trim($changeId) === '') {
            throw new NonDeterministicWorkflow(
                "Version-marker history at workflow sequence {$sequence} is missing its stable change ID.",
                $sequence,
                'non-empty change_id',
                is_scalar($changeId) ? (string) $changeId : get_debug_type($changeId),
                'version_marker_field_missing',
            );
        }

        $version = $this->versionMarkerInteger($payload, 'version', $sequence);
        $minSupported = $this->versionMarkerInteger($payload, 'min_supported', $sequence);
        $maxSupported = $this->versionMarkerInteger($payload, 'max_supported', $sequence);
        if ($minSupported > $maxSupported || $version < $minSupported || $version > $maxSupported) {
            throw new NonDeterministicWorkflow(
                "Version marker {$changeId} at workflow sequence {$sequence} contains an incompatible recorded range.",
                $sequence,
                'min_supported <= version <= max_supported',
                "{$minSupported} <= {$version} <= {$maxSupported}",
                'version_marker_history_range_invalid',
            );
        }

        return [$changeId, $version];
    }

    /** @param array<string, mixed> $payload */
    private function versionMarkerInteger(array $payload, string $field, int $sequence): int
    {
        $value = $payload[$field] ?? null;
        if (!is_int($value) || $value < self::MIN_VERSION || $value > self::MAX_VERSION) {
            throw new NonDeterministicWorkflow(
                "Version-marker history at workflow sequence {$sequence} has a missing or invalid {$field} field.",
                $sequence,
                "32-bit integer {$field}",
                is_scalar($value) ? (string) $value : get_debug_type($value),
                'version_marker_field_missing',
            );
        }

        return $value;
    }

    /** @param array<string, mixed> $payload */
    private function historyEventShape(string $type, array $payload): ?string
    {
        if (str_starts_with($type, 'Activity')) {
            return 'activity';
        }
        if (in_array($type, ['TimerScheduled', 'TimerFired'], true)) {
            return in_array($payload['timer_kind'] ?? null, ['condition_timeout', 'signal_timeout'], true)
                ? null
                : 'timer';
        }
        if ($type === 'ChildWorkflowScheduled' || str_starts_with($type, 'ChildRun')) {
            return 'child_workflow';
        }

        return match ($type) {
            'SideEffectRecorded' => 'side_effect',
            'VersionMarkerRecorded' => 'version_marker',
            'MemoUpserted' => 'memo',
            'SearchAttributesUpserted' => 'search_attributes',
            'ConditionWaitOpened', 'ConditionWaitSatisfied', 'ConditionWaitTimedOut' => 'condition_wait',
            default => null,
        };
    }

    /**
     * @param array{version: int, family: string, sequence: ?int} $decision
     */
    private function assertVersionDecisionCompatible(
        string $changeId,
        array $decision,
        WorkflowCommand $command,
    ): void {
        $family = $this->versionFamily($command);
        if ($decision['family'] !== $family) {
            throw new NonDeterministicWorkflow(
                "Version marker {$changeId} cannot switch between getVersion and patch helpers in one workflow execution.",
                $decision['sequence'],
                $decision['family'],
                $family,
                'version_marker_kind_mismatch',
            );
        }

        $this->assertVersionSupported(
            $changeId,
            $decision['version'],
            (int) $command->attributes['min_supported'],
            (int) $command->attributes['max_supported'],
            $decision['sequence'],
        );
    }

    private function assertVersionSupported(
        string $changeId,
        int $version,
        int $minSupported,
        int $maxSupported,
        ?int $sequence,
    ): void {
        if ($version >= $minSupported && $version <= $maxSupported) {
            return;
        }

        throw new NonDeterministicWorkflow(
            "Recorded version {$version} for change ID {$changeId} is outside the supported range {$minSupported}..{$maxSupported}.",
            $sequence,
            "{$minSupported}..{$maxSupported}",
            (string) $version,
            'version_marker_incompatible_range',
        );
    }

    private function versionFamily(WorkflowCommand $command): string
    {
        return $command->versionResultKind === 'version' ? 'getVersion' : 'patch';
    }

    /**
     * @param array<string, mixed> $payload
     * @param array<string, string> $conditionStepsByWaitId
     */
    private function conditionStepKey(array $payload, array $conditionStepsByWaitId): ?string
    {
        $waitId = $this->stringValue($payload['condition_wait_id'] ?? null);
        if ($waitId !== null && isset($conditionStepsByWaitId[$waitId])) {
            return $conditionStepsByWaitId[$waitId];
        }

        $sequence = $this->sequence($payload);

        return $sequence === null ? null : (string) $sequence;
    }

    /**
     * @param list<array{
     *     sequence: int,
     *     shape: string,
     *     detail: ?string,
     *     resolved: bool,
     *     value: mixed,
     *     failure: ?Throwable,
     *     condition_key: ?string,
     *     condition_definition_fingerprint: ?string,
     *     timeout_seconds: ?int,
     *     parallel_path: list<array<string, mixed>>,
     *     resolution_order: int
     * }> $steps
     * @return list<array{
     *     sequence: int,
     *     shape: string,
     *     detail: ?string,
     *     resolved: bool,
     *     value: mixed,
     *     failure: ?Throwable,
     *     condition_key: ?string,
     *     condition_definition_fingerprint: ?string,
     *     timeout_seconds: ?int,
     *     parallel_path: list<array<string, mixed>>,
     *     resolution_order: int
     * }>
     */
    private function collapseConditionReopens(array $steps): array
    {
        $collapsed = [];
        foreach ($steps as $step) {
            $last = array_key_last($collapsed);
            if ($last !== null
                && $step['shape'] === 'condition_wait'
                && $collapsed[$last]['shape'] === 'condition_wait'
                && $step['condition_key'] === $collapsed[$last]['condition_key']
                && $step['condition_definition_fingerprint'] === $collapsed[$last]['condition_definition_fingerprint']
                && $step['timeout_seconds'] === $collapsed[$last]['timeout_seconds']) {
                $step['sequence'] = $collapsed[$last]['sequence'];
                $collapsed[$last] = $step;
                continue;
            }

            $collapsed[] = $step;
        }

        return $collapsed;
    }

    /**
     * @param array{
     *     sequence: int,
     *     shape: string,
     *     detail: ?string,
     *     resolved: bool,
     *     value: mixed,
     *     failure: ?Throwable,
     *     condition_key: ?string,
     *     condition_definition_fingerprint: ?string,
     *     timeout_seconds: ?int
     * } $step
     */
    private function assertConditionWaitCompatible(array $step, WorkflowCommand $command): void
    {
        $currentKey = $this->stringValue($command->attributes['condition_key'] ?? null);
        $currentFingerprint = $this->stringValue(
            $command->attributes['condition_definition_fingerprint'] ?? null,
        );
        $currentTimeout = $this->intValue($command->attributes['timeout_seconds'] ?? null);

        if ($step['condition_key'] !== $currentKey) {
            throw new NonDeterministicWorkflow(
                sprintf(
                    'Condition wait key changed from %s to %s during replay.',
                    $step['condition_key'] ?? '<none>',
                    $currentKey ?? '<none>',
                ),
                $step['sequence'],
                $step['condition_key'],
                $currentKey,
            );
        }
        if ($step['condition_definition_fingerprint'] !== null
            && $step['condition_definition_fingerprint'] !== $currentFingerprint) {
            throw new NonDeterministicWorkflow(
                'Condition wait predicate fingerprint changed during replay.',
                $step['sequence'],
                $step['condition_definition_fingerprint'],
                $currentFingerprint,
            );
        }
        if ($step['timeout_seconds'] !== $currentTimeout) {
            throw new NonDeterministicWorkflow(
                sprintf(
                    'Condition wait timeout changed from %s to %s during replay.',
                    $step['timeout_seconds'] === null ? '<none>' : (string) $step['timeout_seconds'],
                    $currentTimeout === null ? '<none>' : (string) $currentTimeout,
                ),
                $step['sequence'],
                $step['timeout_seconds'] === null ? null : (string) $step['timeout_seconds'],
                $currentTimeout === null ? null : (string) $currentTimeout,
            );
        }
    }

    private function conditionDetail(?string $conditionKey, ?string $definitionFingerprint): string
    {
        return sprintf(
            'key=%s;predicate=%s',
            $conditionKey ?? '<none>',
            $definitionFingerprint ?? '<none>',
        );
    }

    private function stringValue(mixed $value): ?string
    {
        return is_string($value) && $value !== '' ? $value : null;
    }

    private function intValue(mixed $value): ?int
    {
        return is_int($value) ? $value : (is_numeric($value) ? (int) $value : null);
    }

    /** @return array{type: string, result: array{codec: string, blob: string}} */
    private function completeCommand(mixed $result): array
    {
        return ['type' => 'complete_workflow', 'result' => $this->codec->envelope($result)];
    }
}
