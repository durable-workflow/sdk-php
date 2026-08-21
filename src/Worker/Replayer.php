<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Codec\PayloadCodec;
use DurableWorkflow\Exception\ActivityFailed;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
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
        $completedHistory = $this->hasCompletedHistory($history);
        $execution = new Fiber(function () use ($handler, $history, $input, $task): mixed {
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
            );

            return $handler($context, ...$input);
        });

        $stepCursor = 0;
        $commands = [];
        /** @var array<string, array{version: int, family: string, sequence: ?int}> $versionDecisions */
        $versionDecisions = [];
        $suspended = $execution->start();

        while (!$execution->isTerminated()) {
            if (!$suspended instanceof WorkflowCommand) {
                throw new NonDeterministicWorkflow('Workflow suspended with an unsupported value instead of WorkflowCommand.');
            }
            if ($suspended->type === 'continue_as_new') {
                $this->assertNoRemainingSteps($steps, $stepCursor, 'continue_as_new');
                $commands[] = $suspended->toWire($this->codec, $taskQueue);

                return new ReplayResult($commands);
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
                if ($step['shape'] !== $suspended->historyShape) {
                    throw new NonDeterministicWorkflow(
                        "History contains {$step['shape']} but workflow scheduled {$suspended->historyShape}.",
                        $step['sequence'],
                        $step['shape'],
                        $suspended->historyShape,
                    );
                }
                $actualDetail = $this->commandDetail($suspended);
                if ($suspended->type !== 'open_condition_wait'
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
                ++$stepCursor;
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

                    return new ReplayResult($commands);
                }
                if ($step['resolved'] === false) {
                    return new ReplayResult($commands);
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
            if ($suspended->type === 'record_side_effect' || $suspended->type === 'upsert_search_attributes') {
                $suspended = $execution->resume($suspended->localResult);
                continue;
            }

            return new ReplayResult($commands);
        }

        $this->assertNoRemainingSteps($steps, $stepCursor, 'complete_workflow');
        $result = $execution->getReturn();
        if ($result instanceof \Generator) {
            throw new LogicException(
                'Workflow handlers must call WorkflowContext operations directly; Generator results are not supported.',
            );
        }
        $commands[] = $this->completeCommand($result);

        return new ReplayResult($commands);
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
     *     timeout_seconds: ?int
     * }>
     */
    private function recordedSteps(array $history): array
    {
        $steps = [];
        $conditionStepsByWaitId = [];
        $versionMarkerSequences = [];
        $versionMarkerChangeIds = [];
        $fallbackSequence = 1_000_000;
        foreach ($history as $event) {
            $type = (string) ($event['event_type'] ?? $event['type'] ?? '');
            $payload = isset($event['payload']) && is_array($event['payload']) ? $event['payload'] : [];
            $sequence = $type === 'VersionMarkerRecorded'
                ? $this->versionMarkerSequence($payload)
                : ($this->sequence($payload) ?? $fallbackSequence++);
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
                $steps[$key] ??= $this->step($sequence, 'activity', $this->payloadDetail($payload, 'activity'));
            } elseif ($type === 'ActivityCompleted') {
                $steps[$key] = $this->resolvedStep(
                    $sequence,
                    'activity',
                    $this->decodeResult($payload),
                    detail: $this->payloadDetail($payload, 'activity') ?? ($steps[$key]['detail'] ?? null),
                );
            } elseif (in_array($type, ['ActivityFailed', 'ActivityTimedOut'], true)) {
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
                );
            } elseif ($type === 'TimerScheduled') {
                if (!in_array($payload['timer_kind'] ?? null, ['condition_timeout', 'signal_timeout'], true)) {
                    $steps[$key] ??= $this->step($sequence, 'timer', $this->payloadDetail($payload, 'timer'));
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
                        );
                    }
                } elseif (($payload['timer_kind'] ?? null) !== 'signal_timeout') {
                    $steps[$key] = $this->resolvedStep(
                        $sequence,
                        'timer',
                        null,
                        detail: $this->payloadDetail($payload, 'timer') ?? ($steps[$key]['detail'] ?? null),
                    );
                }
            } elseif (in_array($type, ['ChildWorkflowScheduled', 'ChildRunStarted'], true)) {
                $steps[$key] ??= $this->step($sequence, 'child_workflow', $this->payloadDetail($payload, 'child_workflow'));
            } elseif ($type === 'ChildRunCompleted') {
                $steps[$key] = $this->resolvedStep(
                    $sequence,
                    'child_workflow',
                    $this->decodeResult($payload),
                    detail: $this->payloadDetail($payload, 'child_workflow') ?? ($steps[$key]['detail'] ?? null),
                );
            } elseif (in_array($type, ['ChildRunFailed', 'ChildRunCancelled', 'ChildRunTerminated'], true)) {
                $steps[$key] = $this->resolvedStep(
                    $sequence,
                    'child_workflow',
                    null,
                    new ActivityFailed((string) ($payload['message'] ?? 'Child workflow failed.'), failure: $payload),
                    $this->payloadDetail($payload, 'child_workflow') ?? ($steps[$key]['detail'] ?? null),
                );
            } elseif ($type === 'SideEffectRecorded') {
                $steps[$key] = $this->resolvedStep($sequence, 'side_effect', $this->decodeResult($payload));
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
                );
            } elseif ($type === 'SearchAttributesUpserted') {
                $steps[$key] = $this->resolvedStep($sequence, 'search_attributes', null);
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
                    );
                }
            }
        }
        ksort($steps, SORT_NUMERIC);

        return $this->collapseConditionReopens(array_values($steps));
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
     *     timeout_seconds: ?int
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
     * @return array{
     *     sequence: int,
     *     shape: string,
     *     detail: ?string,
     *     resolved: bool,
     *     value: mixed,
     *     failure: ?Throwable,
     *     condition_key: ?string,
     *     condition_definition_fingerprint: ?string,
     *     timeout_seconds: ?int
     * }
     */
    private function step(
        int $sequence,
        string $shape,
        ?string $detail = null,
        ?string $conditionKey = null,
        ?string $conditionDefinitionFingerprint = null,
        ?int $timeoutSeconds = null,
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
        ];
    }

    /**
     * @return array{
     *     sequence: int,
     *     shape: string,
     *     detail: ?string,
     *     resolved: bool,
     *     value: mixed,
     *     failure: ?Throwable,
     *     condition_key: ?string,
     *     condition_definition_fingerprint: ?string,
     *     timeout_seconds: ?int
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
        ];
    }

    private function commandDetail(WorkflowCommand $command): ?string
    {
        $value = match ($command->historyShape) {
            'activity' => $command->attributes['activity_type'] ?? null,
            'timer' => $command->attributes['delay_seconds'] ?? null,
            'child_workflow' => $command->attributes['workflow_type'] ?? null,
            'version_marker' => $command->attributes['change_id'] ?? null,
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
     *     timeout_seconds: ?int
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
     *     timeout_seconds: ?int
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
