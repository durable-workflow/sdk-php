<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Codec\PayloadCodec;
use DurableWorkflow\Exception\ActivityFailed;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use Generator;
use Throwable;

/** Re-executes a workflow generator against committed, sequence-ordered history. */
final class Replayer
{
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
        $context = new WorkflowContext(
            (string) ($task['workflow_id'] ?? ''),
            (string) ($task['run_id'] ?? ''),
            $history,
            $this->codec,
            (bool) ($task['cancel_requested'] ?? false),
        );
        $execution = $handler($context, ...$input);
        if (!$execution instanceof Generator) {
            $this->assertNoRemainingSteps($steps, 0, 'complete_workflow');

            return new ReplayResult([$this->completeCommand($execution)]);
        }

        $stepCursor = 0;
        $commands = [];
        $yielded = $execution->current();

        while ($execution->valid()) {
            if (!$yielded instanceof WorkflowCommand) {
                throw new NonDeterministicWorkflow('Workflow yielded an unsupported value instead of WorkflowCommand.');
            }
            if ($yielded->type === 'continue_as_new') {
                $this->assertNoRemainingSteps($steps, $stepCursor, 'continue_as_new');
                $commands[] = $yielded->toWire($this->codec, $taskQueue);

                return new ReplayResult($commands);
            }

            $step = $steps[$stepCursor] ?? null;
            if ($step !== null) {
                if ($step['shape'] !== $yielded->historyShape) {
                    throw new NonDeterministicWorkflow(
                        "History contains {$step['shape']} but workflow yielded {$yielded->historyShape}.",
                        $step['sequence'],
                        $step['shape'],
                        $yielded->historyShape,
                    );
                }
                $actualDetail = $this->commandDetail($yielded);
                if ($step['detail'] !== null && $actualDetail !== null && $step['detail'] !== $actualDetail) {
                    throw new NonDeterministicWorkflow(
                        "Recorded {$step['shape']} detail changed from {$step['detail']} to {$actualDetail}.",
                        $step['sequence'],
                        $step['detail'],
                        $actualDetail,
                    );
                }
                ++$stepCursor;
                if ($step['resolved'] === false) {
                    return new ReplayResult($commands);
                }
                $yielded = $step['failure'] instanceof Throwable
                    ? $execution->throw($step['failure'])
                    : $execution->send($step['value']);
                continue;
            }

            if ($yielded->type === 'record_side_effect') {
                $yielded = $yielded->resolveSideEffect();
            }
            $commands[] = $yielded->toWire($this->codec, $taskQueue);
            if ($yielded->type === 'record_side_effect' || $yielded->type === 'upsert_search_attributes') {
                $yielded = $execution->send($yielded->localResult);
                continue;
            }

            return new ReplayResult($commands);
        }

        $this->assertNoRemainingSteps($steps, $stepCursor, 'complete_workflow');
        $commands[] = $this->completeCommand($execution->getReturn());

        return new ReplayResult($commands);
    }

    /**
     * @param list<array<string, mixed>> $history
     * @return list<array{sequence: int, shape: string, detail: ?string, resolved: bool, value: mixed, failure: ?Throwable}>
     */
    private function recordedSteps(array $history): array
    {
        $steps = [];
        $fallbackSequence = 1_000_000;
        foreach ($history as $event) {
            $type = (string) ($event['event_type'] ?? $event['type'] ?? '');
            $payload = isset($event['payload']) && is_array($event['payload']) ? $event['payload'] : [];
            $sequence = $this->sequence($payload) ?? $fallbackSequence++;
            $key = (string) $sequence;

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
                if (!in_array($payload['timer_kind'] ?? null, ['condition_timeout', 'signal_timeout'], true)) {
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
            } elseif ($type === 'SearchAttributesUpserted') {
                $steps[$key] = $this->resolvedStep($sequence, 'search_attributes', null);
            }
        }
        ksort($steps, SORT_NUMERIC);

        return array_values($steps);
    }

    /**
     * @param list<array{sequence: int, shape: string, detail: ?string, resolved: bool, value: mixed, failure: ?Throwable}> $steps
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

    /** @return array{sequence: int, shape: string, detail: ?string, resolved: bool, value: mixed, failure: ?Throwable} */
    private function step(int $sequence, string $shape, ?string $detail = null): array
    {
        return ['sequence' => $sequence, 'shape' => $shape, 'detail' => $detail, 'resolved' => false, 'value' => null, 'failure' => null];
    }

    /** @return array{sequence: int, shape: string, detail: ?string, resolved: bool, value: mixed, failure: ?Throwable} */
    private function resolvedStep(
        int $sequence,
        string $shape,
        mixed $value,
        ?Throwable $failure = null,
        ?string $detail = null,
    ): array
    {
        return ['sequence' => $sequence, 'shape' => $shape, 'detail' => $detail, 'resolved' => true, 'value' => $value, 'failure' => $failure];
    }

    private function commandDetail(WorkflowCommand $command): ?string
    {
        $value = match ($command->historyShape) {
            'activity' => $command->attributes['activity_type'] ?? null,
            'timer' => $command->attributes['delay_seconds'] ?? null,
            'child_workflow' => $command->attributes['workflow_type'] ?? null,
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

    /** @return array{type: string, result: array{codec: string, blob: string}} */
    private function completeCommand(mixed $result): array
    {
        return ['type' => 'complete_workflow', 'result' => $this->codec->envelope($result)];
    }
}
