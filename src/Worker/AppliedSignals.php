<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use Closure;

/**
 * @internal Which received signals workflow code observes (sdk-php#434).
 *
 * The Server records `SignalReceived` when it accepts a signal, and applies the
 * signal to the run later, in message order with updates, when the workflow
 * task that delivers it completes. Until then the signal can still be rejected,
 * for example when the run closes first (`SignalRejected`). A signal is applied
 * when:
 *
 * - a `SignalApplied` event names its signal ID or command ID;
 * - its `SignalReceived` carries a `workflow_sequence`, which the Server adds
 *   from the signal record once a wait has consumed it;
 * - it is the signal this workflow task delivers (`resume_source_kind:
 *   workflow_signal` with its `workflow_signal_id` or `workflow_command_id`),
 *   which the task's completion applies; or
 * - its `SignalReceived` names neither a signal ID nor a command ID, so the
 *   history cannot say whether it was applied (imported or synthetic
 *   histories, and test fixtures).
 *
 * Earlier releases showed every received signal. The applied-only view is a
 * versioned change recorded with the standard version marker
 * {@see self::CHANGE_ID}:
 *
 * - The marker is recorded the first time the applied-only view changes what
 *   `signals()` returns to a live decision: a call at the end of the recorded
 *   history, or the predicate of a condition wait whose outcome is not
 *   recorded. The applied-only view applies from that call on.
 * - With the marker in the history, live decisions and replayed calls at or
 *   after the marker's sequence use the applied-only view. Replayed calls
 *   before it keep the earlier view, so the decisions recorded before the
 *   marker replay unchanged.
 * - Without the marker, every replayed call keeps the earlier view, and a run
 *   that never meets an unapplied signal records nothing.
 *
 * The marker step is transparent to the authored command sequence.
 */
final class AppliedSignals
{
    /** The version-marker change ID that selects the applied-only view. */
    public const CHANGE_ID = 'durable-workflow.sdk-php.signals-applied-only';

    /** @var (Closure(): array{int, bool})|null the replay position and whether it is the live end */
    private ?Closure $position = null;

    private bool $liveEvaluation = false;

    private bool $markerRequested = false;

    private bool $markerFlushed = false;

    /**
     * @param array<string, true> $signalIds
     * @param array<string, true> $commandIds
     */
    private function __construct(
        private readonly array $signalIds,
        private readonly array $commandIds,
        private readonly ?int $markerSequence,
    ) {
    }

    /**
     * @param list<array<string, mixed>> $history
     * @param array<string, mixed> $task
     */
    public static function fromHistory(array $history, array $task): self
    {
        $signalIds = [];
        $commandIds = [];
        $markerSequence = null;
        foreach ($history as $event) {
            $type = $event['event_type'] ?? $event['type'] ?? null;
            $payload = is_array($event['payload'] ?? null) ? $event['payload'] : [];
            if ($type === 'SignalApplied'
                || ($type === 'SignalReceived' && self::recordedSequence($payload['workflow_sequence'] ?? null))) {
                self::remember($signalIds, $commandIds, $payload['signal_id'] ?? null,
                    $payload['workflow_command_id'] ?? $event['workflow_command_id'] ?? null);
            } elseif ($markerSequence === null && self::isMarker($type, $payload)) {
                $markerSequence = $payload['sequence'];
            }
        }
        if (($task['resume_source_kind'] ?? null) === 'workflow_signal') {
            self::remember($signalIds, $commandIds, $task['workflow_signal_id'] ?? $task['resume_source_id'] ?? null,
                $task['workflow_command_id'] ?? null);
        }

        return new self($signalIds, $commandIds, $markerSequence);
    }

    /** @param array<string, mixed> $payload */
    public static function isMarker(mixed $type, array $payload): bool
    {
        return $type === 'VersionMarkerRecorded' && ($payload['change_id'] ?? null) === self::CHANGE_ID
            && is_int($payload['sequence'] ?? null) && $payload['sequence'] > 0;
    }

    /** @internal The Replayer reports its position: the next authored sequence and whether it is the live end. */
    public function bind(Closure $position): void
    {
        $this->position = $position;
    }

    /**
     * @internal Evaluate a condition predicate whose outcome is not recorded.
     *
     * @template T
     * @param Closure(): T $evaluation
     * @return T
     */
    public function live(Closure $evaluation): mixed
    {
        $previous = $this->liveEvaluation;
        $this->liveEvaluation = true;
        try {
            return $evaluation();
        } finally {
            $this->liveEvaluation = $previous;
        }
    }

    /**
     * Keep the received signals workflow code observes, in history order.
     *
     * @param array<int, array<string, mixed>> $received `SignalReceived` events of one name by history position
     * @return array<int, array<string, mixed>>
     */
    public function select(array $received): array
    {
        $applied = array_filter($received, fn (array $event): bool => $this->observes($event));
        if (count($applied) === count($received)) {
            return $received;
        }

        return $this->appliedOnly() || $this->requestMarker() ? $applied : $received;
    }

    /** @param array<string, mixed> $event a `SignalReceived` event */
    public function observes(array $event): bool
    {
        $payload = is_array($event['payload'] ?? null) ? $event['payload'] : [];
        $signalId = self::identity($payload['signal_id'] ?? null);
        $commandId = self::identity($payload['workflow_command_id'] ?? $event['workflow_command_id'] ?? null);

        return self::recordedSequence($payload['workflow_sequence'] ?? null)
            || ($signalId === null && $commandId === null)
            || ($signalId !== null && isset($this->signalIds[$signalId]))
            || ($commandId !== null && isset($this->commandIds[$commandId]));
    }

    /**
     * @internal Whether this replay must record the marker it has not sent yet.
     *
     * @phpstan-impure
     */
    public function takeMarker(): bool
    {
        if (!$this->markerRequested || $this->markerFlushed) {
            return false;
        }
        $this->markerFlushed = true;

        return true;
    }

    private function appliedOnly(): bool
    {
        if ($this->markerRequested) {
            return true;
        }
        if ($this->markerSequence === null) {
            return false;
        }
        [$sequence, $live] = $this->position();

        return $live || $sequence >= $this->markerSequence;
    }

    /** Record the marker at a live decision of a history without one. */
    private function requestMarker(): bool
    {
        if ($this->markerSequence !== null || !$this->position()[1]) {
            return false;
        }
        $this->markerRequested = true;

        return true;
    }

    /** @return array{int, bool} */
    private function position(): array
    {
        [$sequence, $end] = $this->position === null ? [PHP_INT_MAX, false] : ($this->position)();

        return [$sequence, $end || ($this->liveEvaluation && $this->position !== null)];
    }

    /**
     * @param array<string, true> $signalIds
     * @param array<string, true> $commandIds
     */
    private static function remember(array &$signalIds, array &$commandIds, mixed $signalId, mixed $commandId): void
    {
        if (($signalId = self::identity($signalId)) !== null) {
            $signalIds[$signalId] = true;
        }
        if (($commandId = self::identity($commandId)) !== null) {
            $commandIds[$commandId] = true;
        }
    }

    private static function identity(mixed $value): ?string
    {
        return (is_string($value) && $value !== '') || is_int($value) ? (string) $value : null;
    }

    private static function recordedSequence(mixed $value): bool
    {
        return (is_int($value) && $value > 0) || (is_string($value) && preg_match('/\A[1-9]\d*\z/', $value) === 1);
    }
}
