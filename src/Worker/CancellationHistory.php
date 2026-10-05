<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DateTimeImmutable;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use InvalidArgumentException;

/** Observations identify a request. Only canonical history can authorize its delivery. */
final class CancellationHistory
{
    public const REQUEST_EVENT = 'CooperativeCancellationRequested';
    public const DELIVERY_EVENT = 'CooperativeCancellationDelivered';

    private const RESOLUTION_EVENTS = [
        'ActivityCompleted', 'ActivityFailed', 'ActivityCancelled', 'ActivityTimedOut',
        'TimerFired', 'TimerCancelled', 'ConditionWaitSatisfied', 'ConditionWaitTimedOut',
        'SignalApplied', 'ChildRunCompleted', 'ChildRunFailed', 'ChildRunCancelled', 'ChildRunTerminated',
    ];
    private const FAILURE_EVENTS = [
        'ActivityFailed', 'ActivityCancelled', 'ActivityTimedOut',
        'ChildRunFailed', 'ChildRunCancelled', 'ChildRunTerminated',
    ];

    /**
     * @param array<int, true> $resolvedBeforeRequest
     * @param array<int, true> $failedBeforeRequest
     * @param array<string, true> $selectedBeforeRequest
     */
    private function __construct(
        public readonly ?CancellationRequest $request,
        public readonly ?CancellationDelivery $delivery,
        public readonly int $requestIndex,
        public readonly ?int $deliveryIndex,
        private readonly array $resolvedBeforeRequest,
        private readonly array $failedBeforeRequest,
        private readonly array $selectedBeforeRequest,
    ) {
    }

    /** @param list<array<string, mixed>> $events */
    public static function fromEvents(
        array $events,
        string $runId = '',
        ?CancellationRequest $observation = null,
    ): self {
        $request = $observation;
        $requestIndex = count($events);
        $delivery = null;
        $deliveryIndex = null;
        $sawRequest = false;
        foreach ($events as $index => $event) {
            $kind = $event['event_type'] ?? $event['type'] ?? null;
            if (!in_array($kind, [self::REQUEST_EVENT, self::DELIVERY_EVENT], true)) {
                continue;
            }
            $payload = $event['payload'] ?? null;
            if (!is_array($payload) || array_is_list($payload)) {
                throw self::invalid('Canonical cancellation event is missing its payload.');
            }
            $requestId = $payload['workflow_command_id'] ?? null;
            $eventRun = $payload['workflow_run_id'] ?? null;
            if (!is_string($requestId) || trim($requestId) === ''
                || (array_key_exists('workflow_command_id', $event) && $event['workflow_command_id'] !== $requestId)
                || !is_string($eventRun) || trim($eventRun) === '' || ($runId !== '' && $eventRun !== $runId)) {
                throw self::invalid('Canonical cancellation identity does not match the event and workflow run.');
            }
            try {
                if ($kind === self::REQUEST_EVENT) {
                    if ($sawRequest || $delivery !== null) {
                        throw self::invalid('History must contain one request before delivery.');
                    }
                    $recordedAt = $event['recorded_at'] ?? $event['timestamp'] ?? null;
                    if (!is_string($recordedAt)) {
                        throw self::invalid('Canonical request must have a recorded timestamp.');
                    }
                    $canonical = CancellationRequest::fromHistoryPayload($payload, $recordedAt);
                    if ($observation !== null && ($canonical->requestId !== $observation->requestId
                        || new DateTimeImmutable($canonical->cleanupDeadlineAt) != new DateTimeImmutable($observation->cleanupDeadlineAt))) {
                        throw self::invalid('Observation changes the original request or cleanup deadline.');
                    }
                    $request = $canonical->context !== null
                        ? $canonical->withHistoryRefreshPageToken($observation?->historyRefreshPageToken)
                        : ($observation ?? $canonical);
                    $requestIndex = $index;
                    $sawRequest = true;
                } else {
                    if (!$sawRequest || $request === null || $delivery !== null) {
                        throw self::invalid('Delivery requires one earlier canonical request and one marker.');
                    }
                    $delivery = CancellationDelivery::fromPayload($payload);
                    if ($delivery->requestId !== $request->requestId) {
                        throw self::invalid('Delivery names a different cancellation request.', $delivery->sequence);
                    }
                    if (array_key_exists('cancellation', $payload)) {
                        $snapshot = $payload['cancellation'];
                        if (!is_array($snapshot) || array_is_list($snapshot) || $request->context === null
                            || CancellationContext::fromArray($snapshot)->toArray() !== $request->context->toArray()) {
                            throw self::invalid('Delivery changes the canonical cancellation context.', $delivery->sequence);
                        }
                    }
                    $deliveryIndex = $index;
                }
            } catch (InvalidArgumentException $error) {
                throw self::invalid($error->getMessage());
            }
        }

        if ($request === null) {
            return new self(null, null, $requestIndex, null, [], [], []);
        }

        [$resolved, $failed, $selected] = self::resolutionsBefore($events, $requestIndex);
        $state = new self($request, $delivery, $requestIndex, $deliveryIndex, $resolved, $failed, $selected);
        if ($delivery !== null && !$state->rangeEligible(
            $delivery->operationSequence ?? $delivery->sequence,
            $delivery->operationSequence === null ? $delivery->sequenceSpan : $delivery->operationSequenceSpan,
        )) {
            throw self::invalid('Delivery cannot replace an earlier committed result.', $delivery->sequence);
        }

        return $state;
    }

    /** @param list<array<string, mixed>> $events
     * @return array{array<int, true>, array<int, true>, array<string, true>}
     */
    private static function resolutionsBefore(array $events, int $requestIndex): array
    {
        $resolved = [];
        $failed = [];
        $selected = [];
        foreach ($events as $index => $event) {
            if ($index >= $requestIndex) {
                break;
            }
            $kind = $event['event_type'] ?? $event['type'] ?? null;
            $payload = $event['payload'] ?? null;
            if (!is_array($payload)) {
                continue;
            }
            if ($kind === 'SelectionResolved' || $kind === 'SelectionOperationCancelled') {
                $base = $payload[$kind === 'SelectionResolved' ? 'selection_group_base_sequence' : 'member_base_sequence'] ?? null;
                $span = $payload[$kind === 'SelectionResolved' ? 'selection_group_size' : 'member_size'] ?? null;
                if (is_int($base) && $base > 0 && is_int($span) && $span >= 1 && $span <= 1000
                    && $base <= PHP_INT_MAX - $span) {
                    if ($kind === 'SelectionResolved') {
                        $selected[$base.':'.$span] = true;
                    } else {
                        foreach (range($base, $base + $span - 1) as $sequence) {
                            $resolved[$sequence] = true;
                        }
                    }
                }
            }
            $sequence = $payload['sequence'] ?? null;
            if (is_int($sequence) && $sequence > 0 && in_array($kind, self::RESOLUTION_EVENTS, true)) {
                $resolved[$sequence] = true;
                if (in_array($kind, self::FAILURE_EVENTS, true)) {
                    $failed[$sequence] = true;
                }
            }
        }
        return [$resolved, $failed, $selected];
    }

    /** @internal Shared ordering rules for an already validated canonical request.
     * @param list<array<string, mixed>> $events
     */
    public static function rangeEligibleBefore(array $events, int $requestIndex, int $sequence, int $span = 1): bool
    {
        if ($requestIndex < 0 || $requestIndex >= count($events)) {
            throw new InvalidArgumentException('Cancellation eligibility needs its original canonical request index.');
        }
        [$resolved, $failed, $selected] = self::resolutionsBefore($events, $requestIndex);

        return (new self(null, null, $requestIndex, null, $resolved, $failed, $selected))->rangeEligible($sequence, $span);
    }

    public function eligible(int $sequence, int $span = 1): bool
    {
        return $this->request !== null && $this->delivery === null && $this->rangeEligible($sequence, $span);
    }

    private function rangeEligible(int $sequence, int $span): bool
    {
        if ($sequence < 1 || $span < 1 || $span > 1000 || $sequence > PHP_INT_MAX - $span
            || isset($this->selectedBeforeRequest[$sequence.':'.$span])) {
            return false;
        }
        $allResolved = true;
        foreach (range($sequence, $sequence + $span - 1) as $member) {
            if (isset($this->failedBeforeRequest[$member])) {
                return false;
            }
            if (!isset($this->resolvedBeforeRequest[$member])) {
                $allResolved = false;
            }
        }

        return !$allResolved;
    }

    private static function invalid(string $detail, ?int $sequence = null): NonDeterministicWorkflow
    {
        return new NonDeterministicWorkflow(
            $detail, $sequence, 'canonical cooperative cancellation', null, 'invalid_cooperative_cancellation_history',
        );
    }
}
