<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Codec\PayloadCodec;
use LogicException;

/** @internal An authored call awaiting durable admission, never an executed callback. */
final class PreparedLocalActivityCall
{
    /** @var array<string, string>|null Derived only from canonical scope history. */
    private ?array $scopeCleanup = null;

    public function __construct(
        public readonly WorkflowCommand $command,
        public readonly int $sequence,
        public readonly bool $recover,
        public readonly ?CancellationContext $cancellationCleanup = null,
        public readonly ?string $deliveryHistoryEventId = null,
    ) {
        if ($command->type !== 'record_local_activity' || $sequence < 1
            || array_key_exists('cancellation_cleanup', $command->attributes)
            || (($cancellationCleanup === null) !== ($deliveryHistoryEventId === null))
            || ($deliveryHistoryEventId !== null && trim($deliveryHistoryEventId) === '')) {
            throw new LogicException('Prepared local admission requires an authored local call and positive sequence.');
        }
    }

    /**
     * Source profile for cleanup in a committed delivery's frozen subtree,
     * including each scope's original context and narrower authority ceiling.
     *
     * @param list<array<string, mixed>> $history
     */
    public static function fromCommittedScopeDelivery(
        WorkflowCommand $command, int $sequence, bool $recover, array $history,
        string $runId, string $workflowId,
    ): self {
        $scopeId = $command->attributes['cancellation_scope_id'] ?? null;
        if (!is_string($scopeId) || trim($scopeId) === '' || $scopeId === 'root') {
            throw new LogicException('Scoped cleanup requires its original authored operation membership.');
        }
        $scopes = new CancellationScopeHistory($history, $runId);
        $committed = new CommittedCancellationScopeHistory($history, $runId, $workflowId, $scopes,
            inspectOperationProjections: true, allowPreparedLocalBoundary: true);
        foreach ($committed->deliveries as $delivery) {
            $state = $committed->scopeStatesForDelivery($delivery)[$scopeId] ?? null;
            if ($state === null) {
                continue;
            }
            $boundary = $delivery['boundary'];
            if ($sequence < $boundary->sequence + $boundary->sequenceSpan) {
                throw new LogicException('Scoped cleanup must follow its original delivered operation range.');
            }
            $context = $state['context'];
            $call = new self($command, $sequence, $recover);
            $call->scopeCleanup = CancellationScopeTimerCleanup::snapshot(
                $context, $state['authority_deadline_at'], $delivery['event'],
            );

            return $call;
        }
        throw new LogicException('Scoped cleanup requires a committed delivery for its original scope.');
    }

    /** @return array<string, mixed> */
    public function descriptor(PayloadCodec $codec): array
    {
        $attributes = $this->command->attributes;
        $arguments = $attributes['arguments_value'];
        unset($attributes['arguments_value']);
        $descriptor = ['type' => 'record_local_activity', ...$attributes,
            'arguments' => $codec->envelope($arguments), 'payload_codec' => $codec->name()];
        if ($this->scopeCleanup !== null) {
            $descriptor['cancellation_cleanup'] = [
                'scope_id' => $this->scopeCleanup['scope_id'],
                'request_id' => $this->scopeCleanup['request_id'],
                'delivery_history_event_id' => $this->scopeCleanup['delivery_history_event_id'],
            ];
        } elseif ($this->cancellationCleanup !== null) {
            $descriptor['cancellation_cleanup'] = [
                'request_id' => $this->cancellationCleanup->requestId,
                'delivery_history_event_id' => $this->deliveryHistoryEventId,
            ];
        }
        json_encode($descriptor, JSON_THROW_ON_ERROR);

        return $descriptor;
    }

    /** @return array<string, string>|null */
    public function cleanupSnapshot(): ?array
    {
        if ($this->scopeCleanup !== null) {
            return $this->scopeCleanup;
        }
        return $this->cancellationCleanup === null ? null : [
            'request_id' => $this->cancellationCleanup->requestId,
            'root_request_id' => $this->cancellationCleanup->rootRequestId,
            'delivery_history_event_id' => $this->deliveryHistoryEventId ?? throw new LogicException('Missing canonical delivery identity.'),
            'cleanup_deadline_at' => $this->cancellationCleanup->deadline()->format('Y-m-d\TH:i:s.u\Z'),
        ];
    }

    /** @param list<array<string, mixed>> $history */
    public static function needsRecovery(array $history, int $sequence): bool
    {
        $started = false;
        foreach ($history as $event) {
            if (($event['payload']['sequence'] ?? null) !== $sequence) {
                continue;
            }
            $kind = $event['event_type'] ?? $event['type'] ?? null;
            if ($kind === 'ActivityStarted') {
                $started = true;
            } elseif ($kind === 'ActivityRetryScheduled') {
                $started = false;
            }
        }

        return $started;
    }
}
