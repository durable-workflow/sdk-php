<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Codec\PayloadCodec;
use LogicException;

/** @internal An authored call awaiting durable admission, never an executed callback. */
final class PreparedLocalActivityCall
{
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

    /** @return array<string, mixed> */
    public function descriptor(PayloadCodec $codec): array
    {
        $attributes = $this->command->attributes;
        $arguments = $attributes['arguments_value'];
        unset($attributes['arguments_value']);
        $descriptor = ['type' => 'record_local_activity', ...$attributes,
            'arguments' => $codec->envelope($arguments), 'payload_codec' => $codec->name()];
        if ($this->cancellationCleanup !== null) {
            $descriptor['cancellation_cleanup'] = [
                'request_id' => $this->cancellationCleanup->requestId,
                'delivery_history_event_id' => $this->deliveryHistoryEventId,
            ];
        }
        json_encode($descriptor, JSON_THROW_ON_ERROR);

        return $descriptor;
    }

    /** @return array{request_id: string, root_request_id: string, delivery_history_event_id: string, cleanup_deadline_at: string}|null */
    public function cleanupSnapshot(): ?array
    {
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
