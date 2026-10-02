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
    ) {
        if ($command->type !== 'record_local_activity' || $sequence < 1) {
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
        json_encode($descriptor, JSON_THROW_ON_ERROR);

        return $descriptor;
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
