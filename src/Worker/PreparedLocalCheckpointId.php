<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Transport\JsonDocument;

/**
 * @internal The idempotency id of one prepared-local checkpoint request.
 *
 * The runtime compares it with the receipt it retained for the same claim,
 * so a retry must compute the same id. The commands can carry floats (for
 * example search attribute values), so the id is hashed with round-trip
 * precision whatever the host's serialize_precision is (#282).
 */
final class PreparedLocalCheckpointId
{
    /** @param list<array<string, mixed>> $commands */
    public static function for(string $taskId, string $owner, int $epoch, int $startSequence, array $commands): string
    {
        return hash('sha256', JsonDocument::encodeWith([$taskId, $owner, $epoch, $startSequence, $commands], JSON_THROW_ON_ERROR));
    }
}
