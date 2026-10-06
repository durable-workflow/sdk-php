<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use LogicException;

/** @internal A complete authored all group awaiting admission or local supervision. */
final class PreparedLocalActivityGroup
{
    /**
     * @param list<array<string, mixed>> $commands Complete admission batch, empty after canonical scheduling.
     * @param list<PreparedLocalActivityCall> $calls Only unresolved local members.
     */
    public function __construct(
        public readonly int $baseSequence,
        public readonly int $size,
        public readonly array $commands,
        public readonly array $calls,
        public readonly bool $committed,
    ) {
        if ($baseSequence < 1 || $size < 1 || $size > 100 || $calls === []
            || count($commands) !== ($committed ? 0 : $size)) {
            throw new LogicException('Prepared local groups require a complete bounded authored all group.');
        }
        $sequences = [];
        foreach ($calls as $call) {
            if ($call->sequence < $baseSequence
                || $call->sequence >= $baseSequence + $size || isset($sequences[$call->sequence])
                || ($call->command->attributes['parallel_group_path'] ?? []) === []) {
                throw new LogicException('Prepared local group members require distinct authored sequences and paths.');
            }
            $sequences[$call->sequence] = true;
        }
    }
}
