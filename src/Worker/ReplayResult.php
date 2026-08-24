<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

final class ReplayResult
{
    /**
     * @param list<array<string, mixed>> $commands
     * @param list<array{stream_name: string, through_position: int}> $messageStreamCursors
     * @param list<array{stream_name: string, after_position: int}> $messageStreamWaits
     */
    public function __construct(
        public readonly array $commands,
        public readonly array $messageStreamCursors = [],
        public readonly array $messageStreamWaits = [],
    ) {
    }
}
