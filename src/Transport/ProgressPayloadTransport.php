<?php

declare(strict_types=1);

namespace DurableWorkflow\Transport;

/** Optional progress checkpoints for an ordinary streamed payload download. */
interface ProgressPayloadTransport extends PayloadTransport
{
    /**
     * @param array<string, string> $headers
     * @param \Closure(): void $progress Exceptions interrupt the transfer unchanged.
     */
    public function fetchPayloadWithProgress(string $uri, array $headers, int $maxBytes, \Closure $progress): string;
}
