<?php

declare(strict_types=1);

namespace DurableWorkflow\Transport;

/** Optional payload download capability with a complete request time bound. */
interface BoundedPayloadTransport extends PayloadTransport
{
    /** @param array<string, string> $headers */
    public function fetchPayloadBounded(string $uri, array $headers, int $maxBytes, int $timeoutSeconds): string;
}
