<?php

declare(strict_types=1);

namespace DurableWorkflow\Transport;

/** Optional binary capability; existing JSON-only custom transports remain valid. */
interface PayloadTransport extends Transport
{
    /**
     * Fetch bounded bytes without following redirects and verify declared response metadata.
     * @param array<string, string> $headers
     */
    public function fetchPayload(string $uri, array $headers, int $maxBytes): string;
}
