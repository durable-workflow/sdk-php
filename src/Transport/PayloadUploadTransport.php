<?php

declare(strict_types=1);

namespace DurableWorkflow\Transport;

/** Optional upload capability; download-only custom transports remain compatible. */
interface PayloadUploadTransport extends Transport
{
    /**
     * Upload encoded bytes without redirects; return a bounded JSON response.
     * @param array<string, string> $headers
     * @return array<string, mixed>
     */
    public function uploadPayload(string $uri, array $headers, string $blob, int $timeoutSeconds): array;
}
