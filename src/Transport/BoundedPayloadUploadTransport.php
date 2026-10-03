<?php

declare(strict_types=1);

namespace DurableWorkflow\Transport;

/** Optional payload upload capability that bounds transmission and response reads. */
interface BoundedPayloadUploadTransport extends PayloadUploadTransport
{
    /**
     * @param array<string, string> $headers
     * @return array<string, mixed>
     */
    public function uploadPayloadBounded(string $uri, array $headers, string $blob, int $timeoutSeconds): array;
}
