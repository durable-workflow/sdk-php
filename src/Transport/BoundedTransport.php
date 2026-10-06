<?php

declare(strict_types=1);

namespace DurableWorkflow\Transport;

/** A transport that can bound the complete request, including connection and body reads. */
interface BoundedTransport extends Transport
{
    public function supportsBoundedRequests(): bool;

    /**
     * @param array<string, string> $headers
     * @param array<string, mixed>|null $body
     * @return array<string, mixed>|list<mixed>|null
     */
    public function sendBounded(string $method, string $uri, array $headers, ?array $body, int $timeoutSeconds): ?array;
}
