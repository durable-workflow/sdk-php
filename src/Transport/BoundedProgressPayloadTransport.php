<?php

declare(strict_types=1);

namespace DurableWorkflow\Transport;

/** Optional finite payload streaming with supervision during transfer and idle I/O. */
interface BoundedProgressPayloadTransport extends ProgressPayloadTransport
{
    /** @param array<string, string> $headers */
    public function fetchPayloadWithBoundedProgress(string $uri, array $headers, int $maxBytes, \Closure $progress, ?int $timeoutSeconds = null): string;
}
