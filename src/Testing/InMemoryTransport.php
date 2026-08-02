<?php

declare(strict_types=1);

namespace DurableWorkflow\Testing;

use DurableWorkflow\Transport\Transport;

/** @internal Minimal transport used to construct handler contexts in tests. */
final class InMemoryTransport implements Transport
{
    /** @var list<array{method: string, uri: string, body: ?array<string, mixed>}> */
    public array $requests = [];

    public function send(string $method, string $uri, array $headers, ?array $body = null): array
    {
        $this->requests[] = compact('method', 'uri', 'body');

        return ['acknowledged' => true, 'cancel_requested' => false, 'can_continue' => true];
    }
}
