<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests\Support;

use DurableWorkflow\Transport\Transport;

final class FakeTransport implements Transport
{
    /** @var list<array{method: string, uri: string, headers: array<string, string>, body: ?array<string, mixed>}> */
    public array $requests = [];
    /** @var list<array<string, mixed>|null> */
    private array $responses;

    /** @param list<array<string, mixed>|null> $responses */
    public function __construct(array $responses = [])
    {
        $this->responses = $responses;
    }

    public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
    {
        $this->requests[] = compact('method', 'uri', 'headers', 'body');

        return array_shift($this->responses);
    }
}
