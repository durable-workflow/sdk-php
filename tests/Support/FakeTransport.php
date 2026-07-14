<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests\Support;

use DurableWorkflow\Transport\Transport;
use Throwable;

final class FakeTransport implements Transport
{
    /** @var list<array{method: string, uri: string, headers: array<string, string>, body: ?array<string, mixed>}> */
    public array $requests = [];
    /** @var list<array<string, mixed>|Throwable|null> */
    private array $responses;
    /** @var (\Closure(string, string, array<string, string>, ?array<string, mixed>): ?array<string, mixed>)|null */
    private readonly ?\Closure $handler;

    /**
     * @param list<array<string, mixed>|Throwable|null> $responses
     * @param (\Closure(string, string, array<string, string>, ?array<string, mixed>): ?array<string, mixed>)|null $handler
     */
    public function __construct(array $responses = [], ?\Closure $handler = null)
    {
        $this->responses = $responses;
        $this->handler = $handler;
    }

    public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
    {
        $this->requests[] = compact('method', 'uri', 'headers', 'body');

        $response = $this->handler !== null
            ? ($this->handler)($method, $uri, $headers, $body)
            : array_shift($this->responses);
        if ($response instanceof Throwable) {
            throw $response;
        }

        return $response;
    }
}
