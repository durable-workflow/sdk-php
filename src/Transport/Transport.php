<?php

declare(strict_types=1);

namespace DurableWorkflow\Transport;

interface Transport
{
    /**
     * @param array<string, string> $headers
     * @param array<string, mixed>|null $body
     * @return array<string, mixed>|list<mixed>|null
     */
    public function send(string $method, string $uri, array $headers, ?array $body = null): ?array;
}
