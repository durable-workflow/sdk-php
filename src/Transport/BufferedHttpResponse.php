<?php

declare(strict_types=1);

namespace DurableWorkflow\Transport;

use WpOrg\Requests\Transport;

/** @internal Supplies completed bytes to Requests' mature framing/content decoder. */
final class BufferedHttpResponse implements Transport
{
    public function __construct(private readonly string $response) {}

    /**
     * @param array<string, mixed> $headers
     * @param string|array<mixed> $data
     * @param array<string, mixed> $options
     */
    public function request($url, $headers = [], $data = [], $options = []): string
    {
        return $this->response;
    }

    /** @param array<mixed> $requests @param array<string, mixed> $options @return array<mixed> */
    public function request_multiple($requests, $options): array
    {
        throw new \LogicException('A buffered HTTP response represents one completed request.');
    }

    public static function test($capabilities = []): bool
    {
        return true;
    }
}
