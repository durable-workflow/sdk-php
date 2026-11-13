<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests\Integration;

use DurableWorkflow\Transport\BoundedTransport;
use DurableWorkflow\Transport\Psr18Transport;

/** Observes only acknowledged presence from the independent lease helper. */
final class ActivityLeaseObservationTransport implements BoundedTransport
{
    private readonly Psr18Transport $inner;

    public function __construct(private readonly string $path)
    {
        $this->inner = new Psr18Transport();
    }

    public function supportsBoundedRequests(): bool { return $this->inner->supportsBoundedRequests(); }

    public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
    {
        return $this->inner->send($method, $uri, $headers, $body);
    }

    public function sendBounded(string $method, string $uri, array $headers, ?array $body, int $timeoutSeconds): ?array
    {
        $reply = $this->inner->sendBounded($method, $uri, $headers, $body, $timeoutSeconds);
        if (str_ends_with($uri, '/worker/heartbeat') && ($reply['acknowledged'] ?? false) === true) {
            $temporary = $this->path.'.tmp';
            file_put_contents($temporary, json_encode([
                'worker_id' => $reply['worker_id'] ?? null, 'acknowledged' => true,
            ], JSON_THROW_ON_ERROR), LOCK_EX);
            chmod($temporary, 0600);
            rename($temporary, $this->path);
        }

        return $reply;
    }
}
