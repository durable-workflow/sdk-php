<?php

declare(strict_types=1);

namespace DurableWorkflow\Qualification;

use Closure;
use DurableWorkflow\Transport\BoundedTransport;

/** Private, temporary turn receipt. Authentication headers are never retained. */
final class DeferredChildAllTurnTransport implements BoundedTransport
{
    /** @var array<string, mixed>|null */
    private ?array $task = null;

    public function __construct(
        private readonly BoundedTransport $http,
        private readonly string $receipt,
        private readonly Closure $completed,
    ) {
    }

    public function supportsBoundedRequests(): bool
    {
        return $this->http->supportsBoundedRequests();
    }

    public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
    {
        return $this->observe($uri, $body, $this->http->send($method, $uri, $headers, $body));
    }

    public function sendBounded(string $method, string $uri, array $headers, ?array $body, int $timeoutSeconds): ?array
    {
        return $this->observe($uri, $body, $this->http->sendBounded($method, $uri, $headers, $body, $timeoutSeconds));
    }

    /**
     * @param array<string, mixed>|null $body
     * @param array<string, mixed>|null $response
     * @return array<string, mixed>|null
     */
    private function observe(string $uri, ?array $body, ?array $response): ?array
    {
        if (str_ends_with($uri, '/workflow-tasks/poll') && is_array($response['task'] ?? null)) {
            $this->task = array_intersect_key($response['task'], array_flip([
                'task_id', 'workflow_id', 'run_id', 'task_queue', 'arguments',
            ]));
        }
        if (preg_match('~/workflow-tasks/([^/]+)/complete$~', $uri, $match) === 1) {
            DeferredChildAllScenario::check(($response['recorded'] ?? null) === true, 'Worker turn was not recorded.');
            DeferredChildAllScenario::check(($this->task['task_id'] ?? null) === rawurldecode($match[1]), 'Completion has no observed task.');
            $bytes = json_encode(['task' => $this->task, 'body' => $body, 'response' => $response], JSON_THROW_ON_ERROR);
            DeferredChildAllScenario::check(file_put_contents($this->receipt, $bytes, LOCK_EX) === strlen($bytes), 'Could not retain the private turn receipt.');
            ($this->completed)();
        }

        return $response;
    }
}
