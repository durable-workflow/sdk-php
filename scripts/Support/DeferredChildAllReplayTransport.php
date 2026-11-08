<?php

declare(strict_types=1);

namespace DurableWorkflow\Qualification;

use DurableWorkflow\Transport\BoundedTransport;

/**
 * Sends a recorded workflow-task completion again, byte for byte (#185).
 *
 * The duplicate-completion check must repeat the exact request the managed
 * worker sent. Client::completeWorkflowTask() builds a fresh body without the
 * worker's other fields, which Native treats as a different (conflicting)
 * completion. Armed with the recorded task and body, this transport replaces
 * the next completion body for that task with the recorded one, so the Client
 * still supplies its normal authentication and protocol headers.
 */
final class DeferredChildAllReplayTransport implements BoundedTransport
{
    /** @var array{task_id: string, body: array<string, mixed>}|null */
    private ?array $armed = null;

    /** @var array<string, mixed>|null */
    private ?array $sent = null;

    public function __construct(private readonly BoundedTransport $http)
    {
    }

    /** @param array<string, mixed> $body */
    public function arm(string $taskId, array $body): void
    {
        $this->armed = ['task_id' => $taskId, 'body' => $body];
        $this->sent = null;
    }

    /** @return array<string, mixed>|null The body the last armed replay actually sent. */
    public function sent(): ?array
    {
        return $this->sent;
    }

    public function supportsBoundedRequests(): bool
    {
        return $this->http->supportsBoundedRequests();
    }

    public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
    {
        return $this->http->send($method, $uri, $headers, $this->replay($uri, $body));
    }

    public function sendBounded(string $method, string $uri, array $headers, ?array $body, int $timeoutSeconds): ?array
    {
        return $this->http->sendBounded($method, $uri, $headers, $this->replay($uri, $body), $timeoutSeconds);
    }

    /**
     * @param array<string, mixed>|null $body
     * @return array<string, mixed>|null
     */
    private function replay(string $uri, ?array $body): ?array
    {
        if ($this->armed === null) {
            return $body;
        }
        DeferredChildAllScenario::check(preg_match('~/workflow-tasks/([^/]+)/complete$~', $uri, $match) === 1
            && rawurldecode($match[1]) === $this->armed['task_id'], 'Duplicate replay targeted another request or task.');
        $this->sent = $this->armed['body'];
        $this->armed = null;

        return $this->sent;
    }
}
