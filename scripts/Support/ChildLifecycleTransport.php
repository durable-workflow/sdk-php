<?php

declare(strict_types=1);

namespace DurableWorkflow\Qualification;

use Closure;
use DurableWorkflow\Transport\BoundedTransport;
use DurableWorkflow\Transport\Psr18Transport;

/**
 * Fixture transport for qualify-child-start-lifecycle.php.
 *
 * It records the committed child-start completion (the workflow-task
 * completion whose first command has await "started") without logging
 * authentication headers, then calls $started.
 *
 * A stop-after-start fixture worker exits right after that completion. On a
 * Server with carried polls, a workflow_poll carried on that completion would
 * lease the fresh parent start turn to this exiting worker in the same
 * transaction. The lifecycle scenario needs that turn to stay available for
 * its replacement worker, so with $withholdCarriedWorkflowPoll the transport
 * removes workflow_poll from that one completion before sending. The receipt
 * records the body actually sent, so a duplicate replays the same bytes. Every
 * other request is sent unchanged.
 */
final class ChildLifecycleTransport implements BoundedTransport
{
    private readonly BoundedTransport $http;

    public function __construct(
        private readonly string $receipt,
        private readonly Closure $started,
        private readonly bool $withholdCarriedWorkflowPoll = false,
        ?BoundedTransport $http = null,
    ) {
        $this->http = $http ?? new Psr18Transport();
    }

    public function supportsBoundedRequests(): bool
    {
        return $this->http->supportsBoundedRequests();
    }

    public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
    {
        $body = $this->prepare($uri, $body);

        return $this->observe($uri, $body, $this->http->send($method, $uri, $headers, $body));
    }

    public function sendBounded(string $method, string $uri, array $headers, ?array $body, int $timeoutSeconds): ?array
    {
        $body = $this->prepare($uri, $body);

        return $this->observe($uri, $body, $this->http->sendBounded($method, $uri, $headers, $body, $timeoutSeconds));
    }

    /** @param array<string, mixed>|null $body */
    public static function startCompletionTask(string $uri, ?array $body): ?string
    {
        if (preg_match('~/workflow-tasks/([^/]+)/complete$~', $uri, $match) !== 1
            || ($body['commands'][0]['await'] ?? null) !== 'started') {
            return null;
        }

        return rawurldecode($match[1]);
    }

    /**
     * @param array<string, mixed>|null $body
     * @return array<string, mixed>|null
     */
    private function prepare(string $uri, ?array $body): ?array
    {
        if ($this->withholdCarriedWorkflowPoll && is_array($body) && self::startCompletionTask($uri, $body) !== null) {
            unset($body['workflow_poll']);
        }

        return $body;
    }

    /**
     * @param array<string, mixed>|null $body
     * @param array<string, mixed>|list<mixed>|null $response
     * @return array<string, mixed>|list<mixed>|null
     */
    private function observe(string $uri, ?array $body, ?array $response): ?array
    {
        $taskId = self::startCompletionTask($uri, $body);
        if ($taskId !== null) {
            file_put_contents($this->receipt, json_encode([
                'task_id' => $taskId, 'body' => $body, 'response' => $response,
            ], JSON_THROW_ON_ERROR));
            ($this->started)();
        }

        return $response;
    }
}
