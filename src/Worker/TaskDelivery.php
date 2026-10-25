<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use Closure;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TaskDeliveryUncertain;
use DurableWorkflow\Transport\PollTransport;

/** Per-Client registration and unresolved delivery state, shared by its clones. */
final class TaskDelivery
{
    public const CAPABILITY = 'task_delivery_offers';
    public const SCHEMA = 'durable-workflow.task-delivery.v1';
    public const HEADER = 'X-Durable-Workflow-Task-Delivery';
    public const VERSION = 'offer-accept.v1';

    /** @var array<string, array{queue: string, token: string}> */
    private array $registrations = [];
    /** @var array<string, TaskDeliveryAttempt> */
    private array $pending = [];

    /**
     * @param list<string> $capabilities
     * @param array<string, mixed> $response
     */
    public function registered(string $workerId, string $queue, string $protocol, array $capabilities, array $response): void
    {
        $server = $response['server_capabilities'] ?? [];
        $feature = is_array($server) ? ($server['task_delivery'] ?? null) : null;
        if ($protocol !== '1.20' || !in_array(self::CAPABILITY, $capabilities, true)
            || $feature === null || (is_array($feature) && ($feature['supported'] ?? null) === false)) {
            foreach ($this->pending as $attempt) {
                if ($attempt->workerId === $workerId) {
                    throw new ServerException('An unresolved delivery cannot change transport modes.', 201, 'invalid_task_delivery_capability');
                }
            }
            $this->forget($workerId);
            return;
        }
        $token = $response['registration_token'] ?? null;
        if (!is_array($feature) || array_is_list($feature) || ($feature['supported'] ?? null) !== true
            || ($feature['schema'] ?? null) !== self::SCHEMA || ($feature['version'] ?? null) !== self::VERSION
            || ($feature['header'] ?? null) !== self::HEADER || ($feature['worker_capability'] ?? null) !== self::CAPABILITY
            || ($feature['minimum_protocol_version'] ?? null) !== '1.20' || ($feature['endpoint_suffix'] ?? null) !== '/offers'
            || ($feature['unaccepted_offer_reserves_task'] ?? null) !== false
            || !is_int($feature['acceptance_timeout_seconds'] ?? null)
            || $feature['acceptance_timeout_seconds'] < 1 || $feature['acceptance_timeout_seconds'] > 10
            || !is_string($token) || preg_match('/^[a-f0-9]{32}$/D', $token) !== 1) {
            throw new ServerException('The task-delivery registration contract is invalid.', 201, 'invalid_task_delivery_capability');
        }
        if (($this->registrations[$workerId] ?? null) !== ['queue' => $queue, 'token' => $token]) {
            $this->forget($workerId);
        }
        $this->registrations[$workerId] = ['queue' => $queue, 'token' => $token];
    }

    /**
     * @param array<string, mixed> $body
     * @param Closure(string, array<string, mixed>, ?TaskDeliveryAttempt): array<string, mixed> $send
     * @return array<string, mixed>|null Null means negotiation never selected this mode.
     */
    public function poll(string $path, array $body, Closure $send): ?array
    {
        $workerId = (string) $body['worker_id'];
        $registration = $this->registrations[$workerId] ?? null;
        if ($registration === null || $registration['queue'] !== ($body['task_queue'] ?? null)) {
            return null;
        }
        $key = hash('sha256', json_encode([$path, $workerId, $body['task_queue'], $body['poll_request_id']], JSON_THROW_ON_ERROR));
        $selected = $this->pending[$key] ?? null;
        try {
            if ($selected === null) {
                $wirePath = substr($path, 0, -5).'/offers';
                $body['registration_token'] = $registration['token'];
                $response = $send($wirePath, $body, null);
                if (!array_key_exists('task_offer', $response)) {
                    return $response;
                }
                $candidate = TaskDeliveryAttempt::fromResponse($wirePath, $body, $response,
                    (string) PollTransport::taskKind('POST', $path));
                // Concurrent callers for one logical poll select only its first packet.
                $selected = $this->pending[$key] ??= $candidate;
            }
            $response = $send($selected->path, $selected->requestBody(), $selected);
            if ($response['task'] === null && !PollResponse::isTerminal($response)) {
                throw new TaskDeliveryUncertain((string) $body['poll_request_id'], 200);
            }
            $this->resolved($key, $selected);

            return $response;
        } catch (ServerException $exception) {
            if ($this->definitiveRefusal($exception)) {
                $this->resolved($key, $selected);
                $response = $exception->details ?? [];
                if ($exception->reason === 'worker_registration_lost_authority') {
                    $this->forget($workerId);
                } else {
                    $response['poll_status'] = 'empty';
                }

                return $response;
            }
            if ($exception instanceof TaskDeliveryUncertain) {
                throw $exception;
            }
            if (($selected !== null && ($exception->status >= 500 || $exception->status === 409 || $exception->status === 429
                    || $exception->reason === 'invalid_task_delivery_response'
                    || in_array($exception->status, [0, 404, 405], true)))
                || ($selected === null && in_array($exception->status, [404, 405], true))) {
                if (PollResponse::isTransientPayloadFailure($exception)) {
                    throw $exception;
                }
                throw new TaskDeliveryUncertain((string) $body['poll_request_id'], $exception->status, $exception);
            }

            throw $exception;
        }
    }

    /**
     * Validate before payload hydration can initialize a claim supervisor.
     * @param array<string, mixed> $response
     * @param array<string, mixed> $body
     */
    public static function assertResponse(array $response, array $body, string $path, ?TaskDeliveryAttempt $selected): void
    {
        $task = $response['task'] ?? null;
        $kind = PollTransport::taskKind('POST', $path);
        $idField = match ($kind) {
            'query' => 'query_task_id', 'update-validation' => 'update_validation_task_id', default => 'task_id',
        };
        if (!array_key_exists('task', $response)
            || (($selected !== null || $task !== null) && array_key_exists('task_offer', $response))
            || (($response['poll_status'] ?? null) === 'offered' && !array_key_exists('task_offer', $response))
            || ($task !== null && (!is_array($task) || array_is_list($task)
                || !is_string($task[$idField] ?? null) || $task[$idField] === ''
                || ($task['lease_owner'] ?? null) !== $body['worker_id']
                || ($selected !== null && $task[$idField] !== $selected->taskId)
                || (isset($task['task_kind']) && $task['task_kind'] !== str_replace('-', '_', (string) $kind))))) {
            throw new ServerException('The task-delivery response is invalid.', 200, 'invalid_task_delivery_response');
        }
    }

    public function forget(string $workerId): void
    {
        unset($this->registrations[$workerId]);
        foreach ($this->pending as $key => $attempt) {
            if ($attempt->workerId === $workerId) {
                unset($this->pending[$key]);
            }
        }
    }

    /** @return array<string, int> */
    public function __debugInfo(): array
    {
        return ['registered_workers' => count($this->registrations), 'unresolved_deliveries' => count($this->pending)];
    }

    private function definitiveRefusal(ServerException $exception): bool
    {
        $response = $exception->details;

        return $exception->status === 409 && $response !== null && !array_is_list($response)
            && array_key_exists('task', $response) && $response['task'] === null
            && ($response['reason'] ?? null) === $exception->reason
            && in_array($exception->reason, ['task_offer_expired', 'worker_registration_lost_authority'], true);
    }

    private function resolved(string $key, ?TaskDeliveryAttempt $selected): void
    {
        if (($this->pending[$key] ?? null) === $selected) {
            unset($this->pending[$key]);
        }
    }
}
