<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use Closure;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TaskDeliveryUncertain;
use DurableWorkflow\Transport\PollTransport;
use InvalidArgumentException;

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
    /** @var array<string, list<string>> */
    private array $accepting = [];

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
        $allowedKinds = self::requestedKinds($path, $body);
        if (array_key_exists('task_kinds', $body)) {
            $body['task_kinds'] = $allowedKinds;
        }
        $key = hash('sha256', json_encode([$path, $workerId, $body['task_queue'], $body['poll_request_id']], JSON_THROW_ON_ERROR));
        $selected = $this->pending[$key] ?? null;
        if (($selected !== null && !$selected->isForKinds($allowedKinds))
            || (isset($this->accepting[$key]) && $this->accepting[$key] !== $allowedKinds)) {
            throw new InvalidArgumentException('A logical task poll cannot change its requested task kinds.');
        }
        if (isset($this->accepting[$key])) {
            return ['task' => null];
        }
        // Hold ownership across both requests, including a suspended offer read.
        $this->accepting[$key] = $allowedKinds;
        try {
            $body['registration_token'] = $registration['token'];

            return $this->deliver($key, substr($path, 0, -5).'/offers', $body, $allowedKinds, $selected, $send);
        } finally {
            unset($this->accepting[$key]);
        }
    }

    /**
     * @param array<string, mixed> $body
     * @param list<string> $allowedKinds
     * @param Closure(string, array<string, mixed>, ?TaskDeliveryAttempt): array<string, mixed> $send
     * @return array<string, mixed>
     */
    private function deliver(string $key, string $wirePath, array $body, array $allowedKinds, ?TaskDeliveryAttempt $selected, Closure $send): array
    {
        try {
            if ($selected === null) {
                $response = $send($wirePath, $body, null);
                $this->assertCurrentRegistration($body);
                if (!array_key_exists('task_offer', $response)) {
                    return $response;
                }
                $candidate = TaskDeliveryAttempt::fromResponse($wirePath, $body, $response, $allowedKinds);
                $selected = $this->pending[$key] ??= $candidate;
            }
            $response = $send($selected->path, $selected->requestBody(), $selected);
            $this->assertCurrentRegistration($selected->requestBody());
            if ($response['task'] === null) {
                throw new TaskDeliveryUncertain((string) $body['poll_request_id'], 200);
            }
        } catch (ServerException $exception) {
            $response = $selected === null ? null : $this->refusedResponse($exception, $selected->requestBody());
            if ($response === null) {
                if ($this->isUncertain($exception, $selected)) {
                    throw new TaskDeliveryUncertain((string) $body['poll_request_id'], $exception->status, $exception);
                }
                throw $exception;
            }
        }
        $this->resolved($key, $selected);

        return $response;
    }

    /** @internal Recheck after I/O and before observing or returning its response.
     * @param array<string, mixed> $body
     */
    public function assertCurrentRegistration(array $body): void
    {
        $current = $this->registrations[(string) $body['worker_id']] ?? null;
        if ($current === null || $current !== ['queue' => $body['task_queue'] ?? null, 'token' => $body['registration_token'] ?? null]) {
            throw new ServerException('The task-delivery registration changed before its response.', 200,
                'stale_task_delivery_registration');
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
        $kind = $selected?->taskKind;
        $idField = match ($kind) {
            'query' => 'query_task_id', 'update_validation' => 'update_validation_task_id', default => 'task_id',
        };
        if (!array_key_exists('task', $response)
            || (($selected !== null || $task !== null) && array_key_exists('task_offer', $response))
            || (($response['poll_status'] ?? null) === 'offered' && !array_key_exists('task_offer', $response))
            || ($task !== null && ($selected === null || !is_array($task) || array_is_list($task)
                || !is_string($task[$idField] ?? null) || $task[$idField] === ''
                || ($task['lease_owner'] ?? null) !== $body['worker_id']
                || $task[$idField] !== $selected->taskId
                || ($task['task_kind'] ?? null) !== $kind
                || !$selected->isForKinds(self::requestedKinds($path, $body))))) {
            throw new ServerException('The task-delivery response is invalid.', 200, 'invalid_task_delivery_response');
        }
    }

    /**
     * @param array<string, mixed> $body
     * @return list<string>
     */
    private static function requestedKinds(string $path, array $body): array
    {
        $kind = str_replace('-', '_', (string) PollTransport::taskKind('POST', $path));
        if (!in_array($kind, ['workflow', 'activity', 'query', 'update_validation'], true)) {
            throw new InvalidArgumentException('Task delivery requires a supported poll endpoint.');
        }
        $permitted = $kind === 'workflow' ? ['workflow', 'update_validation'] : [$kind];
        $requested = array_key_exists('task_kinds', $body) ? $body['task_kinds'] : [$kind];
        if (!is_array($requested) || !array_is_list($requested) || $requested === []) {
            throw new InvalidArgumentException('Task kinds must be a nonempty list allowed by the poll endpoint.');
        }
        $normalized = [];
        foreach ($requested as $value) {
            if (!is_string($value) || !in_array($value, $permitted, true)) {
                throw new InvalidArgumentException('Task kinds must be a nonempty list allowed by the poll endpoint.');
            }
            // Copy scalar values rather than retaining caller-owned array references.
            $normalized[] = $value;
        }
        $normalized = array_values(array_unique($normalized));
        sort($normalized, SORT_STRING);

        return $normalized;
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

    /**
     * @param array<string, mixed> $request
     * @return array<string, mixed>|null
     */
    private function refusedResponse(ServerException $exception, array $request): ?array
    {
        if (!TaskDeliveryRefusal::matches($exception)) {
            return null;
        }
        $response = $exception->details ?? [];
        if ($exception->reason === 'task_offer_expired') {
            $response['poll_status'] = 'empty';
        } else {
            $workerId = (string) $request['worker_id'];
            $current = $this->registrations[$workerId] ?? null;
            if ($current !== null && $current['token'] === ($request['registration_token'] ?? null)
                && $current['queue'] === ($request['task_queue'] ?? null)) {
                $this->forget($workerId);
            }
        }

        return $response;
    }

    private function isUncertain(ServerException $exception, ?TaskDeliveryAttempt $selected): bool
    {
        return !$exception instanceof TaskDeliveryUncertain && !PollResponse::isTransientPayloadFailure($exception)
            && ($selected !== null || in_array($exception->status, [404, 405], true));
    }

    private function resolved(string $key, ?TaskDeliveryAttempt $selected): void
    {
        if (($this->pending[$key] ?? null) === $selected) {
            unset($this->pending[$key]);
        }
    }
}
