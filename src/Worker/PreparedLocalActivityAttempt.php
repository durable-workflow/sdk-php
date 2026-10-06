<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DateTimeImmutable;
use InvalidArgumentException;

/** @internal Original admission, fixed execution budgets and acknowledged heartbeat deadline. */
final class PreparedLocalActivityAttempt
{
    private const DEADLINES = ['start_to_close_deadline_at', 'schedule_to_close_deadline_at'];

    /** @var array<string, mixed>|null Original scope fence, never renewed callback authority. */
    private ?array $scopeFence = null;

    /**
     * @param array<string, string|null> $deadlines
     * @param array<string, string>|null $cleanup
     */
    private function __construct(
        public readonly string $taskId,
        public readonly string $runId,
        public readonly string $leaseOwner,
        public readonly int $workflowTaskAttempt,
        public readonly string $executionId,
        public readonly string $attemptId,
        public readonly string $workerAttemptId,
        public readonly int $attemptNumber,
        private readonly array $deadlines,
        private ?string $heartbeatDeadline,
        private readonly ?int $heartbeatTimeout,
        private readonly ?array $cleanup,
        public readonly string $cancellationScopeId,
    ) {
    }

    /**
     * @param array<string, mixed> $response
     * @param array<string, string>|null $expectedCleanup
     */
    public static function fromPreparation(
        array $response, string $taskId, string $runId, string $owner, int $epoch, string $nonce,
        ?int $heartbeatTimeout = null, ?array $expectedCleanup = null,
        string $cancellationScopeId = 'root',
    ): self
    {
        if ($epoch < 1 || trim($taskId) === '' || trim($runId) === '' || trim($owner) === '' || trim($nonce) === ''
            || trim($cancellationScopeId) === ''
            || ($response['prepared'] ?? null) !== true || !is_bool($response['duplicate'] ?? null)
            || !array_key_exists('reason', $response) || $response['reason'] !== null
            || ($response['workflow_task_id'] ?? null) !== $taskId
            || ($response['workflow_task_attempt'] ?? null) !== $epoch
            || ($response['lease_owner'] ?? null) !== $owner
            || ($response['worker_attempt_id'] ?? null) !== $nonce
            || !is_int($response['attempt_number'] ?? null) || $response['attempt_number'] < 1) {
            throw new InvalidArgumentException('Local admission does not match its original workflow claim and worker nonce.');
        }
        if ($heartbeatTimeout !== null && $heartbeatTimeout < 1) {
            throw new InvalidArgumentException('Application heartbeat timeout must be positive.');
        }
        self::validateCleanup($response, $expectedCleanup, $cancellationScopeId);
        $serverTime = self::timestamp($response['server_time'] ?? null);
        $cleanupDeadline = self::cleanupDeadline($expectedCleanup);
        if ($cleanupDeadline !== null && $cleanupDeadline <= $serverTime) {
            throw new InvalidArgumentException('Local cleanup admission has exhausted its original budget.');
        }
        if (self::timestamp($response['lease_expires_at'] ?? null) <= $serverTime) {
            throw new InvalidArgumentException('Local admission has no live callback lease.');
        }
        if ($cleanupDeadline !== null && self::timestamp($response['lease_expires_at']) > $cleanupDeadline) {
            throw new InvalidArgumentException('Local admission exceeds its original cleanup authority ceiling.');
        }
        $deadlines = [];
        foreach ([...self::DEADLINES, 'heartbeat_deadline_at'] as $field) {
            if (!array_key_exists($field, $response) || ($response[$field] !== null && !is_string($response[$field]))
                || ($response[$field] !== null && self::timestamp($response[$field]) <= $serverTime)) {
                throw new InvalidArgumentException('Local admission has an absent or elapsed execution deadline.');
            }
            if ($cleanupDeadline !== null && $response[$field] !== null
                && self::timestamp($response[$field]) > $cleanupDeadline) {
                throw new InvalidArgumentException('Local admission exceeds the original cancellation deadline.');
            }
            $deadlines[$field] = $response[$field];
        }
        $heartbeatDeadline = $deadlines['heartbeat_deadline_at'];
        unset($deadlines['heartbeat_deadline_at']);
        if (($heartbeatTimeout !== null && $heartbeatDeadline === null)
            || ($heartbeatTimeout === null && ($cleanupDeadline === null
                ? $heartbeatDeadline !== null
                : self::timestamp($heartbeatDeadline) != $cleanupDeadline))) {
            throw new InvalidArgumentException('Local admission does not match the authored application heartbeat timeout.');
        }
        if ($heartbeatTimeout !== null && self::timestamp($heartbeatDeadline) > $serverTime->modify('+'.$heartbeatTimeout.' seconds')) {
            throw new InvalidArgumentException('Local admission exceeds the authored application heartbeat timeout.');
        }

        return new self($taskId, $runId, $owner, $epoch, self::text($response, 'activity_execution_id'),
            self::text($response, 'activity_attempt_id'), $nonce, $response['attempt_number'], $deadlines,
            $heartbeatDeadline, $heartbeatTimeout, $expectedCleanup, $cancellationScopeId);
    }

    /**
     * Validate the accepted scope membership and immutable stop fact. This
     * observation cannot renew a lease or authorize workflow cleanup.
     * @param array<string, mixed> $response
     */
    public function scopedCancellation(array $response): ScopedCancellationContext
    {
        $this->validateIdentity($response);
        $snapshot = $response['cancellation_scope'] ?? null;
        $fields = ['schema', 'workflow_run_id', 'scope_id', 'request_id',
            'request_history_event_id', 'cancellation', 'authority_deadline_at'];
        if (($response['active'] ?? null) !== false || ($response['stop_required'] ?? null) !== true
            || ($response['renewed'] ?? null) !== false || ($response['fenced'] ?? null) !== true
            || !in_array($response['reason'] ?? null, ['cancellation_scope_requested', 'cancellation_scope_deadline_expired'], true)
            || ($response['cancellation_request'] ?? null) !== null
            || !is_array($snapshot) || count($snapshot) !== count($fields)
            || array_diff($fields, array_keys($snapshot)) !== []
            || $snapshot['schema'] !== 'durable-workflow.activity-scope-cancellation/v1'
            || $snapshot['workflow_run_id'] !== $this->runId
            || $snapshot['scope_id'] !== $this->cancellationScopeId || !is_array($snapshot['cancellation'])) {
            throw new InvalidArgumentException('Prepared scope stop does not fence its authored membership.');
        }
        self::text($response, 'history_refresh_page_token');
        $eventId = self::text($response, 'cancellation_history_event_id');
        self::text($snapshot, 'request_history_event_id');
        $context = ScopedCancellationContext::fromArray($snapshot['cancellation']);
        $deadline = self::timestamp($snapshot['authority_deadline_at']);
        if ($context->workflowRunId !== $this->runId || $context->scopeId !== $this->cancellationScopeId
            || $snapshot['request_id'] !== $context->requestId
            || $deadline > $context->deadline() || $deadline <= $context->requestedAt()) {
            throw new InvalidArgumentException('Prepared scope stop changed its original request or deadline.');
        }
        $fact = [...$snapshot, 'cancellation' => $context->toArray(),
            'authority_deadline_at' => $deadline->format('U.u'), 'cancellation_history_event_id' => $eventId];
        ksort($fact);
        if ($this->scopeFence !== null && $fact !== $this->scopeFence) {
            throw new InvalidArgumentException('Prepared scope stop replaced its accepted cancellation fact.');
        }
        $this->scopeFence = $fact;

        return $context;
    }

    /** @param array<string, mixed> $response */
    public function validateControl(array $response, bool $renew): void
    {
        if (($response['heartbeat_recorded'] ?? false) !== false || ($response['heartbeat_history_event_id'] ?? null) !== null) {
            throw new InvalidArgumentException('Supervisor control cannot acknowledge an application heartbeat.');
        }
        $this->validateObservation($response, $renew, false);
    }

    /**
     * Only an actual application-heartbeat receipt may advance its deadline.
     * @param array<string, mixed> $response
     */
    public function validateHeartbeat(array $response): void
    {
        if (!is_bool($response['heartbeat_recorded'] ?? null)
            || $response['heartbeat_recorded'] !== ($response['active'] ?? null)
            || ($response['heartbeat_recorded']
                ? !is_string($response['heartbeat_history_event_id'] ?? null) || trim($response['heartbeat_history_event_id']) === ''
                : ($response['heartbeat_history_event_id'] ?? null) !== null)) {
            throw new InvalidArgumentException('Application heartbeat requires its canonical event receipt.');
        }
        $this->validateObservation($response, false, true);
        if ($response['active']) {
            // Mutate only after validating the entire acknowledgment. A rejected reply cannot advance authority.
            $this->heartbeatDeadline = $response['heartbeat_deadline_at'];
        }
    }

    /** @param array<string, mixed> $response */
    private function validateObservation(array $response, bool $renew, bool $applicationHeartbeat): void
    {
        $this->validateIdentity($response);
        self::validateCleanup($response, $this->cleanup, $this->cancellationScopeId);
        $cleanupDeadline = self::cleanupDeadline($this->cleanup);
        if (!is_bool($response['active'] ?? null) || !is_bool($response['renewed'] ?? null)
            || !is_bool($response['stop_required'] ?? null)
            || ($response['lease_owner'] ?? null) !== $this->leaseOwner
            || $response['active'] === $response['stop_required']
            || !array_key_exists('reason', $response)
            || ($response['active'] && ($response['reason'] !== null || $response['renewed'] !== $renew))
            || (!$response['active'] && (!is_string($response['reason']) || $response['reason'] === '' || $response['renewed']))) {
            throw new InvalidArgumentException('Local control does not establish live authority or an explicit stop.');
        }
        $serverTime = self::timestamp($response['server_time'] ?? null);
        foreach ($this->deadlines as $field => $original) {
            if (!array_key_exists($field, $response)
                || ($original === null ? $response[$field] !== null :
                    self::timestamp($response[$field]) != self::timestamp($original))) {
                throw new InvalidArgumentException('Local control changed an original execution deadline.');
            }
        }
        if (!array_key_exists('heartbeat_deadline_at', $response)) {
            throw new InvalidArgumentException('Local control omitted the application heartbeat deadline.');
        }
        $heartbeatDeadline = $response['heartbeat_deadline_at'];
        if ($applicationHeartbeat && $response['active'] && $this->heartbeatTimeout !== null) {
            $updated = self::timestamp($heartbeatDeadline);
            if (self::timestamp($this->heartbeatDeadline) <= $serverTime
                || $updated < self::timestamp($this->heartbeatDeadline)
                || $updated > $serverTime->modify('+'.$this->heartbeatTimeout.' seconds')
                || ($cleanupDeadline !== null && $updated > $cleanupDeadline)) {
                throw new InvalidArgumentException('Application heartbeat changed a fixed budget or exceeded its timeout.');
            }
        } elseif ($this->heartbeatDeadline === null ? $heartbeatDeadline !== null :
            self::timestamp($heartbeatDeadline) != self::timestamp($this->heartbeatDeadline)) {
            throw new InvalidArgumentException('Local control changed an unacknowledged application heartbeat deadline.');
        }
        if ($response['active']) {
            foreach (['lease_expires_at', 'workflow_lease_expires_at'] as $field) {
                if (self::timestamp($response[$field] ?? null) <= $serverTime) {
                    throw new InvalidArgumentException('Local control returned an expired activity or workflow lease.');
                }
            }
            if ($cleanupDeadline !== null && self::timestamp($response['lease_expires_at']) > $cleanupDeadline) {
                throw new InvalidArgumentException('Local control exceeds its original cleanup authority ceiling.');
            }
            foreach ([...array_values($this->deadlines), $heartbeatDeadline,
                $this->cleanup['cleanup_deadline_at'] ?? null, $this->cleanup['authority_deadline_at'] ?? null] as $deadline) {
                if ($deadline !== null && self::timestamp($deadline) <= $serverTime) {
                    throw new InvalidArgumentException('Local control returned active after an execution deadline.');
                }
            }
        }
    }

    /**
     * @param array<string, mixed> $response
     * @param array<string, mixed>|null $expected
     */
    private static function validateCleanup(array $response, ?array $expected, string $operationScope): void
    {
        $actual = $response['cancellation_cleanup'] ?? null;
        if ($expected === null) {
            if ($actual !== null) {
                throw new InvalidArgumentException('Local receipt invented cancellation cleanup authority.');
            }
            return;
        }
        $fields = ['request_id', 'root_request_id', 'delivery_history_event_id', 'cleanup_deadline_at'];
        if (array_key_exists('scope_id', $expected)) {
            $fields = [...$fields, 'scope_id', 'operation_scope_id', 'preparation_history_event_id', 'authority_deadline_at'];
            if (($expected['scope_id'] ?? null) === 'root'
                || ($expected['operation_scope_id'] ?? null) !== $operationScope
                || ($response['cancellation_scope_id'] ?? $operationScope) !== $operationScope
                || self::timestamp($expected['authority_deadline_at'] ?? null) > self::timestamp($expected['cleanup_deadline_at'] ?? null)) {
                throw new InvalidArgumentException('Local cleanup changed its original authored scope or authority ceiling.');
            }
        }
        if (!is_array($actual) || count($actual) !== count($fields) || count($expected) !== count($fields)) {
            throw new InvalidArgumentException('Local receipt changed canonical cancellation cleanup authority.');
        }
        foreach ($fields as $field) {
            $original = self::text($expected, $field);
            $value = self::text($actual, $field);
            if (in_array($field, ['cleanup_deadline_at', 'authority_deadline_at'], true)
                ? self::timestamp($original) != self::timestamp($value) : $original !== $value) {
                throw new InvalidArgumentException('Local receipt changed canonical cancellation cleanup authority.');
            }
        }
    }

    /** @param array<string, string>|null $cleanup */
    private static function cleanupDeadline(?array $cleanup): ?DateTimeImmutable
    {
        if ($cleanup === null) {
            return null;
        }
        $deadline = self::timestamp($cleanup['cleanup_deadline_at']);
        $authority = isset($cleanup['scope_id']) ? self::timestamp($cleanup['authority_deadline_at']) : $deadline;

        return $authority < $deadline ? $authority : $deadline;
    }

    /** @param array<string, mixed> $response */
    public function validateOutcome(array $response): void
    {
        $this->validateIdentity($response);
        $kind = $response['event_type'] ?? null;
        if (($response['recorded'] ?? null) !== true || !is_bool($response['duplicate'] ?? null)
            || ($response['workflow_run_id'] ?? null) !== $this->runId
            || ($response['worker_attempt_id'] ?? null) !== $this->workerAttemptId
            || !array_key_exists('reason', $response) || $response['reason'] !== null
            || !in_array($kind, ['ActivityCompleted', 'ActivityFailed', 'ActivityTimedOut', 'ActivityRetryScheduled'], true)
            || ($response['claim_released'] ?? null) !== ($kind === 'ActivityRetryScheduled')
            || !is_array($response['created_task_ids'] ?? null) || !array_is_list($response['created_task_ids'])
            || count($response['created_task_ids']) !== ($kind === 'ActivityRetryScheduled' ? 1 : 0)) {
            throw new InvalidArgumentException('Local outcome does not prove a canonical receipt for the original attempt.');
        }
        self::text($response, 'event_id');
        self::timestamp($response['recorded_at'] ?? null);
        foreach ($response['created_task_ids'] as $id) {
            self::text(['id' => $id], 'id');
        }
    }

    /** @param array<string, mixed> $response */
    private function validateIdentity(array $response): void
    {
        if (($response['activity_execution_id'] ?? null) !== $this->executionId
            || ($response['activity_attempt_id'] ?? null) !== $this->attemptId
            || ($response['workflow_task_id'] ?? null) !== $this->taskId
            || ($response['workflow_task_attempt'] ?? null) !== $this->workflowTaskAttempt
            || (array_key_exists('lease_owner', $response) && $response['lease_owner'] !== $this->leaseOwner)) {
            throw new InvalidArgumentException('Local receipt belongs to a different attempt or workflow claim.');
        }
    }

    /** @param array<string, mixed> $value */
    private static function text(array $value, string $field): string
    {
        $text = $value[$field] ?? null;
        if (!is_string($text) || trim($text) === '') {
            throw new InvalidArgumentException('Local receipt identity must be a non-empty string.');
        }
        return $text;
    }

    private static function timestamp(mixed $value): DateTimeImmutable
    {
        if (!is_string($value) || preg_match('/\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})\z/', $value) !== 1) {
            throw new InvalidArgumentException('Local receipt timestamps require an ISO date, time and timezone.');
        }
        $timestamp = new DateTimeImmutable($value);
        $errors = DateTimeImmutable::getLastErrors();
        if ($errors !== false && ($errors['warning_count'] !== 0 || $errors['error_count'] !== 0)) {
            throw new InvalidArgumentException('Local receipt timestamp is invalid.');
        }
        return $timestamp;
    }
}
