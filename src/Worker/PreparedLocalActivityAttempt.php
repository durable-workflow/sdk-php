<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DateTimeImmutable;
use InvalidArgumentException;

/** @internal Immutable admission identity and execution deadlines issued by the runtime. */
final class PreparedLocalActivityAttempt
{
    private const DEADLINES = ['start_to_close_deadline_at', 'schedule_to_close_deadline_at', 'heartbeat_deadline_at'];

    /** @param array<string, string|null> $deadlines */
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
    ) {
    }

    /** @param array<string, mixed> $response */
    public static function fromPreparation(array $response, string $taskId, string $runId, string $owner, int $epoch, string $nonce): self
    {
        if ($epoch < 1 || trim($taskId) === '' || trim($runId) === '' || trim($owner) === '' || trim($nonce) === ''
            || ($response['prepared'] ?? null) !== true || !is_bool($response['duplicate'] ?? null)
            || !array_key_exists('reason', $response) || $response['reason'] !== null
            || ($response['workflow_task_id'] ?? null) !== $taskId
            || ($response['workflow_task_attempt'] ?? null) !== $epoch
            || ($response['lease_owner'] ?? null) !== $owner
            || ($response['worker_attempt_id'] ?? null) !== $nonce
            || !is_int($response['attempt_number'] ?? null) || $response['attempt_number'] < 1) {
            throw new InvalidArgumentException('Local admission does not match its original workflow claim and worker nonce.');
        }
        $serverTime = self::timestamp($response['server_time'] ?? null);
        if (self::timestamp($response['lease_expires_at'] ?? null) <= $serverTime) {
            throw new InvalidArgumentException('Local admission has no live callback lease.');
        }
        $deadlines = [];
        foreach (self::DEADLINES as $field) {
            if (!array_key_exists($field, $response) || ($response[$field] !== null && !is_string($response[$field]))
                || ($response[$field] !== null && self::timestamp($response[$field]) <= $serverTime)) {
                throw new InvalidArgumentException('Local admission has an absent or elapsed execution deadline.');
            }
            $deadlines[$field] = $response[$field];
        }

        return new self($taskId, $runId, $owner, $epoch, self::text($response, 'activity_execution_id'),
            self::text($response, 'activity_attempt_id'), $nonce, $response['attempt_number'], $deadlines);
    }

    /** @param array<string, mixed> $response */
    public function validateControl(array $response, bool $renew): void
    {
        $this->validateIdentity($response);
        if (!is_bool($response['active'] ?? null) || !is_bool($response['renewed'] ?? null)
            || !is_bool($response['stop_required'] ?? null)
            || ($response['lease_owner'] ?? null) !== $this->leaseOwner
            || $response['active'] === $response['stop_required']
            || !array_key_exists('reason', $response)
            || ($response['active'] && ($response['reason'] !== null || ($renew && !$response['renewed'])))
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
        if ($response['active']) {
            foreach (['lease_expires_at', 'workflow_lease_expires_at'] as $field) {
                if (self::timestamp($response[$field] ?? null) <= $serverTime) {
                    throw new InvalidArgumentException('Local control returned an expired activity or workflow lease.');
                }
            }
            foreach ($this->deadlines as $deadline) {
                if ($deadline !== null && self::timestamp($deadline) <= $serverTime) {
                    throw new InvalidArgumentException('Local control returned active after an execution deadline.');
                }
            }
        }
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
