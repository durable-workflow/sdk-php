<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Exception\ActivityHeartbeatResumeUnavailable;

/**
 * @internal Heartbeat-resume metadata captured from one negotiated activity poll.
 *
 * Server lists `activity_heartbeat_resume: {supported: true, version: 1}` in
 * `server_capabilities` and, for a worker that registered the capability, adds
 * `last_heartbeat_details` and `last_heartbeat_at` to each activity claim.
 * A claim without both fields, or a response without the capability, is
 * unavailable rather than an invented empty checkpoint.
 */
final class ActivityHeartbeatResume
{
    public const CAPABILITY = 'activity_heartbeat_resume';

    private const IDENTITY = ['task_id', 'activity_execution_id', 'activity_attempt_id', 'workflow_id', 'run_id', 'namespace'];

    /** @param list<mixed> $identity */
    private function __construct(
        public readonly bool $available,
        public readonly ?ActivityHeartbeatCheckpoint $checkpoint,
        private readonly array $identity,
        private readonly bool $invalid,
    ) {
    }

    public static function unavailable(): self
    {
        return new self(false, null, [], false);
    }

    /**
     * Capture the claim's resume state. A malformed negotiated claim is retained
     * as invalid, so the attempt is refused before its handler runs.
     *
     * @param array<string, mixed> $response The complete activity poll response.
     */
    public static function fromPoll(array $response, string $namespace, string $workerId, string $taskQueue): self
    {
        $task = $response['task'] ?? null;
        $capabilities = $response['server_capabilities'] ?? null;
        $capability = is_array($capabilities) ? ($capabilities[self::CAPABILITY] ?? null) : null;
        if (!is_array($task) || !is_array($capability) || ($capability['supported'] ?? null) !== true
            || ($capability['version'] ?? null) !== 1) {
            return self::unavailable();
        }
        $hasDetails = array_key_exists('last_heartbeat_details', $task);
        $hasAt = array_key_exists('last_heartbeat_at', $task);
        if (!$hasDetails && !$hasAt) {
            return self::unavailable();
        }
        $identity = self::identity($task);
        if ($identity === null || !$hasDetails || !$hasAt || $task['namespace'] !== $namespace
            || ($task['lease_owner'] ?? null) !== $workerId
            || (array_key_exists('task_queue', $task) && $task['task_queue'] !== $taskQueue)) {
            return new self(false, null, [], true);
        }
        $details = $task['last_heartbeat_details'];
        $at = $task['last_heartbeat_at'];
        if ($details === null && $at === null) {
            return new self(true, null, $identity, false);
        }
        if ($details === null || $task['attempt_number'] < 2) {
            // Server v1 resumes only an earlier attempt's recorded details.
            return new self(false, null, [], true);
        }
        try {
            $checkpoint = ActivityHeartbeatCheckpoint::received($details, $at);
        } catch (\UnexpectedValueException) {
            return new self(false, null, [], true);
        }

        return new self(true, $checkpoint, $identity, false);
    }

    /**
     * Bind this state to the claim it was captured from.
     *
     * @param array<string, mixed> $task
     * @throws \UnexpectedValueException When the negotiated claim is malformed.
     */
    public function forClaim(array $task): self
    {
        if ($this->invalid || ($this->available && self::identity($task) !== $this->identity)) {
            throw ActivityHeartbeatCheckpoint::invalid();
        }

        return $this;
    }

    /** @throws ActivityHeartbeatResumeUnavailable */
    public function checkpoint(): ?ActivityHeartbeatCheckpoint
    {
        if (!$this->available) {
            throw new ActivityHeartbeatResumeUnavailable();
        }

        return $this->checkpoint;
    }

    /**
     * @param array<string, mixed> $task
     * @return list<mixed>|null
     */
    private static function identity(array $task): ?array
    {
        $values = [];
        foreach (self::IDENTITY as $field) {
            $value = $task[$field] ?? null;
            if (!is_string($value) || trim($value) === '') {
                return null;
            }
            $values[] = $value;
        }
        $attempt = $task['attempt_number'] ?? null;
        if (!is_int($attempt) || $attempt < 1) {
            return null;
        }
        $values[] = $attempt;
        $values[] = $task['lease_owner'] ?? null;
        $values[] = $task['activity_type'] ?? null;
        $values[] = $task['task_queue'] ?? null;

        return $values;
    }
}
