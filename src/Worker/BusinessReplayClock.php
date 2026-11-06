<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DateTimeImmutable;
use DateTimeZone;
use DurableWorkflow\Exception\ReplayClockUnavailable;

/** @internal Business time is separate from cancellation's monotonic budget clock. */
final class BusinessReplayClock
{
    private const ACTIVITY_TERMINALS = ['ActivityCompleted', 'ActivityFailed', 'ActivityTimedOut', 'ActivityCancelled'];

    /** @var array<int, list<int>> */
    private array $bySequence = [];

    private ?DateTimeImmutable $time = null;
    private bool $active = false;
    private bool $redriveClaimed = false;
    private ?string $sourceRun = null;
    private ?int $resumeStep = null;
    private ?DateTimeImmutable $redriveInserted = null;

    /** @param list<array<string, mixed>> $history */
    public function __construct(private readonly array $history, private readonly string $workflowId, private readonly string $runId)
    {
        $starts = [];
        foreach ($history as $index => $event) {
            if (self::kind($event) === 'WorkflowStarted') {
                $starts[] = $index;
            }
            $payload = $event['payload'] ?? null;
            if (is_array($payload) && ((self::kind($event) === 'WorkflowStarted' && ($payload['recovery_kind'] ?? null) === 'redrive')
                || (self::kind($event) === 'StartAccepted' && ($payload['outcome'] ?? null) === 'redriven'))) {
                $this->redriveClaimed = true;
            }
            if (is_array($payload) && ($sequence = self::sequence($payload)) !== null) {
                $this->bySequence[$sequence][] = $index;
            }
        }
        if (count($starts) === 1) {
            $this->initializeRedrive($starts[0], $workflowId, $runId);
        }
    }

    public function time(): DateTimeImmutable
    {
        if (!$this->active || $this->time === null) {
            throw new ReplayClockUnavailable('Business replay time requires an active, validated consumed boundary.');
        }

        return $this->time;
    }

    /** The engine leaves this scope before suspension teardown can run finally.
     * @template TResult
     * @param callable(): TResult $callback
     * @return TResult
     */
    public function invoke(callable $callback): mixed
    {
        $this->active = true;
        try {
            return $callback();
        } finally {
            $this->active = false;
        }
    }

    /** Every resume resets authority, including unsupported or unrecorded calls.
     * @param array<string, mixed>|null $step The step actually matched by the executor.
     */
    public function consume(?array $step): void
    {
        $this->time = null;
        if (!self::identity($this->workflowId) || !self::identity($this->runId)
            || ($this->redriveClaimed && $this->sourceRun === null)
            || $step === null || !in_array($step['shape'] ?? null, ['activity', 'timer'], true)
            || !is_int($step['sequence'] ?? null) || !is_int($step['resolution_order'] ?? null)) {
            return;
        }
        $index = $step['resolution_order'];
        $event = $this->history[$index] ?? null;
        if (!is_array($event) || !$this->scalarBoundary($index, $step['shape'], $step['sequence'])) {
            return;
        }
        $payload = $event['payload'];
        $reusedPrefix = $this->sourceRun !== null && $this->resumeStep !== null && $step['sequence'] < $this->resumeStep;
        if (in_array(self::kind($event), ['ActivityCompleted', 'TimerFired'], true)
            && ($reusedPrefix || array_key_exists('reused_recorded_at', $payload) || array_key_exists('reused_from_run_id', $payload))) {
            $this->time = $this->reusedTime($payload, $step['shape'], $step['sequence']);
        } elseif (!$reusedPrefix) {
            $this->time = self::recordedTime($event);
        }
    }

    /** @param array<string, mixed> $payload */
    private function reusedTime(array $payload, string $family, int $sequence): ?DateTimeImmutable
    {
        if ($this->sourceRun === null || ($payload['reused_from_run_id'] ?? null) !== $this->sourceRun
            || $this->resumeStep === null || $sequence >= $this->resumeStep) {
            return null;
        }
        if ($family === 'activity' && (!self::identity($payload['reused_activity_execution_id'] ?? null)
            || $payload['reused_activity_execution_id'] === $payload['activity_execution_id'])) {
            return null;
        }
        $original = self::parseTime($payload['reused_recorded_at'] ?? null);

        return $original !== null && $this->redriveInserted !== null && $original <= $this->redriveInserted ? $original : null;
    }

    private function initializeRedrive(int $index, string $workflowId, string $runId): void
    {
        $payload = $this->history[$index]['payload'] ?? null;
        if (!is_array($payload) || ($payload['recovery_kind'] ?? null) !== 'redrive'
            || !$this->startPrefix($index, $payload)) {
            return;
        }
        if (!$this->redriveIdentity($payload, $workflowId, $runId) || !$this->orderedHistory()) {
            return;
        }
        $inserted = self::recordedTime($this->history[$index]);
        $original = self::parseTime($payload['replayed_started_at'] ?? null);
        if ($inserted === null || $original === null || $original > $inserted) {
            return;
        }
        $this->sourceRun = $payload['continued_from_run_id'];
        $this->resumeStep = $payload['resume_step_sequence'];
        $this->redriveInserted = $inserted;
        $this->time = $original;
    }

    /** @param array<string, mixed> $payload */
    private function redriveIdentity(array $payload, string $workflowId, string $runId): bool
    {
        foreach (['workflow_instance_id', 'workflow_run_id', 'workflow_command_id', 'continued_from_run_id', 'workflow_link_id'] as $key) {
            if (!self::identity($payload[$key] ?? null)) {
                return false;
            }
        }

        return $workflowId !== '' && $runId !== '' && $payload['workflow_instance_id'] === $workflowId
            && $payload['workflow_run_id'] === $runId && $payload['continued_from_run_id'] !== $runId
            && is_int($payload['resume_step_sequence'] ?? null) && $payload['resume_step_sequence'] >= 1;
    }

    private function orderedHistory(): bool
    {
        foreach ($this->history as $position => $event) {
            if (($event['sequence'] ?? null) !== $position + 1) {
                return false;
            }
        }

        return true;
    }

    /** The shared StartAccepted prefix proves correlation, never producer identity.
     * @param array<string, mixed> $payload
     */
    private function startPrefix(int $index, array $payload): bool
    {
        if ($index === 0) {
            return true;
        }
        $accepted = $index === 1 && self::kind($this->history[0]) === 'StartAccepted'
            ? ($this->history[0]['payload'] ?? null) : null;
        if (!is_array($accepted) || ($accepted['outcome'] ?? null) !== 'redriven') {
            return false;
        }

        return $this->acceptedIdentity($accepted, $payload);
    }

    /** @param array<string, mixed> $accepted
     * @param array<string, mixed> $payload
     */
    private function acceptedIdentity(array $accepted, array $payload): bool
    {
        foreach (['workflow_instance_id', 'workflow_run_id', 'workflow_command_id'] as $key) {
            if (!self::identity($payload[$key] ?? null) || ($accepted[$key] ?? null) !== $payload[$key]) {
                return false;
            }
        }

        return true;
    }

    private function scalarBoundary(int $index, string $family, int $sequence): bool
    {
        $event = $this->history[$index];
        $payload = $event['payload'] ?? null;
        if (!is_array($payload) || self::sequence($payload) !== $sequence) {
            return false;
        }
        $terminals = $family === 'activity' ? self::ACTIVITY_TERMINALS : ['TimerFired'];
        $resource = $family === 'activity' ? 'activity_execution_id' : 'timer_id';
        $identity = $payload[$resource] ?? null;
        if (!in_array(self::kind($event), $terminals, true) || !self::identity($identity)) {
            return false;
        }
        return $this->scalarLifecycle($index, $family, $sequence, $resource, $identity, $terminals, $payload);
    }

    /** @param list<string> $terminals
     * @param array<string, mixed> $payload
     */
    private function scalarLifecycle(int $index, string $family, int $sequence, string $resource, string $identity, array $terminals, array $payload): bool
    {
        $opening = $family === 'activity' ? 'ActivityScheduled' : 'TimerScheduled';
        $allowed = [$opening, ...$terminals];
        if ($family === 'activity') {
            $allowed[] = 'ActivityStarted';
        }
        $positions = [];
        foreach ($this->bySequence[$sequence] ?? [] as $position) {
            $row = $this->history[$position];
            $body = $row['payload'];
            $kind = self::kind($row);
            if ($position > $index || !in_array($kind, $allowed, true) || ($body[$resource] ?? null) !== $identity
                || !$this->scalarRow($body, $family)) {
                return false;
            }
            $positions[$kind][] = $position;
        }
        $openings = $positions[$opening] ?? [];
        $starts = $positions['ActivityStarted'] ?? [];
        $closures = array_merge(...array_values(array_intersect_key($positions, array_flip($terminals))));
        $reusedActivity = $family === 'activity' && self::kind($this->history[$index]) === 'ActivityCompleted'
            && array_key_exists('reused_recorded_at', $payload) && $this->sourceRun !== null;

        return $this->scalarOrder($openings, $starts, $closures, $index, $reusedActivity);
    }

    /** @param list<int> $openings
     * @param list<int> $starts
     * @param list<int> $closures
     */
    private function scalarOrder(array $openings, array $starts, array $closures, int $index, bool $reusedActivity): bool
    {
        if ($closures !== [$index] || count($starts) > 1 || ($starts !== [] && ($openings === [] || $starts[0] < $openings[0]))) {
            return false;
        }

        return (count($openings) === 1 && $openings[0] < $index) || ($reusedActivity && $openings === [] && $starts === []);
    }

    /** @param array<string, mixed> $body */
    private function scalarRow(array $body, string $family): bool
    {
        if (!in_array($body['execution_mode'] ?? null, [null, 'remote'], true) || ($body['local_activity'] ?? false) !== false
            || ($body['parallel_group'] ?? null) !== null || !in_array($body['parallel_group_path'] ?? null, [null, []], true)
            || ($body['selection_group'] ?? null) !== null || ($body['selection_group_id'] ?? null) !== null
            || ($body['parallel_group_id'] ?? null) !== null || ($body['parallel_group_base_sequence'] ?? null) !== null
            || ($body['parallel_group_size'] ?? null) !== null || ($body['cancellation_cleanup'] ?? null) !== null
            || !in_array($body['cancellation_scope_id'] ?? null, [null, 'root'], true)) {
            return false;
        }

        return $family !== 'timer' || (in_array($body['timer_kind'] ?? null, [null, 'sleep', 'durable_timer'], true)
            && !array_key_exists('condition_wait_id', $body) && !array_key_exists('signal_wait_id', $body));
    }

    /** @param array<string, mixed> $event */
    private static function kind(array $event): ?string
    {
        if (array_key_exists('event_type', $event) && array_key_exists('type', $event) && $event['event_type'] !== $event['type']) {
            return null;
        }
        $kind = $event['event_type'] ?? $event['type'] ?? null;

        return is_string($kind) && $kind !== '' ? $kind : null;
    }

    /** @param array<string, mixed> $payload */
    private static function sequence(array $payload): ?int
    {
        $value = $payload['sequence'] ?? null;
        if (!is_int($value) || $value < 1 || (array_key_exists('workflow_sequence', $payload) && $payload['workflow_sequence'] !== $value)) {
            return null;
        }

        return $value;
    }

    private static function identity(mixed $value): bool
    {
        return is_string($value) && trim($value) !== '';
    }

    /** Public-history timestamp is authoritative when present, including invalid values.
     * @param array<string, mixed> $event
     */
    private static function recordedTime(array $event): ?DateTimeImmutable
    {
        return self::parseTime(array_key_exists('timestamp', $event) ? $event['timestamp'] : ($event['recorded_at'] ?? null));
    }

    private static function parseTime(mixed $value): ?DateTimeImmutable
    {
        if (!is_string($value) || preg_match('/\A\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,6})?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)\z/', $value) !== 1) {
            return null;
        }
        try {
            $time = new DateTimeImmutable($value);
        } catch (\Exception) {
            return null;
        }
        $errors = DateTimeImmutable::getLastErrors();
        return $errors !== false && ($errors['warning_count'] !== 0 || $errors['error_count'] !== 0)
            ? null : $time->setTimezone(new DateTimeZone('UTC'));
    }
}
