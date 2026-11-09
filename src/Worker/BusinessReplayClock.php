<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DateTimeImmutable;
use Closure;
use DurableWorkflow\Exception\ReplayClockUnavailable;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use Throwable;

/** @internal Business time is separate from cancellation's monotonic budget clock. */
final class BusinessReplayClock
{
    private const ACTIVITY_TERMINALS = ['ActivityCompleted', 'ActivityFailed', 'ActivityTimedOut', 'ActivityCancelled'];

    private const CHILD_SHAPE = 'child_workflow';

    /** @var array<int, list<int>> */
    private array $bySequence = [];

    private ?DateTimeImmutable $time = null;
    private bool $active = false;
    private bool $redriveClaimed = false;
    private ?string $sourceRun = null;
    private ?int $resumeStep = null;
    private ?DateTimeImmutable $redriveInserted = null;
    private int $unavailableReads = 0;

    /** @param list<array<string, mixed>> $history */
    public function __construct(private readonly array $history, private readonly string $workflowId, private readonly string $runId)
    {
        $starts = [];
        foreach ($history as $index => $event) {
            if (ReplayClockHistory::kind($event) === 'WorkflowStarted') {
                $starts[] = $index;
            }
            $payload = $event['payload'] ?? null;
            if (is_array($payload) && ((ReplayClockHistory::kind($event) === 'WorkflowStarted' && ($payload['recovery_kind'] ?? null) === 'redrive')
                || (ReplayClockHistory::kind($event) === 'StartAccepted' && ($payload['outcome'] ?? null) === 'redriven'))) {
                $this->redriveClaimed = true;
            }
            if (is_array($payload) && ($sequence = ReplayClockHistory::legacySequenceClaim($payload)) !== null) {
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
            $this->noteUnavailableRead();
            throw new ReplayClockUnavailable('Business replay time requires an active, validated consumed boundary.');
        }

        return $this->time;
    }

    /** Workflow code asked for business time that this replay could not supply. */
    public function noteUnavailableRead(): void
    {
        $this->unavailableReads++;
    }

    /** Continuation speculation must not depend on a time the server has not assigned yet. */
    public function unavailableReads(): int
    {
        return $this->unavailableReads;
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
        if (!ReplayClockHistory::identity($this->workflowId) || !ReplayClockHistory::identity($this->runId)
            || ($this->redriveClaimed && $this->sourceRun === null)
            || $step === null || !in_array($step['shape'] ?? null, ['activity', 'timer', self::CHILD_SHAPE, 'condition_wait'], true)
            || !is_int($step['sequence'] ?? null) || !is_int($step['resolution_order'] ?? null)) {
            return;
        }
        $index = $step['resolution_order'];
        $event = $this->history[$index] ?? null;
        if (!is_array($event) || !$this->matchesBoundary($index, $step['shape'], $step['sequence'])) {
            return;
        }
        $payload = $event['payload'];
        $reusedPrefix = $this->sourceRun !== null && $this->resumeStep !== null && $step['sequence'] < $this->resumeStep;
        if (in_array($step['shape'], ['activity', 'timer'], true)
            && in_array(ReplayClockHistory::kind($event), ['ActivityCompleted', 'TimerFired'], true)
            && ($reusedPrefix || array_key_exists('reused_recorded_at', $payload) || array_key_exists('reused_from_run_id', $payload))) {
            $this->time = $this->reusedTime($payload, $step['shape'], $step['sequence']);
        } elseif (!$reusedPrefix) {
            $this->time = ReplayClockHistory::recordedTime($event);
        }
    }

    private function matchesBoundary(int $index, string $family, int $sequence): bool
    {
        return match ($family) {
            self::CHILD_SHAPE => ChildReplayClockEvidence::matches($this->history, $this->bySequence[$sequence] ?? [], $index, $sequence),
            'condition_wait' => ConditionReplayClockEvidence::matches($this->history, $index, $sequence),
            default => $this->scalarBoundary($index, $family, $sequence),
        };
    }

    /** @param list<array{step: array<string, mixed>, path: list<array<string, mixed>>, offset: int}> $members
     * @param Closure(array<string, mixed>, list<array<string, mixed>>): bool $pathMatches
     */
    public function consumeGroup(array $members, int $size, Closure $pathMatches, ?int $failedOrder = null): void
    {
        $this->time = null;
        if (!ReplayClockHistory::identity($this->workflowId) || !ReplayClockHistory::identity($this->runId)
            || ($this->redriveClaimed && $this->sourceRun === null)
            || ($failedOrder === null && count($members) !== $size)
            || !ChildReplayClockEvidence::uniqueGroupCalls($this->history, $members)) {
            return;
        }
        $candidates = [];
        foreach ($members as $member) {
            if ($failedOrder !== null && !($member['step']['failure'] instanceof Throwable)) {
                continue;
            }
            $time = $this->groupMemberTime($member['step'], $member['path'], $pathMatches);
            if ($time === null) {
                return;
            }
            $candidates[] = ['time' => $time, 'offset' => $member['offset'], 'order' => $member['step']['resolution_order']];
        }
        $this->time = GroupReplayClockEvidence::time($candidates, $failedOrder);
    }

    /** @param array<string, mixed> $step
     * @param list<array<string, mixed>> $path
     * @param Closure(array<string, mixed>, list<array<string, mixed>>): bool $pathMatches
     */
    private function groupMemberTime(array $step, array $path, Closure $pathMatches): ?DateTimeImmutable
    {
        $family = $step['shape'] ?? null;
        $index = $step['resolution_order'] ?? null;
        $sequence = $step['sequence'] ?? null;
        if (!in_array($family, ['activity', 'timer', self::CHILD_SHAPE, 'condition_wait'], true) || !is_int($index) || !is_int($sequence)
            || !isset($this->history[$index]) || $path === []) {
            return null;
        }
        $rowMatches = fn (array $body, string $kind): bool => $this->groupRow($body, $kind, $path, $pathMatches);
        $matches = match ($family) {
            self::CHILD_SHAPE => ChildReplayClockEvidence::matches($this->history, $this->bySequence[$sequence] ?? [], $index, $sequence, $rowMatches),
            'condition_wait' => ConditionReplayClockEvidence::matches($this->history, $index, $sequence, $rowMatches),
            default => $this->scalarBoundary($index, $family, $sequence, $rowMatches),
        };
        if (!$matches) {
            return null;
        }
        $event = $this->history[$index];
        $prefix = $this->sourceRun !== null && $this->resumeStep !== null && $sequence < $this->resumeStep;

        // Reused-prefix group metadata is not qualified; suffix dispatch uses its own event time.
        return $prefix ? null : ReplayClockHistory::recordedTime($event);
    }

    /** @param array<string, mixed> $body
     * @param list<array<string, mixed>> $path
     * @param Closure(array<string, mixed>, list<array<string, mixed>>): bool $pathMatches
     */
    private function groupRow(array $body, string $family, array $path, Closure $pathMatches): bool
    {
        foreach ($path as $entry) {
            if (($entry['parallel_group_mode'] ?? 'all') !== 'all') {
                return false;
            }
        }
        try {
            $matches = $pathMatches($body, $path);
        } catch (NonDeterministicWorkflow) {
            $matches = false;
        }
        $selection = array_filter(array_keys($body), static fn (mixed $field): bool => str_starts_with((string) $field, 'selection_member_'));
        if (!$matches || $selection !== []) {
            return false;
        }
        foreach (['parallel_group_id', 'parallel_group_kind', 'parallel_group_mode', 'parallel_group_base_sequence',
            'parallel_group_size', 'parallel_group_index', 'parallel_group_path'] as $field) {
            unset($body[$field]);
        }

        return ReplayClockHistory::scalarRow($body, $family);
    }

    /** @param array<string, mixed> $payload */
    private function reusedTime(array $payload, string $family, int $sequence): ?DateTimeImmutable
    {
        if ($this->sourceRun === null || ($payload['reused_from_run_id'] ?? null) !== $this->sourceRun
            || $this->resumeStep === null || $sequence >= $this->resumeStep) {
            return null;
        }
        if ($family === 'activity' && (!ReplayClockHistory::identity($payload['reused_activity_execution_id'] ?? null)
            || $payload['reused_activity_execution_id'] === $payload['activity_execution_id'])) {
            return null;
        }
        $original = ReplayClockHistory::parseTime($payload['reused_recorded_at'] ?? null);

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
        $inserted = ReplayClockHistory::recordedTime($this->history[$index]);
        $original = ReplayClockHistory::parseTime($payload['replayed_started_at'] ?? null);
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
            if (!ReplayClockHistory::identity($payload[$key] ?? null)) {
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
        $accepted = $index === 1 && ReplayClockHistory::kind($this->history[0]) === 'StartAccepted'
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
            if (!ReplayClockHistory::identity($payload[$key] ?? null) || ($accepted[$key] ?? null) !== $payload[$key]) {
                return false;
            }
        }

        return true;
    }

    private function scalarBoundary(int $index, string $family, int $sequence, ?Closure $rowMatches = null): bool
    {
        $event = $this->history[$index];
        $payload = $event['payload'] ?? null;
        if (!is_array($payload) || ReplayClockHistory::sequence($payload) !== $sequence) {
            return false;
        }
        $terminals = $family === 'activity' ? self::ACTIVITY_TERMINALS : ['TimerFired'];
        $resource = $family === 'activity' ? 'activity_execution_id' : 'timer_id';
        $identity = $payload[$resource] ?? null;
        if (!in_array(ReplayClockHistory::kind($event), $terminals, true) || !ReplayClockHistory::identity($identity)) {
            return false;
        }
        return $this->scalarLifecycle($index, $family, $sequence, $resource, $identity, $terminals, $rowMatches);
    }

    /** @param list<string> $terminals
     * @param Closure(array<string, mixed>, string): bool|null $rowMatches
     */
    private function scalarLifecycle(int $index, string $family, int $sequence, string $resource, string $identity, array $terminals, ?Closure $rowMatches): bool
    {
        $payload = $this->history[$index]['payload'];
        $rowMatches ??= ReplayClockHistory::scalarRow(...);
        $opening = $family === 'activity' ? 'ActivityScheduled' : 'TimerScheduled';
        $allowed = [$opening, ...$terminals];
        if ($family === 'activity') {
            $allowed[] = 'ActivityStarted';
        }
        $positions = [];
        foreach ($this->bySequence[$sequence] ?? [] as $position) {
            $row = $this->history[$position];
            $body = $row['payload'];
            $kind = ReplayClockHistory::kind($row);
            if ($position > $index || ReplayClockHistory::sequence($body) !== $sequence
                || !in_array($kind, $allowed, true) || ($body[$resource] ?? null) !== $identity
                || !$rowMatches($body, $family)) {
                return false;
            }
            $positions[$kind][] = $position;
        }
        $openings = $positions[$opening] ?? [];
        $starts = $positions['ActivityStarted'] ?? [];
        $closures = array_merge(...array_values(array_intersect_key($positions, array_flip($terminals))));
        $reusedActivity = $family === 'activity' && ReplayClockHistory::kind($this->history[$index]) === 'ActivityCompleted'
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
}
