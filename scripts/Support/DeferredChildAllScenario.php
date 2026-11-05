<?php

declare(strict_types=1);

namespace DurableWorkflow\Qualification;

use Closure;
use DurableWorkflow\Exception\ChildWorkflowFailed;
use DurableWorkflow\Worker\WorkflowContext;
use InvalidArgumentException;
use RuntimeException;

/** Source adapter for Server's flat deferred-child-only all contract (#185). */
final class DeferredChildAllScenario
{
    public const SOURCE = 'ea993064428fa9e0294520aa3efd498bef5b64dc';

    public const FIXTURE_SHA256 = '6aaf1fe339af8637fc994d6dbfecc28c6f3fcad4e60f0d9971205e7562e4d01f';

    /** @var array<string, mixed> */
    public readonly array $fixture;

    public readonly int $size;

    /** @var list<string> */
    public readonly array $queues;

    /** @param array<mixed> $queues */
    public function __construct(public readonly string $case, array $queues)
    {
        $bytes = (string) file_get_contents(dirname(__DIR__).'/fixtures/parallel-child-all.json');
        self::check(PHP_INT_SIZE === 8, 'The canonical int64 fixture requires 64-bit PHP.');
        self::check(hash('sha256', $bytes) === self::FIXTURE_SHA256, 'Canonical child-all fixture identity changed.');
        $this->fixture = json_decode($bytes, true, flags: JSON_THROW_ON_ERROR);
        $sizes = $this->fixture['parallel_child_all']['cases'] + ['retry' => 2, 'continue' => 2];
        if (!isset($sizes[$case])) {
            throw new InvalidArgumentException('Unknown deferred-child case.');
        }
        $this->size = $sizes[$case];
        self::check(array_is_list($queues) && count($queues) === $this->size, 'One queue is required per child.');
        foreach ($queues as $queue) {
            self::check(is_string($queue) && trim($queue) !== '' && strlen($queue) <= 191, 'Invalid child task queue.');
        }
        self::check(count(array_unique($queues)) === $this->size, 'Child queues must be distinct for deterministic execution.');
        $this->queues = $queues;
    }

    /** Handler for the canonical parent workflow. */
    public function parent(): Closure
    {
        return function (WorkflowContext $context, array $value): array {
            $operations = [];
            foreach ($this->queues as $index => $queue) {
                $options = ['queue' => $queue];
                if ($this->case === 'retry' && $index === 0) {
                    $options['retry_policy'] = ['max_attempts' => 2, 'backoff_seconds' => [0]];
                }
                $operations[] = $context->deferChildWorkflow($this->childType(), [['index' => $index, 'value' => $value]], $options);
            }
            try {
                return ['results' => $context->all($operations)];
            } catch (ChildWorkflowFailed $error) {
                return ['failure_class' => $error::class, 'failure_type' => $error->failureType,
                    'workflow_type' => $error->workflowType, 'message' => $error->getMessage()];
            }
        };
    }

    /**
     * The retry fixture fails only its recorded initial run. This configuration
     * is fixed before each worker starts and never reads a mutable attempt counter.
     *
     */
    public function child(?string $initialRunId = null): Closure
    {
        return function (WorkflowContext $context, array $value, int $stage = 0) use ($initialRunId): array {
            if (($value['index'] ?? null) === 0) {
                if ($this->case === 'failed') {
                    throw new ChildAllProbeFailure($this->fixture['parallel_child_all']['failure_messages']['failed']);
                }
                if ($this->case === 'retry') {
                    self::check(is_string($initialRunId) && $initialRunId !== '', 'Retry requires the recorded initial child run.');
                    if ($context->runId === $initialRunId) {
                        throw new ChildAllProbeFailure('retry group member');
                    }
                }
                if ($this->case === 'continue' && $stage === 0) {
                    $context->continueAsNew([$value, 1]);
                }
            }

            return ['echo' => $value];
        };
    }

    public function parentType(): string
    {
        return $this->fixture['parallel_child_all']['parent_type'];
    }

    public function childType(): string
    {
        return $this->fixture['parallel_child_all']['child_type'];
    }

    /** @return list<int> */
    public function order(): array
    {
        return match ($this->case) {
            'out_of_order' => [2, 0, 1], 'retry', 'continue' => [1, 0], default => range(0, $this->size - 1),
        };
    }

    public function failsEarly(): bool
    {
        return in_array($this->case, ['failed', 'cancelled', 'terminated'], true);
    }

    /** @return array<string, mixed> */
    public function expected(): array
    {
        if ($this->failsEarly()) {
            return ['failure_class' => ChildWorkflowFailed::class,
                'failure_type' => match ($this->case) {
                    'failed' => 'ChildRunFailed', 'cancelled' => 'ChildRunCancelled', default => 'ChildRunTerminated',
                }, 'workflow_type' => $this->childType(),
                'message' => $this->fixture['parallel_child_all']['failure_messages'][$this->case]];
        }

        return ['results' => array_map(fn (int $index): array => ['echo' => [
            'index' => $index, 'value' => $this->fixture['input'],
        ]], range(0, $this->size - 1))];
    }

    /**
     * Bind all starts to the actual acknowledged commands. The private receipt
     * supplies metadata only; no lease authority is included in returned identities.
     *
     * @param list<array<string, mixed>> $history
     * @param list<array<string, mixed>> $commands
     * @return list<array<string, mixed>>
     */
    public function identities(array $history, array $commands): array
    {
        $scheduled = array_values(array_filter($history, static fn (array $event): bool => $event['event_type'] === 'ChildWorkflowScheduled'));
        self::check(count($scheduled) === $this->size && count($commands) === $this->size, 'Every member must start exactly once.');
        $identities = [];
        foreach ($scheduled as $index => $event) {
            $payload = $event['payload'];
            $command = $commands[$index];
            self::check(($command['type'] ?? null) === 'start_child_workflow'
                && ($command['queue'] ?? null) === $this->queues[$index]
                && ($command['workflow_type'] ?? null) === $this->childType(), 'Unexpected child command or queue.');
            self::check(($payload['sequence'] ?? null) === $index + 1, 'Original child call sequence changed.');
            $identity = [];
            foreach (['child_call_id', 'workflow_link_id', 'child_workflow_instance_id', 'child_workflow_run_id'] as $key) {
                self::check(is_string($payload[$key] ?? null) && $payload[$key] !== '', 'Missing canonical child identity.');
                $identity[$key] = $payload[$key];
            }
            foreach (['parallel_group_id', 'parallel_group_path'] as $key) {
                self::check(isset($payload[$key]) && $payload[$key] === ($command[$key] ?? null), 'Canonical child group path changed.');
                $identity[$key] = $payload[$key];
            }
            $identities[] = ['sequence' => $index + 1, ...$identity];
        }
        foreach (['child_call_id', 'workflow_link_id', 'child_workflow_instance_id', 'child_workflow_run_id'] as $key) {
            self::check(count(array_unique(array_column($identities, $key))) === $this->size, 'Two members share one canonical identity.');
        }

        return $identities;
    }

    /**
     * @param list<array<string, mixed>> $history
     * @param list<array<string, mixed>> $commands
     * @param list<array<string, mixed>> $initial
     */
    public function assertStable(array $history, array $commands, array $initial): void
    {
        self::check($this->identities($history, $commands) === $initial, 'Replacement changed a recorded initial identity.');
        $runs = array_fill(0, $this->size, []);
        foreach ($history as $event) {
            if (!str_starts_with($event['event_type'], 'ChildRun')) {
                continue;
            }
            $payload = $event['payload'];
            $original = $initial[($payload['sequence'] ?? 0) - 1] ?? null;
            self::check(is_array($original), 'Child event has no original member.');
            foreach (['child_call_id', 'workflow_link_id', 'child_workflow_instance_id', 'parallel_group_id', 'parallel_group_path'] as $key) {
                self::check(isset($payload[$key]) && $payload[$key] === $original[$key], 'Child event changed its original call/link/path.');
            }
            $canReplaceRun = in_array($this->case, ['retry', 'continue'], true) && $payload['sequence'] === 1;
            self::check(is_string($payload['child_workflow_run_id'] ?? null) && $payload['child_workflow_run_id'] !== '', 'Child event has no run identity.');
            $index = $payload['sequence'] - 1;
            $run = $payload['child_workflow_run_id'];
            if ($event['event_type'] === 'ChildRunStarted') {
                self::check(!in_array($run, $runs[$index], true), 'Duplicate child run start.');
                self::check($runs[$index] === [] ? $run === $original['child_workflow_run_id'] : $canReplaceRun && count($runs[$index]) === 1,
                    'Unexpected replacement child run.');
                $runs[$index][] = $run;
            } else {
                self::check($runs[$index] !== [] && $run === $runs[$index][array_key_last($runs[$index])], 'Child event does not belong to its current recorded run.');
            }
        }
        foreach ($runs as $chain) {
            self::check($chain !== [], 'Atomic child start has no initial run event.');
        }
    }

    /** @phpstan-assert true $condition */
    public static function check(bool $condition, string $message): void
    {
        if (!$condition) {
            throw new RuntimeException($message);
        }
    }
}

final class ChildAllProbeFailure extends RuntimeException
{
}
