<?php

declare(strict_types=1);

namespace DurableWorkflow\Qualification;

use Closure;
use DurableWorkflow\Exception\ChildWorkflowFailed;
use DurableWorkflow\Worker\WorkflowContext;
use InvalidArgumentException;
use RuntimeException;

require_once __DIR__.'/DeferredChildAllLedger.php';

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
            if (($value['index'] ?? null) !== 0) {
                return ['echo' => $value];
            }
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
            self::check(($payload['child_workflow_type'] ?? null) === $this->childType(), 'Original child workflow type changed.');
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
     * @return list<list<array{run_id: string, workflow_link_id: string, terminal: ?string}>>
     */
    public function assertStable(array $history, array $commands, array $initial, bool $parentFinished = false): array
    {
        self::check($this->identities($history, $commands) === $initial, 'Replacement changed a recorded initial identity.');

        return (new DeferredChildAllLedger($this, $initial))->validate($history, $parentFinished);
    }

    /**
     * The parent continuation event intentionally has no predecessor field.
     * Verify that relation from the replacement child's own canonical start.
     *
     * @param list<array<string, mixed>> $history
     * @param list<array<string, mixed>> $predecessor
     * @param array<string, mixed> $original
     * @param array{run_id: string, workflow_link_id: string, terminal: ?string} $replacement
     * @param array{workflow_id: string, run_id: string} $parent
     */
    public function assertReplacementHistory(array $history, array $original, array $replacement, array $parent, array $predecessor): void
    {
        self::check(in_array($this->case, ['retry', 'continue'], true), 'Unqualified replacement history.');
        $starts = array_values(array_filter($history, static fn (array $event): bool => ($event['event_type'] ?? null) === 'WorkflowStarted'));
        self::check(count($starts) === 1, 'Replacement child must have exactly one canonical start.');
        $payload = $starts[0]['payload'];
        foreach (['workflow_instance_id' => $original['child_workflow_instance_id'], 'workflow_run_id' => $replacement['run_id'],
            'child_call_id' => $original['child_call_id'], 'parent_workflow_instance_id' => $parent['workflow_id'],
            'parent_workflow_run_id' => $parent['run_id'], 'parent_sequence' => 1, 'workflow_type' => $this->childType()] as $key => $value) {
            self::check(($payload[$key] ?? null) === $value, 'Replacement child start changed its parent/call/type identity.');
        }
        if ($this->case === 'retry') {
            self::check(($payload['retry_of_child_workflow_run_id'] ?? null) === $original['child_workflow_run_id']
                && ($payload['retry_attempt'] ?? null) === 2 && ($payload['continued_from_run_id'] ?? null) === null
                && ($payload['workflow_link_id'] ?? null) === $replacement['workflow_link_id'], 'Replacement child has no exact retry predecessor/link.');
            $failed = array_filter($predecessor, static fn (array $event): bool => ($event['event_type'] ?? null) === 'WorkflowFailed');
            self::check(count($failed) === 1, 'Retry predecessor has no unique canonical failure.');
        } else {
            self::check(($payload['continued_from_run_id'] ?? null) === $original['child_workflow_run_id']
                && ($payload['retry_of_child_workflow_run_id'] ?? null) === null && ($payload['retry_attempt'] ?? null) === null
                && is_string($payload['workflow_link_id'] ?? null) && $payload['workflow_link_id'] !== '', 'Replacement child has no exact continuation predecessor.');
            self::check(is_string($original['workflow_link_id'] ?? null) && $original['workflow_link_id'] !== ''
                && $payload['workflow_link_id'] !== $original['workflow_link_id']
                && $payload['workflow_link_id'] !== $replacement['workflow_link_id'], 'Continuation link reuses a parent-child link role.');
            $continued = array_values(array_filter($predecessor, static fn (array $event): bool => ($event['event_type'] ?? null) === 'WorkflowContinuedAsNew'));
            self::check(count($continued) === 1, 'Continuation predecessor has no unique canonical handoff.');
            $handoff = $continued[0]['payload'];
            self::check(($handoff['continued_to_run_id'] ?? null) === $replacement['run_id']
                && ($handoff['continued_to_run_number'] ?? null) === 2 && ($handoff['sequence'] ?? null) === 1
                && ($handoff['workflow_link_id'] ?? null) === $payload['workflow_link_id'], 'Continuation predecessor did not hand off to this child run/link.');
        }
    }

    /** @param array<string, mixed> $response */
    public static function assertEmptyPoll(array $response): void
    {
        self::check(array_key_exists('task', $response) && $response['task'] === null
            && in_array($response['poll_status'] ?? null, ['empty', 'timeout'], true)
            && ($response['reason'] ?? null) === null, 'Poll did not prove an empty parent queue.');
    }

    /** @param array<string, mixed> $task */
    public static function assertParentTask(array $task, string $workflow, string $run, string $queue): void
    {
        self::check(($task['workflow_id'] ?? null) === $workflow && ($task['run_id'] ?? null) === $run
            && ($task['task_queue'] ?? null) === $queue, 'Managed parent received another workflow, run or queue.');
    }

    /** @phpstan-assert true $condition */
    public static function check(bool $condition, string $message): void
    {
        if (!$condition) {
            throw new ChildAllQualificationFailure($message);
        }
    }
}

final class ChildAllProbeFailure extends RuntimeException
{
}

final class ChildAllQualificationFailure extends RuntimeException
{
}
