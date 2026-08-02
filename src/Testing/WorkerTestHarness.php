<?php

declare(strict_types=1);

namespace DurableWorkflow\Testing;

use DurableWorkflow\Client;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\ReplayResult;
use DurableWorkflow\Worker\Replayer;

/** Executes and asserts registered worker handlers without polling a server. */
final class WorkerTestHarness
{
    private readonly Client $contextClient;
    private readonly Replayer $replayer;

    public function __construct(private readonly Worker $worker, ?Client $contextClient = null)
    {
        $this->contextClient = $contextClient ?? new Client(
            'http://durable-workflow.test',
            transport: new InMemoryTransport(),
        );
        $this->replayer = new Replayer($this->contextClient->payloadCodec());
        $this->worker->validate();
    }

    /**
     * @param list<mixed> $input
     * @param list<array<string, mixed>> $history
     * @param array<string, mixed> $task
     */
    public function runWorkflow(
        string $workflowType,
        array $input = [],
        array $history = [],
        array $task = [],
    ): ReplayResult {
        return $this->replayer->replay(
            $this->worker->registeredHandler('workflow', $workflowType),
            $history,
            $input,
            $this->worker->taskQueue,
            $task,
        );
    }

    /** @param list<mixed> $arguments */
    public function runActivity(string $activityType, array $arguments = []): mixed
    {
        $context = new ActivityContext(
            $this->contextClient,
            'test-task',
            'test-attempt',
            'test-worker',
            $activityType,
            1,
        );

        return ($this->worker->registeredHandler('activity', $activityType))($context, ...$arguments);
    }

    /**
     * @param list<mixed> $arguments
     * @param list<array<string, mixed>> $history
     */
    public function runQuery(
        string $workflowType,
        string $queryName,
        array $arguments = [],
        array $history = [],
    ): mixed {
        return ($this->worker->registeredHandler('query', $queryName, $workflowType))(
            $this->queryContext($workflowType, $history),
            ...$arguments,
        );
    }

    /**
     * @param list<mixed> $arguments
     * @param list<array<string, mixed>> $history
     */
    public function runUpdate(
        string $workflowType,
        string $updateName,
        array $arguments = [],
        array $history = [],
    ): mixed {
        return ($this->worker->registeredHandler('update', $updateName, $workflowType))(
            $this->queryContext($workflowType, $history),
            ...$arguments,
        );
    }

    public function assertRegistered(string $kind, string $name, ?string $workflowType = null): void
    {
        try {
            $this->worker->registeredHandler($kind, $name, $workflowType);
        } catch (\InvalidArgumentException $exception) {
            throw new AssertionFailed($exception->getMessage(), previous: $exception);
        }
    }

    /** @param list<mixed> $input */
    public function assertWorkflowEmits(string $workflowType, string $commandType, array $input = []): void
    {
        foreach ($this->runWorkflow($workflowType, $input)->commands as $command) {
            if (($command['type'] ?? null) === $commandType) {
                return;
            }
        }

        throw new AssertionFailed("Workflow {$workflowType} did not emit command {$commandType}.");
    }

    /** @param list<mixed> $arguments */
    public function assertActivityResult(string $activityType, mixed $expected, array $arguments = []): void
    {
        $this->assertSame($expected, $this->runActivity($activityType, $arguments), "activity {$activityType}");
    }

    /** @param list<mixed> $arguments */
    public function assertQueryResult(
        string $workflowType,
        string $queryName,
        mixed $expected,
        array $arguments = [],
    ): void {
        $this->assertSame(
            $expected,
            $this->runQuery($workflowType, $queryName, $arguments),
            "query {$workflowType}.{$queryName}",
        );
    }

    /** @param list<mixed> $arguments */
    public function assertUpdateResult(
        string $workflowType,
        string $updateName,
        mixed $expected,
        array $arguments = [],
    ): void {
        $this->assertSame(
            $expected,
            $this->runUpdate($workflowType, $updateName, $arguments),
            "update {$workflowType}.{$updateName}",
        );
    }

    /** @param list<array<string, mixed>> $history */
    private function queryContext(string $workflowType, array $history): QueryContext
    {
        return new QueryContext('test-workflow', 'test-run', $history, [
            'workflow_id' => 'test-workflow',
            'run_id' => 'test-run',
            'workflow_type' => $workflowType,
        ]);
    }

    private function assertSame(mixed $expected, mixed $actual, string $contract): void
    {
        if ($expected !== $actual) {
            throw new AssertionFailed(sprintf(
                'Expected %s to return %s; received %s.',
                $contract,
                var_export($expected, true),
                var_export($actual, true),
            ));
        }
    }
}
