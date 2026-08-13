<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Attribute\Activity;
use DurableWorkflow\Attribute\Query;
use DurableWorkflow\Attribute\Update;
use DurableWorkflow\Attribute\Workflow;
use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\InvalidWorkerDefinition;
use DurableWorkflow\Testing\WorkerTestHarness;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\TestCase;
use Psr\Container\ContainerInterface;

final class WorkflowHandlerLifetimeTest extends TestCase
{
    public function testNoContainerWorkflowStateIsFreshAcrossExecutionsAndReplay(): void
    {
        $worker = Worker::create(
            new Client('https://server.example', transport: new FakeTransport()),
            'php-workers',
        )->register(StatefulWorkflow::class);

        $this->assertReplaySafeLifetime($worker, 'stateful', 'step-1');
    }

    public function testContainerWorkflowStateIsFreshWithoutLosingConstructorDependencies(): void
    {
        $service = new StatefulWorkflow(new WorkflowStepPrefix('injected'));
        $container = new class($service) implements ContainerInterface {
            public int $resolutions = 0;

            public function __construct(private readonly object $service)
            {
            }

            public function get(string $id): mixed
            {
                ++$this->resolutions;

                return $this->service;
            }

            public function has(string $id): bool
            {
                return $id === StatefulWorkflow::class;
            }
        };
        $worker = Worker::create(
            new Client('https://server.example', transport: new FakeTransport()),
            'php-workers',
            $container,
        )->register(StatefulWorkflow::class);

        $this->assertReplaySafeLifetime($worker, 'stateful', 'injected-step-1');
        self::assertSame(1, $container->resolutions);
    }

    public function testActivityServicesAndLowLevelCallablesKeepTheirOwnedLifetime(): void
    {
        $workflowCalls = 0;
        $worker = Worker::create(
            new Client('https://server.example', transport: new FakeTransport()),
            'php-workers',
        )
            ->register(StatefulActivities::class)
            ->registerWorkflow(
                'low-level',
                static function (WorkflowContext $context) use (&$workflowCalls): int {
                    return ++$workflowCalls;
                },
            );
        $harness = new WorkerTestHarness($worker);

        self::assertSame(1, $harness->runActivity('stateful-activity'));
        self::assertSame(2, $harness->runActivity('stateful-activity'));
        $harness->runWorkflow('low-level');
        $harness->runWorkflow('low-level');
        self::assertSame(2, $workflowCalls);
    }

    public function testAttributedWorkflowHandlersMustBeCloneable(): void
    {
        $worker = Worker::create(
            new Client('https://server.example', transport: new FakeTransport()),
            'php-workers',
        );

        $this->expectException(InvalidWorkerDefinition::class);
        $this->expectExceptionMessage('Allow the workflow handler object to be cloned');

        $worker->register(NonCloneableWorkflow::class);
    }

    private function assertReplaySafeLifetime(Worker $worker, string $workflowType, string $activityType): void
    {
        $history = [
            [
                'event_type' => 'ActivityScheduled',
                'payload' => ['sequence' => 1, 'activity_type' => $activityType],
            ],
            [
                'event_type' => 'ActivityCompleted',
                'payload' => [
                    'sequence' => 1,
                    'activity_type' => $activityType,
                    'result' => (new AvroPayloadCodec())->envelope('recorded'),
                ],
            ],
        ];
        $harness = new WorkerTestHarness($worker);

        $firstReplay = $harness->runWorkflow(
            $workflowType,
            history: $history,
            task: ['workflow_id' => 'workflow-a', 'run_id' => 'run-a'],
        )->commands;
        $otherExecution = $harness->runWorkflow(
            $workflowType,
            history: $history,
            task: ['workflow_id' => 'workflow-b', 'run_id' => 'run-b'],
        )->commands;
        $otherRun = $harness->runWorkflow(
            $workflowType,
            history: $history,
            task: ['workflow_id' => 'workflow-a', 'run_id' => 'run-b'],
        )->commands;
        $repeatedReplay = $harness->runWorkflow(
            $workflowType,
            history: $history,
            task: ['workflow_id' => 'workflow-a', 'run_id' => 'run-a'],
        )->commands;

        self::assertSame($firstReplay, $otherExecution);
        self::assertSame($firstReplay, $otherRun);
        self::assertSame($firstReplay, $repeatedReplay);
        self::assertSame(
            'amount',
            $worker->contracts()['workflow_commands'][$workflowType]['update_contracts'][0]['parameters'][0]['name'],
        );
        self::assertSame(0, $harness->runQuery($workflowType, 'state'));
        self::assertSame(1, $harness->runUpdate($workflowType, 'increment'));
        self::assertSame(0, $harness->runQuery($workflowType, 'state'));
    }
}

final class StatefulWorkflow
{
    private int $replays = 0;

    public function __construct(private readonly ?WorkflowStepPrefix $prefix = null)
    {
    }

    #[Workflow('stateful')]
    public function run(WorkflowContext $context): array
    {
        ++$this->replays;
        $prefix = $this->prefix === null ? '' : "{$this->prefix->value}-";
        $result = $context->activity("{$prefix}step-{$this->replays}");

        return ['replays' => $this->replays, 'result' => $result];
    }

    #[Query('state')]
    public function state(QueryContext $context): int
    {
        return $this->replays;
    }

    #[Update('increment')]
    public function increment(QueryContext $context, int $amount = 1): int
    {
        return $this->replays += $amount;
    }
}

final class WorkflowStepPrefix
{
    public function __construct(public readonly string $value)
    {
    }
}

final class StatefulActivities
{
    private int $executions = 0;

    #[Activity('stateful-activity')]
    public function run(ActivityContext $context): int
    {
        return ++$this->executions;
    }
}

final class NonCloneableWorkflow
{
    private function __clone(): void
    {
    }

    #[Workflow('non-cloneable')]
    public function run(WorkflowContext $context): string
    {
        return 'complete';
    }
}
