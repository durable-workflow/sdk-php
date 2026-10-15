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
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Testing\WorkerTestHarness;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\WorkflowContext;
use DurableWorkflow\Worker\WorkflowFactory;
use PHPUnit\Framework\TestCase;
use Psr\Container\ContainerInterface;

final class WorkflowHandlerLifetimeTest extends TestCase
{
    public function testConstructorCreatedNestedStateIsFreshAcrossExecutionsReplayQueriesAndUpdates(): void
    {
        $worker = Worker::create(
            new Client('https://server.example', transport: new FakeTransport()),
            'php-workers',
        )->register(NestedStateWorkflow::class);

        $this->assertReplaySafeLifetime($worker, 'nested-state', 'step-1');
    }

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
        $prefix = new WorkflowStepPrefix('injected');
        $container = new class($prefix) implements ContainerInterface {
            public int $resolutions = 0;
            /** @var list<NestedStateWorkflow> */
            public array $instances = [];

            public function __construct(private readonly WorkflowStepPrefix $prefix)
            {
            }

            public function get(string $id): mixed
            {
                ++$this->resolutions;

                $instance = new NestedStateWorkflow($this->prefix);
                $this->instances[] = $instance;

                return $instance;
            }

            public function has(string $id): bool
            {
                return $id === NestedStateWorkflow::class;
            }
        };
        $worker = Worker::create(
            new Client('https://server.example', transport: new FakeTransport()),
            'php-workers',
            $container,
        )->register(NestedStateWorkflow::class);

        $this->assertReplaySafeLifetime($worker, 'nested-state', 'injected-step-1');
        self::assertGreaterThan(1, $container->resolutions);
        foreach ($container->instances as $instance) {
            self::assertSame($prefix, $instance->prefix);
        }
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

    public function testFreshConstructionDoesNotRequireCloning(): void
    {
        $worker = Worker::create(
            new Client('https://server.example', transport: new FakeTransport()),
            'php-workers',
        );

        $worker->register(NonCloneableWorkflow::class);
        $result = (new WorkerTestHarness($worker))->runWorkflow('non-cloneable');
        self::assertSame('complete', (new AvroPayloadCodec())->decodeEnvelope($result->commands[0]['result']));
    }

    public function testAnExplicitFactoryRebuildsNestedStateAndPreservesSharedDependencies(): void
    {
        $prefix = new WorkflowStepPrefix('injected');
        $instances = [];
        $factory = new WorkflowFactory(static function () use ($prefix, &$instances): NestedStateWorkflow {
            $instance = new NestedStateWorkflow($prefix);
            $instances[] = $instance;

            return $instance;
        });
        $worker = Worker::create(
            new Client('https://server.example', transport: new FakeTransport()),
            'php-workers',
        )->register($factory);

        $this->assertReplaySafeLifetime($worker, 'nested-state', 'injected-step-1');
        foreach ($instances as $instance) {
            self::assertSame($prefix, $instance->prefix);
        }
    }

    public function testAReusedFactoryInstanceIsRejectedBeforePolling(): void
    {
        $transport = new FakeTransport();
        $instance = new NestedStateWorkflow();
        $worker = Worker::create(new Client('https://server.example', transport: $transport), 'php-workers');

        try {
            $worker->register(new WorkflowFactory(static fn (): NestedStateWorkflow => $instance));
            self::fail('A singleton workflow was registered.');
        } catch (InvalidWorkerDefinition $exception) {
            self::assertStringContainsString('Create a new workflow instance', $exception->remediation);
        }
        self::assertSame([], $transport->requests);
        self::assertSame([], $worker->contracts()['workflows']);
    }

    public function testAnExistingWorkflowObjectRequiresItsConstructionFactory(): void
    {
        $worker = Worker::create(
            new Client('https://server.example', transport: new FakeTransport()),
            'php-workers',
        );

        $this->expectException(InvalidWorkerDefinition::class);
        $this->expectExceptionMessage('Register the workflow class or a WorkflowFactory');
        $worker->register(new NestedStateWorkflow());
    }

    public function testFactoryResultsMustBeObjectsOfTheOriginalHandlerClass(): void
    {
        $transport = new FakeTransport();
        $worker = Worker::create(new Client('https://server.example', transport: $transport), 'php-workers');
        foreach ([
            new WorkflowFactory(static fn (): int => 42),
            new WorkflowFactory((static function (): \Closure {
                $calls = 0;

                return static function () use (&$calls): object {
                    return ++$calls === 1 ? new NestedStateWorkflow() : new StatefulWorkflow();
                };
            })()),
            new WorkflowFactory(static function (): never {
                throw new \RuntimeException('Constructor dependency is unavailable.');
            }),
        ] as $factory) {
            try {
                $worker->register($factory);
                self::fail('An invalid factory was registered.');
            } catch (InvalidWorkerDefinition $exception) {
                self::assertNotSame('', $exception->remediation);
            }
        }
        self::assertSame([], $transport->requests);
        self::assertSame([], $worker->contracts()['workflows']);
    }

    public function testFactoryReuseAfterStartupCannotBecomeAWorkflowFailure(): void
    {
        $calls = 0;
        $instance = new NestedStateWorkflow();
        $factory = new WorkflowFactory(static function () use (&$calls, $instance): NestedStateWorkflow {
            return ++$calls <= 2 ? new NestedStateWorkflow() : $instance;
        });
        $worker = Worker::create(
            new Client('https://server.example', transport: new FakeTransport()),
            'php-workers',
        )->register($factory);
        $harness = new WorkerTestHarness($worker);
        $harness->runWorkflow('nested-state');

        try {
            $harness->runWorkflow('nested-state');
            self::fail('A reused factory result was invoked.');
        } catch (NonDeterministicWorkflow $exception) {
            self::assertSame('workflow_instance_factory_invalid', $exception->reason);
        }
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

final class NestedStateWorkflow
{
    private readonly object $state;

    public function __construct(public readonly ?WorkflowStepPrefix $prefix = null)
    {
        $this->state = (object) ['replays' => 0];
    }

    #[Workflow('nested-state')]
    public function run(WorkflowContext $context): array
    {
        ++$this->state->replays;
        $prefix = $this->prefix === null ? '' : "{$this->prefix->value}-";
        $result = $context->activity("{$prefix}step-{$this->state->replays}");

        return ['replays' => $this->state->replays, 'result' => $result];
    }

    #[Query('state')]
    public function state(QueryContext $context): int
    {
        return $this->state->replays;
    }

    #[Update('increment')]
    public function increment(QueryContext $context, int $amount = 1): int
    {
        return $this->state->replays += $amount;
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
