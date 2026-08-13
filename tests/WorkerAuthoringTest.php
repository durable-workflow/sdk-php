<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Attribute\Activity;
use DurableWorkflow\Attribute\Query;
use DurableWorkflow\Attribute\Signal;
use DurableWorkflow\Attribute\Update;
use DurableWorkflow\Attribute\Workflow;
use DurableWorkflow\Client;
use DurableWorkflow\Exception\InvalidWorkerDefinition;
use DurableWorkflow\Testing\WorkerTestHarness;
use DurableWorkflow\Testing\WorkflowClientFake;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\WorkflowContext;
use DurableWorkflow\WorkflowClientInterface;
use PHPUnit\Framework\TestCase;
use Psr\Container\ContainerInterface;
use Psr\Log\AbstractLogger;

final class WorkerAuthoringTest extends TestCase
{
    public function testClassHandlersAreDiscoveredAndExercisedThroughThePublicHarness(): void
    {
        $worker = Worker::create(new Client('https://server.example', transport: new FakeTransport()), 'php-workers')
            ->register(GreetingWorkflow::class, GreetingActivities::class);

        self::assertSame(['greeter'], $worker->contracts()['workflows']);
        self::assertSame(['greet'], $worker->contracts()['activities']);
        self::assertSame(
            ['status'],
            $worker->contracts()['workflow_commands']['greeter']['queries'],
        );
        self::assertSame(
            ['set-language'],
            $worker->contracts()['workflow_commands']['greeter']['signals'],
        );
        self::assertSame(
            ['rename'],
            $worker->contracts()['workflow_commands']['greeter']['updates'],
        );

        $harness = new WorkerTestHarness($worker);
        $harness->assertWorkflowEmits('greeter', 'schedule_activity', ['Ada']);
        $harness->assertActivityResult('greet', 'hello, Ada', ['Ada']);
        $harness->assertQueryResult('greeter', 'status', ['events' => 0]);
        $harness->assertUpdateResult('greeter', 'rename', 'renamed Ada', ['Ada']);
        $harness->assertRegistered('signal', 'set-language', 'greeter');
    }

    public function testInvalidAttributedContractExplainsTheRemediationBeforePolling(): void
    {
        $transport = new FakeTransport();
        $worker = Worker::create(new Client('https://server.example', transport: $transport), 'php-workers');

        try {
            $worker->register(InvalidGreetingWorkflow::class);
            self::fail('Invalid workflow was registered.');
        } catch (InvalidWorkerDefinition $exception) {
            self::assertSame(InvalidGreetingWorkflow::class.'::run()', $exception->contract);
            self::assertStringContainsString(WorkflowContext::class, $exception->remediation);
        }

        self::assertSame([], $transport->requests);
    }

    public function testConstructorDependenciesRequireAndUseTheNarrowPsrContainerContract(): void
    {
        $client = new Client('https://server.example', transport: new FakeTransport());

        $this->expectException(InvalidWorkerDefinition::class);
        $this->expectExceptionMessage('Pass a PSR-11 container');
        Worker::create($client, 'php-workers')->register(DependentActivities::class);
    }

    public function testPsrContainerResolvesClassHandlers(): void
    {
        $service = new DependentActivities(new GreetingPrefix('welcome'));
        $container = new class($service) implements ContainerInterface {
            public function __construct(private readonly object $service)
            {
            }

            public function get(string $id): mixed
            {
                return $this->service;
            }

            public function has(string $id): bool
            {
                return $id === DependentActivities::class;
            }
        };
        $worker = Worker::create(
            new Client('https://server.example', transport: new FakeTransport()),
            'php-workers',
            $container,
        )->register(DependentActivities::class);

        $harness = new WorkerTestHarness($worker);
        $harness->assertActivityResult('welcome', 'welcome Ada', ['Ada']);
        self::assertSame('welcome Ada', $harness->runActivity('welcome', ['Ada']));
    }

    public function testWorkerLifecycleIsReportedThroughPsrLoggingAndDiagnostics(): void
    {
        $logger = new RecordingLogger();
        $events = [];
        $worker = Worker::create(
            new Client('https://server.example', transport: new FakeTransport([
                ['registered' => true, 'heartbeat_interval_seconds' => 10],
                ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'],
            ])),
            'php-workers',
            logger: $logger,
            diagnosticListener: static function (string $event) use (&$events): void {
                $events[] = $event;
            },
        )->register(GreetingWorkflow::class);

        $worker->run(0);

        self::assertSame(
            ['worker.starting', 'worker.registered', 'worker.stopped_by_server', 'worker.stopped'],
            $events,
        );
        self::assertSame($events, array_column($logger->records, 'message'));
    }

    public function testExplicitShutdownDeregistersTheWorker(): void
    {
        $worker = null;
        $transport = new FakeTransport(handler: static function (
            string $method,
            string $uri,
            array $headers,
            ?array $body,
        ) use (&$worker): ?array {
            if (str_ends_with($uri, '/api/worker/register')) {
                return ['registered' => true];
            }
            if (str_ends_with($uri, '/api/worker/workflow-tasks/poll')) {
                $worker?->requestShutdown();

                return ['task' => null, 'poll_status' => 'empty'];
            }
            if (str_ends_with($uri, '/api/worker/registrations/test-worker')) {
                return [
                    'worker_id' => 'test-worker',
                    'outcome' => 'deregistered',
                    'recovered_workflow_task_count' => 0,
                ];
            }

            self::fail("Unexpected worker request: {$method} {$uri}");
        });
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'php-workers',
            workerId: 'test-worker',
        );

        $worker->run(0);

        self::assertSame('DELETE', $transport->requests[2]['method']);
        self::assertStringEndsWith('/api/worker/registrations/test-worker', $transport->requests[2]['uri']);
    }

    public function testHandlerFailuresReachTheStandardLogger(): void
    {
        $logger = new RecordingLogger();
        $transport = new FakeTransport([
            ['registered' => true],
            ['task' => null, 'poll_status' => 'empty'],
            ['poll_status' => 'leased', 'task' => [
                'task_id' => 'activity-1',
                'activity_attempt_id' => 'attempt-1',
                'lease_owner' => 'worker-1',
                'activity_type' => 'broken',
            ]],
            ['failed' => true],
            ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'],
        ]);
        $worker = Worker::create(
            new Client('https://server.example', transport: $transport),
            'php-workers',
            logger: $logger,
        )->register(BrokenActivities::class);

        $worker->run(0);

        self::assertContains('worker.handler_failed', array_column($logger->records, 'message'));
        $failure = array_values(array_filter(
            $logger->records,
            static fn (array $record): bool => $record['message'] === 'worker.handler_failed',
        ))[0];
        self::assertSame('activity', $failure['context']['handler_kind']);
        self::assertInstanceOf(\RuntimeException::class, $failure['context']['exception']);
    }

    public function testWorkflowClientFakeCoversEveryWorkflowInteraction(): void
    {
        $client = (new WorkflowClientFake())
            ->setQueryResult('greeting-1', 'status', 'running')
            ->setUpdateResult('greeting-1', 'rename', 'accepted')
            ->setWorkflowResult('greeting-1', ['greeting' => 'hello, Ada']);
        self::assertInstanceOf(WorkflowClientInterface::class, $client);
        $handle = $client->startWorkflow('greeter', 'greeting-1', 'php-workers', ['Ada']);

        $handle->signal('set-language', ['en']);
        self::assertSame('running', $handle->query('status'));
        self::assertSame('accepted', $handle->update('rename', ['Grace']));
        self::assertSame(['greeting' => 'hello, Ada'], $handle->result());

        $client->assertWorkflowStarted('greeter', ['Ada']);
        $client->assertSignalSent('greeting-1', 'set-language', ['en']);
        $client->assertQueryRequested('greeting-1', 'status');
        $client->assertUpdateRequested('greeting-1', 'rename', ['Grace']);
        $client->assertResultRequested('greeting-1');
    }
}

final class GreetingWorkflow
{
    #[Workflow('greeter')]
    public function run(WorkflowContext $context, string $name): array
    {
        $greeting = $context->activity('greet', [$name]);

        return ['greeting' => $greeting];
    }

    #[Query]
    public function status(QueryContext $context): array
    {
        return ['events' => count($context->history)];
    }

    #[Signal('set-language')]
    public function setLanguage(string $language): void
    {
    }

    #[Update]
    public function rename(QueryContext $context, string $name): string
    {
        return "renamed {$name}";
    }
}

final class GreetingActivities
{
    #[Activity]
    public function greet(ActivityContext $context, string $name): string
    {
        return "hello, {$name}";
    }
}

final class InvalidGreetingWorkflow
{
    #[Workflow('invalid')]
    public function run(string $name): string
    {
        return $name;
    }
}

final class GreetingPrefix
{
    public function __construct(public readonly string $value)
    {
    }
}

final class DependentActivities
{
    public function __construct(private readonly GreetingPrefix $prefix)
    {
    }

    #[Activity('welcome')]
    public function greet(ActivityContext $context, string $name): string
    {
        return "{$this->prefix->value} {$name}";
    }
}

final class BrokenActivities
{
    #[Activity('broken')]
    public function fail(ActivityContext $context): never
    {
        throw new \RuntimeException('handler failed');
    }
}

final class RecordingLogger extends AbstractLogger
{
    /** @var list<array{level: mixed, message: string, context: array<string, mixed>}> */
    public array $records = [];

    public function log($level, $message, array $context = []): void
    {
        $this->records[] = ['level' => $level, 'message' => (string) $message, 'context' => $context];
    }
}
