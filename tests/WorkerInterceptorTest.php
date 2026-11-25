<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\InvalidWorkerDefinition;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\Interceptor\ActivityInterceptorContext;
use DurableWorkflow\Worker\Interceptor\PassthroughWorkerInterceptor;
use DurableWorkflow\Worker\Interceptor\QueryTaskInterceptorContext;
use DurableWorkflow\Worker\Interceptor\WorkerInterceptor;
use DurableWorkflow\Worker\Interceptor\WorkflowTaskInterceptorContext;
use DurableWorkflow\Worker\Interceptor\WorkflowTaskOutcome;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\TestCase;
use Psr\Container\ContainerInterface;
use Psr\Container\NotFoundExceptionInterface;
use RuntimeException;

final class WorkerInterceptorTest extends TestCase
{
    public function testWorkflowTaskInterceptorsRunInOrderWithTheTaskContext(): void
    {
        $log = new InterceptorLog();
        $handlerRuns = 0;
        [$requests] = $this->runWorkflowTask(
            static function (WorkflowContext $context) use ($log, &$handlerRuns): string {
                ++$handlerRuns;
                $log->events[] = 'workflow';

                return 'done';
            },
            [new RecordingInterceptor('outer', $log), new RecordingInterceptor('inner', $log)],
        );

        self::assertSame(1, $handlerRuns);
        self::assertSame(['outer:before', 'inner:before', 'workflow', 'inner:after', 'outer:after'], $log->events);
        $context = $log->workflowContexts[0];
        self::assertSame('owner-original', $context->workerId);
        self::assertSame('workers', $context->taskQueue);
        self::assertSame('orders', $context->namespace);
        self::assertSame('task-original', $context->taskId);
        self::assertSame('interceptor.test', $context->workflowType);
        self::assertSame('workflow-original', $context->workflowId);
        self::assertSame('run-original', $context->runId);
        self::assertSame(3, $context->workflowTaskAttempt);
        self::assertSame(0, $context->historyLength);
        self::assertFalse($context->replaying);
        self::assertSame('task-original', $context->task['task_id']);
        self::assertSame([['complete_workflow'], ['complete_workflow']], $log->outcomeTypes);
        self::assertSame('complete_workflow', self::completed($requests)[0]['type']);
    }

    public function testWorkflowTaskInterceptorsSeeReplayOfACommittedPrefix(): void
    {
        $codec = new AvroPayloadCodec();
        $log = new InterceptorLog();
        $history = [
            ['event_type' => 'WorkflowStarted', 'payload' => []],
            ['event_type' => 'ActivityScheduled', 'payload' => ['sequence' => 1, 'activity_type' => 'greet']],
            ['event_type' => 'ActivityCompleted', 'payload' => ['sequence' => 1, 'activity_type' => 'greet', 'result' => $codec->envelope('hello')]],
        ];
        [$requests] = $this->runWorkflowTask(
            static fn (WorkflowContext $context): string => $context->activity('greet', []).' again',
            [new RecordingInterceptor('audit', $log)],
            $history,
        );

        self::assertTrue($log->workflowContexts[0]->replaying);
        self::assertSame(3, $log->workflowContexts[0]->historyLength);
        $commands = self::completed($requests);
        self::assertSame('complete_workflow', $commands[0]['type']);
        self::assertSame('hello again', $codec->decodeEnvelope($commands[0]['result']));
        self::assertFalse(WorkflowTaskInterceptorContext::replaysCommittedPrefix([
            ['event_type' => 'WorkflowStarted'], ['event_type' => 'SignalReceived'],
        ]));
        self::assertTrue(WorkflowTaskInterceptorContext::replaysCommittedPrefix([['event_type' => 'TimerScheduled']]));
    }

    public function testWorkflowTaskInterceptorCannotChangeTheTaskOrCommands(): void
    {
        $codec = new AvroPayloadCodec();
        $seenTypes = [];
        $tamper = new class () extends PassthroughWorkerInterceptor {
            public function executeWorkflowTask(WorkflowTaskInterceptorContext $context, callable $next): WorkflowTaskOutcome
            {
                $swapped = new WorkflowTaskInterceptorContext($context->workerId, $context->taskQueue, $context->namespace,
                    ['workflow_type' => 'other.type'] + $context->task, $context->taskId, 'other.type', 'other-workflow',
                    'other-run', 9, 0, false);
                $outcome = $next($swapped);

                return new WorkflowTaskOutcome([['type' => 'fail_workflow', 'message' => 'forged'], ...$outcome->commands]);
            }
        };
        [$requests] = $this->runWorkflowTask(
            static function (WorkflowContext $context) use (&$seenTypes): string {
                $seenTypes[] = 'ran';

                return 'original';
            },
            [$tamper],
        );

        self::assertSame(['ran'], $seenTypes);
        $commands = self::completed($requests);
        self::assertCount(1, $commands);
        self::assertSame('complete_workflow', $commands[0]['type']);
        self::assertSame('original', $codec->decodeEnvelope($commands[0]['result']));
    }

    public function testWorkflowTaskCoreFailurePassesThroughAnInterceptorThatSwallowsIt(): void
    {
        $log = new InterceptorLog();
        $swallow = new class ($log) extends PassthroughWorkerInterceptor {
            public function __construct(private readonly InterceptorLog $log)
            {
            }

            public function executeWorkflowTask(WorkflowTaskInterceptorContext $context, callable $next): WorkflowTaskOutcome
            {
                try {
                    return $next($context);
                } catch (\Throwable $failure) {
                    $this->log->events[] = 'caught:'.$failure->getMessage();

                    return new WorkflowTaskOutcome([['type' => 'complete_workflow']]);
                }
            }
        };
        [$requests] = $this->runWorkflowTask(null, [$swallow]);

        self::assertSame(['caught:No workflow handler is registered for interceptor.test.'], $log->events);
        self::assertSame([], self::requestsTo($requests, '/task-original/complete'));
        $failures = self::requestsTo($requests, '/task-original/fail');
        self::assertCount(1, $failures);
        self::assertStringContainsString('No workflow handler is registered', $failures[0]['body']['failure']['message']);
    }

    public function testWorkflowTaskInterceptorFailureAfterTheCoreFailsTheTaskWithoutRecording(): void
    {
        $throwing = new class () extends PassthroughWorkerInterceptor {
            public function executeWorkflowTask(WorkflowTaskInterceptorContext $context, callable $next): WorkflowTaskOutcome
            {
                $next($context);

                throw new RuntimeException('audit sink unavailable');
            }
        };
        [$requests] = $this->runWorkflowTask(static fn (WorkflowContext $context): string => 'done', [$throwing]);

        self::assertSame([], self::requestsTo($requests, '/task-original/complete'));
        $failures = self::requestsTo($requests, '/task-original/fail');
        self::assertCount(1, $failures);
        self::assertStringContainsString('audit sink unavailable', $failures[0]['body']['failure']['message']);
    }

    public function testWorkflowTaskInterceptorMustCallNextExactlyOnce(): void
    {
        foreach (['skip' => 0, 'twice' => 2] as $case => $calls) {
            $runs = 0;
            $interceptor = new NextCallingInterceptor($calls);
            [$requests] = $this->runWorkflowTask(
                static function (WorkflowContext $context) use (&$runs): string {
                    ++$runs;

                    return 'done';
                },
                [$interceptor],
            );

            self::assertSame(min($calls, 1), $runs, $case);
            self::assertSame([], self::requestsTo($requests, '/task-original/complete'), $case);
            $failures = self::requestsTo($requests, '/task-original/fail');
            self::assertCount(1, $failures, $case);
            self::assertStringContainsString('WorkflowTaskInterceptorViolation', json_encode($failures[0]['body'], JSON_THROW_ON_ERROR), $case);
            self::assertStringContainsString(NextCallingInterceptor::class, $failures[0]['body']['failure']['message'], $case);
        }
    }

    public function testSwallowedNextViolationStillFailsTheTask(): void
    {
        $swallow = new class () extends PassthroughWorkerInterceptor {
            public function executeWorkflowTask(WorkflowTaskInterceptorContext $context, callable $next): WorkflowTaskOutcome
            {
                try {
                    return $next($context);
                } catch (\Throwable) {
                    return new WorkflowTaskOutcome([['type' => 'complete_workflow']]);
                }
            }
        };
        foreach ([0, 2] as $calls) {
            [$requests] = $this->runWorkflowTask(static fn (WorkflowContext $context): string => 'done',
                [$swallow, new NextCallingInterceptor($calls)]);

            self::assertSame([], self::requestsTo($requests, '/task-original/complete'), (string) $calls);
            $failures = self::requestsTo($requests, '/task-original/fail');
            self::assertCount(1, $failures, (string) $calls);
            self::assertStringContainsString(NextCallingInterceptor::class, $failures[0]['body']['failure']['message']);
        }
    }

    public function testActivityInterceptorsRunInOrderAndMayTransformArgumentsAndResult(): void
    {
        $codec = new AvroPayloadCodec();
        $log = new InterceptorLog();
        $transform = new class () extends PassthroughWorkerInterceptor {
            public function executeActivity(ActivityInterceptorContext $context, callable $next): mixed
            {
                return strtoupper((string) $next($context->withArguments(...array_map(
                    static fn (mixed $argument): string => 'dear '.$argument, $context->arguments))));
            }
        };
        $seen = [];
        $requests = $this->runActivityTask(
            static function (ActivityContext $context, string $name) use (&$seen, $log): string {
                $seen[] = [$context->activityType, $context->taskId, $name];
                $log->events[] = 'activity';

                return 'hello '.$name;
            },
            [new RecordingInterceptor('outer', $log), $transform, new RecordingInterceptor('inner', $log)],
        );

        self::assertSame([['greet', 'activity-task', 'dear Ada']], $seen);
        self::assertSame(['outer:before', 'inner:before', 'activity', 'inner:after', 'outer:after'], $log->events);
        $context = $log->activityContexts[0];
        self::assertSame('greet', $context->activityType);
        self::assertSame(['Ada'], $context->arguments);
        self::assertSame(2, $context->attemptNumber);
        self::assertSame('activity-task', $context->taskId);
        self::assertSame('activity-attempt', $context->activityAttemptId);
        self::assertSame('workflow-1', $context->workflowId);
        self::assertSame('run-1', $context->runId);
        self::assertSame('owner-original', $context->workerId);
        self::assertSame('orders', $context->namespace);
        self::assertFalse($context->local);
        self::assertSame('activity-task', $context->task['task_id']);
        self::assertSame(['dear Ada'], $log->activityContexts[1]->arguments);
        $completions = self::requestsTo($requests, '/activity-task/complete');
        self::assertCount(1, $completions);
        self::assertSame('HELLO DEAR ADA', $codec->decodeEnvelope($completions[0]['body']['result']));
    }

    public function testActivityInterceptorsWrapInlineLocalActivities(): void
    {
        $codec = new AvroPayloadCodec();
        $log = new InterceptorLog();
        $completion = null;
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use (&$completion): array {
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                return ['task' => ['task_id' => 'task', 'workflow_id' => 'local-workflow', 'run_id' => 'local-run',
                    'workflow_type' => 'flow', 'lease_owner' => 'owner', 'payload_codec' => 'avro', 'history_events' => []]];
            }
            if (str_ends_with($uri, '/workflow-tasks/task/complete')) {
                $completion = $body;

                return ['completed' => true];
            }
            if (str_ends_with($uri, '/workflow-tasks/task/heartbeat')) {
                return ['task_id' => 'task', 'workflow_task_attempt' => 1, 'lease_owner' => 'owner', 'renewed' => true];
            }
            if (str_ends_with($uri, '/poll')) {
                return ['task' => null];
            }
            self::fail('Unexpected request: '.$method.' '.$uri);
        });
        $worker = new Worker(new Client('https://server.example', transport: $transport), 'queue', workerId: 'owner',
            interceptors: [new RecordingInterceptor('audit', $log)]);
        $worker->registerWorkflow('flow', static fn (WorkflowContext $context): mixed => $context->localActivity('greet', ['Ada']));
        $worker->registerActivity('greet', static fn (ActivityContext $context, string $name): string => 'hello '.$name);

        self::assertTrue($worker->tick(0));
        self::assertNotNull($completion);
        $local = array_values(array_filter($log->activityContexts, static fn (ActivityInterceptorContext $context): bool => $context->local));
        self::assertCount(1, $local);
        self::assertSame('greet', $local[0]->activityType);
        self::assertSame(['Ada'], $local[0]->arguments);
        self::assertSame('local-workflow', $local[0]->workflowId);
        self::assertSame([], $local[0]->task);
        $finished = array_values(array_filter($completion['commands'], static fn (array $command): bool => $command['type'] === 'complete_workflow'));
        self::assertSame('hello Ada', $codec->decodeEnvelope($finished[0]['result']));
        self::assertSame(['audit:before', 'audit:before', 'audit:after', 'audit:after'], $log->events);
    }

    public function testActivityFailurePassesThroughInterceptors(): void
    {
        $log = new InterceptorLog();
        $requests = $this->runActivityTask(
            static function (ActivityContext $context, string $name): never {
                throw new RuntimeException('card declined');
            },
            [new RecordingInterceptor('outer', $log), new RecordingInterceptor('inner', $log)],
        );

        self::assertSame(['outer:before', 'inner:before', 'inner:error:card declined', 'outer:error:card declined'], $log->events);
        self::assertSame([], self::requestsTo($requests, '/activity-task/complete'));
        $failures = self::requestsTo($requests, '/activity-task/fail');
        self::assertCount(1, $failures);
        self::assertStringContainsString('card declined', json_encode($failures[0]['body'], JSON_THROW_ON_ERROR));
    }

    public function testQueryInterceptorsRunInOrderAroundTheHandler(): void
    {
        $log = new InterceptorLog();
        $transport = new FakeTransport([['completed' => true]]);
        $worker = new Worker(new Client('http://runtime', namespace: 'orders', transport: $transport), 'workers',
            workerId: 'worker-1', interceptors: [new RecordingInterceptor('outer', $log), new RecordingInterceptor('inner', $log)]);
        $worker->registerQuery('query.workflow', 'value', static function (QueryContext $context, int $offset) use ($log): int {
            $log->events[] = 'query:'.$context->workflowId;

            return 40 + $offset;
        });
        (new \ReflectionMethod($worker, 'executeQueryTask'))->invoke($worker, [
            'payload_codec' => 'avro', 'query_task_id' => 'query-1', 'query_task_attempt' => 1, 'lease_owner' => 'worker-1',
            'workflow_id' => 'workflow-1', 'run_id' => 'run-1', 'workflow_type' => 'query.workflow', 'query_name' => 'value',
            'history_events' => [], 'query_arguments' => (new AvroPayloadCodec())->envelope([2]),
        ]);

        self::assertSame(['outer:before', 'inner:before', 'query:workflow-1', 'inner:after', 'outer:after'], $log->events);
        $context = $log->queryContexts[0];
        self::assertSame('query-1', $context->taskId);
        self::assertSame('value', $context->queryName);
        self::assertSame('query.workflow', $context->workflowType);
        self::assertSame('workflow-1', $context->workflowId);
        self::assertSame('run-1', $context->runId);
        self::assertSame([2], $context->arguments);
        self::assertSame('orders', $context->namespace);
        self::assertStringEndsWith('/query-1/complete', $transport->requests[0]['uri']);
        self::assertSame(42, $transport->requests[0]['body']['result']);
    }

    public function testInterceptorClassNamesResolveFromTheContainer(): void
    {
        $log = new InterceptorLog();
        $container = new class ($log) implements ContainerInterface {
            /** @var list<string> */
            public array $resolved = [];

            public function __construct(private readonly InterceptorLog $log)
            {
            }

            public function get(string $id): mixed
            {
                $this->resolved[] = $id;
                if ($id === ContainerAuditInterceptor::class) {
                    return new ContainerAuditInterceptor($this->log);
                }
                throw new class ("{$id} is not registered.") extends RuntimeException implements NotFoundExceptionInterface {
                };
            }

            public function has(string $id): bool
            {
                return $id === ContainerAuditInterceptor::class;
            }
        };
        [$requests] = $this->runWorkflowTask(static fn (WorkflowContext $context): string => 'done',
            [ContainerAuditInterceptor::class], container: $container);

        self::assertSame([ContainerAuditInterceptor::class], $container->resolved);
        self::assertSame(['container:task-original'], $log->events);
        self::assertCount(1, self::requestsTo($requests, '/task-original/complete'));
    }

    public function testInterceptorResolutionWithoutContainerAndInvalidEntries(): void
    {
        $worker = new Worker(new Client('http://runtime'), 'workers', interceptors: [PassthroughWorkerInterceptor::class]);
        $chain = (new \ReflectionProperty($worker, 'interceptors'))->getValue($worker);
        $resolved = (new \ReflectionProperty($chain, 'interceptors'))->getValue($chain);
        self::assertInstanceOf(PassthroughWorkerInterceptor::class, $resolved[0]);

        try {
            new Worker(new Client('http://runtime'), 'workers', interceptors: [\stdClass::class]);
            self::fail('A non-interceptor class was accepted.');
        } catch (InvalidWorkerDefinition $failure) {
            self::assertStringContainsString(WorkerInterceptor::class, $failure->getMessage());
        }
        try {
            new Worker(new Client('http://runtime'), 'workers', interceptors: [ContainerAuditInterceptor::class]);
            self::fail('A class with required dependencies resolved without a container.');
        } catch (InvalidWorkerDefinition $failure) {
            self::assertStringContainsString('PSR-11 container', $failure->getMessage());
        }
    }

    /**
     * @param list<WorkerInterceptor|class-string<WorkerInterceptor>> $interceptors
     * @param list<array<string, mixed>> $history
     * @return array{0: list<array{method: string, uri: string, headers: array<string, string>, body: array<string, mixed>|null}>}
     */
    private function runWorkflowTask(?callable $handler, array $interceptors, array $history = [], ?ContainerInterface $container = null): array
    {
        $codec = new AvroPayloadCodec();
        $transport = new FakeTransport(handler: static function (string $method, string $uri) use ($codec, $history): array {
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                return ['poll_status' => 'leased', 'task' => [
                    'task_id' => 'task-original', 'workflow_task_attempt' => 3, 'lease_owner' => 'owner-original',
                    'workflow_id' => 'workflow-original', 'run_id' => 'run-original', 'workflow_type' => 'interceptor.test',
                    'payload_codec' => 'avro', 'arguments' => $codec->envelope([]), 'history_events' => $history,
                ]];
            }
            if (str_ends_with($uri, '/activity-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
            }
            if (str_ends_with($uri, '/heartbeat')) {
                return ['task_id' => 'task-original', 'workflow_task_attempt' => 3, 'lease_owner' => 'owner-original', 'renewed' => true];
            }
            if (str_ends_with($uri, '/complete')) {
                return ['completed' => true];
            }
            if (str_ends_with($uri, '/fail')) {
                return ['failed' => true, 'recorded' => true];
            }
            self::fail('Unexpected worker request '.$method.' '.$uri);
        });
        $worker = new Worker(new Client('https://server.example', namespace: 'orders', transport: $transport), 'workers',
            workerId: 'owner-original', container: $container, interceptors: $interceptors);
        if ($handler !== null) {
            $worker->registerWorkflow('interceptor.test', $handler);
        }
        self::assertTrue($worker->tick(0));

        return [$transport->requests];
    }

    /**
     * @param list<WorkerInterceptor> $interceptors
     * @return list<array{method: string, uri: string, headers: array<string, string>, body: array<string, mixed>|null}>
     */
    private function runActivityTask(callable $handler, array $interceptors): array
    {
        $codec = new AvroPayloadCodec();
        $polls = 0;
        $transport = new FakeTransport(handler: static function (string $method, string $uri) use ($codec, &$polls): array {
            if (str_ends_with($uri, '/activity-tasks/poll')) {
                if (++$polls > 1) {
                    return ['task' => null];
                }

                return ['task' => ['task_id' => 'activity-task', 'activity_attempt_id' => 'activity-attempt',
                    'workflow_id' => 'workflow-1', 'run_id' => 'run-1', 'lease_owner' => 'owner-original',
                    'activity_type' => 'greet', 'attempt_number' => 2, 'payload_codec' => 'avro',
                    'arguments' => $codec->envelope(['Ada'])]];
            }
            if (str_ends_with($uri, '/fail')) {
                return ['failed' => true, 'recorded' => true];
            }
            if (str_ends_with($uri, '/complete')) {
                return ['completed' => true];
            }
            if (str_ends_with($uri, '/poll')) {
                return ['task' => null];
            }
            self::fail('Unexpected request: '.$method.' '.$uri);
        });
        $worker = new Worker(new Client('https://server.example', namespace: 'orders', transport: $transport), 'workers',
            workerId: 'owner-original', interceptors: $interceptors);
        $worker->registerActivity('greet', $handler);
        self::assertTrue($worker->tick(0));

        return $transport->requests;
    }

    /**
     * @param list<array{method: string, uri: string, headers: array<string, string>, body: array<string, mixed>|null}> $requests
     * @return list<array{method: string, uri: string, headers: array<string, string>, body: array<string, mixed>|null}>
     */
    private static function requestsTo(array $requests, string $suffix): array
    {
        return array_values(array_filter($requests, static fn (array $row): bool => str_ends_with($row['uri'], $suffix)));
    }

    /**
     * @param list<array{method: string, uri: string, headers: array<string, string>, body: array<string, mixed>|null}> $requests
     * @return list<array<string, mixed>>
     */
    private static function completed(array $requests): array
    {
        $complete = self::requestsTo($requests, '/task-original/complete');
        self::assertCount(1, $complete);

        return $complete[0]['body']['commands'];
    }
}

final class InterceptorLog
{
    /** @var list<string> */
    public array $events = [];
    /** @var list<WorkflowTaskInterceptorContext> */
    public array $workflowContexts = [];
    /** @var list<list<string>> */
    public array $outcomeTypes = [];
    /** @var list<ActivityInterceptorContext> */
    public array $activityContexts = [];
    /** @var list<QueryTaskInterceptorContext> */
    public array $queryContexts = [];
}

final class RecordingInterceptor implements WorkerInterceptor
{
    public function __construct(private readonly string $name, private readonly InterceptorLog $log)
    {
    }

    public function executeWorkflowTask(WorkflowTaskInterceptorContext $context, callable $next): WorkflowTaskOutcome
    {
        $this->log->workflowContexts[] = $context;

        return $this->around(function () use ($context, $next): WorkflowTaskOutcome {
            $outcome = $next($context);
            $this->log->outcomeTypes[] = $outcome->commandTypes();

            return $outcome;
        });
    }

    public function executeActivity(ActivityInterceptorContext $context, callable $next): mixed
    {
        $this->log->activityContexts[] = $context;

        return $this->around(static fn (): mixed => $next($context));
    }

    public function executeQueryTask(QueryTaskInterceptorContext $context, callable $next): mixed
    {
        $this->log->queryContexts[] = $context;

        return $this->around(static fn (): mixed => $next($context));
    }

    private function around(\Closure $next): mixed
    {
        $this->log->events[] = $this->name.':before';
        try {
            $result = $next();
        } catch (\Throwable $failure) {
            $this->log->events[] = $this->name.':error:'.$failure->getMessage();

            throw $failure;
        }
        $this->log->events[] = $this->name.':after';

        return $result;
    }
}

final class NextCallingInterceptor extends PassthroughWorkerInterceptor
{
    public function __construct(private readonly int $calls)
    {
    }

    public function executeWorkflowTask(WorkflowTaskInterceptorContext $context, callable $next): WorkflowTaskOutcome
    {
        $outcome = new WorkflowTaskOutcome([]);
        for ($call = 0; $call < $this->calls; ++$call) {
            $outcome = $next($context);
        }

        return $outcome;
    }
}

final class ContainerAuditInterceptor extends PassthroughWorkerInterceptor
{
    public function __construct(private readonly InterceptorLog $log)
    {
    }

    public function executeWorkflowTask(WorkflowTaskInterceptorContext $context, callable $next): WorkflowTaskOutcome
    {
        $this->log->events[] = 'container:'.$context->taskId;

        return $next($context);
    }
}
