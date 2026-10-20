<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Telemetry\ExecutionTelemetry;
use DurableWorkflow\Telemetry\LogContext;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\CooperativeActivityExecutor;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use Fiber;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use OpenTelemetry\API\Instrumentation\Configurator;
use OpenTelemetry\API\Globals;
use OpenTelemetry\API\Trace\Span;
use OpenTelemetry\API\Trace\SpanBuilderInterface;
use OpenTelemetry\API\Trace\SpanContext;
use OpenTelemetry\API\Trace\SpanInterface;
use OpenTelemetry\API\Trace\StatusCode;
use OpenTelemetry\API\Trace\TracerInterface;
use OpenTelemetry\API\Trace\TracerProviderInterface;
use OpenTelemetry\Context\Context;
use OpenTelemetry\SDK\Trace\SpanExporter\InMemoryExporter;
use OpenTelemetry\SDK\Trace\SpanProcessor\SimpleSpanProcessor;
use OpenTelemetry\SDK\Trace\TracerProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class ExecutionTelemetryTest extends TestCase
{
    public function testNestedAttemptLogsFollowTheActualApplicationSpanAndRestoreCaller(): void
    {
        [$telemetry, $provider, $exporter] = $this->instrumentation();
        $caller = $provider->getTracer('application')->spanBuilder('request')->startSpan();
        $callerScope = Configurator::create()->withTracerProvider($provider)
            ->storeInContext($caller->storeInContext(Context::getRoot()))->activate();
        $handler = new TestHandler();
        $logger = new Logger('application', [$handler], [new LogContext()]);
        try {
            $result = $telemetry->task('activity', self::task(), 'tenant-a', 'orders', function () use ($telemetry, $provider, $logger): object {
                $task = ExecutionTelemetry::current();
                self::assertSame('run-1', $task['workflow_run_id']);
                self::assertSame('logical-activity-1', $task['activity_id']);
                return $telemetry->activity(['activity_attempt_id' => 'attempt-2', 'activity_attempt_number' => 2],
                    function () use ($provider, $logger, $task): object {
                        $attempt = ExecutionTelemetry::current();
                        self::assertSame('attempt-2', $attempt['activity_attempt_id']);
                        self::assertNotSame($task['span_id'], $attempt['span_id']);
                        $http = $provider->getTracer('http-client')->spanBuilder('http.request')->startSpan();
                        $scope = $http->activate();
                        try {
                            $logger->info('application message', ['request_id' => 'application-request', 'custom' => 'kept']);
                            self::assertSame($http->getContext()->getSpanId(), ExecutionTelemetry::current()['span_id']);
                        } finally {
                            $scope->detach();
                            $http->end();
                        }
                        self::assertSame($attempt, ExecutionTelemetry::current());

                        return new \stdClass();
                    });
            });
            self::assertInstanceOf(\stdClass::class, $result);
            self::assertSame([], ExecutionTelemetry::current());
            self::assertSame($caller->getContext()->getSpanId(), Span::getCurrent()->getContext()->getSpanId());
            $records = $handler->getRecords();
            $record = $records[0];
            $context = $record instanceof \Monolog\LogRecord ? $record->context : $record['context'];
            self::assertSame('application-request', $context['request_id']);
            self::assertSame('run-1', $context['workflow_run_id']);
            self::assertSame('logical-activity-1', $context['activity_id']);
            self::assertSame('attempt-2', $context['activity_attempt_id']);
            self::assertSame(2, $context['activity_attempt_number']);
            self::assertSame('kept', $context['custom']);
            $spans = $exporter->getSpans();
            self::assertSame('http.request', $spans[0]->getName());
            self::assertSame($spans[1]->getContext()->getSpanId(), $spans[0]->getParentContext()->getSpanId());
            self::assertSame($spans[2]->getContext()->getSpanId(), $spans[1]->getParentContext()->getSpanId());
            self::assertFalse($spans[2]->getParentContext()->isValid());
            self::assertNotSame($caller->getContext()->getTraceId(), $spans[2]->getContext()->getTraceId());
        } finally {
            $callerScope->detach();
            $caller->end();
        }
    }

    public function testBoundWorkflowFibersKeepSeparateContextsAcrossSuspensionAndUnwinding(): void
    {
        [$telemetry] = $this->instrumentation();
        $fibers = [];
        foreach (['run-a', 'run-b'] as $runId) {
            $fibers[] = new Fiber(function () use ($telemetry, $runId): void {
                $telemetry->task('workflow', ['run_id' => $runId], 'tenant-a', 'orders', static function () use ($runId): void {
                    self::assertSame($runId, ExecutionTelemetry::current()['workflow_run_id']);
                    $unbound = new Fiber(static fn (): array => ExecutionTelemetry::current());
                    $unbound->start();
                    self::assertSame([], $unbound->getReturn());
                    Fiber::suspend();
                    self::assertSame($runId, ExecutionTelemetry::current()['workflow_run_id']);
                });
            });
        }
        $fibers[0]->start();
        self::assertSame([], ExecutionTelemetry::current());
        $fibers[1]->start();
        self::assertSame([], ExecutionTelemetry::current());
        $fibers[0]->resume();
        $failure = new RuntimeException('private callback message');
        try {
            $fibers[1]->throw($failure);
            self::fail('Fiber failure was swallowed.');
        } catch (RuntimeException $caught) {
            self::assertSame($failure, $caught);
        }
        self::assertSame([], ExecutionTelemetry::current());
    }

    public function testReplayerBindsContextBeforeResolvingAndCallingWorkflowHandler(): void
    {
        [$telemetry, , $exporter] = $this->instrumentation();
        $client = new Client('https://server.example');
        $result = $telemetry->task('workflow', self::task(), 'tenant-a', 'orders',
            static fn () => (new Replayer($client->payloadCodec()))->replay(
                static function (WorkflowContext $context): string {
                    self::assertSame('run-1', ExecutionTelemetry::current()['workflow_run_id']);
                    self::assertArrayNotHasKey('activity_id', ExecutionTelemetry::current());

                    return 'workflow-result';
                }, [], [], 'orders', self::task()));
        self::assertSame('workflow-result', $client->payloadCodec()->decodeEnvelope($result->commands[0]['result']));
        self::assertSame([], ExecutionTelemetry::current());
        self::assertCount(1, $exporter->getSpans());
        self::assertSame('durable.workflow.task', $exporter->getSpans()[0]->getName());
    }

    public function testPrivacyAndOriginalExceptionIdentity(): void
    {
        [$telemetry, , $exporter] = $this->instrumentation();
        $failure = new RuntimeException('secret credential and customer payload');
        try {
            $telemetry->task('activity', self::task() + ['arguments' => ['secret'], 'authorization' => 'secret'],
                'tenant-a', 'orders', static function () use ($failure): never { throw $failure; });
            self::fail('Business failure was swallowed.');
        } catch (RuntimeException $caught) {
            self::assertSame($failure, $caught);
        }
        $span = $exporter->getSpans()[0];
        self::assertSame(StatusCode::STATUS_ERROR, $span->getStatus()->getCode());
        $attributes = $span->getAttributes()->toArray();
        self::assertSame(RuntimeException::class, $attributes['error.type']);
        self::assertStringNotContainsString('secret', json_encode($attributes, JSON_THROW_ON_ERROR));
        self::assertSame([], $span->getEvents());
        self::assertSame([], ExecutionTelemetry::current());
    }

    public function testDisabledAndFailingProvidersExecuteExactlyOnceWithoutChangingTheResult(): void
    {
        $provider = $this->createMock(TracerProviderInterface::class);
        $provider->expects(self::once())->method('getTracer')->willThrowException(new RuntimeException('provider unavailable'));
        $calls = 0;
        $result = new \stdClass();
        foreach ([new ExecutionTelemetry($provider, enabled: false), new ExecutionTelemetry($provider)] as $telemetry) {
            self::assertSame($result, $telemetry->task('activity', self::task(), 'tenant-a', 'orders',
                static function () use (&$calls, $result): object { ++$calls; return $result; }));
            self::assertSame([], ExecutionTelemetry::current());
        }
        self::assertSame(2, $calls);
    }

    public function testSequentialWorkerAttemptsKeepLogicalIdentityAndWireOutcomes(): void
    {
        [$telemetry, , $exporter] = $this->instrumentation();
        $polls = 0;
        $transport = new FakeTransport(handler: static function (string $method, string $uri) use (&$polls): ?array {
            if (str_ends_with($uri, '/workflow-tasks/poll') || str_ends_with($uri, '/query-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }
            if (str_ends_with($uri, '/activity-tasks/poll')) {
                ++$polls;

                return ['task' => array_replace(self::task(), ['task_id' => 'task-'.$polls,
                    'activity_attempt_id' => 'attempt-'.$polls, 'attempt_number' => $polls]), 'poll_status' => 'leased'];
            }
            if (str_ends_with($uri, '/fail') || str_ends_with($uri, '/complete')) {
                return ['recorded' => true];
            }
            throw new \LogicException('Unexpected request '.$method.' '.$uri);
        });
        $seen = [];
        $worker = new Worker(new Client('https://server.example', namespace: 'tenant-a', transport: $transport),
            'orders', workerId: 'worker-1', clock: static fn (): float => 0, telemetry: $telemetry);
        $worker->registerActivity('orders.charge', static function (ActivityContext $context) use (&$seen): string {
            $seen[] = ExecutionTelemetry::current();
            if ($context->attemptNumber === 1) {
                throw new RuntimeException('genuine body failure');
            }

            return 'complete';
        });
        self::assertTrue($worker->tick(0));
        self::assertSame([], ExecutionTelemetry::current());
        self::assertTrue($worker->tick(0));
        self::assertSame([], ExecutionTelemetry::current());
        self::assertCount(2, $seen);
        self::assertSame('logical-activity-1', $seen[0]['activity_id']);
        self::assertSame($seen[0]['activity_id'], $seen[1]['activity_id']);
        self::assertSame('attempt-1', $seen[0]['activity_attempt_id']);
        self::assertSame('attempt-2', $seen[1]['activity_attempt_id']);
        self::assertNotSame($seen[0]['span_id'], $seen[1]['span_id']);
        self::assertNotSame($seen[0]['trace_id'], $seen[1]['trace_id']);
        $outcomes = array_values(array_filter($transport->requests,
            static fn (array $r): bool => str_ends_with($r['uri'], '/fail') || str_ends_with($r['uri'], '/complete')));
        self::assertCount(2, $outcomes);
        self::assertStringEndsWith('/task-1/fail', $outcomes[0]['uri']);
        self::assertStringEndsWith('/task-2/complete', $outcomes[1]['uri']);
        self::assertSame('attempt-1', $outcomes[0]['body']['activity_attempt_id']);
        self::assertSame('attempt-2', $outcomes[1]['body']['activity_attempt_id']);
        self::assertCount(4, $exporter->getSpans());
    }

    public function testOptionalApiAndMonologCanBothBeAbsent(): void
    {
        $pipes = [];
        $process = proc_open([PHP_BINARY, __DIR__.'/Fixtures/telemetry-without-api.php'],
            [1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
        self::assertIsResource($process);
        $stdout = stream_get_contents($pipes[1]);
        $stderr = stream_get_contents($pipes[2]);
        fclose($pipes[1]);
        fclose($pipes[2]);
        self::assertSame(0, proc_close($process), $stderr);
        self::assertSame("optional-api-absent-pass\n", $stdout);
        self::assertSame('', $stderr);
    }

    public function testArrayLogRecordsPreserveApplicationRequestAndOutsideLogs(): void
    {
        [$telemetry] = $this->instrumentation();
        $processor = new LogContext();
        $record = ['message' => 'message', 'context' => ['request_id' => 'application-request']];
        self::assertSame($record, $processor($record));
        $actual = $telemetry->task('activity', self::task(), 'tenant-a', 'orders', static fn () => $processor($record));
        self::assertSame('application-request', $actual['context']['request_id']);
        self::assertSame('run-1', $actual['context']['workflow_run_id']);
        self::assertSame('logical-activity-1', $actual['context']['activity_id']);
        self::assertSame($record, $processor($record));
    }

    public function testCooperativeCallbackUsesFreshProviderAndExportsBeforeProcessExit(): void
    {
        self::assertTrue(CooperativeActivityExecutor::available());
        $path = tempnam(sys_get_temp_dir(), 'wf773-child-');
        self::assertIsString($path);
        [$owner, $provider, $ownerExporter] = $this->instrumentation();
        $telemetry = new ExecutionTelemetry($provider, childProviderFactory: static function () use ($path): TracerProvider {
            $exporter = new class($path) extends InMemoryExporter {
                public function __construct(private readonly string $path) { parent::__construct(); }
                protected function doExport(iterable $spans): bool
                {
                    foreach ($spans as $span) {
                        file_put_contents($this->path, json_encode(['pid' => getmypid(), 'name' => $span->getName(),
                            'span_id' => $span->getContext()->getSpanId(),
                            'parent_span_id' => $span->getParentContext()->getSpanId()], JSON_THROW_ON_ERROR));
                    }
                    return true;
                }
            };
            return new TracerProvider(new SimpleSpanProcessor($exporter));
        });
        try {
            $codec = (new Client('https://server.example'))->payloadCodec();
            $fields = $owner->task('activity', self::task(), 'tenant-a', 'orders',
                static fn () => (new CooperativeActivityExecutor($codec))->execute(
                    static fn (\Closure $heartbeat) => $telemetry->activity(
                        ExecutionTelemetry::taskFields(self::task(), 'tenant-a', 'orders'),
                        static fn () => ExecutionTelemetry::current()),
                    static fn (array $details): array => [], static function (bool $force): void {}));
            $exported = json_decode(file_get_contents($path), true, flags: JSON_THROW_ON_ERROR);
            self::assertNotSame(getmypid(), $exported['pid']);
            self::assertSame('durable.activity.attempt', $exported['name']);
            self::assertSame($fields['span_id'], $exported['span_id']);
            self::assertSame('logical-activity-1', $fields['activity_id']);
            self::assertCount(1, $ownerExporter->getSpans());
            self::assertSame($ownerExporter->getSpans()[0]->getContext()->getSpanId(), $exported['parent_span_id']);
            self::assertSame([], ExecutionTelemetry::current());
        } finally {
            unlink($path);
        }
    }

    public function testMissingOrFailingChildFactoryCannotReuseTheOwnerProvider(): void
    {
        self::assertTrue(CooperativeActivityExecutor::available());
        [$owner, $provider, $exporter] = $this->instrumentation();
        $codec = (new Client('https://server.example'))->payloadCodec();
        foreach ([null, static function (): never { throw new RuntimeException('child setup failed'); }] as $factory) {
            $telemetry = new ExecutionTelemetry($provider, childProviderFactory: $factory);
            $actual = $owner->task('activity', self::task(), 'tenant-a', 'orders',
                static fn () => (new CooperativeActivityExecutor($codec))->execute(
                    static fn (\Closure $heartbeat) => $telemetry->activity(
                        ExecutionTelemetry::taskFields(self::task(), 'tenant-a', 'orders'), static function (): array {
                            $span = Globals::tracerProvider()->getTracer('application')->spanBuilder('external-call')->startSpan();
                            return ['recording' => $span->isRecording(), 'fields' => ExecutionTelemetry::current()];
                        }), static fn (array $details): array => [], static function (bool $force): void {}));
            self::assertFalse($actual['recording']);
            self::assertSame('run-1', $actual['fields']['workflow_run_id']);
            self::assertArrayNotHasKey('trace_id', $actual['fields']);
            self::assertSame([], ExecutionTelemetry::current());
        }
        self::assertCount(2, $exporter->getSpans());
    }

    public function testExporterEndFailureCannotReplaceCallbackResultOrException(): void
    {
        $span = $this->createMock(SpanInterface::class);
        $span->method('getContext')->willReturn(SpanContext::getInvalid());
        $span->method('storeInContext')->willReturn(Context::getRoot());
        $span->expects(self::exactly(2))->method('end')->willThrowException(new RuntimeException('export failed'));
        $builder = $this->createMock(SpanBuilderInterface::class);
        foreach (['setSpanKind', 'setParent', 'setAttributes'] as $method) {
            $builder->method($method)->willReturnSelf();
        }
        $builder->method('startSpan')->willReturn($span);
        $tracer = $this->createMock(TracerInterface::class);
        $tracer->method('spanBuilder')->willReturn($builder);
        $provider = $this->createMock(TracerProviderInterface::class);
        $provider->method('getTracer')->willReturn($tracer);
        $telemetry = new ExecutionTelemetry($provider);
        $result = new \stdClass();
        self::assertSame($result, $telemetry->task('activity', self::task(), 'tenant-a', 'orders', static fn () => $result));
        $failure = new RuntimeException('original business failure');
        try {
            $telemetry->task('activity', self::task(), 'tenant-a', 'orders', static function () use ($failure): never { throw $failure; });
            self::fail('Original failure was swallowed.');
        } catch (RuntimeException $caught) {
            self::assertSame($failure, $caught);
        }
        self::assertSame([], ExecutionTelemetry::current());
    }

    public function testUntrustedIdentityFieldsAreBoundedAndResumeMetadataIsNotAnActiveActivity(): void
    {
        [$telemetry, , $exporter] = $this->instrumentation();
        $fields = $telemetry->task('workflow', array_replace(self::task(), [
            'workflow_type' => "orders\nsecret", 'request_id' => str_repeat('r', 257),
            'run_id' => ['malformed'], 'attempt_number' => '2',
        ]), 'tenant-a', 'orders', static fn () => ExecutionTelemetry::current());
        self::assertSame('workflow-1', $fields['workflow_instance_id']);
        foreach (['workflow_type', 'request_id', 'workflow_run_id', 'activity_id', 'activity_type',
            'activity_attempt_id', 'activity_attempt_number'] as $name) {
            self::assertArrayNotHasKey($name, $fields);
            self::assertArrayNotHasKey($name, $exporter->getSpans()[0]->getAttributes()->toArray());
        }
    }

    /** @return array<string, mixed> */
    private static function task(): array
    {
        return ['task_id' => 'task-1', 'workflow_id' => 'workflow-1', 'run_id' => 'run-1',
            'workflow_type' => 'orders.workflow', 'activity_execution_id' => 'logical-activity-1',
            'activity_attempt_id' => 'attempt-1', 'attempt_number' => 1, 'activity_type' => 'orders.charge',
            'lease_owner' => 'worker-1', 'payload_codec' => 'avro', 'request_id' => 'transport-request'];
    }

    /** @return array{ExecutionTelemetry, TracerProvider, InMemoryExporter} */
    private function instrumentation(): array
    {
        $exporter = new InMemoryExporter();
        $provider = new TracerProvider(new SimpleSpanProcessor($exporter));

        return [new ExecutionTelemetry($provider), $provider, $exporter];
    }
}
