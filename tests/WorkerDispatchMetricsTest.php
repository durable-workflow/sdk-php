<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Metrics\MetricsRecorder;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Tests\Support\RecordingMetricsRecorder;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\AbstractLogger;

final class WorkerDispatchMetricsTest extends TestCase
{
    private const METRIC = 'durable_workflow_worker_task_schedule_to_start_seconds';

    #[DataProvider('validAges')]
    public function testServerAgeIsObservedBeforeActualDispatch(string $kind, int $age): void
    {
        $recorder = new RecordingMetricsRecorder();
        [$worker, $client, $transport] = $this->worker($recorder, $kind, ['ready_age_ms' => $age]);
        $called = 0;
        $atDispatch = [];
        $handler = function () use (&$called, &$atDispatch, $recorder): string {
            ++$called;
            $atDispatch = self::samples($recorder);

            return 'done';
        };
        if ($kind === 'activity') {
            $worker->registerActivity('orders.simple', static fn (ActivityContext $context): string => $handler());
        } else {
            $worker->registerWorkflow('orders.simple', static fn (WorkflowContext $context): string => $handler());
        }
        self::assertTrue($worker->tick(0));
        self::assertSame(1, $called);
        self::assertSame([['method' => 'record', 'name' => self::METRIC, 'value' => (float) $age / 1000,
            'tags' => ['task_kind' => $kind, 'task_queue' => 'orders']]], $atDispatch);
        self::assertCount(1, self::samples($recorder));
        self::assertCount(1, array_filter($transport->requests, static fn (array $r): bool => str_ends_with($r['uri'], '/cluster/info')));
        self::assertStringNotContainsString('private-', json_encode($recorder->events, JSON_THROW_ON_ERROR));
    }

    /** @return array<string, array{string, int}> */
    public static function validAges(): array
    {
        $cases = [];
        foreach (['workflow', 'activity'] as $kind) {
            foreach ([0, 1, 1500, 9007199254740991] as $age) {
                $cases[$kind.' '.$age] = [$kind, $age];
            }
        }

        return $cases;
    }

    #[DataProvider('invalidAges')]
    public function testInvalidAgeProducesOnlyABoundedDiagnostic(mixed $age): void
    {
        $recorder = new RecordingMetricsRecorder();
        $diagnostics = [];
        [$worker, $client] = $this->worker($recorder, 'activity', ['ready_age_ms' => $age], diagnostics: $diagnostics);
        $worker->registerActivity('orders.simple', static fn (ActivityContext $context): string => 'done');
        self::assertTrue($worker->tick(0));
        self::assertSame([], self::samples($recorder));
        $invalid = array_values(array_filter($diagnostics, static fn (array $row): bool => $row[0] === 'worker.metrics_invalid_ready_age'));
        self::assertSame([['worker.metrics_invalid_ready_age', ['task_kind' => 'activity']]], $invalid);
    }

    /** @return array<string, array{mixed}> */
    public static function invalidAges(): array
    {
        return ['null' => [null], 'true' => [true], 'false' => [false], 'text' => ['1500'],
            'integral float' => [1500.0], 'negative' => [-1], 'overflow' => [9007199254740992],
            'infinity' => [INF], 'nan' => [NAN], 'array' => [[]]];
    }

    #[DataProvider('unsupportedCapabilities')]
    public function testUnsupportedCapabilityNeverTrustsTaskMetadata(mixed $capability): void
    {
        $recorder = new RecordingMetricsRecorder();
        [$worker, $client] = $this->worker($recorder, 'activity', ['ready_age_ms' => 1500], $capability);
        $worker->registerActivity('orders.simple', static fn (ActivityContext $context): string => 'done');
        self::assertTrue($worker->tick(0));
        self::assertSame([], self::samples($recorder));
    }

    /** @return array<string, array{mixed}> */
    public static function unsupportedCapabilities(): array
    {
        return ['absent' => [null], 'false' => [false], 'integer' => [1], 'text' => ['true'], 'array' => [[]]];
    }

    public function testMissingAgeIsNotZero(): void
    {
        $recorder = new RecordingMetricsRecorder();
        $diagnostics = [];
        [$worker, $client] = $this->worker($recorder, 'activity', [], diagnostics: $diagnostics);
        $worker->registerActivity('orders.simple', static fn (ActivityContext $context): string => 'done');
        self::assertTrue($worker->tick(0));
        self::assertSame([], self::samples($recorder));
        self::assertSame([], array_filter($diagnostics, static fn (array $r): bool => $r[0] === 'worker.metrics_invalid_ready_age'));
    }

    public function testUndiscoveredCapabilityEmitsNothing(): void
    {
        $recorder = new RecordingMetricsRecorder();
        [$worker] = $this->worker($recorder, 'activity', ['ready_age_ms' => 1500], discover: false);
        $worker->registerActivity('orders.simple', static fn (ActivityContext $context): string => 'done');
        self::assertTrue($worker->tick(0));
        self::assertSame([], self::samples($recorder));
    }

    public function testManagedRegistrationSuppliesCapabilityWithoutExtraDiscovery(): void
    {
        $recorder = new RecordingMetricsRecorder();
        [$worker, , $transport] = $this->worker($recorder, 'workflow', ['ready_age_ms' => 1500], discover: false);
        $worker->registerWorkflow('orders.simple', static function (WorkflowContext $context) use ($worker): string {
            $worker->requestShutdown();

            return 'done';
        });
        $worker->run(0);
        self::assertCount(1, self::samples($recorder));
        self::assertSame([], array_filter($transport->requests, static fn (array $r): bool => str_ends_with($r['uri'], '/cluster/info')));
    }

    public function testQueryDispatchHasNoScheduleToStartObservation(): void
    {
        $recorder = new RecordingMetricsRecorder();
        [$worker] = $this->worker($recorder, 'query', ['ready_age_ms' => 1500, 'query_name' => 'status']);
        $called = 0;
        $worker->registerQuery('orders.simple', 'status', static function (QueryContext $context) use (&$called): string {
            ++$called;

            return 'done';
        });
        self::assertTrue($worker->tick(0));
        self::assertSame(1, $called);
        self::assertSame([], self::samples($recorder));
    }

    public function testPollTransportRetryStillHasOneDispatchSample(): void
    {
        $recorder = new RecordingMetricsRecorder();
        $polls = 0;
        [$worker, , $transport] = $this->worker($recorder, 'activity', ['ready_age_ms' => 1500],
            beforeRequest: static function (string $method, string $uri) use (&$polls): void {
                if (str_ends_with($uri, '/activity-tasks/poll') && ++$polls === 1) {
                    throw new TransportException('connection reset', transientConnectionFailure: true);
                }
            });
        $called = 0;
        $worker->registerActivity('orders.simple', static function (ActivityContext $context) use (&$called): string {
            ++$called;

            return 'done';
        });
        self::assertTrue($worker->tick(0));
        self::assertSame(1, $called);
        self::assertCount(1, self::samples($recorder));
        $requests = array_values(array_filter($transport->requests, static fn (array $r): bool => str_ends_with($r['uri'], '/activity-tasks/poll')));
        self::assertCount(2, $requests);
        self::assertSame($requests[0]['body']['poll_request_id'], $requests[1]['body']['poll_request_id']);
    }

    public function testCompletionTransportRetryDoesNotRedispatch(): void
    {
        $recorder = new RecordingMetricsRecorder();
        $completions = 0;
        [$worker] = $this->worker($recorder, 'activity', ['ready_age_ms' => 1500],
            beforeRequest: static function (string $method, string $uri) use (&$completions): void {
                if (str_ends_with($uri, '/complete') && ++$completions === 1) {
                    throw new TransportException('connection reset', transientConnectionFailure: true);
                }
            });
        $called = 0;
        $worker->registerActivity('orders.simple', static function (ActivityContext $context) use (&$called): string {
            ++$called;

            return 'done';
        });
        self::assertTrue($worker->tick(0));
        self::assertSame(1, $called);
        self::assertSame(2, $completions);
        self::assertCount(1, self::samples($recorder));
    }

    public function testActualRedispatchRemainsAProcessObservation(): void
    {
        $recorder = new RecordingMetricsRecorder();
        [$worker] = $this->worker($recorder, 'activity', ['ready_age_ms' => 1500]);
        $worker->registerActivity('orders.simple', static fn (ActivityContext $context): string => 'done');
        self::assertTrue($worker->tick(0));
        self::assertTrue($worker->tick(0));
        $samples = self::samples($recorder);
        self::assertCount(2, $samples);
        self::assertSame($samples[0], $samples[1], 'A replayed claim retains its age; samples are not durable execution counts.');
    }

    public function testFailingDiagnosticCannotFailTheTask(): void
    {
        $recorder = new RecordingMetricsRecorder();
        [$worker] = $this->worker($recorder, 'activity', ['ready_age_ms' => -1], throwDiagnostic: true);
        $called = 0;
        $worker->registerActivity('orders.simple', static function (ActivityContext $context) use (&$called): string {
            ++$called;

            return 'done';
        });
        self::assertTrue($worker->tick(0));
        self::assertSame(1, $called);
        self::assertSame([], self::samples($recorder));
    }

    public function testFailingRecorderCannotFailTheTask(): void
    {
        $recorder = new class implements MetricsRecorder {
            public function increment(string $name, float $value = 1.0, array $tags = []): void
            {
                throw new \RuntimeException('export unavailable');
            }
            public function record(string $name, float $value, array $tags = []): void
            {
                throw new \RuntimeException('export unavailable');
            }
        };
        [$worker] = $this->worker($recorder, 'activity', ['ready_age_ms' => 1500]);
        $called = 0;
        $worker->registerActivity('orders.simple', static function (ActivityContext $context) use (&$called): string {
            ++$called;

            return 'done';
        });
        self::assertTrue($worker->tick(0));
        self::assertSame(1, $called);
    }

    /**
     * @param array<string, mixed> $metadata
     * @param list<array{string, array<string, mixed>}> $diagnostics
     * @return array{Worker, Client, FakeTransport}
     */
    private function worker(MetricsRecorder $recorder, string $kind, array $metadata, mixed $capability = true, array &$diagnostics = [], bool $discover = true, ?\Closure $beforeRequest = null, bool $throwDiagnostic = false): array
    {
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use ($kind, $metadata, $capability, $beforeRequest): ?array {
            $beforeRequest?->__invoke($method, $uri);
            if (str_ends_with($uri, '/register')) {
                return ['registered' => true, 'server_capabilities' => ['claimed_task_ready_age' => $capability]];
            }
            if ($method === 'DELETE') {
                return ['deregistered' => true];
            }
            if (str_ends_with($uri, '/cluster/info')) {
                return ['worker_protocol' => ['server_capabilities' => $capability === null ? [] : ['claimed_task_ready_age' => $capability]]];
            }
            if (str_ends_with($uri, '/'.$kind.'-tasks/poll')) {
                return ['task' => $metadata + ['task_id' => $kind.'-task', 'workflow_id' => 'private-workflow',
                    'run_id' => 'private-run', 'workflow_type' => 'orders.simple', 'workflow_task_attempt' => 1,
                    'activity_type' => 'orders.simple', 'activity_attempt_id' => 'private-attempt',
                    'lease_owner' => 'worker-1', 'payload_codec' => 'avro', 'history_events' => []], 'poll_status' => 'leased'];
            }
            if (str_ends_with($uri, '/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }
            if (str_ends_with($uri, '/heartbeat')) {
                return ['renewed' => true, 'task_id' => 'workflow-task', 'lease_owner' => $body['lease_owner'], 'workflow_task_attempt' => $body['workflow_task_attempt']];
            }
            if (str_ends_with($uri, '/complete')) {
                return ['recorded' => true, 'outcome' => 'completed'];
            }
            throw new \LogicException('Unexpected request '.$method.' '.$uri);
        });
        $client = new Client('https://server.example', transport: $transport, metrics: $recorder);
        if ($discover) {
            $client->workerClusterInfo();
        }
        $logger = new class($diagnostics, $throwDiagnostic) extends AbstractLogger {
            public function __construct(private array &$events, private bool $throwDiagnostic) {}
            public function log($level, $message, array $context = []): void
            {
                $this->events[] = [(string) $message, $context];
                if ($this->throwDiagnostic && $message === 'worker.metrics_invalid_ready_age') {
                    throw new \RuntimeException('diagnostic unavailable');
                }
            }
        };
        $now = 0.0;
        $worker = new Worker($client, 'orders', workerId: 'worker-1',
            clock: static function () use (&$now): float { return $now; },
            sleeper: static function (int $microseconds) use (&$now): void { $now += $microseconds / 1_000_000; },
            logger: $logger);


        return [$worker, $client, $transport];
    }

    /** @return list<array<string, mixed>> */
    private static function samples(RecordingMetricsRecorder $recorder): array
    {
        return array_values(array_filter($recorder->events, static fn (array $event): bool => $event['name'] === self::METRIC));
    }
}
