<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DateTimeImmutable;
use DateTimeZone;
use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Metrics\MetricsRecorder;
use DurableWorkflow\Tests\Support\ActivityGrantScenario;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Tests\Support\RecordingMetricsRecorder;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Transport\PayloadTransport;
use DurableWorkflow\Transport\Transport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use GuzzleHttp\Client as HttpClient;
use GuzzleHttp\Promise\FulfilledPromise;
use GuzzleHttp\Psr7\Response;
use Psr\Http\Message\RequestInterface;

final class WorkerCarriedPollTest extends TestCase
{
    public function testEmbeddedActivityUsesTheNormalPayloadPathWithoutAnActivityPoll(): void
    {
        $transport = self::transport(static fn (): array => ['activity_poll' => ['leased' => true, 'answer' => self::activityAnswer()]]);
        $calls = [];
        $worker = self::worker($transport, $calls);

        self::assertTrue($worker->tick(0));
        self::assertSame(['workflow', 'activity:7'], $calls);
        self::assertSame([], self::requests($transport, '/activity-tasks/poll'));
        $completion = self::requests($transport, '/workflow-tasks/workflow-task/complete')[0]['body'];
        self::assertSame('worker-1', $completion['activity_poll']['worker_id']);
        self::assertSame('orders', $completion['activity_poll']['task_queue']);
        self::assertSame(0, $completion['activity_poll']['timeout_seconds']);
        self::assertNotEmpty($completion['activity_poll']['poll_request_id']);
        self::assertSame('activity:7', (new AvroPayloadCodec())->decodeEnvelope(
            self::requests($transport, '/activity-tasks/activity-task/complete')[0]['body']['result']));
    }

    #[DataProvider('unavailableCapabilities')]
    public function testOldOrNonBooleanCapabilityKeepsTheOriginalRequestPath(mixed $capability): void
    {
        $transport = self::transport(static fn (): array => ['completed' => true], $capability);
        $calls = [];
        self::assertTrue(self::worker($transport, $calls)->tick(0));

        self::assertArrayNotHasKey('activity_poll', self::requests($transport, '/workflow-tasks/workflow-task/complete')[0]['body']);
        self::assertCount(1, self::requests($transport, '/activity-tasks/poll'));
        self::assertSame(['workflow', 'activity:7'], $calls);
    }

    public static function unavailableCapabilities(): iterable
    {
        foreach ([null, false, 1, 'true'] as $value) {
            yield [$value];
        }
    }

    public function testFlagOnlyLeaseFollowsUpWithTheOriginalCarriedIdentity(): void
    {
        $transport = self::transport(static fn (): array => ['activity_poll' => ['leased' => true]]);
        $calls = [];
        self::assertTrue(self::worker($transport, $calls)->tick(0));

        $carried = self::requests($transport, '/workflow-tasks/workflow-task/complete')[0]['body']['activity_poll'];
        self::assertSame($carried['poll_request_id'], self::requests($transport, '/activity-tasks/poll')[0]['body']['poll_request_id']);
        self::assertSame(['workflow', 'activity:7'], $calls);
    }

    #[DataProvider('successfulNoLeaseReplies')]
    public function testSuccessfulNoLeaseReleasesTheReservationWithoutSuppressingOrdinaryWork(array $reply): void
    {
        $transport = self::transport(static fn (): array => $reply);
        $calls = [];
        self::assertTrue(self::worker($transport, $calls)->tick(0));

        $carried = self::requests($transport, '/workflow-tasks/workflow-task/complete')[0]['body']['activity_poll'];
        self::assertNotSame($carried['poll_request_id'], self::requests($transport, '/activity-tasks/poll')[0]['body']['poll_request_id']);
        self::assertSame(['workflow', 'activity:7'], $calls);
    }

    public static function successfulNoLeaseReplies(): iterable
    {
        yield 'omitted carrier' => [['completed' => true]];
        yield 'explicit no lease' => [['activity_poll' => ['leased' => false]]];
    }

    public function testLostCompletionAcknowledgementRecoversTheOriginalIdDespiteDuplicateOmission(): void
    {
        $attempts = 0;
        $transport = self::transport(static function () use (&$attempts): array {
            if (++$attempts === 1) {
                throw new TransportException('The completion answer was lost.', 502);
            }

            return ['completed' => true];
        });
        $calls = [];
        self::assertTrue(self::worker($transport, $calls)->tick(0));

        $completed = self::requests($transport, '/workflow-tasks/workflow-task/complete');
        self::assertCount(2, $completed);
        self::assertSame($completed[0]['body'], $completed[1]['body']);
        self::assertSame($completed[0]['body']['activity_poll']['poll_request_id'],
            self::requests($transport, '/activity-tasks/poll')[0]['body']['poll_request_id']);
        self::assertSame(['workflow', 'activity:7'], $calls, 'Retry the completion, never its callback.');
        self::assertSame([], self::requests($transport, '/workflow-tasks/workflow-task/fail'));
    }

    #[DataProvider('shutdownReplies')]
    public function testShutdownFinishesOnlyAnAlreadyDeliveredActivityLease(array $result, bool $delivered): void
    {
        $worker = null;
        $transport = self::transport(static function () use (&$worker, $result): array {
            $worker->requestShutdown();

            return $result;
        });
        $calls = [];
        $worker = self::worker($transport, $calls);
        self::assertTrue($worker->tick(0));

        self::assertSame($delivered ? ['workflow', 'activity:7'] : ['workflow'], $calls);
        self::assertSame([], self::requests($transport, '/query-tasks/poll'));
        self::assertFalse($worker->tick(0), 'The drain must not clear the caller shutdown request.');
    }

    public static function shutdownReplies(): iterable
    {
        yield 'embedded granted activity' => [['activity_poll' => ['leased' => true, 'answer' => self::activityAnswer()]], true];
        yield 'older flag-only granted activity' => [['activity_poll' => ['leased' => true]], true];
        yield 'no grant' => [['completed' => true], false];
    }

    #[DataProvider('invalidResults')]
    public function testMalformedSidebandCannotTurnAnAcceptedParentCompletionIntoAParentFailure(mixed $result): void
    {
        $transport = self::transport(static fn (): array => ['activity_poll' => $result]);
        $calls = [];
        try {
            self::worker($transport, $calls)->tick(0);
            self::fail('A malformed carrier must remain a visible protocol failure.');
        } catch (ServerException $error) {
            self::assertSame('invalid_carried_poll_response', $error->reason);
        }
        self::assertSame(['workflow'], $calls);
        self::assertSame([], self::requests($transport, '/workflow-tasks/workflow-task/fail'));
        self::assertSame([], self::requests($transport, '/activity-tasks/poll'));
    }

    public static function invalidResults(): iterable
    {
        yield 'null' => [null];
        yield 'wrong leased type' => [['leased' => 1]];
        yield 'false with answer' => [['leased' => false, 'answer' => self::activityAnswer()]];
        $foreign = self::activityAnswer();
        $foreign['task']['lease_owner'] = 'another-worker';
        yield 'another worker grant' => [['leased' => true, 'answer' => $foreign]];
    }

    public function testNoActivityCarrierIsCreatedWithoutARegisteredActivityHandler(): void
    {
        $transport = self::transport(static fn (): array => ['completed' => true], true, false);
        $calls = [];
        $worker = self::worker($transport, $calls, false);
        self::assertTrue($worker->tick(0));
        self::assertArrayNotHasKey('activity_poll', self::requests($transport, '/workflow-tasks/workflow-task/complete')[0]['body']);
    }

    public function testInlineDeliveryDoesNotInventPollOrHttpRequestMetrics(): void
    {
        $transport = self::transport(static fn (): array => ['activity_poll' => ['leased' => true, 'answer' => self::activityAnswer()]]);
        $recorder = new RecordingMetricsRecorder();
        $calls = [];
        self::assertTrue(self::worker($transport, $calls, metrics: $recorder)->tick(0));

        self::assertSame([], array_values(array_filter($recorder->events, static fn (array $event): bool =>
            ($event['tags']['route'] ?? null) === '/worker/activity-tasks/poll'
            || (str_contains($event['name'], 'worker_poll') && ($event['tags']['task_kind'] ?? null) === 'activity'))));
        self::assertSame(['workflow', 'activity:7'], $calls);
    }

    public function testAnExecutingActivityLeavesNoFreeSlotForANestedWorkflowCarrier(): void
    {
        $workflowPolls = $activityPolls = 0;
        $completion = null;
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use (
            &$workflowPolls, &$activityPolls, &$completion,
        ): array {
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                return ++$workflowPolls === 1 ? ['task' => null, 'server_capabilities' => ['workflow_task_completion_activity_poll' => true]]
                    : self::workflowAnswer();
            }
            if (str_ends_with($uri, '/activity-tasks/poll')) {
                return ++$activityPolls === 1 ? self::activityAnswer() : ['task' => null];
            }
            if (str_ends_with($uri, '/workflow-tasks/workflow-task/complete')) {
                $completion = $body;
            }

            return ['completed' => true];
        });
        $worker = new Worker(new Client('https://server.example', transport: $transport), 'orders', workerId: 'worker-1');
        $calls = [];
        $worker->registerWorkflow('test.workflow', static function (WorkflowContext $context) use (&$calls): string {
            $calls[] = 'workflow';
            return 'done';
        });
        $worker->registerActivity('test.activity', static function (ActivityContext $context, int $value) use ($worker, &$calls): int {
            $calls[] = 'activity:start';
            $worker->tick(0);
            $calls[] = 'activity:end';
            return $value;
        });

        self::assertTrue($worker->tick(0));
        self::assertSame(['activity:start', 'workflow', 'activity:end'], $calls);
        self::assertIsArray($completion);
        self::assertArrayNotHasKey('activity_poll', $completion);
    }

    public function testTypedNormalizationRetryKeepsItsOriginalId(): void
    {
        $transport = self::normalizationTransport();
        $calls = [];
        self::assertFalse(self::worker($transport, $calls)->tick(0));
        $polls = self::requests($transport, '/workflow-tasks/poll');
        self::assertCount(2, $polls);
        self::assertSame($polls[0]['body']['poll_request_id'], $polls[1]['body']['poll_request_id']);
    }

    #[DataProvider('invalidNormalizationReplies')]
    public function testMalformedNormalizationStateRemainsAFatalProtocolError(array $replace): void
    {
        $transport = self::normalizationTransport($replace);
        $calls = [];
        try {
            self::worker($transport, $calls)->tick(0);
            self::fail('A malformed cutover refusal must not be retried as its typed state.');
        } catch (ServerException $error) {
            self::assertSame('workflow_poll_normalization_pending', $error->reason);
        }
        self::assertCount(1, self::requests($transport, '/workflow-tasks/poll'));
        self::assertSame([], $calls);
    }

    public static function invalidNormalizationReplies(): iterable
    {
        foreach ([['poll_request_id' => 'foreign-id'], ['retry_same_poll_request_id' => false],
            ['claim_admitted' => true], ['task' => ['task_id' => 'not-empty']],
            ['server_capabilities' => true], ['protocol_version' => '1.19'], ['extra' => true]] as $row) {
            yield [$row];
        }
    }

    #[DataProvider('completionDeliveryDelays')]
    public function testOriginalCompletionTransitGovernsTheReusedActivityHelper(int $delay, int $expectedCallbacks): void
    {
        $observed = ActivityGrantScenario::runWithRouter(static function (string $uri, string $path) use ($delay): array {
            file_put_contents($path, json_encode(['activity_count' => 2, 'lease_seconds' => 4,
                'delay_poll_delivery' => 2, 'poll_reply_delay_micros' => $delay], JSON_THROW_ON_ERROR));
            $worker = new Worker(new Client($uri, namespace: 'tenant', workerToken: 'fixture-worker'), 'queue', workerId: 'owner');
            (new \ReflectionProperty($worker, 'registered'))->setValue($worker, true);
            (new \ReflectionProperty($worker, 'lastHeartbeatAt'))->setValue($worker, microtime(true));
            (new \ReflectionMethod($worker, 'captureRegistrationFence'))->invoke($worker,
                ['server_capabilities' => ['activity_lease_renewal' => true]]);
            $calls = 0;
            $helpers = [];
            $worker->registerWorkflow('carry', static fn (WorkflowContext $context): string => 'done');
            $worker->registerActivity('slow', static function (ActivityContext $context) use ($worker, &$calls, &$helpers): int {
                $helper = (new \ReflectionProperty($worker, 'activityLeaseProcess'))->getValue($worker);
                $process = (new \ReflectionProperty($helper, 'process'))->getValue($helper);
                $helpers[] = proc_get_status($process)['pid'];
                return ++$calls;
            });
            try {
                self::assertTrue($worker->tick(0));
                self::assertTrue($worker->tick(0));
                $state = json_decode(file_get_contents($path), true, flags: JSON_THROW_ON_ERROR);

                return ['callbacks' => $calls, 'activity_wire_polls' => count(array_filter($state['wire_requests'],
                    static fn (string $path): bool => $path === '/api/worker/activity-tasks/poll')),
                    'parent_completions' => count($state['workflow_completions'] ?? []),
                    'helpers' => count(array_unique($helpers)), 'renewals' => $state['renewals'] ?? 0];
            } finally {
                $worker->requestShutdown();
            }
        }, __DIR__.'/Support/carried-activity-http.php');
        self::assertSame(['callbacks' => $expectedCallbacks, 'activity_wire_polls' => 1, 'parent_completions' => 1,
            'helpers' => 1, 'renewals' => 1], $observed);
    }

    public static function completionDeliveryDelays(): iterable
    {
        yield 'fresh completion grant' => [0, 2];
        yield 'expired during original completion transit' => [4_250_000, 1];
    }

    #[DataProvider('payloadRefusals')]
    public function testCarriedPayloadFailuresBelongToTheNextLeaseNotTheCompletedParent(int $status, bool $retryable): void
    {
        $requests = [];
        $carriedId = null;
        $blob = (new AvroPayloadCodec())->envelope([7])['blob'];
        $http = new HttpClient(['handler' => static function (RequestInterface $request) use (&$requests, &$carriedId, $blob, $status): FulfilledPromise {
            $path = $request->getUri()->getPath();
            $body = json_decode((string) $request->getBody(), true);
            $requests[] = ['path' => $path, 'body' => $body];
            if (str_contains($path, '/external-payloads/')) {
                return new FulfilledPromise(new Response($status, [], json_encode([
                    'reason' => $status === 403 ? 'external_payload_unauthorized' : 'external_payload_unavailable',
                    'retryable' => true,
                ], JSON_THROW_ON_ERROR)));
            }
            if (str_ends_with($path, '/workflow-tasks/poll')) {
                $reply = self::workflowAnswer();
            } elseif (str_ends_with($path, '/workflow-tasks/workflow-task/complete')) {
                $carriedId = $body['activity_poll']['poll_request_id'];
                $answer = self::activityAnswer();
                $answer['task']['arguments'] = ['codec' => 'avro', 'external_payload' => RuntimeExternalPayloadTest::reference($blob)];
                $reply = ['completed' => true, 'activity_poll' => ['leased' => true, 'answer' => $answer]];
            } elseif (str_ends_with($path, '/activity-tasks/poll')) {
                self::assertSame($carriedId, $body['poll_request_id']);
                $reply = self::activityAnswer();
            } else {
                $reply = ['task' => null, 'completed' => true];
            }

            return new FulfilledPromise(new Response(200, [], json_encode($reply, JSON_THROW_ON_ERROR)));
        }]);
        $calls = [];
        try {
            self::worker(new Psr18Transport($http), $calls)->tick(0);
            self::assertTrue($retryable, 'Permanent payload authorization failures must remain visible.');
        } catch (ServerException $error) {
            self::assertFalse($retryable, $error->getMessage());
            self::assertSame('external_payload_unauthorized', $error->reason);
        }
        self::assertSame($retryable ? ['workflow', 'activity:7'] : ['workflow'], $calls);
        self::assertCount(1, array_filter($requests, static fn (array $r): bool => str_ends_with($r['path'], '/workflow-tasks/workflow-task/complete')));
        self::assertSame([], array_values(array_filter($requests, static fn (array $r): bool => str_ends_with($r['path'], '/workflow-tasks/workflow-task/fail'))));
    }

    public static function payloadRefusals(): iterable
    {
        yield 'retryable payload outage' => [503, true];
        yield 'permanent payload authorization' => [403, false];
    }

    public function testCustomPayloadTransportConnectionFailureRecoversTheSameCarriedId(): void
    {
        $answer = self::activityAnswer();
        $blob = (new AvroPayloadCodec())->envelope([7])['blob'];
        $answer['task']['arguments'] = ['codec' => 'avro', 'external_payload' => RuntimeExternalPayloadTest::reference($blob)];
        $control = self::transport(static fn (): array => ['activity_poll' => ['leased' => true, 'answer' => $answer]]);
        $transport = new class($control) implements PayloadTransport {
            public function __construct(private readonly FakeTransport $control) {}

            public function send(string $method, string $uri, array $headers, ?array $body = null): array
            {
                return $this->control->send($method, $uri, $headers, $body);
            }

            public function fetchPayload(string $uri, array $headers, int $maxBytes): string
            {
                throw new TransportException('The custom payload connection was lost.', transientConnectionFailure: true);
            }
        };
        $calls = [];
        self::assertTrue(self::worker($transport, $calls)->tick(0));
        self::assertSame(['workflow', 'activity:7'], $calls);
        $completion = self::requests($control, '/workflow-tasks/workflow-task/complete');
        self::assertCount(1, $completion);
        self::assertSame($completion[0]['body']['activity_poll']['poll_request_id'],
            self::requests($control, '/activity-tasks/poll')[0]['body']['poll_request_id']);
        self::assertSame([], self::requests($control, '/workflow-tasks/workflow-task/fail'));
    }

    private static function normalizationTransport(array $replace = []): FakeTransport
    {
        $attempt = 0;
        return new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use (&$attempt, $replace): array {
            if (str_ends_with($uri, '/workflow-tasks/poll') && ++$attempt === 1) {
                throw new TransportException('normalization pending', 503, array_replace([
                    'task' => null, 'poll_status' => 'workflow_poll_normalization_pending',
                    'reason' => 'workflow_poll_normalization_pending', 'message' => 'workflow_poll_normalization_pending',
                    'retryable' => true, 'retry_after_seconds' => 1, 'poll_request_id' => $body['poll_request_id'],
                    'retry_same_poll_request_id' => true, 'claim_admitted' => false, 'protocol_version' => '1.20',
                    'server_capabilities' => ['workflow_task_completion_activity_poll' => true],
                ], $replace));
            }

            return ['task' => null, 'poll_status' => 'empty'];
        });
    }

    private static function worker(Transport $transport, array &$calls, bool $activity = true, ?MetricsRecorder $metrics = null): Worker
    {
        $clock = 0.0;
        $worker = new Worker(new Client('https://server.example', transport: $transport, metrics: $metrics), 'orders', workerId: 'worker-1',
            clock: static function () use (&$clock): float { return $clock; },
            sleeper: static function (int $us) use (&$clock): void { $clock += $us / 1_000_000; });
        $worker->registerWorkflow('test.workflow', static function (WorkflowContext $context) use (&$calls): string {
            $calls[] = 'workflow';

            return 'done';
        });
        if ($activity) {
            $worker->registerActivity('test.activity', static function (ActivityContext $context, int $value) use (&$calls): string {
                $calls[] = 'activity:'.$value;

                return 'activity:'.$value;
            });
        }

        return $worker;
    }

    private static function transport(\Closure $complete, mixed $capability = true, bool $activityAvailable = true): FakeTransport
    {
        return new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use ($complete, $capability, $activityAvailable): array {
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                $answer = self::workflowAnswer();
                $answer['server_capabilities'] = $capability === null ? [] : ['workflow_task_completion_activity_poll' => $capability];

                return $answer;
            }
            if (str_ends_with($uri, '/workflow-tasks/workflow-task/complete')) {
                return $complete($body);
            }
            if (str_ends_with($uri, '/activity-tasks/poll')) {
                return $activityAvailable ? self::activityAnswer() : ['task' => null, 'poll_status' => 'empty'];
            }
            if (str_ends_with($uri, '/activity-tasks/activity-task/complete')) {
                return ['recorded' => true, 'outcome' => 'completed'];
            }
            if (str_ends_with($uri, '/query-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }
            if (str_ends_with($uri, '/workflow-tasks/workflow-task/heartbeat')) {
                return ['task_id' => 'workflow-task', 'workflow_task_attempt' => 3, 'lease_owner' => 'worker-1', 'renewed' => true];
            }
            self::fail('Unexpected worker request: '.$uri);
        });
    }

    private static function workflowAnswer(): array
    {
        return self::answer(['task_id' => 'workflow-task', 'workflow_task_attempt' => 3, 'lease_owner' => 'worker-1',
            'workflow_id' => 'workflow', 'run_id' => 'run', 'workflow_type' => 'test.workflow',
            'payload_codec' => 'avro', 'history_events' => []]);
    }

    private static function activityAnswer(): array
    {
        return self::answer(['task_id' => 'activity-task', 'activity_attempt_id' => 'attempt-1', 'lease_owner' => 'worker-1',
            'activity_type' => 'test.activity', 'payload_codec' => 'avro', 'arguments' => (new AvroPayloadCodec())->envelope([7])]);
    }

    private static function answer(array $task): array
    {
        $now = new DateTimeImmutable('now', new DateTimeZone('UTC'));

        return ['task' => $task + ['lease_expires_at' => $now->modify('+30 seconds')->format('Y-m-d\TH:i:s.u\Z')],
            'poll_status' => 'leased', 'protocol_version' => '1.20',
            'server_capabilities' => ['workflow_task_completion_activity_poll' => true],
            'server_time' => $now->format('Y-m-d\TH:i:s.u\Z')];
    }

    private static function requests(FakeTransport $transport, string $suffix): array
    {
        return array_values(array_filter($transport->requests,
            static fn (array $request): bool => str_ends_with($request['uri'], $suffix)));
    }
}
