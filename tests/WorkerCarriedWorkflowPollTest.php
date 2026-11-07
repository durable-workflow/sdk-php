<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DateTimeImmutable;
use DateTimeZone;
use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\ExternalPayloadException;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Transport\PayloadTransport;
use DurableWorkflow\Transport\Transport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class WorkerCarriedWorkflowPollTest extends TestCase
{
    public function testActivityCompletionDeliversTheNextWorkflowWithoutAnotherPoll(): void
    {
        $transport = self::transport(static fn (): array => ['workflow_poll' => ['leased' => true, 'answer' => self::workflowAnswer()]]);
        $calls = [];
        $worker = self::worker($transport, $calls);
        self::assertTrue($worker->tick(0));
        self::assertSame(['activity'], $calls, 'An ordinary tick still executes at most one workflow.');
        self::assertTrue($worker->tick(0));
        self::assertSame(['activity', 'workflow'], $calls);
        self::assertCount(1, self::requests($transport, '/workflow-tasks/poll'));
        $carried = self::requests($transport, '/activity-tasks/activity/complete')[0]['body']['workflow_poll'];
        self::assertSame('worker', $carried['worker_id']);
        self::assertSame('queue', $carried['task_queue']);
        self::assertSame(['workflow'], $carried['task_kinds']);
        self::assertSame(0, $carried['timeout_seconds']);
        self::assertGreaterThan(0, $carried['history_page_size']);
    }

    #[DataProvider('noCapability')]
    public function testOldServersKeepTheOriginalCompletionAndPollPath(mixed $capability): void
    {
        $transport = self::transport(static fn (): array => ['completed' => true], $capability);
        $calls = [];
        $worker = self::worker($transport, $calls);
        self::assertTrue($worker->tick(0));
        self::assertTrue($worker->tick(0));
        self::assertArrayNotHasKey('workflow_poll', self::requests($transport, '/activity-tasks/activity/complete')[0]['body']);
        self::assertCount(2, self::requests($transport, '/workflow-tasks/poll'));
        self::assertSame(['activity', 'workflow'], $calls);
    }

    public static function noCapability(): iterable
    {
        foreach ([null, false, 'true', 1] as $value) yield [$value];
    }

    public function testFlagOnlyAnswerRecoversUnderTheSameId(): void
    {
        $transport = self::transport(static fn (): array => ['workflow_poll' => ['leased' => true]]);
        $calls = [];
        $worker = self::worker($transport, $calls);
        $worker->tick(0);
        $worker->tick(0);
        self::assertSame(['activity', 'workflow'], $calls);
        self::assertSame(self::carriedId($transport), self::requests($transport, '/workflow-tasks/poll')[1]['body']['poll_request_id']);
    }

    #[DataProvider('noLease')]
    public function testSuccessfulOmissionReleasesOnlyTheReservation(array $reply): void
    {
        $transport = self::transport(static fn (): array => $reply);
        $calls = [];
        $worker = self::worker($transport, $calls);
        $worker->tick(0);
        $worker->tick(0);
        self::assertSame(['activity', 'workflow'], $calls);
        self::assertNotSame(self::carriedId($transport), self::requests($transport, '/workflow-tasks/poll')[1]['body']['poll_request_id']);
    }

    public static function noLease(): iterable
    {
        yield 'omitted result' => [['completed' => true]];
        yield 'explicit false' => [['workflow_poll' => ['leased' => false]]];
    }

    public function testLostActivityAcknowledgementCannotRotateTheCarriedIdOrRepeatTheCallback(): void
    {
        $attempts = 0;
        $transport = self::transport(static function () use (&$attempts): array {
            if (++$attempts === 1) throw new TransportException('Acknowledgement lost.', 502);
            return ['completed' => true];
        });
        $calls = [];
        $worker = self::worker($transport, $calls);
        $worker->tick(0);
        $worker->tick(0);
        self::assertSame(['activity', 'workflow'], $calls);
        $completions = self::requests($transport, '/activity-tasks/activity/complete');
        self::assertCount(2, $completions);
        self::assertSame($completions[0]['body'], $completions[1]['body']);
        self::assertSame(self::carriedId($transport), self::requests($transport, '/workflow-tasks/poll')[1]['body']['poll_request_id']);
        self::assertSame([], self::requests($transport, '/activity-tasks/activity/fail'));
    }

    #[DataProvider('shutdownAnswers')]
    public function testShutdownDrainsAKnownWorkflowLeaseWithoutClearingTheStop(array $reply, bool $delivered): void
    {
        $worker = null;
        $transport = self::transport(static function () use ($reply, &$worker): array {
            $worker->requestShutdown();
            return $reply;
        });
        $calls = [];
        $worker = self::worker($transport, $calls);
        self::assertTrue($worker->tick(0));
        self::assertSame($delivered ? ['activity', 'workflow'] : ['activity'], $calls);
        self::assertSame([], self::requests($transport, '/query-tasks/poll'));
        self::assertFalse($worker->tick(0));
    }

    public static function shutdownAnswers(): iterable
    {
        yield 'embedded grant' => [['workflow_poll' => ['leased' => true, 'answer' => self::workflowAnswer()]], true];
        yield 'flag-only grant' => [['workflow_poll' => ['leased' => true]], true];
        yield 'no grant' => [['completed' => true], false];
    }

    #[DataProvider('malformedAnswers')]
    public function testMalformedWorkflowResultCannotFailTheCompletedActivity(mixed $reply): void
    {
        $transport = self::transport(static fn (): array => ['workflow_poll' => $reply]);
        $calls = [];
        try {
            self::worker($transport, $calls)->tick(0);
            self::fail('A malformed workflow carrier must remain visible.');
        } catch (ServerException $error) {
            self::assertSame('invalid_carried_poll_response', $error->reason);
        }
        self::assertSame(['activity'], $calls);
        self::assertSame([], self::requests($transport, '/activity-tasks/activity/fail'));
    }

    public static function malformedAnswers(): iterable
    {
        yield 'null' => [null];
        yield 'false with answer' => [['leased' => false, 'answer' => self::workflowAnswer()]];
        $validation = self::workflowAnswer();
        $validation['task']['task_kind'] = 'update_validation';
        yield 'validation in workflow-only answer' => [['leased' => true, 'answer' => $validation]];
        $foreign = self::workflowAnswer();
        $foreign['task']['lease_owner'] = 'foreign-worker';
        yield 'foreign worker answer' => [['leased' => true, 'answer' => $foreign]];
    }

    public function testNoWorkflowHandlerMeansNoReservedWorkflowPoll(): void
    {
        $transport = self::transport(static fn (): array => ['completed' => true]);
        $worker = new Worker(new Client('https://server.example', transport: $transport), 'queue', workerId: 'worker');
        $worker->registerActivity('test.activity', static fn (ActivityContext $context): int => 1);
        self::assertTrue($worker->tick(0));
        self::assertArrayNotHasKey('workflow_poll', self::requests($transport, '/activity-tasks/activity/complete')[0]['body']);
    }

    public function testAnExecutingWorkflowPreventsANestedActivityFromReservingItsSlot(): void
    {
        $workflowPolls = $activityPolls = 0;
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use (
            &$workflowPolls, &$activityPolls,
        ): array {
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                return ++$workflowPolls === 1 ? self::workflowAnswer()
                    : ['task' => null, 'server_capabilities' => ['activity_completion_workflow_poll' => true]];
            }
            if (str_ends_with($uri, '/activity-tasks/poll')) {
                return ++$activityPolls === 1 ? ['task' => ['task_id' => 'activity', 'activity_attempt_id' => 'attempt',
                    'activity_type' => 'test.activity', 'payload_codec' => 'avro', 'lease_owner' => 'worker'],
                    'server_capabilities' => ['activity_completion_workflow_poll' => true]] : ['task' => null];
            }

            return ['completed' => true];
        });
        $worker = new Worker(new Client('https://server.example', transport: $transport), 'queue', workerId: 'worker');
        $calls = [];
        $worker->registerWorkflow('test.workflow', static function (WorkflowContext $context, int $value) use ($worker, &$calls): string {
            $calls[] = 'workflow:start';
            $worker->tick(0);
            $calls[] = 'workflow:end';
            return 'done';
        });
        $worker->registerActivity('test.activity', static function (ActivityContext $context) use (&$calls): int {
            $calls[] = 'activity';
            return 1;
        });
        self::assertTrue($worker->tick(0));
        self::assertSame(['workflow:start', 'activity', 'workflow:end'], $calls);
        self::assertArrayNotHasKey('workflow_poll', self::requests($transport, '/activity-tasks/activity/complete')[0]['body']);
    }

    #[DataProvider('payloadFailures')]
    public function testWorkflowPayloadFailuresCannotBecomeActivityFailureAcknowledgements(bool $transient): void
    {
        $answer = self::workflowAnswer();
        $blob = (new AvroPayloadCodec())->envelope([7])['blob'];
        $answer['task']['arguments'] = ['codec' => 'avro', 'external_payload' => RuntimeExternalPayloadTest::reference($blob)];
        $control = self::transport(static fn (): array => ['workflow_poll' => ['leased' => true, 'answer' => $answer]]);
        $transport = new class($control, $transient) implements PayloadTransport {
            public function __construct(private readonly FakeTransport $control, private readonly bool $transient) {}

            public function send(string $method, string $uri, array $headers, ?array $body = null): array
            {
                return $this->control->send($method, $uri, $headers, $body);
            }

            public function fetchPayload(string $uri, array $headers, int $maxBytes): string
            {
                if ($this->transient) throw new TransportException('Payload connection lost.', transientConnectionFailure: true);
                throw new ExternalPayloadException('Payload authorization refused.', 403, 'external_payload_unauthorized');
            }
        };
        $calls = [];
        $worker = self::worker($transport, $calls);
        $worker->tick(0);
        try {
            $worker->tick(0);
            self::assertTrue($transient);
            self::assertSame(self::carriedId($control), self::requests($control, '/workflow-tasks/poll')[1]['body']['poll_request_id']);
        } catch (ServerException $error) {
            self::assertFalse($transient);
            self::assertSame('external_payload_unauthorized', $error->reason);
        }
        self::assertSame($transient ? ['activity', 'workflow'] : ['activity'], $calls);
        self::assertCount(1, self::requests($control, '/activity-tasks/activity/complete'));
        self::assertSame([], self::requests($control, '/activity-tasks/activity/fail'));
    }

    public static function payloadFailures(): iterable
    {
        yield 'transient connection' => [true];
        yield 'permanent authorization' => [false];
    }

    #[DataProvider('completionTransit')]
    public function testOriginalActivityCompletionTransitIsChargedBeforeWorkflowDispatch(bool $delayed, bool $lostAck, bool $flagOnly): void
    {
        $answer = self::workflowAnswer();
        $answer['server_time'] = '2026-07-18T00:00:00.000000Z';
        $answer['task']['lease_expires_at'] = $delayed ? '2026-07-18T00:00:10.050000Z' : '2026-07-18T00:00:30.000000Z';
        $attempts = 0;
        $transport = self::transport(static function () use ($answer, $delayed, $lostAck, $flagOnly, &$attempts): array {
            if (++$attempts === 1) {
                if ($delayed) usleep(120_000);
                if ($lostAck) throw new TransportException('Completion answer lost.', 502);
            }
            return ['workflow_poll' => $flagOnly ? ['leased' => true] : ['leased' => true, 'answer' => $answer]];
        }, followup: $answer);
        $calls = [];
        $worker = self::worker($transport, $calls);
        $worker->tick(0);
        $worker->tick(0);
        self::assertSame(['activity', 'workflow'], $calls);
        $heartbeats = self::requests($transport, '/workflow-tasks/workflow/heartbeat');
        self::assertCount($delayed ? 1 : 0, $heartbeats);
        if ($delayed) self::assertSame(['lease_owner' => 'worker', 'workflow_task_attempt' => 3], $heartbeats[0]['body']);
        self::assertCount($lostAck ? 2 : 1, self::requests($transport, '/activity-tasks/activity/complete'));
        if ($flagOnly) self::assertSame(self::carriedId($transport), self::requests($transport, '/workflow-tasks/poll')[1]['body']['poll_request_id']);
    }

    public static function completionTransit(): iterable
    {
        yield 'fresh inline grant' => [false, false, false];
        yield 'completion transit consumes margin' => [true, false, false];
        yield 'lost acknowledgement retains original transit' => [true, true, false];
        yield 'ordinary recovery retains original transit' => [true, false, true];
    }

    private static function worker(Transport $transport, array &$calls): Worker
    {
        $now = 0.0;
        $worker = new Worker(new Client('https://server.example', transport: $transport), 'queue', workerId: 'worker',
            clock: static function () use (&$now): float { return $now; },
            sleeper: static function (int $us) use (&$now): void { $now += $us / 1_000_000; });
        $worker->registerWorkflow('test.workflow', static function (WorkflowContext $context, int $value) use (&$calls): string {
            $calls[] = 'workflow';
            return 'workflow-result';
        });
        $worker->registerActivity('test.activity', static function (ActivityContext $context) use (&$calls): string {
            $calls[] = 'activity';
            return 'activity-result';
        });
        return $worker;
    }

    private static function transport(\Closure $complete, mixed $capability = true, ?array $followup = null): FakeTransport
    {
        $workflows = $activities = 0;
        return new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use (
            $complete, $capability, $followup, &$workflows, &$activities,
        ): array {
            $caps = $capability === null ? [] : ['activity_completion_workflow_poll' => $capability];
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                return ++$workflows === 1 ? ['task' => null, 'poll_status' => 'empty', 'server_capabilities' => $caps]
                    : ($followup ?? self::workflowAnswer());
            }
            if (str_ends_with($uri, '/activity-tasks/poll')) {
                return ++$activities === 1 ? ['task' => ['task_id' => 'activity', 'activity_attempt_id' => 'activity-attempt',
                    'lease_owner' => 'worker', 'activity_type' => 'test.activity', 'payload_codec' => 'avro'],
                    'poll_status' => 'leased', 'server_capabilities' => $caps] : ['task' => null, 'poll_status' => 'empty'];
            }
            if (str_ends_with($uri, '/activity-tasks/activity/complete')) return $complete($body);
            if (str_ends_with($uri, '/workflow-tasks/workflow/complete')) return ['completed' => true];
            if (str_ends_with($uri, '/workflow-tasks/workflow/heartbeat')) {
                return ['task_id' => 'workflow', 'workflow_task_attempt' => 3, 'lease_owner' => 'worker', 'renewed' => true];
            }
            if (str_ends_with($uri, '/query-tasks/poll')) return ['task' => null, 'poll_status' => 'empty'];
            self::fail('Unexpected workflow carrier request: '.$uri);
        });
    }

    private static function workflowAnswer(): array
    {
        $now = new DateTimeImmutable('now', new DateTimeZone('UTC'));
        return ['task' => ['task_id' => 'workflow', 'workflow_task_attempt' => 3, 'lease_owner' => 'worker',
            'workflow_id' => 'workflow-instance', 'run_id' => 'run', 'workflow_type' => 'test.workflow',
            'payload_codec' => 'avro', 'arguments' => (new AvroPayloadCodec())->envelope([7]),
            'history_events' => [], 'lease_expires_at' => $now->modify('+30 seconds')->format('Y-m-d\TH:i:s.u\Z')],
            'poll_status' => 'leased', 'protocol_version' => '1.20', 'server_capabilities' => ['activity_completion_workflow_poll' => true],
            'server_time' => $now->format('Y-m-d\TH:i:s.u\Z')];
    }

    private static function carriedId(FakeTransport $transport): string
    {
        return self::requests($transport, '/activity-tasks/activity/complete')[0]['body']['workflow_poll']['poll_request_id'];
    }

    private static function requests(FakeTransport $transport, string $suffix): array
    {
        return array_values(array_filter($transport->requests, static fn (array $row): bool => str_ends_with($row['uri'], $suffix)));
    }
}
