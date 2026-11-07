<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DateTimeImmutable;
use DateTimeZone;
use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Tests\Support\RecordingMetricsRecorder;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class WorkerWorkflowCompletionPollTest extends TestCase
{
    #[DataProvider('inlineCarrierKinds')]
    public function testWorkflowCompletionEmbedsTheNextWorkflowWithoutAPollOrInventedMetrics(bool $activityProbe): void
    {
        $reply = ['workflow_poll' => ['leased' => true, 'answer' => self::workflowAnswer('next')]];
        if ($activityProbe) $reply['activity_poll'] = ['leased' => false];
        $transport = self::transport(static fn (): array => $reply, activityCapability: $activityProbe);
        $calls = [];
        $metrics = new RecordingMetricsRecorder();
        $worker = self::worker($transport, $calls, $metrics);
        self::assertTrue($worker->tick(0));
        self::assertSame(['workflow:first'], $calls);
        self::assertTrue($worker->tick(0));
        self::assertSame(['workflow:first', 'workflow:next'], $calls);
        self::assertCount(1, self::requests($transport, '/workflow-tasks/poll'));
        $carried = self::completion($transport)['workflow_poll'];
        self::assertSame('worker', $carried['worker_id']);
        self::assertSame('queue', $carried['task_queue']);
        self::assertSame(['workflow'], $carried['task_kinds']);
        self::assertSame(0, $carried['timeout_seconds']);
        self::assertSame($activityProbe, array_key_exists('activity_poll', self::completion($transport)));
        self::assertCount(1, array_filter($metrics->events, static fn (array $event): bool => $event['method'] === 'increment'
            && ($event['tags']['route'] ?? null) === '/worker/workflow-tasks/poll'));
        self::assertCount(1, array_filter($metrics->events, static fn (array $event): bool => $event['method'] === 'increment'
            && str_contains($event['name'], 'worker_poll') && ($event['tags']['task_kind'] ?? null) === 'workflow'));
    }

    public static function inlineCarrierKinds(): iterable
    {
        yield 'workflow carrier only' => [false];
        yield 'activity probe finds nothing then workflow is leased' => [true];
    }

    #[DataProvider('unavailableCapabilities')]
    public function testOldServersKeepTheirOriginalCompletionBodyAndPolls(mixed $capability): void
    {
        $transport = self::transport(static fn (): array => ['completed' => true], $capability);
        $calls = [];
        $worker = self::worker($transport, $calls);
        $worker->tick(0);
        $worker->tick(0);
        self::assertSame(['workflow:first', 'workflow:next'], $calls);
        self::assertArrayNotHasKey('workflow_poll', self::completion($transport));
        self::assertCount(2, self::requests($transport, '/workflow-tasks/poll'));
    }

    public static function unavailableCapabilities(): iterable
    {
        foreach ([null, false, 1, 'true'] as $value) yield [$value];
    }

    #[DataProvider('noLeaseReplies')]
    public function testSuccessfulMissingWorkflowResultReleasesCapacityWithoutSuppressingPolling(array $reply): void
    {
        $transport = self::transport(static fn (): array => $reply);
        $calls = [];
        $worker = self::worker($transport, $calls);
        $worker->tick(0);
        $worker->tick(0);
        self::assertSame(['workflow:first', 'workflow:next'], $calls);
        self::assertNotSame(self::completion($transport)['workflow_poll']['poll_request_id'],
            self::requests($transport, '/workflow-tasks/poll')[1]['body']['poll_request_id']);
    }

    public static function noLeaseReplies(): iterable
    {
        yield 'omitted workflow result' => [['completed' => true]];
        yield 'explicit false' => [['workflow_poll' => ['leased' => false]]];
    }

    public function testFlagOnlyReplyUsesTheCarriedWorkflowId(): void
    {
        $transport = self::transport(static fn (): array => ['workflow_poll' => ['leased' => true]]);
        $calls = [];
        $worker = self::worker($transport, $calls);
        $worker->tick(0);
        $worker->tick(0);
        self::assertSame(['workflow:first', 'workflow:next'], $calls);
        self::assertSame(self::completion($transport)['workflow_poll']['poll_request_id'],
            self::requests($transport, '/workflow-tasks/poll')[1]['body']['poll_request_id']);
    }

    public function testActivityFirstOmissionDoesNotPretendToHaveLeasedAWorkflow(): void
    {
        $transport = self::transport(static fn (): array => ['activity_poll' => ['leased' => true, 'answer' => self::activityAnswer()]], activityCapability: true);
        $calls = [];
        $worker = self::worker($transport, $calls);
        $worker->tick(0);
        self::assertSame(['workflow:first', 'activity'], $calls);
        self::assertSame([], self::requests($transport, '/activity-tasks/poll'));
        self::assertArrayHasKey('activity_poll', self::completion($transport));
        $worker->tick(0);
        self::assertSame(['workflow:first', 'activity', 'workflow:next'], $calls);
        self::assertNotSame(self::completion($transport)['workflow_poll']['poll_request_id'],
            self::requests($transport, '/workflow-tasks/poll')[1]['body']['poll_request_id']);
    }

    public function testUnknownCompletionCannotTreatADuplicateOmissionAsANoLeaseResult(): void
    {
        $attempt = 0;
        $transport = self::transport(static function () use (&$attempt): array {
            if (++$attempt === 1) throw new TransportException('Completion answer lost.', 502);
            return ['completed' => true];
        });
        $calls = [];
        $worker = self::worker($transport, $calls);
        $worker->tick(0);
        $worker->tick(0);
        self::assertSame(['workflow:first', 'workflow:next'], $calls);
        $completions = self::requests($transport, '/workflow-tasks/first/complete');
        self::assertCount(2, $completions);
        self::assertSame($completions[0]['body'], $completions[1]['body']);
        self::assertSame($completions[0]['body']['workflow_poll']['poll_request_id'],
            self::requests($transport, '/workflow-tasks/poll')[1]['body']['poll_request_id']);
        self::assertSame([], self::requests($transport, '/workflow-tasks/first/fail'));
    }

    #[DataProvider('shutdownReplies')]
    public function testShutdownDrainsOnlyTheAlreadyDeliveredWorkflow(array $reply, bool $delivered): void
    {
        $worker = null;
        $transport = self::transport(static function () use ($reply, &$worker): array {
            $worker->requestShutdown();
            return $reply;
        });
        $calls = [];
        $worker = self::worker($transport, $calls);
        self::assertTrue($worker->tick(0));
        self::assertSame($delivered ? ['workflow:first', 'workflow:next'] : ['workflow:first'], $calls);
        self::assertSame([], self::requests($transport, '/activity-tasks/poll'));
        self::assertSame([], self::requests($transport, '/query-tasks/poll'));
        self::assertFalse($worker->tick(0));
        if ($delivered) self::assertArrayNotHasKey('workflow_poll', self::requests($transport, '/workflow-tasks/next/complete')[0]['body']);
    }

    public static function shutdownReplies(): iterable
    {
        yield 'embedded workflow' => [['workflow_poll' => ['leased' => true, 'answer' => self::workflowAnswer('next')]], true];
        yield 'flag-only workflow' => [['workflow_poll' => ['leased' => true]], true];
        yield 'no delivered workflow' => [['completed' => true], false];
    }

    public function testBothPositiveCarrierResultsRemainAnErrorOutsideTheCompletedParent(): void
    {
        $transport = self::transport(static fn (): array => [
            'activity_poll' => ['leased' => true, 'answer' => self::activityAnswer()],
            'workflow_poll' => ['leased' => true, 'answer' => self::workflowAnswer('next')],
        ], activityCapability: true);
        $calls = [];
        try {
            self::worker($transport, $calls)->tick(0);
            self::fail('The producer cannot deliver both task kinds from one completion.');
        } catch (ServerException $error) {
            self::assertSame('invalid_carried_poll_response', $error->reason);
        }
        self::assertSame(['workflow:first'], $calls);
        self::assertSame([], self::requests($transport, '/workflow-tasks/first/fail'));
    }

    public function testACompletingNestedWorkflowCannotReserveTheStillExecutingOuterWorkflowSlot(): void
    {
        $polls = 0;
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use (&$polls): array {
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                $answer = self::workflowAnswer(++$polls === 1 ? 'outer' : 'inner');
                $answer['server_capabilities'] = ['workflow_task_completion_workflow_poll' => true];
                return $answer;
            }
            if (str_ends_with($uri, '/poll')) return ['task' => null, 'poll_status' => 'empty',
                'server_capabilities' => ['workflow_task_completion_workflow_poll' => true]];
            if (str_ends_with($uri, '/complete')) return ['completed' => true];
            self::fail('Unexpected nested workflow request: '.$uri);
        });
        $worker = new Worker(new Client('https://server.example', transport: $transport), 'queue', workerId: 'worker');
        $calls = [];
        $worker->registerWorkflow('test.workflow', static function (WorkflowContext $context, string $name) use ($worker, &$calls): string {
            $calls[] = $name.':start';
            if ($name === 'outer') $worker->tick(0);
            $calls[] = $name.':end';
            return $name;
        });
        self::assertTrue($worker->tick(0));
        self::assertSame(['outer:start', 'inner:start', 'inner:end', 'outer:end'], $calls);
        self::assertArrayNotHasKey('workflow_poll', self::requests($transport, '/workflow-tasks/inner/complete')[0]['body']);
        self::assertArrayHasKey('workflow_poll', self::requests($transport, '/workflow-tasks/outer/complete')[0]['body']);
    }

    private static function worker(FakeTransport $transport, array &$calls, ?RecordingMetricsRecorder $metrics = null): Worker
    {
        $clock = 0.0;
        $worker = new Worker(new Client('https://server.example', transport: $transport, metrics: $metrics), 'queue', workerId: 'worker',
            clock: static function () use (&$clock): float { return $clock; },
            sleeper: static function (int $us) use (&$clock): void { $clock += $us / 1_000_000; });
        $worker->registerWorkflow('test.workflow', static function (WorkflowContext $context, string $name) use (&$calls): string {
            $calls[] = 'workflow:'.$name;
            return $name;
        });
        $worker->registerActivity('test.activity', static function (ActivityContext $context) use (&$calls): string {
            $calls[] = 'activity';
            return 'done';
        });
        return $worker;
    }

    private static function transport(\Closure $complete, mixed $capability = true, bool $activityCapability = false): FakeTransport
    {
        $polls = 0;
        return new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use (
            $complete, $capability, $activityCapability, &$polls,
        ): array {
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                if (++$polls === 1) {
                    $answer = self::workflowAnswer('first');
                    $answer['server_capabilities'] = $capability === null ? [] : ['workflow_task_completion_workflow_poll' => $capability,
                        'workflow_task_completion_activity_poll' => $activityCapability];
                    return $answer;
                }
                return self::workflowAnswer('next');
            }
            if (str_ends_with($uri, '/workflow-tasks/first/complete')) return $complete($body);
            if (str_ends_with($uri, '/workflow-tasks/next/complete') || str_ends_with($uri, '/activity-tasks/activity/complete')) return ['completed' => true];
            if (str_ends_with($uri, '/activity-tasks/poll') || str_ends_with($uri, '/query-tasks/poll')) return ['task' => null, 'poll_status' => 'empty'];
            self::fail('Unexpected workflow completion carrier request: '.$uri);
        });
    }

    private static function workflowAnswer(string $name): array
    {
        return self::answer(['task_id' => $name, 'workflow_task_attempt' => 2, 'lease_owner' => 'worker',
            'workflow_id' => 'workflow-'.$name, 'run_id' => 'run-'.$name, 'workflow_type' => 'test.workflow', 'payload_codec' => 'avro',
            'arguments' => (new AvroPayloadCodec())->envelope([$name]), 'history_events' => []]);
    }

    private static function activityAnswer(): array
    {
        return self::answer(['task_id' => 'activity', 'activity_attempt_id' => 'attempt', 'lease_owner' => 'worker',
            'activity_type' => 'test.activity', 'payload_codec' => 'avro']);
    }

    private static function answer(array $task): array
    {
        $now = new DateTimeImmutable('now', new DateTimeZone('UTC'));
        return ['task' => $task + ['lease_expires_at' => $now->modify('+30 seconds')->format('Y-m-d\TH:i:s.u\Z')],
            'poll_status' => 'leased', 'protocol_version' => '1.20', 'server_capabilities' => [], 'server_time' => $now->format('Y-m-d\TH:i:s.u\Z')];
    }

    private static function completion(FakeTransport $transport): array
    {
        return self::requests($transport, '/workflow-tasks/first/complete')[0]['body'];
    }

    private static function requests(FakeTransport $transport, string $suffix): array
    {
        return array_values(array_filter($transport->requests, static fn (array $row): bool => str_ends_with($row['uri'], $suffix)));
    }
}
