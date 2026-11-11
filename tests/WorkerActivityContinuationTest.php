<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DateTimeImmutable;
use DateTimeZone;
use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/** Server #1078: an activity completion that carries the run's next workflow commands. */
final class WorkerActivityContinuationTest extends TestCase
{
    private const CONTRACT = ['version' => 1,
        'history_endpoint' => '/worker/activity-tasks/{taskId}/continuation/history',
        'complete_endpoint' => '/worker/activity-tasks/{taskId}/continuation/complete',
        'supported_commands' => ['schedule_activity', 'start_timer', 'complete_workflow', 'record_version_marker'],
        'max_history_events' => 256, 'max_commands' => 16];

    public function testAcceptedContinuationSendsTheReplayedCommandsWithTheResult(): void
    {
        $calls = [];
        $transport = self::transport(static fn (): array => self::accepted());
        $worker = self::worker($transport, $calls, static fn (WorkflowContext $context): int => $context->activity('test.activity') + 1);
        self::assertTrue($worker->tick(0));
        self::assertSame(['activity'], $calls, 'The activity ran once.');
        self::assertSame([], self::requests($transport, '/activity-tasks/activity/complete'));
        $sent = self::requests($transport, '/activity-tasks/activity/continuation/complete');
        self::assertCount(1, $sent);
        $body = $sent[0]['body'];
        self::assertSame(['version', 'history_through_sequence', 'history_last_event_id', 'commands'], array_keys($body['continuation']));
        self::assertSame(1, $body['continuation']['history_through_sequence']);
        self::assertSame('event-1', $body['continuation']['history_last_event_id']);
        self::assertCount(1, $body['continuation']['commands']);
        self::assertSame('complete_workflow', $body['continuation']['commands'][0]['type']);
        self::assertSame(8, (new AvroPayloadCodec())->decodeEnvelope($body['continuation']['commands'][0]['result']));
        self::assertSame(7, (new AvroPayloadCodec())->decodeEnvelope($body['result']));
        self::assertSame('worker', $body['workflow_poll']['worker_id']);
        // The accepted answer omits workflow_poll: the carried lease is never drained.
        $worker->tick(0);
        self::assertSame(['activity'], $calls);
        $carriedId = $body['workflow_poll']['poll_request_id'];
        foreach (self::requests($transport, '/workflow-tasks/poll') as $poll) {
            self::assertNotSame($carriedId, $poll['body']['poll_request_id'] ?? null);
        }
    }

    public function testAcceptedContinuationSweepsTheNextTickWithoutWaiting(): void
    {
        // Server #1129: the accepted continuation already scheduled the next
        // activity, so the next tick must not hold a workflow poll first.
        $calls = [];
        $transport = self::transport(static fn (): array => self::accepted());
        $worker = self::worker($transport, $calls, static fn (WorkflowContext $context): int => $context->activity('test.activity') + 1);
        self::assertTrue($worker->tick(0));
        $before = count(self::requests($transport, '/workflow-tasks/poll'));
        $worker->tick(5);
        $polls = self::requests($transport, '/workflow-tasks/poll');
        self::assertGreaterThan($before, count($polls));
        self::assertSame(0, $polls[$before]['body']['timeout_seconds'], 'The first poll after an accepted continuation does not wait.');
    }

    public function testDeclinedContinuationKeepsTheOrdinaryPollTimeout(): void
    {
        $calls = [];
        $transport = self::transport(static fn (): array => ['task_id' => 'activity', 'outcome' => 'completed', 'recorded' => true,
            'next_task_id' => 'workflow', 'continuation' => ['version' => 1, 'outcome' => 'declined', 'reason' => 'no_workflow_task'],
            'workflow_poll' => ['leased' => false]]);
        $worker = self::worker($transport, $calls, static fn (WorkflowContext $context): int => $context->activity('test.activity') + 1);
        self::assertTrue($worker->tick(0));
        $before = count(self::requests($transport, '/workflow-tasks/poll'));
        $worker->tick(5);
        $polls = self::requests($transport, '/workflow-tasks/poll');
        self::assertSame(5, $polls[$before]['body']['timeout_seconds'] ?? null);
    }

    #[DataProvider('ordinaryPaths')]
    public function testUnsupportedOrDisabledContinuationKeepsTheOrdinaryCompletion(bool $enabled, mixed $contract, ?array $history): void
    {
        $calls = [];
        $transport = self::transport(static fn (): array => self::accepted(), $contract, $history);
        $worker = self::worker($transport, $calls, static fn (WorkflowContext $context): int => $context->activity('test.activity') + 1,
            enabled: $enabled);
        self::assertTrue($worker->tick(0));
        self::assertSame([], self::requests($transport, '/continuation/complete'));
        self::assertCount(1, self::requests($transport, '/activity-tasks/activity/complete'));
    }

    public static function ordinaryPaths(): iterable
    {
        yield 'option off' => [false, self::CONTRACT, null];
        yield 'no capability' => [true, null, null];
        yield 'other version' => [true, ['version' => 2] + self::CONTRACT, null];
        yield 'ineligible run' => [true, self::CONTRACT, ['task_id' => 'activity', 'activity_attempt_id' => 'activity-attempt',
            'continuation' => ['version' => 1, 'eligible' => false, 'reason' => 'pending_workflow_task']]];
        yield 'prefix count mismatch' => [true, self::CONTRACT, ['history_through_sequence' => 2] + self::history()];
    }

    #[DataProvider('clockReaders')]
    public function testReadingBusinessTimeAfterTheResultAbandonsSpeculation(\Closure $workflow): void
    {
        $calls = [];
        $transport = self::transport(static fn (): array => self::accepted());
        $worker = self::worker($transport, $calls, $workflow);
        self::assertTrue($worker->tick(0));
        self::assertCount(1, self::requests($transport, '/continuation/history'));
        self::assertSame([], self::requests($transport, '/continuation/complete'));
        self::assertCount(1, self::requests($transport, '/activity-tasks/activity/complete'));
    }

    public static function clockReaders(): iterable
    {
        yield 'uncaught read' => [static function (WorkflowContext $context): string {
            $context->activity('test.activity');

            return $context->replayNow()->format(DATE_ATOM);
        }];
        yield 'caught read still counts' => [static function (WorkflowContext $context): string {
            $context->activity('test.activity');
            try {
                return $context->replayNow()->format(DATE_ATOM);
            } catch (\Throwable) {
                return 'branch-the-server-would-not-see';
            }
        }];
    }

    public function testDeclinedContinuationFinishesTheCarriedLeaseTheOrdinaryWay(): void
    {
        $calls = [];
        $transport = self::transport(static fn (): array => ['task_id' => 'activity', 'outcome' => 'completed', 'recorded' => true,
            'next_task_id' => 'workflow', 'continuation' => ['version' => 1, 'outcome' => 'declined', 'reason' => 'history_changed'],
            'workflow_poll' => ['leased' => true, 'answer' => self::workflowAnswer()]]);
        $worker = self::worker($transport, $calls, static fn (WorkflowContext $context): int => $context->activity('test.activity') + 1);
        self::assertTrue($worker->tick(0));
        self::assertCount(1, self::requests($transport, '/continuation/complete'));
        self::assertTrue($worker->tick(0));
        self::assertSame(['activity'], $calls);
        self::assertCount(1, self::requests($transport, '/workflow-tasks/workflow/complete'), 'The carried lease is finished ordinarily.');
    }

    private static function worker(FakeTransport $transport, array &$calls, \Closure $workflow, bool $enabled = true): Worker
    {
        $worker = new Worker(new Client('https://continuation.invalid', transport: $transport), 'queue', workerId: 'worker',
            enableActivityContinuation: $enabled);
        $worker->registerWorkflow('test.workflow', static fn (WorkflowContext $context): mixed => $workflow($context));
        $worker->registerActivity('test.activity', static function (ActivityContext $context) use (&$calls): int {
            $calls[] = 'activity';

            return 7;
        });

        return $worker;
    }

    private static function transport(\Closure $continue, mixed $contract = self::CONTRACT, ?array $history = null): FakeTransport
    {
        $workflows = $activities = 0;
        $caps = ['activity_completion_workflow_poll' => true] + ($contract === null ? [] : ['activity_workflow_continuation' => $contract]);

        return new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use (
            $continue, $caps, $history, &$workflows, &$activities,
        ): array {
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                return ++$workflows === 1 ? ['task' => null, 'poll_status' => 'empty', 'server_capabilities' => $caps] : self::workflowAnswer();
            }
            if (str_ends_with($uri, '/activity-tasks/poll')) {
                return ++$activities === 1 ? ['task' => ['task_id' => 'activity', 'activity_attempt_id' => 'activity-attempt',
                    'activity_execution_id' => 'execution-1', 'attempt_number' => 1, 'lease_owner' => 'worker',
                    'activity_type' => 'test.activity', 'payload_codec' => 'avro'],
                    'poll_status' => 'leased', 'server_capabilities' => $caps] : ['task' => null, 'poll_status' => 'empty'];
            }
            if (str_ends_with($uri, '/activity-tasks/activity/continuation/history')) return $history ?? self::history();
            if (str_ends_with($uri, '/activity-tasks/activity/continuation/complete')) return $continue($body);
            if (str_ends_with($uri, '/activity-tasks/activity/complete')) return ['task_id' => 'activity', 'recorded' => true];
            if (str_ends_with($uri, '/workflow-tasks/workflow/complete')) return ['completed' => true];
            if (str_ends_with($uri, '/workflow-tasks/workflow/heartbeat')) {
                return ['task_id' => 'workflow', 'workflow_task_attempt' => 1, 'lease_owner' => 'worker', 'renewed' => true];
            }
            if (str_ends_with($uri, '/query-tasks/poll')) return ['task' => null, 'poll_status' => 'empty'];
            self::fail('Unexpected continuation request: '.$uri);
        });
    }

    private static function history(): array
    {
        return ['task_id' => 'activity', 'activity_attempt_id' => 'activity-attempt',
            'continuation' => ['version' => 1, 'eligible' => true, 'reason' => null],
            'workflow_id' => 'workflow-instance', 'run_id' => 'run', 'workflow_type' => 'test.workflow',
            'payload_codec' => 'avro', 'arguments' => (new AvroPayloadCodec())->envelope([]),
            'history_through_sequence' => 1, 'history_last_event_id' => 'event-1',
            'history_events' => [['id' => 'event-1', 'sequence' => 1, 'event_type' => 'ActivityScheduled',
                'payload' => ['sequence' => 1, 'activity_execution_id' => 'execution-1', 'activity_type' => 'test.activity',
                    'activity' => ['type' => 'test.activity']],
                'recorded_at' => '2026-11-09T12:00:00.000000Z', 'workflow_task_id' => 'workflow-0', 'workflow_command_id' => null]]];
    }

    private static function accepted(): array
    {
        return ['task_id' => 'activity', 'activity_attempt_id' => 'activity-attempt', 'outcome' => 'completed', 'recorded' => true,
            'reason' => null, 'next_task_id' => 'workflow',
            'continuation' => ['version' => 1, 'outcome' => 'accepted', 'workflow_task_id' => 'workflow', 'workflow_task_attempt' => 1,
                'completion' => ['recorded' => true, 'run_status' => 'completed']]];
    }

    private static function workflowAnswer(): array
    {
        $now = new DateTimeImmutable('now', new DateTimeZone('UTC'));
        $completed = ['id' => 'event-2', 'sequence' => 2, 'event_type' => 'ActivityCompleted',
            'payload' => ['sequence' => 1, 'activity_execution_id' => 'execution-1', 'activity' => ['type' => 'test.activity'],
                'result' => (new AvroPayloadCodec())->envelope(7), 'payload_codec' => 'avro'],
            'recorded_at' => $now->format('Y-m-d\TH:i:s.u\Z')];

        return ['task' => ['task_id' => 'workflow', 'workflow_task_attempt' => 1, 'lease_owner' => 'worker',
            'workflow_id' => 'workflow-instance', 'run_id' => 'run', 'workflow_type' => 'test.workflow',
            'payload_codec' => 'avro', 'arguments' => (new AvroPayloadCodec())->envelope([]),
            'history_events' => [self::history()['history_events'][0], $completed],
            'lease_expires_at' => $now->modify('+30 seconds')->format('Y-m-d\TH:i:s.u\Z')],
            'poll_status' => 'leased', 'protocol_version' => '1.20', 'server_capabilities' => ['activity_completion_workflow_poll' => true],
            'server_time' => $now->format('Y-m-d\TH:i:s.u\Z')];
    }

    private static function requests(FakeTransport $transport, string $suffix): array
    {
        return array_values(array_filter($transport->requests, static fn (array $row): bool => str_ends_with($row['uri'], $suffix)));
    }
}
