<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DateTimeImmutable;
use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ReplayClockUnavailable;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkerReplayClock;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/** The Server's `replay_clock` entry carrier (WorkerReplayClock v1, .github#215). */
final class EntryBusinessReplayClockTest extends TestCase
{
    private const FORMAT = 'Y-m-d\TH:i:s.uP';
    private const STARTED = '2026-11-12T10:00:00.123456Z';
    private const ENTRY = '2026-11-12T10:00:00.123456+00:00';

    public function testAValidCarrierKeepsItsMicrosecondsInUtc(): void
    {
        $clock = WorkerReplayClock::fromTask(self::carrier(), 'run');

        self::assertNotNull($clock);
        self::assertSame(self::ENTRY, $clock->startedAt->format(self::FORMAT));
        self::assertSame(WorkerReplayClock::RUN_STARTED, $clock->source);
        self::assertSame('2026-11-12T10:00:00.000000+00:00', WorkerReplayClock::fromTask(
            self::carrier(['started_at' => '2026-11-12T10:00:00.000000Z']), 'run',
        )?->startedAt->format(self::FORMAT));
        self::assertSame(WorkerReplayClock::REDRIVE_ANCHOR, WorkerReplayClock::fromTask(
            self::carrier(['source' => 'redrive_replayed_started_at']), 'run',
        )?->source);
    }

    #[DataProvider('invalidCarriers')]
    public function testAnInvalidCarrierIsAbsent(mixed $carrier, string $runId = 'run'): void
    {
        self::assertNull(WorkerReplayClock::fromTask($carrier, $runId));
    }

    public static function invalidCarriers(): array
    {
        $carrier = self::carrier();
        $without = static function (string $field) use ($carrier): array {
            unset($carrier[$field]);

            return $carrier;
        };

        return [
            'absent' => [null],
            'scalar' => [self::STARTED],
            'list' => [array_values($carrier)],
            'empty' => [[]],
            'missing schema' => [$without('schema')],
            'other schema' => [self::carrier(['schema' => 'durable-workflow.worker-replay-clock/v2'])],
            'missing run' => [$without('workflow_run_id')],
            'other run' => [self::carrier(['workflow_run_id' => 'other-run'])],
            'non-string run' => [self::carrier(['workflow_run_id' => 7]), '7'],
            'task without run' => [self::carrier(['workflow_run_id' => '']), ''],
            'missing source' => [$without('source')],
            'unknown source' => [self::carrier(['source' => 'history_inserted_at'])],
            'integer source' => [self::carrier(['source' => 7])],
            'array source' => [self::carrier(['source' => ['run_started_at']])],
            'extra key' => [self::carrier(['lease_started_at' => self::STARTED])],
            'integer key' => [self::carrier() + [0 => 'extra']],
            'missing started_at' => [$without('started_at')],
            'null started_at' => [self::carrier(['started_at' => null])],
            'numeric started_at' => [self::carrier(['started_at' => 1762941600])],
            'whole seconds' => [self::carrier(['started_at' => '2026-11-12T10:00:00Z'])],
            'milliseconds' => [self::carrier(['started_at' => '2026-11-12T10:00:00.123Z'])],
            'seven digits' => [self::carrier(['started_at' => '2026-11-12T10:00:00.1234567Z'])],
            'offset' => [self::carrier(['started_at' => '2026-11-12T10:00:00.123456+00:00'])],
            'lowercase z' => [self::carrier(['started_at' => '2026-11-12T10:00:00.123456z'])],
            'space separator' => [self::carrier(['started_at' => '2026-11-12 10:00:00.123456Z'])],
            'february 30' => [self::carrier(['started_at' => '2026-02-30T10:00:00.000000Z'])],
            'hour 24' => [self::carrier(['started_at' => '2026-11-12T24:00:00.000000Z'])],
            'minute 60' => [self::carrier(['started_at' => '2026-11-12T10:60:00.000000Z'])],
            'trailing newline' => [self::carrier(['started_at' => self::STARTED."\n"])],
        ];
    }

    public function testEntryReadsTheCarrierIdenticallyOnEveryReplayAndLease(): void
    {
        $handler = static function (WorkflowContext $context): array {
            $first = $context->replayNow();
            self::assertSame($first, $context->replayNow());

            return [$first->format(self::FORMAT), $context->replayNow()->format(self::FORMAT)];
        };
        $task = self::task();

        $first = self::replayResult($handler, self::ordinaryStart(), $task);
        $cold = self::replayResult($handler, self::ordinaryStart(), $task);

        self::assertSame([self::ENTRY, self::ENTRY], $first);
        self::assertSame($first, $cold);
    }

    public function testHistoryInsertionTimeIsNotTheEntryInstant(): void
    {
        $history = self::ordinaryStart();
        $history[0]['recorded_at'] = '2026-11-12T10:00:07.000000+00:00';

        self::assertSame(self::ENTRY, self::replayResult(
            static fn (WorkflowContext $context): string => $context->replayNow()->format(self::FORMAT), $history, self::task(),
        ));
    }

    public function testTheFirstConsumedBoundaryReplacesTheEntryInstant(): void
    {
        $history = [...self::ordinaryStart(), ...self::activity(1, '2026-11-12T10:00:04.250000+00:00'),
            ...self::timer(2, '2026-11-12T10:00:09.500000+00:00')];

        self::assertSame([self::ENTRY, '2026-11-12T10:00:04.250000+00:00', '2026-11-12T10:00:09.500000+00:00'],
            self::replayResult(self::threeReads(), $history, self::task()));
        self::assertSame([null, '2026-11-12T10:00:04.250000+00:00', '2026-11-12T10:00:09.500000+00:00'],
            self::replayResult(self::threeReads(), $history, ['workflow_id' => 'workflow', 'run_id' => 'run']));
    }

    public function testAnInlineResultClearsTheEntryInstantLikeAnyBoundary(): void
    {
        $history = [...self::ordinaryStart(), [
            'event_type' => 'SideEffectRecorded', 'recorded_at' => '2026-11-12T10:00:02Z',
            'payload' => ['sequence' => 1, 'result' => (new AvroPayloadCodec())->envelope('fixed')],
        ]];

        self::assertSame([self::ENTRY, null], self::replayResult(static function (WorkflowContext $context): array {
            $entry = self::read($context);
            $context->sideEffect(static fn (): string => 'fixed');

            return [$entry, self::read($context)];
        }, $history, self::task()));
    }

    public function testADeadlineLoopFromEntryTerminates(): void
    {
        $history = [...self::ordinaryStart(), ...self::timer(1, '2026-11-12T10:00:02.123456+00:00'),
            ...self::timer(2, '2026-11-12T10:00:04.123456+00:00')];

        self::assertSame([self::ENTRY, '2026-11-12T10:00:02.123456+00:00', '2026-11-12T10:00:04.123456+00:00'],
            self::replayResult(static function (WorkflowContext $context): array {
                $start = $context->replayNow();
                $deadline = $start->modify('+4 seconds');
                $times = [$start->format(self::FORMAT)];
                while ($context->replayNow() < $deadline) {
                    $context->sleep(2);
                    $times[] = $context->replayNow()->format(self::FORMAT);
                }

                return $times;
            }, $history, self::task()));
    }

    #[DataProvider('unusableTasks')]
    public function testAnAbsentOrUnusableCarrierKeepsEntryUnavailable(array $task): void
    {
        $this->expectException(ReplayClockUnavailable::class);
        self::replayResult(static fn (WorkflowContext $context): DateTimeImmutable => $context->replayNow(), self::ordinaryStart(), $task);
    }

    public static function unusableTasks(): array
    {
        return [
            'absent' => [['workflow_id' => 'workflow', 'run_id' => 'run']],
            'other run' => [self::task(['workflow_run_id' => 'other-run'])],
            'malformed time' => [self::task(['started_at' => '2026-11-12T10:00:00Z'])],
            'redrive source on an ordinary history' => [self::task(['source' => 'redrive_replayed_started_at'])],
            'no run on the task' => [['workflow_id' => 'workflow', 'replay_clock' => self::carrier()]],
        ];
    }

    public function testAHistoryWithoutOneStartIgnoresTheCarrier(): void
    {
        $this->expectException(ReplayClockUnavailable::class);
        self::replayResult(static fn (WorkflowContext $context): DateTimeImmutable => $context->replayNow(),
            [...self::ordinaryStart(), ...self::ordinaryStart()], self::task());
    }

    public function testAProvedRedriveEntryIsUnchangedByAnyCarrier(): void
    {
        $fixture = self::redriveFixture();
        $expected = ['2026-11-03T00:00:00.000000+00:00', '2026-11-03T00:00:00.000000+00:00', '2026-11-03T00:00:06.000000+00:00'];
        $task = $fixture['workflow_tasks'][0];
        $carrier = ['schema' => WorkerReplayClock::SCHEMA, 'workflow_run_id' => $task['run_id'], 'source' => 'redrive_replayed_started_at'];

        foreach ([
            'agreeing' => $carrier + ['started_at' => '2026-11-03T00:00:00.000000Z'],
            'disagreeing' => $carrier + ['started_at' => '2026-11-02T23:00:00.000000Z'],
            'ordinary source' => ['source' => 'run_started_at'] + $carrier + ['started_at' => '2026-11-03T00:05:00.000000Z'],
        ] as $case => $replayClock) {
            self::assertSame($expected, self::replayResult(self::redriveReads(), $fixture['history'], $task + ['replay_clock' => $replayClock]), $case);
        }
    }

    public function testAnUnprovedRedriveTakesOnlyItsEntryFromAMatchingCarrier(): void
    {
        $fixture = self::redriveFixture();
        $fixture['history'][0]['payload']['resume_step_sequence'] = '3';
        $task = $fixture['workflow_tasks'][0];
        $carrier = ['schema' => WorkerReplayClock::SCHEMA, 'workflow_run_id' => $task['run_id'], 'started_at' => '2026-11-03T00:00:00.000000Z'];

        self::assertSame(['2026-11-03T00:00:00.000000+00:00', null, null], self::replayResult(self::redriveReads(), $fixture['history'],
            $task + ['replay_clock' => $carrier + ['source' => 'redrive_replayed_started_at']]));
        self::assertSame([null, null, null], self::replayResult(self::redriveReads(), $fixture['history'],
            $task + ['replay_clock' => $carrier + ['source' => 'run_started_at']]));
        self::assertSame([null, null, null], self::replayResult(self::redriveReads(), $fixture['history'], $task));
        // The start still records a readable original start; a carrier that disagrees with it is ignored.
        self::assertSame([null, null, null], self::replayResult(self::redriveReads(), $fixture['history'],
            $task + ['replay_clock' => ['started_at' => '2026-11-02T23:00:00.000000Z'] + $carrier + ['source' => 'redrive_replayed_started_at']]));
        unset($fixture['history'][0]['payload']['replayed_started_at']);
        self::assertSame(['2026-11-03T00:00:00.000000+00:00', null, null], self::replayResult(self::redriveReads(), $fixture['history'],
            $task + ['replay_clock' => $carrier + ['source' => 'redrive_replayed_started_at']]));
    }

    public function testAnUpdateTaskWithACarrierRunsOnlyItsHandler(): void
    {
        // Update tasks run the handler against a history snapshot. They never
        // re-run the workflow body, so its entry read cannot diverge there.
        $completedCommand = null;
        $codecClient = new Client('https://server.example', transport: new FakeTransport());
        $arguments = $codecClient->payloadCodec()->envelope([41]);
        $transport = new FakeTransport(handler: static function (
            string $method,
            string $uri,
            array $headers,
            ?array $body,
        ) use (&$completedCommand, $arguments): ?array {
            if (str_ends_with($uri, '/api/worker/register')) {
                return ['registered' => true];
            }
            if (str_ends_with($uri, '/api/worker/workflow-tasks/poll')) {
                return ['poll_status' => 'leased', 'task' => [
                    'task_id' => 'update-task-1', 'workflow_task_attempt' => 1, 'lease_owner' => 'worker-1',
                    'workflow_id' => 'workflow', 'run_id' => 'run', 'workflow_type' => 'clocked',
                    'payload_codec' => 'avro', 'workflow_update_id' => 'update-1', 'replay_clock' => self::carrier(),
                    'history_events' => [...self::ordinaryStart(), [
                        'event_type' => 'UpdateAccepted',
                        'payload' => ['update_id' => 'update-1', 'update_name' => 'increment', 'arguments' => $arguments],
                    ]],
                ]];
            }
            if (str_ends_with($uri, '/api/worker/workflow-tasks/update-task-1/heartbeat')) {
                return ['task_id' => 'update-task-1', 'workflow_task_attempt' => 1, 'lease_owner' => 'worker-1',
                    'renewed' => true, 'reason' => null];
            }
            if (str_ends_with($uri, '/api/worker/workflow-tasks/update-task-1/complete')) {
                $completedCommand = $body['commands'][0] ?? null;

                return ['completed' => true];
            }
            if (str_ends_with($uri, '/api/worker/activity-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
            }
            self::fail("Unexpected worker request: {$method} {$uri}");
        });
        $client = new Client('https://server.example', transport: $transport);
        $bodyRuns = 0;
        $handlerClock = null;
        (new Worker($client, 'queue', workerId: 'worker-1'))
            ->registerWorkflow('clocked', static function (WorkflowContext $context) use (&$bodyRuns): string {
                $bodyRuns++;

                return $context->replayNow()->format(self::FORMAT);
            })
            ->registerUpdate('clocked', 'increment', static function (QueryContext $context, int $value) use (&$handlerClock): int {
                try {
                    $handlerClock = $context->replayNow()->format(self::FORMAT);
                } catch (ReplayClockUnavailable) {
                    $handlerClock = 'unavailable';
                }

                return $value + 1;
            })
            ->run(0);

        self::assertSame(0, $bodyRuns);
        self::assertSame('unavailable', $handlerClock);
        self::assertIsArray($completedCommand);
        self::assertSame('complete_update', $completedCommand['type'] ?? null);
        self::assertSame('update-1', $completedCommand['update_id'] ?? null);
        self::assertSame(42, $client->payloadCodec()->decodeEnvelope($completedCommand['result'] ?? []));
    }

    public function testAnOrdinaryStartNamingAnotherRunIgnoresTheCarrier(): void
    {
        $history = self::ordinaryStart();
        $history[0]['payload']['workflow_run_id'] = 'another-run';
        $entryRead = static fn (WorkflowContext $context): ?string => self::read($context);
        self::assertNull(self::replayResult($entryRead, $history, self::task()));
        unset($history[0]['payload']['workflow_run_id']);
        self::assertSame(self::ENTRY, self::replayResult($entryRead, $history, self::task()));
    }

    public function testCancellationStillHidesTheEntryInstant(): void
    {
        $this->expectException(ReplayClockUnavailable::class);
        self::replayResult(static fn (WorkflowContext $context): DateTimeImmutable => $context->replayNow(),
            self::ordinaryStart(), self::task() + ['cancel_requested' => true]);
    }

    /** @param array<string, mixed> $overrides */
    private static function carrier(array $overrides = []): array
    {
        return array_merge([
            'schema' => 'durable-workflow.worker-replay-clock/v1',
            'workflow_run_id' => 'run',
            'started_at' => self::STARTED,
            'source' => 'run_started_at',
        ], $overrides);
    }

    /** @param array<string, mixed> $overrides */
    private static function task(array $overrides = []): array
    {
        return ['workflow_id' => 'workflow', 'run_id' => 'run', 'replay_clock' => self::carrier($overrides)];
    }

    /** @return list<array<string, mixed>> */
    private static function ordinaryStart(): array
    {
        return [[
            'event_type' => 'WorkflowStarted',
            'recorded_at' => '2026-11-12T10:00:00.500000+00:00',
            'payload' => ['workflow_instance_id' => 'workflow', 'workflow_run_id' => 'run'],
        ]];
    }

    /** @return list<array<string, mixed>> */
    private static function activity(int $sequence, string $timestamp): array
    {
        $payload = ['sequence' => $sequence, 'activity_execution_id' => 'activity-'.$sequence, 'activity_type' => 'anchor'];

        return [
            ['event_type' => 'ActivityScheduled', 'payload' => $payload],
            ['event_type' => 'ActivityCompleted', 'recorded_at' => $timestamp,
                'payload' => [...$payload, 'result' => (new AvroPayloadCodec())->envelope('fixed')]],
        ];
    }

    /** @return list<array<string, mixed>> */
    private static function timer(int $sequence, string $timestamp): array
    {
        $payload = ['sequence' => $sequence, 'timer_id' => 'timer-'.$sequence, 'delay_seconds' => 2];

        return [
            ['event_type' => 'TimerScheduled', 'payload' => $payload],
            ['event_type' => 'TimerFired', 'recorded_at' => $timestamp, 'payload' => $payload],
        ];
    }

    private static function read(WorkflowContext $context): ?string
    {
        try {
            return $context->replayNow()->format(self::FORMAT);
        } catch (ReplayClockUnavailable) {
            return null;
        }
    }

    private static function threeReads(): callable
    {
        return static function (WorkflowContext $context): array {
            $times = [self::read($context)];
            $context->activity('anchor');
            $times[] = self::read($context);
            $context->sleep(2);
            $times[] = self::read($context);

            return $times;
        };
    }

    private static function redriveReads(): callable
    {
        return static function (WorkflowContext $context): array {
            $times = [self::read($context)];
            $context->activity('Tests\\Fixtures\\V2\\TestRedriveFirstActivity', ['Taylor']);
            $times[] = self::read($context);
            $context->sleep(5);
            $times[] = self::read($context);

            return $times;
        };
    }

    private static function redriveFixture(): array
    {
        return json_decode((string) file_get_contents(__DIR__.'/fixtures/replay-regressions/business-replay-clock-canonical-redrive.json'),
            true, flags: JSON_THROW_ON_ERROR);
    }

    /** @param list<array<string, mixed>> $history */
    private static function replayResult(callable $handler, array $history, array $task): mixed
    {
        $codec = new AvroPayloadCodec();
        $result = (new Replayer($codec))->replay($handler, $history, [], 'php-workers', $task);

        return $codec->decodeEnvelope($result->commands[array_key_last($result->commands)]['result']);
    }
}
