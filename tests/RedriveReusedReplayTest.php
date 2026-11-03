<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Tests\Support\ReplayRegressionFixture;
use DurableWorkflow\Worker\ReplayResult;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\TestCase;

/**
 * Redrive starts a new run whose history reuses completed steps of the failed
 * run. Each reused event carries reused_from_run_id and reused_recorded_at, and
 * a reused fired timer has a new timer_id and no timer row. A cold worker must
 * replay those steps as recorded, not run them again.
 */
final class RedriveReusedReplayTest extends TestCase
{
    private const SOURCE_RUN = '01JBREDRIVESOURCERUN000000';

    private const FIRST_ACTIVITY = 'Tests\\Fixtures\\V2\\TestRedriveFirstActivity';

    private const SECOND_ACTIVITY = 'Tests\\Fixtures\\V2\\TestRedriveSecondActivity';

    public function testCoreReusedMarkersFixtureReplaysColdToTheSameNextCommand(): void
    {
        $path = __DIR__.'/fixtures/replay-regressions/redrive-reused-markers-clock.json';
        self::assertReusedEvents($path, ['SideEffectRecorded', 'VersionMarkerRecorded']);

        $commands = ReplayRegressionFixture::executeFile($path);

        self::assertCount(1, $commands);
        self::assertSame('schedule_activity', $commands[0]['type']);
        self::assertSame(self::FIRST_ACTIVITY, $commands[0]['activity_type']);
        self::assertSame(['Taylor', '0123456789abcdef', 1], $commands[0]['arguments']);
    }

    public function testCoreReusedTimerFixtureReplaysColdWithoutWaitingOnTheTimer(): void
    {
        $path = __DIR__.'/fixtures/replay-regressions/redrive-reused-timer-clock.json';
        self::assertReusedEvents($path, ['ActivityCompleted', 'TimerFired']);

        $commands = ReplayRegressionFixture::executeFile($path);

        self::assertCount(1, $commands);
        self::assertSame('schedule_activity', $commands[0]['type']);
        self::assertSame(self::SECOND_ACTIVITY, $commands[0]['activity_type']);
        self::assertSame(['Hello, Taylor!'], $commands[0]['arguments']);
    }

    public function testReusedStepsAdvanceTheCancellationClockToTheirOriginalTime(): void
    {
        $path = __DIR__.'/fixtures/replay-regressions/redrive-reused-cancellation-clock.json';
        self::assertReusedEvents($path, ['ActivityCompleted', 'TimerFired']);

        $tasks = ReplayRegressionFixture::executeFile($path);
        self::assertCount(1, $tasks);
        $commands = $tasks[0]['command_sequence'];

        // Deadline 00:00:30 less the reused timer's original 00:00:12, not the
        // 00:05:00 the successor run copied it at, which would leave 0.0.
        self::assertCount(1, $commands);
        self::assertSame('complete_workflow', $commands[0]['type']);
        self::assertSame(18.0, $commands[0]['result']);
    }

    public function testReusedSideEffectReturnsTheRecordedValueWithoutRunningAgain(): void
    {
        $calls = 0;
        $result = $this->replay(
            static function (WorkflowContext $context) use (&$calls): mixed {
                return $context->sideEffect(static function () use (&$calls): string {
                    ++$calls;

                    return 'fresh-value';
                });
            },
            [self::reused('SideEffectRecorded', [
                'sequence' => 1,
                'result' => $this->codec()->envelope('recorded-value'),
            ])],
        );

        self::assertSame(0, $calls);
        self::assertSame(['complete_workflow'], array_column($result->commands, 'type'));
        self::assertSame('recorded-value', $this->codec()->decodeEnvelope($result->commands[0]['result']));
    }

    public function testReusedVersionMarkerReturnsTheRecordedVersionNotTheNewest(): void
    {
        $result = $this->replay(
            static fn (WorkflowContext $context): int => $context->getVersion('redrive-markers', -1, 1),
            [self::reused('VersionMarkerRecorded', [
                'sequence' => 1,
                'change_id' => 'redrive-markers',
                'version' => -1,
                'min_supported' => -1,
                'max_supported' => 1,
            ])],
        );

        self::assertSame(['complete_workflow'], array_column($result->commands, 'type'));
        self::assertSame(-1, $this->codec()->decodeEnvelope($result->commands[0]['result']));
    }

    public function testReusedFiredTimerWithANewTimerIdDoesNotWait(): void
    {
        $result = $this->replay(
            static function (WorkflowContext $context): string {
                $context->sleep(5);

                return 'after-timer';
            },
            [
                [
                    'event_type' => 'TimerScheduled',
                    'payload' => [
                        'timer_id' => '01JBREDRIVESUCCESSORTIMER0',
                        'sequence' => 1,
                        'delay_seconds' => 5,
                        'fire_at' => '2026-11-03T00:00:05.000000Z',
                    ],
                    'recorded_at' => '2026-11-03T00:05:00+00:00',
                ],
                self::reused('TimerFired', [
                    'timer_id' => '01JBREDRIVESUCCESSORTIMER0',
                    'sequence' => 1,
                    'delay_seconds' => 5,
                    'fired_at' => '2026-11-03T00:00:06.000000Z',
                ], '2026-11-03T00:00:06+00:00'),
            ],
        );

        self::assertSame(['complete_workflow'], array_column($result->commands, 'type'));
        self::assertSame('after-timer', $this->codec()->decodeEnvelope($result->commands[0]['result']));
    }

    public function testReusedActivityCompletionReturnsTheRecordedResultWithoutRescheduling(): void
    {
        $result = $this->replay(
            static function (WorkflowContext $context): mixed {
                $greeting = $context->activity(self::FIRST_ACTIVITY, ['Taylor']);

                return $context->activity(self::SECOND_ACTIVITY, [$greeting]);
            },
            [self::reused('ActivityCompleted', [
                'sequence' => 1,
                'activity_type' => self::FIRST_ACTIVITY,
                'activity_execution_id' => '01JBREDRIVESUCCESSORACT000',
                'reused_activity_execution_id' => '01JBREDRIVESOURCEACT00000',
                'result' => $this->codec()->envelope('Hello, Taylor!'),
            ])],
        );

        self::assertCount(1, $result->commands);
        self::assertSame('schedule_activity', $result->commands[0]['type']);
        self::assertSame(self::SECOND_ACTIVITY, $result->commands[0]['activity_type']);
        self::assertSame(['Hello, Taylor!'], $this->codec()->decodeEnvelope($result->commands[0]['arguments']));
    }

    /**
     * @param array<string, mixed> $payload
     * @return array<string, mixed>
     */
    private static function reused(
        string $eventType,
        array $payload,
        string $recordedAt = '2026-11-03T00:00:00+00:00',
    ): array {
        return [
            'event_type' => $eventType,
            'payload' => $payload + [
                'reused_from_run_id' => self::SOURCE_RUN,
                'reused_recorded_at' => $recordedAt,
            ],
            'recorded_at' => '2026-11-03T00:05:00+00:00',
        ];
    }

    /** @param list<string> $eventTypes */
    private static function assertReusedEvents(string $path, array $eventTypes): void
    {
        $fixture = json_decode((string) file_get_contents($path), true, flags: JSON_THROW_ON_ERROR);
        self::assertIsArray($fixture);
        $reused = [];
        foreach ($fixture['history'] as $event) {
            $payload = $event['payload'];
            if (!array_key_exists('reused_from_run_id', $payload)) {
                continue;
            }
            self::assertSame(self::SOURCE_RUN, $payload['reused_from_run_id']);
            self::assertIsString($payload['reused_recorded_at'] ?? null);
            self::assertNotSame($event['timestamp'] ?? $event['recorded_at'], $payload['reused_recorded_at']);
            $reused[] = $event['event_type'];
        }

        self::assertSame($eventTypes, $reused);
    }

    /**
     * @param callable(WorkflowContext): mixed $workflow
     * @param list<array<string, mixed>> $history
     */
    private function replay(callable $workflow, array $history): ReplayResult
    {
        return (new Replayer($this->codec()))->replay($workflow, $history, [], 'redrive-replay', [
            'workflow_id' => 'redrive-workflow',
            'run_id' => '01JBREDRIVESUCCESSORRUN000',
        ]);
    }

    private function codec(): AvroPayloadCodec
    {
        return new AvroPayloadCodec();
    }
}
