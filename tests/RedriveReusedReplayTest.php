<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Tests\Support\RedriveSuccessorHistory;
use DurableWorkflow\Tests\Support\ReplayRegressionFixture;
use DurableWorkflow\Worker\ReplayResult;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
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

    public function testCoreReusedUpsertsFixtureReplaysColdWithTheRecordedValues(): void
    {
        $path = __DIR__.'/fixtures/replay-regressions/redrive-reused-upserts-clock.json';
        self::assertReusedEvents($path, ['MemoUpserted', 'SearchAttributesUpserted']);
        self::assertValidSuccessor($path, 3);
        $events = self::eventsByType($path);
        $memo = $events['MemoUpserted']['payload'];
        self::assertSame(['customer' => 'Taylor', 'stage' => 'greeting'], $this->codec()->decodeEnvelope($memo['entries']));
        self::assertSame(['customer' => 'Taylor', 'stage' => 'greeting'], $this->codec()->decodeEnvelope($memo['merged']));
        $attributes = $events['SearchAttributesUpserted']['payload'];
        self::assertSame(['Stage' => 'greeting'], $attributes['attributes']);
        self::assertSame(['Stage' => 'greeting'], $attributes['merged']);

        $tasks = ReplayRegressionFixture::executeFile($path);
        self::assertCount(1, $tasks);
        $commands = $tasks[0]['command_sequence'];

        // Neither upsert is emitted again; replay goes straight to the first activity.
        self::assertCount(1, $commands);
        self::assertSame('schedule_activity', $commands[0]['type']);
        self::assertSame(self::FIRST_ACTIVITY, $commands[0]['activity_type']);
        self::assertSame(['Taylor'], $commands[0]['arguments']);
    }

    public function testReusedUpsertsAreNotEmittedAgainAndTheRecordedMemoBindsReplay(): void
    {
        $history = self::fixture(__DIR__.'/fixtures/replay-regressions/redrive-reused-upserts-clock.json')['history'];
        $workflow = static function (string $stage): callable {
            return static function (WorkflowContext $context, string $name) use ($stage): mixed {
                $context->upsertMemo(['stage' => $stage, 'customer' => $name]);
                $context->upsertSearchAttributes(['Stage' => 'greeting']);

                return $context->activity(self::FIRST_ACTIVITY, [$name]);
            };
        };

        // Without the reused events the same code upserts both again.
        $fresh = $this->replay($workflow('greeting'), [$history[0]], ['Taylor']);
        self::assertSame(['upsert_memo', 'upsert_search_attributes', 'schedule_activity'], array_column($fresh->commands, 'type'));

        $replayed = $this->replay($workflow('greeting'), $history, ['Taylor']);
        self::assertSame(['schedule_activity'], array_column($replayed->commands, 'type'));

        $this->expectException(NonDeterministicWorkflow::class);
        $this->replay($workflow('changed'), $history, ['Taylor']);
    }

    public function testCoreReusedSignalWaitFixtureReplaysColdWithTheRecordedSignal(): void
    {
        $path = __DIR__.'/fixtures/replay-regressions/redrive-reused-signal-wait-clock.json';
        self::assertReusedEvents($path, ['SignalWaitOpened', 'SignalReceived', 'SignalApplied']);
        self::assertValidSuccessor($path, 2);
        $events = self::eventsByType($path);
        self::assertSame(['Grace'], $this->codec()->decode($events['SignalReceived']['payload']['arguments']));
        self::assertSame('Grace', $this->codec()->decode($events['SignalApplied']['payload']['value']));

        $tasks = ReplayRegressionFixture::executeFile($path);
        self::assertCount(1, $tasks);
        $commands = $tasks[0]['command_sequence'];

        // The wait is already satisfied, so no condition wait is opened again.
        self::assertCount(1, $commands);
        self::assertSame('schedule_activity', $commands[0]['type']);
        self::assertSame(self::FIRST_ACTIVITY, $commands[0]['activity_type']);
        self::assertSame(['Taylor'], $commands[0]['arguments']);
    }

    public function testReusedSignalWaitDeliversTheRecordedValueWithoutWaitingAgain(): void
    {
        $history = self::fixture(__DIR__.'/fixtures/replay-regressions/redrive-reused-signal-wait-clock.json')['history'];
        $approvedBy = [];
        $workflow = static function (WorkflowContext $context, string $name) use (&$approvedBy): mixed {
            $context->waitCondition(static fn (): bool => $context->signals('approve') !== [], 'approve');
            $approvedBy = $context->signals('approve');

            return $context->activity(self::FIRST_ACTIVITY, [$name]);
        };

        // Without the reused events the same code opens the wait.
        $fresh = $this->replay($workflow, [$history[0]], ['Taylor']);
        self::assertSame(['open_condition_wait'], array_column($fresh->commands, 'type'));

        $replayed = $this->replay($workflow, $history, ['Taylor']);
        self::assertSame([['Grace']], $approvedBy);
        self::assertSame(['schedule_activity'], array_column($replayed->commands, 'type'));
    }

    public function testCancellationAfterReusedUpsertsMeasuresFromItsSuccessorDelivery(): void
    {
        $path = __DIR__.'/fixtures/replay-regressions/redrive-reused-upserts-cancellation-clock.json';
        self::assertReusedEvents($path, ['MemoUpserted', 'SearchAttributesUpserted']);
        self::assertValidSuccessor($path, 3);

        $tasks = ReplayRegressionFixture::executeFile($path);
        self::assertCount(1, $tasks);
        $commands = $tasks[0]['command_sequence'];

        // Redrive writes WorkflowStarted and the reused prefix together, so a
        // successor cancellation is requested and delivered after it. The clock
        // starts at that delivery (00:05:02 of a 00:05:31 deadline); the reused
        // upserts were consumed before it and cannot move it.
        self::assertCount(1, $commands);
        self::assertSame('complete_workflow', $commands[0]['type']);
        self::assertSame(29.0, $commands[0]['result']);
    }

    /**
     * Counterfactual: retime one reused event at a time, both its successor
     * recorded_at and its source reused_recorded_at. A retimed history is not
     * one redrive writes (reused rows share the successor start); it only
     * proves that neither time reaches replay. The commands, including the
     * remaining() result, must not change.
     */
    #[DataProvider('reusedEventTypes')]
    public function testRetimingAReusedUpsertOrSignalWaitEventChangesNothing(string $eventType): void
    {
        $fixtures = str_starts_with($eventType, 'Signal')
            ? ['redrive-reused-signal-wait-clock']
            : ['redrive-reused-upserts-clock', 'redrive-reused-upserts-cancellation-clock'];
        foreach ($fixtures as $name) {
            $fixture = self::fixture(__DIR__."/fixtures/replay-regressions/{$name}.json");
            $baseline = ReplayRegressionFixture::execute($fixture);
            foreach (['2026-11-03T00:00:00+00:00', '2026-11-03T00:04:59.999999+00:00'] as $sourceTime) {
                $retimed = $fixture;
                $matched = 0;
                foreach ($retimed['history'] as $index => $event) {
                    if ($event['event_type'] !== $eventType) {
                        continue;
                    }
                    ++$matched;
                    $retimed['history'][$index]['recorded_at'] = '2026-11-03T00:05:00.250000+00:00';
                    $retimed['history'][$index]['payload']['reused_recorded_at'] = $sourceTime;
                }
                self::assertSame(1, $matched, "{$name} has one {$eventType}");
                self::assertSame($baseline, ReplayRegressionFixture::execute($retimed), "{$name} {$eventType} at {$sourceTime}");
            }
        }
    }

    #[DataProvider('wrongResumeSteps')]
    public function testSuccessorCheckRejectsAWrongResumeStep(string $name, mixed $resume): void
    {
        $fixture = self::fixture(__DIR__."/fixtures/replay-regressions/{$name}.json");
        $fixture['history'][0]['payload']['resume_step_sequence'] = $resume;

        $violation = RedriveSuccessorHistory::violation($fixture['history'], $fixture['workflow_tasks'][0]);

        self::assertNotNull($violation, "{$name} resume_step_sequence ".json_encode($resume));
        self::assertStringContainsString('resume_step_sequence', $violation);
    }

    /** @return array<string, array{string, mixed}> */
    public static function wrongResumeSteps(): array
    {
        $cases = [];
        foreach (['redrive-reused-upserts-clock' => 3, 'redrive-reused-upserts-cancellation-clock' => 3,
            'redrive-reused-signal-wait-clock' => 2] as $name => $resume) {
            foreach (['minus one' => $resume - 1, 'plus one' => $resume + 1, 'null' => null, 'string' => (string) $resume] as $label => $value) {
                $cases["{$name} {$label}"] = [$name, $value];
            }
        }

        return $cases;
    }

    /** @param callable(array<string, mixed>): array<string, mixed> $mutate */
    #[DataProvider('invalidSuccessors')]
    public function testSuccessorCheckRejectsAnInvalidShape(string $name, callable $mutate, string $rule): void
    {
        $fixture = $mutate(self::fixture(__DIR__."/fixtures/replay-regressions/{$name}.json"));

        $violation = RedriveSuccessorHistory::violation($fixture['history'], $fixture['workflow_tasks'][0]);

        self::assertNotNull($violation);
        self::assertStringContainsString($rule, $violation);
    }

    /** @return array<string, array{string, callable(array<string, mixed>): array<string, mixed>, string}> */
    public static function invalidSuccessors(): array
    {
        $cases = [];
        foreach (['workflow_instance_id', 'workflow_run_id', 'workflow_command_id', 'workflow_link_id', 'continued_from_run_id'] as $field) {
            $cases["missing {$field}"] = ['redrive-reused-signal-wait-clock', static function (array $fixture) use ($field): array {
                unset($fixture['history'][0]['payload'][$field]);

                return $fixture;
            }, $field];
        }

        return $cases + [
            'not a redrive' => ['redrive-reused-upserts-clock', static function (array $fixture): array {
                $fixture['history'][0]['payload']['recovery_kind'] = 'continue_as_new';

                return $fixture;
            }, 'recovery_kind'],
            'reused from another run' => ['redrive-reused-signal-wait-clock', static function (array $fixture): array {
                $fixture['history'][2]['payload']['reused_from_run_id'] = '01JBREDRIVEOTHERRUN0000000';

                return $fixture;
            }, 'reused_from_run_id'],
            'reused row after a successor row' => ['redrive-reused-upserts-cancellation-clock', static function (array $fixture): array {
                $fixture['history'][4]['payload']['reused_from_run_id'] = $fixture['history'][0]['payload']['continued_from_run_id'];
                $fixture['history'][4]['payload']['reused_recorded_at'] = '2026-11-03T00:00:00+00:00';

                return $fixture;
            }, 'prefix'],
            'cancellation before the successor start' => ['redrive-reused-upserts-cancellation-clock', static function (array $fixture): array {
                $fixture['history'][4]['recorded_at'] = '2026-11-03T00:00:01+00:00';

                return $fixture;
            }, 'predate'],
            'cancellation bound to another run' => ['redrive-reused-upserts-cancellation-clock', static function (array $fixture): array {
                $fixture['history'][5]['payload']['workflow_run_id'] = 'regression-inline';

                return $fixture;
            }, 'successor run'],
            'cancellation rooted elsewhere' => ['redrive-reused-upserts-cancellation-clock', static function (array $fixture): array {
                $fixture['history'][4]['payload']['cancellation']['root_workflow_instance_id'] = 'regression-workflow';

                return $fixture;
            }, 'root at the successor'],
            'task for another run' => ['redrive-reused-upserts-clock', static function (array $fixture): array {
                $fixture['workflow_tasks'][0]['run_id'] = 'regression-inline';

                return $fixture;
            }, 'workflow task'],
            'out of order' => ['redrive-reused-upserts-clock', static function (array $fixture): array {
                $fixture['history'][2]['sequence'] = 5;

                return $fixture;
            }, 'history order'],
        ];
    }

    /** @return array<string, array{string}> */
    public static function reusedEventTypes(): array
    {
        $types = ['MemoUpserted', 'SearchAttributesUpserted', 'SignalWaitOpened', 'SignalReceived', 'SignalApplied'];

        return array_combine($types, array_map(static fn (string $type): array => [$type], $types));
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

    /** @return array<string, mixed> */
    private static function fixture(string $path): array
    {
        $fixture = json_decode((string) file_get_contents($path), true, flags: JSON_THROW_ON_ERROR);
        self::assertIsArray($fixture);

        return $fixture;
    }

    /** @return array<string, array<string, mixed>> */
    private static function eventsByType(string $path): array
    {
        $events = [];
        foreach (self::fixture($path)['history'] as $event) {
            self::assertArrayNotHasKey($event['event_type'], $events);
            $events[$event['event_type']] = $event;
        }

        return $events;
    }

    private static function assertValidSuccessor(string $path, int $resumeStepSequence): void
    {
        $fixture = self::fixture($path);
        self::assertSame($resumeStepSequence, $fixture['history'][0]['payload']['resume_step_sequence']);
        self::assertNull(RedriveSuccessorHistory::violation($fixture['history'], $fixture['workflow_tasks'][0]));
    }

    /** @param list<string> $eventTypes */
    private static function assertReusedEvents(string $path, array $eventTypes): void
    {
        $fixture = self::fixture($path);
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
     * @param list<mixed> $input
     */
    private function replay(callable $workflow, array $history, array $input = []): ReplayResult
    {
        return (new Replayer($this->codec()))->replay($workflow, $history, $input, 'redrive-replay', [
            'workflow_id' => 'redrive-workflow',
            'run_id' => '01JBREDRIVESUCCESSORRUN000',
        ]);
    }

    private function codec(): AvroPayloadCodec
    {
        return new AvroPayloadCodec();
    }
}
