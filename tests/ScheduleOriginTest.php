<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Model\ScheduleOrigin;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\TestCase;

/** #381: a scheduled run reads the schedule fire that started it from its own history. */
final class ScheduleOriginTest extends TestCase
{
    private const ACTIVITY = 'reports.build';

    public function testATickedFireExposesItsOccurrenceFromTheFirstTaskAndOnColdReplay(): void
    {
        $start = self::scheduledStart('2026-11-11T17:00:03.250000Z', 7);

        $first = $this->replayRun($start);
        self::assertSame(['schedule_activity'], array_column($first['result']->commands, 'type'));
        self::assertSame(['nightly-report', '2026-11-11T17:00:03.250000+00:00', 7], $first['seen']);
        self::assertSame(['nightly-report', '2026-11-11T17:00:03.250000+00:00'],
            (new AvroPayloadCodec())->decodeEnvelope($first['result']->commands[0]['arguments']));

        $cold = $this->replayRun([...$start, ...self::completedActivity()]);
        self::assertSame(['complete_workflow'], array_column($cold['result']->commands, 'type'));
        self::assertSame($first['seen'], $cold['seen']);
    }

    public function testABackfilledFireExposesTheEnumeratedOccurrence(): void
    {
        $seen = $this->replayRun(self::scheduledStart('2026-11-01T00:00:00Z', 42))['seen'];

        self::assertSame(['nightly-report', '2026-11-01T00:00:00.000000+00:00', 42], $seen);
    }

    public function testAnOrdinaryStartHasNoScheduleOrigin(): void
    {
        $run = $this->replayRun([self::started()]);

        self::assertSame([null], $run['seen']);
    }

    public function testAMarkerWithoutAnOccurrenceKeepsTheScheduleButNoTime(): void
    {
        $start = self::scheduledStart(null, 3);

        self::assertSame(['nightly-report', null, 3], $this->replayRun($start)['seen']);
    }

    /**
     * @param list<array<string, mixed>> $history
     * @return array{result: \DurableWorkflow\Worker\ReplayResult, seen: list<mixed>}
     */
    private function replayRun(array $history): array
    {
        $seen = [];
        $workflow = static function (WorkflowContext $context) use (&$seen): mixed {
            $origin = $context->scheduleOrigin();
            $seen = $origin instanceof ScheduleOrigin
                ? [$origin->scheduleId, $origin->occurrenceTime?->format('Y-m-d\TH:i:s.uP'), $origin->triggerNumber]
                : [null];
            if ($origin === null) {
                return 'unscheduled';
            }

            return $context->activity(self::ACTIVITY, [$origin->scheduleId, $origin->occurrenceTime?->format('Y-m-d\TH:i:s.uP')]);
        };
        $result = (new Replayer(new AvroPayloadCodec()))->replay($workflow, $history, [], 'reports', [
            'workflow_id' => 'nightly-report-run',
            'run_id' => '01JSCHEDULEDRUN00000000000',
        ]);

        return ['result' => $result, 'seen' => $seen];
    }

    /** @return array<string, mixed> */
    private static function started(): array
    {
        return ['event_type' => 'WorkflowStarted', 'sequence' => 1, 'recorded_at' => '2026-11-11T17:00:04+00:00',
            'payload' => ['workflow_type' => 'reports.nightly']];
    }

    /** @return list<array<string, mixed>> */
    private static function scheduledStart(?string $occurrence, int $trigger): array
    {
        $marker = ['schedule_id' => 'nightly-report', 'schedule_ulid' => '01JSCHEDULE0000000000000000',
            'overlap_policy' => 'skip', 'trigger_number' => $trigger, 'cron_expression' => '0 17 * * *', 'timezone' => 'UTC'];
        if ($occurrence !== null) {
            $marker['occurrence_time'] = $occurrence;
        }

        return [self::started(), ['event_type' => 'ScheduleTriggered', 'sequence' => 2,
            'recorded_at' => '2026-11-11T17:00:04+00:00', 'payload' => $marker]];
    }

    /** @return list<array<string, mixed>> */
    private static function completedActivity(): array
    {
        $codec = new AvroPayloadCodec();

        return [
            ['event_type' => 'ActivityScheduled', 'sequence' => 3, 'payload' => ['sequence' => 1,
                'activity_type' => self::ACTIVITY, 'activity_execution_id' => 'activity-1']],
            ['event_type' => 'ActivityCompleted', 'sequence' => 4, 'payload' => ['sequence' => 1,
                'activity_type' => self::ACTIVITY, 'activity_execution_id' => 'activity-1', 'result' => $codec->envelope('done')]],
        ];
    }
}
