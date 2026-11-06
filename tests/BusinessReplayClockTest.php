<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ReplayClockUnavailable;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\TestCase;

final class BusinessReplayClockTest extends TestCase
{
    public function testDirectContextHasNoBusinessReplayTime(): void
    {
        $context = new WorkflowContext('workflow', 'run', [], new AvroPayloadCodec());

        $this->expectException(ReplayClockUnavailable::class);
        $context->replayNow();
    }

    public function testQuerySnapshotDoesNotGuessAConsumedPrefix(): void
    {
        $context = new QueryContext('workflow', 'run', [[
            'event_type' => 'ActivityCompleted',
            'recorded_at' => '2026-11-06T00:00:00.123456+00:00',
            'payload' => ['sequence' => 1, 'activity_execution_id' => 'activity'],
        ]], []);

        $this->expectException(ReplayClockUnavailable::class);
        $context->replayNow();
    }

    public function testOrdinaryEntryDoesNotSubstituteHistoryInsertionTime(): void
    {
        $this->expectException(ReplayClockUnavailable::class);
        (new Replayer(new AvroPayloadCodec()))->replay(
            static fn (WorkflowContext $context) => $context->replayNow(),
            [[
                'event_type' => 'WorkflowStarted',
                'recorded_at' => '2026-11-06T00:00:00.123456+00:00',
                'payload' => ['workflow_run_id' => 'run'],
            ]],
            [],
            'php-workers',
            ['workflow_id' => 'workflow', 'run_id' => 'run'],
        );
    }
}
