<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ActivityHeartbeatResumeUnavailable;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Testing\WorkerTestHarness;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\ActivityHeartbeatCheckpoint;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/** Poll-to-handler controls for acknowledged heartbeat checkpoints (.github#218), not a real Server. */
final class ActivityHeartbeatCheckpointTest extends TestCase
{
    private const AT = '2026-11-14T09:00:00.123456789Z';
    private const SUPPORTED = ['activity_heartbeat_resume' => ['supported' => true, 'version' => 1]];

    /** @return array<string, mixed> */
    private static function claim(int $attempt, string $execution, mixed $details, mixed $at, array $capabilities = self::SUPPORTED): array
    {
        return ['task' => ['task_id' => "task-{$execution}-{$attempt}", 'workflow_id' => 'workflow', 'run_id' => 'run',
            'namespace' => 'default', 'activity_execution_id' => $execution,
            'activity_attempt_id' => "attempt-{$execution}-{$attempt}", 'attempt_number' => $attempt,
            'activity_type' => 'inspect', 'task_queue' => 'checkpoint-queue', 'payload_codec' => 'avro',
            'lease_owner' => 'checkpoint-worker',
            'retry_policy' => ['max_attempts' => 3, 'backoff_seconds' => [1, 2], 'schedule_to_close_timeout' => 90],
            'last_heartbeat_details' => $details, 'last_heartbeat_at' => $at],
            'server_capabilities' => $capabilities];
    }

    /**
     * @param list<array<string, mixed>> $polls
     * @param list<array<string, mixed>|\Throwable> $heartbeats
     * @return array{Worker, \ArrayObject<string, mixed>}
     */
    private static function worker(array $polls, array $heartbeats = []): array
    {
        $log = new \ArrayObject(['failures' => [], 'completions' => [], 'heartbeats' => []]);
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body)
            use (&$polls, &$heartbeats, $log): array {
            if (str_ends_with($uri, '/worker/register')) {
                return ['registered' => true];
            }
            if (str_ends_with($uri, '/activity-tasks/poll')) {
                return array_shift($polls) ?? ['task' => null];
            }
            if (str_ends_with($uri, '/heartbeat') && str_contains($uri, '/activity-tasks/')) {
                $log['heartbeats'] = [...$log['heartbeats'], $body['details'] ?? null];
                $reply = array_shift($heartbeats) ?? ['can_continue' => true, 'cancel_requested' => false];
                if ($reply instanceof \Throwable) {
                    throw $reply;
                }

                return $reply;
            }
            if (str_ends_with($uri, '/fail')) {
                $log['failures'] = [...$log['failures'], $body];

                return ['recorded' => true];
            }
            if (str_ends_with($uri, '/complete')) {
                $log['completions'] = [...$log['completions'], $body];

                return ['recorded' => true];
            }
            if (str_ends_with($uri, '/poll')) {
                return ['task' => null];
            }
            if (str_ends_with($uri, '/worker/heartbeat')) {
                return ['heartbeat_recorded' => true];
            }
            self::fail('Unexpected request: '.$method.' '.$uri);
        });

        return [new Worker(new Client('https://server.example', transport: $transport), 'checkpoint-queue',
            workerId: 'checkpoint-worker'), $log];
    }

    /** @return array<string, mixed> */
    private static function observe(ActivityContext $context): array
    {
        try {
            $checkpoint = $context->lastHeartbeatCheckpoint();
        } catch (ActivityHeartbeatResumeUnavailable) {
            return ['state' => 'unavailable', 'version' => $context->heartbeatResumeVersion()];
        }
        if ($checkpoint === null) {
            return ['state' => 'empty', 'version' => $context->heartbeatResumeVersion(),
                'details' => $context->lastHeartbeatDetails(), 'at' => $context->lastHeartbeatAt()];
        }

        return ['state' => 'checkpoint', 'version' => $context->heartbeatResumeVersion(),
            'details' => $checkpoint->details(), 'at' => $checkpoint->acknowledgedAt,
            'execution' => $context->activityExecutionId, 'attempt' => $context->activityAttemptId];
    }

    public function testUnavailableAndSupportedEmptyAreDistinct(): void
    {
        $valid = self::claim(2, 'execution', ['cursor' => 1], self::AT);
        $older = $valid;
        unset($older['server_capabilities']);
        $unknownVersion = self::claim(2, 'execution', ['cursor' => 1], self::AT,
            ['activity_heartbeat_resume' => ['supported' => true, 'version' => 2]]);
        $unsupported = self::claim(2, 'execution', ['cursor' => 1], self::AT,
            ['activity_heartbeat_resume' => ['supported' => false, 'version' => 1]]);
        $nonparticipating = $valid;
        unset($nonparticipating['task']['last_heartbeat_details'], $nonparticipating['task']['last_heartbeat_at']);
        $empty = self::claim(1, 'execution', null, null);
        $retryEmpty = self::claim(2, 'execution', null, null);

        [$worker, $log] = self::worker([$older, $unknownVersion, $unsupported, $nonparticipating, $empty, $retryEmpty]);
        $seen = [];
        $worker->registerActivity('inspect', static function (ActivityContext $context) use (&$seen): string {
            $seen[] = self::observe($context);

            return 'ok';
        });
        for ($tick = 0; $tick < 6; ++$tick) {
            self::assertTrue($worker->tick(0));
        }
        $unavailable = ['state' => 'unavailable', 'version' => null];
        $none = ['state' => 'empty', 'version' => 1, 'details' => null, 'at' => null];
        self::assertSame([$unavailable, $unavailable, $unavailable, $unavailable, $none, $none], $seen);
        self::assertCount(6, $log['completions']);
        self::assertSame([], $log['failures']);
    }

    public function testATypedCheckpointKeepsItsReceivedValuesAndPrecision(): void
    {
        $details = ['cursor' => 7, 'ratio' => 1.0, 'flags' => [true, null, false], 'label' => "caf\u{e9}"];
        [$worker, $log] = self::worker([self::claim(2, 'execution', $details, self::AT)]);
        $seen = [];
        $worker->registerActivity('inspect', static function (ActivityContext $context) use (&$seen): string {
            $checkpoint = $context->lastHeartbeatCheckpoint();
            self::assertInstanceOf(ActivityHeartbeatCheckpoint::class, $checkpoint);
            $seen = [self::observe($context), $checkpoint->contractVersion,
                $checkpoint->acknowledgedAtTime()->format('Y-m-d\TH:i:s.uP'), $context->retryInfo->policy()];

            return 'ok';
        });
        self::assertTrue($worker->tick(0));
        self::assertSame([
            ['state' => 'checkpoint', 'version' => 1, 'details' => $details, 'at' => self::AT,
                'execution' => 'execution', 'attempt' => 'attempt-execution-2'],
            1,
            '2026-11-14T09:00:00.123456+00:00',
            ['max_attempts' => 3, 'backoff_seconds' => [1, 2], 'schedule_to_close_timeout' => 90],
        ], $seen);
        self::assertSame([], $log['failures']);
    }

    public function testOffsetTimesAreNormalizedToUtcWhileTheOriginalTextIsKept(): void
    {
        $checkpoint = ActivityHeartbeatCheckpoint::received(['cursor' => 1], '2026-11-14T10:30:00+01:30');
        self::assertSame('2026-11-14T10:30:00+01:30', $checkpoint->acknowledgedAt);
        self::assertSame('2026-11-14T09:00:00.000000+00:00', $checkpoint->acknowledgedAtTime()->format('Y-m-d\TH:i:s.uP'));
    }

    public function testAcceptedRejectedAndUncertainWritesNeverMutateTheReceivedSnapshot(): void
    {
        [$worker, $log] = self::worker([self::claim(2, 'execution', ['cursor' => 7], self::AT)], [
            ['can_continue' => true, 'cancel_requested' => false, 'heartbeat_recorded' => true],
            new TransportException('Heartbeat response was lost.', transientConnectionFailure: true),
            ['can_continue' => false, 'cancel_requested' => false, 'reason' => 'stale_attempt'],
        ]);
        $snapshots = [];
        $worker->registerActivity('inspect', static function (ActivityContext $context) use (&$snapshots): string {
            $snapshots[] = self::observe($context);
            $context->heartbeat(['cursor' => 8]);
            $snapshots[] = self::observe($context);
            try {
                $context->heartbeat(['cursor' => 9]);
            } catch (\Throwable) {
            }
            $snapshots[] = self::observe($context);
            $copy = $context->lastHeartbeatDetails();
            $copy['cursor'] = 99;
            try {
                $context->heartbeat(['cursor' => 10]);
            } catch (\Throwable) {
            }
            $snapshots[] = self::observe($context);

            return 'ok';
        });
        self::assertTrue($worker->tick(0));
        // A heartbeat whose response was lost is repeated with the same details (cloud#752).
        self::assertSame([['cursor' => 8], ['cursor' => 9], ['cursor' => 9], ['cursor' => 10]], array_slice($log['heartbeats'], 0, 4));
        self::assertCount(4, $snapshots);
        foreach ($snapshots as $snapshot) {
            self::assertSame(['cursor' => 7], $snapshot['details']);
            self::assertSame(self::AT, $snapshot['at']);
        }
    }

    public function testLaterClaimsDoNotLeakCheckpointsBetweenExecutionsOrRuns(): void
    {
        $other = self::claim(1, 'other-execution', null, null);
        $otherRun = self::claim(2, 'execution', null, null);
        $otherRun['task']['run_id'] = 'next-run';
        $legacy = self::claim(2, 'execution', ['cursor' => 3], self::AT);
        unset($legacy['server_capabilities']);
        [$worker] = self::worker([self::claim(2, 'execution', ['cursor' => 7], self::AT), $other, $otherRun, $legacy]);
        $seen = [];
        $worker->registerActivity('inspect', static function (ActivityContext $context) use (&$seen): string {
            $seen[] = self::observe($context)['state'].':'.json_encode($context->heartbeatResumeVersion() === null
                ? null : $context->lastHeartbeatDetails());

            return 'ok';
        });
        for ($tick = 0; $tick < 4; ++$tick) {
            self::assertTrue($worker->tick(0));
        }
        self::assertSame(['checkpoint:{"cursor":7}', 'empty:null', 'empty:null', 'unavailable:null'], $seen);
    }

    /** @return iterable<string, array{array<string, mixed>}> */
    public static function malformedClaims(): iterable
    {
        $original = self::claim(2, 'execution', ['cursor' => 7], self::AT);
        $set = static function (array $wire, string $field, mixed $value): array {
            $wire['task'][$field] = $value;

            return $wire;
        };
        $unset = static function (array $wire, string $field): array {
            unset($wire['task'][$field]);

            return $wire;
        };
        foreach ([
            'null time' => ['last_heartbeat_at', null],
            'text time' => ['last_heartbeat_at', 'no timestamp'],
            'zoneless time' => ['last_heartbeat_at', '2026-11-14T09:00:00'],
            'invalid date' => ['last_heartbeat_at', '2026-02-30T09:00:00Z'],
            'numeric time' => ['last_heartbeat_at', 1763110800],
            'null details' => ['last_heartbeat_details', null],
            'empty execution' => ['activity_execution_id', ''],
            'empty attempt' => ['activity_attempt_id', ''],
            'empty workflow' => ['workflow_id', ''],
            'empty run' => ['run_id', ''],
            'foreign namespace' => ['namespace', 'foreign'],
            'first attempt checkpoint' => ['attempt_number', 1],
            'foreign owner' => ['lease_owner', 'another-worker'],
            'foreign queue' => ['task_queue', 'another-queue'],
        ] as $name => [$field, $value]) {
            yield "checkpoint {$name}" => [$set($original, $field, $value)];
        }
        foreach (['last_heartbeat_at', 'last_heartbeat_details'] as $field) {
            yield "checkpoint without {$field}" => [$unset($original, $field)];
        }
        $empty = self::claim(1, 'execution', null, null);
        foreach ([
            'empty execution' => ['activity_execution_id', ''],
            'foreign namespace' => ['namespace', 'foreign'],
            'zero attempt' => ['attempt_number', 0],
            'negative attempt' => ['attempt_number', -1],
            'string attempt' => ['attempt_number', '1'],
        ] as $name => [$field, $value]) {
            yield "empty {$name}" => [$set($empty, $field, $value)];
        }
        foreach (['activity_execution_id', 'activity_attempt_id', 'workflow_id', 'run_id', 'namespace'] as $field) {
            yield "empty without {$field}" => [$unset($empty, $field)];
        }
    }

    #[DataProvider('malformedClaims')]
    public function testMalformedNegotiatedCheckpointsAreRefusedBeforeTheHandler(array $wire): void
    {
        [$worker, $log] = self::worker([$wire]);
        $worker->registerActivity('inspect', static function (ActivityContext $context): never {
            self::fail('A malformed checkpoint reached the handler.');
        });
        self::assertTrue($worker->tick(0));
        self::assertSame([], $log['completions']);
        self::assertCount(1, $log['failures']);
        self::assertStringContainsString('activity_heartbeat_resume v1 claim has an invalid checkpoint',
            json_encode($log['failures'][0], JSON_THROW_ON_ERROR));
    }

    public function testServerFreeActivitiesReportUnavailable(): void
    {
        $worker = new Worker(new Client('https://server.example', transport: new FakeTransport()), 'checkpoint-queue');
        $worker->registerActivity('inspect', static fn (ActivityContext $context): array => self::observe($context));
        $harness = new WorkerTestHarness($worker);
        self::assertSame(['state' => 'unavailable', 'version' => null], $harness->runActivity('inspect'));

        $direct = new ActivityContext(new Client('https://server.example'), 'task', 'attempt', 'owner', 'inspect', 2,
            taskMetadata: self::claim(2, 'execution', ['cursor' => 1], self::AT)['task']);
        $this->expectException(ActivityHeartbeatResumeUnavailable::class);
        $direct->lastHeartbeatDetails();
    }
}
