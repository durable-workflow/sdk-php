<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Exception\WorkflowCancelled;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ImmediateConditionReplayTest extends TestCase
{
    /** @return iterable<string, array{bool, ?int, bool}> */
    public static function immediateConditions(): iterable
    {
        yield 'satisfied' => [true, null, true];
        yield 'zero timeout' => [false, 0, false];
        yield 'satisfied before zero timeout' => [true, 0, true];
    }

    #[DataProvider('immediateConditions')]
    public function testImmediateConditionBetweenRecordedActivities(bool $ready, ?int $timeout, bool $expected): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static function (WorkflowContext $context) use ($ready, $timeout): array {
            $first = $context->activity('first');
            $satisfied = $context->waitCondition(static fn (): bool => $ready, key: 'immediate', timeout: $timeout);
            $second = $context->activity('second');

            return [$first, $satisfied, $second];
        };
        $history = [
            self::completedActivity($codec, 1, 'first', 17),
            self::completedActivity($codec, 2, 'second', ['label' => 'café', 'nil' => null]),
        ];

        foreach ([new Replayer($codec), new Replayer($codec)] as $coldReplayer) {
            $result = $coldReplayer->replay($workflow, $history, [], 'php-workers');
            self::assertSame(['complete_workflow'], array_column($result->commands, 'type'));
            self::assertSame([17, $expected, ['label' => 'café', 'nil' => null]], $codec->decodeEnvelope($result->commands[0]['result']));
        }
    }

    #[DataProvider('immediateConditions')]
    public function testImmediateConditionLeavesFollowingRecordedWaitIntact(bool $ready, ?int $timeout, bool $expected): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static function (WorkflowContext $context) use ($ready, $timeout, $expected): bool {
            self::assertSame($expected, $context->waitCondition(static fn (): bool => $ready, key: 'immediate', timeout: $timeout));

            return $context->waitCondition(static fn (): bool => false, key: 'later', timeout: 30);
        };
        $result = (new Replayer($codec))->replay($workflow, [self::recordedWait('later', 30)], [], 'php-workers');

        self::assertCount(1, $result->commands);
        self::assertSame('open_condition_wait', $result->commands[0]['type']);
        self::assertSame('later', $result->commands[0]['condition_key']);
        self::assertSame(30, $result->commands[0]['timeout_seconds']);
    }

    public function testImmediateConditionBeforeRecordedTimer(): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static function (WorkflowContext $context): bool {
            $ready = $context->waitCondition(static fn (): bool => true, key: 'immediate');
            $context->sleep(1);

            return $ready;
        };
        $history = [
            ['event_type' => 'TimerScheduled', 'payload' => ['sequence' => 1, 'timer_id' => 'timer:1', 'delay_seconds' => 1]],
            ['event_type' => 'TimerFired', 'payload' => ['sequence' => 1, 'timer_id' => 'timer:1', 'delay_seconds' => 1]],
        ];
        $result = (new Replayer($codec))->replay($workflow, $history, [], 'php-workers');

        self::assertSame(['complete_workflow'], array_column($result->commands, 'type'));
        self::assertTrue($codec->decodeEnvelope($result->commands[0]['result']));
    }

    public function testRecordedTimeoutWinsWithoutReevaluatingPredicate(): void
    {
        $codec = new AvroPayloadCodec();
        $evaluations = 0;
        $workflow = static function (WorkflowContext $context) use (&$evaluations): bool {
            return $context->waitCondition(static function () use (&$evaluations): bool {
                ++$evaluations;

                return true;
            }, key: 'original', timeout: 30);
        };
        $history = [self::recordedWait('original', 30), [
            'event_type' => 'ConditionWaitTimedOut',
            'payload' => ['sequence' => 1, 'condition_wait_id' => 'condition:1', 'condition_key' => 'original'],
        ]];
        $result = (new Replayer($codec))->replay($workflow, $history, [], 'php-workers');

        self::assertFalse($codec->decodeEnvelope($result->commands[0]['result']));
        self::assertSame(0, $evaluations);
    }

    /** @return iterable<string, array{string, int, ?string}> */
    public static function changedWaits(): iterable
    {
        yield 'key' => ['changed', 30, null];
        yield 'timeout' => ['original', 31, null];
        yield 'fingerprint' => ['original', 30, 'sha256:'.str_repeat('a', 64)];
    }

    #[DataProvider('changedWaits')]
    public function testRecordedWaitCannotBeReplacedByTruePredicate(string $key, int $timeout, ?string $fingerprint): void
    {
        $event = self::recordedWait('original', 30);
        if ($fingerprint !== null) { $event['payload']['condition_definition_fingerprint'] = $fingerprint; }
        $workflow = static fn (WorkflowContext $context): bool => $context->waitCondition(
            static fn (): bool => true, key: $key, timeout: $timeout,
        );

        $this->expectException(NonDeterministicWorkflow::class);
        (new Replayer(new AvroPayloadCodec()))->replay($workflow, [$event], [], 'php-workers');
    }

    public function testFalseConditionCannotSkipRecordedActivity(): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static function (WorkflowContext $context): mixed {
            $context->waitCondition(static fn (): bool => false, key: 'not-ready', timeout: 30);

            return $context->activity('recorded');
        };
        $this->expectException(NonDeterministicWorkflow::class);
        (new Replayer($codec))->replay($workflow, [self::completedActivity($codec, 1, 'recorded', 17)], [], 'php-workers');
    }

    public function testSkippingImmediateConditionDoesNotBypassActivityIdentity(): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static function (WorkflowContext $context): mixed {
            $context->waitCondition(static fn (): bool => true, key: 'immediate');

            return $context->activity('changed');
        };
        $this->expectException(NonDeterministicWorkflow::class);
        $this->expectExceptionMessage('Recorded activity detail changed from original to changed.');
        (new Replayer($codec))->replay($workflow, [self::completedActivity($codec, 1, 'original', 17)], [], 'php-workers');
    }

    /** @return iterable<string, array{bool, ?int}> */
    public static function canonicalConditions(): iterable
    {
        foreach (self::immediateConditions() as $name => $arguments) {
            yield $name => [$arguments[0], $arguments[1]];
        }
    }

    #[DataProvider('canonicalConditions')]
    public function testCanonicalConditionCancellationWithoutOpenedWaitStillWins(bool $ready, ?int $timeout): void
    {
        $codec = new AvroPayloadCodec();
        $evaluations = 0;
        $workflow = static function (WorkflowContext $context) use ($ready, $timeout, &$evaluations): string {
            try {
                $context->waitCondition(static function () use ($ready, &$evaluations): bool {
                    ++$evaluations;

                    return $ready;
                }, key: 'ready', timeout: $timeout);
            } catch (WorkflowCancelled $cancelled) {
                return (string) $cancelled->requestId;
            }

            return 'not cancelled';
        };
        $result = (new Replayer($codec))->replay($workflow, self::rootCancellation('condition'), [], 'php-workers', ['run_id' => 'run-1']);

        self::assertSame('request-1', $codec->decodeEnvelope($result->commands[0]['result']));
        self::assertSame(0, $evaluations);
    }

    #[DataProvider('canonicalConditions')]
    public function testUnrecordedConditionBeforeCanonicalTimerDoesNotConsumeCancellation(bool $ready, ?int $timeout): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static function (WorkflowContext $context) use ($ready, $timeout): string {
            $context->waitCondition(static fn (): bool => $ready, key: 'immediate', timeout: $timeout);
            try {
                $context->sleep(10);
            } catch (WorkflowCancelled $cancelled) {
                return (string) $cancelled->requestId;
            }

            return 'not cancelled';
        };
        $result = (new Replayer($codec))->replay($workflow, self::rootCancellation('timer'), [], 'php-workers', ['run_id' => 'run-1']);

        self::assertSame('request-1', $codec->decodeEnvelope($result->commands[0]['result']));
    }

    /** @return iterable<string, array{bool, ?int, bool}> */
    public static function scopedCanonicalConditions(): iterable
    {
        foreach (self::canonicalConditions() as $name => $arguments) {
            yield 'committed '.$name => [...$arguments, false];
            yield 'prepared '.$name => [...$arguments, true];
        }
    }

    #[DataProvider('scopedCanonicalConditions')]
    public function testCanonicalScopedConditionWithoutOpenedWaitIsNotElided(bool $ready, ?int $timeout, bool $prepared): void
    {
        $codec = new AvroPayloadCodec();
        $fixture = json_decode((string) file_get_contents(__DIR__.'/fixtures/committed-scope-delivery.json'), true, flags: JSON_THROW_ON_ERROR)['unshielded'];
        foreach ($fixture['history'] as &$event) {
            if (in_array($event['event_type'], ['CancellationScopeDeliveryPrepared', 'CancellationScopeDelivered'], true)) {
                $event['payload']['call_kind'] = 'condition';
            }
        }
        unset($event);
        if ($prepared) {
            $fixture['history'] = array_values(array_filter($fixture['history'], static fn (array $event): bool => $event['event_type'] !== 'CancellationScopeDelivered'));
        }
        $evaluations = 0;
        $workflow = static function (WorkflowContext $context) use ($ready, $timeout, &$evaluations): string {
            return $context->cancellationScope(static function () use ($context, $ready, $timeout, &$evaluations): string {
                return $context->cancellationScope(static function () use ($context, $ready, $timeout, &$evaluations): string {
                    try {
                        $context->waitCondition(static function () use ($ready, &$evaluations): bool {
                            ++$evaluations;

                            return $ready;
                        }, key: 'ready', timeout: $timeout);
                    } catch (WorkflowCancelled $cancelled) {
                        return (string) $cancelled->requestId;
                    }

                    return 'not cancelled';
                });
            });
        };
        $result = (new Replayer($codec))->replay($workflow, $fixture['history'], [], 'php-workers', $fixture['task'],
            allowCancellationScopeAuthoring: true, replayCommittedCancellationScopes: true, prepareCancellationScopeDelivery: $prepared);

        if ($prepared) {
            self::assertSame([], $result->commands);
            self::assertSame('condition', $result->cancellationScopeDelivery?->boundary->callKind);
            self::assertSame(3, $result->cancellationScopeDelivery?->boundary->sequence);
        } else {
            $request = array_values(array_filter($fixture['history'], static fn (array $event): bool => $event['event_type'] === 'CancellationScopeRequested'))[0];
            $last = $result->commands[array_key_last($result->commands)];
            self::assertSame('complete_workflow', $last['type']);
            self::assertSame($request['payload']['request_id'], $codec->decodeEnvelope($last['result']));
        }
        self::assertSame(0, $evaluations);
    }

    /** @return list<array<string, mixed>> */
    private static function rootCancellation(string $kind): array
    {
        return [
            ['event_type' => 'CooperativeCancellationRequested', 'recorded_at' => '2026-10-01T00:00:00Z', 'payload' => [
                'workflow_run_id' => 'run-1', 'workflow_command_id' => 'request-1', 'cleanup_deadline_at' => '2026-10-01T00:01:00Z',
            ]],
            ['event_type' => 'CooperativeCancellationDelivered', 'payload' => [
                'workflow_run_id' => 'run-1', 'workflow_command_id' => 'request-1', 'sequence' => 1, 'call_kind' => $kind, 'sequence_span' => 1,
            ]],
        ];
    }

    /** @return array<string, mixed> */
    private static function completedActivity(AvroPayloadCodec $codec, int $sequence, string $type, mixed $result): array
    {
        return ['event_type' => 'ActivityCompleted', 'payload' => [
            'sequence' => $sequence, 'activity_type' => $type, 'result' => $codec->envelope($result),
        ]];
    }

    /** @return array{event_type: string, payload: array<string, mixed>} */
    private static function recordedWait(string $key, int $timeout): array
    {
        return ['event_type' => 'ConditionWaitOpened', 'payload' => [
            'sequence' => 1, 'condition_wait_id' => 'condition:1', 'condition_key' => $key, 'timeout_seconds' => $timeout,
        ]];
    }
}
