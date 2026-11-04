<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Tests\Support\RedriveFixtureRegistry;
use DurableWorkflow\Tests\Support\RedriveSuccessorHistory;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/** Source-model controls for the root, single-call cancellation boundary. */
final class RedriveSuccessorStrictnessTest extends TestCase
{
    private const FIXTURE = 'redrive-successor-reused-cancellation-clock';

    #[DataProvider('invalidBoundaries')]
    public function testRejectsAnInvalidSingleCallBoundary(callable $change, string $message): void
    {
        $violation = RedriveSuccessorHistory::violation(
            $change(RedriveFixtureRegistry::fixture(self::FIXTURE)),
            RedriveFixtureRegistry::SUCCESSORS[self::FIXTURE],
        );
        self::assertNotNull($violation);
        self::assertStringContainsString($message, $violation);
    }

    /** @return array<string, array{callable(array<string, mixed>): array<string, mixed>, string}> */
    public static function invalidBoundaries(): array
    {
        $cases = [];
        $cases['root parent must be present'] = [static function (array $fixture): array {
            unset($fixture['history'][5]['payload']['cancellation']['parent_request_id']);
            $fixture['history'][6]['payload']['cancellation'] = $fixture['history'][5]['payload']['cancellation'];

            return $fixture;
        }, 'parent_request_id'];
        foreach (['scope_origin', 'scope_authority_deadline_at'] as $key) {
            $cases["root excludes {$key} even when null"] = [static function (array $fixture) use ($key): array {
                $fixture['history'][5]['payload']['cancellation'][$key] = null;
                $fixture['history'][6]['payload']['cancellation'] = $fixture['history'][5]['payload']['cancellation'];

                return $fixture;
            }, 'root context'];
        }
        $cases['lineage is exactly the own root address'] = [static function (array $fixture): array {
            $fixture['history'][5]['payload']['cancellation']['lineage'][0]['unrelated_run_id'] = 'other-run';
            $fixture['history'][6]['payload']['cancellation'] = $fixture['history'][5]['payload']['cancellation'];

            return $fixture;
        }, 'own root'];
        $cases['request reason binds to context'] = [static function (array $fixture): array {
            $fixture['history'][5]['payload']['reason'] = 'different reason';

            return $fixture;
        }, 'reason'];
        $cases['reason is nullable text'] = [static function (array $fixture): array {
            $fixture['history'][5]['payload']['reason'] = 42;
            $fixture['history'][5]['payload']['cancellation']['reason'] = 42;
            $fixture['history'][6]['payload']['cancellation']['reason'] = 42;

            return $fixture;
        }, 'reason'];
        foreach (['sequence_span', 'operation_sequence', 'operation_sequence_span'] as $key) {
            $cases["single delivery excludes {$key} even when null"] = [static function (array $fixture) use ($key): array {
                $fixture['history'][6]['payload'][$key] = null;

                return $fixture;
            }, 'single-call'];
        }
        foreach (['sequence' => [null, 0, '3'], 'activity_type' => [null, ' '], 'activity_execution_id' => [null, ' ']] as $field => $values) {
            foreach ($values as $index => $value) {
                $cases["typed ActivityCancelled {$field} {$index}"] = [static function (array $fixture) use ($field, $value): array {
                    $terminal = self::terminal($fixture, 'ActivityCancelled');
                    if ($value === null) {
                        unset($terminal['payload'][$field]);
                    } else {
                        $terminal['payload'][$field] = $value;
                    }

                    return self::insert($fixture, 6, $terminal);
                }, 'ActivityCancelled'];
            }
        }
        foreach (['ActivityScheduled', 'TimerScheduled', 'SignalWaitOpened'] as $kind) {
            $cases["next step waits for earlier {$kind}"] = [static function (array $fixture) use ($kind): array {
                $fixture = self::withCall($fixture, $kind);
                $fixture['history'][6]['payload']['sequence'] = 4;

                return $fixture;
            }, 'earlier call is unresolved'];
            $cases["{$kind} resolution must bind the same resource"] = [static function (array $fixture) use ($kind): array {
                $fixture = self::withCall($fixture, $kind);
                $fixture['history'][6]['payload']['sequence'] = 4;
                $terminal = self::terminal($fixture, self::resolution($kind));
                $terminal['payload'][self::identity($kind)] = 'other-resource';

                return self::insert($fixture, 6, $terminal);
            }, 'earlier call is unresolved'];
        }
        $cases['resolution must follow its opener even at equal times'] = [static function (array $fixture): array {
            $fixture['history'][6]['payload']['sequence'] = 4;

            return self::insert($fixture, 4, self::terminal($fixture, 'ActivityCompleted'));
        }, 'earlier call is unresolved'];
        $cases['resolution must use the opener step'] = [static function (array $fixture): array {
            $fixture['history'][6]['payload']['sequence'] = 6;
            $terminal = self::terminal($fixture, 'ActivityCompleted');
            $terminal['payload']['sequence'] = 5;

            return self::insert($fixture, 6, $terminal);
        }, 'earlier call is unresolved'];
        $cases['resolution must use the opener family'] = [static function (array $fixture): array {
            $fixture['history'][6]['payload']['sequence'] = 4;

            return self::insert($fixture, 6, [
                'event_type' => 'TimerFired', 'payload' => ['sequence' => 3, 'timer_id' => 'other-timer'],
            ]);
        }, 'earlier call is unresolved'];
        $cases['resolution after delivery cannot advance the boundary'] = [static function (array $fixture): array {
            $fixture['history'][6]['payload']['sequence'] = 4;
            $terminal = self::terminal($fixture, 'ActivityCompleted');
            $terminal['recorded_at'] = $fixture['history'][6]['recorded_at'];

            return self::insert($fixture, 7, $terminal);
        }, 'earlier call is unresolved'];
        $cases['untyped request metadata cannot create a distant next step'] = [static function (array $fixture): array {
            $fixture = self::insert($fixture, 5, self::terminal($fixture, 'ActivityCompleted'));
            $fixture['history'][6]['payload']['sequence'] = 89;
            $fixture['history'][7]['payload']['sequence'] = 90;

            return $fixture;
        }, 'after the next step'];

        return $cases;
    }

    #[DataProvider('resolvedCalls')]
    public function testMatchedEarlierResolutionAllowsTheNextStep(string $kind): void
    {
        $fixture = self::withCall(RedriveFixtureRegistry::fixture(self::FIXTURE), $kind);
        $fixture['history'][6]['payload']['sequence'] = 4;
        $fixture = self::insert($fixture, 6, self::terminal($fixture, self::resolution($kind)));
        self::assertNull(RedriveSuccessorHistory::violation($fixture, RedriveFixtureRegistry::SUCCESSORS[self::FIXTURE]));
    }

    /** @return array<string, array{string}> */
    public static function resolvedCalls(): array
    {
        return [
            'activity' => ['ActivityScheduled'],
            'timer' => ['TimerScheduled'],
            'signal' => ['SignalWaitOpened'],
        ];
    }

    public function testTypedCancellationAfterRequestAtTheSameTimeIsValid(): void
    {
        $fixture = RedriveFixtureRegistry::fixture(self::FIXTURE);
        $fixture['history'][5]['recorded_at'] = $fixture['history'][6]['recorded_at'];
        $fixture = self::insert($fixture, 6, self::terminal($fixture, 'ActivityCancelled'));
        self::assertNull(RedriveSuccessorHistory::violation($fixture, RedriveFixtureRegistry::SUCCESSORS[self::FIXTURE]));
    }

    public function testTypedCancellationBeforeRequestAtTheSameTimeIsResolved(): void
    {
        $fixture = RedriveFixtureRegistry::fixture(self::FIXTURE);
        $fixture = self::insert($fixture, 5, self::terminal($fixture, 'ActivityCancelled'));
        self::assertStringContainsString('resolved before the request', (string) RedriveSuccessorHistory::violation($fixture, RedriveFixtureRegistry::SUCCESSORS[self::FIXTURE]));
    }

    public function testMatchedActivityCancellationAllowsTheNextStep(): void
    {
        $fixture = RedriveFixtureRegistry::fixture(self::FIXTURE);
        $fixture['history'][6]['payload']['sequence'] = 4;
        $fixture = self::insert($fixture, 6, self::terminal($fixture, 'ActivityCancelled'));
        self::assertNull(RedriveSuccessorHistory::violation($fixture, RedriveFixtureRegistry::SUCCESSORS[self::FIXTURE]));
    }

    public function testRootAddressKeyOrderAndMatchingTextReasonAreValid(): void
    {
        $fixture = RedriveFixtureRegistry::fixture(self::FIXTURE);
        $fixture['history'][5]['payload']['reason'] = 'operator cancellation';
        $context = $fixture['history'][5]['payload']['cancellation'];
        $context['reason'] = 'operator cancellation';
        $context['lineage'][0] = array_reverse($context['lineage'][0], preserve_keys: true);
        $fixture['history'][5]['payload']['cancellation'] = $context;
        $fixture['history'][6]['payload']['cancellation'] = $context;
        self::assertNull(RedriveSuccessorHistory::violation($fixture, RedriveFixtureRegistry::SUCCESSORS[self::FIXTURE]));
    }

    public function testUntypedMetadataCannotHideAReachableNextStep(): void
    {
        $fixture = RedriveFixtureRegistry::fixture(self::FIXTURE);
        $fixture['history'][6]['payload']['sequence'] = 4;
        $fixture['history'][5]['payload']['sequence'] = 99;
        $fixture = self::insert($fixture, 5, self::terminal($fixture, 'ActivityCompleted'));
        self::assertNull(RedriveSuccessorHistory::violation($fixture, RedriveFixtureRegistry::SUCCESSORS[self::FIXTURE]));
    }

    /**
     * @param array<string, mixed> $fixture
     * @return array<string, mixed>
     */
    private static function withCall(array $fixture, string $kind): array
    {
        $fixture['history'][4] = [...$fixture['history'][4], 'event_type' => $kind, 'payload' => match ($kind) {
            'TimerScheduled' => ['sequence' => 3, 'timer_id' => 'successor-timer'],
            'SignalWaitOpened' => ['sequence' => 3, 'signal_wait_id' => 'successor-wait', 'signal_name' => 'approval'],
            default => $fixture['history'][4]['payload'],
        }];

        return $fixture;
    }

    private static function identity(string $kind): string
    {
        return match ($kind) {
            'TimerScheduled' => 'timer_id',
            'SignalWaitOpened' => 'signal_wait_id',
            default => 'activity_execution_id',
        };
    }

    private static function resolution(string $kind): string
    {
        return match ($kind) {
            'TimerScheduled' => 'TimerFired',
            'SignalWaitOpened' => 'SignalApplied',
            default => 'ActivityCompleted',
        };
    }

    /**
     * @param array<string, mixed> $fixture
     * @return array<string, mixed>
     */
    private static function terminal(array $fixture, string $kind): array
    {
        $payload = $fixture['history'][4]['payload'];
        if ($kind === 'SignalApplied') {
            $payload['workflow_command_id'] = 'signal-command';
            $payload['signal_id'] = 'signal-event';
        }

        return ['event_type' => $kind, 'payload' => $payload];
    }

    /**
     * @param array<string, mixed> $fixture
     * @param array<string, mixed> $event
     * @return array<string, mixed>
     */
    private static function insert(array $fixture, int $index, array $event): array
    {
        $event['recorded_at'] ??= $fixture['history'][$index]['recorded_at'] ?? $fixture['history'][count($fixture['history']) - 1]['recorded_at'];
        array_splice($fixture['history'], $index, 0, [$event]);
        foreach ($fixture['history'] as $offset => &$row) {
            $row['sequence'] = $offset + 1;
        }

        return $fixture;
    }
}
