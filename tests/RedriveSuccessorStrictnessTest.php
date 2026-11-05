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

    /** @param array<string, mixed> $opening */
    #[DataProvider('priorCallResolutions')]
    public function testPriorCallAcceptsEverySupportedTerminal(array $opening, string $terminalType): void
    {
        $fixture = self::priorCallFixture($opening);
        $terminal = ['event_type' => $terminalType, 'payload' => $opening['payload']];
        $fixture = self::insert($fixture, 6, $terminal);

        self::assertNull(RedriveSuccessorHistory::violation($fixture, RedriveFixtureRegistry::SUCCESSORS[self::FIXTURE]));
    }

    /** @return iterable<string, array{array<string, mixed>, string}> */
    public static function priorCallResolutions(): iterable
    {
        foreach (self::priorCallFamilies() as $name => [$opening, , $terminals]) {
            foreach ($terminals as $terminal) {
                yield $name.' / '.$terminal => [$opening, $terminal];
            }
        }
    }

    /** @param array<string, mixed> $opening */
    #[DataProvider('priorCallRefusals')]
    public function testPriorCallRequiresItsOwnLaterResolution(array $opening, string $identity, string $terminalType, string $fault): void
    {
        $fixture = self::priorCallFixture($opening);
        $terminal = ['event_type' => $terminalType, 'payload' => $opening['payload']];
        $position = 6;
        if ($fault === 'other resource') {
            $terminal['payload'][$identity] = 'foreign-resource';
        } elseif ($fault === 'other step') {
            $terminal['payload']['sequence'] = 4;
            $fixture['history'][6]['payload']['sequence'] = 5;
        } elseif ($fault === 'other family') {
            $terminal['event_type'] = in_array($identity, ['timer_id', 'condition_wait_id', 'signal_wait_id'], true)
                ? 'ActivityCompleted' : 'TimerFired';
        } elseif ($fault === 'before opener') {
            $position = 4;
        } elseif ($fault === 'after delivery') {
            $position = 7;
        } elseif ($fault === 'missing opener identity') {
            unset($fixture['history'][4]['payload'][$identity]);
        } elseif ($fault === 'missing terminal identity') {
            unset($terminal['payload'][$identity]);
        }
        if ($fault !== 'unresolved') {
            $fixture = self::insert($fixture, $position, $terminal);
        }

        self::assertStringContainsString('earlier call is unresolved', (string) RedriveSuccessorHistory::violation(
            $fixture, RedriveFixtureRegistry::SUCCESSORS[self::FIXTURE],
        ));
    }

    /** @return iterable<string, array{array<string, mixed>, string, string, string}> */
    public static function priorCallRefusals(): iterable
    {
        foreach (self::priorCallFamilies() as $name => [$opening, $identity, $terminals]) {
            foreach (['unresolved', 'other resource', 'other step', 'other family', 'before opener', 'after delivery',
                'missing opener identity', 'missing terminal identity'] as $fault) {
                yield $name.' / '.$fault => [$opening, $identity, $terminals[0], $fault];
            }
        }
    }

    public function testPriorCallDistinguishesLocalAndRemoteActivityResolutions(): void
    {
        foreach ([false, true] as $local) {
            $opening = ['event_type' => 'ActivityScheduled', 'payload' => ['sequence' => 3,
                'activity_execution_id' => 'activity', 'activity_type' => 'effect', 'local_activity' => $local]];
            $fixture = self::priorCallFixture($opening);
            $terminal = ['event_type' => 'ActivityCompleted', 'payload' => [...$opening['payload'], 'local_activity' => !$local]];
            $fixture = self::insert($fixture, 6, $terminal);
            self::assertStringContainsString('earlier call is unresolved', (string) RedriveSuccessorHistory::violation(
                $fixture, RedriveFixtureRegistry::SUCCESSORS[self::FIXTURE],
            ));
        }
    }

    public function testPriorCallChildIdentitySurvivesAChangedRunAndLink(): void
    {
        [$opening] = self::priorCallFamilies()['child'];
        $fixture = self::priorCallFixture($opening);
        $terminal = ['event_type' => 'ChildRunCompleted', 'payload' => [...$opening['payload'],
            'child_workflow_run_id' => 'continued-child-run', 'workflow_link_id' => 'continued-link']];
        $fixture = self::insert($fixture, 6, $terminal);
        self::assertNull(RedriveSuccessorHistory::violation($fixture, RedriveFixtureRegistry::SUCCESSORS[self::FIXTURE]));
    }

    #[DataProvider('serviceOperationEvents')]
    public function testPriorCallExplicitlyExcludesServiceOperationHistories(string $type): void
    {
        $fixture = self::priorCallFixture(['event_type' => $type,
            'payload' => ['sequence' => 3, 'service_operation_id' => 'operation']]);
        self::assertStringContainsString('service-operation histories are outside', (string) RedriveSuccessorHistory::violation(
            $fixture, RedriveFixtureRegistry::SUCCESSORS[self::FIXTURE],
        ));
    }

    /** @return iterable<string, array{string}> */
    public static function serviceOperationEvents(): iterable
    {
        foreach (['ServiceOperationScheduled', 'ServiceCallStarted', 'ServiceCallCompleted', 'ServiceCallFailed', 'ServiceCallCancelled'] as $type) {
            yield $type => [$type];
        }
    }

    public function testPriorCallExplicitlyExcludesServiceOperationDelivery(): void
    {
        $fixture = RedriveFixtureRegistry::fixture(self::FIXTURE);
        $fixture['history'][6]['payload']['call_kind'] = 'service_operation';
        self::assertStringContainsString('single-call kind', (string) RedriveSuccessorHistory::violation(
            $fixture, RedriveFixtureRegistry::SUCCESSORS[self::FIXTURE],
        ));
    }

    #[DataProvider('waitTimeoutResolutions')]
    public function testWaitTimeoutAcceptsItsMatchingTimerTerminal(string $kind, string $layout, string $terminalType): void
    {
        $fixture = self::waitTimeoutFixture($kind, $layout);
        $fixture = self::insert($fixture, self::deliveryIndex($fixture), self::waitTimeoutTerminal($kind, $terminalType));
        self::assertNull(RedriveSuccessorHistory::violation($fixture, RedriveFixtureRegistry::SUCCESSORS[self::FIXTURE]));
    }

    /** @return iterable<string, array{string, string, string}> */
    public static function waitTimeoutResolutions(): iterable
    {
        foreach (['condition', 'signal'] as $kind) {
            foreach (['wait', 'timer', 'both'] as $layout) {
                foreach (['TimerFired', 'TimerCancelled'] as $terminal) {
                    yield $kind.' / '.$layout.' / '.$terminal => [$kind, $layout, $terminal];
                }
            }
        }
    }

    #[DataProvider('waitTimeoutRefusals')]
    public function testWaitTimeoutRejectsAnUnrelatedTimerTerminal(string $kind, string $terminalType, string $fault): void
    {
        $fixture = self::waitTimeoutFixture($kind, 'both');
        $terminal = self::waitTimeoutTerminal($kind, $terminalType);
        $position = self::deliveryIndex($fixture);
        if ($fault === 'missing kind') {
            unset($terminal['payload']['timer_kind']);
        } elseif ($fault === 'null kind') {
            $terminal['payload']['timer_kind'] = null;
        } elseif ($fault === 'other timeout kind') {
            $terminal['payload']['timer_kind'] = $kind === 'condition' ? 'signal_timeout' : 'condition_timeout';
        } elseif ($fault === 'plain kind') {
            $terminal['payload']['timer_kind'] = 'ordinary';
        } elseif ($fault === 'other wait') {
            $terminal['payload'][$kind.'_wait_id'] = 'foreign-wait';
        } elseif ($fault === 'missing wait') {
            unset($terminal['payload'][$kind.'_wait_id']);
        } elseif ($fault === 'other step') {
            $terminal['payload']['sequence'] = 4;
            $fixture['history'][$position]['payload']['sequence'] = 5;
        } elseif ($fault === 'after delivery') {
            ++$position;
        } elseif ($fault === 'before opener') {
            $position = 4;
        }
        $fixture = self::insert($fixture, $position, $terminal);
        self::assertStringContainsString('earlier call is unresolved', (string) RedriveSuccessorHistory::violation(
            $fixture, RedriveFixtureRegistry::SUCCESSORS[self::FIXTURE],
        ));
    }

    /** @return iterable<string, array{string, string, string}> */
    public static function waitTimeoutRefusals(): iterable
    {
        foreach (['condition', 'signal'] as $kind) {
            foreach (['TimerFired', 'TimerCancelled'] as $terminal) {
                foreach (['missing kind', 'null kind', 'other timeout kind', 'plain kind', 'other wait', 'missing wait',
                    'other step', 'after delivery', 'before opener'] as $fault) {
                    yield $kind.' / '.$terminal.' / '.$fault => [$kind, $terminal, $fault];
                }
            }
        }
    }

    #[DataProvider('plainTimerRefusals')]
    public function testPlainTimerResolutionCannotUseAWaitTimeoutOrForeignTimer(string $terminalType, string $fault): void
    {
        [$opening] = self::priorCallFamilies()['timer'];
        $fixture = self::priorCallFixture($opening);
        $terminal = ['event_type' => $terminalType, 'payload' => $opening['payload']];
        if ($fault === 'other timer') {
            $terminal['payload']['timer_id'] = 'foreign-timer';
        } elseif ($fault === 'missing timer') {
            unset($terminal['payload']['timer_id']);
        } else {
            $terminal['payload']['timer_kind'] = $fault.'_timeout';
            $terminal['payload'][$fault.'_wait_id'] = $fault;
        }
        $fixture = self::insert($fixture, 6, $terminal);
        self::assertStringContainsString('earlier call is unresolved', (string) RedriveSuccessorHistory::violation(
            $fixture, RedriveFixtureRegistry::SUCCESSORS[self::FIXTURE],
        ));
    }

    /** @return iterable<string, array{string, string}> */
    public static function plainTimerRefusals(): iterable
    {
        foreach (['TimerFired', 'TimerCancelled'] as $terminal) {
            foreach (['condition', 'signal', 'other timer', 'missing timer'] as $fault) {
                yield $terminal.' / '.$fault => [$terminal, $fault];
            }
        }
    }

    /** @return array<string, mixed> */
    private static function waitTimeoutFixture(string $kind, string $layout): array
    {
        $families = self::priorCallFamilies();
        $fixture = self::priorCallFixture($families[$kind.($layout === 'timer' ? ' timer' : '')][0]);
        if ($layout === 'both') {
            $fixture = self::insert($fixture, 5, $families[$kind.' timer'][0]);
        }

        return $fixture;
    }

    /** @return array<string, mixed> */
    private static function waitTimeoutTerminal(string $kind, string $type): array
    {
        return ['event_type' => $type, 'payload' => ['sequence' => 3, 'timer_id' => 'timer',
            'timer_kind' => $kind.'_timeout', $kind.'_wait_id' => $kind]];
    }

    /** @param array<string, mixed> $fixture */
    private static function deliveryIndex(array $fixture): int
    {
        foreach ($fixture['history'] as $index => $event) {
            if ($event['event_type'] === 'CooperativeCancellationDelivered') {
                return (int) $index;
            }
        }

        throw new \LogicException('The source-control fixture requires its cancellation delivery.');
    }

    /** @return array<string, array{array<string, mixed>, string, list<string>}> */
    private static function priorCallFamilies(): array
    {
        $activity = ['sequence' => 3, 'activity_execution_id' => 'activity', 'activity_type' => 'effect'];
        $activityTerminals = ['ActivityCompleted', 'ActivityFailed', 'ActivityTimedOut', 'ActivityCancelled'];

        return [
            'activity' => [['event_type' => 'ActivityScheduled', 'payload' => $activity], 'activity_execution_id', $activityTerminals],
            'local activity' => [['event_type' => 'ActivityScheduled', 'payload' => [...$activity, 'local_activity' => true]],
                'activity_execution_id', $activityTerminals],
            'timer' => [['event_type' => 'TimerScheduled', 'payload' => ['sequence' => 3, 'timer_id' => 'timer']],
                'timer_id', ['TimerFired', 'TimerCancelled']],
            'condition' => [['event_type' => 'ConditionWaitOpened', 'payload' => ['sequence' => 3, 'condition_wait_id' => 'condition']],
                'condition_wait_id', ['ConditionWaitSatisfied', 'ConditionWaitTimedOut']],
            'signal' => [['event_type' => 'SignalWaitOpened', 'payload' => ['sequence' => 3, 'signal_wait_id' => 'signal']],
                'signal_wait_id', ['SignalApplied']],
            'child' => [['event_type' => 'ChildWorkflowScheduled', 'payload' => ['sequence' => 3, 'child_call_id' => 'child-call',
                'child_workflow_instance_id' => 'child', 'child_workflow_run_id' => 'initial-child-run', 'workflow_link_id' => 'initial-link']],
                'child_call_id', ['ChildRunCompleted', 'ChildRunFailed', 'ChildRunCancelled', 'ChildRunTerminated']],
            'condition timer' => [['event_type' => 'TimerScheduled', 'payload' => ['sequence' => 3, 'timer_id' => 'timer',
                'timer_kind' => 'condition_timeout', 'condition_wait_id' => 'condition']],
                'condition_wait_id', ['ConditionWaitSatisfied', 'ConditionWaitTimedOut']],
            'signal timer' => [['event_type' => 'TimerScheduled', 'payload' => ['sequence' => 3, 'timer_id' => 'timer',
                'timer_kind' => 'signal_timeout', 'signal_wait_id' => 'signal']], 'signal_wait_id', ['SignalApplied']],
        ];
    }

    /**
     * @param array<string, mixed> $opening
     * @return array<string, mixed>
     */
    private static function priorCallFixture(array $opening): array
    {
        $fixture = RedriveFixtureRegistry::fixture(self::FIXTURE);
        $fixture['history'][4] = [...$fixture['history'][4], ...$opening];
        $fixture['history'][6]['payload']['sequence'] = 4;

        return $fixture;
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
