<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DateTimeImmutable;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Tests\Support\RedriveFixtureRegistry;
use DurableWorkflow\Tests\Support\RedriveSuccessorHistory;
use DurableWorkflow\Tests\Support\ReplayRegressionFixture;
use DurableWorkflow\Worker\CancellationReplayClock;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * Valid Workflow 2.19 redrive successor evidence for reused activity
 * completions, fired timers, side effects and version markers, and for a
 * successor cancellation after a reused prefix (#247).
 *
 * The 3.8.3 cancellation-clock fixture was not a history redrive can write:
 * it had no redrive WorkflowStarted and placed the cancellation before the
 * reused prefix. Redrive writes WorkflowStarted and the reused prefix in one
 * transaction, so a successor cancellation is always requested and delivered
 * after it, and remaining() is the cleanup deadline less that delivery. The
 * reused times cannot reach remaining() in a valid history; these tests state
 * that as non-leakage, not as proof that reused_recorded_at is preferred.
 */
final class RedriveSuccessorEvidenceTest extends TestCase
{
    private const SECOND_ACTIVITY = 'Tests\\Fixtures\\V2\\TestRedriveSecondActivity';

    private const CANCELLATION = 'redrive-successor-reused-cancellation-clock';

    /**
     * Gate: every fixture that names redrive, carries a reuse key or opens
     * with a redrive WorkflowStarted is a registered valid successor, a named
     * non-qualifying 3.8.3 fixture, or a source run. A new invalid shape, or a
     * stale registry entry, fails here.
     */
    public function testEveryRedriveFixtureIsAValidSuccessorOrANamedException(): void
    {
        $selected = [];
        foreach (RedriveFixtureRegistry::directories() as $directory) {
            foreach (glob($directory.'/*.json') ?: [] as $path) {
                $name = basename($path, '.json');
                $fixture = RedriveFixtureRegistry::fixture($name);
                if (str_contains($name, 'redrive') || self::carriesReuse($fixture) || self::opensRedrive($fixture)) {
                    $selected[] = $name;
                }
            }
        }
        sort($selected);
        $registered = array_merge(
            array_keys(RedriveFixtureRegistry::SUCCESSORS),
            array_keys(RedriveFixtureRegistry::NON_QUALIFYING),
            RedriveFixtureRegistry::SOURCE_RUNS,
        );
        sort($registered);
        self::assertSame($registered, $selected);

        $passing = [];
        foreach ($selected as $name) {
            $fixture = RedriveFixtureRegistry::fixture($name);
            foreach (array_merge(RedriveFixtureRegistry::SUCCESSORS, array_column(RedriveFixtureRegistry::NON_QUALIFYING, 'prefix')) as $prefix) {
                if (RedriveSuccessorHistory::violation($fixture, $prefix) === null) {
                    $passing[$name] = true;
                }
            }
            if (in_array($name, RedriveFixtureRegistry::SOURCE_RUNS, true)) {
                self::assertFalse(self::carriesReuse($fixture), "{$name} is a source run and carries no reuse key");
                self::assertFalse(self::opensRedrive($fixture), "{$name} is a source run, not a redrive successor");
            }
        }
        $successors = array_keys(RedriveFixtureRegistry::SUCCESSORS);
        sort($successors);
        $passing = array_keys($passing);
        sort($passing);
        self::assertSame($successors, $passing, 'exactly the registered successors pass the successor check');
    }

    #[DataProvider('successors')]
    public function testRegisteredSuccessorPassesTheSuccessorCheck(string $name): void
    {
        self::assertNull(RedriveSuccessorHistory::violation(
            RedriveFixtureRegistry::fixture($name),
            RedriveFixtureRegistry::SUCCESSORS[$name],
        ));
    }

    #[DataProvider('nonQualifying')]
    public function testNonQualifyingFixtureStaysByteIdenticalAndKeepsFailing(string $name): void
    {
        $entry = RedriveFixtureRegistry::NON_QUALIFYING[$name];
        self::assertSame($entry['sha256'], hash_file('sha256', RedriveFixtureRegistry::path($name)));
        self::assertSame($entry['violation'], RedriveSuccessorHistory::violation(RedriveFixtureRegistry::fixture($name), $entry['prefix']));
        self::assertArrayHasKey($entry['replacement'], RedriveFixtureRegistry::SUCCESSORS);
        self::assertNull(RedriveSuccessorHistory::violation(
            RedriveFixtureRegistry::fixture($entry['replacement']),
            RedriveFixtureRegistry::SUCCESSORS[$entry['replacement']],
        ));
    }

    /**
     * Give the 3.8.3 cancellation-clock history its replacement's full redrive
     * envelope and task. It still fails: its cancellation is requested before
     * the successor start, ahead of the reused prefix, an order redrive cannot
     * write.
     */
    public function testTheThreeEightThreeOrderFailsEvenWithAFullEnvelope(): void
    {
        $replacement = RedriveFixtureRegistry::fixture(self::CANCELLATION);
        $old = RedriveFixtureRegistry::fixture('redrive-reused-cancellation-clock');
        $old['workflow'] = $replacement['workflow'];
        $old['workflow_tasks'] = $replacement['workflow_tasks'];
        $old['history'] = array_merge([$replacement['history'][0]], $old['history']);
        foreach ($old['history'] as $index => $event) {
            $old['history'][$index]['sequence'] = $index + 1;
        }

        self::assertSame(
            'history order: CooperativeCancellationRequested must not predate the event before it',
            RedriveSuccessorHistory::violation($old, ['ActivityCompleted', 'TimerScheduled', 'TimerFired']),
        );
    }

    public function testCoreReusedMarkersSuccessorReplaysColdWithoutRunningTheSideEffect(): void
    {
        $fixture = RedriveFixtureRegistry::fixture('redrive-successor-reused-markers');
        self::assertSame(4, $fixture['history'][0]['payload']['resume_step_sequence']);

        self::assertSecondActivityOnly(self::onlyCommands($fixture));

        $calls = 0;
        $result = (new Replayer(new AvroPayloadCodec()))->replay(
            static function (WorkflowContext $context, string $name) use (&$calls): mixed {
                $context->sideEffect(static function () use (&$calls): string {
                    ++$calls;

                    return 'fresh';
                });
                $context->getVersion('redrive-markers', -1, 1);
                $greeting = $context->activity('Tests\\Fixtures\\V2\\TestRedriveFirstActivity', [$name]);

                return $context->activity(self::SECOND_ACTIVITY, [$greeting]);
            },
            $fixture['history'],
            ['Taylor'],
            'redrive-replay',
            $fixture['workflow_tasks'][0],
        );
        self::assertSame(0, $calls);
        self::assertSame(['schedule_activity'], array_column($result->commands, 'type'));
    }

    public function testCoreReusedTimerSuccessorReplaysColdWithoutStartingTheTimer(): void
    {
        $fixture = RedriveFixtureRegistry::fixture('redrive-successor-reused-timer');
        self::assertSame(3, $fixture['history'][0]['payload']['resume_step_sequence']);
        self::assertSame(['TimerScheduled', 'TimerFired'], [$fixture['history'][2]['event_type'], $fixture['history'][3]['event_type']]);
        self::assertSame($fixture['history'][2]['payload']['timer_id'], $fixture['history'][3]['payload']['timer_id']);
        self::assertArrayNotHasKey('reused_from_run_id', $fixture['history'][2]['payload']);

        self::assertSecondActivityOnly(self::onlyCommands($fixture));
    }

    public function testSuccessorCancellationAfterTheReusedPrefixMeasuresFromItsDelivery(): void
    {
        $fixture = RedriveFixtureRegistry::fixture(self::CANCELLATION);

        // Deadline 00:05:31 less the successor delivery at 00:05:02.
        self::assertSame([['type' => 'complete_workflow', 'result' => 29.0]], self::onlyCommands($fixture));
    }

    /**
     * Non-leakage, within valid histories: move a reused event's source time
     * anywhere in the source run. The history still passes the successor
     * check, and the commands, including remaining(), do not change.
     */
    #[DataProvider('reusedEventsOfEverySuccessor')]
    public function testRetimingAReusedEventInsideTheSourceRunChangesNothing(string $name, int $index): void
    {
        $fixture = RedriveFixtureRegistry::fixture($name);
        $baseline = ReplayRegressionFixture::execute($fixture);
        foreach (['2026-11-03T00:00:00+00:00', '2026-11-03T00:04:59.999999+00:00'] as $sourceTime) {
            $retimed = $fixture;
            $retimed['history'][$index]['payload']['reused_recorded_at'] = $sourceTime;

            self::assertNull(RedriveSuccessorHistory::violation($retimed, RedriveFixtureRegistry::SUCCESSORS[$name]));
            self::assertSame($baseline, ReplayRegressionFixture::execute($retimed), "{$name} history[{$index}] at {$sourceTime}");
        }
    }

    /**
     * Counterfactual for a clock that reads pre-delivery events. Retime a
     * reused event (both recorded_at and reused_recorded_at) to 00:05:20,
     * after the 00:05:02 delivery. That is not a history redrive writes; it
     * gives a leaking clock something to read. A clock that observed it would
     * report 31 - 20 = 11s. The replayed result stays 29s.
     */
    #[DataProvider('reusedEventsOfTheCancellationSuccessor')]
    public function testARetimedReusedEventDoesNotReachRemaining(int $index): void
    {
        $fixture = RedriveFixtureRegistry::fixture(self::CANCELLATION);
        $retimed = $fixture;
        $retimed['history'][$index]['recorded_at'] = '2026-11-03T00:05:20+00:00';
        $retimed['history'][$index]['payload']['reused_recorded_at'] = '2026-11-03T00:05:20+00:00';

        self::assertNotNull(RedriveSuccessorHistory::violation($retimed, RedriveFixtureRegistry::SUCCESSORS[self::CANCELLATION]));
        self::assertSame(11.0, self::leakingRemaining($retimed));
        self::assertSame(29.0, self::leakingRemaining($fixture));
        self::assertSame(ReplayRegressionFixture::execute($fixture), ReplayRegressionFixture::execute($retimed));
        self::assertSame([['type' => 'complete_workflow', 'result' => 29.0]], self::onlyCommands($retimed));
    }

    /**
     * Positive control: the same 00:05:20 retiming applied to the delivery,
     * which remaining() does measure from, changes the result to 11s. The
     * assertion above would therefore catch a reused event reaching the clock.
     */
    public function testRetimingTheDeliveryDoesChangeRemaining(): void
    {
        $fixture = RedriveFixtureRegistry::fixture(self::CANCELLATION);
        $delivery = count($fixture['history']) - 1;
        self::assertSame('CooperativeCancellationDelivered', $fixture['history'][$delivery]['event_type']);
        $fixture['history'][$delivery]['recorded_at'] = '2026-11-03T00:05:20+00:00';
        $fixture['expected']['workflow_tasks'][0]['command_sequence'][0]['result'] = 11.0;

        self::assertSame([['type' => 'complete_workflow', 'result' => 11.0]], self::onlyCommands($fixture));
    }

    #[DataProvider('wrongResumeSteps')]
    public function testSuccessorCheckRejectsAWrongResumeStep(string $name, mixed $resume): void
    {
        $fixture = RedriveFixtureRegistry::fixture($name);
        $fixture['history'][0]['payload']['resume_step_sequence'] = $resume;

        self::assertStringContainsString(
            'resume_step_sequence',
            (string) RedriveSuccessorHistory::violation($fixture, RedriveFixtureRegistry::SUCCESSORS[$name]),
        );
    }

    /** @param callable(array<string, mixed>): array<string, mixed> $mutate */
    #[DataProvider('invalidSuccessors')]
    public function testSuccessorCheckRejectsAnInvalidShape(string $name, callable $mutate, string $rule): void
    {
        $fixture = $mutate(RedriveFixtureRegistry::fixture($name));

        $violation = RedriveSuccessorHistory::violation($fixture, RedriveFixtureRegistry::SUCCESSORS[$name]);

        self::assertNotNull($violation);
        self::assertStringContainsString($rule, $violation);
    }

    /** @param list<string> $prefix */
    #[DataProvider('wrongPrefixes')]
    public function testSuccessorCheckBindsTheCopiedTimerPrefix(string $name, array $prefix, string $rule): void
    {
        self::assertStringContainsString($rule, (string) RedriveSuccessorHistory::violation(RedriveFixtureRegistry::fixture($name), $prefix));
    }

    /** @return array<string, array{string}> */
    public static function successors(): array
    {
        $names = array_keys(RedriveFixtureRegistry::SUCCESSORS);

        return array_combine($names, array_map(static fn (string $name): array => [$name], $names));
    }

    /** @return array<string, array{string}> */
    public static function nonQualifying(): array
    {
        $names = array_keys(RedriveFixtureRegistry::NON_QUALIFYING);

        return array_combine($names, array_map(static fn (string $name): array => [$name], $names));
    }

    /** @return array<string, array{string, int}> */
    public static function reusedEventsOfEverySuccessor(): array
    {
        $cases = [];
        foreach (['redrive-successor-reused-markers', 'redrive-successor-reused-timer', self::CANCELLATION] as $name) {
            foreach (RedriveFixtureRegistry::fixture($name)['history'] as $index => $event) {
                if (array_key_exists('reused_recorded_at', $event['payload'])) {
                    $cases["{$name} {$event['event_type']}"] = [$name, $index];
                }
            }
        }

        return $cases;
    }

    /** @return array<string, array{int}> */
    public static function reusedEventsOfTheCancellationSuccessor(): array
    {
        $cases = [];
        foreach (RedriveFixtureRegistry::fixture(self::CANCELLATION)['history'] as $index => $event) {
            if (array_key_exists('reused_recorded_at', $event['payload'])) {
                $cases[$event['event_type']] = [$index];
            }
        }

        return $cases;
    }

    /** @return array<string, array{string, mixed}> */
    public static function wrongResumeSteps(): array
    {
        $cases = [];
        foreach (['redrive-successor-reused-markers' => 4, 'redrive-successor-reused-timer' => 3, self::CANCELLATION => 3] as $name => $resume) {
            foreach (['minus one' => $resume - 1, 'plus one' => $resume + 1, 'null' => null, 'string' => (string) $resume] as $label => $value) {
                $cases["{$name} {$label}"] = [$name, $value];
            }
        }

        return $cases;
    }

    /** @return array<string, array{string, callable(array<string, mixed>): array<string, mixed>, string}> */
    public static function invalidSuccessors(): array
    {
        $timer = 'redrive-successor-reused-timer';
        $set = static fn (int $index, string $key, mixed $value): callable => static function (array $fixture) use ($index, $key, $value): array {
            $fixture['history'][$index]['payload'][$key] = $value;

            return $fixture;
        };
        $cases = [];
        foreach (['redrive-successor-reused-markers', $timer, self::CANCELLATION] as $name) {
            foreach (['workflow_type', 'workflow_instance_id', 'workflow_run_id', 'workflow_command_id', 'workflow_link_id', 'continued_from_run_id'] as $field) {
                $cases["{$name} missing {$field}"] = [$name, static function (array $fixture) use ($field): array {
                    unset($fixture['history'][0]['payload'][$field]);

                    return $fixture;
                }, $field];
                $cases["{$name} blank {$field}"] = [$name, $set(0, $field, ' '), $field];
            }
            foreach (['execution_deadline_at', 'run_deadline_at'] as $field) {
                $cases["{$name} missing {$field}"] = [$name, static function (array $fixture) use ($field): array {
                    unset($fixture['history'][0]['payload'][$field]);

                    return $fixture;
                }, $field];
                $cases["{$name} invalid {$field}"] = [$name, $set(0, $field, 'tomorrow'), $field];
            }
            $cases["{$name} not a redrive"] = [$name, $set(0, 'recovery_kind', 'continue_as_new'), 'recovery_kind'];
            $cases["{$name} replayed after the start"] = [$name, $set(0, 'replayed_started_at', '2026-11-03T00:05:01+00:00'), 'replayed_started_at'];
            foreach (RedriveFixtureRegistry::fixture($name)['history'] as $index => $event) {
                if (!array_key_exists('reused_recorded_at', $event['payload'])) {
                    continue;
                }
                foreach (['reused_from_run_id', 'reused_recorded_at'] as $key) {
                    $cases["{$name} {$event['event_type']} missing {$key}"] = [$name, static function (array $fixture) use ($index, $key): array {
                        unset($fixture['history'][$index]['payload'][$key]);

                        return $fixture;
                    }, $key];
                    $cases["{$name} {$event['event_type']} blank {$key}"] = [$name, $set($index, $key, '  '), $key];
                }
                $cases["{$name} {$event['event_type']} from another run"] = [$name, $set($index, 'reused_from_run_id', '01JBREDRIVEOTHERRUN0000000'), 'reused_from_run_id'];
                $cases["{$name} {$event['event_type']} after the successor start"] = [$name, $set($index, 'reused_recorded_at', '2026-11-03T00:05:00+00:00'), 'source run'];
            }
        }

        // Fired-timer copies, exactly as FailedRunRedrivePlan::successorEvents() writes them.
        foreach (['reused_from_run_id' => '01JBREDRIVESOURCERUN000000', 'reused_recorded_at' => '2026-11-03T00:00:05+00:00'] as $key => $value) {
            $cases["copied TimerScheduled carries {$key}"] = [$timer, $set(2, $key, $value), "must not carry {$key}"];
        }
        $cases['copied TimerScheduled names another timer'] = [$timer, $set(2, 'timer_id', '01JBREDRIVEOTHERTIMER00000'), 'same timer_id and step'];
        $cases['copied TimerScheduled without a timer'] = [$timer, $set(2, 'timer_id', ' '), 'same timer_id and step'];
        $cases['copied TimerScheduled on another step'] = [$timer, $set(2, 'sequence', 3), 'same timer_id and step'];
        $cases['copied TimerScheduled after the successor start'] = [$timer, static function (array $fixture): array {
            $fixture['history'][2]['recorded_at'] = '2026-11-03T00:05:00.100000+00:00';

            return $fixture;
        }, 'recorded with the successor start'];
        $cases['copied TimerScheduled not followed by a TimerFired'] = [$timer, static function (array $fixture): array {
            $fixture['history'][3]['event_type'] = 'TimerCancelled';

            return $fixture;
        }, 'same timer_id and step'];

        // Successor cancellation, which must follow the prefix and bind to the successor.
        $cases['the 3.8.3 order: cancellation before the reused prefix'] = [self::CANCELLATION, static function (array $fixture): array {
            $history = $fixture['history'];
            $fixture['history'] = [$history[0], $history[5], $history[6], $history[1], $history[2], $history[3], $history[4]];
            foreach ($fixture['history'] as $index => $event) {
                $fixture['history'][$index]['sequence'] = $index + 1;
            }

            return $fixture;
        }, 'reused prefix position 0'];
        $cases['delivery to the reused timer step'] = [self::CANCELLATION, $set(6, 'sequence', 2), 'must not reuse a reused step sequence'];
        $cases['delivery to the reused activity step'] = [self::CANCELLATION, $set(6, 'sequence', 1), 'must not reuse a reused step sequence'];
        $cases['request before the successor start'] = [self::CANCELLATION, static function (array $fixture): array {
            $fixture['history'][5]['recorded_at'] = '2026-11-03T00:00:01+00:00';

            return $fixture;
        }, 'predate'];
        $cases['request on the source run'] = [self::CANCELLATION, $set(5, 'workflow_run_id', '01JBREDRIVESOURCERUN000000'), 'successor run'];
        $cases['delivery on the source run'] = [self::CANCELLATION, $set(6, 'workflow_run_id', '01JBREDRIVESOURCERUN000000'), 'successor run'];
        $cases['request for another instance'] = [self::CANCELLATION, $set(5, 'workflow_instance_id', 'another-instance'), 'successor instance'];
        $cases['cancellation rooted at the source run'] = [self::CANCELLATION, static function (array $fixture): array {
            $fixture['history'][5]['payload']['cancellation']['root_workflow_run_id'] = '01JBREDRIVESOURCERUN000000';

            return $fixture;
        }, 'root at the successor'];
        $cases['cancellation lineage on the source run'] = [self::CANCELLATION, static function (array $fixture): array {
            $fixture['history'][6]['payload']['cancellation']['lineage'][0]['workflow_run_id'] = '01JBREDRIVESOURCERUN000000';

            return $fixture;
        }, 'lineage must name the successor'];
        $cases['successor event carries a reuse key'] = [self::CANCELLATION, $set(4, 'reused_from_run_id', '01JBREDRIVESOURCERUN000000'), 'outside the reused prefix'];

        // #250: each case below was accepted before the request/delivery pair
        // was checked against Core 28893f97 (CancellationContext::fromArray,
        // WorkflowStub's request and CooperativeCancellationDelivery).
        $context = static fn (callable $change): callable => static function (array $fixture) use ($change): array {
            foreach ([5, 6] as $index) {
                $fixture['history'][$index]['payload']['cancellation'] = $change($fixture['history'][$index]['payload']['cancellation']);
            }

            return $fixture;
        };
        $insert = static fn (int $at, array $event): callable => static function (array $fixture) use ($at, $event): array {
            array_splice($fixture['history'], $at, 0, [$event]);
            foreach ($fixture['history'] as $index => $_) {
                $fixture['history'][$index]['sequence'] = $index + 1;
            }

            return $fixture;
        };
        $instance = 'regression-corpus-redrive-successor-cancellation';
        $successor = ['request_id' => 'successor-request', 'workflow_instance_id' => $instance, 'workflow_run_id' => 'regression-corpus-run'];
        $cases['root request with a parent'] = [self::CANCELLATION, $context(static fn (array $c): array => [...$c, 'parent_request_id' => 'parent-request']), 'parent_request_id'];
        $cases['root_request_id names another request'] = [self::CANCELLATION, $context(static fn (array $c): array => [...$c, 'root_request_id' => 'other-request']), 'start at the root request'];
        $cases['lineage repeats the successor run'] = [self::CANCELLATION, $context(static fn (array $c): array => [
            ...$c, 'root_request_id' => 'earlier-request', 'parent_request_id' => 'earlier-request',
            'lineage' => [[...$successor, 'request_id' => 'earlier-request'], $successor],
        ]), 'must not repeat a request or run'];
        $cases['lineage ends at another request'] = [self::CANCELLATION, $context(static fn (array $c): array => [
            ...$c, 'lineage' => [[...$successor, 'request_id' => 'other-request']], 'root_request_id' => 'other-request',
        ]), 'end at this request'];
        $cases['context names another request'] = [self::CANCELLATION, $context(static fn (array $c): array => [...$c, 'request_id' => 'other-request']), 'request_id must be the request workflow_command_id'];
        $cases['context schema v2 without a scope'] = [self::CANCELLATION, $context(static fn (array $c): array => [...$c, 'schema' => 'durable-workflow.cancellation-context/v2']), 'schema'];
        $cases['context requester is a list'] = [self::CANCELLATION, $context(static fn (array $c): array => [...$c, 'requester' => ['operator']]), 'requester'];
        $cases['context deadline before its request'] = [self::CANCELLATION, $context(static fn (array $c): array => [...$c, 'cleanup_deadline_at' => '2026-11-03T00:05:00.000000Z']), 'cleanup_deadline_at must be a valid time after requested_at'];
        $cases['context requested after the request record'] = [self::CANCELLATION, $context(static fn (array $c): array => [...$c, 'requested_at' => '2026-11-03T00:05:01.500000Z']), 'requested_at'];
        $cases['request without command_type'] = [self::CANCELLATION, static function (array $fixture): array {
            unset($fixture['history'][5]['payload']['command_type']);

            return $fixture;
        }, 'command_type'];
        foreach (['workflow_instance_id', 'workflow_run_id'] as $field) {
            $cases["request without {$field}"] = [self::CANCELLATION, static function (array $fixture) use ($field): array {
                unset($fixture['history'][5]['payload'][$field]);

                return $fixture;
            }, 'successor workflow_instance_id and workflow_run_id'];
        }
        $cases['request with another cleanup deadline'] = [self::CANCELLATION, $set(5, 'cleanup_deadline_at', '2026-11-03T00:05:30.000000Z'), "cleanup_deadline_at must be its context's"];
        $cases['delivery without the context'] = [self::CANCELLATION, static function (array $fixture): array {
            unset($fixture['history'][6]['payload']['cancellation']);

            return $fixture;
        }, "delivery must carry its request's cancellation context"];
        $cases['delivery with another context'] = [self::CANCELLATION, static function (array $fixture): array {
            $fixture['history'][6]['payload']['cancellation']['reason'] = 'changed';

            return $fixture;
        }, "delivery must carry its request's cancellation context"];
        $cases['delivery names another request'] = [self::CANCELLATION, $set(6, 'workflow_command_id', 'other-request'), 'name its request'];
        $cases['delivery call_kind mismatches the recorded call'] = [self::CANCELLATION, $set(6, 'call_kind', 'timer'), 'call_kind must match'];
        $cases['delivery call_kind is a group kind'] = [self::CANCELLATION, $set(6, 'call_kind', 'parallel'), 'single-call kind'];
        $cases['delivery beyond the next step'] = [self::CANCELLATION, $set(6, 'sequence', 5), 'after the next step'];
        $cases['delivery to a call resolved before the request'] = [self::CANCELLATION, $insert(5, [
            'event_type' => 'ActivityCompleted', 'recorded_at' => '2026-11-03T00:05:00.750000+00:00',
            'payload' => ['activity_execution_id' => '01JBREDRIVESUCCESSORACT001', 'sequence' => 3, 'payload_codec' => 'avro'],
        ]), 'resolved before the request'];
        $cases['a call scheduled after its delivery to the unscheduled step'] = [self::CANCELLATION, static function (array $fixture): array {
            $fixture['history'][6]['payload']['sequence'] = 4;
            $fixture['history'][] = ['event_type' => 'ActivityScheduled', 'sequence' => 8, 'recorded_at' => '2026-11-03T00:05:03+00:00',
                'payload' => ['activity_execution_id' => '01JBREDRIVESUCCESSORACT002', 'activity_type' => 'late', 'sequence' => 4]];

            return $fixture;
        }, 'must not be recorded after a cancellation delivered before it was scheduled'];
        $cases['two requests'] = [self::CANCELLATION, static function (array $fixture): array {
            $request = $fixture['history'][5];
            $fixture['history'][] = [...$request, 'sequence' => 8, 'recorded_at' => '2026-11-03T00:05:03+00:00'];

            return $fixture;
        }, 'exactly one request and one delivery'];
        $cases['delivery before its request'] = [self::CANCELLATION, static function (array $fixture): array {
            [$fixture['history'][5], $fixture['history'][6]] = [$fixture['history'][6], $fixture['history'][5]];
            $fixture['history'][5]['sequence'] = 6;
            $fixture['history'][6]['sequence'] = 7;
            $fixture['history'][5]['recorded_at'] = $fixture['history'][6]['recorded_at'];

            return $fixture;
        }, 'follow its request'];
        foreach (['timer_kind' => 'condition_timeout', 'cancellation_scope_id' => 'scope-1', 'condition_wait_id' => 'wait-1'] as $key => $value) {
            $cases["copied TimerScheduled with {$key}"] = [$timer, $set(2, $key, $value), "plain timer, without {$key}"];
        }

        return $cases;
    }

    public function testADeliveryToTheNextUnscheduledStepIsValid(): void
    {
        $fixture = RedriveFixtureRegistry::fixture(self::CANCELLATION);
        $fixture['history'][6]['payload']['sequence'] = 4;

        self::assertNull(RedriveSuccessorHistory::violation($fixture, RedriveFixtureRegistry::SUCCESSORS[self::CANCELLATION]));
    }

    /** @return array<string, array{string, list<string>, string}> */
    public static function wrongPrefixes(): array
    {
        return [
            'copied schedule left out of the prefix' => ['redrive-successor-reused-timer', ['ActivityCompleted', 'TimerFired'], 'must be TimerFired, not TimerScheduled'],
            'copied schedule without its fired timer' => ['redrive-successor-reused-timer', ['ActivityCompleted', 'TimerScheduled'], 'same timer_id and step'],
            'fired timer without its copied schedule' => ['redrive-successor-reused-timer', ['ActivityCompleted', 'TimerFired', 'TimerScheduled'], 'must be TimerFired, not TimerScheduled'],
            'markers without the reused activity' => ['redrive-successor-reused-markers', ['SideEffectRecorded', 'VersionMarkerRecorded'], 'outside the reused prefix'],
        ];
    }

    /**
     * What a clock that read every event up to the delivery, preferring
     * reused_recorded_at as CancellationReplayClock does, would report.
     *
     * @param array<string, mixed> $fixture
     */
    private static function leakingRemaining(array $fixture): float
    {
        $clock = new CancellationReplayClock();
        $deadline = null;
        foreach ($fixture['history'] as $event) {
            $clock->observe($event);
            if ($event['event_type'] === 'CooperativeCancellationRequested') {
                $deadline = new DateTimeImmutable($event['payload']['cleanup_deadline_at']);
            }
            if ($event['event_type'] === 'CooperativeCancellationDelivered') {
                break;
            }
        }
        self::assertNotNull($deadline);

        return (float) $deadline->format('U.u') - (float) $clock->time()->format('U.u');
    }

    /** @param list<array<string, mixed>> $commands */
    private static function assertSecondActivityOnly(array $commands): void
    {
        self::assertCount(1, $commands);
        self::assertSame('schedule_activity', $commands[0]['type']);
        self::assertSame(self::SECOND_ACTIVITY, $commands[0]['activity_type']);
        self::assertSame(['Hello, Taylor!'], $commands[0]['arguments']);
    }

    /**
     * @param array<string, mixed> $fixture
     * @return list<array<string, mixed>>
     */
    private static function onlyCommands(array $fixture): array
    {
        $tasks = ReplayRegressionFixture::execute($fixture);
        self::assertCount(1, $tasks);

        return $tasks[0]['command_sequence'];
    }

    /** @param array<string, mixed> $fixture */
    private static function carriesReuse(array $fixture): bool
    {
        foreach ($fixture['history'] ?? [] as $event) {
            $payload = is_array($event['payload'] ?? null) ? $event['payload'] : [];
            if (array_key_exists('reused_from_run_id', $payload) || array_key_exists('reused_recorded_at', $payload)) {
                return true;
            }
        }

        return false;
    }

    /** @param array<string, mixed> $fixture */
    private static function opensRedrive(array $fixture): bool
    {
        foreach ($fixture['history'] ?? [] as $event) {
            if (($event['event_type'] ?? null) === 'WorkflowStarted' && ($event['payload']['recovery_kind'] ?? null) === 'redrive') {
                return true;
            }
        }

        return false;
    }
}
