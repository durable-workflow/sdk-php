<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowClaimAborted;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * #269: the opt-in occurrence deadline. A capable Server keeps the first
 * physical open's deadline across false wakes of one authored call. The SDK
 * opts a new chain in only when the Server advertises
 * condition_wait_occurrence_deadline, sends the original authored budget on
 * every physical open of that chain and keeps timeout_seconds as the remaining
 * replay input. A recorded chain keeps its first open's policy.
 */
final class ConditionWaitOccurrenceDeadlineTest extends TestCase
{
    private const OPENED_AT = '2026-11-04T02:36:05.295000+00:00';

    public function testANewChainOptsInOnlyWhenTheServerAdvertisesTheCapability(): void
    {
        $capable = $this->replay([], capable: true);
        self::assertSame([
            'type' => 'open_condition_wait',
            'condition_key' => 'approval',
            'condition_definition_fingerprint' => $capable[0]['condition_definition_fingerprint'],
            'condition_wait_occurrence_id' => 'php:condition-wait:0',
            'timeout_seconds' => 300,
            'deadline_policy' => 'occurrence',
            'condition_wait_timeout_budget_seconds' => 300,
        ], $capable[0]);

        $legacy = $this->replay([], capable: false);
        self::assertArrayNotHasKey('deadline_policy', $legacy[0]);
        self::assertArrayNotHasKey('condition_wait_timeout_budget_seconds', $legacy[0]);
        self::assertSame(300, $legacy[0]['timeout_seconds']);
    }

    /**
     * A false wake on an anchored chain reopens with the remaining time as
     * timeout_seconds and the unchanged original budget: the recorded first
     * open decides the policy.
     */
    public function testAnAnchoredReopenSendsTheRemainingTimeAndTheOriginalBudget(): void
    {
        $commands = $this->replay([self::opened(300, anchored: true), self::wrong('2026-11-04T02:38:05.345000+00:00')], capable: true);

        self::assertCount(1, $commands);
        self::assertSame(['open_condition_wait', 180, 'occurrence', 300, 'php:condition-wait:0'], [
            $commands[0]['type'], $commands[0]['timeout_seconds'], $commands[0]['deadline_policy'],
            $commands[0]['condition_wait_timeout_budget_seconds'], $commands[0]['condition_wait_occurrence_id'],
        ]);
    }

    /**
     * Causal (#269 rollback): a Server that does not advertise the capability
     * strips the policy and records a legacy reopen inside the anchored chain.
     * An anchored chain that needs a new physical open therefore leaves the
     * claim before any command exists, with the explicit unavailable reason.
     * The predecessor sent the policy-bearing reopen here.
     *
     * @param list<array{string, string, array<string, mixed>}> $events
     */
    #[DataProvider('anchoredReopensNeedingANewOpen')]
    public function testAnAnchoredReopenLeavesTheClaimWhenTheCapabilityIsUnavailable(array $events): void
    {
        try {
            $this->replay($events, capable: false);
            self::fail('The anchored reopen was authored without the capability.');
        } catch (WorkflowClaimAborted $error) {
            self::assertStringStartsWith('condition_wait_occurrence_deadline_unavailable: ', $error->getMessage());
        }
    }

    /** @return iterable<string, array{list<array{string, string, array<string, mixed>}>}> */
    public static function anchoredReopensNeedingANewOpen(): iterable
    {
        yield 'first false wake' => [[self::opened(300, anchored: true), self::wrong('2026-11-04T02:38:05.345000+00:00')]];
        yield 'false wake after the deadline' => [[self::opened(300, anchored: true), self::wrong('2026-11-04T02:41:06.000000+00:00')]];
        yield 'second false wake after a recorded anchored reopen' => [[
            self::opened(300, anchored: true), self::wrong('2026-11-04T02:38:05.345000+00:00'),
            self::satisfied(), self::reopened(180, anchored: true), self::wrong('2026-11-04T02:40:05.000000+00:00'),
        ]];
    }

    /** A legacy chain reopens with exactly the same command whether or not the capability is advertised. */
    public function testALegacyChainReopensUnchangedWithoutTheCapability(): void
    {
        $events = [self::opened(300, anchored: false), self::wrong('2026-11-04T02:38:05.345000+00:00')];
        $withdrawn = $this->replay($events, capable: false);

        self::assertSame(180, $withdrawn[0]['timeout_seconds']);
        self::assertArrayNotHasKey('deadline_policy', $withdrawn[0]);
        self::assertSame($this->replay($events, capable: true), $withdrawn);
    }

    /**
     * Anchored replays that need no new physical open do not depend on the
     * current capability: a satisfied wait completes, and a recorded anchored
     * reopen that is still open authors nothing.
     *
     * @param list<array{string, string, array<string, mixed>}> $events
     */
    #[DataProvider('anchoredReplaysWithoutANewOpen')]
    public function testAnAnchoredReplayThatNeedsNoNewOpenIgnoresTheCapability(array $events, ?string $command): void
    {
        $commands = $this->replay($events, capable: false);

        self::assertSame($command, $commands[0]['type'] ?? null);
        self::assertSame($commands, $this->replay($events, capable: true));
    }

    /** @return iterable<string, array{list<array{string, string, array<string, mixed>}>, ?string}> */
    public static function anchoredReplaysWithoutANewOpen(): iterable
    {
        yield 'satisfied by the awaited signal' => [[
            self::opened(300, anchored: true),
            ['SignalReceived', '2026-11-04T02:37:00.000000+00:00', ['workflow_sequence' => 4, 'signal_name' => 'approve', 'arguments' => ['yes']]],
            ['ConditionWaitSatisfied', '2026-11-04T02:37:00.100000+00:00', ['sequence' => 4, 'condition_wait_id' => 'wait-1', 'condition_key' => 'approval']],
        ], 'complete_workflow'];
        yield 'recorded anchored reopen still open' => [[
            self::opened(300, anchored: true), self::wrong('2026-11-04T02:38:05.345000+00:00'),
            self::satisfied(), self::reopened(180, anchored: true),
        ], null];
    }

    /** Every later false wake of the chain sends the same original budget; only the remaining time shrinks. */
    public function testRepeatedFalseWakesKeepTheSameOriginalBudget(): void
    {
        $commands = $this->replay([
            self::opened(300, anchored: true), self::wrong('2026-11-04T02:38:05.345000+00:00'),
            self::satisfied(), self::reopened(180, anchored: true), self::wrong('2026-11-04T02:40:05.000000+00:00'),
        ], capable: true);

        self::assertSame([61, 'occurrence', 300], [
            $commands[0]['timeout_seconds'], $commands[0]['deadline_policy'], $commands[0]['condition_wait_timeout_budget_seconds'],
        ]);
    }

    /** Past the anchored deadline the reopen asks for zero remaining time; the Server's timer is then due at once. */
    public function testAFalseWakeAfterTheAnchoredDeadlineReopensWithZeroRemaining(): void
    {
        $commands = $this->replay([self::opened(300, anchored: true), self::wrong('2026-11-04T02:41:06.000000+00:00')], capable: true);

        self::assertSame([0, 'occurrence', 300], [
            $commands[0]['timeout_seconds'], $commands[0]['deadline_policy'], $commands[0]['condition_wait_timeout_budget_seconds'],
        ]);
    }

    /** A recorded zero-remaining anchored reopen (the deadline had passed) is valid and keeps the chain's budget. */
    public function testARecordedZeroRemainingAnchoredReopenIsAccepted(): void
    {
        $commands = $this->replay([
            self::opened(300, anchored: true), self::wrong('2026-11-04T02:38:05.345000+00:00'),
            self::satisfied(), self::reopened(0, anchored: true), self::wrong('2026-11-04T02:40:05.000000+00:00'),
        ], capable: true);

        self::assertSame([61, 'occurrence', 300], [
            $commands[0]['timeout_seconds'], $commands[0]['deadline_policy'], $commands[0]['condition_wait_timeout_budget_seconds'],
        ]);
    }

    /** A chain that began without the policy never gains it, even after the Server starts advertising it. */
    public function testALegacyChainStaysLegacyWhenTheCapabilityAppears(): void
    {
        $commands = $this->replay([self::opened(300, anchored: false), self::wrong('2026-11-04T02:38:05.345000+00:00')], capable: true);

        self::assertSame(180, $commands[0]['timeout_seconds']);
        self::assertArrayNotHasKey('deadline_policy', $commands[0]);
        self::assertArrayNotHasKey('condition_wait_timeout_budget_seconds', $commands[0]);
    }

    /** A new authored call is a new chain with its own occurrence and its own full budget (C4/C5 shape). */
    public function testANewAuthoredCallStartsAFreshChainWithItsOwnBudget(): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static function (WorkflowContext $context): bool {
            $context->waitCondition(static fn (): bool => $context->signals('first') !== [], key: 'first', timeout: 300);

            return $context->waitCondition(static fn (): bool => $context->signals('second') !== [], key: 'second', timeout: 60);
        };
        $history = self::history($codec, [
            self::opened(300, anchored: true, key: 'first'),
            ['SignalReceived', '2026-11-04T02:37:00.000000+00:00', ['workflow_sequence' => 4, 'signal_name' => 'first', 'arguments' => ['done']]],
            ['ConditionWaitSatisfied', '2026-11-04T02:37:00.100000+00:00', ['sequence' => 4, 'condition_wait_id' => 'wait-1', 'condition_key' => 'first']],
        ]);
        $replayer = new Replayer($codec);
        $replayer->useConditionOccurrenceDeadline(true);
        $commands = $replayer->replay($workflow, $history, [], 'php-workers')->commands;

        self::assertSame(['second', 'php:condition-wait:1', 60, 'occurrence', 60], [
            $commands[0]['condition_key'], $commands[0]['condition_wait_occurrence_id'], $commands[0]['timeout_seconds'],
            $commands[0]['deadline_policy'], $commands[0]['condition_wait_timeout_budget_seconds'],
        ]);
    }

    /** Grouped condition waits keep their authored timeout without the policy until grouped reopens are qualified (#278). */
    public function testGroupedWaitsDoNotOptIn(): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static function (WorkflowContext $context): array {
            return $context->all([
                $context->deferCondition(static fn (): bool => false, key: 'a', timeout: 30),
                $context->deferCondition(static fn (): bool => false, key: 'b', timeout: 40),
            ]);
        };
        $replayer = new Replayer($codec);
        $replayer->useConditionOccurrenceDeadline(true);
        $commands = $replayer->replay($workflow, [], [], 'php-workers')->commands;

        self::assertCount(2, $commands);
        foreach ($commands as $command) {
            self::assertSame('open_condition_wait', $command['type']);
            self::assertArrayHasKey('parallel_group_path', $command);
            self::assertArrayNotHasKey('deadline_policy', $command);
        }
    }

    /**
     * Recorded policy metadata is replay input and is checked strictly.
     *
     * @param list<array{string, string, array<string, mixed>}> $events
     */
    #[DataProvider('invalidRecordedPolicies')]
    public function testInvalidOrInconsistentRecordedPoliciesRefuseReplay(array $events, string $reason): void
    {
        try {
            $this->replay($events, capable: true);
            self::fail('The recorded policy was accepted.');
        } catch (NonDeterministicWorkflow $error) {
            self::assertSame($reason, $error->reason);
        }
    }

    /** @return iterable<string, array{list<array{string, string, array<string, mixed>}>, string}> */
    public static function invalidRecordedPolicies(): iterable
    {
        $wrong = self::wrong('2026-11-04T02:38:05.345000+00:00');
        yield 'unknown policy' => [[self::opened(300, anchored: true, policy: 'first_open'), $wrong], 'condition_wait_deadline_policy_invalid'];
        yield 'policy without a budget' => [[self::opened(300, anchored: true, budget: false), $wrong], 'condition_wait_deadline_policy_invalid'];
        yield 'budget without a policy' => [[self::opened(300, anchored: true, policy: false), $wrong], 'condition_wait_deadline_policy_invalid'];
        yield 'remaining larger than the budget' => [[self::opened(300, anchored: true, budgetSeconds: 200), $wrong], 'condition_wait_deadline_policy_invalid'];
        yield 'string budget' => [[self::opened(300, anchored: true, budgetSeconds: '300'), $wrong], 'condition_wait_deadline_policy_invalid'];
        yield 'first open budget other than its timeout' => [[self::opened(120, anchored: true, budgetSeconds: 300), $wrong], 'condition_wait_occurrence_mismatch'];
        yield 'anchored chain, legacy reopen' => [[self::opened(300, anchored: true), $wrong, self::satisfied(), self::reopened(180, anchored: false),
            self::wrong('2026-11-04T02:40:05.000000+00:00')], 'condition_wait_occurrence_mismatch'];
        yield 'legacy chain, anchored reopen' => [[self::opened(300, anchored: false), $wrong, self::satisfied(), self::reopened(180, anchored: true),
            self::wrong('2026-11-04T02:40:05.000000+00:00')], 'condition_wait_occurrence_mismatch'];
        // Causal (#364 review): a negative recorded remaining timeout on an anchored physical
        // reopen is refused before the chain binding replaces it with the first open's timeout.
        yield 'negative remaining on an anchored reopen' => [[self::opened(300, anchored: true), $wrong, self::satisfied(), self::reopened(-1, anchored: true),
            self::wrong('2026-11-04T02:40:05.000000+00:00')], 'condition_wait_deadline_policy_invalid'];
        yield 'negative remaining on an anchored first open' => [[self::opened(-1, anchored: true, budgetSeconds: 300), $wrong], 'condition_wait_deadline_policy_invalid'];
        yield 'reopen with another budget' => [[self::opened(300, anchored: true), $wrong, self::satisfied(), self::reopened(180, anchored: true, budgetSeconds: 240),
            self::wrong('2026-11-04T02:40:05.000000+00:00')], 'condition_wait_occurrence_mismatch'];
    }

    /** The Worker hands the advertised capability to its replayer on each workflow poll, and clears it when absent. */
    public function testTheWorkerFollowsTheAdvertisedCapability(): void
    {
        $worker = new Worker(new Client('http://127.0.0.1:9', workerToken: 'unused'), 'q', workerId: 'w');
        $remember = new \ReflectionMethod($worker, 'rememberWorkflowPollCapabilities');
        $replayer = (new \ReflectionProperty($worker, 'replayer'))->getValue($worker);
        $flag = new \ReflectionProperty($replayer, 'conditionOccurrenceDeadline');

        $remember->invoke($worker, ['server_capabilities' => ['condition_wait_occurrence_deadline' => true]]);
        self::assertTrue($flag->getValue($replayer));
        $remember->invoke($worker, ['server_capabilities' => ['condition_wait_occurrence_deadline' => 'yes']]);
        self::assertFalse($flag->getValue($replayer));
        $remember->invoke($worker, ['server_capabilities' => ['condition_wait_occurrence_deadline' => true]]);
        $remember->invoke($worker, ['server_capabilities' => []]);
        self::assertFalse($flag->getValue($replayer));
    }

    /**
     * Worker level, through the real Client transport path: each workflow poll
     * decides the capability for its own task, so an earlier advertised value
     * cannot hide a later absent or false one. Without it the anchored reopen
     * is never completed: the claim is failed with the unavailable reason, no
     * completion is sent and so no history is written for that task.
     */
    public function testTheWorkerNeverCompletesAnAnchoredReopenAfterTheCapabilityDisappears(): void
    {
        $codec = new AvroPayloadCodec();
        $history = self::history($codec, [self::opened(300, anchored: true), self::wrong('2026-11-04T02:38:05.345000+00:00')]);
        $polls = [
            ['task-1', ['condition_wait_occurrence_deadline' => true]],
            ['task-2', null],
            ['task-3', ['condition_wait_occurrence_deadline' => false]],
        ];
        $completed = [];
        $failed = [];
        $transport = new FakeTransport(handler: static function (
            string $method, string $uri, array $headers, ?array $body,
        ) use (&$polls, &$completed, &$failed, $history): array {
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                [$taskId, $capabilities] = array_shift($polls) ?? [null, null];
                if ($taskId === null) {
                    return ['task' => null, 'poll_status' => 'empty'];
                }
                $response = ['poll_status' => 'leased', 'task' => ['task_id' => $taskId, 'workflow_task_attempt' => 1,
                    'lease_owner' => 'worker-1', 'workflow_id' => 'approval', 'run_id' => 'run-1',
                    'workflow_type' => 'approval.workflow', 'payload_codec' => 'avro', 'history_events' => $history]];
                if ($capabilities !== null) {
                    $response['server_capabilities'] = $capabilities;
                }

                return $response;
            }
            if (preg_match('#/workflow-tasks/(task-\d)/heartbeat$#', $uri, $match) === 1) {
                return ['task_id' => $match[1], 'workflow_task_attempt' => 1, 'lease_owner' => 'worker-1', 'renewed' => true];
            }
            if (preg_match('#/workflow-tasks/(task-\d)/complete$#', $uri, $match) === 1) {
                $completed[$match[1]] = $body['commands'];

                return ['completed' => true];
            }
            if (preg_match('#/workflow-tasks/(task-\d)/fail$#', $uri, $match) === 1) {
                $failed[$match[1]] = [$body['failure']['type'] ?? null, $body['failure']['message'] ?? null];

                return ['recorded' => true];
            }
            if (str_ends_with($uri, '/activity-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }
            self::fail("Unexpected request {$method} {$uri}");
        });
        $worker = new Worker(new Client('https://runtime.test', transport: $transport), 'approvals', workerId: 'worker-1');
        $worker->registerWorkflow('approval.workflow', static fn (WorkflowContext $context): bool => $context->waitCondition(
            static fn (): bool => $context->signals('approve') !== [], key: 'approval', timeout: '5 minutes'));

        self::assertTrue($worker->tick(0));
        self::assertTrue($worker->tick(0));
        self::assertTrue($worker->tick(0));

        self::assertSame(['task-1'], array_keys($completed), 'only the capable task completes');
        self::assertSame(['open_condition_wait', 180, 'occurrence', 300], [$completed['task-1'][0]['type'],
            $completed['task-1'][0]['timeout_seconds'], $completed['task-1'][0]['deadline_policy'],
            $completed['task-1'][0]['condition_wait_timeout_budget_seconds']]);
        self::assertSame(['task-2', 'task-3'], array_keys($failed));
        foreach ($failed as [$type, $message]) {
            self::assertSame(WorkflowClaimAborted::class, $type);
            self::assertStringStartsWith('PHP workflow task execution failed: condition_wait_occurrence_deadline_unavailable: ', (string) $message);
        }
    }

    /**
     * @param list<array{string, string, array<string, mixed>}> $events
     * @return list<array<string, mixed>>
     */
    private function replay(array $events, bool $capable): array
    {
        $codec = new AvroPayloadCodec();
        $workflow = static function (WorkflowContext $context): bool {
            return $context->waitCondition(
                static fn (): bool => $context->signals('approve') !== [],
                key: 'approval',
                timeout: '5 minutes',
            );
        };
        $replayer = new Replayer($codec);
        $replayer->useConditionOccurrenceDeadline($capable);

        return $replayer->replay($workflow, self::history($codec, $events), [], 'php-workers')->commands;
    }

    /**
     * @param list<array{string, string, array<string, mixed>}> $events
     * @return list<array<string, mixed>>
     */
    private static function history(AvroPayloadCodec $codec, array $events): array
    {
        return array_map(static fn (array $event): array => [
            'event_type' => $event[0],
            'recorded_at' => $event[1],
            'payload' => isset($event[2]['arguments']) ? [...$event[2], 'arguments' => $codec->envelope($event[2]['arguments'])] : $event[2],
        ], $events);
    }

    /** @return array{string, string, array<string, mixed>} */
    private static function opened(int $timeout, bool $anchored, string $key = 'approval', string|false $policy = 'occurrence',
        int|string|false|null $budgetSeconds = null, bool $budget = true): array
    {
        $payload = ['sequence' => 4, 'condition_wait_id' => 'wait-1', 'condition_key' => $key,
            'condition_wait_occurrence_id' => 'php:condition-wait:0', 'timeout_seconds' => $timeout];
        if ($anchored) {
            if ($policy !== false) {
                $payload['deadline_policy'] = $policy;
            }
            if ($budget) {
                $payload['condition_wait_timeout_budget_seconds'] = $budgetSeconds ?? $timeout;
            }
        }

        return ['ConditionWaitOpened', self::OPENED_AT, $payload];
    }

    /** @return array{string, string, array<string, mixed>} */
    private static function reopened(int $timeout, bool $anchored, int $budgetSeconds = 300): array
    {
        $payload = ['sequence' => 5, 'condition_wait_id' => 'wait-2', 'condition_key' => 'approval',
            'condition_wait_occurrence_id' => 'php:condition-wait:0', 'timeout_seconds' => $timeout];
        if ($anchored) {
            $payload += ['deadline_policy' => 'occurrence', 'condition_wait_timeout_budget_seconds' => $budgetSeconds,
                'occurrence_deadline_at' => '2026-11-04T02:41:05.295000+00:00'];
        }

        return ['ConditionWaitOpened', '2026-11-04T02:38:05.402000+00:00', $payload];
    }

    /** @return array{string, string, array<string, mixed>} */
    private static function satisfied(): array
    {
        return ['ConditionWaitSatisfied', '2026-11-04T02:38:05.397000+00:00', [
            'sequence' => 4, 'condition_wait_id' => 'wait-1', 'condition_key' => 'approval',
        ]];
    }

    /** @return array{string, string, array<string, mixed>} */
    private static function wrong(string $at): array
    {
        return ['SignalReceived', $at, ['workflow_sequence' => 4, 'signal_name' => 'reject', 'arguments' => ['another-proposal']]];
    }
}
