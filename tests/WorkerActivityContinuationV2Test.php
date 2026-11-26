<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DateTimeImmutable;
use DateTimeZone;
use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\StickyWorkflowCache;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * Server #1181: activity workflow continuation v2. The Worker seeds its sticky cache from the
 * tail a workflow task completion returns, speculates the next workflow task from that cache
 * plus its own activity result, and keeps the cache exact from every accepted answer's tail.
 */
final class WorkerActivityContinuationV2Test extends TestCase
{
    private const WORKFLOW_ID = 'order-17';
    private const RUN_ID = 'run-1';
    private const BUILD_ID = 'build-9';
    private const COMMANDS = ['schedule_activity', 'start_timer', 'complete_workflow', 'record_version_marker'];

    /** @var list<array{0: string, 1: array<string, mixed>}> */
    private array $diagnostics = [];

    public function testAWorkflowTaskTailSeedsTheCacheAndAcceptedContinuationsChainWithoutHistoryReads(): void
    {
        $transport = $this->transport();
        $calls = [];
        $worker = $this->worker($transport, $calls);
        self::assertTrue($worker->tick(0));

        // The workflow task completion asks for the tail and carries an activity poll.
        $completed = self::requests($transport, '/workflow-tasks/wf-1/complete');
        self::assertCount(1, $completed);
        self::assertSame(['version' => 2], $completed[0]['body']['continuation_tail']);
        self::assertSame(1, $completed[0]['body']['sticky_cache']['history_last_sequence']);
        self::assertArrayHasKey('activity_poll', $completed[0]['body']);

        // Both activities ran in the same tick, each completed by one v2 request.
        self::assertSame(['activity', 'activity'], $calls);
        self::assertSame([], self::requests($transport, '/continuation/history'), 'v2 never asks for history.');
        self::assertSame([], self::requests($transport, '/act-1/complete'));
        self::assertSame([], self::requests($transport, '/act-2/complete'));

        $first = self::requests($transport, '/act-1/continuation/complete');
        self::assertCount(1, $first);
        $body = $first[0]['body'];
        self::assertSame(['version', 'history_through_sequence', 'history_last_event_id', 'sticky_cache', 'commands'],
            array_keys($body['continuation']));
        self::assertSame(2, $body['continuation']['version']);
        self::assertSame(3, $body['continuation']['history_through_sequence']);
        self::assertSame('event-3', $body['continuation']['history_last_event_id']);
        self::assertSame(['worker_id' => 'worker', 'workflow_id' => self::WORKFLOW_ID, 'run_id' => self::RUN_ID,
            'build_id' => self::BUILD_ID, 'history_last_sequence' => 3],
            array_intersect_key($body['continuation']['sticky_cache'],
                array_flip(['worker_id', 'workflow_id', 'run_id', 'build_id', 'history_last_sequence'])));
        self::assertSame(['schedule_activity'], array_column($body['continuation']['commands'], 'type'));
        self::assertSame(7, (new AvroPayloadCodec())->decodeEnvelope($body['result']));
        self::assertSame('worker', $body['workflow_poll']['worker_id']);
        self::assertSame('worker', $body['activity_poll']['worker_id']);

        $second = self::requests($transport, '/act-2/continuation/complete');
        self::assertCount(1, $second);
        $body = $second[0]['body'];
        self::assertSame(6, $body['continuation']['history_through_sequence'], 'Speculated from the first answer\'s tail.');
        self::assertSame('event-6', $body['continuation']['history_last_event_id']);
        self::assertSame(['complete_workflow'], array_column($body['continuation']['commands'], 'type'));
        self::assertSame(14, (new AvroPayloadCodec())->decodeEnvelope($body['continuation']['commands'][0]['result']));

        // The run completed, so its cache entry is gone.
        self::assertNull($this->cachedHistory($worker));
        self::assertSame(['accepted', 'accepted'], array_column($this->events('worker.activity_continuation'), 'outcome'));
        self::assertSame(['appended', 'appended'], array_column($this->events('worker.activity_continuation'), 'tail'));
    }

    public function testAChainThatReachedItsBoundCarriesNoActivityPollSoTheTickEnds(): void
    {
        $transport = $this->transport();
        $calls = [];
        $worker = $this->worker($transport, $calls);
        // Each lease an accepted answer carried counts toward the per-tick bound. At the bound the
        // next v2 request carries no activity poll, so ordinary polling takes that activity.
        $bound = (new \ReflectionClassConstant(Worker::class, 'CONTINUATION_V2_MAX_CHAINED_ACTIVITIES'))->getValue();
        self::assertSame(16, $bound);
        $reserve = new \ReflectionMethod($worker, 'reserveContinuationActivityPoll');
        (new \ReflectionProperty($worker, 'workflowCompletionActivityPollSupported'))->setValue($worker, true);
        self::assertNotNull($reserve->invoke($worker, 'worker'));
        (new \ReflectionProperty($worker, 'continuationChainLength'))->setValue($worker, $bound);
        self::assertNull($reserve->invoke($worker, 'worker'), 'At the bound the v2 request carries no activity poll.');
        self::assertNull($reserve->invoke($worker, 'someone-else'));
    }

    #[DataProvider('v2Unavailable')]
    public function testWithoutAUsableV2MemberTheWorkerKeepsV1AndAsksForNoTail(mixed $continuation): void
    {
        $transport = $this->transport(capability: $continuation);
        $calls = [];
        $worker = $this->worker($transport, $calls);
        self::assertTrue($worker->tick(0));
        self::assertArrayNotHasKey('continuation_tail', self::requests($transport, '/workflow-tasks/wf-1/complete')[0]['body']);
        // A malformed v2 member disables v2 only: v1 still asks for history.
        self::assertCount(1, self::requests($transport, '/act-1/continuation/history'));
        foreach (self::requests($transport, '/continuation/complete') as $request) {
            self::assertSame(1, $request['body']['continuation']['version']);
        }
    }

    public static function v2Unavailable(): iterable
    {
        $contract = self::capability();
        $without = static function (string $field) use ($contract): array {
            unset($contract['v2'][$field]);

            return $contract;
        };
        $with = static function (string $field, mixed $value) use ($contract): array {
            $contract['v2'][$field] = $value;

            return $contract;
        };
        yield 'v1 only' => [array_diff_key($contract, ['supported_versions' => true, 'v2' => true])];
        yield 'versions without 2' => [['supported_versions' => [1]] + $contract];
        yield 'v2 without supported_versions' => [array_diff_key($contract, ['supported_versions' => true])];
        yield 'v2 member is a list' => [['v2' => [2]] + $contract];
        yield 'string version' => [$with('version', '2')];
        yield 'float version' => [$with('version', 2.0)];
        yield 'other endpoint' => [$with('complete_endpoint', '/worker/activity-tasks/{taskId}/continuation/v2')];
        yield 'unknown worker requirement' => [$with('requires_worker_capabilities', ['sticky_execution', 'sticky_history_suffix', 'future'])];
        yield 'missing requirements' => [$without('requires_worker_capabilities')];
        yield 'no commands' => [$with('supported_commands', [])];
        yield 'bad max commands' => [$with('max_commands', 0)];
        yield 'missing tail budget' => [$without('tail_max_events')];
        yield 'bad byte budget' => [$with('tail_max_bytes', '262144')];
    }

    #[DataProvider('workerConditions')]
    public function testV2NeedsEveryWorkerCondition(bool $enabled, bool $suffixes, bool $interceptors, bool $v1): void
    {
        $transport = $this->transport();
        $calls = [];
        $worker = $this->worker($transport, $calls, enabled: $enabled, suffixes: $suffixes, interceptors: $interceptors);
        self::assertTrue($worker->tick(0));
        self::assertArrayNotHasKey('continuation_tail', self::requests($transport, '/workflow-tasks/wf-1/complete')[0]['body']);
        self::assertCount($v1 ? 1 : 0, self::requests($transport, '/continuation/history'));
        foreach (self::requests($transport, '/continuation/complete') as $request) {
            self::assertSame(1, $request['body']['continuation']['version']);
        }
    }

    public static function workerConditions(): iterable
    {
        yield 'option off (the default)' => [false, true, false, false];
        yield 'no sticky history suffixes' => [true, false, false, true];
        yield 'worker interceptors' => [true, true, true, false];
    }

    #[DataProvider('unusableTails')]
    public function testAWorkflowTaskTailThatCannotSeedTheCacheKeepsTheOrdinaryCompletion(\Closure $tail, bool $kept): void
    {
        $transport = $this->transport(workflowCompletion: static fn (array $answer): array => $tail($answer));
        $calls = [];
        $worker = $this->worker($transport, $calls);
        self::assertTrue($worker->tick(0));
        self::assertSame(['activity'], $calls);
        self::assertSame([], self::requests($transport, '/continuation/complete'));
        self::assertSame([], self::requests($transport, '/continuation/history'), 'v2 is negotiated, so v1 is not tried.');
        $ordinary = self::requests($transport, '/act-1/complete');
        self::assertCount(1, $ordinary);
        self::assertArrayHasKey('workflow_poll', $ordinary[0]['body']);
        $history = $this->cachedHistory($worker);
        if ($kept) {
            self::assertNotNull($history);
            self::assertSame(1, $history[array_key_last($history)]['sequence'], 'The cache keeps its old boundary.');
        } else {
            self::assertNull($history, 'A tail that does not continue the cache evicts it.');
        }
    }

    public static function unusableTails(): iterable
    {
        $omit = static fn (string $reason): \Closure => static function (array $answer) use ($reason): array {
            $answer['continuation_tail'] = ['version' => 2, 'after_sequence' => 1, 'omitted' => $reason];

            return $answer;
        };
        yield 'event budget' => [$omit('tail_event_budget_exceeded'), true];
        yield 'byte budget' => [$omit('tail_byte_budget_exceeded'), true];
        yield 'retried completion' => [$omit('tail_unavailable'), true];
        yield 'older Server (no tail)' => [static function (array $answer): array {
            unset($answer['continuation_tail']);

            return $answer;
        }, true];
        yield 'other boundary' => [static function (array $answer): array {
            $answer['continuation_tail']['after_sequence'] = 2;

            return $answer;
        }, false];
        yield 'gap' => [static function (array $answer): array {
            array_shift($answer['continuation_tail']['history_events']);

            return $answer;
        }, false];
        yield 'last sequence mismatch' => [static function (array $answer): array {
            $answer['continuation_tail']['last_sequence'] = 4;

            return $answer;
        }, false];
        yield 'other version' => [static function (array $answer): array {
            $answer['continuation_tail']['version'] = 3;

            return $answer;
        }, false];
        yield 'malformed events' => [static function (array $answer): array {
            $answer['continuation_tail']['history_events'] = 'events';

            return $answer;
        }, false];
    }

    public function testATailEndingInAnotherAttemptsActivityStartedIsCompletedTheOrdinaryWay(): void
    {
        // A replayed carried poll can hand back a lease this completion did not create.
        $transport = $this->transport(workflowCompletion: static function (array $answer): array {
            $answer['continuation_tail']['history_events'][1]['payload']['activity_attempt_id'] = 'attempt-other';

            return $answer;
        });
        $calls = [];
        $worker = $this->worker($transport, $calls);
        self::assertTrue($worker->tick(0));
        self::assertSame([], self::requests($transport, '/continuation/complete'));
        self::assertCount(1, self::requests($transport, '/act-1/complete'));
        self::assertSame(3, self::lastSequence($this->cachedHistory($worker)), 'The tail itself continued the cache.');
        self::assertContains('cache_tail_not_this_activity', array_column($this->events('worker.activity_continuation_skipped'), 'reason'));
    }

    public function testARefusedTailRequestIsSentAgainWithoutTheTailAndEvictsTheCache(): void
    {
        $refused = false;
        $transport = $this->transport(workflowCompletion: static function (array $answer, array $body) use (&$refused): array|TransportException {
            if (isset($body['continuation_tail'])) {
                $refused = true;

                return new TransportException('Boundary past history.', 422,
                    ['reason' => 'invalid_continuation_tail', 'message' => 'invalid_continuation_tail']);
            }
            unset($answer['continuation_tail']);

            return $answer;
        });
        $calls = [];
        $worker = $this->worker($transport, $calls);
        self::assertTrue($worker->tick(0));
        self::assertTrue($refused);
        $completed = self::requests($transport, '/workflow-tasks/wf-1/complete');
        self::assertCount(2, $completed);
        self::assertArrayNotHasKey('continuation_tail', $completed[1]['body']);
        self::assertSame($completed[0]['body']['commands'], $completed[1]['body']['commands']);
        self::assertNull($this->cachedHistory($worker));
        self::assertSame([], self::requests($transport, '/continuation/complete'));
    }

    #[DataProvider('declines')]
    public function testEachDeclineKeepsTheOrdinaryCompletionAndEvictsOnlyWhenTheContractSays(string $reason, bool $evicts): void
    {
        $transport = $this->transport(continuation: static fn (array $body): array => self::declined($reason,
            $reason === 'workflow_slot_unavailable' ? null : ['leased' => false]));
        $calls = [];
        $worker = $this->worker($transport, $calls);
        self::assertTrue($worker->tick(0));
        self::assertSame(['activity'], $calls, 'A decline never leases an activity.');
        self::assertCount(1, self::requests($transport, '/act-1/continuation/complete'));
        self::assertSame([], self::requests($transport, '/act-1/complete'), 'The result is recorded; it is never sent again.');
        self::assertSame($evicts ? null : 3, self::lastSequence($this->cachedHistory($worker)));
        $observed = $this->events('worker.activity_continuation');
        self::assertSame([['declined', $reason]], array_map(static fn (array $event): array => [$event['outcome'], $event['reason']], $observed));
    }

    public static function declines(): iterable
    {
        foreach (['external_payload', 'standalone_activity', 'cancellation_requested', 'parallel_group', 'pending_workflow_task',
            'pending_update', 'history_changed', 'workflow_slot_unavailable', 'no_workflow_task', 'dispatch_selected_other_task'] as $reason) {
            yield $reason => [$reason, false];
        }
        yield 'cache_identity_mismatch' => ['cache_identity_mismatch', true];
        yield 'run_not_open' => ['run_not_open', true];
    }

    public function testADeclinedContinuationsCarriedWorkflowTaskReplaysFromTheKeptCachePlusItsSuffix(): void
    {
        $transport = $this->transport(continuation: static fn (array $body): array
            => self::declined('history_changed', ['leased' => true, 'answer' => self::resumedWorkflowAnswer()]));
        $calls = [];
        $worker = $this->worker($transport, $calls);
        self::assertTrue($worker->tick(0));
        self::assertTrue($worker->tick(0));
        $resumed = self::requests($transport, '/workflow-tasks/wf-2/complete');
        self::assertCount(1, $resumed, 'The carried workflow task is finished the ordinary way.');
        self::assertSame(['schedule_activity'], array_column($resumed[0]['body']['commands'], 'type'));
        self::assertSame([], self::requests($transport, '/workflow-tasks/wf-2/history'), 'The kept cache plus the suffix is a sticky hit.');
    }

    #[DataProvider('refusals')]
    public function testEachRefusalSendsTheOrdinaryCompletionWithTheSameWorkflowPoll(int $status, string $reason, bool $disables, bool $evicts): void
    {
        $transport = $this->transport(continuation: static fn (array $body): TransportException
            => new TransportException($reason, $status, ['reason' => $reason, 'message' => $reason]));
        $calls = [];
        $worker = $this->worker($transport, $calls);
        self::assertTrue($worker->tick(0));
        $sent = self::requests($transport, '/act-1/continuation/complete');
        self::assertCount(1, $sent, 'A refusal is not retried.');
        $ordinary = self::requests($transport, '/act-1/complete');
        self::assertCount(1, $ordinary);
        self::assertSame($sent[0]['body']['workflow_poll'], $ordinary[0]['body']['workflow_poll']);
        self::assertSame($sent[0]['body']['result'], $ordinary[0]['body']['result']);
        self::assertArrayNotHasKey('continuation', $ordinary[0]['body']);
        self::assertSame($disables, self::property($worker, 'activityContinuationV2Refused'));
        self::assertSame($evicts ? null : 3, self::lastSequence($this->cachedHistory($worker)));
        $refusal = $this->events('worker.activity_continuation_refused');
        self::assertCount(1, $refusal);
        self::assertSame([$status, $reason], [$refusal[0]['status'], $refusal[0]['reason']]);
    }

    public static function refusals(): iterable
    {
        yield 'invalid_continuation' => [422, 'invalid_continuation', false, false];
        yield 'unsupported_continuation_version' => [422, 'unsupported_continuation_version', true, false];
        yield 'unsupported_continuation_command' => [422, 'unsupported_continuation_command', false, false];
        yield 'continuation_requires_workflow_poll' => [422, 'continuation_requires_workflow_poll', false, false];
        yield 'continuation_result_exceeds_budget' => [413, 'continuation_result_exceeds_budget', false, false];
        yield 'continuation_receipt_exceeds_budget' => [413, 'continuation_receipt_exceeds_budget', false, false];
        yield 'completion_budget_exceeded' => [409, 'completion_budget_exceeded', false, false];
        yield 'continuation_run_deadline_expired' => [409, 'continuation_run_deadline_expired', false, false];
        yield 'invalid_continuation_receipt' => [500, 'invalid_continuation_receipt', false, true];
        yield 'history_invariant_violated' => [500, 'history_invariant_violated', false, true];
    }

    public function testAServerWithoutV2KeepsV1AfterRefusingTheVersion(): void
    {
        $transport = $this->transport(continuation: static fn (array $body): TransportException => new TransportException(
            'unsupported_continuation_version', 422, ['reason' => 'unsupported_continuation_version']));
        $calls = [];
        $worker = $this->worker($transport, $calls);
        self::assertTrue($worker->tick(0));
        self::assertTrue(self::property($worker, 'activityContinuationV2Refused'));
        $method = new \ReflectionMethod($worker, 'activityContinuationV2Selected');
        self::assertFalse($method->invoke($worker), 'The next workflow task asks for no tail and v1 is the fallback.');
    }

    public function testAStaleAttemptIsHandledAsAnOrdinaryStaleCompletion(): void
    {
        $transport = $this->transport(continuation: static fn (array $body): TransportException => new TransportException(
            'stale', 409, ['reason' => 'stale_attempt', 'recorded' => false, 'task_id' => 'act-1',
                'activity_attempt_id' => 'attempt-1', 'lease_owner' => 'worker', 'outcome' => 'completed',
                'attempt_status' => 'expired', 'activity_status' => 'pending', 'task_status' => 'completed',
                'can_continue' => false]));
        $calls = [];
        $worker = $this->worker($transport, $calls);
        $worker->tick(0);
        self::assertCount(1, self::requests($transport, '/act-1/continuation/complete'));
        self::assertSame([], self::requests($transport, '/act-1/complete'));
        self::assertSame([], self::requests($transport, '/act-1/fail'));
    }

    public function testAConflictingCompletionIsNeverRetriedOrReplaced(): void
    {
        $transport = $this->transport(continuation: static fn (array $body): TransportException => new TransportException(
            'conflict', 409, ['reason' => 'conflicting_completion', 'recorded' => false, 'task_id' => 'act-1']));
        $calls = [];
        $worker = $this->worker($transport, $calls);
        try {
            $worker->tick(0);
        } catch (ServerException $error) {
            self::assertSame('conflicting_completion', $error->reason);
        }
        self::assertCount(1, self::requests($transport, '/act-1/continuation/complete'));
        self::assertSame([], self::requests($transport, '/act-1/complete'));
    }

    #[DataProvider('unknownOutcomes')]
    public function testAnUnknownOutcomeResendsTheIdenticalV2Request(\Closure $failure): void
    {
        $attempts = 0;
        $transport = $this->transport(continuation: function (array $body, string $taskId) use ($failure, &$attempts): array|\Throwable {
            if ($taskId === 'act-1' && ++$attempts === 1) {
                return $failure();
            }

            return $taskId === 'act-1' ? self::acceptedFirst() : self::acceptedLast();
        });
        $calls = [];
        $worker = $this->worker($transport, $calls);
        self::assertTrue($worker->tick(0));
        $sent = self::requests($transport, '/act-1/continuation/complete');
        self::assertCount(2, $sent);
        self::assertSame($sent[0]['body'], $sent[1]['body'], 'The resent request is identical.');
        self::assertSame([], self::requests($transport, '/act-1/complete'), 'An unknown outcome is never replaced by an ordinary completion.');
        self::assertSame(['activity', 'activity'], $calls, 'The activity callback is not repeated.');
    }

    public static function unknownOutcomes(): iterable
    {
        yield 'transport failure' => [static fn (): TransportException => new TransportException('reset', transientConnectionFailure: true)];
        yield 'bodiless 502' => [static fn (): TransportException => new TransportException('Bad gateway', 502)];
        yield 'generic 500' => [static fn (): TransportException => new TransportException('error', 500, ['reason' => 'server_error'])];
        yield 'typed 500' => [static fn (): TransportException => new TransportException('db', 500, ['reason' => 'database_error'])];
        yield 'typed 503' => [static fn (): TransportException => new TransportException('fault', 503, ['reason' => 'transition_fault'])];
    }

    public function testAnUnknownOutcomeThatOutlastsItsRetriesIsLeftToTheServer(): void
    {
        $transport = $this->transport(continuation: static fn (array $body): TransportException
            => new TransportException('db', 500, ['reason' => 'database_error']));
        $calls = [];
        $worker = $this->worker($transport, $calls);
        $worker->tick(0);
        self::assertGreaterThan(2, count(self::requests($transport, '/act-1/continuation/complete')));
        self::assertSame([], self::requests($transport, '/act-1/complete'));
        self::assertSame([], self::requests($transport, '/act-1/fail'), 'The computed result is not an activity failure.');
        self::assertCount(1, $this->events('worker.outcome_unpublished'));
    }

    public function testARetryAnsweredWithAnUnavailableTailCompletesTheLeasedActivityTheOrdinaryWay(): void
    {
        $attempts = 0;
        $transport = $this->transport(continuation: static function (array $body, string $taskId) use (&$attempts): array|\Throwable {
            if (++$attempts === 1) {
                return new TransportException('reset', transientConnectionFailure: true);
            }
            $answer = self::acceptedFirst();
            $answer['recorded'] = false;
            $answer['reason'] = 'already_completed';
            $answer['continuation_tail'] = ['version' => 2, 'after_sequence' => 3, 'omitted' => 'tail_unavailable'];

            return $answer;
        });
        $calls = [];
        $worker = $this->worker($transport, $calls);
        self::assertTrue($worker->tick(0));
        self::assertSame(['activity', 'activity'], $calls, 'The carried activity still runs.');
        self::assertCount(2, self::requests($transport, '/act-1/continuation/complete'));
        self::assertSame([], self::requests($transport, '/act-2/continuation/complete'), 'Nothing is speculated on it.');
        self::assertCount(1, self::requests($transport, '/act-2/complete'));
        self::assertSame(3, self::lastSequence($this->cachedHistory($worker)), 'The cache keeps its old boundary.');
    }

    #[DataProvider('acceptedTails')]
    public function testAnAcceptedAnswersTailMustContinueTheCacheBeforeTheLeasedActivityIsSpeculated(\Closure $edit, ?int $cached): void
    {
        $transport = $this->transport(continuation: static fn (array $body, string $taskId): array
            => $taskId === 'act-1' ? $edit(self::acceptedFirst()) : self::acceptedLast());
        $calls = [];
        $worker = $this->worker($transport, $calls);
        self::assertTrue($worker->tick(0));
        self::assertSame(['activity', 'activity'], $calls);
        self::assertSame([], self::requests($transport, '/act-2/continuation/complete'));
        self::assertCount(1, self::requests($transport, '/act-2/complete'));
        self::assertSame($cached, self::lastSequence($this->cachedHistory($worker)));
    }

    public static function acceptedTails(): iterable
    {
        yield 'omitted' => [static function (array $answer): array {
            $answer['continuation_tail'] = ['version' => 2, 'after_sequence' => 3, 'omitted' => 'tail_byte_budget_exceeded'];

            return $answer;
        }, 3];
        yield 'missing' => [static function (array $answer): array {
            unset($answer['continuation_tail']);

            return $answer;
        }, null];
        yield 'not continuing' => [static function (array $answer): array {
            $answer['continuation_tail']['after_sequence'] = 4;

            return $answer;
        }, null];
        yield 'another attempt started' => [static function (array $answer): array {
            $answer['continuation_tail']['history_events'][2]['payload']['activity_attempt_id'] = 'attempt-replayed';

            return $answer;
        }, 6];
    }

    public function testTheStickyCacheAppendsOnlyAnExactContinuation(): void
    {
        $cache = new StickyWorkflowCache(10, 60, static fn (): float => 0.0);
        $history = [self::event(1, 'WorkflowStarted', ['workflow_type' => 'test.workflow'])];
        self::assertFalse($cache->appendTail('w', 'r', 'b', 1, 2, [self::event(2, 'TimerScheduled', [])]), 'No entry, nothing to continue.');
        $cache->remember('w', 'r', 'b', $history);
        $cache->rememberContext('w', 'r', 'b', ['workflow_type' => 'test.workflow']);
        self::assertTrue($cache->appendTail('w', 'r', 'b', 1, 2, [self::event(2, 'TimerScheduled', [])]));
        self::assertSame([1, 2], array_column($cache->cachedHistory('w', 'r', 'b') ?? [], 'sequence'));
        $cache->remember('w', 'r', 'b', $cache->cachedHistory('w', 'r', 'b') ?? []);
        self::assertSame(['workflow_type' => 'test.workflow'], $cache->context('w', 'r', 'b'), 'Remembering history keeps the context.');
        self::assertFalse($cache->appendTail('w', 'r', 'b', 2, 4, [self::event(4, 'TimerFired', [])]), 'A gap evicts.');
        self::assertNull($cache->cachedHistory('w', 'r', 'b'));
        self::assertNull($cache->context('w', 'r', 'b'));
        self::assertSame(1, $cache->metrics()['eviction']);
    }

    /** @param list<string> $calls */
    private function worker(FakeTransport $transport, array &$calls, bool $enabled = true, bool $suffixes = true,
        bool $interceptors = false): Worker
    {
        // Retry backoff advances a test clock instead of sleeping.
        $now = microtime(true);
        $worker = new Worker(new Client('https://continuation.invalid', transport: $transport), 'queue', workerId: 'worker',
            buildId: self::BUILD_ID, clock: static function () use (&$now): float {
                return $now;
            }, sleeper: static function (int $microseconds) use (&$now): void {
                $now += $microseconds / 1_000_000;
            },
            diagnosticListener: function (string $event, array $context): void {
                $this->diagnostics[] = [$event, $context];
            },
            enableActivityContinuation: $enabled,
            interceptors: $interceptors ? [new \DurableWorkflow\Worker\Interceptor\PassthroughWorkerInterceptor()] : []);
        // Registration is what learns the Server's sticky_history_suffix capability.
        (new \ReflectionProperty($worker, 'stickyHistorySuffixSupported'))->setValue($worker, $suffixes);
        $worker->registerWorkflow('test.workflow', static fn (WorkflowContext $context): int
            => $context->activity('test.activity') + $context->activity('test.activity'));
        $worker->registerActivity('test.activity', static function (ActivityContext $context) use (&$calls): int {
            $calls[] = 'activity';

            return 7;
        });

        return $worker;
    }

    /**
     * A native Server for one run: the first workflow task schedules the first activity and, when
     * asked, answers its tail with the activity's ActivityStarted and carries that activity.
     *
     * @param (\Closure(array<string, mixed>, array<string, mixed>): (array<string, mixed>|\Throwable))|null $workflowCompletion
     * @param (\Closure(array<string, mixed>, string): (array<string, mixed>|\Throwable))|null $continuation
     */
    private function transport(mixed $capability = 'default', ?\Closure $workflowCompletion = null, ?\Closure $continuation = null): FakeTransport
    {
        $caps = self::caps($capability === 'default' ? self::capability() : $capability);
        $workflowPolls = 0;

        return new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use (
            $caps, $workflowCompletion, $continuation, &$workflowPolls,
        ): ?array {
            $respond = static function (array|\Throwable $answer): array {
                if ($answer instanceof \Throwable) {
                    throw $answer;
                }

                return $answer;
            };
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                return ++$workflowPolls === 1 ? self::firstWorkflowAnswer($caps) : ['task' => null, 'poll_status' => 'empty'];
            }
            if (str_ends_with($uri, '/workflow-tasks/wf-1/complete')) {
                $answer = ['task_id' => 'wf-1', 'run_id' => self::RUN_ID, 'run_status' => 'waiting', 'workflow_task_attempt' => 1,
                    'outcome' => 'completed', 'recorded' => true, 'reason' => null, 'created_task_ids' => ['act-1']];
                if (isset($body['continuation_tail'])) {
                    $answer['continuation_tail'] = ['version' => 2, 'after_sequence' => 1, 'last_sequence' => 3,
                        'history_events' => [self::scheduled(2, 'execution-1', 1), self::activityStarted(3, 'execution-1', 1, 'attempt-1')]];
                }
                if (isset($body['activity_poll'])) {
                    $answer['activity_poll'] = ['leased' => true, 'answer' => self::activityAnswer('act-1', 'execution-1', 'attempt-1', $caps)];
                }

                return $respond($workflowCompletion === null ? $answer : $workflowCompletion($answer, $body));
            }
            if (preg_match('~/activity-tasks/(act-\d)/continuation/complete$~', $uri, $match) === 1) {
                $answer = $continuation === null ? ($match[1] === 'act-1' ? self::acceptedFirst() : self::acceptedLast())
                    : $continuation($body, $match[1]);
                if (is_array($answer) && isset($answer['activity_poll']['answer'])) {
                    $answer['activity_poll']['answer']['server_capabilities'] = $caps;
                }

                return $respond($answer);
            }
            if (str_ends_with($uri, '/continuation/history')) {
                return ['task_id' => 'act-1', 'activity_attempt_id' => 'attempt-1',
                    'continuation' => ['version' => 1, 'eligible' => false, 'reason' => 'pending_workflow_task']];
            }
            if (preg_match('~/activity-tasks/(act-\d)/complete$~', $uri, $match) === 1) {
                return ['task_id' => $match[1], 'outcome' => 'completed', 'recorded' => true, 'reason' => null];
            }
            if (preg_match('~/workflow-tasks/(wf-\d)/complete$~', $uri, $match) === 1) {
                return ['task_id' => $match[1], 'outcome' => 'completed', 'recorded' => true, 'reason' => null,
                    'run_status' => 'waiting', 'created_task_ids' => []];
            }
            if (preg_match('~/workflow-tasks/(wf-\d)/heartbeat$~', $uri, $match) === 1) {
                return ['task_id' => $match[1], 'workflow_task_attempt' => 1, 'lease_owner' => 'worker', 'renewed' => true];
            }
            if (str_ends_with($uri, '/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }
            self::fail("Unexpected request {$method} {$uri}");
        });
    }

    /** @return array<string, mixed> */
    private static function capability(): array
    {
        return ['version' => 1,
            'history_endpoint' => '/worker/activity-tasks/{taskId}/continuation/history',
            'complete_endpoint' => '/worker/activity-tasks/{taskId}/continuation/complete',
            'supported_commands' => self::COMMANDS, 'max_history_events' => 256, 'max_commands' => 16,
            'supported_versions' => [1, 2],
            'v2' => ['version' => 2, 'complete_endpoint' => '/worker/activity-tasks/{taskId}/continuation/complete',
                'supported_commands' => self::COMMANDS, 'max_commands' => 16,
                'requires_worker_capabilities' => ['sticky_execution', 'sticky_history_suffix'],
                'tail_max_events' => 64, 'tail_max_bytes' => 262144]];
    }

    /** @return array<string, mixed> */
    private static function caps(mixed $capability): array
    {
        return ['workflow_task_completion_activity_poll' => true, 'activity_completion_workflow_poll' => true]
            + ($capability === null ? [] : ['activity_workflow_continuation' => $capability]);
    }

    /** @return array<string, mixed> */
    private static function event(int $sequence, string $type, array $payload): array
    {
        return ['id' => 'event-'.$sequence, 'sequence' => $sequence, 'event_type' => $type, 'payload' => $payload,
            'recorded_at' => '2026-11-26T12:00:0'.min($sequence, 9).'.000000Z', 'workflow_task_id' => null, 'workflow_command_id' => null];
    }

    /** @return array<string, mixed> */
    private static function scheduled(int $sequence, string $executionId, int $activitySequence): array
    {
        return self::event($sequence, 'ActivityScheduled', ['sequence' => $activitySequence, 'activity_execution_id' => $executionId,
            'activity_type' => 'test.activity', 'activity' => ['type' => 'test.activity']]);
    }

    /** @return array<string, mixed> */
    private static function activityStarted(int $sequence, string $executionId, int $activitySequence, string $attemptId): array
    {
        return self::event($sequence, 'ActivityStarted', ['sequence' => $activitySequence, 'activity_execution_id' => $executionId,
            'activity_type' => 'test.activity', 'activity' => ['type' => 'test.activity'], 'activity_attempt_id' => $attemptId,
            'attempt_number' => 1]);
    }

    /** @return array<string, mixed> */
    private static function activityCompleted(int $sequence, string $executionId, int $activitySequence, string $attemptId): array
    {
        return self::event($sequence, 'ActivityCompleted', ['sequence' => $activitySequence, 'activity_execution_id' => $executionId,
            'activity_type' => 'test.activity', 'activity' => ['type' => 'test.activity'], 'activity_attempt_id' => $attemptId,
            'attempt_number' => 1, 'result' => (new AvroPayloadCodec())->envelope(7), 'payload_codec' => 'avro']);
    }

    private static function now(): DateTimeImmutable
    {
        return new DateTimeImmutable('now', new DateTimeZone('UTC'));
    }

    /** @return array<string, mixed> */
    private static function firstWorkflowAnswer(array $caps): array
    {
        return ['task' => ['task_id' => 'wf-1', 'workflow_task_attempt' => 1, 'lease_owner' => 'worker',
            'workflow_id' => self::WORKFLOW_ID, 'run_id' => self::RUN_ID, 'workflow_type' => 'test.workflow',
            'payload_codec' => 'avro', 'arguments' => (new AvroPayloadCodec())->envelope([]),
            'sticky_replay_mode' => 'cold_replay', 'history_after_sequence' => 0, 'history_last_sequence' => 1,
            'history_events' => [self::event(1, 'WorkflowStarted', ['workflow_type' => 'test.workflow'])],
            'lease_expires_at' => self::now()->modify('+30 seconds')->format('Y-m-d\TH:i:s.u\Z')],
            'poll_status' => 'leased', 'protocol_version' => '1.20', 'server_capabilities' => $caps,
            'server_time' => self::now()->format('Y-m-d\TH:i:s.u\Z')];
    }

    /** The workflow task a declined continuation's carried poll leased: a sticky suffix after the cache. */
    private static function resumedWorkflowAnswer(): array
    {
        return ['task' => ['task_id' => 'wf-2', 'workflow_task_attempt' => 1, 'lease_owner' => 'worker',
            'workflow_id' => self::WORKFLOW_ID, 'run_id' => self::RUN_ID, 'workflow_type' => 'test.workflow',
            'payload_codec' => 'avro', 'arguments' => (new AvroPayloadCodec())->envelope([]),
            'sticky_replay_mode' => 'sticky_hit_expected', 'history_after_sequence' => 3, 'history_last_sequence' => 4,
            'history_events' => [self::activityCompleted(4, 'execution-1', 1, 'attempt-1')],
            'lease_expires_at' => self::now()->modify('+30 seconds')->format('Y-m-d\TH:i:s.u\Z')],
            'poll_status' => 'leased', 'protocol_version' => '1.20', 'server_capabilities' => [],
            'server_time' => self::now()->format('Y-m-d\TH:i:s.u\Z')];
    }

    /** @return array<string, mixed> */
    private static function activityAnswer(string $taskId, string $executionId, string $attemptId, array $caps = []): array
    {
        return ['task' => ['task_id' => $taskId, 'activity_attempt_id' => $attemptId, 'activity_execution_id' => $executionId,
            'attempt_number' => 1, 'lease_owner' => 'worker', 'activity_type' => 'test.activity', 'payload_codec' => 'avro',
            'workflow_id' => self::WORKFLOW_ID, 'run_id' => self::RUN_ID],
            'poll_status' => 'leased', 'protocol_version' => '1.20', 'server_capabilities' => $caps,
            'server_time' => self::now()->format('Y-m-d\TH:i:s.u\Z')];
    }

    /** The first activity's accepted answer: the second activity is scheduled, leased and in the tail. */
    private static function acceptedFirst(): array
    {
        return ['task_id' => 'act-1', 'activity_attempt_id' => 'attempt-1', 'outcome' => 'completed', 'recorded' => true,
            'reason' => null, 'next_task_id' => 'wf-2',
            'continuation' => ['version' => 2, 'outcome' => 'accepted', 'workflow_task_id' => 'wf-2', 'workflow_task_attempt' => 1,
                'completion' => ['task_id' => 'wf-2', 'run_id' => self::RUN_ID, 'run_status' => 'waiting', 'workflow_task_attempt' => 1,
                    'outcome' => 'completed', 'recorded' => true, 'reason' => null, 'created_task_ids' => ['act-2']]],
            'continuation_tail' => ['version' => 2, 'after_sequence' => 3, 'last_sequence' => 6, 'history_events' => [
                self::activityCompleted(4, 'execution-1', 1, 'attempt-1'), self::scheduled(5, 'execution-2', 2),
                self::activityStarted(6, 'execution-2', 2, 'attempt-2')]],
            'activity_poll' => ['leased' => true, 'answer' => self::activityAnswer('act-2', 'execution-2', 'attempt-2')]];
    }

    /** The second activity's accepted answer: the run completed. */
    private static function acceptedLast(): array
    {
        return ['task_id' => 'act-2', 'activity_attempt_id' => 'attempt-2', 'outcome' => 'completed', 'recorded' => true,
            'reason' => null, 'next_task_id' => 'wf-3',
            'continuation' => ['version' => 2, 'outcome' => 'accepted', 'workflow_task_id' => 'wf-3', 'workflow_task_attempt' => 1,
                'completion' => ['task_id' => 'wf-3', 'run_id' => self::RUN_ID, 'run_status' => 'completed', 'workflow_task_attempt' => 1,
                    'outcome' => 'completed', 'recorded' => true, 'reason' => null, 'created_task_ids' => []]],
            'continuation_tail' => ['version' => 2, 'after_sequence' => 6, 'last_sequence' => 8, 'history_events' => [
                self::activityCompleted(7, 'execution-2', 2, 'attempt-2'), self::event(8, 'WorkflowCompleted', [])]],
            'activity_poll' => ['leased' => false]];
    }

    /** @param array<string, mixed>|null $workflowPoll */
    private static function declined(string $reason, ?array $workflowPoll): array
    {
        return ['task_id' => 'act-1', 'activity_attempt_id' => 'attempt-1', 'outcome' => 'completed', 'recorded' => true,
            'reason' => null, 'next_task_id' => 'wf-2', 'continuation' => ['version' => 2, 'outcome' => 'declined', 'reason' => $reason]]
            + ($workflowPoll === null ? [] : ['workflow_poll' => $workflowPoll]);
    }

    /** @return list<array<string, mixed>>|null */
    private function cachedHistory(Worker $worker): ?array
    {
        /** @var StickyWorkflowCache $cache */
        $cache = self::property($worker, 'stickyCache');

        return $cache->cachedHistory(self::WORKFLOW_ID, self::RUN_ID, self::BUILD_ID);
    }

    /** @param list<array<string, mixed>>|null $history */
    private static function lastSequence(?array $history): ?int
    {
        return $history === null || $history === [] ? null : $history[array_key_last($history)]['sequence'];
    }

    private static function property(Worker $worker, string $name): mixed
    {
        return (new \ReflectionProperty($worker, $name))->getValue($worker);
    }

    /** @return list<array<string, mixed>> */
    private function events(string $name): array
    {
        return array_values(array_map(static fn (array $row): array => $row[1],
            array_filter($this->diagnostics, static fn (array $row): bool => $row[0] === $name)));
    }

    /** @return list<array<string, mixed>> */
    private static function requests(FakeTransport $transport, string $suffix): array
    {
        return array_values(array_filter($transport->requests, static fn (array $row): bool => str_ends_with($row['uri'], $suffix)));
    }
}
