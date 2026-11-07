<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Tests\Support\WorkerPipelineScenario;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Worker;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class WorkerCompletionPipeliningTest extends TestCase
{
    public function testOptInOverlapsACompletionAnswerWithOneAlreadyLeasedWorkflow(): void
    {
        $observed = self::runWorker(true);
        self::assertSame([1, 2, 3], $observed['callbacks']);
        $sent = self::position($observed['events'], 'completion_sent', 2);
        $next = self::position($observed['events'], 'callback', 3);
        $answer = self::position($observed['events'], 'completion_answer', 2);
        $completedNext = self::position($observed['events'], 'completion_sent', 3);
        self::assertLessThan($next, $sent, 'The parent body is sent before the next callback.');
        self::assertLessThan($answer, $next, 'Useful leased work happens before the first answer.');
        self::assertLessThan($completedNext, $answer, 'The first answer is joined before the next completion.');
        self::assertArrayHasKey('workflow_poll', $observed['completions'][1], 'The warm-up completion may carry a poll whose result is omitted.');
        self::assertArrayNotHasKey('workflow_poll', $observed['completions'][2]);
    }

    public function testTheDefaultOffWorkerFinishesTheCompletionBeforeTheNextWorkflow(): void
    {
        $observed = self::runWorker(false);
        self::assertSame([1, 2, 3], $observed['callbacks']);
        self::assertLessThan(self::position($observed['events'], 'callback', 3),
            self::position($observed['events'], 'completion_answer', 2));
    }

    public function testOmittingTheOptionAndExplicitFalseSendTheSamePackets(): void
    {
        $implicit = WorkerPipelineScenario::run(null);
        $explicit = WorkerPipelineScenario::run(false);
        self::assertNull($implicit['error']);
        self::assertNull($explicit['error']);
        self::assertSame(self::stablePackets($implicit['requests']), self::stablePackets($explicit['requests']));
        foreach (['__construct', 'create'] as $method) {
            $parameters = (new \ReflectionMethod(Worker::class, $method))->getParameters();
            $option = array_values(array_filter($parameters, static fn ($parameter): bool => $parameter->getName() === 'enableCompletionPipelining'))[0];
            self::assertFalse($option->getDefaultValue());
        }
    }

    #[DataProvider('synchronousModes')]
    public function testIneligibleCasesKeepTheCompletionSynchronous(string $mode): void
    {
        $observed = WorkerPipelineScenario::run(true, ['mode' => $mode]);
        if ($observed['error'] !== null) throw $observed['error'];
        self::assertSame([1, 2, 3], $observed['callbacks']);
        self::assertLessThan(self::position($observed['events'], 'callback', 3),
            self::position($observed['events'], 'completion_answer', 2));
    }

    public static function synchronousModes(): iterable
    {
        foreach (['tick', 'client_metrics', 'worker_metrics', 'worker_telemetry', 'client_telemetry',
            'schedule_activity', 'kept_failure', 'no_local', 'cooperative'] as $mode) yield $mode => [$mode];
    }

    public function testAnEarlierColdDiscoveryRequestPreventsSuspensionWithoutLosingThePrefetch(): void
    {
        $observed = WorkerPipelineScenario::run(true, ['task_count' => 2, 'hold_task' => 1, 'local_task' => 1]);
        if ($observed['error'] !== null) throw $observed['error'];
        self::assertSame([1, 2], $observed['callbacks']);
        self::assertLessThan(self::position($observed['events'], 'callback', 2), self::position($observed['events'], 'completion_answer', 1));
        self::assertArrayNotHasKey('workflow_poll', $observed['completions'][1]);
    }

    public function testAnEmptyProbeAndMissingCarriedResultDoNotKeepTheWorkflowSlotReserved(): void
    {
        $observed = WorkerPipelineScenario::run(true, ['empty_prefetch' => true]);
        if ($observed['error'] !== null) throw $observed['error'];
        self::assertSame([1, 2, 3], $observed['callbacks']);
        $carried = $observed['completions'][2]['workflow_poll']['poll_request_id'];
        $polls = array_values(array_filter($observed['requests'], static fn (array $request): bool => str_ends_with($request['path'], '/workflow-tasks/poll')));
        self::assertNotSame($carried, $polls[3]['body']['poll_request_id']);
    }

    public function testAGenericPrefetchFaultIsReportedAfterTheParentCompletionWithoutAnotherClaim(): void
    {
        $observed = WorkerPipelineScenario::run(true, ['prefetch_error_status' => 500]);
        self::assertInstanceOf(ServerException::class, $observed['error']);
        self::assertSame('fixture_poll_failure', $observed['error']->reason);
        self::assertSame([1, 2], $observed['callbacks']);
        self::assertArrayHasKey(2, $observed['completions']);
        self::assertArrayNotHasKey('workflow_poll', $observed['completions'][2]);
        self::assertSame([], array_filter($observed['requests'], static fn (array $request): bool => str_ends_with($request['path'], '/fail')));
    }

    public function testATransientPrefetchFaultRecoversTheOriginalPollIdentityAfterPublishingTheParent(): void
    {
        $observed = WorkerPipelineScenario::run(true, ['prefetch_error_status' => 502]);
        if ($observed['error'] !== null) throw $observed['error'];
        self::assertSame([1, 2, 3], $observed['callbacks']);
        $polls = array_values(array_filter($observed['requests'], static fn (array $request): bool => str_ends_with($request['path'], '/workflow-tasks/poll')));
        self::assertSame($polls[2]['body']['poll_request_id'], $polls[3]['body']['poll_request_id']);
        self::assertArrayNotHasKey('workflow_poll', $observed['completions'][2]);
    }

    public function testAParentRegistrationRefusalIsThrownAfterUsefulWorkAndPreventsItsPublication(): void
    {
        $observed = WorkerPipelineScenario::run(true, ['parent_status' => 403]);
        self::assertInstanceOf(ServerException::class, $observed['error']);
        self::assertSame(403, $observed['error']->status);
        self::assertSame('worker_registration_fenced', $observed['error']->reason);
        self::assertSame([1, 2, 3], $observed['callbacks']);
        self::assertArrayNotHasKey(3, $observed['completions']);
        self::assertSame([], array_filter($observed['requests'], static fn (array $request): bool => str_ends_with($request['path'], '/fail')));
    }

    public function testAParentRegistrationRefusalAlsoPreventsTheNextTasksFailureAcknowledgement(): void
    {
        $observed = WorkerPipelineScenario::run(true, ['parent_status' => 403, 'child_bad_arguments' => true]);
        self::assertInstanceOf(ServerException::class, $observed['error']);
        self::assertSame('worker_registration_fenced', $observed['error']->reason);
        self::assertSame([1, 2], $observed['callbacks']);
        self::assertArrayNotHasKey(3, $observed['completions']);
        self::assertSame([], array_filter($observed['requests'], static fn (array $request): bool => str_ends_with($request['path'], '/fail')));
    }

    public function testAnUncertainParentAnswerRetriesTheSameFenceAndComputedCommandsWithoutRunningTheHandlerAgain(): void
    {
        $observed = WorkerPipelineScenario::run(true, ['lose_parent_answer' => true]);
        if ($observed['error'] !== null) throw $observed['error'];
        self::assertSame([1, 2, 3], $observed['callbacks']);
        self::assertCount(2, $observed['completion_bodies'][2]);
        self::assertSame($observed['completion_bodies'][2][0], $observed['completion_bodies'][2][1]);
        self::assertSame('pipeline-worker', $observed['completion_bodies'][2][0]['lease_owner']);
        self::assertSame(1, $observed['completion_bodies'][2][0]['workflow_task_attempt']);
        self::assertSame(1, count(array_filter($observed['events'], static fn (array $event): bool => $event === ['kind' => 'local', 'task' => 2])));
    }

    public function testAnIdentityBoundLostParentClaimDoesNotFailTheNextWorkflow(): void
    {
        $observed = WorkerPipelineScenario::run(true, ['parent_status' => 409]);
        if ($observed['error'] !== null) throw $observed['error'];
        self::assertSame([1, 2, 3], $observed['callbacks']);
        self::assertArrayHasKey(3, $observed['completions']);
        self::assertSame([], array_filter($observed['requests'], static fn (array $request): bool => str_ends_with($request['path'], '/fail')));
    }

    public function testAMismatchedLostClaimAnswerRemainsFatalAfterTheNextWorkflowCompletes(): void
    {
        $observed = WorkerPipelineScenario::run(true, ['parent_status' => 409, 'wrong_parent_binding' => true]);
        self::assertInstanceOf(ServerException::class, $observed['error']);
        self::assertSame(409, $observed['error']->status);
        self::assertSame([1, 2, 3], $observed['callbacks']);
        self::assertArrayHasKey(3, $observed['completions']);
    }

    public function testTheParentTicksLaterPollFailureIsReportedAfterJoiningAndCompletingTheNextWorkflow(): void
    {
        $observed = WorkerPipelineScenario::run(true, ['activity_poll_failure' => true]);
        self::assertInstanceOf(ServerException::class, $observed['error']);
        self::assertSame('fixture_activity_poll_failure', $observed['error']->reason);
        self::assertSame([1, 2, 3], $observed['callbacks']);
        self::assertArrayHasKey(3, $observed['completions']);
        self::assertSame([], array_filter($observed['requests'], static fn (array $request): bool => str_ends_with($request['path'], '/fail')));
    }

    public function testAnOldRuntimeWithoutCarriedPollCapabilitiesStillUsesTheOptInOrdinaryProbe(): void
    {
        $observed = WorkerPipelineScenario::run(true, ['workflow_carrier' => false]);
        if ($observed['error'] !== null) throw $observed['error'];
        self::assertSame(['parent_sent', 'next_work', 'parent_answer', 'next_completion'], WorkerPipelineScenario::completionOrder($observed));
        foreach ($observed['completions'] as $completion) self::assertArrayNotHasKey('workflow_poll', $completion);
    }

    public function testAnAgedPrefetchMustConfirmItsOriginalFenceBeforeTheCallback(): void
    {
        $observed = WorkerPipelineScenario::run(true, ['delayed_prefetch' => true, 'lose_claim_task' => 3]);
        if ($observed['error'] !== null) throw $observed['error'];
        self::assertSame([1, 2], $observed['callbacks']);
        self::assertArrayNotHasKey(3, $observed['completions']);
        self::assertGreaterThanOrEqual(0, self::position($observed['events'], 'heartbeat', 3));
    }

    #[DataProvider('shutdownModes')]
    public function testShutdownFinishesTheAlreadyLeasedTaskAndMakesNoFurtherPoll(string $mode): void
    {
        $observed = WorkerPipelineScenario::run(true, ['mode' => $mode]);
        if ($observed['error'] !== null) throw $observed['error'];
        self::assertSame([1, 2, 3], $observed['callbacks']);
        self::assertArrayHasKey(3, $observed['completions']);
        self::assertSame(3, $observed['workflow_polls']);
        self::assertArrayNotHasKey('workflow_poll', $observed['completions'][3]);
    }

    public static function shutdownModes(): iterable
    {
        yield 'explicit request' => ['shutdown'];
        yield 'managed SIGTERM' => ['signal'];
    }

    public function testRollingLocalWorkNeverLeasesMoreThanOneWorkflowAhead(): void
    {
        $observed = WorkerPipelineScenario::run(true, ['task_count' => 5, 'local_all' => true]);
        if ($observed['error'] !== null) throw $observed['error'];
        self::assertSame([1, 2, 3, 4, 5], $observed['callbacks']);
        $held = $maximum = 0;
        foreach ($observed['events'] as $event) {
            if ($event['kind'] === 'leased') ++$held;
            if ($event['kind'] === 'completion_sent') --$held;
            $maximum = max($maximum, $held);
        }
        self::assertSame(2, $maximum, 'One completing workflow and one next lease are the maximum.');
        foreach ([2, 3, 4] as $task) self::assertArrayNotHasKey('workflow_poll', $observed['completions'][$task]);
    }

    private static function stablePackets(mixed $value): mixed
    {
        if (!is_array($value)) return $value;
        foreach ($value as $key => $item) {
            $value[$key] = in_array($key, ['poll_request_id', 'duration_ms'], true) ? '<generated>' : self::stablePackets($item);
        }
        return $value;
    }

    private static function runWorker(bool $enabled): array
    {
        $observed = WorkerPipelineScenario::run($enabled);
        if ($observed['error'] !== null) throw $observed['error'];
        return $observed;
    }

    private static function position(array $events, string $kind, int $task): int
    {
        foreach ($events as $index => $event) {
            if ($event['kind'] === $kind && $event['task'] === $task) return $index;
        }
        self::fail('Expected fixture event: '.$kind.' for task '.$task);
    }
}
