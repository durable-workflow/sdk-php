<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Qualification\CompletionPollEvidence;
use DurableWorkflow\Qualification\CompletionPollTrace;
use PHPUnit\Framework\TestCase;
use RuntimeException;

require_once __DIR__.'/../scripts/conformance/Support/CompletionPollSubject.php';
require_once __DIR__.'/../scripts/conformance/Support/CompletionPollTrace.php';
require_once __DIR__.'/../scripts/conformance/Support/CompletionPollEvidence.php';

final class CompletionPollConsumerEvidenceTest extends TestCase
{
    public function testAllThreeCarrierPathsNeedAnInlineLeaseAndItsActualCallback(): void
    {
        $result = CompletionPollEvidence::verify(self::carriers(), 'carriers', 'fixture-worker', 'fixture');
        self::assertSame(['workflow-activity' => 1, 'activity-workflow' => 1, 'workflow-workflow' => 1], $result['inline_delivery_counts']);
    }

    public function testIgnoringAnInlineAnswerAndPollingItsTaskAgainDoesNotPass(): void
    {
        $events = self::carriers();
        array_push($events, ...self::exchange('extra', '/api/worker/workflow-tasks/poll', [], ['task' => self::task('wf-2', 'fixture-2')]));
        $this->expectExceptionMessage('completion_evidence_inline_delivery');
        CompletionPollEvidence::verify($events, 'carriers', 'fixture-worker', 'fixture');
    }

    public function testAnEarlierAttemptOfTheSameTaskDoesNotInvalidateNewInlineDelivery(): void
    {
        $old = self::task('wf-2', 'fixture-2');
        // Only the new positive lease is authoritative; the old metadata precedes this invocation's delivery.
        $events = array_merge(self::exchange('earlier', '/api/worker/workflow-tasks/poll', [], ['task' => $old]), self::carriers());
        $events[8]['answer']['workflow_poll']['task']['workflow_task_attempt'] = 2;
        $result = CompletionPollEvidence::verify($events, 'carriers', 'fixture-worker', 'fixture');
        self::assertSame(1, $result['inline_delivery_counts']['activity-workflow']);
    }

    public function testACallbackOnALaterRecoveryAttemptCannotProveEarlierInlineDelivery(): void
    {
        $events = self::carriers();
        $new = self::task('wf-2', 'fixture-2');
        $new['workflow_task_attempt'] = 2;
        array_splice($events, 7, 0, self::exchange('recovery', '/api/worker/workflow-tasks/poll', [], ['task' => $new]));
        $this->expectExceptionMessage('completion_evidence_missing_callback');
        CompletionPollEvidence::verify($events, 'carriers', 'fixture-worker', 'fixture');
    }

    public function testACallbackBeforeAnswerDeliveryIsNotInlineDeliveryEvidence(): void
    {
        $events = self::carriers();
        [$events[2], $events[3]] = [$events[3], $events[2]];
        $this->expectExceptionMessage('completion_evidence_missing_callback');
        CompletionPollEvidence::verify($events, 'carriers', 'fixture-worker', 'fixture');
    }

    public function testAnOmittedCarrierIsNotFabricatedAsALease(): void
    {
        $events = self::carriers();
        unset($events[1]['answer']['activity_poll']);
        $this->expectExceptionMessage('completion_evidence_missing_carrier_path');
        CompletionPollEvidence::verify($events, 'carriers', 'fixture-worker', 'fixture');
    }

    public function testAnotherWorkersLeaseCannotBeAccepted(): void
    {
        $events = self::carriers();
        $events[1]['answer']['activity_poll']['task']['lease_owner'] = 'other-worker';
        $this->expectExceptionMessage('completion_evidence_lease_identity');
        CompletionPollEvidence::verify($events, 'carriers', 'fixture-worker', 'fixture');
    }

    public function testAWorkerFailureIsNotHiddenByEarlierSuccessfulCallbacks(): void
    {
        $events = self::carriers();
        $events[] = ['kind' => 'worker_error'];
        $this->expectExceptionMessage('completion_evidence_process_failure');
        CompletionPollEvidence::verify($events, 'carriers', 'fixture-worker', 'fixture');
    }

    public function testPipelineRequiresPrefetchBeforeSendAndJoinBeforeNextCompletion(): void
    {
        $result = CompletionPollEvidence::verify(self::pipeline(), 'pipeline', 'fixture-worker', 'fixture');
        self::assertTrue($result['controlled_answer_overlap']);
        self::assertTrue($result['joined_before_next_completion']);
    }

    public function testSynchronousOrderingCannotBeLabeledAsOverlap(): void
    {
        $events = self::pipeline();
        [$events[8], $events[10]] = [$events[10], $events[8]];
        $this->expectExceptionMessage('completion_evidence_pipeline_order');
        CompletionPollEvidence::verify($events, 'pipeline', 'fixture-worker', 'fixture');
    }

    public function testAnotherWorkflowPollWhileAParentAnswerIsPendingFailsTheBound(): void
    {
        $events = self::pipeline();
        array_splice($events, 10, 0, [['kind' => 'request', 'request' => 'extra', 'path' => '/api/worker/workflow-tasks/poll', 'body' => []]]);
        $this->expectExceptionMessage('completion_evidence_extra_workflow_reservation');
        CompletionPollEvidence::verify($events, 'pipeline', 'fixture-worker', 'fixture');
    }

    public function testTheNextCompletionMustKeepItsOriginalAttemptAndOwner(): void
    {
        $events = self::pipeline();
        $events[11]['body']['workflow_task_attempt'] = 2;
        $this->expectExceptionMessage('completion_evidence_publication_fence');
        CompletionPollEvidence::verify($events, 'pipeline', 'fixture-worker', 'fixture');
    }

    public function testTheControlledLossCaseRequiresAnActualLostDeliveryAndIdenticalRetry(): void
    {
        $events = self::pipeline();
        $events[10]['status'] = 502;
        $retry = self::exchange('retry', '/api/worker/workflow-tasks/wf-2/complete', $events[6]['body'], []);
        array_splice($events, 11, 0, $retry);
        $events[] = ['kind' => 'callback', 'workflow_id' => 'fixture-2'];
        for ($step = 0; $step < 5; ++$step) $events[] = ['kind' => 'local', 'index' => 2];
        $result = CompletionPollEvidence::verify($events, 'pipeline-loss', 'fixture-worker', 'fixture');
        self::assertSame(2, $result['parent_attempts']);
        $events[11]['body']['body_sha256'] = 'changed';
        $this->expectExceptionMessage('completion_evidence_lost_answer_retry');
        CompletionPollEvidence::verify($events, 'pipeline-loss', 'fixture-worker', 'fixture');
    }

    public function testAnUpstreamRefusalCannotBeLabeledAsACommittedAnswerLoss(): void
    {
        $events = self::pipeline();
        $events[7]['status'] = 403;
        $this->expectExceptionMessage('completion_evidence_pipeline_order');
        CompletionPollEvidence::verify($events, 'pipeline', 'fixture-worker', 'fixture');
    }

    public function testACallbackMarkerWithoutUsefulLocalWorkIsNotOverlapEvidence(): void
    {
        $events = array_values(array_filter(self::pipeline(), static fn (array $event): bool => $event['kind'] !== 'local'));
        $this->expectExceptionMessage('completion_evidence_no_overlapped_work');
        CompletionPollEvidence::verify($events, 'pipeline', 'fixture-worker', 'fixture');
    }

    public function testFallbackRefusesCapabilityOrCarrierEvidence(): void
    {
        $this->expectExceptionMessage('completion_evidence_fallback_capability');
        CompletionPollEvidence::verify(self::carriers(), 'fallback', 'fixture-worker', 'fixture');
    }

    public function testAnEmptyOlderServerPollCannotQualifyTheFallbackWorkload(): void
    {
        $events = self::exchange('empty', '/api/worker/workflow-tasks/poll', [], ['task' => null]);
        $events[1]['answer']['capabilities'] = [];
        $this->expectExceptionMessage('completion_evidence_fallback_no_work');
        CompletionPollEvidence::verify($events, 'fallback', 'fixture-worker', 'fixture');
    }

    public function testTraceRetainsNoCredentialHeaderOrOutcomePayload(): void
    {
        $safe = CompletionPollTrace::request(['Authorization' => 'synthetic-header', 'result' => 'synthetic-payload',
            'commands' => [['type' => 'complete_workflow', 'result' => ['blob' => 'synthetic-payload']]],
            'lease_owner' => 'fixture-worker', 'workflow_task_attempt' => 1]);
        self::assertSame(['complete_workflow'], $safe['command_types']);
        self::assertArrayNotHasKey('Authorization', $safe);
        self::assertArrayNotHasKey('result', $safe);
        self::assertStringNotContainsString('synthetic-payload', json_encode($safe, JSON_THROW_ON_ERROR));
        self::assertArrayNotHasKey('registration_token', CompletionPollTrace::answer(['registration_token' => 'synthetic-token']));
    }

    public function testRetryDigestsBindTheActualWireBytes(): void
    {
        $first = CompletionPollTrace::request(['lease_owner' => 'fixture-worker'], '{"lease_owner":"fixture-worker"}');
        $second = CompletionPollTrace::request(['lease_owner' => 'fixture-worker'], '{ "lease_owner" : "fixture-worker" }');
        self::assertNotSame($first['body_sha256'], $second['body_sha256']);
    }

    private static function carriers(): array
    {
        return array_merge(
            self::exchange('a', '/api/worker/workflow-tasks/parent/complete', ['activity_poll' => []],
                ['activity_poll' => ['leased' => true, 'task' => self::task('act-1', 'fixture-1')]]),
            [['kind' => 'activity', 'task_id' => 'act-1']],
            self::exchange('b', '/api/worker/activity-tasks/act-1/complete', ['workflow_poll' => []],
                ['workflow_poll' => ['leased' => true, 'task' => self::task('wf-2', 'fixture-2')]]),
            [['kind' => 'callback', 'workflow_id' => 'fixture-2']],
            self::exchange('c', '/api/worker/workflow-tasks/wf-2/complete', ['workflow_poll' => []],
                ['workflow_poll' => ['leased' => true, 'task' => self::task('wf-3', 'fixture-3')]]),
            [['kind' => 'callback', 'workflow_id' => 'fixture-3']],
        );
    }

    private static function pipeline(): array
    {
        return [
            ['kind' => 'request', 'request' => 'original', 'path' => '/api/worker/workflow-tasks/poll', 'body' => ['timeout_seconds' => 0, 'worker_id' => 'fixture-worker', 'poll_request_id' => 'original-2']],
            ['kind' => 'upstream', 'request' => 'original', 'status' => 200, 'answer' => ['task' => self::task('wf-2', 'fixture-2'), 'capabilities' => self::caps()]],
            ['kind' => 'delivered', 'request' => 'original', 'status' => 200],
            ['kind' => 'request', 'request' => 'poll', 'path' => '/api/worker/workflow-tasks/poll', 'body' => ['timeout_seconds' => 0, 'worker_id' => 'fixture-worker', 'poll_request_id' => 'prefetch-3']],
            ['kind' => 'upstream', 'request' => 'poll', 'status' => 200, 'answer' => ['task' => self::task('wf-3', 'fixture-3'), 'capabilities' => self::caps()]],
            ['kind' => 'delivered', 'request' => 'poll', 'status' => 200],
            ['kind' => 'request', 'request' => 'parent', 'path' => '/api/worker/workflow-tasks/wf-2/complete', 'body' => ['lease_owner' => 'fixture-worker', 'workflow_task_attempt' => 1]],
            ['kind' => 'upstream', 'request' => 'parent', 'status' => 200, 'answer' => []],
            ['kind' => 'callback', 'workflow_id' => 'fixture-3'],
            ['kind' => 'local', 'index' => 3],
            ['kind' => 'delivered', 'request' => 'parent', 'status' => 200],
            ['kind' => 'request', 'request' => 'child', 'path' => '/api/worker/workflow-tasks/wf-3/complete', 'body' => ['lease_owner' => 'fixture-worker', 'workflow_task_attempt' => 1]],
            ['kind' => 'upstream', 'request' => 'child', 'status' => 200, 'answer' => []],
            ['kind' => 'delivered', 'request' => 'child', 'status' => 200],
        ];
    }

    private static function exchange(string $id, string $path, array $body, array $answer): array
    {
        $body += ['lease_owner' => 'fixture-worker'];
        foreach (['activity_poll', 'workflow_poll'] as $key) {
            if (isset($body[$key])) $body[$key] += ['worker_id' => 'fixture-worker', 'poll_request_id' => 'carried-'.$id];
        }
        return [['kind' => 'request', 'request' => $id, 'path' => $path, 'body' => $body],
            ['kind' => 'upstream', 'request' => $id, 'status' => 200, 'answer' => $answer + ['capabilities' => self::caps()]],
            ['kind' => 'delivered', 'request' => $id, 'status' => 200]];
    }

    private static function task(string $id, string $workflow): array
    {
        return ['task_id' => $id, 'workflow_id' => $workflow, 'run_id' => $workflow.'-run',
            'lease_owner' => 'fixture-worker', 'workflow_task_attempt' => 1,
            'activity_attempt_id' => 'fixture-activity-attempt', 'lease_expires_at' => '2026-11-07T06:00:00Z'];
    }

    private static function caps(): array
    {
        return array_fill_keys(CompletionPollEvidence::CAPABILITIES, true);
    }
}
