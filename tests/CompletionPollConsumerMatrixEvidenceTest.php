<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Qualification\CompletionPollEvidence;
use DurableWorkflow\Qualification\CompletionPollTrace;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

require_once __DIR__.'/../scripts/conformance/Support/CompletionPollSubject.php';
require_once __DIR__.'/../scripts/conformance/Support/CompletionPollTrace.php';
require_once __DIR__.'/../scripts/conformance/Support/CompletionPollEvidence.php';

final class CompletionPollConsumerMatrixEvidenceTest extends TestCase
{
    #[DataProvider('lossPaths')]
    public function testEachLostCarriedAnswerNeedsItsOwnOriginalRecovery(string $scenario, string $member, string $parent): void
    {
        $result = CompletionPollEvidence::verify(self::loss($member, $parent), $scenario, 'worker', 'fixture');
        self::assertTrue($result['original_poll_id_recovered']);
        self::assertTrue($result['original_task_history_deadlines_preserved']);
    }

    public static function lossPaths(): iterable
    {
        yield ['carrier-loss-activity', 'activity_poll', 'workflow'];
        yield ['carrier-loss-workflow', 'workflow_poll', 'activity'];
        yield ['carrier-loss-workflow-completion', 'workflow_poll', 'workflow'];
    }

    public function testAnActivityLossDoesNotQualifyAWorkflowLossScenario(): void
    {
        $this->expectExceptionMessage('completion_evidence_loss_not_targeted');
        CompletionPollEvidence::verify(self::loss('activity_poll', 'workflow'), 'carrier-loss-workflow', 'worker', 'fixture');
    }

    public function testRecoveryMustKeepOriginalIdAndImmutableDeadline(): void
    {
        $events = self::loss('workflow_poll', 'activity');
        $events[7]['body']['poll_request_id'] = 'fresh-id';
        $this->expectExceptionMessage('completion_evidence_recovery_identity_changed');
        CompletionPollEvidence::verify($events, 'carrier-loss-workflow', 'worker', 'fixture');
    }

    public function testRestampingTheRecoveredLeaseCannotPass(): void
    {
        $events = self::loss('workflow_poll', 'activity');
        $changed = self::task('workflow');
        $changed['lease_expires_at'] = '2026-11-07T08:01:00.000000Z';
        $events[8]['answer'] = CompletionPollTrace::answer(self::grant($changed));
        $this->expectExceptionMessage('completion_evidence_replay_authority_changed');
        CompletionPollEvidence::verify($events, 'carrier-loss-workflow', 'worker', 'fixture');
    }

    public function testRetryingDifferentCompletionBytesCannotPass(): void
    {
        $events = self::loss('workflow_poll', 'activity');
        $events[4]['body']['body_sha256'] = str_repeat('c', 64);
        $this->expectExceptionMessage('completion_evidence_completion_retry_changed');
        CompletionPollEvidence::verify($events, 'carrier-loss-workflow', 'worker', 'fixture');
    }

    public function testReplayedMetadataWithoutACallbackDoesNotQualifyDelivery(): void
    {
        $events = self::loss('workflow_poll', 'activity');
        array_pop($events);
        $this->expectExceptionMessage('completion_evidence_recovered_callback_missing');
        CompletionPollEvidence::verify($events, 'carrier-loss-workflow', 'worker', 'fixture');
    }

    public function testCompletionTransitNeedsIdentityBoundConfirmationBeforeCallback(): void
    {
        $events = self::transit();
        $result = CompletionPollEvidence::verify($events, 'completion-transit', 'worker', 'fixture');
        self::assertTrue($result['confirmed_before_callback']);
        $events[6]['answer']['workflow_task_attempt'] = 2;
        $this->expectExceptionMessage('completion_evidence_confirmation_missing');
        CompletionPollEvidence::verify($events, 'completion-transit', 'worker', 'fixture');
    }

    public function testAShortRequestCannotQualifyWholeCompletionTransitCharging(): void
    {
        $events = self::transit();
        $events[4]['at'] = 2_000_000_000;
        $this->expectExceptionMessage('completion_evidence_completion_transit_not_targeted');
        CompletionPollEvidence::verify($events, 'completion-transit', 'worker', 'fixture');
    }

    public function testRealExpiryRefusalNeedsNoCallbackOrPublicationForTheOldGeneration(): void
    {
        $events = self::expiry();
        self::assertTrue(CompletionPollEvidence::verify($events, 'carrier-expiry', 'worker', 'fixture')['original_expiry_reached']);
        $events[] = self::event('callback', 32, ['workflow_id' => 'fixture-3']);
        $this->expectExceptionMessage('completion_evidence_expired_callback');
        CompletionPollEvidence::verify($events, 'carrier-expiry', 'worker', 'fixture');
    }

    public function testAnUnrelatedServerFailureIsNotAnExpiryRefusal(): void
    {
        $events = self::expiry();
        $events[5]['status'] = 500;
        $events[5]['answer']['reason'] = 'internal_error';
        $this->expectExceptionMessage('completion_evidence_expiry_refusal_missing');
        CompletionPollEvidence::verify($events, 'carrier-expiry', 'worker', 'fixture');
    }

    public function testActivityRefenceRequiresRotationAndRealOriginalIdRefusal(): void
    {
        $events = self::loss('activity_poll', 'workflow');
        $events[2]['mode'] = 'refenced_activity_answer';
        $events[8]['status'] = 409;
        $events[8]['answer'] = ['reason' => 'poll_cached_task_expired'];
        $events[9]['status'] = 409;
        array_pop($events);
        array_splice($events, 2, 0, [self::event('refence', 0.15, ['worker_id' => 'worker',
            'old_registration_sha256' => str_repeat('a', 64), 'registration_sha256' => str_repeat('b', 64)])]);
        $result = CompletionPollEvidence::verify($events, 'carrier-refence', 'worker', 'fixture');
        self::assertTrue($result['original_activity_recovery_refused']);
        self::assertFalse($result['existing_workflow_grant_revocation_asserted']);
        $events[2]['registration_sha256'] = $events[2]['old_registration_sha256'];
        $this->expectExceptionMessage('completion_evidence_refence_recovery');
        CompletionPollEvidence::verify($events, 'carrier-refence', 'worker', 'fixture');
    }

    public function testBufferedAgeNeedsARealDelayAndLaterLeaseConfirmation(): void
    {
        $events = self::buffer();
        self::assertTrue(CompletionPollEvidence::verify($events, 'pipeline-buffer-age', 'worker', 'fixture')['confirmed_before_callback']);
        $events = array_values(array_filter($events, static fn (array $event): bool => ($event['request'] ?? null) !== 'confirm'));
        $this->expectExceptionMessage('completion_evidence_confirmation_missing');
        CompletionPollEvidence::verify($events, 'pipeline-buffer-age', 'worker', 'fixture');
    }

    public function testDefaultOffRequiresBothTheActualDefaultAndSynchronousDelivery(): void
    {
        $events = self::buffer();
        array_unshift($events, self::event('default_option', 0, ['default' => false, 'explicit' => false]));
        // Move the parent delivery ahead of the callback for the synchronous control.
        $parent = array_values(array_filter($events, static fn (array $event): bool => $event['kind'] === 'delivered' && $event['request'] === 'parent'))[0];
        $parent['at'] = 1_850_000_000;
        $events = array_values(array_filter($events, static fn (array $event): bool => !($event['kind'] === 'delivered' && $event['request'] === 'parent')));
        $at = array_keys(array_filter($events, static fn (array $event): bool => $event['kind'] === 'callback'))[0];
        array_splice($events, $at, 0, [$parent]);
        self::assertTrue(CompletionPollEvidence::verify($events, 'default-off', 'worker', 'fixture')['completion_remained_synchronous']);
        $events[0]['default'] = true;
        $this->expectExceptionMessage('completion_evidence_default_option');
        CompletionPollEvidence::verify($events, 'default-off', 'worker', 'fixture');
    }

    public function testMissingResultReleasesCapacityWithoutDisablingTheCapability(): void
    {
        $events = self::capacity();
        self::assertTrue(CompletionPollEvidence::verify($events, 'capacity', 'worker', 'fixture')['missing_key_released_reservation']);
        $events[1]['answer']['workflow_poll'] = ['leased' => false];
        $this->expectExceptionMessage('completion_evidence_missing_key_capacity');
        CompletionPollEvidence::verify($events, 'capacity', 'worker', 'fixture');
    }

    public function testLostPrefetchKeepsBackoffAndPublishesParentBeforeOriginalRecovery(): void
    {
        $events = self::prefetch();
        self::assertTrue(CompletionPollEvidence::verify($events, 'prefetch-loss', 'worker', 'fixture')['original_poll_id_recovered']);
        $events = array_values(array_filter($events, static fn (array $event): bool => $event['kind'] !== 'poll_retry'));
        $this->expectExceptionMessage('completion_evidence_prefetch_recovery_policy');
        CompletionPollEvidence::verify($events, 'prefetch-loss', 'worker', 'fixture');
    }

    private static function capacity(): array
    {
        $events = [];
        foreach ([['workflow', 'activity_poll', 'activity'], ['activity', 'workflow_poll', 'workflow'],
            ['workflow', 'workflow_poll', 'workflow']] as $index => [$parent, $member, $kind]) {
            $task = array_replace(self::task($kind), ['task_id' => 'task-'.$index, 'workflow_id' => 'fixture-'.$index]);
            $body = ['lease_owner' => 'worker', 'workflow_task_attempt' => 1,
                $member => ['worker_id' => 'worker', 'poll_request_id' => 'carrier-'.$index]];
            if ($index === 0) $body['workflow_poll'] = ['worker_id' => 'worker', 'poll_request_id' => 'omitted'];
            array_push($events,
                self::event('request', $index, ['request' => 'parent-'.$index, 'path' => '/api/worker/'.$parent.'-tasks/parent/complete',
                    'body' => CompletionPollTrace::request($body)]),
                self::event('upstream', $index + 0.1, ['request' => 'parent-'.$index, 'status' => 200,
                    'answer' => CompletionPollTrace::answer(['server_capabilities' => array_fill_keys(CompletionPollEvidence::CAPABILITIES, true),
                        $member => ['leased' => true, 'answer' => self::grant($task)]])]),
                self::event('delivered', $index + 0.2, ['request' => 'parent-'.$index, 'status' => 200]),
                self::event($kind === 'workflow' ? 'callback' : 'activity', $index + 0.3,
                    $kind === 'workflow' ? ['workflow_id' => $task['workflow_id']] : ['task_id' => $task['task_id']]));
        }
        return $events;
    }

    private static function prefetch(): array
    {
        $parent = array_replace(self::task('workflow'), ['task_id' => 'task-2', 'workflow_id' => 'fixture-2']);
        return [
            self::event('request', 0, ['request' => 'first', 'path' => '/api/worker/workflow-tasks/poll', 'body' => []]),
            self::event('upstream', 0.01, ['request' => 'first', 'status' => 200, 'answer' => CompletionPollTrace::answer(self::grant($parent))]),
            self::event('delivered', 0.02, ['request' => 'first', 'status' => 200]),
            self::event('request', 0.1, ['request' => 'prefetch', 'path' => '/api/worker/workflow-tasks/poll',
                'body' => ['worker_id' => 'worker', 'poll_request_id' => 'original-id']]),
            self::event('upstream', 0.2, ['request' => 'prefetch', 'status' => 200, 'answer' => CompletionPollTrace::answer(self::grant(self::task('workflow')))]),
            self::event('fault', 0.21, ['request' => 'prefetch', 'mode' => 'lost_prefetch_answer']),
            self::event('delivered', 0.3, ['request' => 'prefetch', 'status' => 502]),
            self::event('request', 0.31, ['request' => 'parent', 'path' => '/api/worker/workflow-tasks/task-2/complete', 'body' => []]),
            self::event('upstream', 0.32, ['request' => 'parent', 'status' => 200, 'answer' => ['recorded' => true]]),
            self::event('delivered', 0.33, ['request' => 'parent', 'status' => 200]),
            self::event('poll_retry', 0.34, ['task_kind' => 'workflow', 'attempt' => 1, 'delay' => 0.1]),
            self::event('request', 0.45, ['request' => 'recover', 'path' => '/api/worker/workflow-tasks/poll',
                'body' => ['worker_id' => 'worker', 'poll_request_id' => 'original-id']]),
            self::event('upstream', 0.5, ['request' => 'recover', 'status' => 200, 'answer' => CompletionPollTrace::answer(self::grant(self::task('workflow')))]),
            self::event('delivered', 0.6, ['request' => 'recover', 'status' => 200]),
            self::event('callback', 0.7, ['workflow_id' => 'fixture-3']),
        ];
    }

    /** Fixed observation vectors only; these implement no Server or installed acceptance. */
    private static function loss(string $member, string $parent): array
    {
        $kind = $member === 'activity_poll' ? 'activity' : 'workflow';
        $body = ['lease_owner' => 'worker', 'workflow_task_attempt' => 1,
            $member => ['worker_id' => 'worker', 'poll_request_id' => 'original-id', 'timeout_seconds' => 0]];
        $path = '/api/worker/'.$parent.'-tasks/parent/complete';
        return [
            self::event('request', 0, ['request' => 'parent', 'path' => $path, 'body' => CompletionPollTrace::request($body)]),
            self::event('upstream', 0.1, ['request' => 'parent', 'status' => 200, 'answer' => CompletionPollTrace::answer([
                $member => ['leased' => true, 'answer' => self::grant(self::task($kind))]])]),
            self::event('fault', 0.2, ['request' => 'parent', 'mode' => 'lost_carried_answer', 'member' => $member]),
            self::event('delivered', 0.3, ['request' => 'parent', 'status' => 502]),
            self::event('request', 0.4, ['request' => 'retry', 'path' => $path, 'body' => CompletionPollTrace::request($body)]),
            self::event('upstream', 0.5, ['request' => 'retry', 'status' => 200, 'answer' => ['recorded' => false, 'reason' => 'already_completed']]),
            self::event('delivered', 0.6, ['request' => 'retry', 'status' => 200]),
            self::event('request', 0.7, ['request' => 'recover', 'path' => '/api/worker/'.$kind.'-tasks/poll',
                'body' => ['worker_id' => 'worker', 'poll_request_id' => 'original-id']]),
            self::event('upstream', 0.8, ['request' => 'recover', 'status' => 200, 'answer' => CompletionPollTrace::answer(self::grant(self::task($kind)))]),
            self::event('delivered', 0.9, ['request' => 'recover', 'status' => 200]),
            self::event($kind === 'workflow' ? 'callback' : 'activity', 1, $kind === 'workflow' ? ['workflow_id' => 'fixture-3'] : ['task_id' => 'task-3']),
        ];
    }

    private static function transit(): array
    {
        $task = self::task('workflow');
        return [
            self::event('request', 0, ['request' => 'parent', 'path' => '/api/worker/workflow-tasks/parent/complete', 'body' => []]),
            self::event('fault_begin', 0.1, ['request' => 'parent', 'mode' => 'completion_transit']),
            self::event('fault_end', 22.1, ['request' => 'parent', 'mode' => 'completion_transit']),
            self::event('upstream', 22.2, ['request' => 'parent', 'status' => 200,
                'answer' => CompletionPollTrace::answer(['workflow_poll' => ['leased' => true, 'answer' => self::grant($task)]])]),
            self::event('delivered', 22.3, ['request' => 'parent', 'status' => 200]),
            self::event('request', 22.4, ['request' => 'confirm', 'path' => '/api/worker/workflow-tasks/task-3/heartbeat',
                'body' => ['lease_owner' => 'worker', 'workflow_task_attempt' => 1]]),
            self::event('upstream', 22.5, ['request' => 'confirm', 'status' => 200, 'answer' => ['task_id' => 'task-3',
                'lease_owner' => 'worker', 'workflow_task_attempt' => 1, 'renewed' => true]]),
            self::event('delivered', 22.6, ['request' => 'confirm', 'status' => 200]),
            self::event('callback', 22.7, ['workflow_id' => 'fixture-3']),
        ];
    }

    private static function expiry(): array
    {
        return [
            self::event('request', 0, ['request' => 'parent', 'path' => '/api/worker/activity-tasks/parent/complete', 'body' => []]),
            self::event('upstream', 0.1, ['request' => 'parent', 'status' => 200,
                'answer' => CompletionPollTrace::answer(['workflow_poll' => ['leased' => true, 'answer' => self::grant(self::task('workflow'))]])]),
            self::event('fault_begin', 0.2, ['request' => 'parent', 'mode' => 'expiry']),
            self::event('delivered', 30.4, ['request' => 'parent', 'status' => 200]),
            self::event('request', 30.5, ['request' => 'confirm', 'path' => '/api/worker/workflow-tasks/task-3/heartbeat',
                'body' => ['lease_owner' => 'worker', 'workflow_task_attempt' => 1]]),
            self::event('upstream', 30.6, ['request' => 'confirm', 'status' => 409,
                'answer' => ['reason' => 'lease_expired', 'task_id' => 'task-3', 'workflow_task_attempt' => 1]]),
            self::event('delivered', 30.7, ['request' => 'confirm', 'status' => 409]),
            self::event('worker_error', 30.8, ['class' => 'DurableWorkflow\\Exception\\ServerException']),
        ];
    }

    private static function task(string $kind): array
    {
        $task = ['task_id' => 'task-3', 'task_kind' => $kind, 'workflow_id' => 'fixture-3', 'run_id' => 'run-3',
            'lease_owner' => 'worker', 'lease_expires_at' => '2026-11-07T08:00:30.000000Z', 'history_events' => []];
        return $task + ($kind === 'workflow' ? ['workflow_task_attempt' => 1] : ['activity_attempt_id' => 'activity-attempt-1', 'attempt_number' => 1]);
    }

    private static function grant(array $task): array
    {
        return ['task' => $task, 'poll_status' => 'leased', 'server_time' => '2026-11-07T08:00:00.000000Z',
            'server_capabilities' => array_fill_keys(CompletionPollEvidence::CAPABILITIES, true)];
    }

    private static function buffer(): array
    {
        $two = array_replace(self::task('workflow'), ['task_id' => 'task-2', 'workflow_id' => 'fixture-2']);
        $three = self::task('workflow');
        return [
            self::event('request', 0, ['request' => 'original', 'path' => '/api/worker/workflow-tasks/poll',
                'body' => ['worker_id' => 'worker', 'timeout_seconds' => 0, 'poll_request_id' => 'original-2']]),
            self::event('upstream', 0.01, ['request' => 'original', 'status' => 200, 'answer' => CompletionPollTrace::answer(self::grant($two))]),
            self::event('delivered', 0.02, ['request' => 'original', 'status' => 200]),
            self::event('request', 0.1, ['request' => 'prefetch', 'path' => '/api/worker/workflow-tasks/poll',
                'body' => ['worker_id' => 'worker', 'timeout_seconds' => 0, 'poll_request_id' => 'prefetch-3']]),
            self::event('upstream', 0.11, ['request' => 'prefetch', 'status' => 200, 'answer' => CompletionPollTrace::answer(self::grant($three))]),
            self::event('delivered', 0.12, ['request' => 'prefetch', 'status' => 200]),
            self::event('request', 0.2, ['request' => 'registration-heartbeat', 'path' => '/api/worker/heartbeat', 'body' => ['worker_id' => 'worker']]),
            self::event('upstream', 0.21, ['request' => 'registration-heartbeat', 'status' => 200, 'answer' => []]),
            self::event('fault_begin', 0.22, ['request' => 'registration-heartbeat', 'mode' => 'buffer_age']),
            self::event('fault_end', 1.5, ['request' => 'registration-heartbeat', 'mode' => 'buffer_age']),
            self::event('delivered', 1.51, ['request' => 'registration-heartbeat', 'status' => 200]),
            self::event('request', 1.6, ['request' => 'parent', 'path' => '/api/worker/workflow-tasks/task-2/complete',
                'body' => ['lease_owner' => 'worker', 'workflow_task_attempt' => 1]]),
            self::event('upstream', 1.7, ['request' => 'parent', 'status' => 200, 'answer' => []]),
            self::event('request', 1.8, ['request' => 'confirm', 'path' => '/api/worker/workflow-tasks/task-3/heartbeat',
                'body' => ['lease_owner' => 'worker', 'workflow_task_attempt' => 1]]),
            self::event('upstream', 1.81, ['request' => 'confirm', 'status' => 200, 'answer' => ['renewed' => true,
                'task_id' => 'task-3', 'lease_owner' => 'worker', 'workflow_task_attempt' => 1]]),
            self::event('delivered', 1.82, ['request' => 'confirm', 'status' => 200]),
            self::event('callback', 1.9, ['workflow_id' => 'fixture-3']),
            self::event('local', 1.95, ['index' => 3]),
            self::event('delivered', 2, ['request' => 'parent', 'status' => 200]),
            self::event('request', 2.1, ['request' => 'child', 'path' => '/api/worker/workflow-tasks/task-3/complete',
                'body' => ['lease_owner' => 'worker', 'workflow_task_attempt' => 1]]),
            self::event('upstream', 2.11, ['request' => 'child', 'status' => 200, 'answer' => []]),
            self::event('delivered', 2.12, ['request' => 'child', 'status' => 200]),
        ];
    }

    private static function event(string $kind, float $seconds, array $fields): array
    {
        return ['kind' => $kind, 'at' => (int) ($seconds * 1e9)] + $fields;
    }
}
