<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Worker\CancellationScopeDeliveryReceipt;
use DurableWorkflow\Worker\CancellationScopeHistory;
use DurableWorkflow\Worker\CommittedCancellationScopeHistory;
use DurableWorkflow\Worker\WorkflowClaimAborted;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class CancellationScopeActivityProjectionTest extends TestCase
{
    #[DataProvider('policies')]
    public function test_populated_projection_is_proved_from_the_original_prefix_without_granting_execution(string $policy, bool $local): void
    {
        [$history, $receipt, $expected] = self::fixture($policy, $local);
        $prepared = CancellationScopeDeliveryReceipt::fromCanonicalHistory(self::preparedReceipt($receipt), array_slice($history, 0, -1), $expected, false);
        $delivered = CancellationScopeDeliveryReceipt::fromCanonicalHistory($receipt, $history, $expected, true);
        $delivered->assertOriginalPreparation($prepared);
        self::assertSame($receipt['activity_members'], $delivered->activityMembers);
        self::assertCount(1, $delivered->activityMembers);
        self::assertSame('original-activity', $delivered->activityMembers[0]['activity_execution_id']);
        self::assertSame($prepared->preparationHistoryEventId, $delivered->preparationHistoryEventId);
        self::assertSame($prepared->context->toArray(), $delivered->context->toArray());
        self::assertEquals($prepared->authorityDeadline, $delivered->authorityDeadline);
        self::assertNull($prepared->deliveryHistoryEventId);
        self::assertSame($history[8]['id'], $delivered->deliveryHistoryEventId);
        // The default execution parser continues refusing populated projections.
        $this->expectException(WorkflowClaimAborted::class);
        new CommittedCancellationScopeHistory($history, $expected['workflow_run_id'], $expected['workflow_instance_id'],
            new CancellationScopeHistory($history, $expected['workflow_run_id']));
    }

    public static function policies(): array
    {
        return ['remote try' => ['try_cancel', false], 'remote wait' => ['wait_cancellation_completed', false],
            'remote abandon' => ['abandon', false], 'prepared try' => ['try_cancel', true],
            'prepared wait' => ['wait_cancellation_completed', true]];
    }

    #[DataProvider('faults')]
    public function test_changed_or_incomplete_inventory_cannot_become_a_verified_receipt(string $fault): void
    {
        [$history, $receipt, $expected] = self::fixture();
        $target = &$history[4]['payload'];
        $members = &$history[7]['payload']['activity_members'];
        switch ($fault) {
            case 'omitted member': $members = []; $receipt['activity_members'] = []; break;
            case 'borrowed sibling': $members[0]['activity_execution_id'] = 'sibling-activity'; $receipt['activity_members'] = $members; break;
            case 'changed hash': $members[0]['descriptor_hash'] = str_repeat('b', 64); $receipt['activity_members'] = $members; break;
            case 'changed policy': $target['activity']['cancellation_policy'] = 'try_cancel'; break;
            case 'changed local mode': $target['local_activity'] = true; $target['execution_mode'] = 'prepared_local'; break;
            case 'changed execution deadline': $target['activity']['schedule_to_close_deadline_at'] = '2026-10-04T00:00:29Z'; break;
            case 'changed event identity': $history[4]['id'] = 'substituted-schedule'; break;
            case 'changed execution identity': $target['activity']['id'] = 'another'; break;
            case 'duplicate execution identity': $history[5]['payload']['activity_execution_id'] = 'original-activity'; $history[5]['payload']['activity']['id'] = 'original-activity'; break;
            case 'duplicate authored sequence': $history[5]['payload']['sequence'] = 3; break;
            case 'missing descriptor': unset($target['activity']); break;
            case 'invalid policy': $target['activity']['cancellation_policy'] = 'terminal'; break;
            case 'invalid local flag': $target['local_activity'] = 1; break;
            case 'invalid mode': $target['execution_mode'] = []; break;
            case 'invalid deadline': $target['activity']['schedule_to_close_deadline_at'] = []; break;
            case 'missing member field': unset($members[0]['descriptor_hash']); $receipt['activity_members'] = $members; break;
            case 'unexpected member field': $members[0]['borrowed'] = true; $receipt['activity_members'] = $members; break;
            case 'non-list projection': $members = ['member' => $members[0]]; $receipt['activity_members'] = $members; break;
            case 'duplicate member': $members[] = $members[0]; $receipt['activity_members'] = $members; break;
            case 'uppercase hash': $members[0]['descriptor_hash'] = strtoupper($members[0]['descriptor_hash']); $receipt['activity_members'] = $members; break;
            case 'wrong acknowledgement': $receipt['activity_members'] = []; break;
            case 'flat scope changes': $target['cancellation_scope_id'] = $history[2]['payload']['scope_id']; break;
            case 'nested scope changes': $target['activity']['cancellation_scope_id'] = $history[2]['payload']['scope_id']; break;
            case 'unknown cleanup proof': $target['local_preparation']['cancellation_cleanup']['scope_id'] = $expected['scope_id']; break;
            case 'wrong boundary kind': $history[7]['payload']['call_kind'] = 'timer'; $history[8]['payload']['call_kind'] = 'timer'; $receipt['call_kind'] = 'timer'; $expected['call_kind'] = 'timer'; break;
        }
        $this->expectException(WorkflowClaimAborted::class);
        CancellationScopeDeliveryReceipt::fromCanonicalHistory($receipt, $history, $expected, true);
    }

    public static function faults(): array
    {
        return array_map(static fn (string $fault): array => [$fault], ['omitted member', 'borrowed sibling', 'changed hash',
            'changed policy', 'changed local mode', 'changed execution deadline', 'changed event identity', 'changed execution identity',
            'duplicate execution identity', 'duplicate authored sequence', 'missing descriptor', 'invalid policy', 'invalid local flag',
            'invalid mode', 'invalid deadline', 'missing member field', 'unexpected member field', 'non-list projection',
            'duplicate member', 'uppercase hash', 'wrong acknowledgement', 'flat scope changes', 'nested scope changes',
            'unknown cleanup proof', 'wrong boundary kind']);
    }

    public function test_a_later_sibling_and_completed_target_do_not_rewrite_the_original_inventory(): void
    {
        [$history, $receipt, $expected] = self::fixture();
        $prepared = CancellationScopeDeliveryReceipt::fromCanonicalHistory(self::preparedReceipt($receipt), array_slice($history, 0, -1), $expected, false);
        // The frozen inventory includes admitted work even if it has subsequently resolved.
        $resolved = self::event('activity-result', 8, 'ActivityCompleted', ['sequence' => 3,
            'activity_execution_id' => 'original-activity', 'cancellation_scope_id' => $expected['scope_id']]);
        array_splice($history, 7, 0, [$resolved]);
        foreach ($history as $index => &$event) { $event['sequence'] = $index + 1; }
        unset($event);
        $later = $history[5]; $later['id'] = 'later-sibling-admission'; $later['sequence'] = 11;
        $later['payload']['sequence'] = 5; $later['payload']['activity_execution_id'] = 'later-sibling';
        $later['payload']['activity']['id'] = 'later-sibling';
        $history[] = $later;
        $receipt['history_event_id'] = $history[9]['id'];
        $proved = CancellationScopeDeliveryReceipt::fromCanonicalHistory($receipt, $history, $expected, true);
        $proved->assertOriginalPreparation($prepared);
        self::assertSame($prepared->activityMembers, $proved->activityMembers);
        self::assertCount(1, $proved->activityMembers);
    }

    public function test_matching_new_history_and_response_cannot_substitute_original_members(): void
    {
        [$history, $receipt, $expected] = self::fixture();
        $original = CancellationScopeDeliveryReceipt::fromCanonicalHistory(self::preparedReceipt($receipt), array_slice($history, 0, -1), $expected, false);
        $history[4]['payload']['activity_execution_id'] = 'new-member';
        $history[4]['payload']['activity']['id'] = 'new-member';
        // Native's scalar hash excludes the execution ID because membership carries that ID separately.
        $history[7]['payload']['activity_members'][0]['activity_execution_id'] = 'new-member';
        $receipt['activity_members'] = $history[7]['payload']['activity_members'];
        $substituted = CancellationScopeDeliveryReceipt::fromCanonicalHistory($receipt, $history, $expected, true);
        $this->expectException(WorkflowClaimAborted::class);
        $substituted->assertOriginalPreparation($original);
    }

    public function test_database_object_key_order_does_not_change_member_identity(): void
    {
        [$history, $receipt, $expected] = self::fixture();
        $receipt['activity_members'][0] = array_reverse($receipt['activity_members'][0], true);
        $proved = CancellationScopeDeliveryReceipt::fromCanonicalHistory($receipt, $history, $expected, true);
        self::assertSame($history[7]['payload']['activity_members'], $proved->activityMembers);
    }

    public function test_all_original_scope_activities_are_frozen_in_canonical_admission_order(): void
    {
        [$history, $receipt, $expected] = self::fixture();
        $second = $history[4]; $second['id'] = 'second-target-schedule';
        $second['payload']['sequence'] = 5; $second['payload']['activity_execution_id'] = 'second-target';
        $second['payload']['activity']['id'] = 'second-target';
        array_splice($history, 6, 0, [$second]);
        foreach ($history as $index => &$event) { $event['sequence'] = $index + 1; }
        unset($event);
        $member = ['sequence' => 5, 'activity_execution_id' => 'second-target', 'descriptor_hash' => hash('sha256', json_encode([
            $expected['scope_id'], 'second-target-schedule', 'wait_cancellation_completed', false, null, '2026-10-04T00:02:00.123456Z',
        ], JSON_THROW_ON_ERROR))];
        $history[8]['payload']['activity_members'][] = $member;
        $receipt['activity_members'][] = $member;
        $proved = CancellationScopeDeliveryReceipt::fromCanonicalHistory($receipt, $history, $expected, true);
        self::assertSame(['original-activity', 'second-target'], array_column($proved->activityMembers, 'activity_execution_id'));
        // A matching response and preparation still cannot reverse canonical admission order.
        $history[8]['payload']['activity_members'] = array_reverse($history[8]['payload']['activity_members']);
        $receipt['activity_members'] = $history[8]['payload']['activity_members'];
        $this->expectException(WorkflowClaimAborted::class);
        CancellationScopeDeliveryReceipt::fromCanonicalHistory($receipt, $history, $expected, true);
    }

    public function test_historical_omitted_scalar_defaults_keep_the_native_projection_hash(): void
    {
        [$history, $receipt, $expected] = self::fixture('try_cancel');
        unset($history[4]['payload']['activity']['cancellation_policy'], $history[4]['payload']['local_activity'],
            $history[4]['payload']['execution_mode']);
        $proved = CancellationScopeDeliveryReceipt::fromCanonicalHistory($receipt, $history, $expected, true);
        self::assertSame($receipt['activity_members'], $proved->activityMembers);
    }

    /** @return array{array, array, array} */
    private static function fixture(string $policy = 'wait_cancellation_completed', bool $local = false): array
    {
        $fixture = json_decode(file_get_contents(__DIR__.'/fixtures/committed-scope-delivery.json'), true, flags: JSON_THROW_ON_ERROR)['unshielded'];
        $history = array_slice($fixture['history'], 0, 7);
        $scopeId = $fixture['scope_id'];
        $schedule = self::event('original-schedule', 5, 'ActivityScheduled', ['sequence' => 3,
            'cancellation_scope_id' => $scopeId, 'activity_execution_id' => 'original-activity',
            'local_activity' => $local, 'execution_mode' => $local ? 'prepared_local' : null,
            'activity' => ['id' => 'original-activity', 'cancellation_scope_id' => $scopeId,
                'cancellation_policy' => $policy, 'schedule_to_close_deadline_at' => '2026-10-04T00:02:00.123456Z']]);
        $sibling = self::event('sibling-schedule', 6, 'ActivityScheduled', ['sequence' => 4,
            'cancellation_scope_id' => $history[2]['payload']['scope_id'], 'activity_execution_id' => 'sibling-activity',
            'activity' => ['id' => 'sibling-activity', 'cancellation_scope_id' => $history[2]['payload']['scope_id']]]);
        array_splice($history, 4, 0, [$schedule, $sibling]);
        foreach ($history as $index => &$event) { $event['sequence'] = $index + 1; }
        unset($event);
        $member = ['sequence' => 3, 'activity_execution_id' => 'original-activity', 'descriptor_hash' => hash('sha256', json_encode([
            $scopeId, 'original-schedule', $policy, $local, $local ? 'prepared_local' : null, '2026-10-04T00:02:00.123456Z',
        ], JSON_THROW_ON_ERROR))];
        foreach ([7, 8] as $index) { $history[$index]['payload']['call_kind'] = 'activity'; }
        $history[7]['payload']['activity_members'] = [$member];
        $body = array_intersect_key($history[7]['payload'], array_flip(['scope_id', 'request_id', 'sequence', 'call_kind',
            'sequence_span', 'operation_sequence', 'operation_sequence_span']));
        $expected = ['namespace' => 'sdk-scope-fixture', 'task_id' => 'task/one', 'workflow_run_id' => $fixture['task']['run_id'],
            'workflow_instance_id' => $fixture['task']['workflow_id'], 'lease_owner' => 'original-owner', 'workflow_task_attempt' => 4, ...$body];
        $receipt = ['prepared' => true, 'delivered' => true, 'claim_released' => false, 'created_task_ids' => [], 'reason' => null,
            'task_id' => 'task/one', 'workflow_run_id' => $fixture['task']['run_id'], 'lease_owner' => 'original-owner', 'workflow_task_attempt' => 4,
            'history_event_id' => $history[8]['id'], 'preparation_history_event_id' => $history[7]['id'], 'history_refresh_page_token' => 'opaque-start',
            'cancellation' => $history[7]['payload']['cancellation'], 'authority_deadline_at' => $history[7]['payload']['authority_deadline_at'],
            'activity_members' => [$member], 'timer_members' => [], 'wait_members' => [], 'child_members' => [], ...$body];

        return [$history, $receipt, $expected];
    }

    private static function preparedReceipt(array $receipt): array
    {
        return array_replace($receipt, ['delivered' => false, 'history_event_id' => $receipt['preparation_history_event_id']]);
    }

    private static function event(string $id, int $sequence, string $kind, array $payload): array
    {
        return ['id' => $id, 'sequence' => $sequence, 'namespace' => 'sdk-scope-fixture', 'event_type' => $kind,
            'timestamp' => '2026-10-04T00:00:00.123456Z', 'payload' => $payload];
    }
}
