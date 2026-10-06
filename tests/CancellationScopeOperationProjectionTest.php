<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Worker\CancellationScopeDeliveryReceipt;
use DurableWorkflow\Worker\CancellationScopeHistory;
use DurableWorkflow\Worker\CommittedCancellationScopeHistory;
use DurableWorkflow\Worker\ScopedCancellationContext;
use DurableWorkflow\Worker\WorkflowClaimAborted;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class CancellationScopeOperationProjectionTest extends TestCase
{
    #[DataProvider('scenarios')]
    public function test_native_producer_projections_are_verified_at_their_original_boundary(string $scenario): void
    {
        [$history, $receipt, $expected] = self::fixture($scenario);
        $original = self::prove($history, $receipt, $expected, false);
        $delivered = self::prove($history, $receipt, $expected, true);
        $delivered->assertOriginalPreparation($original);
        self::assertSame(['timer-plain', 'signal-timer-id', 'condition-timer-id'], array_column($delivered->timerMembers, 'timer_id'));
        self::assertSame(['signal', 'condition', 'condition'], array_column($delivered->waitMembers, 'kind'));
        self::assertNull($delivered->waitMembers[2]['timer_id']);
        self::assertSame(['try_cancel', 'wait_cancellation_completed', 'abandon'], array_column($delivered->childMembers, 'cancellation_policy'));
        self::assertSame('child-continued-run', $delivered->childMembers[1]['child_workflow_run_id']);
        self::assertSame($original->preparationHistoryEventId, $delivered->preparationHistoryEventId);
        self::assertEquals($original->authorityDeadline, $delivered->authorityDeadline);
        self::assertSame($history, $delivered->history);
        $hasDescendants = in_array($scenario, ['descendants', 'competing'], true);
        self::assertSame($hasDescendants ? ['desc-child', 'desc-grandchild'] : [], array_column($delivered->descendantMembers, 'scope_id'));
        if ($hasDescendants) {
            $child = $delivered->descendantMembers[0];
            self::assertSame('descendant-timer-id', $child['timer_members'][0]['timer_id']);
            self::assertSame($scenario === 'competing' ? '2026-10-04T00:00:20.123456Z' : '2026-10-04T00:00:30.123456Z', $child['authority_deadline_at']);
            self::assertSame($scenario === 'competing' ? 'original-child-conflict' : 'inherited-child-accepted', $child['propagation_history_event_id']);
            self::assertSame($child['cancellation']['root_context'], $delivered->descendantMembers[1]['cancellation']['root_context']);
        }
    }

    public static function scenarios(): array { return [['operations'], ['groups'], ['descendants'], ['competing']]; }

    #[DataProvider('admittedBoundaries')]
    public function test_an_already_admitted_operation_keeps_its_original_kind_and_position(string $kind, int $sequence): void
    {
        [$history, $receipt, $expected] = self::fixture('operations');
        foreach ([count($history)-2, count($history)-1] as $index) {
            $history[$index]['payload']['sequence'] = $sequence; $history[$index]['payload']['call_kind'] = $kind;
        }
        $receipt['sequence'] = $expected['sequence'] = $sequence; $receipt['call_kind'] = $expected['call_kind'] = $kind;
        $proved = self::prove($history, $receipt, $expected, true);
        self::assertSame($sequence, $proved->boundary->sequence);
        self::assertSame($kind, $proved->boundary->callKind);
    }

    public static function admittedBoundaries(): array { return [['timer', 10], ['condition', 12], ['child', 14]]; }

    public function test_a_replacement_claim_keeps_all_original_members_and_descendant_deadlines(): void
    {
        [$history, $receipt, $expected] = self::fixture('competing');
        $original = self::prove($history, $receipt, $expected, false);
        $replacement = ['task_id'=>'replacement-task', 'lease_owner'=>'replacement-owner', 'workflow_task_attempt'=>17];
        $proved = self::prove($history, array_replace($receipt, $replacement), array_replace($expected, $replacement), true);
        $proved->assertOriginalPreparation($original);
        self::assertSame($original->preparationHistoryEventId, $proved->preparationHistoryEventId);
        self::assertSame($original->context->toArray(), $proved->context->toArray());
        self::assertSame($original->descendantMembers, $proved->descendantMembers);
    }

    #[DataProvider('substitutedMembers')]
    public function test_consistent_new_history_cannot_substitute_a_previously_verified_preparation(string $field): void
    {
        [$history, $receipt, $expected] = self::fixture('operations');
        $original = self::prove($history, $receipt, $expected, false);
        foreach ($history as &$event) {
            if ($field === 'timer_members' && $event['id'] === 'plain-timer') { $event['payload']['delay_seconds'] = 3601; }
            if ($field === 'wait_members' && $event['id'] === 'signal-wait') { $event['payload']['signal_name'] = 'new-inbox'; }
            if ($field === 'child_members' && $event['id'] === 'child-0') { $event['payload']['child_workflow_run_id'] = 'substituted-child-run'; }
        } unset($event);
        $scope = $expected['scope_id'];
        $values = match ($field) {
            'timer_members' => [$scope, 'plain-timer', 10, 'timer-plain', 3601, '2026-10-04T01:00:00.123456Z', null, null, null, null, []],
            'wait_members' => [$scope, 'signal-wait', 'signal', 11, 'signal-wait-id', 'signal-timer', 'signal-timer-id', 30, 'new-inbox', null, null, null, []],
            default => [$scope, 'child-0', 14, 'child-call-0', 'child-instance-0', 'substituted-child-run', 'try_cancel', 'request_cancel', 'python-child', null, []],
        };
        $members = $receipt[$field]; $members[0]['descriptor_hash'] = hash('sha256', json_encode($values, JSON_THROW_ON_ERROR));
        if ($field === 'child_members') { $members[0]['child_workflow_run_id'] = 'substituted-child-run'; }
        $receipt[$field] = $history[count($history)-2]['payload'][$field] = $members;
        $substituted = self::prove($history, $receipt, $expected, true);
        $this->expectException(WorkflowClaimAborted::class);
        $substituted->assertOriginalPreparation($original);
    }

    public static function substitutedMembers(): array { return [['timer_members'], ['wait_members'], ['child_members']]; }

    #[DataProvider('scenarios')]
    public function test_receipt_inspection_does_not_enable_worker_scope_execution(string $scenario): void
    {
        [$history, , $expected] = self::fixture($scenario);
        $this->expectException(WorkflowClaimAborted::class);
        $this->expectExceptionMessage('cancellation_scope_execution_not_supported');
        new CommittedCancellationScopeHistory($history, $expected['workflow_run_id'], $expected['workflow_instance_id'],
            new CancellationScopeHistory($history, $expected['workflow_run_id']));
    }

    public function test_future_admissions_and_child_continuations_cannot_retarget_the_original_snapshot(): void
    {
        [$history, $receipt, $expected] = self::fixture('descendants');
        $original = self::prove($history, $receipt, $expected, false);
        $delivered = $history[array_key_last($history)];
        $context = ScopedCancellationContext::fromArray($delivered['payload']['cancellation']);
        $late = self::find($history, 'child-continued-before-preparation');
        $late['id'] = 'continued-after-delivery'; $late['payload']['child_workflow_run_id'] = 'future-child-run';
        $late['sequence'] = count($history)+1; $history[] = $late;
        $late = self::find($history, 'plain-timer');
        $late['id'] = 'future-timer'; $late['payload']['sequence'] = 21; $late['payload']['timer_id'] = 'future-timer-id';
        // A timer admitted in this delivered scope must be shielded cleanup,
        // using the original receipt and a fire time inside its fixed budget.
        $late['timestamp'] = '2026-10-04T00:00:10.123456Z';
        $late['payload']['delay_seconds'] = 1; $late['payload']['fire_at'] = '2026-10-04T00:00:11.123456Z';
        $late['payload']['cancellation_cleanup'] = [
            'scope_id' => $context->scopeId, 'operation_scope_id' => $context->scopeId,
            'request_id' => $context->requestId, 'root_request_id' => $context->rootRequestId,
            'delivery_history_event_id' => $delivered['id'],
            'preparation_history_event_id' => $delivered['payload']['preparation_history_event_id'],
            'cleanup_deadline_at' => $context->deadline()->format('Y-m-d\TH:i:s.u\Z'),
            'authority_deadline_at' => $delivered['payload']['authority_deadline_at'],
        ];
        $late['sequence'] = count($history)+1; $history[] = $late;
        $late = self::find($history, 'desc-child-opened');
        $late['id'] = 'future-scope-opened'; $late['payload']['scope_id'] = 'future-scope'; $late['payload']['sequence'] = 22;
        $late['sequence'] = count($history)+1; $history[] = $late;
        $proved = self::prove($history, $receipt, $expected, true);
        $proved->assertOriginalPreparation($original);
        self::assertSame($original->childMembers, $proved->childMembers);
        self::assertSame($original->timerMembers, $proved->timerMembers);
        self::assertSame($original->descendantMembers, $proved->descendantMembers);
    }

    public function test_database_object_key_order_cannot_change_the_original_operation_snapshot(): void
    {
        [$history, $receipt, $expected] = self::fixture('competing');
        foreach (['timer_members', 'wait_members', 'child_members'] as $field) {
            $receipt[$field] = array_map(static fn (array $member): array => array_reverse($member, true), $receipt[$field]);
        }
        $prepared = count($history)-2;
        $history[$prepared]['payload']['descendant_members'] = array_map(static fn (array $member): array => array_reverse($member, true),
            $history[$prepared]['payload']['descendant_members']);
        $proved = self::prove($history, $receipt, $expected, true);
        self::assertSame($receipt['timer_members'][0]['timer_id'], $proved->timerMembers[0]['timer_id']);
        self::assertSame('original-child-conflict', $proved->descendantMembers[0]['propagation_history_event_id']);
    }

    #[DataProvider('projectionFaults')]
    public function test_matching_receipt_and_preparation_cannot_change_the_canonical_admission(string $field, string $fault): void
    {
        [$history, $receipt, $expected] = self::fixture('operations');
        $members = $receipt[$field];
        switch ($fault) {
            case 'omit': array_shift($members); break;
            case 'order': $members = array_reverse($members); break;
            case 'duplicate': $members[] = $members[0]; break;
            case 'sequence': $members[0]['sequence'] = 19; break;
            case 'hash': $members[0]['descriptor_hash'] = str_repeat('0', 64); break;
            case 'extra': $members[0]['uncommitted'] = true; break;
            case 'identity': $members[0][$field === 'timer_members' ? 'timer_id' : ($field === 'wait_members' ? 'wait_id' : 'child_call_id')] = 'borrowed'; break;
            case 'timeout': $members[0]['timer_id'] = null; break;
            case 'kind': $members[0]['kind'] = 'condition'; break;
            case 'target': $members[0]['child_workflow_run_id'] = 'current-run-is-not-authority'; break;
            case 'policy': $members[0]['cancellation_policy'] = 'abandon'; break;
        }
        $receipt[$field] = $members; $history[count($history)-2]['payload'][$field] = $members;
        $this->expectException(WorkflowClaimAborted::class);
        self::prove($history, $receipt, $expected, true);
    }

    public static function projectionFaults(): iterable
    {
        foreach (['timer_members', 'wait_members', 'child_members'] as $field) {
            foreach (['omit', 'order', 'duplicate', 'sequence', 'hash', 'extra', 'identity'] as $fault) { yield $field.':'.$fault => [$field, $fault]; }
        }
        yield 'wrong timeout' => ['wait_members', 'timeout']; yield 'wrong kind' => ['wait_members', 'kind'];
        yield 'wrong child target' => ['child_members', 'target']; yield 'wrong child policy' => ['child_members', 'policy'];
    }

    #[DataProvider('historyFaults')]
    public function test_malformed_original_descriptors_and_timeout_edges_abort_cold_replay(string $id, array $changes): void
    {
        [$history, $receipt, $expected] = self::fixture('operations');
        foreach ($history as &$event) { if ($event['id'] === $id) { $event['payload'] = array_replace($event['payload'], $changes); } } unset($event);
        $this->expectException(WorkflowClaimAborted::class);
        self::prove($history, $receipt, $expected, true);
    }

    public static function historyFaults(): iterable
    {
        yield 'changed timer delay' => ['plain-timer', ['delay_seconds'=>3601]];
        yield 'invalid timer delay' => ['plain-timer', ['delay_seconds'=>-1]];
        yield 'invalid timer date' => ['plain-timer', ['fire_at'=>'2026-02-30T01:00:00.123456Z']];
        yield 'noncanonical timer date' => ['plain-timer', ['fire_at'=>'2026-10-04T01:00:00Z']];
        yield 'contradictory timer scope' => ['plain-timer', ['timer'=>['cancellation_scope_id'=>'root']]];
        yield 'wrong timeout kind' => ['signal-timer', ['timer_kind'=>'condition_timeout']];
        yield 'wrong timeout position' => ['signal-timer', ['sequence'=>19]];
        yield 'wrong timeout scope' => ['signal-timer', ['cancellation_scope_id'=>'root']];
        yield 'wrong wait predicate' => ['condition-wait', ['condition_definition_fingerprint'=>'predicate-v2']];
        yield 'wrong signal name' => ['signal-wait', ['signal_name'=>'another']];
        yield 'self child target' => ['child-0', ['child_workflow_run_id'=>'01m43xf68q7341x3zjapav5pse']];
        yield 'wrong continued call' => ['child-continued-before-preparation', ['child_call_id'=>'other-call']];
        yield 'wrong continued instance' => ['child-continued-before-preparation', ['child_workflow_instance_id'=>'other-instance']];
        yield 'wrong continued policy' => ['child-continued-before-preparation', ['cancellation_policy'=>'abandon']];
        yield 'wrong continued scope' => ['child-continued-before-preparation', ['cancellation_scope_id'=>'root']];
    }

    #[DataProvider('descendantFaults')]
    public function test_frozen_subtree_cannot_omit_replace_or_renew_original_descendant_authority(string $fault): void
    {
        [$history, $receipt, $expected] = self::fixture('competing');
        $index = count($history)-2; $members = $history[$index]['payload']['descendant_members'];
        switch ($fault) {
            case 'omit': array_pop($members); break;
            case 'order': $members = array_reverse($members); break;
            case 'duplicate': $members[] = $members[0]; break;
            case 'scope opening': $members[0]['scope_history_event_id'] = 'desc-shield-opened'; break;
            case 'shield': $members[0]['scope_id'] = 'desc-shield'; break;
            case 'parent': $members[0]['parent_scope_id'] = 'root'; break;
            case 'request event': $members[0]['request_history_event_id'] = 'original-child-conflict'; break;
            case 'propagation event': $members[0]['propagation_history_event_id'] = 'independent-child-accepted'; break;
            case 'deadline': $members[0]['authority_deadline_at'] = '2026-10-04T00:00:30.123456Z'; break;
            case 'shortened deadline': $members[0]['authority_deadline_at'] = '2026-10-04T00:00:19.123456Z'; break;
            case 'members': $members[0]['timer_members'] = []; break;
            case 'reason': $members[0]['cancellation']['root_context']['reason'] = 'substituted'; break;
            case 'missing conflict': $history = array_values(array_filter($history, static fn (array $event): bool => $event['id'] !== 'original-child-conflict')); --$index; break;
            case 'conflict accepted':
            case 'conflict incoming':
                foreach ($history as &$event) {
                    if ($event['id'] === 'original-child-conflict') {
                        $key = $fault === 'conflict accepted' ? 'accepted_cancellation' : 'incoming_cancellation';
                        $event['payload'][$key]['root_context']['reason'] = 'substituted';
                    }
                } unset($event); break;
            case 'conflict schema':
            case 'conflict run':
            case 'conflict reason':
                foreach ($history as &$event) {
                    if ($event['id'] === 'original-child-conflict') {
                        $key = match ($fault) { 'conflict schema' => 'schema', 'conflict run' => 'workflow_run_id', default => 'reason' };
                        $event['payload'][$key] = 'substituted';
                    }
                } unset($event); break;
        }
        $history[$index]['payload']['descendant_members'] = $members;
        $this->expectException(WorkflowClaimAborted::class);
        self::prove($history, $receipt, $expected, true);
    }

    public static function descendantFaults(): array
    {
        return array_map(static fn (string $fault): array => [$fault], ['omit', 'order', 'duplicate', 'scope opening', 'shield', 'parent',
            'request event', 'propagation event', 'deadline', 'shortened deadline', 'members', 'reason', 'missing conflict', 'conflict accepted', 'conflict incoming',
            'conflict schema', 'conflict run', 'conflict reason']);
    }

    /** @return array{array, array, array} */
    private static function fixture(string $scenario): array
    {
        $fixture = json_decode(file_get_contents(__DIR__.'/fixtures/committed-scope-operation-projections.json'), true, flags: JSON_THROW_ON_ERROR)[$scenario];
        $history = $fixture['history']; $prepared = $history[count($history)-2]; $delivered = $history[count($history)-1];
        $payload = $prepared['payload'];
        $body = array_intersect_key($payload, array_flip(['scope_id', 'request_id', 'sequence', 'call_kind', 'sequence_span', 'operation_sequence', 'operation_sequence_span']));
        $expected = ['namespace'=>'sdk-scope-fixture', 'task_id'=>'original-task', 'workflow_run_id'=>$fixture['task']['run_id'],
            'workflow_instance_id'=>$fixture['task']['workflow_id'], 'lease_owner'=>'original-owner', 'workflow_task_attempt'=>4, ...$body];
        $receipt = ['prepared'=>true, 'delivered'=>true, 'claim_released'=>false, 'created_task_ids'=>[], 'reason'=>null,
            ...array_diff_key($expected, array_flip(['namespace', 'workflow_instance_id'])),
            'preparation_history_event_id'=>$prepared['id'], 'history_event_id'=>$delivered['id'], 'history_refresh_page_token'=>'original-claim-cursor',
            ...array_intersect_key($payload, array_flip(['cancellation', 'authority_deadline_at', 'activity_members', 'timer_members', 'wait_members', 'child_members']))];

        return [$history, $receipt, $expected];
    }

    private static function prove(array $history, array $receipt, array $expected, bool $delivering): CancellationScopeDeliveryReceipt
    {
        if (!$delivering) {
            array_pop($history); $receipt['delivered'] = false; $receipt['history_event_id'] = $receipt['preparation_history_event_id'];
        }

        return CancellationScopeDeliveryReceipt::fromCanonicalHistory($receipt, $history, $expected, $delivering);
    }

    private static function find(array $history, string $id): array
    {
        foreach ($history as $event) { if ($event['id'] === $id) { return $event; } }
        throw new \LogicException('Missing fixture event.');
    }
}
