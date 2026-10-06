<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DateTimeImmutable;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Exception\WorkflowCancelled;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\CancellationScopeHistory;
use DurableWorkflow\Worker\CommittedCancellationScopeHistory;
use DurableWorkflow\Worker\ScopedCancellationContext;
use DurableWorkflow\Worker\WorkflowClaimAborted;
use DurableWorkflow\Worker\WorkflowContext;
use LogicException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class CommittedCancellationScopeReplayTest extends TestCase
{
    public function testAcceptedRunRequestIsTheCanonicalParentOfItsUnshieldedScope(): void
    {
        $fixture = self::runInheritedFixture();
        $scopes = new CancellationScopeHistory($fixture['history'], $fixture['task']['run_id']);
        $committed = new CommittedCancellationScopeHistory($fixture['history'], $fixture['task']['run_id'],
            $fixture['task']['workflow_id'], $scopes, requireCommittedDelivery: false);
        $request = $committed->pendingRequestForScope($fixture['history'][2]['payload']['scope_id'], $scopes);
        self::assertNotNull($request);
        self::assertSame('root-child-request', $request['context']->requestId);
        self::assertSame($fixture['history'][4]['payload']['workflow_command_id'], $request['context']->parentRequestId);
        self::assertSame(['root', $fixture['history'][2]['payload']['scope_id']], array_column($request['context']->lineage, 'scope_id'));
        self::assertSame($fixture['history'][4]['payload']['cleanup_deadline_at'], $request['context']->deadline()->format('Y-m-d\TH:i:s.u\Z'));
        // Canonical parent recognition does not enable scope execution by default.
        $entered = false;
        try {
            (new Replayer(new AvroPayloadCodec()))->replay(static function () use (&$entered): void { $entered = true; },
                $fixture['history'], [], 'php-workers', $fixture['task'], allowCancellationScopeAuthoring: true);
            self::fail('Default replay must retain its scope admission guard.');
        } catch (WorkflowClaimAborted $error) {
            self::assertStringContainsString('cancellation_scope_execution_not_supported', $error->getMessage());
            self::assertFalse($entered);
        }
    }

    #[DataProvider('invalidRunInheritance')]
    public function testRunInheritanceRequiresAnEarlierExactRequestAndCannotCrossAShield(string $failure): void
    {
        $fixture = self::runInheritedFixture();
        if ($failure === 'missing') {
            array_splice($fixture['history'], 4, 1);
        } elseif ($failure === 'later') {
            [$fixture['history'][4], $fixture['history'][5]] = [$fixture['history'][5], $fixture['history'][4]];
        } elseif ($failure === 'shield') {
            $fixture['history'][2]['payload']['shield_parent'] = true;
        } elseif ($failure === 'metadata') {
            $fixture['history'][5]['payload']['cancellation']['root_context']['reason'] = 'substituted reason';
        } elseif ($failure === 'deadline') {
            $fixture['history'][5]['payload']['cancellation']['lineage'][1]['cleanup_deadline_at'] = '2026-10-04T00:00:25.123456Z';
        }
        self::renumber($fixture['history']);
        $scopes = new CancellationScopeHistory($fixture['history'], $fixture['task']['run_id']);
        $this->expectException(NonDeterministicWorkflow::class);
        new CommittedCancellationScopeHistory($fixture['history'], $fixture['task']['run_id'],
            $fixture['task']['workflow_id'], $scopes, requireCommittedDelivery: false);
    }

    public static function invalidRunInheritance(): array
    {
        return [['missing'], ['later'], ['shield'], ['metadata'], ['deadline']];
    }

    #[DataProvider('scopeVariants')]
    public function test_native_delivery_replays_original_identity_clock_and_scope_without_cancelling_parent(string $variant): void
    {
        $fixture = self::fixture($variant);
        $snapshot = $fixture['history'][4]['payload']['cancellation'];
        $seen = [];
        $workflow = static function (WorkflowContext $workflow) use (&$seen, $snapshot, $variant): array {
            $context = $workflow->cancellationScope(static function () use ($workflow, &$seen, $snapshot, $variant): ScopedCancellationContext {
                $cancelled = $workflow->cancellationScope(static function () use ($workflow, &$seen, $snapshot): ScopedCancellationContext {
                    self::assertFalse($workflow->isCancellationRequested());
                    self::assertNull($workflow->cancellationContext());
                    try {
                        $workflow->sleep(10);
                        self::fail('The original unscheduled call must be interrupted.');
                    } catch (WorkflowCancelled $error) {
                        self::assertInstanceOf(ScopedCancellationContext::class, $error->context);
                        self::assertSame($snapshot['lineage'][0]['request_id'], $error->requestId);
                        self::assertEquals($snapshot, $error->context->toArray());
                        self::assertSame($snapshot['root_context']['root_request_id'], $error->context->rootRequestId);
                        self::assertSame('release one scope', $error->context->reason);
                        self::assertEquals($snapshot['root_context']['requester'], $error->context->requester);
                        self::assertSame($snapshot['root_context']['source'], $error->context->source);
                        self::assertSame($error->context, $workflow->cancellationContext());
                        self::assertTrue($workflow->isCancellationRequested());
                        $seen[] = $error->context->remaining();
                        $workflow->cancellationShield(static fn () => $workflow->throwIfCancellationRequested());
                        try {
                            $workflow->throwIfCancellationRequested();
                            self::fail('The explicit check must preserve the scoped request.');
                        } catch (WorkflowCancelled $again) {
                            self::assertSame($error->requestId, $again->requestId);
                            self::assertSame($error->context, $again->context);
                        }
                        return $error->context;
                    } finally {
                        self::assertTrue($workflow->isCancellationRequested());
                        self::assertInstanceOf(ScopedCancellationContext::class, $workflow->cancellationContext());
                    }
                }, shieldParent: $variant === 'shielded');
                self::assertFalse($workflow->isCancellationRequested());
                self::assertNull($workflow->cancellationContext());
                return $cancelled;
            });
            self::assertFalse($workflow->isCancellationRequested());
            self::assertNull($workflow->cancellationContext());
            $result = $workflow->activity('unaffected-root-operation');
            $seen[] = $context->remaining();
            return ['survivor' => $result, 'request' => $context->requestId, 'remaining' => $seen];
        };
        $first = $this->replay($workflow, $fixture);
        self::assertSame([22.623456], $seen);
        self::assertSame(['schedule_activity'], array_column($first->commands, 'type'));
        self::assertSame('unaffected-root-operation', $first->commands[0]['activity_type']);
        self::assertArrayNotHasKey('cancellation_scope_id', $first->commands[0]);
        self::assertNull($first->cancellationDelivery);
        $seen = [];
        self::assertSame($first->commands, $this->replay($workflow, $fixture)->commands);
        self::assertSame([22.623456], $seen);

        $codec = new AvroPayloadCodec();
        $fixture['history'][] = self::event($fixture, 'ActivityCompleted', [
            'sequence' => 4, 'activity_type' => 'unaffected-root-operation', 'result' => $codec->envelope('survivor-result'),
        ], '2026-10-04T00:00:20.000000Z');
        $seen = [];
        $finished = $this->replay($workflow, $fixture);
        self::assertSame([22.623456, 10.123456], $seen);
        self::assertSame(['complete_workflow'], array_column($finished->commands, 'type'));
        self::assertSame(['survivor' => 'survivor-result', 'request' => $snapshot['lineage'][0]['request_id'], 'remaining' => $seen],
            $codec->decodeEnvelope($finished->commands[0]['result']));
        // Replacement claim state is not a clock or a new cancellation budget.
        $fixture['task'] += ['lease_owner' => 'replacement', 'workflow_task_attempt' => 17];
        $seen = [];
        self::assertSame($finished->commands, $this->replay($workflow, $fixture)->commands);
    }

    public static function scopeVariants(): array
    {
        return ['direct request' => ['unshielded'], 'direct request inside parent shield' => ['shielded']];
    }

    public function test_authority_ceiling_does_not_replace_original_context_budget(): void
    {
        $fixture = self::fixture();
        foreach ([5, 6] as $index) {
            $fixture['history'][$index]['payload']['authority_deadline_at'] = '2026-10-04T00:00:26.000000Z';
        }
        $result = $this->replay(self::simpleWorkflow(), $fixture);
        $value = (new AvroPayloadCodec())->decodeEnvelope($result->commands[0]['result']);
        self::assertSame(22.623456, $value['remaining']);
        self::assertSame('2026-10-04T00:00:30.123456Z', $value['deadline']);
    }

    public function test_immutable_context_clock_uses_narrowed_scope_deadline_and_cannot_escape_workflow_lifetime(): void
    {
        $snapshot = self::fixture()['history'][4]['payload']['cancellation'];
        $last = $snapshot['lineage'][0];
        $snapshot['lineage'][] = [...$last, 'request_id' => 'narrowed-request', 'scope_id' => 'narrowed-scope',
            'cleanup_deadline_at' => '2026-10-04T00:00:15.000000Z'];
        $original = ScopedCancellationContext::fromArray($snapshot);
        self::assertEquals($snapshot, $original->toArray());
        try {
            $original->remaining();
            self::fail('A decoded receipt cannot supply workflow time.');
        } catch (LogicException $error) {
            self::assertStringContainsString('deterministic workflow time', $error->getMessage());
        }
        $timed = $original->withReplayClock(static fn () => new DateTimeImmutable('2026-10-04T00:00:07.500000Z'));
        self::assertSame(7.5, $timed->remaining());
        self::assertSame(0.0, $original->withReplayClock(static fn () => new DateTimeImmutable('2026-10-04T00:00:31Z'))->remaining());
        self::assertSame($original->toArray(), $timed->toArray());

        $captured = null;
        $this->replay(static function (WorkflowContext $workflow) use (&$captured): void {
            $workflow->cancellationScope(static function () use ($workflow, &$captured): void {
                $workflow->cancellationScope(static function () use ($workflow, &$captured): void {
                    try { $workflow->sleep(10); } catch (WorkflowCancelled $error) { $captured = $error->context; }
                });
            });
        }, self::fixture());
        self::assertInstanceOf(ScopedCancellationContext::class, $captured);
        $this->expectException(LogicException::class);
        $captured->remaining();
    }

    #[DataProvider('newScopedEffects')]
    public function test_delivery_and_shield_do_not_grant_new_scoped_effect_authority(string $operation): void
    {
        $entered = false;
        try {
            $this->replay(static fn (WorkflowContext $workflow) => $workflow->cancellationScope(static fn () =>
                $workflow->cancellationScope(static function () use ($workflow, $operation, &$entered): void {
                    try { $workflow->sleep(10); } catch (WorkflowCancelled) {
                        $workflow->cancellationShield(static function () use ($workflow, $operation, &$entered): void {
                            match ($operation) {
                                'activity' => $workflow->activity('unqualified-cleanup'),
                                'child' => $workflow->childWorkflow('unqualified-child'),
                                'scope' => $workflow->cancellationScope(static function () use (&$entered): void { $entered = true; }),
                                'side effect' => $workflow->sideEffect(static function () use (&$entered): void { $entered = true; }),
                                'memo' => $workflow->upsertMemo(['unqualified' => 'cleanup']),
                                'continuation' => $workflow->continueAsNew([]),
                            };
                        });
                    }
                })), self::fixture());
            self::fail('Committed delivery cannot authorize new scoped work.');
        } catch (WorkflowClaimAborted $error) {
            self::assertStringContainsString('cancellation_scope_cleanup_authority_missing', $error->getMessage());
            self::assertFalse($entered);
        }
    }

    public static function newScopedEffects(): array
    {
        return array_map(static fn (string $operation): array => [$operation], ['activity', 'child', 'scope', 'side effect', 'memo', 'continuation']);
    }

    #[DataProvider('malformedHistory')]
    public function test_invalid_canonical_scope_fact_is_rejected_before_workflow_entry(string $mutation): void
    {
        $fixture = self::fixture();
        self::mutate($fixture['history'], $mutation);
        $entered = false;
        try {
            $this->replay(static function () use (&$entered): void { $entered = true; }, $fixture);
            self::fail('Malformed scope history must not enter workflow code.');
        } catch (NonDeterministicWorkflow $error) {
            self::assertSame('invalid_cancellation_scope_history', $error->reason);
            self::assertFalse($entered);
        }
    }

    public static function malformedHistory(): array
    {
        $mutations = ['request schema', 'preparation schema', 'delivery schema', 'foreign run', 'foreign instance', 'unknown scope',
            'request identity', 'preparation request', 'delivery request', 'changed reason', 'changed deadline', 'missing parent', 'unknown parent',
            'missing preparation', 'missing reference', 'wrong reference', 'changed boundary', 'changed kind', 'changed ceiling', 'extended ceiling',
            'expired ceiling', 'bad timestamp', 'backwards timestamp', 'string boundary', 'zero span', 'missing span', 'missing operation',
            'missing operation span', 'extra operation', 'duplicate request', 'duplicate preparation', 'duplicate delivery',
            'request before opening', 'delivery before preparation', 'duplicate event id', 'unordered history', 'foreign namespace',
            'missing inventory', 'object inventory', 'omitted admitted member'];
        return array_combine($mutations, array_map(static fn (string $mutation): array => [$mutation], $mutations));
    }

    private static function mutate(array &$history, string $mutation): void
    {
        switch ($mutation) {
            case 'request schema': $history[4]['payload']['schema'] = 'other'; break;
            case 'preparation schema': $history[5]['payload']['schema'] = 'durable-workflow.cancellation-scope-preparation/v4'; break;
            case 'delivery schema': $history[6]['payload']['schema'] = 'other'; break;
            case 'foreign run': $history[4]['payload']['workflow_run_id'] = 'other'; break;
            case 'foreign instance': $history[4]['payload']['cancellation']['lineage'][0]['workflow_instance_id'] = 'other'; break;
            case 'unknown scope': $history[4]['payload']['scope_id'] = 'unknown'; break;
            case 'request identity': $history[4]['payload']['request_id'] = 'other'; break;
            case 'preparation request': $history[5]['payload']['request_id'] = 'other'; break;
            case 'delivery request': $history[6]['payload']['request_id'] = 'other'; break;
            case 'changed reason': $history[5]['payload']['cancellation']['root_context']['reason'] = 'other'; break;
            case 'changed deadline': $history[6]['payload']['cancellation']['lineage'][0]['cleanup_deadline_at'] = '2026-10-04T00:00:31Z'; break;
            case 'missing parent': unset($history[4]['payload']['parent_scope_id']); break;
            case 'unknown parent': $history[4]['payload']['parent_scope_id'] = 'unknown'; break;
            case 'missing preparation': array_splice($history, 5, 1); break;
            case 'missing reference': unset($history[6]['payload']['preparation_history_event_id']); break;
            case 'wrong reference': $history[6]['payload']['preparation_history_event_id'] = 'other'; break;
            case 'changed boundary': $history[6]['payload']['sequence'] = 4; break;
            case 'changed kind': $history[6]['payload']['call_kind'] = 'activity'; break;
            case 'changed ceiling': $history[6]['payload']['authority_deadline_at'] = '2026-10-04T00:00:29Z'; break;
            case 'extended ceiling': $history[5]['payload']['authority_deadline_at'] = '2026-10-04T00:00:31Z'; break;
            case 'expired ceiling': $history[5]['payload']['authority_deadline_at'] = '2026-10-04T00:00:02Z'; break;
            case 'bad timestamp': $history[6]['timestamp'] = '2026-02-30T00:00:07Z'; break;
            case 'backwards timestamp': $history[6]['timestamp'] = '2026-10-04T00:00:01Z'; break;
            case 'string boundary': $history[5]['payload']['sequence'] = '3'; break;
            case 'zero span': $history[5]['payload']['sequence_span'] = 0; break;
            case 'missing span': unset($history[5]['payload']['sequence_span']); break;
            case 'missing operation': unset($history[5]['payload']['operation_sequence']); break;
            case 'missing operation span': unset($history[5]['payload']['operation_sequence_span']); break;
            case 'extra operation': $history[5]['payload']['operation_sequence'] = 1; break;
            case 'duplicate request': $duplicate = $history[4]; $duplicate['id'] = 'second-request-event'; array_splice($history, 5, 0, [$duplicate]); self::renumber($history); break;
            case 'duplicate preparation': $duplicate = $history[5]; $duplicate['id'] = 'second-preparation-event'; array_splice($history, 6, 0, [$duplicate]); self::renumber($history); break;
            case 'duplicate delivery': $duplicate = $history[6]; $duplicate['id'] = 'second-delivery-event'; $history[] = $duplicate; self::renumber($history); break;
            case 'request before opening': $request = $history[4]; array_splice($history, 4, 1); array_splice($history, 3, 0, [$request]); self::renumber($history); break;
            case 'delivery before preparation': [$history[5], $history[6]] = [$history[6], $history[5]]; self::renumber($history); break;
            case 'duplicate event id': $history[6]['id'] = $history[5]['id']; break;
            case 'unordered history': $history[6]['sequence'] = $history[5]['sequence']; break;
            case 'foreign namespace': $history[6]['namespace'] = 'other'; break;
            case 'missing inventory': unset($history[5]['payload']['timer_members']); break;
            case 'object inventory': $history[5]['payload']['timer_members'] = ['timer' => 'invalid']; break;
            case 'omitted admitted member':
                $event = $history[4]; $event['id'] = 'admitted-member'; $event['event_type'] = 'TimerScheduled';
                $event['payload'] = ['sequence' => 3, 'cancellation_scope_id' => $history[4]['payload']['scope_id']];
                array_splice($history, 4, 0, [$event]); self::renumber($history); break;
        }
    }

    #[DataProvider('unqualifiedProjections')]
    public function test_unqualified_projection_or_pending_delivery_is_refused_before_workflow_entry(string $case): void
    {
        $fixture = self::fixture();
        if ($case === 'pending request') { $fixture['history'] = array_slice($fixture['history'], 0, 5); }
        elseif ($case === 'pending preparation') { array_pop($fixture['history']); }
        elseif (in_array($case, ['parallel', 'selection_handle', 'local_activity', 'signal'], true)) {
            foreach ([5, 6] as $index) {
                $fixture['history'][$index]['payload']['call_kind'] = $case;
                if ($case === 'parallel') { $fixture['history'][$index]['payload']['sequence_span'] = 2; }
                elseif ($case === 'selection_handle') { $fixture['history'][$index]['payload']['operation_sequence'] = 1; }
            }
        } else { $fixture['history'][5]['payload'][$case] = [['unqualified' => true]]; }
        $entered = false;
        try {
            $this->replay(static function () use (&$entered): void { $entered = true; }, $fixture);
            self::fail('An unqualified projection cannot run workflow code.');
        } catch (WorkflowClaimAborted $error) {
            self::assertFalse(str_ends_with($case, '_members'));
            self::assertStringContainsString('cancellation_scope_execution_not_supported', $error->getMessage());
            self::assertFalse($entered);
        } catch (NonDeterministicWorkflow $error) {
            self::assertTrue(str_ends_with($case, '_members'));
            self::assertSame('invalid_cancellation_scope_history', $error->reason);
            self::assertFalse($entered);
        }
    }

    public static function unqualifiedProjections(): array
    {
        $cases = ['pending request', 'pending preparation', 'parallel', 'selection_handle', 'local_activity', 'signal', 'activity_members', 'timer_members',
            'wait_members', 'child_members', 'descendant_members'];
        return array_combine($cases, array_map(static fn (string $case): array => [$case], $cases));
    }

    #[DataProvider('changedCalls')]
    public function test_changed_authored_call_or_delivery_shield_is_rejected_before_cleanup(string $change): void
    {
        $cleanup = false;
        try {
            $this->replay(static fn (WorkflowContext $workflow) => $workflow->cancellationScope(static fn () =>
                $workflow->cancellationScope(static function () use ($workflow, $change, &$cleanup): void {
                    try {
                        match ($change) {
                            'activity' => $workflow->activity('changed-call'),
                            'shield' => $workflow->cancellationShield(static fn () => $workflow->sleep(10)),
                            'return' => null,
                        };
                    } catch (WorkflowCancelled) { $cleanup = true; }
                })), self::fixture());
            self::fail('Changed authored scope boundary must be rejected.');
        } catch (NonDeterministicWorkflow $error) {
            self::assertSame('cancellation_scope_boundary_mismatch', $error->reason);
            self::assertFalse($cleanup);
        }
    }

    public static function changedCalls(): array
    {
        return ['kind' => ['activity'], 'shield' => ['shield'], 'return before boundary' => ['return']];
    }

    public function test_default_worker_replay_retains_scope_admission_refusal(): void
    {
        $fixture = self::fixture();
        $entered = false;
        $this->expectException(WorkflowClaimAborted::class);
        try {
            (new Replayer(new AvroPayloadCodec()))->replay(static function () use (&$entered): void { $entered = true; },
                $fixture['history'], [], 'php-workers', $fixture['task'], allowCancellationScopeAuthoring: true);
        } finally { self::assertFalse($entered); }
    }

    private function replay(callable $workflow, array $fixture): \DurableWorkflow\Worker\ReplayResult
    {
        return (new Replayer(new AvroPayloadCodec()))->replay($workflow, $fixture['history'], [], 'php-workers', $fixture['task'],
            allowCancellationScopeAuthoring: true, replayCommittedCancellationScopes: true);
    }

    private static function simpleWorkflow(): callable
    {
        return static fn (WorkflowContext $workflow) => $workflow->cancellationScope(static fn () => $workflow->cancellationScope(static function () use ($workflow): array {
            try { $workflow->sleep(10); } catch (WorkflowCancelled $error) {
                return ['remaining' => $error->context->remaining(), 'deadline' => $error->context->deadline()->format('Y-m-d\TH:i:s.u\Z')];
            }
            return [];
        }));
    }

    private static function fixture(string $variant = 'unshielded'): array
    {
        return json_decode((string) file_get_contents(__DIR__.'/fixtures/committed-scope-delivery.json'), true, flags: JSON_THROW_ON_ERROR)[$variant];
    }

    private static function runInheritedFixture(): array
    {
        $fixture = self::fixture();
        $request = $fixture['history'][4];
        $root = $request['payload']['cancellation']['root_context'];
        $scope = $fixture['history'][2]['payload']['scope_id'];
        $lineage = [...$root['lineage'][0], 'scope_id' => 'root', 'cleanup_deadline_at' => $root['cleanup_deadline_at']];
        $request['payload'] = ['schema' => 'durable-workflow.cancellation-scope-request/v1',
            'workflow_run_id' => $fixture['task']['run_id'], 'scope_id' => $scope,
            'request_id' => 'root-child-request', 'parent_scope_id' => 'root', 'cancellation' => [
                'schema' => ScopedCancellationContext::SCHEMA, 'root_context' => $root,
                'lineage' => [$lineage, [...$lineage, 'scope_id' => $scope, 'request_id' => 'root-child-request']],
            ]];
        $rootEvent = [...$request, 'id' => 'run-cancellation', 'event_type' => 'CooperativeCancellationRequested',
            'payload' => ['workflow_command_id' => $root['request_id'], 'workflow_run_id' => $fixture['task']['run_id'],
                'workflow_instance_id' => $fixture['task']['workflow_id'], 'cleanup_deadline_at' => $root['cleanup_deadline_at'], 'cancellation' => $root]];
        $fixture['history'] = [...array_slice($fixture['history'], 0, 4), $rootEvent, $request];
        self::renumber($fixture['history']);
        return $fixture;
    }

    private static function event(array $fixture, string $kind, array $payload, string $timestamp): array
    {
        $last = $fixture['history'][array_key_last($fixture['history'])];
        return ['id' => 'root-operation-completion', 'namespace' => $last['namespace'], 'sequence' => $last['sequence'] + 1,
            'event_type' => $kind, 'payload' => $payload, 'timestamp' => $timestamp];
    }

    private static function renumber(array &$history): void
    {
        foreach ($history as $index => &$event) { $event['sequence'] = $index + 1; }
    }
}
