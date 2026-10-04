<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Exception\WorkflowCancelled;
use DurableWorkflow\Worker\CancellationContext;
use DurableWorkflow\Worker\CancellationHistory;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\ScopedCancellationContext;
use DurableWorkflow\Worker\WorkflowContext;
use InvalidArgumentException;
use LogicException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ScopedRunCancellationContextTest extends TestCase
{
    public function testNativeChildAndGrandchildKeepEveryScopeAddressAndTheOriginalBudget(): void
    {
        $fixtures = self::fixtures();
        foreach (['child', 'grandchild'] as $name) {
            $context = CancellationContext::fromArray($fixtures[$name]);
            self::assertEquals($fixtures[$name], $context->toArray());
            self::assertSame($context->toArray(), CancellationContext::fromArray($context->toArray())->toArray());
            self::assertSame('root-request', $context->rootRequestId);
            self::assertSame('maintenance', $context->reason);
            self::assertSame('api', $context->source);
            self::assertSame('operator-1', $context->requester['id']);
            self::assertSame('2026-10-04T00:00:00.123456Z', $context->requestedAt()->format('Y-m-d\TH:i:s.u\Z'));
            self::assertSame('2026-10-04T00:00:30.123456Z', $context->scopeOrigin->rootDeadline()->format('Y-m-d\TH:i:s.u\Z'));
        }
        $child = CancellationContext::fromArray($fixtures['child']);
        self::assertSame('inner-request', $child->parentRequestId);
        self::assertSame(['outer', 'inner'], array_column($child->scopeOrigin->lineage, 'scope_id'));
        self::assertSame('2026-10-04T00:00:20.123456Z', $child->scopeOrigin->deadline()->format('Y-m-d\TH:i:s.u\Z'));
        self::assertSame('2026-10-04T00:00:15.123456Z', $child->deadline()->format('Y-m-d\TH:i:s.u\Z'));
        $grandchild = CancellationContext::fromArray($fixtures['grandchild']);
        self::assertSame('child-scope-request', $grandchild->parentRequestId);
        self::assertSame(['outer', 'inner', 'root', 'child-scope'], array_column($grandchild->scopeOrigin->lineage, 'scope_id'));
        self::assertSame(['root-request', 'child-scope-request', 'grandchild-request'], array_column($grandchild->lineage, 'request_id'));
        self::assertSame('2026-10-04T00:00:12.123456Z', $grandchild->deadline()->format('Y-m-d\TH:i:s.u\Z'));
    }

    public function testAvroTimezoneAndObjectKeyOrderPreserveTheCanonicalTree(): void
    {
        $codec = new AvroPayloadCodec();
        $original = self::fixtures()['grandchild'];
        $reorder = static function (array $value) use (&$reorder): array {
            foreach ($value as &$item) {
                if (is_array($item)) {
                    $item = $reorder($item);
                }
            }
            return array_is_list($value) ? $value : array_reverse($value, true);
        };
        $snapshot = $reorder($original);
        $snapshot['requested_at'] = '2026-10-03T20:00:00.123456-04:00';
        $snapshot['cleanup_deadline_at'] = $snapshot['scope_authority_deadline_at'] = '2026-10-03T20:00:12.123456-04:00';
        $decoded = $codec->decodeEnvelope($codec->envelope($snapshot));
        self::assertEquals($original, CancellationContext::fromArray($decoded)->toArray());
    }

    public function testColdReplayKeepsTheOriginAndConsumesOnlyTheOriginalNarrowedCleanupClock(): void
    {
        $codec = new AvroPayloadCodec();
        $snapshot = self::fixtures()['child'];
        $seen = [];
        $workflow = static function (WorkflowContext $workflow) use (&$seen, $snapshot): array {
            self::assertNull($workflow->cancellationContext());
            try {
                $workflow->sleep(10);
            } catch (WorkflowCancelled $cancelled) {
                self::assertEquals($snapshot, $cancelled->context->toArray());
                self::assertSame($cancelled->context, $workflow->cancellationContext());
                return $workflow->cancellationShield(static function () use ($workflow, $cancelled, &$seen): array {
                    $seen[] = $cancelled->context->remaining();
                    $workflow->activity('cleanup');
                    $seen[] = $cancelled->context->remaining();
                    return ['remaining' => $seen, 'cancellation' => $cancelled->context->toArray()];
                });
            }
            return [];
        };
        $history = self::history();
        $first = (new Replayer($codec))->replay($workflow, $history, [], 'php-workers', ['run_id' => 'child-run']);
        self::assertSame([7.123456], $seen);
        self::assertSame('cleanup', $first->commands[0]['activity_type']);
        $history[] = ['event_type' => 'ActivityCompleted', 'timestamp' => '2026-10-04T00:00:12Z', 'payload' => [
            'sequence' => 2, 'activity_type' => 'cleanup', 'result' => $codec->envelope('cleaned'),
        ]];
        $history[] = ['event_type' => 'SignalReceived', 'timestamp' => '2026-10-04T00:00:29Z', 'payload' => [
            'signal_name' => 'future', 'arguments' => $codec->envelope([]),
        ]];
        $seen = [];
        $cold = (new Replayer($codec))->replay($workflow, $history, [], 'php-workers', ['run_id' => 'child-run']);
        self::assertSame('complete_workflow', $cold->commands[0]['type']);
        self::assertSame([7.123456, 3.123456], $seen);
        self::assertEquals(['remaining' => $seen, 'cancellation' => $snapshot], $codec->decodeEnvelope($cold->commands[0]['result']));
    }

    public function testDetachedScopedContextCannotUseTheHostClock(): void
    {
        $this->expectException(LogicException::class);
        CancellationContext::fromArray(self::fixtures()['child'])->remaining();
    }

    public function testCanonicalDeliveryCannotChangeOrDiscardTheOriginalScope(): void
    {
        $history = self::history();
        $history[1]['payload']['cancellation']['scope_origin']['lineage'][1]['scope_id'] = 'different';
        $this->expectException(NonDeterministicWorkflow::class);
        CancellationHistory::fromEvents($history, 'child-run');
    }

    #[DataProvider('invalidContexts')]
    public function testInvalidRootOriginOrBudgetIsRefused(array $snapshot): void
    {
        $this->expectException(InvalidArgumentException::class);
        CancellationContext::fromArray($snapshot);
    }

    public static function invalidContexts(): iterable
    {
        $original = self::fixtures()['grandchild'];
        foreach (['schema' => 'durable-workflow.cancellation-context/v1', 'root_request_id' => 'other',
            'root_workflow_instance_id' => 'other', 'root_workflow_run_id' => 'other',
            'parent_request_id' => 'root-request', 'reason' => 'other', 'source' => 'other',
            'requested_at' => '2026-10-04T00:00:01.123456Z', 'scope_origin' => [],
            'cleanup_deadline_at' => '2026-10-04T00:00:15.123456Z',
            'scope_authority_deadline_at' => '2026-10-04T00:00:15.123456Z'] as $key => $value) {
            yield $key => [[...$original, $key => $value]];
        }
        foreach (['scope_origin', 'scope_authority_deadline_at', 'parent_request_id'] as $key) {
            $snapshot = $original;
            unset($snapshot[$key]);
            yield 'missing '.$key => [$snapshot];
        }
        $snapshot = $original;
        $snapshot['requester']['id'] = 'other';
        yield 'requester' => [$snapshot];
        $snapshot = $original;
        $snapshot['lineage'][1]['request_id'] = 'child-request';
        yield 'discarded local scope hop' => [$snapshot];
        $snapshot = $original;
        $snapshot['cleanup_deadline_at'] = $snapshot['scope_authority_deadline_at'] = '2026-10-04T00:00:30.123456Z';
        yield 'widened global budget' => [$snapshot];
        $snapshot = $original;
        $snapshot['request_id'] = $snapshot['lineage'][2]['request_id'] = 'inner-request';
        yield 'reused request' => [$snapshot];
        $snapshot = $original;
        $snapshot['lineage'][2]['workflow_run_id'] = 'child-run';
        yield 'reused run' => [$snapshot];
        $snapshot = $original;
        array_shift($snapshot['scope_origin']['lineage']);
        yield 'discarded original root address' => [$snapshot];
        $snapshot = $original;
        $snapshot['scope_origin']['lineage'][3]['cleanup_deadline_at'] = '2026-10-04T00:00:16.123456Z';
        yield 'widened scope budget' => [$snapshot];
        $snapshot = $original;
        $snapshot['scope_origin']['lineage'][3]['workflow_instance_id'] = 'other';
        yield 'run reassigned to another instance' => [$snapshot];
        $snapshot = $original;
        $snapshot['scope_origin']['lineage'][3]['workflow_run_id'] = 'root-run';
        $snapshot['scope_origin']['lineage'][3]['workflow_instance_id'] = 'root-instance';
        yield 'reentered original run' => [$snapshot];
        $snapshot = $original;
        $snapshot['scope_origin']['lineage'][3]['request_id'] = 'inner-request';
        yield 'repeated scope request' => [$snapshot];
        $snapshot = $original;
        $snapshot['scope_origin']['lineage'][3]['scope_id'] = 'root';
        yield 'repeated scope address' => [$snapshot];
        $snapshot = $original;
        $snapshot['scope_origin']['root_context']['schema'] = 'durable-workflow.cancellation-context/v2';
        $snapshot['scope_origin']['root_context']['scope_origin'] = $original['scope_origin'];
        yield 'recursive root origin' => [$snapshot];
        $snapshot = $original;
        $snapshot['scope_origin']['lineage'][3]['authority'] = 'unrecorded';
        yield 'unsupported scope field' => [$snapshot];
    }

    public function testOriginalScopeIsAnImmutableMetadataObject(): void
    {
        $snapshot = self::fixtures()['grandchild'];
        $context = CancellationContext::fromArray($snapshot);
        $snapshot['scope_origin']['lineage'][3]['scope_id'] = 'changed';
        self::assertInstanceOf(ScopedCancellationContext::class, $context->scopeOrigin);
        self::assertSame('child-scope', $context->scopeOrigin->scopeId);
        self::assertSame('outer', $context->scopeOrigin->rootScopeId);
        self::assertSame('child-request', $context->scopeOrigin->parentRequestId);
        self::assertSame('child-instance', $context->scopeOrigin->workflowInstanceId);
        self::assertSame('child-run', $context->scopeOrigin->workflowRunId);
    }

    private static function fixtures(): array
    {
        return json_decode((string) file_get_contents(__DIR__.'/fixtures/scoped-run-cancellation-context.json'), true, flags: JSON_THROW_ON_ERROR);
    }

    private static function history(): array
    {
        $snapshot = self::fixtures()['child'];
        return [
            ['event_type' => 'CooperativeCancellationRequested', 'timestamp' => '2026-10-04T00:00:05Z', 'payload' => [
                'workflow_run_id' => 'child-run', 'workflow_instance_id' => 'child-instance',
                'workflow_command_id' => 'child-request', 'reason' => 'maintenance',
                'cleanup_deadline_at' => $snapshot['cleanup_deadline_at'], 'cancellation' => $snapshot,
            ]],
            ['event_type' => 'CooperativeCancellationDelivered', 'timestamp' => '2026-10-04T00:00:08Z', 'payload' => [
                'workflow_run_id' => 'child-run', 'workflow_command_id' => 'child-request',
                'sequence' => 1, 'call_kind' => 'timer', 'cancellation' => $snapshot,
            ]],
        ];
    }
}
