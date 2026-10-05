<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Exception\WorkflowCancelled;
use DurableWorkflow\Worker\CancellationContext;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\ReplayResult;
use DurableWorkflow\Worker\ScopedCancellationContext;
use DurableWorkflow\Worker\WorkflowClaimAborted;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ScopedPreparedLocalReplayTest extends TestCase
{
    #[DataProvider('prefixes')]
    public function testOriginalLocalStopReplaysBeforeScopeAndRootCleanup(string $prefix): void
    {
        $fixture = self::fixture();
        $history = array_slice($fixture['history'], 0, $fixture['history_ranges'][$prefix]);
        $seen = [];
        $result = self::replay(self::handler($seen), $history, $fixture['task']);
        $root = array_values(array_filter($fixture['history'], static fn (array $event): bool =>
            $event['event_type'] === 'CooperativeCancellationRequested'))[0]['payload']['cancellation'];
        if (in_array($prefix, ['opened', 'admitted'], true)) {
            self::assertSame(2, $result->preparedLocalActivity?->sequence);
            self::assertSame($prefix === 'admitted', $result->preparedLocalActivity?->recover);
            self::assertSame($fixture['scope_id'], $result->preparedLocalActivity?->descriptor(new AvroPayloadCodec())['cancellation_scope_id']);
        } elseif (in_array($prefix, ['requested', 'callback_stopped', 'prepared'], true)) {
            self::assertSame(2, $result->cancellationScopeDelivery?->boundary->sequence);
            self::assertSame('local_activity', $result->cancellationScopeDelivery?->boundary->callKind);
            self::assertSame($root['root_request_id'], $result->cancellationScopeDelivery?->context->rootRequestId);
            self::assertSame([], $seen);
        } elseif ($prefix === 'scope_delivered') {
            self::assertSame(3, $result->preparedLocalActivity?->sequence);
            self::assertSame($fixture['scope_id'], $result->preparedLocalActivity?->cleanupSnapshot()['scope_id']);
        } elseif ($prefix === 'scope_cleaned') {
            self::assertSame(4, $result->cancellationDelivery?->sequence);
        } elseif ($prefix === 'root_delivered') {
            self::assertSame(5, $result->preparedLocalActivity?->sequence);
            self::assertSame($root['request_id'], $result->preparedLocalActivity?->cleanupSnapshot()['request_id']);
        } else {
            self::assertSame(['complete_workflow'], array_column($result->commands, 'type'));
            self::assertSame('finished', (new AvroPayloadCodec())->decodeEnvelope($result->commands[0]['result']));
        }
        foreach ($seen as $context) {
            self::assertSame($root['root_request_id'], $context->rootRequestId);
            self::assertEquals(new \DateTimeImmutable($root['cleanup_deadline_at']), $context->deadline());
        }
        $seen = [];
        $replacement = self::replay(self::handler($seen), $history,
            $fixture['task'] + ['lease_owner' => 'replacement', 'workflow_task_attempt' => 19]);
        self::assertEquals($result, $replacement);
    }

    public static function prefixes(): iterable
    {
        foreach (['opened', 'admitted', 'requested', 'callback_stopped', 'prepared', 'scope_delivered',
            'scope_cleaned', 'root_delivered', 'root_cleaned'] as $prefix) {
            yield $prefix => [$prefix];
        }
    }

    public function testChangedOriginalLocalDescriptorFailsBeforeDeliveryOrCleanup(): void
    {
        $fixture = self::fixture();
        $seen = [];
        $this->expectException(NonDeterministicWorkflow::class);
        try {
            self::replay(self::handler($seen, 'tests.changed-work'),
                array_slice($fixture['history'], 0, $fixture['history_ranges']['callback_stopped']), $fixture['task']);
        } finally { self::assertSame([], $seen); }
    }

    public function testOrdinaryScopeProfileCannotStartTheCallbackWithoutItsLocalOptIn(): void
    {
        $fixture = self::fixture();
        $seen = [];
        $this->expectException(WorkflowClaimAborted::class);
        $this->expectExceptionMessage('cancellation_scope_local_activity_not_supported');
        (new Replayer(new AvroPayloadCodec()))->replay(self::handler($seen),
            array_slice($fixture['history'], 0, $fixture['history_ranges']['opened']), [], 'workers', $fixture['task'],
            localActivityExecutor: static fn () => throw new \LogicException('No inline execution.'),
            prepareLocalActivities: true, localActivityCancellationPolicies: ['wait_cancellation_completed'],
            allowCancellationScopeAuthoring: true, replayCommittedCancellationScopes: true, prepareCancellationScopeDelivery: true);
    }

    public function testUnqualifiedScopedGroupRefusesBeforeAdmittingAnyMember(): void
    {
        $fixture = self::fixture();
        $this->expectException(WorkflowClaimAborted::class);
        $this->expectExceptionMessage('selective callback supervision');
        self::replay(static fn (WorkflowContext $context) => $context->cancellationScope(static fn () =>
            $context->all([static fn () => $context->localActivity('tests.first'),
                static fn () => $context->localActivity('tests.second')])),
            array_slice($fixture['history'], 0, $fixture['history_ranges']['opened']), $fixture['task']);
    }

    private static function handler(array &$seen, string $activity = 'tests.scoped-work'): callable
    {
        return static function (WorkflowContext $context) use (&$seen, $activity): string {
            $context->cancellationScope(static function () use ($context, &$seen, $activity): void {
                try { $context->localActivity($activity, options: ['cancellation_policy' => 'wait_cancellation_completed']); }
                catch (WorkflowCancelled $error) {
                    self::assertInstanceOf(ScopedCancellationContext::class, $error->context);
                    $seen[] = $error->context;
                    $context->cancellationShield(static fn () => $context->localActivity('tests.scoped-cleanup'));
                }
            });
            try { $context->sleep(10); } catch (WorkflowCancelled $error) {
                self::assertInstanceOf(CancellationContext::class, $error->context);
                self::assertNotInstanceOf(ScopedCancellationContext::class, $error->context);
                $seen[] = $error->context;
                $context->cancellationShield(static fn () => $context->localActivity('tests.root-cleanup'));
            }
            return 'finished';
        };
    }

    private static function replay(callable $handler, array $history, array $task): ReplayResult
    {
        return (new Replayer(new AvroPayloadCodec()))->replay($handler, $history, [], 'workers', $task,
            localActivityExecutor: static fn () => throw new \LogicException('Prepared or recorded callbacks never execute inline.'),
            prepareLocalActivities: true, prepareLocalActivityGroups: true,
            localActivityCancellationPolicies: ['wait_cancellation_completed'], allowCancellationScopeAuthoring: true,
            replayCommittedCancellationScopes: true, prepareCancellationScopeDelivery: true,
            allowScopedPreparedLocalActivities: true);
    }

    private static function fixture(): array
    {
        return json_decode((string) file_get_contents(__DIR__.'/fixtures/run-scoped-local-delivery.json'), true, flags: JSON_THROW_ON_ERROR);
    }
}
