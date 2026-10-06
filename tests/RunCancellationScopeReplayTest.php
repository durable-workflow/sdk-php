<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Exception\WorkflowCancelled;
use DurableWorkflow\Worker\CancellationContext;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\ScopedCancellationContext;
use DurableWorkflow\Worker\WorkflowClaimAborted;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class RunCancellationScopeReplayTest extends TestCase
{
    #[DataProvider('prefixes')]
    public function testOriginalRunRequestReplaysScopedCleanupBeforeRootDelivery(string $prefix): void
    {
        $fixture = self::fixture();
        $seen = [];
        $history = array_slice($fixture['history'], 0, $fixture['history_ranges'][$prefix]);
        $handler = self::handler($seen);
        $codec = new AvroPayloadCodec();
        $result = (new Replayer($codec))->replay($handler, $history, [], 'php-workers', $fixture['task'],
            localActivityExecutor: static fn () => throw new \LogicException('Recorded or prepared cleanup cannot execute inline.'),
            prepareLocalActivities: true, allowCancellationScopeAuthoring: true,
            replayCommittedCancellationScopes: true, prepareCancellationScopeDelivery: true);
        $root = array_values(array_filter($fixture['history'], static fn (array $event): bool =>
            $event['event_type'] === 'CooperativeCancellationRequested'))[0]['payload']['cancellation'];
        if (in_array($prefix, ['requested', 'prepared'], true)) {
            self::assertNotNull($result->cancellationScopeDelivery);
            self::assertSame(2, $result->cancellationScopeDelivery->boundary->sequence);
            self::assertSame($root['root_request_id'], $result->cancellationScopeDelivery->context->rootRequestId);
            self::assertSame($root['cleanup_deadline_at'], $result->cancellationScopeDelivery->context->deadline()->format('Y-m-d\TH:i:s.u\Z'));
            self::assertSame([], $seen);
        } elseif ($prefix === 'scope_delivered') {
            self::assertSame(3, $result->preparedLocalActivity?->sequence);
            $descriptor = $result->preparedLocalActivity->descriptor($codec);
            self::assertSame($fixture['scope_id'], $descriptor['cancellation_scope_id']);
            self::assertSame($fixture['scope_id'], $descriptor['cancellation_cleanup']['scope_id']);
        } elseif ($prefix === 'scope_cleaned') {
            self::assertSame(4, $result->cancellationDelivery?->sequence);
            self::assertSame($root['request_id'], $result->cancellationDelivery?->requestId);
        } elseif ($prefix === 'root_delivered') {
            self::assertSame(5, $result->preparedLocalActivity?->sequence);
            $descriptor = $result->preparedLocalActivity->descriptor($codec);
            self::assertArrayNotHasKey('cancellation_scope_id', $descriptor);
            self::assertSame($root['request_id'], $descriptor['cancellation_cleanup']['request_id']);
        } else {
            self::assertSame(['complete_workflow'], array_column($result->commands, 'type'));
            self::assertSame('finished', $codec->decodeEnvelope($result->commands[0]['result']));
            self::assertSame('cancelled', $fixture['native_terminal_status']);
        }
        foreach ($seen as $context) {
            self::assertSame($root['root_request_id'], $context->rootRequestId);
            self::assertEquals(new \DateTimeImmutable($root['cleanup_deadline_at']), $context->deadline());
        }
        $expected = $result;
        $seen = [];
        $replacement = $fixture['task'] + ['lease_owner' => 'replacement', 'workflow_task_attempt' => 19];
        $again = (new Replayer($codec))->replay($handler, $history, [], 'php-workers', $replacement,
            localActivityExecutor: static fn () => throw new \LogicException('Replay cannot execute inline.'),
            prepareLocalActivities: true, allowCancellationScopeAuthoring: true,
            replayCommittedCancellationScopes: true, prepareCancellationScopeDelivery: true);
        self::assertEquals($expected->commands, $again->commands);
        self::assertEquals($expected->cancellationDelivery, $again->cancellationDelivery);
        self::assertEquals($expected->cancellationScopeDelivery, $again->cancellationScopeDelivery);
        self::assertEquals($expected->preparedLocalActivity?->descriptor($codec), $again->preparedLocalActivity?->descriptor($codec));
    }

    public static function prefixes(): iterable
    {
        foreach (['requested', 'prepared', 'scope_delivered', 'scope_cleaned', 'root_delivered', 'root_cleaned'] as $prefix) {
            yield $prefix => [$prefix];
        }
    }

    #[DataProvider('changedRootMembership')]
    public function testCommittedRootDeliveryCannotReplaceAScopedCall(string $kind): void
    {
        $fixture = self::fixture();
        $history = array_slice($fixture['history'], 0, $fixture['history_ranges']['root_delivered']);
        if ($kind === 'group') {
            $last = array_key_last($history);
            $history[$last]['payload']['call_kind'] = 'parallel';
            $history[$last]['payload']['sequence_span'] = 2;
        }
        // Replay the scope prefix and change only the root delivery's authored call.
        $cleaned = false;
        $handler = static function (WorkflowContext $context) use ($kind, &$cleaned): void {
            $leaf = $context->cancellationScope(static function () use ($context): mixed {
                $leaf = $context->deferTimer(10);
                try { $context->sleep(10); } catch (WorkflowCancelled) {
                    $context->cancellationShield(static fn () => $context->localActivity('tests.scoped-cleanup'));
                }
                return $leaf;
            });
            try {
                if ($kind === 'group') { $context->all([$leaf, $context->deferTimer(10)]); }
                else { $context->cancellationScope(static fn () => $context->sleep(10), shieldParent: true); }
            } catch (WorkflowCancelled) { $cleaned = true; }
        };
        $this->expectException(NonDeterministicWorkflow::class);
        try {
            (new Replayer(new AvroPayloadCodec()))->replay($handler, $history, [], 'php-workers', $fixture['task'],
                localActivityExecutor: static fn () => null, prepareLocalActivities: true,
                allowCancellationScopeAuthoring: true, replayCommittedCancellationScopes: true,
                prepareCancellationScopeDelivery: true);
        } finally { self::assertFalse($cleaned); }
    }

    public static function changedRootMembership(): iterable
    {
        yield 'captured scoped leaf' => ['group'];
        yield 'new shielded scope' => ['scope'];
    }

    public function testCommittedRootDeliveryIntoAnActiveScopeCannotEmitAnotherScopeIntent(): void
    {
        $fixture = self::fixture();
        $history = array_slice($fixture['history'], 0, $fixture['history_ranges']['requested']);
        $delivery = $fixture['history'][$fixture['history_ranges']['root_delivered'] - 1];
        $delivery['sequence'] = count($history) + 1;
        $delivery['payload']['sequence'] = 2;
        $history[] = $delivery;
        $seen = [];
        $this->expectException(NonDeterministicWorkflow::class);
        $this->expectExceptionMessage('Committed root cancellation cannot replace a scoped operation.');
        try {
            (new Replayer(new AvroPayloadCodec()))->replay(self::handler($seen), $history, [], 'php-workers', $fixture['task'],
                localActivityExecutor: static fn () => null, prepareLocalActivities: true,
                allowCancellationScopeAuthoring: true, replayCommittedCancellationScopes: true,
                prepareCancellationScopeDelivery: true);
        } finally { self::assertSame([], $seen); }
    }

    #[DataProvider('deferredGroups')]
    public function testPendingRootDeliveryCannotBorrowScopedDeferredMembership(string $mode): void
    {
        $fixture = self::fixture();
        $history = array_slice($fixture['history'], 0, $fixture['history_ranges']['requested']);
        $this->expectException(WorkflowClaimAborted::class);
        $this->expectExceptionMessage('root delivery cannot replace deferred scoped group or selection members');
        (new Replayer(new AvroPayloadCodec()))->replay(static function (WorkflowContext $context) use ($mode): void {
            $leaf = $context->cancellationScope(static fn () => $context->deferTimer(10));
            if ($mode === 'all') { $context->all([$leaf, $context->deferTimer(10)]); }
            else { $context->select(['scoped' => $leaf, 'root' => $context->deferTimer(10)]); }
        }, $history, [], 'php-workers', $fixture['task'],
            allowCancellationScopeAuthoring: true, replayCommittedCancellationScopes: true,
            prepareCancellationScopeDelivery: true);
    }

    public static function deferredGroups(): iterable
    {
        yield 'parallel' => ['all'];
        yield 'selection' => ['select'];
    }

    public function testOrdinaryWorkerCannotOpenAScopeAfterRunCancellationWithoutTheSourceOptIn(): void
    {
        $fixture = self::fixture();
        $history = array_values(array_filter($fixture['history'], static fn (array $event): bool =>
            in_array($event['event_type'], ['StartAccepted', 'WorkflowStarted', 'CooperativeCancellationRequested'], true)));
        $entered = false;
        $this->expectException(WorkflowClaimAborted::class);
        try {
            (new Replayer(new AvroPayloadCodec()))->replay(static fn (WorkflowContext $context) =>
                $context->cancellationScope(static function () use (&$entered): void { $entered = true; }),
                $history, [], 'php-workers', $fixture['task'], allowCancellationScopeAuthoring: true);
        } finally { self::assertFalse($entered); }
    }

    private static function handler(array &$seen): callable
    {
        return static function (WorkflowContext $context) use (&$seen): string {
            $context->cancellationScope(static function () use ($context, &$seen): void {
                try { $context->sleep(10); } catch (WorkflowCancelled $error) {
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

    private static function fixture(): array
    {
        return json_decode((string) file_get_contents(__DIR__.'/fixtures/run-inherited-scope-delivery.json'), true, flags: JSON_THROW_ON_ERROR);
    }
}
