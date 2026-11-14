<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\WorkflowCancelled;
use DurableWorkflow\Exception\WorkflowTimeoutCleanupRequested;
use DurableWorkflow\Worker\CancellationContext;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\ScopedCancellationContext;
use DurableWorkflow\Worker\WorkflowContext;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * The optional top-level `cause` of a cancellation context (workflow#863),
 * ported from Core's CancellationContextCauseTest vectors.
 */
final class CancellationContextCauseTest extends TestCase
{
    public function testAnOrdinaryContextIsUnchangedAndStaysACancellation(): void
    {
        $snapshot = self::snapshot();
        $context = CancellationContext::fromArray($snapshot);

        self::assertFalse($context->isTimeout());
        self::assertNull($context->cause);
        self::assertSame($snapshot, $context->toArray());
        self::assertSame(
            json_encode($snapshot, JSON_THROW_ON_ERROR),
            json_encode($context->toArray(), JSON_THROW_ON_ERROR),
        );
        self::assertSame(WorkflowCancelled::class, WorkflowCancelled::forContext('req-1', $context)::class);
    }

    public function testAnExplicitCancellationCauseIsOrdinaryAndRoundTrips(): void
    {
        $snapshot = [...self::snapshot(), 'cause' => 'cancellation'];
        $context = CancellationContext::fromArray($snapshot);

        self::assertFalse($context->isTimeout());
        self::assertSame('cancellation', $context->cause);
        self::assertSame($snapshot, $context->toArray());
        self::assertSame(WorkflowCancelled::class, WorkflowCancelled::forContext('req-1', $context)::class);
    }

    public function testANullCauseIsAnOrdinaryCancellation(): void
    {
        $context = CancellationContext::fromArray([...self::snapshot(), 'cause' => null]);

        self::assertFalse($context->isTimeout());
        self::assertSame(self::snapshot(), $context->toArray());
    }

    public function testATimeoutCauseRoundTripsAndDeliversTheTimeoutSubtype(): void
    {
        $snapshot = [...self::snapshot(), 'cause' => 'timeout'];
        $context = CancellationContext::fromArray($snapshot);

        self::assertTrue($context->isTimeout());
        self::assertSame('timeout', $context->toArray()['cause']);
        self::assertSame($snapshot, $context->toArray());
        $exception = WorkflowCancelled::forContext('req-1', $context);
        self::assertInstanceOf(WorkflowTimeoutCleanupRequested::class, $exception);
        self::assertInstanceOf(WorkflowCancelled::class, $exception);
        self::assertSame($context, $exception->context);
        self::assertSame('req-1', $exception->requestId);
    }

    public function testTimeoutCleanupStartedFieldsAreNotPartOfTheContext(): void
    {
        // The timeout kind and work deadline live on WorkflowTimeoutCleanupStarted.
        // In a context they are unknown keys, read and written back as Core does.
        $context = CancellationContext::fromArray([
            ...self::timeoutSnapshot(),
            'timeout_kind' => 'execution',
            'work_deadline_at' => '2026-11-13T00:01:00.000000Z',
        ]);

        self::assertTrue($context->isTimeout());
        self::assertSame(self::timeoutSnapshot(), $context->toArray());
    }

    public function testADescendantIsCancelledNotTimedOut(): void
    {
        $parent = CancellationContext::fromArray(self::timeoutSnapshot());
        $descendant = $parent->toArray();
        // Built as Core's forDescendant does: the descendant was cancelled
        // because its ancestor was, so it carries no cause.
        unset($descendant['cause']);
        $descendant['request_id'] = 'req-child';
        $descendant['parent_request_id'] = 'req-1';
        $descendant['lineage'][] = [
            'request_id' => 'req-child',
            'workflow_instance_id' => 'child-instance',
            'workflow_run_id' => 'child-run',
        ];
        $context = CancellationContext::fromArray($descendant);

        self::assertFalse($context->isTimeout());
        self::assertSame($descendant, $context->toArray());
        self::assertSame(WorkflowCancelled::class, WorkflowCancelled::forContext('req-child', $context)::class);
    }

    public function testAScopedDescendantOfATimeoutIsCancelledNotTimedOut(): void
    {
        $origin = ScopedCancellationContext::fromRunContext(CancellationContext::fromArray(self::timeoutSnapshot()));
        // Built as Core's scopedDescendantSnapshot does: the root's cause is
        // stripped from the child, while scope_origin.root_context keeps it.
        $root = $origin->rootContext->toArray();
        unset($root['cause']);
        $deadline = $origin->deadline()->format('Y-m-d\TH:i:s.u\Z');
        $child = [
            ...$root,
            'schema' => 'durable-workflow.cancellation-context/v2',
            'request_id' => 'req-child',
            'parent_request_id' => $origin->requestId,
            'cleanup_deadline_at' => $deadline,
            'lineage' => [$root['lineage'][0], [
                'request_id' => 'req-child',
                'workflow_instance_id' => 'child-instance',
                'workflow_run_id' => 'child-run',
            ]],
            'scope_origin' => $origin->toArray(),
            'scope_authority_deadline_at' => $deadline,
        ];
        self::assertSame('timeout', $child['scope_origin']['root_context']['cause']);
        $context = CancellationContext::fromArray($child);

        self::assertFalse($context->isTimeout());
        self::assertNull($context->cause);
        self::assertTrue($context->scopeOrigin?->rootContext->isTimeout());
        self::assertArrayNotHasKey('cause', $context->toArray());
        self::assertSame($child, $context->toArray());
        self::assertFalse(CancellationContext::fromArray($context->toArray())->isTimeout());
        self::assertSame(WorkflowCancelled::class, WorkflowCancelled::forContext('req-child', $context)::class);
    }

    public function testAScopedRootFromATimeoutRunContextKeepsItsCause(): void
    {
        $scoped = ScopedCancellationContext::fromRunContext(CancellationContext::fromArray(self::timeoutSnapshot()));

        self::assertTrue($scoped->rootContext->isTimeout());
        self::assertSame(self::timeoutSnapshot(), $scoped->rootContext->toArray());
        self::assertSame($scoped->toArray(), ScopedCancellationContext::fromArray($scoped->toArray())->toArray());
    }

    /** @param array<string, mixed> $snapshot */
    #[DataProvider('invalidCauseProvider')]
    public function testAnInvalidCauseIsRefused(array $snapshot): void
    {
        $this->expectException(InvalidArgumentException::class);

        CancellationContext::fromArray($snapshot);
    }

    /** @return array<string, array{array<string, mixed>}> */
    public static function invalidCauseProvider(): array
    {
        return [
            'unknown cause' => [[...self::snapshot(), 'cause' => 'deadline']],
            'empty cause' => [[...self::snapshot(), 'cause' => '']],
            'uppercase cause' => [[...self::snapshot(), 'cause' => 'TIMEOUT']],
            'non-string cause' => [[...self::snapshot(), 'cause' => true]],
            'list cause' => [[...self::snapshot(), 'cause' => ['timeout']]],
        ];
    }

    public function testReplayDeliversTheTimeoutSubtypeToExistingCancellationHandlers(): void
    {
        $caught = [];
        $workflow = static function (WorkflowContext $context) use (&$caught): string {
            try {
                $context->sleep(10);
            } catch (WorkflowCancelled $cancelled) {
                $caught[] = $cancelled;
                self::assertSame($cancelled->context, $context->cancellationContext());
                try {
                    $context->throwIfCancellationRequested();
                } catch (WorkflowCancelled $again) {
                    $caught[] = $again;
                }

                return 'compensated';
            }

            return 'finished';
        };
        $snapshot = self::timeoutSnapshot();
        $result = (new Replayer(new AvroPayloadCodec()))->replay(
            $workflow,
            [self::request($snapshot), self::delivery($snapshot)],
            [],
            'php-workers',
            ['run_id' => 'run-1'],
        );

        self::assertSame('complete_workflow', $result->commands[0]['type']);
        self::assertCount(2, $caught);
        foreach ($caught as $exception) {
            self::assertInstanceOf(WorkflowTimeoutCleanupRequested::class, $exception);
            self::assertSame('req-1', $exception->requestId);
            self::assertInstanceOf(CancellationContext::class, $exception->context);
            self::assertTrue($exception->context->isTimeout());
            self::assertSame($snapshot, $exception->context->toArray());
        }
    }

    public function testReplayDeliversThePlainCancellationForAnOrdinaryContext(): void
    {
        $caught = null;
        $workflow = static function (WorkflowContext $context) use (&$caught): string {
            try {
                $context->sleep(10);
            } catch (WorkflowCancelled $cancelled) {
                $caught = $cancelled;
            }

            return 'done';
        };
        $snapshot = self::snapshot();
        (new Replayer(new AvroPayloadCodec()))->replay(
            $workflow,
            [self::request($snapshot), self::delivery($snapshot)],
            [],
            'php-workers',
            ['run_id' => 'run-1'],
        );

        self::assertNotNull($caught);
        self::assertSame(WorkflowCancelled::class, $caught::class);
        self::assertSame($snapshot, $caught->context?->toArray());
    }

    /** @return array<string, mixed> */
    private static function snapshot(): array
    {
        return [
            'schema' => 'durable-workflow.cancellation-context/v1',
            'request_id' => 'req-1',
            'root_request_id' => 'req-1',
            'root_workflow_instance_id' => 'instance-1',
            'root_workflow_run_id' => 'run-1',
            'parent_request_id' => null,
            'reason' => 'work deadline passed',
            'requester' => [
                'id' => 'timeout_cleanup',
                'type' => 'system',
            ],
            'source' => 'system',
            'requested_at' => '2026-11-13T00:01:00.000000Z',
            'cleanup_deadline_at' => '2026-11-13T00:01:10.000000Z',
            'lineage' => [[
                'request_id' => 'req-1',
                'workflow_instance_id' => 'instance-1',
                'workflow_run_id' => 'run-1',
            ]],
        ];
    }

    /** @return array<string, mixed> */
    private static function timeoutSnapshot(): array
    {
        return [...self::snapshot(), 'cause' => 'timeout'];
    }

    /**
     * @param array<string, mixed> $snapshot
     * @return array<string, mixed>
     */
    private static function request(array $snapshot): array
    {
        return ['event_type' => 'CooperativeCancellationRequested', 'recorded_at' => '2026-11-13T00:01:00Z', 'payload' => [
            'workflow_run_id' => 'run-1', 'workflow_instance_id' => 'instance-1',
            'workflow_command_id' => 'req-1', 'cleanup_deadline_at' => '2026-11-13T00:01:10.000000Z',
            'reason' => 'work deadline passed', 'cancellation' => $snapshot,
        ]];
    }

    /**
     * @param array<string, mixed> $snapshot
     * @return array<string, mixed>
     */
    private static function delivery(array $snapshot): array
    {
        return ['event_type' => 'CooperativeCancellationDelivered', 'payload' => [
            'workflow_run_id' => 'run-1', 'workflow_command_id' => 'req-1',
            'sequence' => 1, 'call_kind' => 'timer', 'cancellation' => $snapshot,
        ]];
    }
}
