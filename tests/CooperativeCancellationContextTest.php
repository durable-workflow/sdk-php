<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Exception\WorkflowCancelled;
use DurableWorkflow\Worker\CancellationContext;
use DurableWorkflow\Worker\CancellationHistory;
use DurableWorkflow\Worker\CancellationRequest;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class CooperativeCancellationContextTest extends TestCase
{
    public function testSnapshotIsImmutableAndKeepsTheRootBudget(): void
    {
        $snapshot = self::snapshot();
        $context = CancellationContext::fromArray($snapshot);
        $snapshot['reason'] = 'changed';
        $snapshot['requester']['id'] = 'changed';
        $snapshot['lineage'][0]['request_id'] = 'changed';
        $context->deadline()->modify('+1 hour');
        $context->requestedAt()->modify('+1 day');

        self::assertSame('request-1', $context->requestId);
        self::assertSame('root-1', $context->rootRequestId);
        self::assertSame('root-1', $context->parentRequestId);
        self::assertSame('parent-instance', $context->rootWorkflowInstanceId);
        self::assertSame('parent-run', $context->rootWorkflowRunId);
        self::assertSame('maintenance', $context->reason);
        self::assertSame('operator-1', $context->requester['id']);
        self::assertSame('control_plane', $context->source);
        self::assertSame('2026-10-01T00:00:00.123456+00:00', $context->requestedAt()->format('Y-m-d\TH:i:s.uP'));
        self::assertSame('2026-10-01T00:00:30.123456+00:00', $context->deadline()->format('Y-m-d\TH:i:s.uP'));
        self::assertEquals(self::snapshot(), $context->toArray());
        self::assertSame($context->toArray(), CancellationContext::fromArray($context->toArray())->toArray());
    }

    public function testTimezoneAndObjectKeyOrderNormalizeWithoutChangingLineageOrder(): void
    {
        $snapshot = self::snapshot();
        $snapshot['requested_at'] = '2026-09-30T20:00:00.123456-04:00';
        $snapshot['cleanup_deadline_at'] = '2026-09-30T20:00:30.123456-04:00';
        $snapshot['requester'] = array_reverse($snapshot['requester'], true);
        $snapshot['lineage'] = array_map(static fn (array $entry): array => array_reverse($entry, true), $snapshot['lineage']);
        self::assertSame(CancellationContext::fromArray(self::snapshot())->toArray(), CancellationContext::fromArray($snapshot)->toArray());
    }

    #[DataProvider('invalidSnapshotProvider')]
    public function testInvalidSnapshotIsRejected(array $snapshot): void
    {
        $this->expectException(InvalidArgumentException::class);
        CancellationContext::fromArray($snapshot);
    }

    public static function invalidSnapshotProvider(): array
    {
        $cases = [];
        foreach ([
            'schema' => 'unknown', 'request_id' => '', 'source' => ' ',
            'parent_request_id' => 'wrong', 'root_request_id' => 'wrong',
            'root_workflow_run_id' => 'wrong', 'reason' => [], 'requester' => [],
            'lineage' => [], 'requested_at' => '2026-02-30T00:00:00Z',
            'cleanup_deadline_at' => '2026-10-01T00:00:00.123456Z',
        ] as $field => $value) {
            $cases[$field] = [[...self::snapshot(), $field => $value]];
        }
        $cycle = self::snapshot();
        $cycle['lineage'][1]['workflow_run_id'] = 'parent-run';
        $cases['run cycle'] = [$cycle];
        $cycle['lineage'][1]['workflow_run_id'] = 'run-1';
        $cycle['lineage'][1]['request_id'] = 'root-1';
        $cases['request cycle'] = [$cycle];
        $cases['lineage reordered'] = [[...self::snapshot(), 'lineage' => array_reverse(self::snapshot()['lineage'])]];
        $requester = self::snapshot();
        $requester['requester']['authorization'] = 'unsupported';
        $cases['unrelated requester metadata'] = [$requester];

        return $cases;
    }

    public function testInheritedCanonicalRequestUsesRootTimeEvenWhenLocalAdmissionIsAfterDeadline(): void
    {
        $request = self::request();
        $request['recorded_at'] = '2026-10-01T00:00:31Z';
        $state = CancellationHistory::fromEvents([$request], 'run-1');
        self::assertSame('2026-10-01T00:00:00.123456Z', $state->request?->requestedAt);
        self::assertSame('2026-10-01T00:00:30.123456Z', $state->request?->cleanupDeadlineAt);
        self::assertSame('root-1', $state->request?->context?->rootRequestId);
        self::assertNull($state->delivery);
    }

    public function testObservationRetainsRefreshRouteAndCanonicalHistorySuppliesTheContext(): void
    {
        $observation = self::observation();
        $observed = CancellationHistory::fromEvents([], 'run-1', $observation);
        self::assertNull($observed->request?->context);
        $canonical = CancellationHistory::fromEvents([self::request(), self::delivery()], 'run-1', $observation);
        self::assertSame('opaque-first-page', $canonical->request?->historyRefreshPageToken);
        self::assertSame('2026-10-01T00:00:00.123456Z', $canonical->request?->requestedAt);
        self::assertSame('maintenance', $canonical->request?->context?->reason);
        self::assertSame('root-1', $canonical->request?->context?->rootRequestId);
    }

    #[DataProvider('mismatchedRequestProvider')]
    public function testContextMustMatchItsCanonicalLocalRunAndRequest(array $request): void
    {
        $this->expectException(NonDeterministicWorkflow::class);
        CancellationHistory::fromEvents([$request], 'run-1');
    }

    public static function mismatchedRequestProvider(): array
    {
        $cases = [];
        foreach ([
            'workflow_command_id' => 'other', 'workflow_instance_id' => 'other',
            'cleanup_deadline_at' => '2026-10-01T00:00:35Z',
            'reason' => 'changed', 'cancellation' => null,
        ] as $field => $value) {
            $request = self::request();
            $request['payload'][$field] = $value;
            $cases[$field] = [$request];
        }
        $request = self::request();
        $request['payload']['cancellation']['lineage'][1]['workflow_run_id'] = 'other';
        $cases['context local run'] = [$request];
        $request = self::request();
        $request['recorded_at'] = '2026-09-30T23:59:59Z';
        $cases['request predates origin'] = [$request];

        return $cases;
    }

    public function testDeliveryCannotChangeTheCommittedContext(): void
    {
        $delivery = self::delivery();
        $delivery['payload']['cancellation']['reason'] = 'changed';
        $this->expectException(NonDeterministicWorkflow::class);
        CancellationHistory::fromEvents([self::request(), $delivery], 'run-1');
    }

    public function testColdReplayExposesTheSameContextOnlyAtCommittedDeliveryAndDuringCleanup(): void
    {
        $codec = new AvroPayloadCodec();
        $seen = [];
        $workflow = static function (WorkflowContext $context) use (&$seen): array {
            $seen[] = $context->cancellationContext();
            try {
                $context->sleep(10);
            } catch (WorkflowCancelled $cancelled) {
                self::assertNotNull($cancelled->context);
                self::assertSame($cancelled->context, $context->cancellationContext());
                $context->cancellationShield(static function () use ($context, $cancelled): void {
                    self::assertSame($cancelled->context, $context->cancellationContext());
                    $context->throwIfCancellationRequested();
                    $context->activity('cleanup');
                });
                return $cancelled->context->toArray();
            }
            return [];
        };
        $history = [self::request(), self::delivery()];
        $first = (new Replayer($codec))->replay($workflow, $history, [], 'php-workers', ['run_id' => 'run-1']);
        self::assertSame([null], $seen);
        self::assertSame('cleanup', $first->commands[0]['activity_type']);
        $history[] = ['event_type' => 'ActivityCompleted', 'payload' => [
            'sequence' => 2, 'activity_type' => 'cleanup', 'result' => $codec->envelope('cleaned'),
        ]];
        $cold = (new Replayer($codec))->replay($workflow, $history, [], 'php-workers', ['run_id' => 'run-1']);
        self::assertSame([null, null], $seen);
        self::assertSame('complete_workflow', $cold->commands[0]['type']);
        self::assertSame(CancellationContext::fromArray(self::snapshot())->toArray(), $codec->decodeEnvelope($cold->commands[0]['result']));
    }

    public function testExplicitCheckKeepsTheDeliveredContext(): void
    {
        $workflow = static function (WorkflowContext $context): void {
            try {
                $context->sleep(10);
            } catch (WorkflowCancelled) {
                $context->throwIfCancellationRequested();
            }
        };
        try {
            (new Replayer(new AvroPayloadCodec()))->replay($workflow, [self::request(), self::delivery()], [], 'php-workers', ['run_id' => 'run-1']);
            self::fail('Expected the original cooperative cancellation.');
        } catch (WorkflowCancelled $error) {
            self::assertSame('request-1', $error->requestId);
            self::assertSame(CancellationContext::fromArray(self::snapshot())->toArray(), $error->context?->toArray());
        }
    }

    private static function snapshot(): array
    {
        return json_decode((string) file_get_contents(__DIR__.'/fixtures/cooperative-cancellation-context.json'), true, flags: JSON_THROW_ON_ERROR);
    }

    private static function observation(): CancellationRequest
    {
        return CancellationRequest::fromObservation([
            'request_id' => 'request-1', 'requested_at' => '2026-10-01T00:00:05Z',
            'cleanup_deadline_at' => '2026-10-01T00:00:30.123456Z',
            'history_refresh_page_token' => 'opaque-first-page',
            'cancellation' => [...self::snapshot(), 'reason' => 'untrusted observation'],
        ]);
    }

    private static function request(): array
    {
        return ['event_type' => 'CooperativeCancellationRequested', 'recorded_at' => '2026-10-01T00:00:05Z', 'payload' => [
            'workflow_run_id' => 'run-1', 'workflow_instance_id' => 'child-instance',
            'workflow_command_id' => 'request-1', 'cleanup_deadline_at' => '2026-10-01T00:00:30.123456Z',
            'reason' => 'maintenance', 'cancellation' => self::snapshot(),
        ]];
    }

    private static function delivery(): array
    {
        return ['event_type' => 'CooperativeCancellationDelivered', 'payload' => [
            'workflow_run_id' => 'run-1', 'workflow_command_id' => 'request-1',
            'sequence' => 1, 'call_kind' => 'timer', 'cancellation' => self::snapshot(),
        ]];
    }
}
