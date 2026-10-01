<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker\CancellationDelivery;
use InvalidArgumentException;
use LogicException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class CooperativeCancellationDeliveryTest extends TestCase
{
    public function testDeliveryUsesWorkerCredentialsNamespaceAndTheExactLease(): void
    {
        $ack = self::ack();
        $transport = new FakeTransport([$ack]);
        $client = new Client('https://server.example', namespace: 'tenant-a', transport: $transport,
            controlToken: 'control', workerToken: 'worker', workerProtocolVersion: '1.20',
        );
        self::assertSame($ack, $client->deliverWorkflowCancellation('task/1', 'worker-a', 3, self::boundary()));
        self::assertSame('https://server.example/api/worker/workflow-tasks/task%2F1/deliver-cancellation', $transport->requests[0]['uri']);
        self::assertSame('POST', $transport->requests[0]['method']);
        self::assertSame('Bearer worker', $transport->requests[0]['headers']['Authorization']);
        self::assertSame('tenant-a', $transport->requests[0]['headers']['X-Namespace']);
        self::assertSame('1.20', $transport->requests[0]['headers']['X-Durable-Workflow-Protocol-Version']);
        self::assertSame([
            'lease_owner' => 'worker-a', 'workflow_task_attempt' => 3,
            'request_id' => 'request-1', 'sequence' => 4, 'call_kind' => 'timer', 'sequence_span' => 1,
        ], $transport->requests[0]['body']);
    }

    public function testSelectionHandleRetainsOriginalOperationRange(): void
    {
        $ack = [...self::ack(), 'call_kind' => 'selection_handle', 'operation_sequence' => 1, 'operation_sequence_span' => 2];
        $transport = new FakeTransport([$ack]);
        $client = new Client('https://server.example', transport: $transport, workerProtocolVersion: '1.20');
        $client->deliverWorkflowCancellation('task/1', 'worker-a', 3, self::boundary($ack));
        self::assertSame(1, $transport->requests[0]['body']['operation_sequence']);
        self::assertSame(2, $transport->requests[0]['body']['operation_sequence_span']);
    }

    public function testDefaultProtocolRefusesDeliveryBeforeMutation(): void
    {
        $transport = new FakeTransport();
        $client = new Client('https://server.example', transport: $transport);
        try {
            $client->deliverWorkflowCancellation('task/1', 'worker-a', 3, self::boundary());
            self::fail('Default workers must not advertise or deliver this capability.');
        } catch (LogicException) {
            self::assertSame([], $transport->requests);
        }
    }

    public function testExplicitChildPendingReplyDoesNotClaimCanonicalDelivery(): void
    {
        $pending = ['delivered' => false, 'task_id' => 'task/1', 'request_id' => null,
            'sequence' => null, 'call_kind' => null, 'sequence_span' => null,
            'operation_sequence' => null, 'operation_sequence_span' => null,
            'reason' => 'cancellation_waiting_for_child', 'claim_released' => true];
        $transport = new FakeTransport([$pending]);
        $client = new Client('https://server.example', transport: $transport, workerProtocolVersion: '1.20');
        self::assertSame($pending, $client->deliverWorkflowCancellation('task/1', 'worker-a', 3,
            self::boundary(['call_kind' => 'child'])));
    }

    public function testPendingChildReplyCannotBeReturnedForAnUnrelatedTimerCall(): void
    {
        $transport = new FakeTransport([['delivered' => false, 'task_id' => 'task/1',
            'reason' => 'cancellation_waiting_for_child']]);
        $client = new Client('https://server.example', transport: $transport, workerProtocolVersion: '1.20');
        $this->expectException(ServerException::class);
        $client->deliverWorkflowCancellation('task/1', 'worker-a', 3, self::boundary());
    }

    #[DataProvider('invalidPendingProvider')]
    public function testPendingReplyCannotCarryAChangedTaskOrPretendDeliveryOccurred(array $change): void
    {
        $transport = new FakeTransport([[...['delivered' => false, 'task_id' => 'task/1',
            'reason' => 'cancellation_waiting_for_child', 'claim_released' => true], ...$change]]);
        $client = new Client('https://server.example', transport: $transport, workerProtocolVersion: '1.20');
        $this->expectException(ServerException::class);
        $client->deliverWorkflowCancellation('task/1', 'worker-a', 3, self::boundary(['call_kind' => 'child']));
    }

    public static function invalidPendingProvider(): array
    {
        return array_map(static fn (array $change): array => [$change], [
            ['task_id' => 'other'], ['delivered' => 'false'], ['reason' => 'unknown_pending'],
            ['request_id' => 'other'], ['sequence' => 4], ['call_kind' => 'child'],
            ['sequence_span' => 1], ['operation_sequence' => 1], ['operation_sequence_span' => 1],
            ['claim_released' => false], ['claim_released' => 'true'], ['claim_released' => null],
        ]);
    }

    #[DataProvider('invalidLeaseProvider')]
    public function testInvalidLeaseIsRejectedBeforeMutation(string $task, string $owner, int $attempt): void
    {
        $transport = new FakeTransport();
        $client = new Client('https://server.example', transport: $transport, workerProtocolVersion: '1.20');
        try {
            $client->deliverWorkflowCancellation($task, $owner, $attempt, self::boundary());
            self::fail('Invalid leases must fail before mutation.');
        } catch (InvalidArgumentException) {
            self::assertSame([], $transport->requests);
        }
    }

    public static function invalidLeaseProvider(): array
    {
        return [['', 'worker-a', 1], ['task', ' ', 1], ['task', 'worker-a', 0]];
    }

    #[DataProvider('invalidAckProvider')]
    public function testMalformedOrChangedDeliveryAcknowledgmentIsRejected(array $change): void
    {
        $transport = new FakeTransport([[...self::ack(), ...$change]]);
        $client = new Client('https://server.example', transport: $transport, workerProtocolVersion: '1.20');
        try {
            $client->deliverWorkflowCancellation('task/1', 'worker-a', 3, self::boundary());
            self::fail('Acknowledgments must match the complete authored boundary.');
        } catch (ServerException $error) {
            self::assertSame(200, $error->status);
            self::assertSame('invalid_cooperative_cancellation_delivery', $error->reason);
        }
    }

    public static function invalidAckProvider(): array
    {
        return array_map(static fn (array $change): array => [$change], [
            ['delivered' => 'true'], ['delivered' => false], ['task_id' => 'other'],
            ['request_id' => 'other'], ['sequence' => 5], ['sequence' => '4'],
            ['call_kind' => 'activity'], ['sequence_span' => 2], ['sequence_span' => null],
            ['operation_sequence' => 1], ['operation_sequence_span' => null],
        ]);
    }

    public function testOptedInWorkerProtocolDoesNotChangeControlPlaneProtocol(): void
    {
        $transport = new FakeTransport([[], []]);
        $client = new Client('https://server.example', transport: $transport, workerProtocolVersion: '1.20');
        $client->clusterInfo();
        $client->heartbeatWorkflowTask('task', 'worker-a', 3);
        self::assertSame('2', $transport->requests[0]['headers']['X-Durable-Workflow-Control-Plane-Version']);
        self::assertArrayNotHasKey('X-Durable-Workflow-Protocol-Version', $transport->requests[0]['headers']);
        self::assertSame('1.20', $transport->requests[1]['headers']['X-Durable-Workflow-Protocol-Version']);
    }

    public function testUnsupportedProtocolCannotBeAdvertised(): void
    {
        $this->expectException(InvalidArgumentException::class);
        new Client('https://server.example', workerProtocolVersion: '2.0');
    }

    private static function ack(): array
    {
        return ['delivered' => true, 'task_id' => 'task/1', 'request_id' => 'request-1', 'sequence' => 4,
            'call_kind' => 'timer', 'sequence_span' => 1, 'operation_sequence' => null, 'operation_sequence_span' => 1];
    }

    private static function boundary(array $ack = []): CancellationDelivery
    {
        $ack = [...self::ack(), ...$ack];
        return CancellationDelivery::fromPayload([
            'workflow_command_id' => $ack['request_id'], 'sequence' => $ack['sequence'],
            'call_kind' => $ack['call_kind'], 'sequence_span' => $ack['sequence_span'],
            'operation_sequence' => $ack['operation_sequence'], 'operation_sequence_span' => $ack['operation_sequence_span'],
        ]);
    }
}
