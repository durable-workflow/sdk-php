<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Exception\UpdateFailed;
use DurableWorkflow\Exception\UpdateStillPending;
use DurableWorkflow\Tests\Support\FakeTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/** #380: read an accepted update's outcome later by update id (server#1131), never by resubmitting. */
final class UpdateOutcomeReadTest extends TestCase
{
    private const CLUSTER = ['version' => '3.6.0', 'capabilities' => ['update_outcome_reads' => true]];

    public function testACompletedReadReturnsTheDecodedResultIncludingNull(): void
    {
        $codec = new AvroPayloadCodec();
        $transport = new FakeTransport([
            self::CLUSTER,
            ['update_id' => 'upd-1', 'update_status' => 'completed', 'wait_timed_out' => false, 'result_envelope' => $codec->envelope('renamed')],
            ['update_id' => 'upd-2', 'update_status' => 'completed', 'wait_timed_out' => false, 'result_envelope' => $codec->envelope(null)],
        ]);
        $client = new Client('https://server.example', transport: $transport);

        self::assertSame('renamed', $client->updateOutcome('order 1', 'upd-1', waitTimeoutSeconds: 10));
        self::assertNull($client->updateOutcome('order 1', 'upd-2'));

        // Discovery happens once; each read is a GET on the update, never a POST that could admit another update.
        self::assertCount(3, $transport->requests);
        self::assertSame('GET', $transport->requests[1]['method']);
        self::assertStringEndsWith('/workflows/order%201/updates/upd-1?wait_for=completed&wait_timeout_seconds=10', $transport->requests[1]['uri']);
        self::assertNull($transport->requests[1]['body']);
        self::assertStringEndsWith('/workflows/order%201/updates/upd-2?wait_for=completed', $transport->requests[2]['uri']);
    }

    public function testAPendingReadThrowsAndPointsAtReadingAgain(): void
    {
        $transport = new FakeTransport([
            self::CLUSTER,
            ['workflow_id' => 'order-1', 'run_id' => 'run-9', 'update_id' => 'upd-1', 'update_name' => 'rename',
                'request_id' => 'rename-1', 'update_status' => 'accepted', 'result_envelope' => null, 'wait_timed_out' => true],
        ]);
        $client = new Client('https://server.example', transport: $transport);

        try {
            $client->updateOutcome('order-1', 'upd-1', waitTimeoutSeconds: 1);
            self::fail('A pending update must not return a value.');
        } catch (UpdateStillPending $pending) {
            self::assertSame('upd-1', $pending->updateId);
            self::assertSame('run-9', $pending->runId);
            self::assertSame('rename', $pending->updateName);
            self::assertSame('rename-1', $pending->requestId);
            self::assertStringContainsString('updateOutcome() again with update id "upd-1"', $pending->getMessage());
        }
    }

    public function testReadingAgainAfterPendingReturnsTheResult(): void
    {
        $codec = new AvroPayloadCodec();
        $transport = new FakeTransport([
            self::CLUSTER,
            ['update_id' => 'upd-1', 'update_status' => 'accepted', 'wait_timed_out' => true],
            ['update_id' => 'upd-1', 'update_status' => 'completed', 'result_envelope' => $codec->envelope(['ok' => true])],
        ]);
        $client = new Client('https://server.example', transport: $transport);

        try {
            $client->updateOutcome('order-1', 'upd-1');
            self::fail('The first read was still pending.');
        } catch (UpdateStillPending $pending) {
            self::assertSame(['ok' => true], $client->updateOutcome('order-1', (string) $pending->updateId));
        }
    }

    public function testAnAcceptedReadReturnsTheReceiptWithoutWaiting(): void
    {
        $receipt = ['update_id' => 'upd-1', 'update_status' => 'accepted', 'result_envelope' => null, 'wait_timed_out' => false];
        $transport = new FakeTransport([self::CLUSTER, $receipt]);
        $client = new Client('https://server.example', transport: $transport);

        self::assertSame($receipt, $client->updateOutcomeReceipt('order-1', 'upd-1'));
        self::assertStringEndsWith('/workflows/order-1/updates/upd-1', $transport->requests[1]['uri']);
    }

    public function testAServerWithoutTheCapabilityIsRefusedBeforeAnyRead(): void
    {
        $transport = new FakeTransport([['version' => '3.5.0', 'capabilities' => []]]);
        $client = new Client('https://server.example', transport: $transport);

        try {
            $client->updateOutcome('order-1', 'upd-1');
            self::fail('A Server without update_outcome_reads must be refused.');
        } catch (\LogicException $error) {
            self::assertStringContainsString('update_outcome_reads', $error->getMessage());
        }
        self::assertCount(1, $transport->requests, 'Only discovery ran: nothing was read or resubmitted.');
    }

    /** @return iterable<string, array{int, array<string, mixed>}> */
    public static function refusals(): iterable
    {
        yield 'failed handler' => [422, ['update_id' => 'upd-1', 'update_status' => 'failed', 'reason' => 'update_failed', 'failure_message' => 'boom']];
        yield 'unknown or pruned update' => [404, ['reason' => 'update_not_found', 'message' => 'No retained update has this id.']];
    }

    /** @param array<string, mixed> $body */
    #[DataProvider('refusals')]
    public function testFailedAndUnknownUpdatesRaiseUpdateFailed(int $status, array $body): void
    {
        $transport = new FakeTransport([
            self::CLUSTER,
            TransportException::fromResponse($status, $body, json_encode($body, JSON_THROW_ON_ERROR)),
        ]);
        $client = new Client('https://server.example', transport: $transport);

        try {
            $client->updateOutcome('order-1', 'upd-1');
            self::fail('The refusal must surface.');
        } catch (UpdateFailed $error) {
            self::assertSame($status, $error->status);
            self::assertSame($body['reason'], $error->reason);
        }
    }
}
