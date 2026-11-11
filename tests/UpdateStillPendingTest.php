<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\UpdateStillPending;
use DurableWorkflow\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

/** #380: a completed wait that ends before the update finishes never returns the receipt as a result. */
final class UpdateStillPendingTest extends TestCase
{
    public function testATimedOutCompletedWaitThrowsAnActionablePendingOutcome(): void
    {
        $transport = new FakeTransport([[
            'workflow_id' => 'order-1', 'run_id' => 'run-7', 'update_id' => 'upd-1', 'update_name' => 'rename',
            'update_status' => 'accepted', 'result' => null, 'result_envelope' => null,
            'wait_for' => 'completed', 'wait_timed_out' => true, 'wait_timeout_seconds' => 5,
        ]]);
        $client = new Client('https://server.example', transport: $transport);

        try {
            $client->updateWorkflow('order-1', 'rename', ['Grace'], waitTimeoutSeconds: 5, requestId: 'rename-1');
            self::fail('A pending update must not return a value.');
        } catch (UpdateStillPending $pending) {
            self::assertSame('order-1', $pending->workflowId);
            self::assertSame('run-7', $pending->runId);
            self::assertSame('rename', $pending->updateName);
            self::assertSame('upd-1', $pending->updateId);
            self::assertSame('rename-1', $pending->requestId);
            self::assertSame('accepted', $pending->updateStatus);
            self::assertStringContainsString('requestId "rename-1"', $pending->getMessage());
        }
    }

    public function testAPendingStatusWithoutTheTimeoutFlagIsStillPending(): void
    {
        $transport = new FakeTransport([['update_id' => 'upd-2', 'update_status' => 'accepted']]);
        $client = new Client('https://server.example', transport: $transport);

        $this->expectException(UpdateStillPending::class);
        $client->updateWorkflow('order-1', 'rename', ['Grace']);
    }

    public function testWaitingAgainWithTheSameRequestIdReturnsTheResult(): void
    {
        $codec = new AvroPayloadCodec();
        $transport = new FakeTransport([
            ['update_id' => 'upd-1', 'update_status' => 'accepted', 'wait_timed_out' => true],
            ['update_id' => 'upd-1', 'update_status' => 'completed', 'wait_timed_out' => false,
                'retained' => true, 'result_envelope' => $codec->envelope('renamed')],
        ]);
        $client = new Client('https://server.example', transport: $transport);

        try {
            $client->updateWorkflow('order-1', 'rename', ['Grace']);
            self::fail('The first wait timed out.');
        } catch (UpdateStillPending $pending) {
            self::assertSame('renamed', $client->updateWorkflow('order-1', 'rename', ['Grace'], requestId: $pending->requestId));
        }
        self::assertSame($transport->requests[0]['body']['request_id'], $transport->requests[1]['body']['request_id']);
    }

    public function testACompletedUpdateReturnsItsResultIncludingNull(): void
    {
        $codec = new AvroPayloadCodec();
        $transport = new FakeTransport([
            ['update_id' => 'upd-1', 'update_status' => 'completed', 'wait_timed_out' => false, 'result_envelope' => $codec->envelope(null)],
            ['update_id' => 'upd-2', 'update_status' => 'completed', 'result_envelope' => $codec->envelope(['ok' => true])],
        ]);
        $client = new Client('https://server.example', transport: $transport);

        self::assertNull($client->updateWorkflow('order-1', 'clear'));
        self::assertSame(['ok' => true], $client->updateWorkflow('order-1', 'save'));
    }

    public function testWaitingOnlyForAcceptanceStillReturnsTheReceipt(): void
    {
        $transport = new FakeTransport([['update_id' => 'upd-1', 'update_status' => 'accepted', 'wait_for' => 'accepted']]);
        $client = new Client('https://server.example', transport: $transport);

        $receipt = $client->updateWorkflow('order-1', 'rename', ['Grace'], 'accepted', requestId: 'rename-1');

        self::assertSame(['update_id' => 'upd-1', 'update_status' => 'accepted', 'wait_for' => 'accepted', 'request_id' => 'rename-1'], $receipt);
    }
}
