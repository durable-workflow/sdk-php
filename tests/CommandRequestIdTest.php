<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\SignalFailed;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Exception\UpdateFailed;
use DurableWorkflow\Tests\Support\FakeTransport;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;

/** The caller command key (`request_id`) that makes signal and update retries safe. */
final class CommandRequestIdTest extends TestCase
{
    private const SIGNAL_KEY_PATTERN = '/^sdk-php-signal-[0-9a-f]{32}$/';
    private const UPDATE_KEY_PATTERN = '/^sdk-php-update-[0-9a-f]{32}$/';
    private const CONFLICT = [
        'reason' => 'request_id_conflict',
        'message' => 'Request ID approve-1 retained a command with a different intent.',
        'request_id' => 'approve-1',
        'retained_command_id' => 'cmd-9',
        'retained_command_type' => 'signal',
        'retained_outcome' => 'accepted',
        'conflict' => ['arguments'],
    ];

    public function testSignalWithoutAKeySendsAGeneratedKeyAndReturnsIt(): void
    {
        $transport = new FakeTransport([['accepted' => true, 'command_id' => 'cmd-1']]);
        $client = new Client('https://server.example', transport: $transport);

        $receipt = $client->signalWorkflow('order-1', 'approve', ['Ada']);

        $sent = $transport->requests[0]['body']['request_id'];
        self::assertMatchesRegularExpression(self::SIGNAL_KEY_PATTERN, $sent);
        self::assertSame(['accepted' => true, 'command_id' => 'cmd-1', 'request_id' => $sent], $receipt);
        self::assertSame('https://server.example/api/workflows/order-1/signal/approve', $transport->requests[0]['uri']);
        self::assertSame(['Ada'], $client->payloadCodec()->decodeEnvelope($transport->requests[0]['body']['input']));
    }

    public function testUpdateWithoutAKeySendsAGeneratedKeyAndTheReceiptCarriesIt(): void
    {
        $transport = new FakeTransport([
            ['update_id' => 'u-1', 'status' => 'completed', 'result_envelope' => (new AvroPayloadCodec())->envelope('renamed')],
            ['update_id' => 'u-2', 'status' => 'accepted'],
        ]);
        $client = new Client('https://server.example', transport: $transport);

        self::assertSame('renamed', $client->updateWorkflow('order-1', 'rename', ['Grace']));
        $first = $transport->requests[0]['body']['request_id'];
        self::assertMatchesRegularExpression(self::UPDATE_KEY_PATTERN, $first);
        self::assertSame('completed', $transport->requests[0]['body']['wait_for']);

        $receipt = $client->updateWorkflowReceipt('order-1', 'rename', ['Grace'], 'accepted');
        $second = $transport->requests[1]['body']['request_id'];
        self::assertMatchesRegularExpression(self::UPDATE_KEY_PATTERN, $second);
        self::assertNotSame($first, $second);
        self::assertSame(['update_id' => 'u-2', 'status' => 'accepted', 'request_id' => $second], $receipt);
    }

    public function testAnExplicitKeyIsSentVerbatimForSignalsAndUpdates(): void
    {
        $key = 'Order/1 Approve:Ünïcode';
        $transport = new FakeTransport([[], ['update_id' => 'u-1', 'status' => 'accepted']]);
        $client = new Client('https://server.example', transport: $transport);

        $signal = $client->signalWorkflow('order-1', 'approve', ['Ada'], 'run-1', $key);
        $update = $client->updateWorkflow('order-1', 'rename', ['Grace'], 'accepted', 5, $key, 'run-1');

        self::assertSame($key, $transport->requests[0]['body']['request_id']);
        self::assertSame(['request_id' => $key], $signal);
        self::assertSame('https://server.example/api/workflows/order-1/runs/run-1/signal/approve', $transport->requests[0]['uri']);
        self::assertSame($key, $transport->requests[1]['body']['request_id']);
        self::assertSame(5, $transport->requests[1]['body']['wait_timeout_seconds']);
        self::assertSame(['update_id' => 'u-1', 'status' => 'accepted', 'request_id' => $key], $update);
        self::assertSame('https://server.example/api/workflows/order-1/runs/run-1/update/rename', $transport->requests[1]['uri']);
    }

    public function testTheSameKeyAppearsInTheRequestBodyAndTheTransportFailure(): void
    {
        $transport = new FakeTransport([
            new TransportException('Signal reply lost.', transientConnectionFailure: true),
            new TransportException('Update reply lost.', transientConnectionFailure: true),
        ]);
        $client = new Client('https://server.example', transport: $transport);

        try {
            $client->signalWorkflow('order-1', 'approve', ['Ada']);
            self::fail('A lost signal reply must surface as SignalFailed.');
        } catch (SignalFailed $exception) {
            self::assertCount(1, $transport->requests);
            self::assertSame($transport->requests[0]['body']['request_id'], $exception->requestId());
            self::assertMatchesRegularExpression(self::SIGNAL_KEY_PATTERN, (string) $exception->requestId());
            self::assertNull($exception->reason());
            self::assertSame([], $exception->details());
            self::assertFalse($exception->isRequestIdConflict());
            self::assertTrue($exception->isTransientConnectionFailure());
        }

        try {
            $client->updateWorkflow('order-1', 'rename', ['Grace']);
            self::fail('A lost update reply must surface as UpdateFailed.');
        } catch (UpdateFailed $exception) {
            self::assertCount(2, $transport->requests);
            self::assertSame($transport->requests[1]['body']['request_id'], $exception->requestId());
            self::assertMatchesRegularExpression(self::UPDATE_KEY_PATTERN, (string) $exception->requestId());
            self::assertNull($exception->reason());
            self::assertSame([], $exception->details());
            self::assertFalse($exception->isRequestIdConflict());
            self::assertTrue($exception->isTransientConnectionFailure());
        }
    }

    public function testARequestIdConflictIsATypedFailureThatNamesTheRetainedCommand(): void
    {
        $updateConflict = ['request_id' => 'rename-1', 'retained_command_type' => 'update', 'conflict' => ['name', 'target']]
            + self::CONFLICT;
        $transport = new FakeTransport([
            new TransportException('Request ID approve-1 retained a different signal.', 409, self::CONFLICT),
            new TransportException('Request ID rename-1 retained a different update.', 409, $updateConflict),
        ]);
        $client = new Client('https://server.example', transport: $transport);

        try {
            $client->signalWorkflow('order-1', 'approve', ['Grace'], null, 'approve-1');
            self::fail('A conflicting signal key must surface as SignalFailed.');
        } catch (SignalFailed $exception) {
            self::assertSame(409, $exception->status);
            self::assertSame('request_id_conflict', $exception->reason());
            self::assertTrue($exception->isRequestIdConflict());
            self::assertSame('approve-1', $exception->requestId());
            self::assertSame(self::CONFLICT, $exception->details());
            self::assertSame('cmd-9', $exception->details()['retained_command_id']);
            self::assertSame('Request ID approve-1 retained a different signal.', $exception->getMessage());
        }

        try {
            $client->updateWorkflow('order-1', 'rename', ['Grace'], requestId: 'rename-1');
            self::fail('A conflicting update key must surface as UpdateFailed.');
        } catch (UpdateFailed $exception) {
            self::assertSame(409, $exception->status);
            self::assertSame('request_id_conflict', $exception->reason());
            self::assertTrue($exception->isRequestIdConflict());
            self::assertSame('rename-1', $exception->requestId());
            self::assertSame('cmd-9', $exception->details()['retained_command_id']);
            self::assertSame('update', $exception->details()['retained_command_type']);
            self::assertSame(['name', 'target'], $exception->details()['conflict']);
        }
    }

    public function testARetainedReceiptIsReturnedUnchangedWithItsKey(): void
    {
        $signalReceipt = [
            'command_id' => 'cmd-1',
            'status' => 'received',
            'request_id' => 'approve-1',
            'retained' => true,
            'intent_verified' => true,
        ];
        $updateReceipt = [
            'update_id' => 'u-1',
            'status' => 'completed',
            'result_envelope' => (new AvroPayloadCodec())->envelope('renamed'),
            'request_id' => 'rename-1',
            'retained' => true,
            'intent_verified' => true,
        ];
        $transport = new FakeTransport([$signalReceipt, $updateReceipt, $updateReceipt]);
        $client = new Client('https://server.example', transport: $transport);

        self::assertSame($signalReceipt, $client->signalWorkflow('order-1', 'approve', ['Ada'], null, 'approve-1'));
        self::assertSame($updateReceipt, $client->updateWorkflowReceipt('order-1', 'rename', ['Grace'], requestId: 'rename-1'));
        self::assertSame('renamed', $client->updateWorkflow('order-1', 'rename', ['Grace'], requestId: 'rename-1'));
        foreach ($transport->requests as $index => $request) {
            self::assertSame($index === 0 ? 'approve-1' : 'rename-1', $request['body']['request_id']);
        }
    }

    public function testHandleMethodsPassTheKeyThrough(): void
    {
        $transport = new FakeTransport([[], [], [], [], []]);
        $client = new Client('https://server.example', transport: $transport);
        $handle = $client->workflowHandle('order/1', 'run/2');

        $handle->signal('approve', ['Ada'], 'sig-1');
        $handle->signalSelectedRun('approve', ['Ada'], 'sig-2');
        $handle->update('rename', ['Grace'], 'accepted', 5, 'upd-1');
        $handle->updateSelectedRun('rename', ['Grace'], requestId: 'upd-2');
        $handle->signal('approve');

        $bodies = array_column($transport->requests, 'body');
        self::assertSame(['sig-1', 'sig-2', 'upd-1', 'upd-2'], array_slice(array_column($bodies, 'request_id'), 0, 4));
        self::assertMatchesRegularExpression(self::SIGNAL_KEY_PATTERN, $bodies[4]['request_id']);
        self::assertSame('accepted', $bodies[2]['wait_for']);
        self::assertSame(5, $bodies[2]['wait_timeout_seconds']);
        self::assertSame([
            'https://server.example/api/workflows/order%2F1/signal/approve',
            'https://server.example/api/workflows/order%2F1/runs/run%2F2/signal/approve',
            'https://server.example/api/workflows/order%2F1/update/rename',
            'https://server.example/api/workflows/order%2F1/runs/run%2F2/update/rename',
            'https://server.example/api/workflows/order%2F1/signal/approve',
        ], array_column($transport->requests, 'uri'));
    }

    public function testTheKeyTravelsInTheBodyAndNeverInARequestIdHeader(): void
    {
        $transport = new FakeTransport([[], []]);
        $client = new Client('https://server.example', transport: $transport, token: 'token', namespace: 'tenant-a');

        $client->signalWorkflow('order-1', 'approve', ['Ada'], null, 'approve-1');
        $client->updateWorkflow('order-1', 'rename', ['Grace'], requestId: 'rename-1');

        self::assertCount(2, $transport->requests);
        foreach ($transport->requests as $request) {
            self::assertNotContains('x-request-id', array_map(strtolower(...), array_keys($request['headers'])));
            self::assertSame('tenant-a', $request['headers']['X-Namespace']);
            self::assertArrayHasKey('request_id', $request['body']);
        }
    }

    public function testAnInvalidKeyIsRejectedBeforeAnyRequestIsSent(): void
    {
        $transport = new FakeTransport([[]]);
        $client = new Client('https://server.example', transport: $transport);

        foreach (['', str_repeat('k', 256)] as $invalid) {
            foreach (['signal', 'update'] as $command) {
                try {
                    $command === 'signal'
                        ? $client->signalWorkflow('order-1', 'approve', [], null, $invalid)
                        : $client->updateWorkflow('order-1', 'rename', [], requestId: $invalid);
                    self::fail("A {$command} must reject an invalid request ID.");
                } catch (InvalidArgumentException $exception) {
                    self::assertSame(ucfirst($command).' request IDs must be from 1 to 255 bytes.', $exception->getMessage());
                }
            }
        }
        self::assertSame([], $transport->requests);

        $limit = str_repeat('k', 255);
        $client->signalWorkflow('order-1', 'approve', [], null, $limit);
        self::assertSame($limit, $transport->requests[0]['body']['request_id']);
    }

    public function testThePublishedServerUnicodeKeyBoundaryIsPreserved(): void
    {
        $key = str_repeat('é', 255);
        $transport = new FakeTransport([[], []]);
        $client = new Client('https://server.example', transport: $transport);
        $client->signalWorkflow('order-1', 'approve', [], requestId: $key);
        $client->updateWorkflowReceipt('order-1', 'approve', [], requestId: $key);
        self::assertSame([$key, $key], array_column(array_column($transport->requests, 'body'), 'request_id'));
    }
}
