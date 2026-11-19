<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ServerCapabilityUnavailable;
use DurableWorkflow\Exception\SignalWithStartFailed;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Model\ClusterInfo;
use DurableWorkflow\Tests\Support\FakeTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/** Atomic signal-with-start intake (.github#222, server#1133): one request, one transaction. */
final class SignalWithStartTest extends TestCase
{
    private const CLUSTER = ['version' => '3.15.0', 'capabilities' => ['signal_with_start' => true]];

    private const RECEIPT = [
        'workflow_id' => 'order 1', 'run_id' => 'run-1', 'signal_name' => 'changed', 'command_id' => 'cmd-2',
        'command_sequence' => 2, 'command_status' => 'accepted', 'outcome' => 'signal_received',
        'start_command_id' => 'cmd-1', 'start_command_sequence' => 1, 'start_outcome' => 'started_new',
        'start_command_status' => 'accepted', 'intake_group_id' => 'group-1', 'request_id' => 'order-1:rev-1',
        'retained' => false, 'intent_verified' => true,
    ];

    public function testClusterInfoReportsTheCapability(): void
    {
        self::assertTrue(ClusterInfo::fromArray(self::CLUSTER)->supportsSignalWithStart());
        self::assertFalse(ClusterInfo::fromArray(['capabilities' => ['signal_with_start' => 'true']])->supportsSignalWithStart());
        self::assertFalse(ClusterInfo::fromArray([])->supportsSignalWithStart());
    }

    public function testOneRequestCarriesTheStartAndTheSignal(): void
    {
        $codec = new AvroPayloadCodec();
        $transport = new FakeTransport([self::CLUSTER, self::RECEIPT]);
        $client = new Client('https://server.example', transport: $transport);

        $receipt = $client->signalWithStart(
            'orders.entity', 'order 1', 'orders', 'changed', [1],
            input: ['order 1'], requestId: 'order-1:rev-1', closedRunPolicy: 'start_new_run',
            memo: ['tenant' => 'a'], businessKey: 'order-1',
        );

        self::assertSame('started_new', $receipt['start_outcome']);
        self::assertSame('cmd-2', $receipt['command_id']);
        self::assertCount(2, $transport->requests);
        $intake = $transport->requests[1];
        self::assertSame('POST', $intake['method']);
        self::assertStringEndsWith('/workflows/order%201/signal-with-start/changed', $intake['uri']);
        $body = $intake['body'] ?? [];
        self::assertSame('orders.entity', $body['workflow_type']);
        self::assertSame('orders', $body['task_queue']);
        self::assertSame('order-1:rev-1', $body['request_id']);
        self::assertSame('start_new_run', $body['closed_run_policy']);
        self::assertSame(['tenant' => 'a'], $body['memo']);
        self::assertSame('order-1', $body['business_key']);
        self::assertArrayNotHasKey('duplicate_policy', $body);
        self::assertSame($codec->envelope(['order 1']), $body['input']);
        self::assertSame($codec->envelope([1]), $body['signal_input']);
    }

    public function testAGeneratedKeyIsSentAndReturned(): void
    {
        $transport = new FakeTransport([self::CLUSTER, ['start_outcome' => 'returned_existing_active']]);
        $client = new Client('https://server.example', transport: $transport);

        $receipt = $client->signalWithStart('orders.entity', 'order-1', 'orders', 'changed');

        $sent = $transport->requests[1]['body']['request_id'] ?? null;
        self::assertIsString($sent);
        self::assertStringStartsWith('sdk-php-signal-with-start-', $sent);
        self::assertSame($sent, $receipt['request_id']);
        self::assertArrayNotHasKey('signal_input', $transport->requests[1]['body'] ?? []);
    }

    /** @return iterable<string, array{int, string}> */
    public static function refusals(): iterable
    {
        yield 'closed run' => [409, 'run_not_active'];
        yield 'open run under fail' => [409, 'instance_already_started'];
        yield 'another type' => [409, 'workflow_type_conflict'];
        yield 'another queue' => [409, 'task_queue_conflict'];
        yield 'another namespace' => [409, 'workflow_id_reserved_in_namespace'];
        yield 'undeclared signal' => [404, 'unknown_signal'];
        yield 'invalid arguments' => [422, 'invalid_signal_arguments'];
        yield 'key conflict' => [409, 'request_id_conflict'];
    }

    #[DataProvider('refusals')]
    public function testEveryRefusalIsTypedAndSentOnce(int $status, string $reason): void
    {
        $transport = new FakeTransport([
            self::CLUSTER,
            new TransportException('refused', $status, ['reason' => $reason, 'workflow_id' => 'order-1']),
        ]);
        $client = new Client('https://server.example', transport: $transport);

        try {
            $client->signalWithStart('orders.entity', 'order-1', 'orders', 'changed', [1], requestId: 'k-1');
            self::fail('The refusal must throw.');
        } catch (SignalWithStartFailed $failure) {
            self::assertSame($status, $failure->status);
            self::assertSame($reason, $failure->reason());
            self::assertSame('k-1', $failure->requestId());
            self::assertSame($reason === 'request_id_conflict', $failure->isRequestIdConflict());
        }
        self::assertCount(2, $transport->requests);
    }

    public function testARuntimeWithoutTheCapabilityIsRefusedBeforeAnyWrite(): void
    {
        $transport = new FakeTransport([['version' => '3.14.0', 'capabilities' => []]]);
        $client = new Client('https://server.example', transport: $transport);

        try {
            $client->signalWithStart('orders.entity', 'order-1', 'orders', 'changed');
            self::fail('A Server without the capability must refuse locally.');
        } catch (ServerCapabilityUnavailable $unavailable) {
            self::assertSame('signal_with_start', $unavailable->capability);
        }
        self::assertCount(1, $transport->requests);
        self::assertSame('GET', $transport->requests[0]['method']);
    }

    public function testAnUnknownClosedRunPolicyIsRefusedLocally(): void
    {
        $transport = new FakeTransport([]);
        $client = new Client('https://server.example', transport: $transport);

        $this->expectException(\InvalidArgumentException::class);
        try {
            $client->signalWithStart('orders.entity', 'order-1', 'orders', 'changed', closedRunPolicy: 'reopen');
        } finally {
            self::assertSame([], $transport->requests);
        }
    }
}
