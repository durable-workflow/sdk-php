<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\CodecException;
use DurableWorkflow\Tests\Support\FakeTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ClientOutboundPayloadValidationTest extends TestCase
{
    #[DataProvider('unsupportedPayloadProvider')]
    public function testScheduleUpdatesRejectUnsupportedActionInputBeforeTransport(mixed $payload): void
    {
        $transport = new FakeTransport();
        $client = new Client('https://server.example', transport: $transport);

        try {
            $client->updateSchedule('schedule-1', ['action' => ['input' => $payload]]);
            self::fail('Expected the schedule action input to be rejected.');
        } catch (CodecException $exception) {
            self::assertStringContainsString('unsupported_payload_codec', $exception->getMessage());
            self::assertStringContainsString('schedule action input', $exception->getMessage());
        }

        self::assertSame([], $transport->requests);
    }

    #[DataProvider('unsupportedWorkflowCommandPayloadProvider')]
    public function testWorkflowCommandsRejectUnsupportedPayloadsBeforeTransport(
        string $type,
        string $field,
        mixed $payload,
    ): void {
        $transport = new FakeTransport();
        $client = new Client('https://server.example', transport: $transport);

        try {
            $client->completeWorkflowTask('task-1', 'worker-1', 1, [[
                'type' => $type,
                $field => $payload,
            ]]);
            self::fail('Expected the workflow command payload to be rejected.');
        } catch (CodecException $exception) {
            self::assertStringContainsString('unsupported_payload_codec', $exception->getMessage());
            self::assertStringContainsString($field, $exception->getMessage());
        }

        self::assertSame([], $transport->requests);
    }

    public function testWorkflowCommandRejectsNonAvroDeclaredCodecBeforeTransport(): void
    {
        $transport = new FakeTransport();
        $client = new Client('https://server.example', transport: $transport);

        try {
            $client->completeWorkflowTask('task-1', 'worker-1', 1, [[
                'type' => 'record_side_effect',
                'result' => $client->payloadCodec()->encode('valid bytes'),
                'payload_codec' => 'json',
            ]]);
            self::fail('Expected the declared command codec to be rejected.');
        } catch (CodecException $exception) {
            self::assertStringContainsString('unsupported_payload_codec', $exception->getMessage());
        }

        self::assertSame([], $transport->requests);
    }

    public function testValidScheduleActionInputAndCodecLookingMetadataAreSentUnchanged(): void
    {
        $transport = new FakeTransport([[]]);
        $client = new Client('https://server.example', transport: $transport);
        $codecMetadata = [
            'codec' => 'json',
            'payload_codec' => 'custom',
            'blob' => 'user-owned',
        ];
        $changes = [
            'action' => [
                'workflow_type' => 'orders.process',
                'input' => $client->payloadCodec()->envelope([['order_id' => 'order-1']]),
            ],
            'memo' => $codecMetadata,
            'search_attributes' => ['CodecMetadata' => $codecMetadata],
        ];

        $client->updateSchedule('schedule-1', $changes);

        self::assertCount(1, $transport->requests);
        self::assertSame($changes, $transport->requests[0]['body']);
    }

    public function testValidWorkflowCommandPayloadsAndCodecLookingMetadataAreSentUnchanged(): void
    {
        $transport = new FakeTransport([['outcome' => 'completed']]);
        $client = new Client('https://server.example', transport: $transport);
        $envelope = $client->payloadCodec()->envelope(['value' => 1]);
        $codecMetadata = [
            'codec' => 'json',
            'payload_codec' => 'custom',
            'blob' => 'user-owned',
        ];
        $commands = [
            ['type' => 'complete_workflow', 'result' => $envelope, 'memo' => $codecMetadata],
            ['type' => 'schedule_activity', 'arguments' => $envelope],
            ['type' => 'start_child_workflow', 'arguments' => $envelope],
            ['type' => 'continue_as_new', 'arguments' => $envelope],
            ['type' => 'complete_update', 'result' => $envelope],
            [
                'type' => 'record_side_effect',
                'result' => $client->payloadCodec()->encode(['value' => 1]),
            ],
            [
                'type' => 'start_service_operation',
                'request_payload' => $envelope,
                'payload_codec' => 'avro',
            ],
            ['type' => 'upsert_search_attributes', 'attributes' => $codecMetadata],
            ['type' => 'fail_workflow', 'message' => 'failed', 'exception' => $codecMetadata],
        ];

        $response = $client->completeWorkflowTask('task-1', 'worker-1', 1, $commands);

        self::assertSame('completed', $response['outcome']);
        self::assertCount(1, $transport->requests);
        self::assertSame($commands, $transport->requests[0]['body']['commands']);
    }

    /** @return iterable<string, array{mixed}> */
    public static function unsupportedPayloadProvider(): iterable
    {
        $json = '{"legacy":true}';

        yield 'JSON-tagged envelope' => [[
            'codec' => 'json',
            'blob' => base64_encode($json),
        ]];
        yield 'unknown-codec envelope' => [[
            'codec' => 'custom',
            'blob' => base64_encode($json),
        ]];
        yield 'untagged JSON envelope' => [[
            'blob' => base64_encode($json),
        ]];
        yield 'raw JSON document' => [$json];
        yield 'falsely Avro-tagged bytes' => [[
            'codec' => 'avro',
            'blob' => base64_encode('not-an-avro-value'),
        ]];
    }

    /** @return iterable<string, array{string, string, mixed}> */
    public static function unsupportedWorkflowCommandPayloadProvider(): iterable
    {
        $jsonEnvelope = [
            'codec' => 'json',
            'blob' => base64_encode('{"legacy":true}'),
        ];
        $payloadFields = [
            'complete_workflow' => 'result',
            'schedule_activity' => 'arguments',
            'start_child_workflow' => 'arguments',
            'continue_as_new' => 'arguments',
            'complete_update' => 'result',
            'record_side_effect' => 'result',
            'start_service_operation' => 'request_payload',
        ];

        foreach ($payloadFields as $type => $field) {
            yield "{$type} JSON-tagged payload" => [$type, $field, $jsonEnvelope];
        }

        foreach (self::unsupportedPayloadProvider() as $name => [$payload]) {
            if ($name === 'JSON-tagged envelope') {
                continue;
            }

            yield "complete_workflow {$name}" => ['complete_workflow', 'result', $payload];
        }
    }
}
