<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
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

    #[DataProvider('validOutboundPayloadProvider')]
    public function testValidScheduleActionInputAndCodecLookingMetadataAreSentUnchanged(mixed $payload): void
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
                'input' => $payload,
            ],
            'memo' => $codecMetadata,
            'search_attributes' => ['CodecMetadata' => $codecMetadata],
        ];

        $client->updateSchedule('schedule-1', $changes);

        self::assertCount(1, $transport->requests);
        self::assertSame($changes, $transport->requests[0]['body']);
    }

    #[DataProvider('validOutboundPayloadProvider')]
    public function testValidWorkflowCommandPayloadsAreSentUnchanged(mixed $payload): void
    {
        $transport = new FakeTransport([['outcome' => 'completed']]);
        $client = new Client('https://server.example', transport: $transport);
        $commands = [];
        foreach (self::workflowCommandPayloadFields() as $type => $field) {
            $commands[] = [
                'type' => $type,
                $field => $payload,
                'payload_codec' => 'avro',
            ];
        }

        $client->completeWorkflowTask('task-1', 'worker-1', 1, $commands);

        self::assertCount(1, $transport->requests);
        self::assertSame($commands, $transport->requests[0]['body']['commands']);
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
        $avro = (new AvroPayloadCodec())->encode(['valid' => true]);

        yield 'JSON-tagged envelope' => [[
            'codec' => 'json',
            'blob' => base64_encode($json),
        ]];
        yield 'unknown-codec envelope' => [[
            'codec' => 'custom',
            'blob' => base64_encode($json),
        ]];
        yield 'missing-codec envelope with valid Avro bytes' => [[
            'blob' => $avro,
        ]];
        yield 'missing-codec external envelope' => [[
            'external_storage' => self::externalStorageReference($avro),
        ]];
        yield 'inline envelope with an extra wrapper key' => [[
            'codec' => 'avro',
            'blob' => $avro,
            'metadata' => [],
        ]];
        yield 'raw JSON document' => [$json];
        yield 'falsely Avro-tagged bytes' => [[
            'codec' => 'avro',
            'blob' => base64_encode('not-an-avro-value'),
        ]];
        yield 'inline envelope with a non-string blob' => [[
            'codec' => 'avro',
            'blob' => ['not-a-string'],
        ]];
        yield 'inline envelope with a missing blob' => [[
            'codec' => 'avro',
        ]];
        yield 'inline envelope with both payload forms' => [[
            'codec' => 'avro',
            'blob' => $avro,
            'external_storage' => self::externalStorageReference($avro),
        ]];
        yield 'external envelope with an extra wrapper key' => [[
            'codec' => 'avro',
            'external_storage' => self::externalStorageReference($avro),
            'metadata' => [],
        ]];
        yield 'external envelope with a non-object reference' => [[
            'codec' => 'avro',
            'external_storage' => 'not-an-object',
        ]];
        yield 'external envelope with an incomplete reference' => [[
            'codec' => 'avro',
            'external_storage' => ['codec' => 'avro'],
        ]];
        yield 'external envelope with a mismatched reference codec' => [[
            'codec' => 'avro',
            'external_storage' => [
                ...self::externalStorageReference($avro),
                'codec' => 'json',
            ],
        ]];
    }

    /** @return iterable<string, array{mixed}> */
    public static function validOutboundPayloadProvider(): iterable
    {
        $avro = (new AvroPayloadCodec())->encode(['valid' => true]);

        yield 'raw Avro bytes' => [$avro];
        yield 'inline Avro envelope' => [[
            'codec' => 'avro',
            'blob' => $avro,
        ]];
        yield 'external-storage Avro envelope' => [[
            'codec' => 'avro',
            'external_storage' => self::externalStorageReference($avro),
        ]];
    }

    /** @return iterable<string, array{string, string, mixed}> */
    public static function unsupportedWorkflowCommandPayloadProvider(): iterable
    {
        foreach (self::workflowCommandPayloadFields() as $type => $field) {
            foreach (self::unsupportedPayloadProvider() as $name => [$payload]) {
                yield "{$type} {$name}" => [$type, $field, $payload];
            }
        }
    }

    /** @return array<string, string> */
    private static function workflowCommandPayloadFields(): array
    {
        return [
            'complete_workflow' => 'result',
            'schedule_activity' => 'arguments',
            'start_child_workflow' => 'arguments',
            'continue_as_new' => 'arguments',
            'complete_update' => 'result',
            'record_side_effect' => 'result',
            'start_service_operation' => 'request_payload',
        ];
    }

    /** @return array{schema: string, uri: string, sha256: string, size_bytes: int, codec: string} */
    private static function externalStorageReference(string $payload): array
    {
        return [
            'schema' => 'durable-workflow.v2.external-payload-reference.v1',
            'uri' => 's3://workflow-payloads/'.hash('sha256', $payload),
            'sha256' => hash('sha256', $payload),
            'size_bytes' => strlen($payload),
            'codec' => 'avro',
        ];
    }
}
