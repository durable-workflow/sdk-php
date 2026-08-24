<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroBinaryValue;
use DurableWorkflow\Codec\AvroMapValue;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Testing\WorkflowClientFake;
use DurableWorkflow\Version;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\TestCase;

final class MessageStreamTest extends TestCase
{
    public function testReplayConsumesAnOrderedBoundedBatchAndAcknowledgesItsCursor(): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static function (WorkflowContext $context): array {
            return array_map(
                static fn ($message): array => [$message->messageId, $message->position, $message->arguments],
                $context->messageStream('orders')->receive(2),
            );
        };

        $result = (new Replayer($codec))->replay($workflow, [
            self::messageEvent($codec, 'message-1', 1, ['one']),
            self::messageEvent($codec, 'message-2', 2, ['two']),
        ], [], 'php-workers');

        self::assertSame([
            ['message-1', 1, ['one']],
            ['message-2', 2, ['two']],
        ], $codec->decodeEnvelope($result->commands[0]['result']));
        self::assertSame([
            ['stream_name' => 'orders', 'through_position' => 2],
        ], $result->messageStreamCursors);
        self::assertSame([], $result->messageStreamWaits);
    }

    public function testReplayDeduplicatesRepeatedInternalDelivery(): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static fn (WorkflowContext $context): int => count(
            $context->messageStream('orders')->receive(2),
        );

        $result = (new Replayer($codec))->replay($workflow, [
            self::messageEvent($codec, 'message-1', 1, ['one']),
            self::messageEvent($codec, 'message-1', 1, ['one']),
            self::messageEvent($codec, 'message-2', 2, ['two']),
        ], [], 'php-workers');

        self::assertSame(2, $codec->decodeEnvelope($result->commands[0]['result']));
        self::assertSame(2, $result->messageStreamCursors[0]['through_position']);
    }

    public function testEmptyStreamReportsPendingDurableWait(): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static fn (WorkflowContext $context): array => $context->messageStream('orders')->receive();

        $result = (new Replayer($codec))->replay($workflow, [], [], 'php-workers');

        self::assertSame('open_condition_wait', $result->commands[0]['type']);
        self::assertSame('message-stream:orders:0', $result->commands[0]['condition_key']);
        self::assertSame([
            ['stream_name' => 'orders', 'after_position' => 0],
        ], $result->messageStreamWaits);
    }

    public function testArrivingMessageReplaysPendingWaitAcrossWorkerReplacement(): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static fn (WorkflowContext $context): array => array_map(
            static fn ($message): string => $message->messageId,
            $context->messageStream('orders')->receive(),
        );
        $replayer = new Replayer($codec);
        $waiting = $replayer->replay($workflow, [], [], 'php-workers');
        $history = [
            [
                'event_type' => 'ConditionWaitOpened',
                'payload' => [
                    'sequence' => 1,
                    'condition_wait_id' => 'message-stream-wait-1',
                    'condition_key' => $waiting->commands[0]['condition_key'],
                    'condition_definition_fingerprint' => $waiting->commands[0]['condition_definition_fingerprint'],
                ],
            ],
            self::messageEvent($codec, 'message-1', 1, ['one']),
        ];

        $resumedWorker = $replayer->replay($workflow, $history, [], 'php-workers');
        $replacementWorker = (new Replayer($codec))->replay($workflow, $history, [], 'php-workers');

        self::assertSame('complete_workflow', $resumedWorker->commands[0]['type']);
        self::assertSame(['message-1'], $codec->decodeEnvelope($resumedWorker->commands[0]['result']));
        self::assertSame($resumedWorker->commands, $replacementWorker->commands);
        self::assertSame([
            ['stream_name' => 'orders', 'through_position' => 1],
        ], $replacementWorker->messageStreamCursors);
        self::assertSame([], $replacementWorker->messageStreamWaits);
    }

    public function testContinueAsNewCursorCheckpointPreservesTheGlobalPendingPosition(): void
    {
        $codec = new AvroPayloadCodec();
        $workflow = static fn (WorkflowContext $context): array => $context->messageStream('orders')->receive();

        $result = (new Replayer($codec))->replay($workflow, [
            self::cursorEvent($codec, 2),
        ], [], 'php-workers');

        self::assertSame('message-stream:orders:2', $result->commands[0]['condition_key']);
        self::assertSame([
            ['stream_name' => 'orders', 'through_position' => 2],
        ], $result->messageStreamCursors);
        self::assertSame([
            ['stream_name' => 'orders', 'after_position' => 2],
        ], $result->messageStreamWaits);
    }

    public function testTypedAvroArgumentsSurviveDuplicateDeliveryAndWorkerReplacement(): void
    {
        $codec = new AvroPayloadCodec();
        $values = [
            AvroBinaryValue::fromBytes("\x00bytes"),
            1,
            1.0,
            [],
            AvroMapValue::fromPairs([]),
            ['nested' => [AvroBinaryValue::fromBytes('value')]],
        ];
        $workflow = static fn (WorkflowContext $context): array => array_map(
            static fn ($message): array => $message->arguments,
            $context->messageStream('orders')->receive(2),
        );
        $history = [
            self::messageEvent($codec, 'message-1', 1, $values),
            self::messageEvent($codec, 'message-1', 1, $values),
            self::messageEvent($codec, 'message-2', 2, ['two']),
        ];
        $replayer = new Replayer($codec);

        $firstWorker = $replayer->replay($workflow, $history, [], 'php-workers');
        $replacementWorker = $replayer->replay($workflow, $history, [], 'php-workers');
        $firstResult = $codec->decodeEnvelope($firstWorker->commands[0]['result']);
        $replacementResult = $codec->decodeEnvelope($replacementWorker->commands[0]['result']);

        self::assertEquals($firstResult, $replacementResult);
        self::assertEquals($values, $replacementResult[0]);
        self::assertIsInt($replacementResult[0][1]);
        self::assertIsFloat($replacementResult[0][2]);
    }

    public function testClientAndHandleAppendStableMessageIdentityWithAvroInput(): void
    {
        $transport = new FakeTransport([['accepted' => true, 'position' => 3]]);
        $codec = new AvroPayloadCodec();
        $client = new Client('http://server.test', namespace: 'default', transport: $transport, codec: $codec);

        $result = $client->workflowHandle('workflow-1')->appendMessage(
            'orders',
            'message-3',
            [['n' => 3]],
        );

        self::assertSame(3, $result['position']);
        self::assertSame(
            'http://server.test/api/workflows/workflow-1/message-streams/orders/messages',
            $transport->requests[0]['uri'],
        );
        self::assertSame('message-3', $transport->requests[0]['body']['message_id']);
        self::assertSame([['n' => 3]], $codec->decodeEnvelope($transport->requests[0]['body']['input']));
    }

    public function testWorkerCompletionCarriesCursorAndWaitMetadata(): void
    {
        $transport = new FakeTransport([['outcome' => 'completed']]);
        $client = new Client('http://server.test', namespace: 'default', transport: $transport);

        $client->completeWorkflowTask(
            'task-1',
            'worker-1',
            1,
            [['type' => 'complete_workflow']],
            [['stream_name' => 'orders', 'through_position' => 2]],
            [['stream_name' => 'approval', 'after_position' => 0]],
        );

        self::assertSame(
            [['stream_name' => 'orders', 'through_position' => 2]],
            $transport->requests[0]['body']['message_stream_cursors'],
        );
        self::assertSame(
            [['stream_name' => 'approval', 'after_position' => 0]],
            $transport->requests[0]['body']['message_stream_waits'],
        );
    }

    public function testWorkflowClientFakeRecordsIdempotentMessageAppends(): void
    {
        $client = new WorkflowClientFake();
        $handle = $client->workflowHandle('workflow-1');

        $first = $handle->appendMessage('orders', 'message-1', [['n' => 1]]);
        $duplicate = $handle->appendMessage('orders', 'message-1', [['n' => 1]]);

        self::assertSame(1, $first['position']);
        self::assertFalse($first['duplicate']);
        self::assertSame(1, $duplicate['position']);
        self::assertTrue($duplicate['duplicate']);
        $client->assertMessageAppended('workflow-1', 'orders', 'message-1', [['n' => 1]]);
    }

    public function testWorkflowClientFakeRejectsMessageIdentityConflict(): void
    {
        $client = new WorkflowClientFake();
        $handle = $client->workflowHandle('workflow-1');

        $handle->appendMessage('orders', 'message-1', [['n' => 1]]);
        $conflict = $handle->appendMessage('orders', 'message-1', [['n' => 2]]);

        self::assertSame([
            'accepted' => false,
            'duplicate' => false,
            'outcome' => 'rejected',
            'reason' => 'message_identity_conflict',
            'position' => 1,
        ], $conflict);
        $client->assertMessageAppended('workflow-1', 'orders', 'message-1', [['n' => 1]]);
    }

    public function testMessageStreamsRequireWorkerProtocolOneFifteen(): void
    {
        self::assertFalse(Version::supportsMessageStreams('1.14'));
        self::assertTrue(Version::supportsMessageStreams('1.15'));
        self::assertSame('1.15', Version::WORKER_PROTOCOL);
    }

    /**
     * @param list<mixed> $arguments
     * @return array<string, mixed>
     */
    private static function messageEvent(
        AvroPayloadCodec $codec,
        string $messageId,
        int $position,
        array $arguments,
    ): array {
        return [
            'event_type' => 'SignalReceived',
            'payload' => [
                'signal_name' => WorkflowContext::MESSAGE_STREAM_SIGNAL,
                'value' => $codec->envelope([[
                    'schema' => WorkflowContext::MESSAGE_STREAM_SCHEMA,
                    'stream_name' => 'orders',
                    'message_id' => $messageId,
                    'position' => $position,
                    'payload_envelope' => $codec->envelope($arguments),
                ]]),
                'payload_codec' => 'avro',
            ],
        ];
    }

    /** @return array<string, mixed> */
    private static function cursorEvent(AvroPayloadCodec $codec, int $throughPosition): array
    {
        return [
            'event_type' => 'SignalReceived',
            'payload' => [
                'signal_name' => WorkflowContext::MESSAGE_STREAM_SIGNAL,
                'value' => $codec->envelope([[
                    'schema' => WorkflowContext::MESSAGE_STREAM_CURSOR_SCHEMA,
                    'stream_name' => 'orders',
                    'through_position' => $throughPosition,
                ]]),
                'payload_codec' => 'avro',
            ],
        ];
    }
}
