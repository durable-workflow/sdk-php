<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Model\WorkflowStreamAppendItem;
use DurableWorkflow\Model\WorkflowStreamAppendResult;
use DurableWorkflow\Model\WorkflowStreamDescription;
use DurableWorkflow\Model\WorkflowStreamItem;
use DurableWorkflow\Model\WorkflowStreamPage;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Testing\WorkflowClientFake;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use DurableWorkflow\WorkflowClientInterface;
use PHPUnit\Framework\TestCase;

final class WorkflowStreamsTest extends TestCase
{
    public function testClientExposesTypedLifecycleAndResumeOperations(): void
    {
        $codec = new AvroPayloadCodec();
        $transport = new FakeTransport([
            ['count' => 1, 'streams' => [$this->stream('open')]],
            ['stream' => $this->stream('open')],
            [
                'stream' => $this->stream('open'),
                'items' => [[
                    'offset' => 4,
                    'payload' => $codec->envelope(['token' => 'hello']),
                    'payload_codec' => 'avro',
                    'payload_reference' => null,
                    'origin' => 'workflow_command',
                ]],
                'next_offset' => 5,
                'terminal' => false,
            ],
            [
                'stream' => $this->stream('open'),
                'accepted_offsets' => [5],
                'accepted' => 1,
                'deduped' => 0,
            ],
            ['stream' => $this->stream('errored')],
        ]);
        $client = new Client('https://server.example', transport: $transport, codec: $codec);

        self::assertSame('tokens', $client->listWorkflowStreams('wf/1', 'run/1')[0]->streamName);
        self::assertSame(4, $client->describeWorkflowStream('wf/1', 'run/1', 'tokens')->lastOffset);

        $page = $client->subscribeWorkflowStream('wf/1', 'run/1', 'tokens', 4, 25, 10);
        self::assertSame(['token' => 'hello'], $page->items[0]->payload);
        self::assertSame(5, $page->nextOffset);
        self::assertFalse($page->terminal);

        $append = $client->appendWorkflowStream('wf/1', 'run/1', 'tokens', [
            new WorkflowStreamAppendItem(['token' => 'world'], itemType: 'token'),
        ], 100);
        self::assertSame([5], $append->acceptedOffsets);
        self::assertSame('avro', $transport->requests[3]['body']['items'][0]['payload_codec']);
        self::assertSame(['token' => 'world'], $codec->decodeEnvelope(
            $transport->requests[3]['body']['items'][0]['payload'],
        ));

        $closed = $client->closeWorkflowStream('wf/1', 'run/1', 'tokens', 'producer_failed', 600);
        self::assertSame('errored', $closed->status);
        self::assertSame(
            'https://server.example/api/workflows/wf%2F1/runs/run%2F1/streams/tokens/items?from=4&max_items=25&wait_seconds=10',
            $transport->requests[2]['uri'],
        );
    }

    public function testWorkflowAuthoringDerivesReplayStableItemIdentity(): void
    {
        $codec = new AvroPayloadCodec();
        $replayer = new Replayer($codec);
        $workflow = static function (WorkflowContext $context): string {
            $context->appendWorkflowStream('tokens', [
                new WorkflowStreamAppendItem(['token' => 'hello']),
                new WorkflowStreamAppendItem(payloadReference: 's3://payloads/token-2'),
            ]);
            $context->closeWorkflowStream('tokens');

            return 'done';
        };
        $task = [
            'workflow_id' => 'wf-1',
            'run_id' => 'run-1',
            'workflow_command_id' => '01JCOMMAND0000000000000000',
        ];

        $initial = $replayer->replay($workflow, [], [], 'workers', $task);

        self::assertSame('record_side_effect', $initial->commands[0]['type']);
        self::assertSame('append', $initial->commands[0]['workflow_stream']['operation']);
        self::assertSame(
            'dw-stream:01JCOMMAND0000000000000000:0:0',
            $initial->commands[0]['workflow_stream']['items'][0]['idempotency_key'],
        );
        self::assertSame('s3://payloads/token-2', $initial->commands[0]['workflow_stream']['items'][1]['payload_reference']);
        self::assertSame('close', $initial->commands[1]['workflow_stream']['operation']);

        $streamCommands = array_slice($initial->commands, 0, 2);
        $history = array_map(static fn (array $command, int $index): array => [
            'event_type' => 'SideEffectRecorded',
            'payload' => ['sequence' => $index + 1, 'result' => $command['result']],
        ], $streamCommands, array_keys($streamCommands));
        $replayed = $replayer->replay($workflow, $history, [], 'workers', $task);

        self::assertCount(1, $replayed->commands);
        self::assertSame('complete_workflow', $replayed->commands[0]['type']);
    }

    public function testApplicationClientContractAndFakeExposeWorkflowStreams(): void
    {
        $open = WorkflowStreamDescription::fromArray($this->stream('open'));
        $closed = WorkflowStreamDescription::fromArray($this->stream('closed'));
        $item = new WorkflowStreamItem(
            4,
            ['token' => 'hello'],
            null,
            null,
            'avro',
            'item-4',
            'workflow_command',
            'command-1',
            'token',
            null,
            null,
            [],
        );
        $page = new WorkflowStreamPage($closed, [$item], 5, true, []);
        $append = new WorkflowStreamAppendResult($open, [5], 1, 0, []);
        $fake = (new WorkflowClientFake())
            ->setWorkflowStreams('wf-1', 'run-1', [$open])
            ->setWorkflowStreamPage('wf-1', 'run-1', 'tokens', 4, $page)
            ->setWorkflowStreamAppendResult('wf-1', 'run-1', 'tokens', $append)
            ->setWorkflowStreamCloseResult('wf-1', 'run-1', 'tokens', $closed);
        $client = $fake;

        self::assertInstanceOf(WorkflowClientInterface::class, $client);
        self::assertSame([$open], $client->listWorkflowStreams('wf-1', 'run-1'));
        self::assertSame($open, $client->describeWorkflowStream('wf-1', 'run-1', 'tokens'));
        self::assertSame($page, $client->subscribeWorkflowStream('wf-1', 'run-1', 'tokens', 4));
        self::assertSame([$item], iterator_to_array(
            $client->iterateWorkflowStream('wf-1', 'run-1', 'tokens', 4),
            false,
        ));
        self::assertSame($append, $client->appendWorkflowStream('wf-1', 'run-1', 'tokens', [
            new WorkflowStreamAppendItem(['token' => 'world']),
        ]));
        self::assertSame($closed, $client->closeWorkflowStream('wf-1', 'run-1', 'tokens'));
    }

    /** @return array<string, mixed> */
    private function stream(string $status): array
    {
        return [
            'stream_name' => 'tokens',
            'status' => $status,
            'last_offset' => 4,
            'total_items' => 5,
            'pending_items' => 1,
            'error_reason' => $status === 'errored' ? 'producer_failed' : null,
        ];
    }
}
