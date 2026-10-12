<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\WorkflowFailed;
use DurableWorkflow\Exception\ExternalPayloadException;
use DurableWorkflow\Exception\WorkflowTimedOut;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Transport\Psr18Transport;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Promise\Create;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;

final class ClientWorkflowResultTest extends TestCase
{
    public function testWaitingForStatusDoesNotDownloadUnusedInputOrOutput(): void
    {
        [$client, $requests] = $this->payloadClient('waiting');
        try {
            $client->workflowHandle('order', 'run')->resultOfSelectedRun(timeoutSeconds: 0);
            self::fail('Waiting must retain the caller timeout.');
        } catch (WorkflowTimedOut $exception) {
            self::assertSame('Workflow order was not terminal after 0 seconds.', $exception->getMessage());
        }
        self::assertCount(1, $requests->value);
    }

    #[DataProvider('historyFields')]
    public function testOnlyTheTerminalHistoryPayloadIsDownloaded(string $historyField): void
    {
        [$client, $requests] = $this->payloadClient('completed', $historyField);
        self::assertSame(['finished' => true], $client->workflowHandle('order', 'run')->resultOfSelectedRun(timeoutSeconds: 0));
        self::assertSame(['/api/workflows/order/runs/run', '/api/workflows/order/runs/run/history',
            '/api/external-payloads/v1/ep_01ARZ3NDEKTSV4RRFFQ69G5FAV'], array_map(static fn (RequestInterface $request): string => $request->getUri()->getPath(), $requests->value));
        self::assertSame('Bearer fixture-client', $requests->value[2]->getHeaderLine('Authorization'));
    }

    public static function historyFields(): iterable
    {
        yield 'events' => ['events'];
        yield 'history_events' => ['history_events'];
    }

    public function testMissingTerminalHistoryStillUsesTheRunOutput(): void
    {
        [$client, $requests] = $this->payloadClient('completed', emptyHistory: true);
        self::assertSame(['finished' => true], $client->workflowHandle('order', 'run')->resultOfSelectedRun(timeoutSeconds: 0));
        self::assertCount(3, $requests->value);
        self::assertStringEndsWith('/ep_01ARZ3NDEKTSV4RRFFQ69G5FAV', (string) $requests->value[2]->getUri());
    }

    public function testAnUnreadableInputDoesNotHideTheTerminalFailure(): void
    {
        [$client, $requests] = $this->payloadClient('failed');
        try {
            $client->workflowHandle('order', 'run')->resultOfSelectedRun(timeoutSeconds: 0);
            self::fail('The canonical terminal failure must be preserved.');
        } catch (WorkflowFailed $exception) {
            self::assertSame('Order rejected.', $exception->getMessage());
        }
        self::assertCount(2, $requests->value);
    }

    public function testTheSelectedTerminalOutputStillRequiresValidIntegrityMetadata(): void
    {
        [$client] = $this->payloadClient('completed', corruptOutput: true);
        try {
            $client->workflowHandle('order', 'run')->resultOfSelectedRun(timeoutSeconds: 0);
            self::fail('The selected output must pass its integrity check.');
        } catch (ExternalPayloadException $exception) {
            self::assertSame(422, $exception->status);
            self::assertSame('external_payload_integrity_mismatch', $exception->reason);
        }
    }

    /** @return array{Client, object} */
    private function payloadClient(string $status, string $historyField = 'events', bool $emptyHistory = false, bool $corruptOutput = false): array
    {
        $codec = new AvroPayloadCodec();
        $blob = $codec->encode(['finished' => true]);
        $reference = RuntimeExternalPayloadTest::reference($blob);
        $output = ['codec' => 'avro', 'external_payload' => $reference];
        $unused = ['codec' => 'avro', 'external_payload' => array_replace($reference, ['reference_id' => 'ep_01ARZ3NDEKTSV4RRFFQ69G5FAW'])];
        $requests = new class { public array $value = []; };
        $http = new GuzzleClient(['handler' => static function (RequestInterface $request) use ($requests, $status, $historyField, $emptyHistory, $blob, $output, $unused, $corruptOutput) {
            $requests->value[] = $request;
            $path = $request->getUri()->getPath();
            if (str_ends_with($path, '/ep_01ARZ3NDEKTSV4RRFFQ69G5FAW')) {
                throw new \LogicException('Result observation must not download workflow/activity input or unrelated output.');
            }
            if (str_ends_with($path, '/ep_01ARZ3NDEKTSV4RRFFQ69G5FAV')) {
                return Create::promiseFor(new Response(200, ['Content-Type' => 'application/octet-stream',
                    'X-Durable-Workflow-Payload-Codec' => 'avro', 'X-Durable-Workflow-Payload-Size' => (string) strlen($blob),
                    'X-Durable-Workflow-Payload-SHA256' => $corruptOutput ? str_repeat('0', 64) : hash('sha256', $blob)], $blob));
            }
            if (str_ends_with($path, '/history')) {
                $body = [$historyField => $emptyHistory ? [] : [
                    ['event_type' => 'WorkflowStarted', 'payload' => ['arguments' => $unused]],
                    ['event_type' => 'ActivityCompleted', 'payload' => ['result' => $unused]],
                    $status === 'failed'
                        ? ['event_type' => 'WorkflowFailed', 'payload' => ['message' => 'Order rejected.', 'exception_type' => 'OrderRejected']]
                        : ['event_type' => 'WorkflowCompleted', 'payload' => ['output' => $output, 'result' => $unused]],
                ]];
            } else {
                $body = ['workflow_id' => 'order', 'run_id' => 'run', 'status' => $status,
                    'input_envelope' => $unused, 'output_envelope' => $output];
            }

            return Create::promiseFor(new Response(200, [], json_encode($body, JSON_THROW_ON_ERROR)));
        }]);

        return [new Client('https://runtime.test', transport: new Psr18Transport($http), controlToken: 'fixture-client'), $requests];
    }

    #[DataProvider('timeoutProvider')]
    public function testTerminalDeadlineUsesTheSelectedRun(string $timeoutKind, string $historyField): void
    {
        $transport = new FakeTransport([
            [
                'workflow_id' => 'order/1',
                'run_id' => 'run/2',
                'status' => 'failed',
                'is_terminal' => true,
                'failure' => ['failure_category' => 'timeout', 'reason' => $timeoutKind],
            ],
            [$historyField => [[
                'event_type' => 'WorkflowTimedOut',
                'payload' => ['timeout_kind' => $timeoutKind, 'deadline_at' => '2026-01-01T00:00:00Z'],
            ]]],
        ]);
        $client = new Client('https://server.example', transport: $transport);

        try {
            $client->workflowHandle('order/1', 'run/2')->resultOfSelectedRun(timeoutSeconds: 0);
            self::fail('A persisted workflow deadline must raise the typed timeout.');
        } catch (WorkflowTimedOut $exception) {
            self::assertSame('Workflow execution timed out.', $exception->getMessage());
        }

        self::assertSame([
            'https://server.example/api/workflows/order%2F1/runs/run%2F2',
            'https://server.example/api/workflows/order%2F1/runs/run%2F2/history',
        ], array_column($transport->requests, 'uri'));
    }

    /** @return iterable<string, array{string, string}> */
    public static function timeoutProvider(): iterable
    {
        foreach (['execution_timeout', 'run_timeout'] as $kind) {
            foreach (['events', 'history_events'] as $field) {
                yield $kind.' '.$field => [$kind, $field];
            }
        }
    }

    public function testCallerWaitTimeoutDoesNotFetchTerminalHistory(): void
    {
        $transport = new FakeTransport([[
            'workflow_id' => 'order', 'run_id' => 'run', 'status' => 'waiting', 'is_terminal' => false,
        ]]);
        $client = new Client('https://server.example', transport: $transport);

        try {
            $client->workflowHandle('order', 'run')->resultOfSelectedRun(timeoutSeconds: 0);
            self::fail('The existing caller wait timeout must still be enforced.');
        } catch (WorkflowTimedOut $exception) {
            self::assertSame('Workflow order was not terminal after 0 seconds.', $exception->getMessage());
        }
        self::assertCount(1, $transport->requests);
    }

    public function testAnOrdinaryWorkflowFailureRemainsAFailure(): void
    {
        $transport = new FakeTransport([
            ['workflow_id' => 'order', 'run_id' => 'run', 'status' => 'failed'],
            ['events' => [[
                'event_type' => 'WorkflowFailed',
                'payload' => ['message' => 'Order rejected.', 'exception_type' => 'OrderRejected'],
            ]]],
        ]);
        $client = new Client('https://server.example', transport: $transport);

        $this->expectException(WorkflowFailed::class);
        $this->expectExceptionMessage('Order rejected.');
        $client->workflowHandle('order', 'run')->resultOfSelectedRun(timeoutSeconds: 0);
    }
}
