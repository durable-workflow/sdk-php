<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\WorkflowFailed;
use DurableWorkflow\Exception\WorkflowTimedOut;
use DurableWorkflow\Tests\Support\FakeTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ClientWorkflowResultTest extends TestCase
{
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
