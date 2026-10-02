<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Version;
use DurableWorkflow\Worker\CancellationRequest;
use InvalidArgumentException;
use LogicException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class CooperativeCancellationClientTest extends TestCase
{
    public function testSelectedRunRequestUsesControlCredentialsAndPreservesOriginalObservation(): void
    {
        $ack = self::ack();
        $transport = new FakeTransport([self::discovery(), $ack]);
        $client = new Client('https://server.example', transport: $transport,
            namespace: 'tenant-a', controlToken: 'control', workerToken: 'worker',
        );
        self::assertSame($ack, $client->workflowHandle('order/1', 'run/1')->requestSelectedRunCancellation('cleanup', 60));
        self::assertSame('https://server.example/api/workflows/order%2F1/runs/run%2F1/request-cancellation', $transport->requests[1]['uri']);
        self::assertSame(['reason' => 'cleanup', 'cleanup_timeout_seconds' => 60], $transport->requests[1]['body']);
        foreach ($transport->requests as $request) {
            self::assertSame('Bearer control', $request['headers']['Authorization']);
            self::assertSame('tenant-a', $request['headers']['X-Namespace']);
            self::assertSame('2', $request['headers']['X-Durable-Workflow-Control-Plane-Version']);
            self::assertArrayNotHasKey('X-Durable-Workflow-Protocol-Version', $request['headers']);
        }
        $observation = CancellationRequest::fromObservation($ack['cancellation_request']);
        self::assertSame('request-1', $observation->requestId);
        self::assertSame('2026-10-01T00:01:00.123456Z', $observation->cleanupDeadlineAt);
        self::assertSame('opaque-first-page', $observation->historyRefreshPageToken);
    }

    public function testDuplicateRequestDoesNotReplaceTheServerIdentityOrDeadline(): void
    {
        $first = self::ack();
        $duplicate = [...$first, 'duplicate' => true];
        $transport = new FakeTransport([self::discovery(), $first, self::discovery(), $duplicate]);
        $client = new Client('https://server.example', transport: $transport);
        $firstResult = $client->requestWorkflowCancellation('order/1', cleanupTimeoutSeconds: 1);
        $secondResult = $client->workflowHandle('order/1')->requestCancellation(cleanupTimeoutSeconds: 3600);
        self::assertSame($firstResult['cancellation_request'], $secondResult['cancellation_request']);
        self::assertSame('https://server.example/api/workflows/order%2F1/request-cancellation', $transport->requests[3]['uri']);
        self::assertSame(['cleanup_timeout_seconds' => 3600], $transport->requests[3]['body']);
        self::assertSame('1.19', Version::WORKER_PROTOCOL);
        self::assertFalse(Version::supportsCooperativeCancellation());
    }

    #[DataProvider('incapableRuntimeProvider')]
    public function testRuntimeMustExplicitlyDiscoverTheCompatibleCapability(mixed $version, mixed $capability): void
    {
        $transport = new FakeTransport([self::discovery($version, $capability)]);
        $client = new Client('https://server.example', transport: $transport);
        try {
            $client->requestWorkflowCancellation('order/1');
            self::fail('Expected capability refusal.');
        } catch (LogicException $error) {
            self::assertStringContainsString('explicitly discover', $error->getMessage());
        }
        self::assertCount(1, $transport->requests);
        self::assertSame('GET', $transport->requests[0]['method']);
    }

    public static function incapableRuntimeProvider(): array
    {
        return [
            ['1.19', true], ['1.20', false], ['1.20', 'true'], ['1.20', 1],
            ['1.20', null], ['2.0', true], ['malformed', true], [null, true],
        ];
    }

    #[DataProvider('invalidAcknowledgmentProvider')]
    public function testMalformedAcknowledgmentCannotBeAccepted(array $ack): void
    {
        $transport = new FakeTransport([self::discovery(), $ack]);
        $client = new Client('https://server.example', transport: $transport);
        try {
            $client->requestWorkflowCancellation('order/1', runId: 'run/1');
            self::fail('Expected acknowledgment refusal.');
        } catch (ServerException $error) {
            self::assertSame('invalid_cooperative_cancellation_response', $error->reason);
        }
        self::assertCount(2, $transport->requests);
    }

    public static function invalidAcknowledgmentProvider(): array
    {
        $cases = [];
        foreach (['accepted' => 'true', 'duplicate' => 1, 'workflow_id' => 'other', 'run_id' => 'other', 'cancellation_request' => []] as $key => $value) {
            $cases[$key] = [[...self::ack(), $key => $value]];
        }
        foreach ([
            'request_id' => '', 'requested_at' => '2026-10-01T00:00:00',
            'cleanup_deadline_at' => '2026-10-01T00:00:00.123456Z', 'history_refresh_page_token' => '',
        ] as $key => $value) {
            $ack = self::ack();
            $ack['cancellation_request'][$key] = $value;
            $cases[$key] = [$ack];
        }
        $invalidDate = self::ack();
        $invalidDate['cancellation_request']['requested_at'] = '2026-02-30T00:00:00Z';
        $cases['calendar'] = [$invalidDate];
        $wrongType = self::ack();
        $wrongType['cancellation_request']['history_refresh_page_token'] = [];
        $cases['opaque type'] = [$wrongType];

        return $cases;
    }

    #[DataProvider('invalidRequestProvider')]
    public function testInvalidRequestIsRejectedBeforeDiscovery(string $workflowId, ?int $timeout, ?string $runId): void
    {
        $transport = new FakeTransport();
        $client = new Client('https://server.example', transport: $transport);
        try {
            $client->requestWorkflowCancellation($workflowId, cleanupTimeoutSeconds: $timeout, runId: $runId);
            self::fail('Expected invalid request refusal.');
        } catch (InvalidArgumentException) {
            self::assertSame([], $transport->requests);
        }
    }

    public static function invalidRequestProvider(): array
    {
        return [['', null, null], ['order/1', 0, null], ['order/1', 3601, null], ['order/1', null, ' ']];
    }

    public function testSelectedRunMustBePresentAndTerminalOperationsKeepTheirRoutes(): void
    {
        $transport = new FakeTransport([[], []]);
        $client = new Client('https://server.example', transport: $transport);
        try {
            $client->workflowHandle('order/1')->requestSelectedRunCancellation();
            self::fail('Expected missing selected run refusal.');
        } catch (LogicException) {
            self::assertSame([], $transport->requests);
        }
        $client->cancelWorkflow('order/1', 'cancel');
        $client->terminateWorkflow('order/1', 'terminate', 'run/1');
        self::assertSame('https://server.example/api/workflows/order%2F1/cancel', $transport->requests[0]['uri']);
        self::assertSame('https://server.example/api/workflows/order%2F1/runs/run%2F1/terminate', $transport->requests[1]['uri']);
    }

    public function testStoppedActivityReceiptUsesWorkerCredentialsAndOriginalIdentity(): void
    {
        $ack = ['acknowledged' => true, 'history_event_id' => 'original-receipt'];
        $transport = new FakeTransport([$ack, [...$ack, 'duplicate' => true]]);
        $client = new Client('https://server.example', transport: $transport, namespace: 'tenant-a',
            controlToken: 'control', workerToken: 'worker', workerProtocolVersion: '1.20');
        self::assertSame($ack, $client->acknowledgeActivityCancellation('task/1', 'attempt/1', 'owner', 'original-request'));
        $duplicate = $client->acknowledgeActivityCancellation('task/1', 'attempt/1', 'owner', 'original-request');
        self::assertSame($ack['history_event_id'], $duplicate['history_event_id']);
        foreach ($transport->requests as $request) {
            self::assertSame('https://server.example/api/worker/activity-tasks/task%2F1/acknowledge-cancellation', $request['uri']);
            self::assertSame('Bearer worker', $request['headers']['Authorization']);
            self::assertSame('tenant-a', $request['headers']['X-Namespace']);
            self::assertSame('1.20', $request['headers']['X-Durable-Workflow-Protocol-Version']);
            self::assertSame(['activity_attempt_id' => 'attempt/1', 'lease_owner' => 'owner', 'request_id' => 'original-request'], $request['body']);
        }
    }

    public static function invalidActivityReceiptRequestProvider(): array
    {
        return [['1.19', 'task', 'attempt', 'owner', 'request'], ['1.20', '', 'attempt', 'owner', 'request'],
            ['1.20', 'task', '', 'owner', 'request'], ['1.20', 'task', 'attempt', ' ', 'request'],
            ['1.20', 'task', 'attempt', 'owner', ''], ['1.20', 'task', 'attempt', 'owner', str_repeat('x', 256)]];
    }

    #[DataProvider('invalidActivityReceiptRequestProvider')]
    public function testActivityReceiptRejectsLegacyProtocolOrMissingIdentityBeforeSending(string $protocol, string $task, string $attempt, string $owner, string $request): void
    {
        $transport = new FakeTransport();
        $client = new Client('https://server.example', transport: $transport, workerProtocolVersion: $protocol);
        try {
            $client->acknowledgeActivityCancellation($task, $attempt, $owner, $request);
            self::fail('Expected acknowledgment refusal.');
        } catch (InvalidArgumentException) { self::assertSame([], $transport->requests); }
    }

    private static function discovery(mixed $version = '1.20', mixed $capability = true): array
    {
        return ['worker_protocol' => ['version' => $version, 'server_capabilities' => ['cooperative_cancellation' => $capability]]];
    }

    private static function ack(): array
    {
        return [
            'accepted' => true, 'duplicate' => false, 'workflow_id' => 'order/1', 'run_id' => 'run/1',
            'cancellation_request' => [
                'request_id' => 'request-1', 'requested_at' => '2026-10-01T00:00:00.123456Z',
                'cleanup_deadline_at' => '2026-10-01T00:01:00.123456Z',
                'history_refresh_page_token' => 'opaque-first-page',
            ],
        ];
    }
}
