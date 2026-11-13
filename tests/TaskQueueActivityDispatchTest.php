<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerCapabilityUnavailable;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Model\ClusterInfo;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Transport\Psr18Transport;
use GuzzleHttp\Psr7\Response;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

/** Per-task-queue activity dispatch pause (native Server 3.8.0, task-queue-activity-dispatch-pause contract). */
final class TaskQueueActivityDispatchTest extends TestCase
{
    private const PAUSED = [
        'state' => 'paused',
        'paused' => true,
        'reason' => 'payments vendor outage',
        'paused_at' => '2026-11-12T10:00:00.123456Z',
        'paused_by' => ['type' => 'auth:token', 'id' => 'role:operator', 'label' => 'Operator'],
        'resumed_at' => null,
        'resumed_by' => null,
    ];

    private const RESUMED = [
        'state' => 'active',
        'paused' => false,
        'reason' => 'payments vendor outage',
        'paused_at' => '2026-11-12T10:00:00.123456Z',
        'paused_by' => ['type' => 'auth:token', 'id' => 'role:operator', 'label' => 'Operator'],
        'resumed_at' => '2026-11-12T11:00:00.000000Z',
        'resumed_by' => ['type' => 'auth:token', 'id' => 'role:admin'],
    ];

    private const NEVER_PAUSED = [
        'state' => 'active',
        'paused' => false,
        'reason' => null,
        'paused_at' => null,
        'paused_by' => null,
        'resumed_at' => null,
        'resumed_by' => null,
    ];

    public function testPauseWithAReasonSendsItOnceAndReturnsTheServerAnswer(): void
    {
        $answer = $this->answer('payments', 'paused', self::PAUSED);
        $transport = new FakeTransport([$answer]);

        $result = $this->client($transport)->pauseTaskQueueActivityDispatch('payments', 'payments vendor outage');

        self::assertSame($answer, $result);
        self::assertCount(1, $transport->requests);
        self::assertSame('POST', $transport->requests[0]['method']);
        self::assertSame('https://server.example/api/task-queues/payments/activity-dispatch/pause', $transport->requests[0]['uri']);
        self::assertSame(['reason' => 'payments vendor outage'], $transport->requests[0]['body']);
        $this->assertControlPlaneHeaders($transport);
    }

    public function testPauseWithoutAReasonSendsNoReasonField(): void
    {
        $transport = new FakeTransport([$this->answer('payments', 'paused', ['reason' => null] + self::PAUSED)]);

        $result = $this->client($transport)->pauseTaskQueueActivityDispatch('payments');

        self::assertSame('paused', $result['outcome']);
        self::assertNull($result['activity_dispatch']['reason']);
        self::assertSame([], $transport->requests[0]['body']);
    }

    public function testPauseOfAPausedQueueReportsAlreadyPausedWithTheOriginalPause(): void
    {
        $transport = new FakeTransport([$this->answer('payments', 'already_paused', self::PAUSED)]);

        $result = $this->client($transport)->pauseTaskQueueActivityDispatch('payments', 'another reason');

        self::assertSame('already_paused', $result['outcome']);
        self::assertSame('payments vendor outage', $result['activity_dispatch']['reason']);
        self::assertSame(['reason' => 'another reason'], $transport->requests[0]['body']);
    }

    public function testResumeSendsNoFieldsAndReturnsTheServerAnswer(): void
    {
        $answer = $this->answer('payments', 'resumed', self::RESUMED);
        $transport = new FakeTransport([$answer]);

        $result = $this->client($transport)->resumeTaskQueueActivityDispatch('payments');

        self::assertSame($answer, $result);
        self::assertSame('active', $result['activity_dispatch']['state']);
        self::assertSame('role:admin', $result['activity_dispatch']['resumed_by']['id']);
        self::assertSame('POST', $transport->requests[0]['method']);
        self::assertSame('https://server.example/api/task-queues/payments/activity-dispatch/resume', $transport->requests[0]['uri']);
        self::assertNull($transport->requests[0]['body']);
        $this->assertControlPlaneHeaders($transport);
    }

    public function testResumeOfAnActiveQueueReportsNotPaused(): void
    {
        $transport = new FakeTransport([$this->answer('payments', 'not_paused', self::NEVER_PAUSED)]);

        $result = $this->client($transport)->resumeTaskQueueActivityDispatch('payments');

        self::assertSame('not_paused', $result['outcome']);
        self::assertSame('active', $result['activity_dispatch']['state']);
        self::assertCount(1, $transport->requests);
    }

    public function testDescribeReadsTheQueueStateWithoutABody(): void
    {
        $answer = ['namespace' => 'orders', 'task_queue' => 'payments', 'activity_dispatch' => self::PAUSED];
        $transport = new FakeTransport([$answer]);

        $result = $this->client($transport)->describeTaskQueueActivityDispatch('payments');

        self::assertSame($answer, $result);
        self::assertSame('GET', $transport->requests[0]['method']);
        self::assertSame('https://server.example/api/task-queues/payments/activity-dispatch', $transport->requests[0]['uri']);
        self::assertNull($transport->requests[0]['body']);
        $this->assertControlPlaneHeaders($transport);
    }

    public function testQueueNamesAreAddressedExactlyAsOneSegment(): void
    {
        $transport = new FakeTransport(array_fill(0, 3, ['ok' => true]));
        $client = $this->client($transport);

        $client->describeTaskQueueActivityDispatch('billing/EU west');
        $client->pauseTaskQueueActivityDispatch('billing/EU west');
        $client->resumeTaskQueueActivityDispatch('Orders');

        self::assertSame([
            'https://server.example/api/task-queues/billing%2FEU%20west/activity-dispatch',
            'https://server.example/api/task-queues/billing%2FEU%20west/activity-dispatch/pause',
            'https://server.example/api/task-queues/Orders/activity-dispatch/resume',
        ], array_column($transport->requests, 'uri'));
    }

    public function testTheSelectedNamespaceIsSent(): void
    {
        $transport = new FakeTransport([['ok' => true]]);

        $this->client($transport)->withNamespace('billing')->pauseTaskQueueActivityDispatch('payments');

        self::assertSame('billing', $transport->requests[0]['headers']['X-Namespace']);
    }

    public function testAnEmptyQueueNameIsRefusedBeforeSending(): void
    {
        $transport = new FakeTransport();
        $this->expectException(InvalidArgumentException::class);

        try {
            $this->client($transport)->pauseTaskQueueActivityDispatch('');
        } finally {
            self::assertSame([], $transport->requests);
        }
    }

    #[DataProvider('calls')]
    public function testAWorkerCredentialRefusalKeepsTheServerAnswer(string $call): void
    {
        $refusal = ['message' => 'This action is unauthorized.', 'reason' => 'forbidden'];
        $transport = new FakeTransport([new TransportException('This action is unauthorized.', 403, $refusal)]);

        try {
            $this->call($this->client($transport), $call);
            self::fail('A 403 must be raised.');
        } catch (ServerException $exception) {
            self::assertNotInstanceOf(ServerCapabilityUnavailable::class, $exception);
            self::assertSame(403, $exception->status);
            self::assertSame('forbidden', $exception->reason);
            self::assertSame($refusal, $exception->details);
            self::assertSame('This action is unauthorized.', $exception->getMessage());
        }
        self::assertCount(1, $transport->requests);
    }

    public function testAnInvalidReasonKeepsTheServerValidationErrors(): void
    {
        $errors = ['reason' => ['The reason may not be greater than 1000 characters.']];
        $transport = new FakeTransport([new TransportException(
            'The given data was invalid.',
            422,
            ['message' => 'The given data was invalid.', 'errors' => $errors],
        )]);

        try {
            $this->client($transport)->pauseTaskQueueActivityDispatch('payments', str_repeat('x', 1001));
            self::fail('A 422 must be raised.');
        } catch (ServerException $exception) {
            self::assertSame(422, $exception->status);
            self::assertNull($exception->reason);
            self::assertSame($errors, $exception->details['errors'] ?? null);
        }
        self::assertSame(str_repeat('x', 1001), $transport->requests[0]['body']['reason'] ?? null);
    }

    /** @return array<string, array{string}> */
    public static function calls(): array
    {
        return ['describe' => ['describe'], 'pause' => ['pause'], 'resume' => ['resume']];
    }

    /** @return array<string, array{string, array<string, mixed>|null}> */
    public static function missingRoutes(): array
    {
        $rows = [];
        foreach (self::calls() as $name => [$call]) {
            // Published PHP Server 2.x answers Laravel's route 404 without a reason.
            $rows["published PHP {$name}"] = [$call, ['message' => 'The route could not be found.']];
            // Older native Server answers its route fallback with reason not_found.
            $rows["older native {$name}"] = [$call, ['message' => 'Not Found', 'reason' => 'not_found']];
        }
        $rows['empty body'] = ['pause', null];

        return $rows;
    }

    /** @param array<string, mixed>|null $response */
    #[DataProvider('missingRoutes')]
    public function testAnOlderServerWithoutTheRoutesRaisesATypedCapabilityError(string $call, ?array $response): void
    {
        $transport = new FakeTransport([new TransportException('Not Found', 404, $response)]);

        try {
            $this->call($this->client($transport), $call);
            self::fail('A missing route must be raised as a capability error.');
        } catch (ServerCapabilityUnavailable $exception) {
            self::assertSame('task_queue_activity_dispatch_pause', $exception->capability);
            self::assertSame(501, $exception->status);
            self::assertSame('backend_capability_unavailable', $exception->reason);
            self::assertSame('task_queue_activity_dispatch_pause', $exception->details['capability'] ?? null);
            self::assertSame(404, $exception->details['server_status'] ?? null);
            self::assertArrayHasKey('server_response', $exception->details);
            self::assertSame($response, $exception->details['server_response']);
            self::assertStringContainsString('task_queue_activity_dispatch_pause', $exception->getMessage());
            self::assertInstanceOf(ServerException::class, $exception->getPrevious());
            self::assertSame(404, $exception->getPrevious()->status);
        }
        self::assertCount(1, $transport->requests);
    }

    public function testAnUnknownNamespaceKeepsTheServerAnswer(): void
    {
        $response = ['message' => 'Namespace not found.', 'reason' => 'namespace_not_found', 'namespace' => 'orders'];
        $transport = new FakeTransport([new TransportException('Namespace not found.', 404, $response)]);

        try {
            $this->client($transport)->pauseTaskQueueActivityDispatch('payments');
            self::fail('A 404 namespace_not_found must be raised.');
        } catch (ServerException $exception) {
            self::assertNotInstanceOf(ServerCapabilityUnavailable::class, $exception);
            self::assertSame(404, $exception->status);
            self::assertSame('namespace_not_found', $exception->reason);
        }
    }

    #[DataProvider('mutations')]
    public function testALostAnswerIsNeverResent(string $call): void
    {
        $transport = new FakeTransport([
            new TransportException('Connection reset by peer.', transientConnectionFailure: true),
            $this->answer('payments', 'paused', self::PAUSED),
        ]);

        try {
            $this->call($this->client($transport), $call);
            self::fail('The transport failure must be raised.');
        } catch (ServerException $exception) {
            self::assertSame(0, $exception->status);
            self::assertTrue($exception->isTransientConnectionFailure());
        }
        self::assertCount(1, $transport->requests);
    }

    #[DataProvider('mutations')]
    public function testAnUpstreamFailureIsNeverResent(string $call): void
    {
        $transport = new FakeTransport([
            new TransportException('Server returned HTTP 503.', 503),
            $this->answer('payments', 'paused', self::PAUSED),
        ]);

        try {
            $this->call($this->client($transport), $call);
            self::fail('The upstream failure must be raised.');
        } catch (ServerException $exception) {
            self::assertSame(503, $exception->status);
        }
        self::assertCount(1, $transport->requests);
    }

    /** @return array<string, array{string}> */
    public static function mutations(): array
    {
        return ['pause' => ['pause'], 'resume' => ['resume']];
    }

    public function testTheDefaultTransportRaisesTheServerAnswerAndSendsOnce(): void
    {
        $http = new class implements ClientInterface {
            /** @var list<RequestInterface> */
            public array $requests = [];

            public function sendRequest(RequestInterface $request): ResponseInterface
            {
                $this->requests[] = $request;

                return new Response(403, ['Content-Type' => 'application/json'], json_encode([
                    'message' => 'This action is unauthorized.',
                    'reason' => 'forbidden',
                ], JSON_THROW_ON_ERROR));
            }
        };
        $client = new Client('https://server.example', transport: new Psr18Transport($http), token: 'worker-token', namespace: 'orders');

        try {
            $client->pauseTaskQueueActivityDispatch('payments', 'outage');
            self::fail('A 403 must be raised.');
        } catch (ServerException $exception) {
            self::assertSame(403, $exception->status);
            self::assertSame('forbidden', $exception->reason);
        }
        self::assertCount(1, $http->requests);
        self::assertSame('{"reason":"outage"}', (string) $http->requests[0]->getBody());
        self::assertSame('orders', $http->requests[0]->getHeaderLine('X-Namespace'));
        self::assertSame('2', $http->requests[0]->getHeaderLine('X-Durable-Workflow-Control-Plane-Version'));
    }

    public function testClusterInfoReportsTheCapability(): void
    {
        self::assertTrue(ClusterInfo::fromArray(['capabilities' => ['task_queue_activity_dispatch_pause' => true]])
            ->supportsTaskQueueActivityDispatchPause());
        self::assertFalse(ClusterInfo::fromArray(['capabilities' => ['task_queue_activity_dispatch_pause' => 'yes']])
            ->supportsTaskQueueActivityDispatchPause());
        self::assertFalse(ClusterInfo::fromArray(['capabilities' => []])->supportsTaskQueueActivityDispatchPause());
        self::assertFalse(ClusterInfo::fromArray([])->supportsTaskQueueActivityDispatchPause());
    }

    private function client(FakeTransport $transport): Client
    {
        return new Client('https://server.example', transport: $transport, token: 'operator-token', namespace: 'orders');
    }

    /** @return array<string, mixed> */
    private function call(Client $client, string $call): array
    {
        return match ($call) {
            'describe' => $client->describeTaskQueueActivityDispatch('payments'),
            'pause' => $client->pauseTaskQueueActivityDispatch('payments', 'outage'),
            'resume' => $client->resumeTaskQueueActivityDispatch('payments'),
        };
    }

    /**
     * @param array<string, mixed> $state
     * @return array<string, mixed>
     */
    private function answer(string $taskQueue, string $outcome, array $state): array
    {
        return ['namespace' => 'orders', 'task_queue' => $taskQueue, 'outcome' => $outcome, 'activity_dispatch' => $state];
    }

    private function assertControlPlaneHeaders(FakeTransport $transport): void
    {
        foreach ($transport->requests as $request) {
            self::assertSame('orders', $request['headers']['X-Namespace']);
            self::assertSame('2', $request['headers']['X-Durable-Workflow-Control-Plane-Version']);
            self::assertSame('Bearer operator-token', $request['headers']['Authorization'] ?? null);
            self::assertArrayNotHasKey('X-Durable-Workflow-Protocol-Version', $request['headers']);
        }
    }
}
