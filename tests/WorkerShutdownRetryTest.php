<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Transport\BoundedTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\TestCase;

final class WorkerShutdownRetryTest extends TestCase
{
    private const TOKEN = '0123456789abcdef0123456789abcdef';

    public function testTemporaryContentionRetriesTheSameIncarnationWithoutPolling(): void
    {
        $failure = self::temporaryFailure();
        [$transport, $events] = $this->runShutdown([$failure, self::receipt()]);
        self::assertCount(3, $transport->requests);
        self::assertSame(['POST', 'POST', 'POST'], array_column($transport->requests, 'method'));
        foreach (array_slice($transport->requests, 1) as $request) {
            self::assertStringEndsWith('/worker/registrations/worker-a/deregister', $request['uri']);
            self::assertSame(['registration_token' => self::TOKEN], $request['body']);
            self::assertGreaterThanOrEqual(1, $request['timeout']);
            self::assertLessThanOrEqual(10, $request['timeout']);
        }
        self::assertContains('worker.retrying', $events);
        self::assertContains('worker.deregistered', $events);
        self::assertNotContains('worker.shutdown_failed', $events);
    }

    public function testLostReplyCanReplayTheOriginalReceipt(): void
    {
        [$transport] = $this->runShutdown([
            new TransportException('Reply lost after commit.', transientConnectionFailure: true),
            self::receipt(),
        ]);
        self::assertCount(3, $transport->requests);
        self::assertSame($transport->requests[1]['body'], $transport->requests[2]['body']);
    }

    public function testPersistentPressureHasOneBudgetAndStopsBeforeAnotherDelayWouldExceedIt(): void
    {
        $now = 1000.0;
        $transport = new ShutdownBoundedTransport([self::registration(), ...array_fill(0, 20, self::temporaryFailure())]);
        $worker = $this->worker($transport, $now);
        try {
            $worker->run(0);
            self::fail('Persistent pressure was acknowledged as successful shutdown.');
        } catch (ServerException $error) {
            self::assertSame('backend_lock_pressure', $error->reason);
        }
        self::assertLessThan(1010.0, $now);
        self::assertGreaterThan(1005.0, $now);
        self::assertLessThan(12, count($transport->requests));
        self::assertNotContains('DELETE', array_column($transport->requests, 'method'));
    }

    public function testWrongIdentityAndTerminalResponsesAreNeverRetried(): void
    {
        $wrong = self::temporaryFailure()->response;
        self::assertIsArray($wrong);
        $wrong['registration_token'] = str_repeat('f', 32);
        foreach ([new TransportException('Wrong identity.', 503, $wrong),
            new TransportException('Unknown token.', 404, ['reason' => 'worker_registration_token_not_found']),
            new TransportException('Forbidden.', 403, ['reason' => 'forbidden'])] as $failure) {
            $now = 1000.0;
            $transport = new ShutdownBoundedTransport([self::registration(), $failure, self::receipt()]);
            $worker = $this->worker($transport, $now);
            try {
                $worker->run(0);
                self::fail('A terminal response was acknowledged.');
            } catch (ServerException) {
                self::assertCount(2, $transport->requests);
                self::assertSame(1000.0, $now);
            }
        }
    }

    public function testInvalidSuccessReceiptFailsWithoutRetry(): void
    {
        $receipt = self::receipt();
        $receipt['registration_token'] = str_repeat('f', 32);
        $now = 1000.0;
        $transport = new ShutdownBoundedTransport([self::registration(), $receipt]);
        $worker = $this->worker($transport, $now);
        try {
            $worker->run(0);
            self::fail('The wrong receipt was accepted.');
        } catch (ServerException $error) {
            self::assertSame('invalid_worker_deregistration_receipt', $error->reason);
            self::assertCount(2, $transport->requests);
        }
    }

    public function testLegacyServerKeepsOneUnfencedAttempt(): void
    {
        $now = 1000.0;
        $transport = new ShutdownBoundedTransport([['registered' => true], self::temporaryFailure()]);
        $worker = $this->worker($transport, $now);
        try {
            $worker->run(0);
            self::fail('Legacy failure was acknowledged.');
        } catch (ServerException) {
            self::assertCount(2, $transport->requests);
            self::assertSame('DELETE', $transport->requests[1]['method']);
            self::assertNull($transport->requests[1]['timeout']);
        }
    }

    public function testUnboundedCustomAdapterMakesOneFencedAttempt(): void
    {
        $transport = new FakeTransport([self::registration(), self::temporaryFailure(), self::receipt()]);
        $events = [];
        $worker = null;
        $worker = new Worker(new Client('https://server.example', transport: $transport), 'queue', workerId: 'worker-a',
            diagnosticListener: static function (string $event) use (&$events, &$worker): void {
                $events[] = $event;
                if ($event === 'worker.registered') { $worker?->requestShutdown(); }
            });
        try {
            $worker->run(0);
            self::fail('Unbounded adapter retried a failed request.');
        } catch (ServerException) {
            self::assertCount(2, $transport->requests);
            self::assertSame('POST', $transport->requests[1]['method']);
            self::assertContains('worker.shutdown_retry_unavailable', $events);
        }
    }

    public function testMalformedCapabilityCannotFallBackToUnfencedDeletion(): void
    {
        foreach ([['registration_token' => self::TOKEN],
            ['server_capabilities' => 'malformed'],
            ['server_capabilities' => ['worker_deregistration_fencing' => ['supported' => true]]]] as $fields) {
            $now = 1000.0;
            $transport = new ShutdownBoundedTransport([['registered' => true, ...$fields]]);
            $worker = $this->worker($transport, $now);
            try {
                $worker->run(0);
                self::fail('Malformed capability fell back to legacy shutdown.');
            } catch (ServerException $error) {
                self::assertSame('invalid_worker_registration_fence', $error->reason);
                self::assertCount(1, $transport->requests);
            }
        }
    }

    public function testRequestTimeAndBackoffConsumeTheOriginalIoBudget(): void
    {
        $transport = new ShutdownBoundedTransport([self::registration(), self::temporaryFailure(), self::receipt()],
            static function (int $call): void { if ($call === 2) { usleep(2_000_000); } });
        $worker = null;
        $worker = new Worker(new Client('https://server.example', transport: $transport), 'queue', workerId: 'worker-a',
            diagnosticListener: static function (string $event) use (&$worker): void {
                if ($event === 'worker.registered') { $worker?->requestShutdown(); }
            });
        $started = microtime(true);
        $worker->run(0);
        self::assertGreaterThanOrEqual(3.0, microtime(true) - $started);
        self::assertLessThan($transport->requests[1]['timeout'], $transport->requests[2]['timeout']);
        self::assertLessThanOrEqual(7, $transport->requests[2]['timeout']);
    }

    public function testOriginalWorkerFailureSurvivesASecondShutdownFailure(): void
    {
        $original = new \RuntimeException('Original task failure.');
        $transport = new ShutdownBoundedTransport([self::registration(), $original,
            new TransportException('Forbidden shutdown.', 403, ['reason' => 'forbidden'])]);
        $events = [];
        $worker = new Worker(new Client('https://server.example', transport: $transport), 'queue', workerId: 'worker-a',
            diagnosticListener: static function (string $event) use (&$events): void { $events[] = $event; });
        $worker->registerWorkflow('tests.shutdown', static fn (WorkflowContext $context): string => 'unused');
        try {
            $worker->run(0);
            self::fail('Original task failure was lost.');
        } catch (\RuntimeException $error) {
            self::assertSame($original, $error);
        }
        self::assertContains('worker.failed', $events);
        self::assertContains('worker.shutdown_failed', $events);
        self::assertCount(3, $transport->requests);
    }

    /** @param list<array<string, mixed>|\Throwable> $responses
     *  @return array{ShutdownBoundedTransport, list<string>} */
    private function runShutdown(array $responses): array
    {
        $now = 1000.0;
        $events = [];
        $transport = new ShutdownBoundedTransport([self::registration(), ...$responses]);
        $worker = $this->worker($transport, $now, $events);
        $worker->run(0);
        return [$transport, $events];
    }

    /** @param list<string> $events */
    private function worker(ShutdownBoundedTransport $transport, float &$now, array &$events = []): Worker
    {
        $worker = null;
        $worker = new Worker(new Client('https://server.example', transport: $transport), 'queue', workerId: 'worker-a',
            clock: static function () use (&$now): float { return $now; },
            sleeper: static function (int $microseconds) use (&$now): void { $now += $microseconds / 1_000_000; },
            diagnosticListener: static function (string $event) use (&$events, &$worker): void {
                $events[] = $event;
                if ($event === 'worker.registered') { $worker?->requestShutdown(); }
            });
        return $worker;
    }

    /** @return array<string, mixed> */
    private static function registration(): array
    {
        return ['registered' => true, 'registration_token' => self::TOKEN,
            'server_capabilities' => ['worker_deregistration_fencing' => [
                'schema' => 'durable-workflow.v2.worker-deregistration.v1', 'supported' => true,
                'receipt_retention_seconds' => 600, 'endpoint' => '/worker/registrations/{workerId}/deregister',
            ]]];
    }

    /** @return array<string, mixed> */
    private static function receipt(): array
    {
        return ['worker_id' => 'worker-a', 'registration_token' => self::TOKEN,
            'outcome' => 'deregistered', 'recovered_workflow_task_count' => 1];
    }

    private static function temporaryFailure(): TransportException
    {
        return new TransportException('SQLite temporarily locked.', 503, [
            'reason' => 'backend_lock_pressure', 'operation' => 'deregister_worker',
            'worker_id' => 'worker-a', 'registration_token' => self::TOKEN,
            'outcome' => 'unknown', 'retryable' => true, 'retry_after_seconds' => 1,
        ]);
    }
}

final class ShutdownBoundedTransport implements BoundedTransport
{
    /** @var list<array{method: string, uri: string, body: ?array<string, mixed>, timeout: ?int}> */
    public array $requests = [];

    /** @param list<array<string, mixed>|\Throwable> $responses */
    public function __construct(private array $responses, private readonly ?\Closure $beforeReply = null) {}

    public function supportsBoundedRequests(): bool { return true; }

    public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
    {
        return $this->respond($method, $uri, $body, null);
    }

    public function sendBounded(string $method, string $uri, array $headers, ?array $body, int $timeoutSeconds): ?array
    {
        return $this->respond($method, $uri, $body, $timeoutSeconds);
    }

    /** @param array<string, mixed>|null $body
     *  @return array<string, mixed>|null */
    private function respond(string $method, string $uri, ?array $body, ?int $timeout): ?array
    {
        $this->requests[] = compact('method', 'uri', 'body', 'timeout');
        if ($this->beforeReply !== null) { ($this->beforeReply)(count($this->requests)); }
        $response = array_shift($this->responses);
        if ($response instanceof \Throwable) { throw $response; }
        return $response;
    }
}
