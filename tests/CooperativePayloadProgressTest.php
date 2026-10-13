<?php
declare(strict_types=1);
namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ExternalPayloadException;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Transport\RequestBudget;
use DurableWorkflow\Transport\RuntimePayloads;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\WorkflowClaimRevoked;
use GuzzleHttp\Client as Http;
use GuzzleHttp\Promise\FulfilledPromise;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;

final class CooperativePayloadProgressTest extends TestCase
{
    public function testWorkflowLoadingKeepsOriginalCancellationDeadlineAndRefusesChangedIdentity(): void
    {
        foreach (['expired', 'changed'] as $outcome) {
            $now = microtime(true);
            $calls = 0;
            $observation = ['request_id' => 'original-cancel', 'requested_at' => gmdate('Y-m-d\TH:i:s\Z', (int) $now - 1),
                'cleanup_deadline_at' => gmdate('Y-m-d\TH:i:s\Z', (int) $now + 15), 'history_refresh_page_token' => 'original-page'];
            $http = new Http(['handler' => static function (RequestInterface $request, array $options) use (&$calls, $observation): FulfilledPromise {
                self::assertLessThanOrEqual(5, $options['timeout']);
                if (str_contains($request->getUri()->getPath(), '/workflow-tasks/')) {
                    $body = json_decode((string) $request->getBody(), true, flags: JSON_THROW_ON_ERROR);
                    self::assertSame('worker-1', $body['lease_owner']);
                    self::assertSame(1, $body['workflow_task_attempt']);
                    $cancel = $observation;
                    if (++$calls > 1) $cancel['request_id'] = 'different-cancel';
                    return new FulfilledPromise(new Response(200, [], json_encode(['renewed' => true,
                        'task_id' => 'task-1', 'workflow_task_attempt' => 1, 'lease_owner' => 'worker-1',
                        'cancellation_request' => $cancel], JSON_THROW_ON_ERROR)));
                }
                return new FulfilledPromise(new Response(200, [], '{"acknowledged":true}'));
            }]);
            $worker = new Worker(new Client('http://runtime', transport: new Psr18Transport($http)), 'queue',
                workerId: 'worker-1', clock: static function () use (&$now): float { return $now; }, enableCooperativeCancellation: true);
            $observer = (new \ReflectionMethod($worker, 'workerPayloadProgress'))->invoke($worker, '/worker/workflow-tasks/poll', [],
                ['task' => ['task_id' => 'task-1', 'workflow_task_attempt' => 1, 'lease_owner' => 'worker-1']], null);
            $observer();
            $now += $outcome === 'expired' ? 20 : 11;
            try {
                $observer();
                self::fail('Loading escaped original cancellation authority.');
            } catch (WorkflowClaimRevoked $error) {
                self::assertSame($outcome === 'expired' ? 'payload_deadline_expired' : 'payload_lease_unconfirmed', $error->reason);
            }
            self::assertSame($outcome === 'expired' ? 1 : 2, $calls);
        }
    }

    public function testExpiredQueryStopsBeforePresenceOrApplicationDispatch(): void
    {
        $calls = 0;
        $http = new Http(['handler' => static function () use (&$calls): FulfilledPromise {
            ++$calls;
            return new FulfilledPromise(new Response(200, [], '{"acknowledged":true}'));
        }]);
        $worker = new Worker(new Client('http://runtime', transport: new Psr18Transport($http)), 'queue',
            workerId: 'worker-1', enableCooperativeCancellation: true);
        (new \ReflectionProperty($worker, 'registered'))->setValue($worker, true);
        $observer = (new \ReflectionMethod($worker, 'workerPayloadProgress'))->invoke($worker, '/worker/query-tasks/poll', [],
            ['task' => ['query_task_id' => 'query-1', 'lease_expires_at' => '2000-01-01T00:00:00Z']], null);
        try {
            $observer();
            self::fail('Expired query loading made another request.');
        } catch (WorkflowClaimRevoked $error) {
            self::assertSame('payload_deadline_expired', $error->reason);
        }
        self::assertSame(0, $calls);
    }

    public function testActivityLoadingObservesCancellationWithoutApplicationHeartbeats(): void
    {
        $now = microtime(true);
        $statuses = 0;
        $http = new Http(['handler' => static function (RequestInterface $request, array $options) use (&$statuses): FulfilledPromise {
            self::assertLessThanOrEqual(5, $options['timeout']);
            if (str_contains($request->getUri()->getPath(), '/activity-tasks/')) {
                self::assertStringEndsWith('/status', $request->getUri()->getPath());
                $body = json_decode((string) $request->getBody(), true, flags: JSON_THROW_ON_ERROR);
                self::assertSame(['activity_attempt_id' => 'attempt-1', 'lease_owner' => 'worker-1'], $body);
                $cancelled = ++$statuses > 1;
                return new FulfilledPromise(new Response(200, [], json_encode(['task_id' => 'task-1',
                    'activity_attempt_id' => 'attempt-1', 'lease_owner' => 'worker-1', 'can_continue' => !$cancelled,
                    'cancel_requested' => $cancelled, 'heartbeat_recorded' => false, 'reason' => null,
                    'lease_expires_at' => '2100-01-01T00:00:00Z'], JSON_THROW_ON_ERROR)));
            }
            return new FulfilledPromise(new Response(200, [], '{"acknowledged":true}'));
        }]);
        $worker = new Worker(new Client('http://runtime', transport: new Psr18Transport($http)), 'queue',
            workerId: 'worker-1', clock: static function () use (&$now): float { return $now; }, enableCooperativeCancellation: true);
        (new \ReflectionProperty($worker, 'registered'))->setValue($worker, true);
        $observer = (new \ReflectionMethod($worker, 'workerPayloadProgress'))->invoke($worker, '/worker/activity-tasks/poll', [],
            ['task' => ['task_id' => 'task-1', 'activity_attempt_id' => 'attempt-1', 'lease_owner' => 'worker-1']], null);
        $observer();
        $now += 2;
        try {
            $observer();
            self::fail('Cancelled activity loading continued.');
        } catch (WorkflowClaimRevoked $error) {
            self::assertSame('payload_activity_claim_unconfirmed', $error->reason);
        }
        self::assertSame(2, $statuses);
    }

    public function testRealSlowTransferRunsSupervisionDuringNativeCurlAndKeepsExactBytes(): void
    {
        $blob = str_repeat('p', 32768);
        $this->withTcpServer(static function ($connection) use ($blob): void {
            $headers = "HTTP/1.1 200 OK\r\nContent-Length: ".strlen($blob)."\r\n";
            foreach (self::metadata($blob) as $key => $value) { $headers .= $key.': '.$value."\r\n"; }
            fwrite($connection, $headers."\r\n");
            for ($offset = 0; $offset < strlen($blob); $offset += 4096) {
                usleep(750000);
                fwrite($connection, substr($blob, $offset, 4096));
                fflush($connection);
            }
        }, static function (string $uri) use ($blob): void {
            $calls = 0;
            $start = microtime(true);
            $result = (new Psr18Transport())->fetchPayloadWithBoundedProgress($uri, self::metadata($blob), strlen($blob),
                static function () use (&$calls): void { ++$calls; }, 10);
            self::assertSame($blob, $result);
            self::assertGreaterThan(5, microtime(true) - $start);
            self::assertGreaterThan(8, $calls, 'Native waiting and sink writes must both supervise.');
        });
    }

    public function testRealStalledTransferCanBeStoppedBeforeItsTransferTimeout(): void
    {
        $blob = 'payload';
        $this->withTcpServer(static function ($connection) use ($blob): void {
            $headers = "HTTP/1.1 200 OK\r\nContent-Length: ".strlen($blob)."\r\n";
            foreach (self::metadata($blob) as $key => $value) { $headers .= $key.': '.$value."\r\n"; }
            fwrite($connection, $headers."\r\n");
            fflush($connection);
            sleep(15);
        }, static function (string $uri) use ($blob): void {
            $start = microtime(true);
            $stop = new WorkflowClaimRevoked('worker_stopped', 'Original shutdown');
            try {
                (new Psr18Transport())->fetchPayloadWithBoundedProgress($uri, self::metadata($blob), strlen($blob),
                    static function () use ($start, $stop): void { if (microtime(true) - $start >= 1) throw $stop; }, 10);
                self::fail('A stalled transfer ignored shutdown.');
            } catch (WorkflowClaimRevoked $error) {
                self::assertSame($stop, $error);
                self::assertLessThan(3, microtime(true) - $start);
            }
        });
    }

    public function testTransferBudgetCanBeSelectedWithoutChangingControlBounds(): void
    {
        $blob = 'payload';
        $http = new Http(['handler' => static function (RequestInterface $request, array $options) use ($blob): FulfilledPromise {
            self::assertSame(180, $options['timeout']);
            $options['sink']->write($blob);
            $options['sink']->rewind();
            return new FulfilledPromise(new Response(200, self::metadata($blob), $options['sink']));
        }]);
        $transport = new Psr18Transport($http, payloadTransferTimeoutSeconds: 180);
        self::assertSame($blob, $transport->fetchPayloadWithBoundedProgress('http://runtime/payload', self::metadata($blob), strlen($blob), static function (): void {}));
        $this->expectException(\InvalidArgumentException::class);
        $transport->sendBounded('GET', 'http://runtime/control', [], null, 180);
    }

    public function testSupervisionFailurePreservesItsOriginalExceptionAndStopsTheBody(): void
    {
        $original = new WorkflowClaimRevoked('worker_stopped', 'Original stop');
        $written = false;
        $http = new Http(['handler' => static function (RequestInterface $request, array $options) use (&$written): FulfilledPromise {
            $options['progress'](100, 0, 0, 0);
            $written = true;
            return new FulfilledPromise(new Response(200));
        }]);
        try {
            (new Psr18Transport($http))->fetchPayloadWithBoundedProgress('http://runtime/payload', [], 100,
                static function () use ($original): never { throw $original; });
            self::fail('A revoked claim returned bytes.');
        } catch (WorkflowClaimRevoked $error) {
            self::assertSame($original, $error);
        }
        self::assertFalse($written);
    }

    public function testExplicitBudgetIsSharedAcrossReferencesAndCannotRestart(): void
    {
        $blob = (new AvroPayloadCodec())->envelope(['value'])['blob'];
        $calls = 0;
        $http = new Http(['handler' => static function (RequestInterface $request, array $options) use ($blob, &$calls): FulfilledPromise {
            ++$calls;
            self::assertSame(1, $options['timeout']);
            usleep(1100000);
            $options['progress'](strlen($blob), 0, 0, 0);
            $options['sink']->write($blob);
            $options['sink']->rewind();
            return new FulfilledPromise(new Response(200, self::metadata($blob), $options['sink']));
        }]);
        $budget = new RequestBudget(1);
        $one = RuntimeExternalPayloadTest::reference($blob);
        $two = array_replace($one, ['reference_id' => 'ep_01ARZ3NDEKTSV4RRFFQ69G5FAW']);
        try {
            (new RuntimePayloads(new Psr18Transport($http), 'http://runtime', [], 4096, $budget,
                static function () use ($budget): void { $budget->remainingSeconds(); }, true))->response([
                    'task' => ['arguments' => ['codec' => 'avro', 'external_payload' => $one],
                        'workflow_arguments' => ['codec' => 'avro', 'external_payload' => $two]],
                ], '/worker/workflow-tasks/poll', true);
            self::fail('An expired original budget returned a hydrated task.');
        } catch (TransportException $error) {
            self::assertStringContainsString('time budget expired', $error->getMessage());
        }
        self::assertSame(1, $calls);
    }

    public function testSupervisedTransfersKeepIntegrityAndAuthorizationRefusals(): void
    {
        foreach ([401, 200] as $status) {
            $http = new Http(['handler' => static function (RequestInterface $request, array $options) use ($status): FulfilledPromise {
                $options['sink']->write('bad');
                $options['sink']->rewind();
                return new FulfilledPromise(new Response($status, [], $options['sink']));
            }]);
            try {
                (new Psr18Transport($http))->fetchPayloadWithBoundedProgress('http://runtime/payload', self::metadata('expected'), 8, static function (): void {});
                self::fail('An unauthorized or invalid payload returned bytes.');
            } catch (ExternalPayloadException $error) {
                self::assertSame($status === 401 ? 'external_payload_unauthorized' : 'external_payload_integrity_mismatch', $error->reason);
            }
        }
    }

    public function testClaimedPayloadsStreamWithPresenceAndSeparateControlBounds(): void
    {
        $blob = (new AvroPayloadCodec())->envelope([str_repeat('p', 32768)])['blob'];
        $presence = $progress = $fetches = 0;
        $client = null;
        $http = new Http(['handler' => static function (RequestInterface $request, array $options) use ($blob, &$presence, &$fetches): FulfilledPromise {
            $path = $request->getUri()->getPath();
            if (str_ends_with($path, '/poll')) {
                self::assertLessThanOrEqual(10, $options['timeout']);
                return new FulfilledPromise(new Response(200, [], json_encode(['task' => ['query_task_id' => 'query-1',
                    'query_task_attempt' => 1, 'lease_owner' => 'worker-1', 'arguments' => ['codec' => 'avro',
                    'external_payload' => RuntimeExternalPayloadTest::reference($blob)]]], JSON_THROW_ON_ERROR)));
            }
            if (str_ends_with($path, '/worker/heartbeat')) {
                ++$presence;
                self::assertLessThanOrEqual(5, $options['timeout'], 'Body time must not enlarge a presence RPC.');
                return new FulfilledPromise(new Response(200, [], '{"acknowledged":true}'));
            }
            if (str_contains($path, '/external-payloads/')) {
                ++$fetches;
                self::assertGreaterThan(5, $options['timeout'], 'Valid slow bytes need their own finite transfer window.');
                self::assertLessThanOrEqual(65, $options['timeout']);
                self::assertFalse($options['stream']);
                self::assertArrayHasKey('progress', $options, 'Supervision must also run while waiting for bytes.');
                for ($offset = 0; $offset < strlen($blob); $offset += 8192) {
                    $options['progress'](strlen($blob), $offset, 0, 0);
                    $options['sink']->write(substr($blob, $offset, 8192));
                }
                $options['sink']->rewind();
                return new FulfilledPromise(new Response(200, [
                    'Content-Type' => 'application/octet-stream',
                    'X-Durable-Workflow-Payload-Codec' => 'avro',
                    'X-Durable-Workflow-Payload-Size' => (string) strlen($blob),
                    'X-Durable-Workflow-Payload-SHA256' => hash('sha256', $blob),
                ], $options['sink']));
            }
            throw new \RuntimeException('Unexpected request '.$path);
        }]);
        $client = (new Client('http://runtime', transport: new Psr18Transport($http)))->withBoundedWorkerRequests()
            ->withWorkerPayloadObserver(static function () use (&$client, &$progress): \Closure {
                return static function () use (&$client, &$progress): void {
                    ++$progress;
                    $client->heartbeatWorker('worker-1', [], new RequestBudget(5));
                };
            });
        $response = $client->pollQueryTaskResponse('worker-1', 'queue', 5, 'original-poll-1');
        self::assertSame('query-1', $response['task']['query_task_id']);
        self::assertSame($blob, $response['task']['arguments']['blob']);
        self::assertSame(1, $fetches);
        self::assertGreaterThan(3, $progress);
        self::assertSame($progress, $presence);
    }

    private static function metadata(string $blob): array
    {
        return ['Content-Type' => 'application/octet-stream', 'X-Durable-Workflow-Payload-Codec' => 'avro',
            'X-Durable-Workflow-Payload-Size' => (string) strlen($blob),
            'X-Durable-Workflow-Payload-SHA256' => hash('sha256', $blob)];
    }

    private function withTcpServer(callable $serve, callable $request): void
    {
        if (!extension_loaded('curl') || !function_exists('pcntl_fork') || !function_exists('posix_kill')) {
            self::markTestSkipped('Real cURL and Unix process control are required.');
        }
        $listener = stream_socket_server('tcp://127.0.0.1:0', $errno, $error);
        self::assertIsResource($listener);
        $address = stream_socket_get_name($listener, false);
        self::assertIsString($address);
        $pid = pcntl_fork();
        self::assertGreaterThanOrEqual(0, $pid);
        if ($pid === 0) {
            $connection = stream_socket_accept($listener, 3);
            if ($connection !== false) {
                while (($line = fgets($connection)) !== false && $line !== "\r\n") {}
                $serve($connection);
                fclose($connection);
            }
            posix_kill(getmypid(), SIGKILL);
        }
        fclose($listener);
        try {
            $request('http://'.$address);
        } finally {
            posix_kill($pid, SIGKILL);
            self::assertSame($pid, pcntl_waitpid($pid, $status));
        }
    }
}
