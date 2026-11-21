<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Transport\RuntimePayloads;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Promise\Create;
use GuzzleHttp\Promise\PromiseInterface;
use GuzzleHttp\Psr7\Response;
use GuzzleHttp\TransferStats;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;

/**
 * The Server's inline limit can drop while a Worker holds an older payload
 * policy. The Server refuses the inline completion (413 payload_too_large)
 * before reading it; the Worker rediscovers, externalises the result under the
 * new limit and sends it once more. It never exits and never loses the result.
 */
final class WorkerStalePayloadPolicyTest extends TestCase
{
    public const OLD_LIMIT = 2_097_152;
    public const NEW_LIMIT = 102_400;

    private static function server(int $discoveryOutages = 0, bool $redeliver = false, bool $storage = true): object
    {
        return new class ($discoveryOutages, $redeliver, $storage) {
            /** @var list<string> */
            public array $paths = [];
            public int $limit = WorkerStalePayloadPolicyTest::OLD_LIMIT;
            public int $discoveries = 0;
            public int $refusals = 0;
            public int $uploads = 0;
            public ?array $completion = null;
            public ?array $failure = null;
            public float $now = 0.0;

            public function __construct(private int $discoveryOutages, private bool $redeliver, private bool $storage)
            {
            }

            /** @param array<string, mixed> $options */
            public function __invoke(RequestInterface $request, array $options): PromiseInterface
            {
                $path = $request->getUri()->getPath();
                $this->paths[] = $path;
                if ($path === '/api/cluster/info') {
                    if (++$this->discoveries > 1 && $this->discoveryOutages-- > 0) {
                        $this->now += 5.0;
                        ($options['on_stats'] ?? static fn () => null)(new TransferStats($request, null, 0.0, 7));

                        return Create::rejectionFor(new ConnectException('Connection refused', $request));
                    }
                    $answer = ['limits' => ['max_payload_bytes' => $this->limit], 'namespace' => ['external_payload_storage' => [
                        'status' => 'available', 'threshold_bytes' => $this->limit,
                        'transport' => ['schema' => 'durable-workflow.v2.runtime-external-payload-transport.v1', 'version' => 1,
                            'reference_schema' => RuntimePayloads::SCHEMA, 'mode' => 'authenticated_namespace_runtime',
                            'upload' => ['method' => 'POST', 'path' => '/api/external-payloads/v1'],
                            'fetch' => ['method' => 'GET', 'path_template' => '/api/external-payloads/v1/{referenceId}'],
                            'limits' => ['max_payload_bytes' => 16_777_216, 'request_timeout_seconds' => 45]]]]];
                    if (!$this->storage && $this->limit === WorkerStalePayloadPolicyTest::NEW_LIMIT) {
                        unset($answer['namespace']);
                    }
                    // The operator lowers the inline limit right after this Worker read the old one.
                    $this->limit = WorkerStalePayloadPolicyTest::NEW_LIMIT;

                    return self::json(200, $answer);
                }
                if ($path === '/api/external-payloads/v1') {
                    ++$this->uploads;
                    $blob = (string) $request->getBody();

                    return self::json(200, ['schema' => 'durable-workflow.v2.runtime-external-payload-upload.v1', 'transport_version' => 1,
                        'reference' => ['schema' => RuntimePayloads::SCHEMA, 'reference_id' => 'ep_01ARZ3NDEKTSV4RRFFQ69G5FAV',
                            'codec' => 'avro', 'size_bytes' => strlen($blob), 'sha256' => hash('sha256', $blob)]]);
                }
                if ($path === '/api/worker/activity-tasks/activity-1/fail') {
                    $this->failure = json_decode((string) $request->getBody(), true, flags: JSON_THROW_ON_ERROR);

                    return self::json(200, ['recorded' => true, 'outcome' => 'failed']);
                }
                if ($path === '/api/worker/activity-tasks/activity-1/complete') {
                    if ($request->getBody()->getSize() > $this->limit) {
                        // The Server's admission check refuses before reading the body.
                        ++$this->refusals;

                        return self::json(413, ['reason' => 'payload_too_large', 'limit' => $this->limit,
                            'message' => "Request payload exceeds the maximum allowed size of {$this->limit} bytes."]);
                    }
                    $this->completion = json_decode((string) $request->getBody(), true, flags: JSON_THROW_ON_ERROR);

                    return self::json(200, ['recorded' => true, 'outcome' => 'completed']);
                }

                return self::json(200, match (true) {
                    str_ends_with($path, '/worker/register') => ['registered' => true],
                    // Offered until it is completed (as after a lease expiry).
                    str_ends_with($path, '/activity-tasks/poll') && $this->completion === null && $this->failure === null
                        && ($this->redeliver || !in_array('/api/worker/activity-tasks/activity-1/complete', $this->paths, true)) => [
                            'poll_status' => 'leased', 'task' => ['task_id' => 'activity-1', 'activity_attempt_id' => 'attempt-1',
                                'lease_owner' => 'worker-1', 'activity_type' => 'reports.render', 'payload_codec' => 'avro']],
                    str_ends_with($path, '/query-tasks/poll') && ($this->completion !== null || $this->failure !== null || count($this->paths) > 300) => [
                        'task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'],
                    default => ['task' => null, 'poll_status' => 'empty'],
                });
            }

            /** @param array<string, mixed> $body */
            private static function json(int $status, array $body): PromiseInterface
            {
                return Create::promiseFor(new Response($status, ['Content-Type' => 'application/json'], json_encode($body, JSON_THROW_ON_ERROR)));
            }
        };
    }

    /** @return array{0: Worker, 1: \ArrayObject<int, mixed>, 2: \ArrayObject<int, mixed>} */
    private static function worker(object $server): array
    {
        $events = new \ArrayObject();
        $runs = new \ArrayObject();
        $worker = new Worker(
            new Client('https://runtime.test', transport: new Psr18Transport(new GuzzleClient(['handler' => $server])), workerToken: 'w'),
            'reports',
            workerId: 'worker-1',
            clock: static fn (): float => $server->now,
            sleeper: static function (int $microseconds) use ($server): void {
                $server->now += $microseconds / 1_000_000;
            },
            diagnosticListener: static function (string $event, array $context) use ($events): void {
                $events[] = $event;
            },
        );
        $worker->registerActivity('reports.render', static function (ActivityContext $context) use ($runs): string {
            $runs[] = true;

            return str_repeat('r', 500_000);
        });

        return [$worker, $events, $runs];
    }

    public function testALoweredInlineLimitIsRediscoveredAndTheResultExternalisedAndSentOnceMore(): void
    {
        $server = self::server();
        [$worker, $events, $runs] = self::worker($server);

        $worker->run(0);

        self::assertCount(1, $runs, 'The activity ran once; its result was never recomputed.');
        self::assertSame(1, $server->refusals, 'One inline attempt met the lowered limit.');
        self::assertSame(2, $server->discoveries, 'The stale policy was dropped and read again.');
        self::assertSame(1, $server->uploads, 'The result was externalised under the new limit.');
        self::assertIsArray($server->completion);
        self::assertSame('ep_01ARZ3NDEKTSV4RRFFQ69G5FAV', $server->completion['result']['external_payload']['reference_id'] ?? null);
        self::assertSame([], preg_grep('~/fail$~', $server->paths), 'Nothing is reported as an activity failure.');
        self::assertNotContains('worker.fatal_failure', $events->getArrayCopy());
        self::assertNotContains('worker.failed', $events->getArrayCopy());
    }

    public function testRediscoveryThatMeetsAnOutageIsRetriedWithoutLosingTheResult(): void
    {
        $server = self::server(discoveryOutages: 4);
        [$worker, $events, $runs] = self::worker($server);

        $worker->run(0);

        self::assertCount(1, $runs, 'The computed result was kept through the outage.');
        self::assertSame(1, $server->uploads);
        self::assertSame('ep_01ARZ3NDEKTSV4RRFFQ69G5FAV', $server->completion['result']['external_payload']['reference_id'] ?? null);
        self::assertContains('worker.retrying', $events->getArrayCopy());
        self::assertSame([], preg_grep('~/fail$~', $server->paths));
        self::assertNotContains('worker.fatal_failure', $events->getArrayCopy());
    }

    public function testAnOutageThatOutlastsTheClaimLeavesTheAttemptToTheServerAndTheWorkerRuns(): void
    {
        $server = self::server(discoveryOutages: 30, redeliver: true);
        [$worker, $events, $runs] = self::worker($server);

        $worker->run(0);

        self::assertContains('worker.outcome_unpublished', $events->getArrayCopy());
        self::assertContains('worker.outage_persisting', $events->getArrayCopy());
        self::assertSame('ep_01ARZ3NDEKTSV4RRFFQ69G5FAV', $server->completion['result']['external_payload']['reference_id'] ?? null,
            'The re-delivered attempt completes once discovery answers.');
        self::assertSame([], preg_grep('~/fail$~', $server->paths), 'Never reported as an activity failure.');
        self::assertNotContains('worker.fatal_failure', $events->getArrayCopy());
    }

    public function testAResultTooLargeEvenUnderTheNewLimitFailsTheAttemptAndTheWorkerRuns(): void
    {
        $server = self::server(storage: false);
        [$worker, $events, $runs] = self::worker($server);

        $worker->run(0);

        self::assertCount(1, $runs);
        self::assertSame(2, $server->discoveries, 'The policy was read again before giving up.');
        self::assertNull($server->completion);
        self::assertIsArray($server->failure, 'The attempt is reported as failed.');
        self::assertSame('ActivityResultTooLarge', $server->failure['failure']['type'] ?? null, 'The same in every SDK.');
        self::assertSame('ActivityResultTooLarge', $server->failure['failure']['class'] ?? null);
        self::assertTrue($server->failure['failure']['non_retryable'] ?? null, 'The same result would be refused again.');
        self::assertNotContains('worker.fatal_failure', $events->getArrayCopy());
        self::assertNotContains('worker.failed', $events->getArrayCopy());
    }
}
