<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use Closure;
use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ExternalPayloadException;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\PollResponse;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\WorkflowContext;
use GuzzleHttp\Psr7\PumpStream;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

final class WorkerExternalPayloadRecoveryTest extends TestCase
{
    #[DataProvider('recoverablePolls')]
    public function testAdmittedTaskRecoversWithOriginalPollIdentityAndExecutesOnce(string $kind, string $failure, int $failures): void
    {
        $blob = (new AvroPayloadCodec())->envelope(['portable'])['blob'];
        $polls = [];
        $fetches = $registrations = $heartbeats = $executions = $completions = 0;
        $delays = [];
        $now = 0.0;
        $client = self::client(static function (RequestInterface $request) use (
            $kind, $failure, $failures, $blob, &$polls, &$fetches, &$registrations, &$heartbeats, &$completions,
        ): ResponseInterface {
            $path = $request->getUri()->getPath();
            if (str_ends_with($path, '/register')) {
                ++$registrations;
                return self::json(['registered' => true, 'heartbeat_interval_seconds' => 1]);
            }
            if (str_ends_with($path, '/heartbeat')) {
                if (str_ends_with($path, '/worker/heartbeat')) {
                    ++$heartbeats;
                    return self::json(['acknowledged' => true]);
                }
                return self::json(['renewed' => true, 'task_id' => 'task-1',
                    'workflow_task_attempt' => 1, 'lease_owner' => 'worker-1']);
            }
            if (str_contains($path, '/external-payloads/')) {
                ++$fetches;
                self::assertSame('Bearer fixture-worker', $request->getHeaderLine('Authorization'));
                if ($fetches <= $failures) {
                    if ($failure === 'http-503') {
                        return new Response(503, [], '{"reason":"external_payload_unavailable","retryable":true}');
                    }
                    return self::payload($blob, new PumpStream(static function (): never {
                        throw new \RuntimeException('Injected payload stream read timeout.');
                    }));
                }
                return self::payload($blob, $blob);
            }
            if (str_ends_with($path, '/complete')) {
                $body = json_decode((string) $request->getBody(), true, flags: JSON_THROW_ON_ERROR);
                self::assertSame('worker-1', $body['lease_owner']);
                if ($kind === 'activity') {
                    self::assertSame('attempt-1', $body['activity_attempt_id']);
                }
                if ($kind === 'workflow') {
                    self::assertSame(1, $body['workflow_task_attempt']);
                    self::assertSame('complete_workflow', $body['commands'][0]['type']);
                }
                ++$completions;
                return self::json(['completed' => true]);
            }
            if (str_ends_with($path, '/poll')) {
                if ($completions > 0) {
                    return self::json(['task' => null, 'poll_status' => 'stopped']);
                }
                if (!str_ends_with($path, "/{$kind}-tasks/poll")) {
                    return self::json(['task' => null, 'poll_status' => 'empty']);
                }
                $polls[] = (string) $request->getBody();
                return self::json(['poll_status' => 'leased', 'task' => self::task($blob)]);
            }
            return self::json(['deregistered' => true]);
        });
        $worker = new Worker($client, 'payload-workers', workerId: 'worker-1',
            clock: static function () use (&$now): float { return $now; },
            sleeper: static function (int $us) use (&$now): void {
                $now += $us / 1_000_000;
                self::assertLessThan(30, $now, 'Recovery must terminate.');
            }, transientPollRetryObserver: static function (string $phase, int $attempt, float $delay) use (&$delays): void {
                $delays[] = $delay;
            });
        $handler = static function (string $argument) use (&$executions): string {
            self::assertSame('portable', $argument);
            ++$executions;
            return 'recovered';
        };
        $worker->registerWorkflow('payload.workflow', static fn (WorkflowContext $context, string $argument): string => $handler($argument))
            ->registerActivity('payload.activity', static fn (ActivityContext $context, string $argument): string => $handler($argument))
            ->registerQuery('payload.workflow', 'payload.query', static fn (QueryContext $context, string $argument): string => $handler($argument));

        $worker->run(0);

        self::assertSame(1, $registrations);
        self::assertSame(1, $executions);
        self::assertSame(1, $completions);
        self::assertSame($failures + 1, $fetches);
        self::assertCount($failures + 1, $polls);
        self::assertCount(1, array_unique($polls), 'An admitted poll must retain its original request ID.');
        self::assertSame(array_slice([0.1, 0.2, 0.4, 0.8, 1.6, 3.2, 5.0], 0, $failures), $delays);
        if ($failures === 7) {
            self::assertGreaterThanOrEqual(5, $heartbeats, 'Worker health must continue during payload recovery.');
        }
    }

    public static function recoverablePolls(): iterable
    {
        foreach (['workflow', 'activity', 'query'] as $kind) {
            foreach (['stream-timeout', 'http-503'] as $failure) {
                yield "$kind $failure" => [$kind, $failure, 2];
            }
        }
        yield 'worker heartbeats through long payload outage' => ['activity', 'stream-timeout', 7];
    }

    #[DataProvider('permanentFailures')]
    public function testPermanentPayloadFailuresDoNotRetry(int $status, string $reason): void
    {
        $blob = (new AvroPayloadCodec())->envelope(['portable'])['blob'];
        $fetches = $polls = 0;
        $worker = new Worker(self::client(static function (RequestInterface $request) use ($blob, $status, $reason, &$fetches, &$polls): ResponseInterface {
            if (str_contains($request->getUri()->getPath(), '/external-payloads/')) {
                ++$fetches;
                if ($reason === 'external_payload_integrity_mismatch') {
                    return self::payload($blob, str_repeat('x', strlen($blob)));
                }
                return new Response($status, [], json_encode(['reason' => $reason, 'retryable' => false], JSON_THROW_ON_ERROR));
            }
            ++$polls;
            return self::json(['task' => self::task($blob)]);
        }), 'payload-workers');
        try {
            $worker->tick(0);
            self::fail('Permanent payload failure must be surfaced.');
        } catch (ExternalPayloadException $exception) {
            self::assertSame($reason, $exception->reason);
            self::assertSame(1, $polls);
            self::assertSame(1, $fetches);
        }
    }

    public static function permanentFailures(): iterable
    {
        yield 'authentication' => [401, 'external_payload_unauthorized'];
        yield 'authorization' => [403, 'external_payload_unauthorized'];
        yield 'missing reference' => [404, 'external_payload_not_found'];
        yield 'expired reference' => [410, 'external_payload_expired'];
        yield 'oversized payload' => [413, 'external_payload_oversized'];
        yield 'unsupported payload' => [415, 'external_payload_unsupported'];
        yield 'wrong bytes' => [200, 'external_payload_integrity_mismatch'];
        yield 'explicit permanent refusal' => [503, 'external_payload_unavailable'];
    }

    public function testShutdownInterruptsPayloadRecoveryWithoutAnotherPoll(): void
    {
        $blob = (new AvroPayloadCodec())->envelope(['portable'])['blob'];
        $polls = $fetches = 0;
        $worker = null;
        $worker = new Worker(self::client(static function (RequestInterface $request) use ($blob, &$polls, &$fetches): ResponseInterface {
            if (str_contains($request->getUri()->getPath(), '/external-payloads/')) {
                ++$fetches;
                return new Response(503, [], '{"reason":"external_payload_unavailable"}');
            }
            ++$polls;
            return self::json(['task' => self::task($blob)]);
        }), 'payload-workers', sleeper: static function () use (&$worker): void { $worker->requestShutdown(); });

        self::assertFalse($worker->tick(0));
        self::assertSame(1, $polls);
        self::assertSame(1, $fetches);
    }

    public function testOrdinaryServerFailureCannotMasqueradeAsPayloadAvailability(): void
    {
        self::assertFalse(PollResponse::isTransientFailure(new ServerException('Refused.', 503, 'external_payload_unavailable')));
    }

    private static function task(string $blob): array
    {
        $arguments = ['codec' => 'avro', 'external_payload' => RuntimeExternalPayloadTest::reference($blob)];
        return ['task_id' => 'task-1', 'lease_owner' => 'worker-1', 'workflow_task_attempt' => 1,
            'activity_attempt_id' => 'attempt-1', 'workflow_id' => 'workflow-1', 'run_id' => 'run-1',
            'workflow_type' => 'payload.workflow', 'activity_type' => 'payload.activity', 'query_name' => 'payload.query',
            'payload_codec' => 'avro', 'arguments' => $arguments, 'query_arguments' => $arguments,
            'history_events' => []];
    }

    private static function client(Closure $handler): Client
    {
        $http = new class($handler) implements ClientInterface {
            public function __construct(private readonly Closure $handler) {}
            public function sendRequest(RequestInterface $request): ResponseInterface { return ($this->handler)($request); }
        };
        return new Client('https://server.example', transport: new Psr18Transport($http), workerToken: 'fixture-worker');
    }

    private static function payload(string $blob, mixed $body): ResponseInterface
    {
        return new Response(200, ['Content-Type' => 'application/octet-stream',
            'X-Durable-Workflow-Payload-Codec' => 'avro',
            'X-Durable-Workflow-Payload-Size' => (string) strlen($blob),
            'X-Durable-Workflow-Payload-SHA256' => hash('sha256', $blob)], $body);
    }

    private static function json(array $body): ResponseInterface
    {
        return new Response(200, [], json_encode($body, JSON_THROW_ON_ERROR));
    }
}
