<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use Closure;
use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

final class WorkerUpstreamRecoveryTest extends TestCase
{
    #[DataProvider('upstreamResponses')]
    public function testHttpStatusSurvivesNonProtocolErrorBodies(int $status, string $body, bool $transient): void
    {
        $client = self::client(static fn () => new Response($status, [], $body));
        try {
            $client->heartbeatWorker('worker-1');
            self::fail('An HTTP error must be surfaced.');
        } catch (ServerException $exception) {
            self::assertSame($status, $exception->status);
            self::assertSame($transient, $exception->isTransientUpstreamFailure());
            self::assertFalse($exception->isTransientConnectionFailure());
            self::assertSame("Server returned HTTP {$status}.", $exception->getMessage());
            self::assertNull($exception->details);
        }
    }

    public static function upstreamResponses(): iterable
    {
        foreach ([502, 503, 504, 520, 521, 522, 523, 524, 530, 301, 400, 401, 403, 404, 409, 429, 500, 501, 525, 526] as $status) {
            foreach (['<html>sensitive origin diagnostics</html>', '', 'null', 'false', '"not an envelope"'] as $body) {
                yield "{$status}: {$body}" => [$status, $body, in_array($status, [502, 503, 504, 520, 521, 522, 523, 524, 530], true)];
            }
        }
    }

    #[DataProvider('pollKinds')]
    public function testAllPollKindsKeepTheirIdentityAndRecover(string $kind, int $status): void
    {
        $requests = [];
        $target = "/{$kind}-tasks/poll";
        $client = self::client(static function (RequestInterface $request) use (&$requests, $target, $status): ResponseInterface {
            if (str_ends_with($request->getUri()->getPath(), $target)) {
                $requests[] = (string) $request->getBody();
                if (count($requests) <= 2) {
                    return new Response($status, ['Content-Type' => 'text/html'], '<html>Unavailable</html>');
                }
            }

            return self::json(['task' => null, 'poll_status' => 'empty']);
        });
        $now = 0.0;
        $worker = self::worker($client, $now);

        self::assertFalse($worker->tick(0));
        self::assertCount(3, $requests);
        self::assertSame($requests[0], $requests[1]);
        self::assertSame($requests[0], $requests[2]);
        self::assertEqualsWithDelta(0.3, $now, 0.00001);
    }

    public static function pollKinds(): iterable
    {
        foreach (['workflow', 'activity', 'query'] as $kind) {
            foreach ([503, 530] as $status) {
                yield "{$kind} {$status}" => [$kind, $status];
            }
        }
    }

    public function testSameWorkerHeartbeatsThroughOutageThenExecutesActivityOnce(): void
    {
        $now = 0.0;
        $heartbeats = 0;
        $registrations = 0;
        $executions = 0;
        $completions = 0;
        $polls = [];
        $delays = [];
        $client = self::client(static function (RequestInterface $request) use (&$heartbeats, &$registrations, &$completions, &$polls): ResponseInterface {
            $path = $request->getUri()->getPath();
            if (str_ends_with($path, '/register')) {
                ++$registrations;
                return self::json(['registered' => true, 'heartbeat_interval_seconds' => 1]);
            }
            if (str_ends_with($path, '/heartbeat')) {
                if (++$heartbeats < 10) {
                    return new Response(530, [], '<html>Origin unreachable</html>');
                }
                return self::json(['acknowledged' => true]);
            }
            if ($heartbeats < 10) {
                $polls[] = (string) $request->getBody();
                return new Response(503, [], '<html>Unavailable</html>');
            }
            if (str_ends_with($path, '/activity-tasks/poll')) {
                return self::json(['task' => ['task_id' => 'activity-1', 'activity_attempt_id' => 'attempt-1',
                    'lease_owner' => 'worker-1', 'activity_type' => 'greet', 'payload_codec' => 'avro']]);
            }
            if (str_ends_with($path, '/activity-tasks/activity-1/complete')) {
                ++$completions;
                return self::json(['completed' => true]);
            }
            return self::json(['task' => null, 'poll_status' => str_ends_with($path, '/query-tasks/poll') ? 'stopped' : 'empty']);
        });
        $worker = self::worker($client, $now, static function (string $phase, int $attempt, float $delay) use (&$delays): void {
            if ($phase === 'heartbeat') $delays[] = $delay;
        });
        $worker->registerActivity('greet', static function (ActivityContext $context) use (&$executions): string {
            ++$executions;
            return 'hello';
        });
        $worker->run(0);

        self::assertSame(1, $registrations);
        self::assertSame(1, $executions);
        self::assertSame(1, $completions);
        self::assertSame([0.1, 0.2, 0.4, 0.8, 1.6, 3.2, 5.0, 5.0, 5.0], $delays);
        self::assertCount(1, array_unique($polls));
    }

    public function testShutdownInterruptsUpstreamBackoff(): void
    {
        $polls = 0;
        $deletes = 0;
        $now = 0.0;
        $worker = null;
        $client = self::client(static function (RequestInterface $request) use (&$polls, &$deletes): ResponseInterface {
            if ($request->getMethod() === 'DELETE') {
                ++$deletes;
                return self::json(['deregistered' => true]);
            }
            if (str_ends_with($request->getUri()->getPath(), '/register')) return self::json(['registered' => true]);
            ++$polls;
            return new Response(530, [], '<html>Unavailable</html>');
        });
        $worker = new Worker($client, 'orders', clock: static function () use (&$now): float { return $now; },
            sleeper: static function (int $us) use (&$now, &$worker): void { $now += $us / 1_000_000; $worker->requestShutdown(); });
        $worker->run(0);
        self::assertSame(1, $polls);
        self::assertSame(1, $deletes);
        self::assertLessThanOrEqual(0.1, $now);
    }

    #[DataProvider('terminalResponses')]
    public function testMalformedSuccessAndStructuredErrorsRemainTerminal(int $status, string $body): void
    {
        $calls = 0;
        $worker = new Worker(self::client(static function () use (&$calls, $status, $body): ResponseInterface {
            ++$calls;
            return new Response($status, [], $body);
        }), 'orders');
        try {
            $worker->tick(0);
            self::fail('This failure must remain terminal.');
        } catch (ServerException $exception) {
            self::assertFalse($exception->isTransientUpstreamFailure());
            self::assertSame(1, $calls);
        }
    }

    public static function terminalResponses(): iterable
    {
        yield 'malformed success' => [200, '<html>Wrong endpoint</html>'];
        yield 'scalar success' => [200, 'true'];
        yield 'auth' => [401, '<html>Unauthorized</html>'];
        yield 'explicit refusal' => [503, '{"reason":"service_unavailable","poll_status":"service_unavailable","task":null,"retryable":false}'];
        yield 'unknown structured error' => [503, '{"error":"refused"}'];
    }

    public function testRegistrationDoesNotBlindlyRepeatAnUncertainRequest(): void
    {
        $calls = 0;
        $worker = new Worker(self::client(static function () use (&$calls): ResponseInterface {
            ++$calls;
            return new Response(503, [], '<html>Unavailable</html>');
        }), 'orders');
        try {
            $worker->run(0);
            self::fail('Registration requires reconciliation, not blind retry.');
        } catch (ServerException $exception) {
            self::assertSame(503, $exception->status);
            self::assertTrue($exception->isTransientUpstreamFailure());
            self::assertSame(1, $calls);
        }
    }

    #[DataProvider('upstreamStatuses')]
    public function testUncertainCompletionDoesNotRepeatTheActivityOrAcknowledgement(int $status): void
    {
        $requests = [];
        $executions = 0;
        $client = self::client(static function (RequestInterface $request) use (&$requests, $status): ResponseInterface {
            $path = $request->getUri()->getPath();
            $requests[] = $path;
            if (str_ends_with($path, '/activity-tasks/poll')) {
                return self::json(['task' => [
                    'task_id' => 'activity-1',
                    'activity_attempt_id' => 'attempt-1',
                    'lease_owner' => 'worker-1',
                    'activity_type' => 'greet',
                    'payload_codec' => 'avro',
                ]]);
            }
            if (str_ends_with($path, '/activity-tasks/activity-1/complete')) {
                return new Response($status, [], '<html>Response lost</html>');
            }

            return self::json(['task' => null, 'poll_status' => 'empty']);
        });
        $worker = new Worker($client, 'orders', workerId: 'worker-1');
        $worker->registerActivity('greet', static function (ActivityContext $context) use (&$executions): string {
            ++$executions;

            return 'hello';
        });

        try {
            $worker->tick(0);
            self::fail('An ambiguous acknowledgement must be surfaced, not repeated.');
        } catch (ServerException $exception) {
            self::assertSame($status, $exception->status);
            self::assertTrue($exception->isTransientUpstreamFailure());
        }
        self::assertSame(1, $executions);
        self::assertSame([
            '/api/worker/workflow-tasks/poll',
            '/api/worker/activity-tasks/poll',
            '/api/cluster/info',
            '/api/worker/activity-tasks/activity-1/complete',
        ], $requests);
    }

    #[DataProvider('upstreamStatuses')]
    public function testHeartbeatFailureDoesNotDelayAnAlreadyLeasedActivity(int $status): void
    {
        $now = 0.0;
        $heartbeats = 0;
        $executions = 0;
        $client = self::client(static function (RequestInterface $request) use (&$now, &$heartbeats, $status): ResponseInterface {
            $path = $request->getUri()->getPath();
            if (str_ends_with($path, '/register')) {
                return self::json(['registered' => true, 'heartbeat_interval_seconds' => 1]);
            }
            if (str_ends_with($path, '/heartbeat')) {
                ++$heartbeats;

                return new Response($status, [], '<html>Heartbeat unavailable</html>');
            }
            if (str_ends_with($path, '/workflow-tasks/poll')) {
                $now += 1.0;
            }
            if (str_ends_with($path, '/activity-tasks/poll')) {
                $now += 1.0;

                return self::json(['task' => [
                    'task_id' => 'activity-1',
                    'activity_attempt_id' => 'attempt-1',
                    'lease_owner' => 'worker-1',
                    'activity_type' => 'greet',
                    'payload_codec' => 'avro',
                ]]);
            }

            return self::json(['task' => null, 'poll_status' => str_ends_with($path, '/query-tasks/poll') ? 'stopped' : 'empty']);
        });
        $worker = new Worker(
            $client,
            'orders',
            workerId: 'worker-1',
            clock: static function () use (&$now): float { return $now; },
            sleeper: static function (): void { self::fail('Heartbeat recovery must not delay a leased task.'); },
        );
        $worker->registerActivity('greet', static function (ActivityContext $context) use (&$executions): string {
            ++$executions;

            return 'hello';
        });
        $worker->run(0);

        self::assertSame(2, $heartbeats);
        self::assertSame(1, $executions);
    }

    /** @return iterable<string, array{int}> */
    public static function upstreamStatuses(): iterable
    {
        yield 'HTTP 503' => [503];
        yield 'HTTP 530' => [530];
    }

    private static function client(Closure $handler): Client
    {
        $http = new class($handler) implements ClientInterface {
            public function __construct(private readonly Closure $handler) {}
            public function sendRequest(RequestInterface $request): ResponseInterface { return ($this->handler)($request); }
        };
        return new Client('https://server.example', transport: new Psr18Transport($http));
    }

    private static function worker(Client $client, float &$now, ?Closure $observer = null): Worker
    {
        return new Worker($client, 'orders', workerId: 'worker-1',
            clock: static function () use (&$now): float { return $now; },
            sleeper: static function (int $us) use (&$now): void {
                $now += $us / 1_000_000;
                self::assertLessThan(60, $now, 'Retry must terminate after recovery.');
            }, transientPollRetryObserver: $observer);
    }

    private static function json(array $body): ResponseInterface
    {
        return new Response(200, ['Content-Type' => 'application/json'], json_encode($body, JSON_THROW_ON_ERROR));
    }
}
