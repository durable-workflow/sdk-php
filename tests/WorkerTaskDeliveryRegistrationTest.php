<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use Closure;
use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TaskDeliveryUncertain;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Tests\Support\RecordingMetricsRecorder;
use DurableWorkflow\Worker\TaskDelivery;
use Fiber;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;

final class WorkerTaskDeliveryRegistrationTest extends TestCase
{
    #[DataProvider('registrationRaces')]
    public function testTheResponseStillBelongsToItsRegistration(string $endpoint, string $kind, string $phase, string $change, bool $metrics): void
    {
        $registrations = 0;
        $replacementAttempts = 0;
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use (
            &$registrations, &$replacementAttempts, $kind, $phase, $change,
        ): array {
            if (str_ends_with($uri, '/worker/register')) {
                ++$registrations;
                return self::registration($registrations > 1 && in_array($change, ['token', 'unrelated'], true) ? 'b' : 'a');
            }
            $poll = $body['poll_request_id'];
            if (!isset($body['task_offer_token'])) {
                if ($poll === 'original' && $phase === 'offer') { Fiber::suspend(); }
                return self::offer($kind, $poll);
            }
            if ($poll === 'original' && $phase === 'accept') { Fiber::suspend(); }
            if ($poll === 'replacement' && ++$replacementAttempts === 1) {
                throw new TransportException('Synthetic lost response.', 504);
            }
            return self::claimed($kind, $poll);
        });
        $observed = 0;
        $client = (new Client('https://server.example', transport: $transport,
            metrics: $metrics ? new RecordingMetricsRecorder() : null))->withWorkerPayloadObserver(
                static function (string $path, ?array $body, array $response) use (&$observed, $phase): ?Closure {
                    if (($body['poll_request_id'] ?? null) === 'original' && ($response['task'] ?? null) !== null) {
                        ++$observed;
                        if ($phase === 'observer') { Fiber::suspend(); }
                    }
                    return null;
                });
        $client->registerWorker('worker', 'orders', [], []);
        $old = new Fiber(static function () use ($client, $endpoint, $kind): array {
            try { return ['result' => self::poll($client, $endpoint, $kind, 'original')]; }
            catch (ServerException $error) { return ['error' => $error]; }
        });
        $old->start();
        self::assertTrue($old->isSuspended());
        $replacement = clone $client;
        $queue = $change === 'queue' ? 'other-orders' : 'orders';
        $replacement->registerWorker($change === 'unrelated' ? 'other-worker' : 'worker', $queue, [], []);
        try {
            self::poll($replacement, $endpoint, $kind, 'replacement', $queue);
            self::fail('The replacement fixture must retain its uncertain packet.');
        } catch (TaskDeliveryUncertain) {
        }
        $old->resume();
        $outcome = $old->getReturn();
        if (in_array($change, ['same', 'unrelated'], true)) {
            self::assertSame(self::claimed($kind, 'original'), $outcome['result'] ?? null);
            self::assertSame(1, $observed);
        } else {
            self::assertArrayNotHasKey('result', $outcome, 'A stale successful receipt cannot authorize execution.');
            self::assertInstanceOf(ServerException::class, $outcome['error'] ?? null);
            self::assertSame($phase === 'observer' ? 1 : 0, $observed, 'Transport replacement must refuse before payload observation.');
        }
        $expected = self::claimed($kind, 'replacement');
        $actual = self::poll($replacement, $endpoint, $kind, 'replacement', $queue);
        self::assertSame($expected, $actual);
        $packets = array_values(array_filter($transport->requests, static fn (array $request): bool =>
            ($request['body']['poll_request_id'] ?? null) === 'replacement' && isset($request['body']['task_offer_token'])));
        self::assertCount(2, $packets);
        self::assertSame($packets[0], $packets[1], 'The old response must not mutate or erase the replacement packet.');
        self::assertSame(str_repeat($change === 'token' ? 'b' : 'a', 32), $packets[1]['body']['registration_token']);
        self::assertSame($queue, $packets[1]['body']['task_queue']);
    }

    /** @return iterable<string, array{string, string, string, string, bool}> */
    public static function registrationRaces(): iterable
    {
        $cases = [
            ['workflow', 'workflow', 'accept', 'token'],
            ['workflow', 'update_validation', 'accept', 'token'],
            ['activity', 'activity', 'accept', 'token'],
            ['query', 'query', 'accept', 'token'],
            ['update-validation', 'update_validation', 'accept', 'token'],
            ['workflow', 'workflow', 'offer', 'token'],
            ['workflow', 'workflow', 'accept', 'queue'],
            ['workflow', 'workflow', 'offer', 'queue'],
            ['workflow', 'workflow', 'observer', 'token'],
            ['workflow', 'workflow', 'accept', 'same'],
            ['workflow', 'workflow', 'accept', 'unrelated'],
        ];
        foreach ($cases as $case) {
            foreach ([false, true] as $metrics) {
                yield implode(' / ', $case).($metrics ? ' / metrics' : '') => [...$case, $metrics];
            }
        }
    }

    /** @return array<string, mixed> */
    private static function poll(Client $client, string $endpoint, string $kind, string $poll, string $queue = 'orders'): array
    {
        return (new ReflectionMethod(Client::class, 'pollTaskResponse'))->invoke($client, '/worker/'.$endpoint.'-tasks/poll', [
            'worker_id' => 'worker', 'task_queue' => $queue, 'timeout_seconds' => 4, 'poll_request_id' => $poll, 'task_kinds' => [$kind],
        ]);
    }

    /** @return array<string, mixed> */
    private static function registration(string $token): array
    {
        return ['registered' => true, 'registration_token' => str_repeat($token, 32), 'server_capabilities' => ['task_delivery' => [
            'schema' => TaskDelivery::SCHEMA, 'supported' => true, 'minimum_protocol_version' => '1.20',
            'worker_capability' => TaskDelivery::CAPABILITY, 'header' => TaskDelivery::HEADER, 'version' => TaskDelivery::VERSION,
            'endpoint_suffix' => '/offers', 'acceptance_timeout_seconds' => 2, 'unaccepted_offer_reserves_task' => false,
        ]]];
    }

    /** @return array<string, mixed> */
    private static function offer(string $kind, string $poll): array
    {
        return ['task' => null, 'poll_status' => 'offered', 'task_offer' => [
            'schema' => TaskDelivery::SCHEMA, 'task_kind' => $kind, 'task_id' => $poll, 'token' => 'synthetic-'.$poll, 'expires_at_ms' => 1000,
        ]];
    }

    /** @return array<string, mixed> */
    private static function claimed(string $kind, string $poll): array
    {
        $field = match ($kind) { 'query' => 'query_task_id', 'update_validation' => 'update_validation_task_id', default => 'task_id' };
        return ['task' => [$field => $poll, 'lease_owner' => 'worker', 'task_kind' => $kind]];
    }
}
