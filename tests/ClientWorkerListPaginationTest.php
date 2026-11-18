<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\PaginationException;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Tests\Support\FakeTransport;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ClientWorkerListPaginationTest extends TestCase
{
    public function testOnePageCarriesPageSizeAndTokenOnlyWhenGiven(): void
    {
        $transport = new FakeTransport([
            ['workers' => [], 'next_page_token' => null],
            ['workers' => [], 'next_page_token' => null],
        ]);
        $client = new Client('https://server.example', transport: $transport, namespace: 'orders');

        $client->listWorkers('orders', 'stale');
        $client->listWorkers('orders', 'stale', 25, 'opaque/+ =');

        self::assertSame('/api/workers?task_queue=orders&status=stale', self::pathAndQuery($transport->requests[0]['uri']));
        parse_str((string) parse_url($transport->requests[1]['uri'], PHP_URL_QUERY), $query);
        self::assertSame(['task_queue' => 'orders', 'status' => 'stale', 'page_size' => '25', 'next_page_token' => 'opaque/+ ='], $query);
    }

    public function testAServerThatDoesNotPageIsReadOnceAndReturnedAsIs(): void
    {
        $response = [
            'namespace' => 'orders',
            'workers' => array_map(self::worker(...), range(1, 150)),
        ];
        $transport = new FakeTransport([$response]);
        $client = new Client('https://server.example', transport: $transport);

        self::assertSame($response, $client->listAllWorkers());
        self::assertCount(1, $transport->requests);
        self::assertSame('/api/workers', self::pathAndQuery($transport->requests[0]['uri']));
    }

    public function testEveryPageIsFollowedWithTheSameFilters(): void
    {
        $transport = new FakeTransport([
            ['workers' => [self::worker(1), self::worker(2)], 'next_page_token' => 'opaque/+ =', 'registration_retention_seconds' => 86400],
            ['workers' => [], 'next_page_token' => 'later', 'registration_retention_seconds' => 86400],
            ['workers' => [self::worker(3)], 'next_page_token' => null, 'registration_retention_seconds' => 86400],
        ]);
        $client = (new Client('https://server.example', transport: $transport, token: 'synthetic'))->withNamespace('orders');

        $response = $client->listAllWorkers('orders', 'active', 2);

        self::assertSame(['worker-1', 'worker-2', 'worker-3'], array_column($response['workers'], 'worker_id'));
        self::assertSame([0, 1, 2], array_keys($response['workers']));
        self::assertNull($response['next_page_token']);
        self::assertSame(86400, $response['registration_retention_seconds']);
        self::assertCount(3, $transport->requests);
        foreach ($transport->requests as $index => $request) {
            self::assertSame('/api/workers', parse_url($request['uri'], PHP_URL_PATH));
            parse_str((string) parse_url($request['uri'], PHP_URL_QUERY), $query);
            self::assertSame('orders', $query['task_queue']);
            self::assertSame('active', $query['status']);
            self::assertSame('2', $query['page_size']);
            self::assertSame([null, 'opaque/+ =', 'later'][$index], $query['next_page_token'] ?? null);
            self::assertSame('orders', $request['headers']['X-Namespace']);
            self::assertSame('Bearer synthetic', $request['headers']['Authorization']);
        }
    }

    public function testThePageBoundStopsAndReturnsTheUnreadToken(): void
    {
        $calls = 0;
        $transport = new FakeTransport(handler: static function () use (&$calls): array {
            ++$calls;

            return ['workers' => [self::worker($calls)], 'next_page_token' => 'token-'.$calls];
        });
        $client = new Client('https://server.example', transport: $transport);

        $response = $client->listAllWorkers(maxPages: 3);

        self::assertSame(3, $calls);
        self::assertSame(['worker-1', 'worker-2', 'worker-3'], array_column($response['workers'], 'worker_id'));
        self::assertSame('token-3', $response['next_page_token']);
    }

    public function testTheDefaultPageBoundIsFifty(): void
    {
        $calls = 0;
        $transport = new FakeTransport(handler: static function () use (&$calls): array {
            ++$calls;

            return ['workers' => [], 'next_page_token' => 'token-'.$calls];
        });

        $response = (new Client('https://server.example', transport: $transport))->listAllWorkers();

        self::assertSame(Client::LIST_ALL_WORKERS_MAX_PAGES, $calls);
        self::assertSame(50, Client::LIST_ALL_WORKERS_MAX_PAGES);
        self::assertSame('token-50', $response['next_page_token']);
    }

    public function testAPageBoundBelowOneIsRefusedBeforeAnyRequest(): void
    {
        $transport = new FakeTransport();
        $this->expectException(InvalidArgumentException::class);
        try {
            (new Client('https://server.example', transport: $transport))->listAllWorkers(maxPages: 0);
        } finally {
            self::assertCount(0, $transport->requests);
        }
    }

    public function testARepeatedTokenFails(): void
    {
        $transport = new FakeTransport([
            ['workers' => [self::worker(1)], 'next_page_token' => 'a'],
            ['workers' => [self::worker(2)], 'next_page_token' => 'b'],
            ['workers' => [self::worker(3)], 'next_page_token' => 'a'],
        ]);
        try {
            (new Client('https://server.example', transport: $transport))->listAllWorkers();
            self::fail('The repeated token was accepted.');
        } catch (PaginationException $error) {
            self::assertSame('repeated_page_token', $error->reason);
        }
        self::assertCount(3, $transport->requests);
    }

    /** @return iterable<string, array{array<string, mixed>, string}> */
    public static function malformedPages(): iterable
    {
        yield 'missing workers' => [['next_page_token' => null], 'invalid_page'];
        yield 'keyed workers' => [['workers' => ['a' => self::worker(1)], 'next_page_token' => null], 'invalid_page'];
        yield 'scalar row' => [['workers' => ['private-value'], 'next_page_token' => null], 'invalid_item'];
        yield 'integer token' => [['workers' => [], 'next_page_token' => 3], 'invalid_continuation'];
    }

    /** @param array<string, mixed> $page */
    #[DataProvider('malformedPages')]
    public function testAMalformedLaterPageCannotSilentlyTruncate(array $page, string $reason): void
    {
        $transport = new FakeTransport([['workers' => [self::worker(1)], 'next_page_token' => 'later'], $page]);
        try {
            (new Client('https://server.example', transport: $transport))->listAllWorkers();
            self::fail('The malformed page was accepted.');
        } catch (PaginationException $error) {
            self::assertSame($reason, $error->reason);
            self::assertStringNotContainsString('private', $error->getMessage());
        }
    }

    public function testALaterPageErrorPropagates(): void
    {
        $error = new ServerException('Invalid page token', 400, 'invalid_page_token');
        $transport = new FakeTransport([['workers' => [self::worker(1)], 'next_page_token' => 'later'], $error]);
        try {
            (new Client('https://server.example', transport: $transport))->listAllWorkers();
            self::fail('The later-page error was swallowed.');
        } catch (ServerException $caught) {
            self::assertSame($error, $caught);
        }
        self::assertCount(2, $transport->requests);
    }

    /** @return array<string, mixed> */
    private static function worker(int $index): array
    {
        return ['worker_id' => 'worker-'.$index, 'namespace' => 'orders', 'task_queue' => 'orders', 'status' => 'active'];
    }

    private static function pathAndQuery(string $uri): string
    {
        $query = parse_url($uri, PHP_URL_QUERY);

        return parse_url($uri, PHP_URL_PATH).(is_string($query) ? '?'.$query : '');
    }
}
