<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\PaginationException;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Model\WorkflowExecution;
use DurableWorkflow\Tests\Support\FakeTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use WeakReference;

final class ClientPaginationTest extends TestCase
{
    public function testWorkflowIterationIsLazyTypedAndPreservesEveryPageAndFilter(): void
    {
        $transport = new FakeTransport([
            ['workflows' => [self::workflow('first'), self::workflow('second')], 'next_page_token' => 'opaque/+ ='],
            ['workflows' => [], 'next_page_token' => 'last'],
            ['workflows' => [self::workflow('third')], 'next_page_token' => null, 'workflow_count' => 999],
        ]);
        $original = new Client('https://server.example', transport: $transport, namespace: 'default', token: 'synthetic');
        $client = $original->withNamespace('orders');
        $iterator = $client->iterateWorkflows(workflowType: 'orders.process', status: 'running', query: 'CustomerId = "42"', pageSize: 2);
        self::assertCount(0, $transport->requests);
        self::assertInstanceOf(WorkflowExecution::class, $iterator->current());
        self::assertCount(1, $transport->requests);
        $all = iterator_to_array($iterator);
        self::assertSame([0, 1, 2], array_keys($all));
        self::assertSame(['first', 'second', 'third'], array_column($all, 'workflowId'));
        self::assertCount(3, $transport->requests);
        self::assertSame('default', $original->namespace);
        foreach ($transport->requests as $index => $request) {
            parse_str((string) parse_url($request['uri'], PHP_URL_QUERY), $query);
            self::assertSame('orders.process', $query['workflow_type']);
            self::assertSame('running', $query['status']);
            self::assertSame('CustomerId = "42"', $query['query']);
            self::assertSame('2', $query['page_size']);
            self::assertSame([null, 'opaque/+ =', 'last'][$index], $query['next_page_token'] ?? null);
            self::assertSame('orders', $request['headers']['X-Namespace']);
            self::assertSame('2', $request['headers']['X-Durable-Workflow-Control-Plane-Version']);
            self::assertSame('Bearer synthetic', $request['headers']['Authorization']);
        }
    }

    public function testHistoryIterationPreservesExactRunRawEventsAndOpaqueCursor(): void
    {
        $first = ['sequence' => 1, 'payload' => ['integer' => PHP_INT_MAX, 'unicode' => 'λ', 'nil' => null]];
        $second = ['sequence' => 2, 'event_type' => 'WorkflowCompleted'];
        $transport = new FakeTransport([
            ['events' => [$first], 'next_page_token' => 'opaque/+ ='],
            ['events' => [$second], 'next_page_token' => ''],
        ]);
        $client = new Client('https://server.example', transport: $transport, namespace: 'orders');
        $iterator = $client->iterateWorkflowHistory('workflow/a', 'run/b', 1, 'resume/+ =');
        self::assertCount(0, $transport->requests);
        self::assertSame([$first, $second], iterator_to_array($iterator));
        self::assertCount(2, $transport->requests);
        foreach ($transport->requests as $index => $request) {
            self::assertSame('/api/workflows/workflow%2Fa/runs/run%2Fb/history', parse_url($request['uri'], PHP_URL_PATH));
            parse_str((string) parse_url($request['uri'], PHP_URL_QUERY), $query);
            self::assertSame(['resume/+ =', 'opaque/+ ='][$index], $query['next_page_token']);
            self::assertSame('1', $query['page_size']);
            self::assertSame('orders', $request['headers']['X-Namespace']);
        }
    }

    /** @return iterable<string, array{bool}> */
    public static function surfaces(): iterable
    {
        yield 'workflow list' => [false];
        yield 'run history' => [true];
    }

    #[DataProvider('surfaces')]
    public function testAnEmptyTerminalPageStopsWithoutAnotherRequest(bool $history): void
    {
        $transport = new FakeTransport([[($history ? 'events' : 'workflows') => [], 'next_page_token' => null]]);
        $iterator = self::iterator(new Client('https://server.example', transport: $transport), $history);
        self::assertSame([], iterator_to_array($iterator));
        self::assertCount(1, $transport->requests);
    }

    #[DataProvider('surfaces')]
    public function testStoppingEarlyDoesNotPrefetch(bool $history): void
    {
        $transport = new FakeTransport([self::page($history, 'later')]);
        $iterator = self::iterator(new Client('https://server.example', transport: $transport), $history);
        foreach ($iterator as $_) {
            break;
        }
        unset($iterator);
        self::assertCount(1, $transport->requests);
    }

    #[DataProvider('surfaces')]
    public function testLaterPageErrorsPropagateOnceAndCloseTheGenerator(bool $history): void
    {
        $error = new ServerException('Denied', 403, 'not_authorized');
        $transport = new FakeTransport([self::page($history, 'later'), $error]);
        $iterator = self::iterator(new Client('https://server.example', transport: $transport), $history);
        $iterator->current();
        try {
            $iterator->next();
            self::fail('The later-page error was swallowed.');
        } catch (ServerException $caught) {
            self::assertSame($error, $caught);
        }
        self::assertFalse($iterator->valid());
        self::assertCount(2, $transport->requests);
    }

    /** @return iterable<string, array{array<string, mixed>, string}> */
    public static function invalidHistoryPages(): iterable
    {
        yield 'missing token' => [['events' => []], 'missing_continuation'];
        yield 'integer token' => [['events' => [], 'next_page_token' => 3], 'invalid_continuation'];
        yield 'object token' => [['events' => [], 'next_page_token' => ['private' => 'value']], 'invalid_continuation'];
        yield 'missing items' => [['next_page_token' => null], 'invalid_page'];
        yield 'non-list items' => [['events' => ['wrong' => []], 'next_page_token' => null], 'invalid_page'];
        yield 'scalar items' => [['events' => true, 'next_page_token' => null], 'invalid_page'];
        yield 'scalar row' => [['events' => ['private-value'], 'next_page_token' => null], 'invalid_item'];
    }

    #[DataProvider('invalidHistoryPages')]
    public function testMalformedHistoryCannotSilentlyTruncate(array $page, string $reason): void
    {
        $client = new Client('https://server.example', transport: new FakeTransport([$page]));
        try {
            iterator_to_array($client->iterateWorkflowHistory('workflow', 'run'));
            self::fail('The malformed page was accepted.');
        } catch (PaginationException $error) {
            self::assertSame($reason, $error->reason);
            self::assertStringNotContainsString('private', $error->getMessage());
        }
    }

    public function testMalformedWorkflowRowsAreNotSilentlyDroppedByTheIterator(): void
    {
        $client = new Client('https://server.example', transport: new FakeTransport([
            ['workflows' => [self::workflow('first'), 'private-value'], 'next_page_token' => null],
        ]));
        $this->expectException(PaginationException::class);
        iterator_to_array($client->iterateWorkflows());
    }

    #[DataProvider('surfaces')]
    public function testRepeatedTokenFailsBeforeARepeatedPageIsYielded(bool $history): void
    {
        $transport = new FakeTransport([self::page($history, 'private-token'), self::page($history, 'private-token')]);
        $iterator = self::iterator(new Client('https://server.example', transport: $transport), $history);
        $iterator->current();
        try {
            $iterator->next();
            self::fail('The repeated token was accepted.');
        } catch (PaginationException $error) {
            self::assertSame('repeated_page_token', $error->reason);
            self::assertStringNotContainsString('private-token', $error->getMessage());
        }
        self::assertFalse($iterator->valid());
        self::assertCount(2, $transport->requests);
    }

    #[DataProvider('surfaces')]
    public function testLongerTokenCycleTerminatesWithoutRetainingAllTokens(bool $history): void
    {
        $calls = 0;
        $transport = new FakeTransport(handler: static function (string $method, string $uri) use ($history, &$calls): array {
            self::assertLessThan(12, ++$calls, 'Token cycle was not detected.');
            parse_str((string) parse_url($uri, PHP_URL_QUERY), $query);
            $next = match ($query['next_page_token'] ?? null) {
                null => 'a', 'a' => 'b', 'b' => 'c', 'c' => 'a',
            };

            return [($history ? 'events' : 'workflows') => [], 'next_page_token' => $next];
        });
        $this->expectException(PaginationException::class);
        iterator_to_array(self::iterator(new Client('https://server.example', transport: $transport), $history));
    }

    public function testFinishedPagesAreReleasedBeforeFetchingTheNextPage(): void
    {
        $first = null;
        $calls = 0;
        $transport = new FakeTransport(handler: static function () use (&$calls, &$first): array {
            if (++$calls === 1) {
                return ['workflows' => [self::workflow('first'), self::workflow('second')], 'next_page_token' => 'later'];
            }
            self::assertInstanceOf(WeakReference::class, $first);
            self::assertNull($first->get(), 'The preceding page is still retained.');

            return ['workflows' => [], 'next_page_token' => null];
        });
        $iterator = (new Client('https://server.example', transport: $transport))->iterateWorkflows();
        $first = WeakReference::create($iterator->current());
        $iterator->next();
        $iterator->next();
        self::assertFalse($iterator->valid());
        self::assertSame(2, $calls);
    }

    /** @return array<string, mixed> */
    private static function workflow(string $id): array
    {
        return ['workflow_id' => $id, 'run_id' => 'run-'.$id, 'workflow_type' => 'orders.process', 'status' => 'running'];
    }

    /** @return array<string, mixed> */
    private static function page(bool $history, ?string $next): array
    {
        return [($history ? 'events' : 'workflows') => [$history ? ['sequence' => 1] : self::workflow('first')], 'next_page_token' => $next];
    }

    private static function iterator(Client $client, bool $history): \Generator
    {
        return $history ? $client->iterateWorkflowHistory('workflow', 'run') : $client->iterateWorkflows();
    }
}
