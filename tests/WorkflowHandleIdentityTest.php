<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Testing\WorkflowClientFake;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\WorkflowClientInterface;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class WorkflowHandleIdentityTest extends TestCase
{
    /** @return iterable<string, array{\Closure(FakeTransport): WorkflowClientInterface}> */
    public static function clients(): iterable
    {
        yield 'network client' => [static fn (FakeTransport $transport): WorkflowClientInterface
            => new Client('https://server.example', transport: $transport)];
        yield 'test fake' => [static fn (FakeTransport $transport): WorkflowClientInterface => new WorkflowClientFake()];
    }

    /** @param \Closure(FakeTransport): WorkflowClientInterface $client */
    #[DataProvider('clients')]
    public function testAnEmptyWorkflowIdIsRejectedWhenTheHandleIsCreated(\Closure $client): void
    {
        $transport = new FakeTransport();

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Workflow ID cannot be empty.');
        try {
            $client($transport)->workflowHandle('');
        } finally {
            self::assertSame([], $transport->requests);
        }
    }

    /** @param \Closure(FakeTransport): WorkflowClientInterface $client */
    #[DataProvider('clients')]
    public function testAnEmptySelectedRunIdIsRejectedWhenTheHandleIsCreated(\Closure $client): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Selected run ID cannot be empty. Pass null to follow the current run.');

        $client(new FakeTransport())->workflowHandle('order-1', '');
    }

    public function testAnEmptyIdentifierNeverReachesTheServerAsACollapsedRoute(): void
    {
        $transport = new FakeTransport();
        $client = new Client('https://server.example', transport: $transport);

        foreach ([
            static fn () => $client->describeWorkflow(''),
            static fn () => $client->signalWorkflow('', 'approve'),
            static fn () => $client->describeWorkflow('order-1', ''),
            static fn () => $client->describeSchedule(''),
        ] as $call) {
            try {
                $call();
                self::fail('An empty identifier reached the transport.');
            } catch (InvalidArgumentException $exception) {
                self::assertSame('Durable Workflow request path identifiers cannot be empty.', $exception->getMessage());
            }
        }
        self::assertSame([], $transport->requests);
    }
}
