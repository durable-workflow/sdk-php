<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\HandlerDefinition;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class WorkflowEntryBindingTest extends TestCase
{
    #[DataProvider('incompatibleEntries')]
    public function testInputBindingRefusesBeforeEnteringApplication(callable $handler, array $input): void
    {
        try {
            (new Replayer(new AvroPayloadCodec()))->replay($handler, [], $input, 'workers', ['workflow_type' => 'entry.test']);
            self::fail('Incompatible entry executed.');
        } catch (\Throwable $failure) {
            self::assertSame('workflow_entry_arguments_mismatch', $failure->reason ?? null);
            self::assertStringContainsString('entry.test', $failure->getMessage());
            self::assertStringNotContainsString('private-recorded-value', $failure->getMessage());
        }
    }

    public static function incompatibleEntries(): iterable
    {
        yield 'required input missing' => [static fn (WorkflowContext $ctx, int $amount, int $required): int => $required, [7]];
        yield 'strict scalar changed' => [static fn (WorkflowContext $ctx, string $amount): string => $amount, [7]];
        yield 'scalar to DTO' => [static fn (WorkflowContext $ctx, \stdClass $amount): object => $amount, [7]];
        yield 'nullable is not optional' => [static fn (WorkflowContext $ctx, ?int $amount): ?int => $amount, []];
        yield 'union mismatch' => [static fn (WorkflowContext $ctx, int|bool $amount): mixed => $amount, ['private-recorded-value']];
        yield 'intersection mismatch' => [static fn (WorkflowContext $ctx, \Countable&\Iterator $amount): object => $amount, [new \stdClass()]];
        yield 'variadic element mismatch' => [static fn (WorkflowContext $ctx, int ...$amounts): array => $amounts, [7, 'private-recorded-value']];
        yield 'discovered contract is not variadic wrapper' => [HandlerDefinition::shared(static fn (WorkflowContext $ctx, int $amount, int $required): int => $required), [7]];
    }

    #[DataProvider('compatibleEntries')]
    public function testCompatiblePhpInvocationKeepsRecordedTypes(callable $handler, array $input, mixed $expected): void
    {
        $codec = new AvroPayloadCodec();
        $result = (new Replayer($codec))->replay($handler, [], $input, 'workers');
        self::assertSame('complete_workflow', $result->commands[0]['type']);
        self::assertSame($expected, $codec->decodeEnvelope($result->commands[0]['result']));
    }

    public static function compatibleEntries(): iterable
    {
        yield 'optional addition' => [static fn (WorkflowContext $ctx, int $amount, ?int $extra = null): int => $amount, [7], 7];
        yield 'nullable value' => [static fn (WorkflowContext $ctx, ?int $amount): ?int => $amount, [null], null];
        yield 'int accepted for float' => [static fn (WorkflowContext $ctx, float $amount): float => $amount, [7], 7.0];
        yield 'union member' => [static fn (WorkflowContext $ctx, int|string $amount): mixed => $amount, [7], 7];
        yield 'variadic values' => [static fn (WorkflowContext $ctx, int ...$amounts): array => $amounts, [7, 8], [7, 8]];
        yield 'trailing argument removed' => [static fn (WorkflowContext $ctx): int => 7, [7], 7];
    }

    public function testWorkerReportsBindingAsTaskFailureWithOriginalClaim(): void
    {
        $requests = $this->executeWorker(static fn (WorkflowContext $ctx, int $amount, int $required): int => $required);
        $failures = array_values(array_filter($requests, static fn (array $row): bool => str_ends_with($row['uri'], '/task-original/fail')));
        self::assertCount(1, $failures);
        self::assertSame('owner-original', $failures[0]['body']['lease_owner']);
        self::assertSame(3, $failures[0]['body']['workflow_task_attempt']);
        self::assertSame('workflow_entry_arguments_mismatch', $failures[0]['body']['failure']['reason']);
        self::assertSame([], array_filter($requests, static fn (array $row): bool => str_ends_with($row['uri'], '/task-original/complete')));
    }

    public function testValidBoundBodyTypeErrorIsStillBusinessFailure(): void
    {
        $requests = $this->executeWorker(static function (WorkflowContext $ctx, int $amount): never {
            throw new \TypeError('application-body-failure');
        });
        $complete = array_values(array_filter($requests, static fn (array $row): bool => str_ends_with($row['uri'], '/task-original/complete')));
        self::assertCount(1, $complete);
        self::assertSame('fail_workflow', $complete[0]['body']['commands'][0]['type']);
        self::assertSame([], array_filter($requests, static fn (array $row): bool => str_ends_with($row['uri'], '/task-original/fail')));
    }

    private function executeWorker(callable $handler): array
    {
        $codec = new AvroPayloadCodec();
        $transport = new FakeTransport(handler: static function (string $method, string $uri) use ($codec): array {
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                return ['poll_status' => 'leased', 'task' => [
                    'task_id' => 'task-original', 'workflow_task_attempt' => 3, 'lease_owner' => 'owner-original',
                    'workflow_id' => 'workflow-original', 'run_id' => 'run-original', 'workflow_type' => 'entry.test',
                    'payload_codec' => 'avro', 'arguments' => $codec->envelope([7]), 'history_events' => [],
                ]];
            }
            if (str_ends_with($uri, '/activity-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
            }
            if (str_ends_with($uri, '/heartbeat')) {
                return ['task_id' => 'task-original', 'workflow_task_attempt' => 3, 'lease_owner' => 'owner-original', 'renewed' => true];
            }
            if (str_ends_with($uri, '/complete')) {
                return ['completed' => true];
            }
            if (str_ends_with($uri, '/fail')) {
                return ['failed' => true, 'recorded' => true];
            }
            self::fail('Unexpected worker request '.$method.' '.$uri);
        });
        $worker = new Worker(new Client('https://server.example', transport: $transport), 'workers', workerId: 'owner-original');
        $worker->registerWorkflow('entry.test', $handler);
        self::assertTrue($worker->tick(0));

        return $transport->requests;
    }
}
