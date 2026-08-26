<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class WorkerTaskPayloadCodecTest extends TestCase
{
    /** @param array{present: bool, value?: mixed} $codecCase */
    #[DataProvider('unsupportedTaskCodecs')]
    public function testUnsupportedRootTaskCodecsFailBeforePayloadDecodeOrUserHandlers(array $codecCase): void
    {
        foreach (['workflow', 'update', 'activity', 'query'] as $path) {
            $handlerCalls = 0;
            [$worker, $transport] = $this->workerForTask(
                $path,
                $this->task($path, $codecCase, ['codec' => 'avro', 'blob' => 'decode-must-not-run']),
                $handlerCalls,
            );

            self::assertTrue($worker->tick(0), $path);
            self::assertSame(0, $handlerCalls, $path);

            $failureRequests = array_values(array_filter(
                $transport->requests,
                static fn (array $request): bool => str_ends_with($request['uri'], '/fail'),
            ));
            self::assertCount(1, $failureRequests, $path);
            $failureBody = $failureRequests[0]['body'];
            $failure = json_encode($failureBody, JSON_THROW_ON_ERROR);
            self::assertStringContainsString('unsupported_payload_codec', $failure, $path);
            self::assertStringContainsString('payload_codec=\"avro\"', $failure, $path);
            self::assertStringNotContainsString('invalid_payload_framing', $failure, $path);
            if ($path === 'activity') {
                self::assertTrue($failureBody['failure']['non_retryable'] ?? null, $path);
            }
            self::assertSame([], array_values(array_filter(
                $transport->requests,
                static fn (array $request): bool => str_ends_with($request['uri'], '/complete'),
            )), $path);
        }
    }

    public function testApplicationActivityFailuresRemainRetryable(): void
    {
        $handlerCalls = 0;
        [$worker, $transport] = $this->workerForTask(
            'activity',
            $this->task(
                'activity',
                ['present' => true, 'value' => 'avro'],
                (new AvroPayloadCodec())->envelope(['input']),
            ),
            $handlerCalls,
            static function (ActivityContext $context, mixed $input = null) use (&$handlerCalls): string {
                ++$handlerCalls;

                throw new \RuntimeException('application activity failed');
            },
        );

        self::assertTrue($worker->tick(0));
        self::assertSame(1, $handlerCalls);

        $failureRequests = array_values(array_filter(
            $transport->requests,
            static fn (array $request): bool => str_ends_with($request['uri'], '/fail'),
        ));
        self::assertCount(1, $failureRequests);
        self::assertFalse($failureRequests[0]['body']['failure']['non_retryable'] ?? null);
        self::assertSame('application activity failed', $failureRequests[0]['body']['failure']['message'] ?? null);
    }

    #[DataProvider('workerPaths')]
    public function testValidAvroTasksDecodeAndReachEveryUserHandler(string $path): void
    {
        $handlerCalls = 0;
        $arguments = (new AvroPayloadCodec())->envelope([$path === 'update' ? 41 : 'input']);
        [$worker, $transport] = $this->workerForTask(
            $path,
            $this->task($path, ['present' => true, 'value' => 'avro'], $arguments),
            $handlerCalls,
        );

        self::assertTrue($worker->tick(0));
        self::assertSame(1, $handlerCalls);
        self::assertCount(1, array_filter(
            $transport->requests,
            static fn (array $request): bool => str_ends_with($request['uri'], '/complete'),
        ));
        self::assertSame([], array_values(array_filter(
            $transport->requests,
            static fn (array $request): bool => str_ends_with($request['uri'], '/fail'),
        )));
    }

    /** @return iterable<string, array{array{present: bool, value?: mixed}}> */
    public static function unsupportedTaskCodecs(): iterable
    {
        yield 'missing' => [['present' => false]];
        yield 'empty' => [['present' => true, 'value' => '']];
        yield 'json' => [['present' => true, 'value' => 'json']];
        yield 'unknown' => [['present' => true, 'value' => 'custom']];
        yield 'wrong case' => [['present' => true, 'value' => 'Avro']];
        yield 'null' => [['present' => true, 'value' => null]];
        yield 'non-string' => [['present' => true, 'value' => ['avro']]];
    }

    /** @return iterable<string, array{string}> */
    public static function workerPaths(): iterable
    {
        yield 'workflow' => ['workflow'];
        yield 'update' => ['update'];
        yield 'activity' => ['activity'];
        yield 'query' => ['query'];
    }

    /**
     * @param array{present: bool, value?: mixed} $codecCase
     * @param array<string, mixed> $arguments
     * @return array<string, mixed>
     */
    private function task(string $path, array $codecCase, array $arguments): array
    {
        $task = match ($path) {
            'workflow' => [
                'task_id' => 'workflow-codec-task',
                'workflow_task_attempt' => 1,
                'lease_owner' => 'codec-worker',
                'workflow_id' => 'workflow-codec',
                'run_id' => 'workflow-codec-run',
                'workflow_type' => 'codec.workflow',
                'arguments' => $arguments,
                'history_events' => [],
            ],
            'update' => [
                'task_id' => 'update-codec-task',
                'workflow_task_attempt' => 1,
                'lease_owner' => 'codec-worker',
                'workflow_id' => 'update-codec',
                'run_id' => 'update-codec-run',
                'workflow_type' => 'codec.workflow',
                'workflow_update_id' => 'update-codec-id',
                'history_events' => [[
                    'event_type' => 'UpdateAccepted',
                    'payload' => [
                        'update_id' => 'update-codec-id',
                        'update_name' => 'increment',
                        'arguments' => $arguments,
                    ],
                ]],
            ],
            'activity' => [
                'task_id' => 'activity-codec-task',
                'activity_attempt_id' => 'activity-codec-attempt',
                'lease_owner' => 'codec-worker',
                'activity_type' => 'codec.activity',
                'arguments' => $arguments,
            ],
            'query' => [
                'query_task_id' => 'query-codec-task',
                'query_task_attempt' => 1,
                'lease_owner' => 'codec-worker',
                'workflow_id' => 'query-codec',
                'run_id' => 'query-codec-run',
                'workflow_type' => 'codec.workflow',
                'query_name' => 'status',
                'query_arguments' => $arguments,
                'history_events' => [],
            ],
            default => throw new \InvalidArgumentException("Unknown path {$path}."),
        };
        if ($codecCase['present']) {
            $task['payload_codec'] = $codecCase['value'] ?? null;
        }

        return $task;
    }

    /**
     * @param array<string, mixed> $task
     * @return array{Worker, FakeTransport}
     */
    private function workerForTask(
        string $path,
        array $task,
        int &$handlerCalls,
        ?\Closure $activityHandler = null,
    ): array {
        $delivered = false;
        $transport = new FakeTransport(handler: static function (
            string $method,
            string $uri,
            array $headers,
            ?array $body,
        ) use ($path, $task, &$delivered): array {
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                if (!$delivered && in_array($path, ['workflow', 'update'], true)) {
                    $delivered = true;

                    return ['poll_status' => 'leased', 'task' => $task];
                }

                return ['poll_status' => 'empty', 'task' => null];
            }
            if (str_ends_with($uri, '/activity-tasks/poll')) {
                if (!$delivered && $path === 'activity') {
                    $delivered = true;

                    return ['poll_status' => 'leased', 'task' => $task];
                }

                return ['poll_status' => 'empty', 'task' => null];
            }
            if (str_ends_with($uri, '/query-tasks/poll')) {
                if (!$delivered && $path === 'query') {
                    $delivered = true;

                    return ['poll_status' => 'leased', 'task' => $task];
                }

                return ['poll_status' => 'empty', 'task' => null];
            }
            if (str_ends_with($uri, '/heartbeat')) {
                return [
                    'task_id' => $task['task_id'],
                    'workflow_task_attempt' => $task['workflow_task_attempt'],
                    'lease_owner' => $task['lease_owner'],
                    'renewed' => true,
                    'reason' => null,
                ];
            }
            if (str_ends_with($uri, '/complete') || str_ends_with($uri, '/fail')) {
                return ['completed' => true];
            }

            self::fail("Unexpected worker request: {$method} {$uri}");
        });
        $worker = new Worker(
            new Client('https://server.example', transport: $transport),
            'codec-queue',
            workerId: 'codec-worker',
        );
        $worker
            ->registerWorkflow(
                'codec.workflow',
                static function (WorkflowContext $context, mixed $input = null) use (&$handlerCalls): string {
                    ++$handlerCalls;

                    return 'workflow-complete';
                },
            )
            ->registerUpdate(
                'codec.workflow',
                'increment',
                static function (QueryContext $context, int $value = 0) use (&$handlerCalls): int {
                    ++$handlerCalls;

                    return $value + 1;
                },
            )
            ->registerActivity(
                'codec.activity',
                $activityHandler ?? static function (
                    ActivityContext $context,
                    mixed $input = null,
                ) use (&$handlerCalls): string {
                    ++$handlerCalls;

                    return 'activity-complete';
                },
            )
            ->registerQuery(
                'codec.workflow',
                'status',
                static function (QueryContext $context, mixed $input = null) use (&$handlerCalls): string {
                    ++$handlerCalls;

                    return 'query-complete';
                },
            );

        return [$worker, $transport];
    }
}
