<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class WorkerWorkflowCompletionLockPressureTest extends TestCase
{
    #[DataProvider('recordedStates')]
    public function testLockPressureKeepsTheSameCompletionAndExecutesOnce(bool $recorded): void
    {
        $now = 0.0;
        $calls = 0;
        $completions = [];
        $worker = self::worker($now, $calls, $completions, self::lockPressure($recorded), $recorded);

        self::assertTrue($worker->tick(0));
        self::assertSame(1, $calls);
        self::assertCount(2, $completions);
        self::assertSame($completions[0], $completions[1]);
        self::assertEqualsWithDelta(1.0, $now, 0.00001);
    }

    /** @return iterable<string, array{bool}> */
    public static function recordedStates(): iterable
    {
        yield 'unrecorded then accepted' => [false];
        yield 'recorded then authoritative completed refusal' => [true];
    }

    #[DataProvider('invalidResponses')]
    public function testUnqualifiedResponsesDoNotRetry(array $response): void
    {
        $now = 0.0;
        $calls = 0;
        $completions = [];
        $worker = self::worker($now, $calls, $completions, $response);

        try {
            $worker->tick(0);
            self::fail('An unqualified completion response was accepted.');
        } catch (ServerException) {
            self::assertSame(1, $calls);
            self::assertCount(1, $completions);
            self::assertSame(0.0, $now);
        }
    }

    /** @return iterable<string, array{array<string, mixed>}> */
    public static function invalidResponses(): iterable
    {
        foreach ([
            'reason' => 'other_backend_error', 'operation' => 'complete_activity_task',
            'task_id' => 'other-task', 'workflow_task_attempt' => 4,
            'lease_owner' => 'other-worker', 'outcome' => 'unknown',
            'retryable' => false, 'retry_after_seconds' => 0,
            'recorded' => 'false', 'backend' => ['driver' => 'sqlite', 'lock_pressure' => false],
        ] as $field => $value) {
            yield "wrong {$field}" => [array_replace(self::lockPressure(false), [$field => $value])];
            $response = self::lockPressure(false);
            unset($response[$field]);
            yield "missing {$field}" => [$response];
        }
        foreach (['1', 1.5, true, -1] as $index => $value) {
            yield "malformed delay {$index}" => [array_replace(self::lockPressure(false), ['retry_after_seconds' => $value])];
        }
        foreach ([6, PHP_INT_MAX] as $value) {
            yield "delay beyond retry ceiling {$value}" => [array_replace(self::lockPressure(false), ['retry_after_seconds' => $value])];
        }
    }

    public function testUnrecordedAndPublicationDeferralAreDistinctClassifications(): void
    {
        foreach ([false, true] as $recorded) {
            $error = new ServerException('Locked', 503, 'backend_lock_pressure', self::lockPressure($recorded));
            self::assertSame(!$recorded, $error->isWorkflowTaskCompletionDeferred('pressure-task', 'pressure-worker', 3));
            self::assertSame($recorded, $error->isWorkflowTaskCompletionPublicationDeferred('pressure-task', 'pressure-worker', 3));
            self::assertFalse($error->isWorkflowTaskCompletionDeferred('pressure-task', 'pressure-worker', 4));
            self::assertFalse($error->isWorkflowTaskCompletionPublicationDeferred('pressure-task', 'pressure-worker', 4));
        }
    }

    public function testAdvertisedSupportedDelayIsNotShortened(): void
    {
        $now = 0.0;
        $calls = 0;
        $completions = [];
        $response = array_replace(self::lockPressure(false), ['retry_after_seconds' => 4]);
        $worker = self::worker($now, $calls, $completions, $response);
        self::assertTrue($worker->tick(0));
        self::assertEqualsWithDelta(4.0, $now, 0.00001);
        self::assertSame(1, $calls);
    }

    public function testProlongedContentionSleepsWithoutReexecutingOrChangingCompletion(): void
    {
        $now = 0.0;
        $calls = 0;
        $completions = [];
        $worker = self::worker($now, $calls, $completions, self::lockPressure(false), refusals: 8);
        self::assertTrue($worker->tick(0));
        self::assertSame(1, $calls);
        self::assertCount(9, $completions);
        self::assertCount(1, array_unique($completions));
        self::assertEqualsWithDelta(18.8, $now, 0.00001);
    }

    public function testStaleOriginalLeaseStopsCompletionRetries(): void
    {
        $now = 0.0;
        $calls = 0;
        $completions = [];
        $worker = self::worker($now, $calls, $completions, self::lockPressure(false), staleAfterDeferral: true);
        self::assertTrue($worker->tick(0));
        self::assertSame(1, $calls);
        self::assertCount(2, $completions);
        self::assertSame($completions[0], $completions[1]);
    }

    public function testShutdownDuringDeferralStopsWithoutAnotherCompletionOrFatalFailure(): void
    {
        $now = 0.0;
        $calls = 0;
        $completions = [];
        $worker = null;
        $stop = static function () use (&$worker): void { $worker?->requestShutdown(); };
        $worker = self::worker($now, $calls, $completions, self::lockPressure(false), onSleep: $stop);
        self::assertTrue($worker->tick(0));
        self::assertSame(1, $calls);
        self::assertCount(1, $completions);
        self::assertEqualsWithDelta(0.1, $now, 0.00001);
        self::assertFalse($worker->tick(0));
    }

    /**
     * @param list<string> $completions
     * @param array<string, mixed> $response
     */
    private static function worker(
        float &$now,
        int &$calls,
        array &$completions,
        array $response,
        bool $alreadyRecorded = false,
        int $refusals = 1,
        bool $staleAfterDeferral = false,
        ?\Closure $onSleep = null,
    ): Worker {
        $transport = new FakeTransport(handler: static function (
            string $method, string $uri, array $headers, ?array $body,
        ) use (&$completions, $response, $alreadyRecorded, $refusals, $staleAfterDeferral): array {
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                return ['poll_status' => 'leased', 'task' => [
                    'task_id' => 'pressure-task', 'workflow_task_attempt' => 3,
                    'lease_owner' => 'pressure-worker', 'workflow_id' => 'pressure-workflow',
                    'run_id' => 'pressure-run', 'workflow_type' => 'pressure.workflow',
                    'payload_codec' => 'avro', 'history_events' => [],
                ]];
            }
            if (str_ends_with($uri, '/pressure-task/heartbeat')) {
                return ['task_id' => 'pressure-task', 'workflow_task_attempt' => 3,
                    'lease_owner' => 'pressure-worker', 'renewed' => true];
            }
            if (str_ends_with($uri, '/pressure-task/complete')) {
                $completions[] = json_encode($body, JSON_THROW_ON_ERROR);
                if (count($completions) <= $refusals) {
                    throw TransportException::fromResponse(503, $response, json_encode($response, JSON_THROW_ON_ERROR));
                }
                if ($staleAfterDeferral) {
                    $stale = ['reason' => 'lease_expired', 'task_id' => 'pressure-task',
                        'workflow_task_attempt' => 3, 'lease_owner' => 'pressure-worker', 'task_status' => 'leased'];
                    throw TransportException::fromResponse(409, $stale, json_encode($stale, JSON_THROW_ON_ERROR));
                }
                if ($alreadyRecorded) {
                    $completed = ['reason' => 'task_not_leased', 'task_id' => 'pressure-task',
                        'workflow_task_attempt' => 3, 'task_status' => 'completed'];
                    throw TransportException::fromResponse(409, $completed, json_encode($completed, JSON_THROW_ON_ERROR));
                }

                return ['completed' => true];
            }
            if (str_ends_with($uri, '/activity-tasks/poll')) {
                return ['poll_status' => 'stopped', 'task' => null, 'reason' => 'worker_stopped'];
            }
            self::fail("Unexpected request: {$method} {$uri}");
        });
        $worker = new Worker(new Client('https://server.example', transport: $transport), 'pressure-queue',
            workerId: 'pressure-worker', clock: static function () use (&$now): float { return $now; },
            sleeper: static function (int $microseconds) use (&$now, $onSleep): void {
                $now += $microseconds / 1000000;
                $onSleep?->__invoke();
            });
        $worker->registerWorkflow('pressure.workflow', static function (WorkflowContext $context) use (&$calls): string {
            ++$calls;

            return 'one-result';
        });

        return $worker;
    }

    /** @return array<string, mixed> */
    private static function lockPressure(bool $recorded): array
    {
        return ['reason' => 'backend_lock_pressure', 'operation' => 'complete_workflow_task',
            'task_id' => 'pressure-task', 'workflow_task_attempt' => 3, 'lease_owner' => 'pressure-worker',
            'outcome' => 'deferred', 'recorded' => $recorded, 'retryable' => true,
            'retry_after_seconds' => 1, 'backend' => ['driver' => 'sqlite', 'lock_pressure' => true]];
    }
}
