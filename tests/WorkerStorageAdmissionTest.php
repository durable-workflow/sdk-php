<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ActivityCancelled;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class WorkerStorageAdmissionTest extends TestCase
{
    #[DataProvider('pollCases')]
    public function testPressurePreservesEvenAPreviouslyUncertainPoll(string $kind, string $reason, bool $atEntry): void
    {
        $now = 0.0;
        $polls = [];
        $transport = new FakeTransport(handler: static function (
            string $method, string $uri, array $headers, ?array $body,
        ) use ($kind, $reason, $atEntry, &$polls): array {
            if (str_ends_with($uri, "/{$kind}-tasks/poll")) {
                $polls[] = $body;
                if (count($polls) === 1) {
                    throw new TransportException('Response lost after a possible claim.', transientConnectionFailure: true);
                }
                if (count($polls) <= 3) {
                    $response = self::pressure($body['poll_request_id'], $reason);
                    if (!$atEntry) {
                        unset($response['request_admitted']);
                    }
                    throw self::refused($response);
                }
            }

            return self::emptyPoll();
        });

        self::assertFalse(self::worker($transport, $now)->tick(0));
        self::assertCount(4, $polls);
        foreach ($polls as $poll) {
            self::assertSame($polls[0], $poll);
        }
        self::assertEqualsWithDelta(2.0, $now, 0.00001);
    }

    public static function pollCases(): iterable
    {
        foreach (['workflow', 'activity', 'query'] as $kind) {
            foreach (['storage_pressure', 'storage_admission_unavailable'] as $reason) {
                foreach ([true, false] as $atEntry) {
                    yield "{$kind} {$reason} ".($atEntry ? 'entry' : 'long poll') => [$kind, $reason, $atEntry];
                }
            }
        }
    }

    public function testRegistrationAndHeartbeatRecoverWithoutRecursiveWaiting(): void
    {
        $now = 0.0;
        $registrations = [];
        $heartbeats = [];
        $transport = new FakeTransport(handler: static function (
            string $method, string $uri, array $headers, ?array $body,
        ) use (&$now, &$registrations, &$heartbeats): array {
            if (str_ends_with($uri, '/register')) {
                $registrations[] = $body;
                if (count($registrations) === 1) {
                    throw self::refused(self::pressure());
                }

                return ['registered' => true, 'heartbeat_interval_seconds' => 1];
            }
            if (str_ends_with($uri, '/heartbeat')) {
                $heartbeats[] = $now;
                if (count($heartbeats) === 1) {
                    throw self::refused(self::pressure(reason: 'storage_admission_unavailable'));
                }

                return ['acknowledged' => true];
            }
            if (count($heartbeats) < 2) {
                throw self::refused(self::pressure($body['poll_request_id']));
            }

            return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
        });

        self::worker($transport, $now)->run(0);

        self::assertCount(2, $registrations);
        self::assertSame($registrations[0], $registrations[1]);
        self::assertCount(2, $heartbeats);
        self::assertEqualsWithDelta(2.0, $heartbeats[0], 0.00001);
        self::assertEqualsWithDelta(3.0, $heartbeats[1], 0.00001);
    }

    #[DataProvider('acknowledgements')]
    public function testAcknowledgementRecoveryDoesNotReexecuteHandlers(string $kind, bool $fails): void
    {
        $now = 0.0;
        $handlerCalls = 0;
        $acks = [];
        $transport = self::taskTransport($kind, static function (array $body) use (&$acks): array {
            $acks[] = $body;
            if (count($acks) <= 2) {
                throw self::refused(self::pressure(reason: 'storage_admission_unavailable'));
            }

            return ['recorded' => true];
        }, $fails ? 'fail' : 'complete');
        $worker = self::worker($transport, $now);
        self::registerTask($worker, $kind, $handlerCalls, $fails);

        self::assertTrue($worker->tick(0));
        self::assertSame(1, $handlerCalls);
        self::assertCount(3, $acks);
        self::assertSame($acks[0], $acks[1]);
        self::assertSame($acks[0], $acks[2]);
        self::assertEqualsWithDelta(2.0, $now, 0.00001);
        self::assertSame('storage-worker', $acks[0]['lease_owner']);
        self::assertSame($kind === 'activity' ? 'activity-attempt-7' : 7,
            $acks[0][$kind === 'activity' ? 'activity_attempt_id' : "{$kind}_task_attempt"]);
    }

    public static function acknowledgements(): iterable
    {
        foreach (['workflow', 'activity', 'query'] as $kind) {
            yield "{$kind} success" => [$kind, false];
            yield "{$kind} failure" => [$kind, true];
        }
    }

    public function testCompletedLocalActivityIsNotRepeatedWhenWorkflowAcknowledgementIsRefused(): void
    {
        $now = 0.0;
        $calls = 0;
        $acks = [];
        $transport = self::taskTransport('workflow', static function (array $body) use (&$acks): array {
            $acks[] = $body;
            if (count($acks) === 1) {
                throw self::refused(self::pressure());
            }

            return ['completed' => true];
        });
        $worker = self::worker($transport, $now);
        $worker->registerActivity('local.receipt', static function (ActivityContext $context) use (&$calls): array {
            ++$calls;

            return ['receipt' => 'already-created'];
        });
        $worker->registerWorkflow('storage.workflow', static fn (WorkflowContext $context): mixed =>
            $context->localActivity('local.receipt'));

        self::assertTrue($worker->tick(0));
        self::assertSame(1, $calls);
        self::assertCount(2, $acks);
        self::assertSame($acks[0], $acks[1]);
        self::assertSame('record_local_activity', $acks[0]['commands'][0]['type']);
    }

    #[DataProvider('heartbeatCases')]
    public function testTaskHeartbeatRetriesTheSameLeaseAndPreservesCancellation(string $kind, bool $cancelled): void
    {
        $now = 0.0;
        $calls = 0;
        $heartbeats = [];
        $acks = [];
        $transport = self::taskTransport($kind, static function (array $body) use (&$acks): array {
            $acks[] = $body;

            return ['recorded' => true];
        }, $cancelled ? 'fail' : 'complete', static function (array $body) use ($kind, $cancelled, &$heartbeats): array {
            $heartbeats[] = $body;
            if (count($heartbeats) === 1) {
                throw self::refused(self::pressure());
            }

            return $kind === 'workflow'
                ? self::leaseRenewed($body)
                : ['cancel_requested' => $cancelled, 'can_continue' => !$cancelled];
        });
        $worker = self::worker($transport, $now);
        if ($kind === 'activity') {
            $worker->registerActivity('storage.activity', static function (ActivityContext $context) use (&$calls): string {
                ++$calls;
                $context->heartbeat(['step' => 2]);

                return 'done';
            });
        } else {
            self::registerTask($worker, $kind, $calls);
        }

        self::assertTrue($worker->tick(0));
        self::assertSame(1, $calls);
        self::assertCount(2, $heartbeats);
        self::assertSame($heartbeats[0], $heartbeats[1]);
        self::assertCount(1, $acks);
        if ($cancelled) {
            self::assertSame(ActivityCancelled::class, $acks[0]['failure']['type']);
        }
    }

    public static function heartbeatCases(): array
    {
        return [['workflow', false], ['activity', false], ['activity', true]];
    }

    #[DataProvider('invalidPollContracts')]
    public function testInvalidStoragePollContractIsNotRetried(array $overrides): void
    {
        $now = 0.0;
        $transport = new FakeTransport(handler: static function (
            string $method, string $uri, array $headers, ?array $body,
        ) use ($overrides): array {
            throw self::refused(array_replace(self::pressure($body['poll_request_id']), $overrides));
        });

        try {
            self::worker($transport, $now)->tick(0);
            self::fail('Invalid storage refusal must not enter the generic poll retry path.');
        } catch (ServerException) {
            self::assertCount(1, $transport->requests);
            self::assertSame(0.0, $now);
        }
    }

    public static function invalidPollContracts(): array
    {
        return [
            'wrong identity' => [['poll_request_id' => 'different-poll']],
            'wrong status' => [['poll_status' => 'empty']],
            'claimed task' => [['task' => ['task_id' => 'claimed']]],
            'claim admitted' => [['claim_admitted' => true]],
            'reuse missing' => [['retry_same_poll_request_id' => null]],
            'retry disabled' => [['retryable' => false]],
            'delay missing' => [['retry_after_seconds' => null]],
            'delay zero' => [['retry_after_seconds' => 0]],
            'delay string' => [['retry_after_seconds' => '1']],
            'state normal' => [['storage_state' => 'normal']],
            'state absent' => [['storage_state' => null]],
            'request admitted' => [['request_admitted' => true]],
            'unavailable is not fenced' => [[
                'reason' => 'storage_admission_unavailable',
                'poll_status' => 'storage_admission_unavailable',
                'storage_state' => 'draining',
            ]],
        ];
    }

    #[DataProvider('terminalAcknowledgements')]
    public function testUncertainOrInvalidAcknowledgementIsNotBlindlyRetried(TransportException $failure): void
    {
        $now = 0.0;
        $calls = 0;
        $acks = 0;
        $transport = self::taskTransport('activity', static function () use ($failure, &$acks): array {
            ++$acks;
            throw $failure;
        });
        $worker = self::worker($transport, $now);
        self::registerTask($worker, 'activity', $calls);

        try {
            $worker->tick(0);
            self::fail('Only explicit entry rejection authorizes acknowledgement retry.');
        } catch (ServerException $exception) {
            self::assertSame($failure->status ?? 0, $exception->status);
            self::assertSame(1, $calls);
            self::assertSame(1, $acks);
            self::assertSame(0.0, $now);
        }
    }

    public static function terminalAcknowledgements(): iterable
    {
        yield 'ambiguous transport loss' => [new TransportException('Response lost.', transientConnectionFailure: true)];
        yield 'generic unavailable' => [self::refused(['retryable' => true])];
        yield 'authentication' => [self::refused(self::pressure(), 401)];
        yield 'lease mismatch' => [self::refused(['reason' => 'lease_owner_mismatch'], 409)];
        yield 'outcome unspecified' => [self::refused(array_diff_key(self::pressure(), ['request_admitted' => true]))];
        yield 'outcome admitted' => [self::refused(array_replace(self::pressure(), ['request_admitted' => true]))];
    }

    public function testShutdownInterruptsAcknowledgementRetryWithoutRunningTheHandlerAgain(): void
    {
        $now = 0.0;
        $calls = 0;
        $acks = 0;
        $worker = null;
        $transport = self::taskTransport('activity', static function () use (&$acks): array {
            ++$acks;
            throw self::refused(self::pressure());
        });
        $worker = self::worker($transport, $now, static function () use (&$worker): void {
            $worker->requestShutdown();
        });
        self::registerTask($worker, 'activity', $calls);

        try {
            $worker->tick(0);
            self::fail('Stopping cannot acknowledge the task.');
        } catch (ServerException $exception) {
            self::assertSame('storage_pressure', $exception->reason);
            self::assertSame(1, $calls);
            self::assertSame(1, $acks);
            self::assertEqualsWithDelta(0.1, $now, 0.00001);
        }
    }

    #[DataProvider('taskKinds')]
    public function testExpiredLeaseAfterStorageRecoveryRemainsTerminal(string $kind): void
    {
        $now = 0.0;
        $calls = 0;
        $acks = [];
        $transport = self::taskTransport($kind, static function (array $body) use (&$acks): array {
            $acks[] = $body;
            if (count($acks) === 1) {
                throw self::refused(self::pressure());
            }
            throw self::refused(['reason' => 'lease_owner_mismatch'], 409);
        });
        $worker = self::worker($transport, $now);
        self::registerTask($worker, $kind, $calls);

        try {
            $worker->tick(0);
            self::fail('Recovery cannot bypass a lost lease.');
        } catch (ServerException $exception) {
            self::assertSame(409, $exception->status);
            self::assertSame('lease_owner_mismatch', $exception->reason);
            self::assertSame(1, $calls);
            self::assertCount(2, $acks);
            self::assertSame($acks[0], $acks[1]);
        }
    }

    public static function taskKinds(): array
    {
        return [['workflow'], ['activity'], ['query']];
    }

    #[DataProvider('localHeartbeatShutdownCases')]
    public function testShutdownDuringLocalHeartbeatDoesNotBecomeAnApplicationFailure(bool $completedEarlier): void
    {
        $now = 0.0;
        $calls = 0;
        $heartbeats = 0;
        $worker = null;
        $transport = self::taskTransport('workflow', static function (): array {
            self::fail('No completion or application failure is authorized by a refused heartbeat.');
        }, heartbeat: static function (array $body) use (&$heartbeats): array {
            ++$heartbeats;
            if ($heartbeats > 1) {
                throw self::refused(self::pressure());
            }

            return self::leaseRenewed($body);
        });
        $worker = self::worker($transport, $now, static function () use (&$worker): void {
            $worker->requestShutdown();
        });
        $worker->registerActivity('local.receipt', static function (ActivityContext $context) use (&$calls, $completedEarlier): string {
            ++$calls;
            if ($completedEarlier && $calls === 1) {
                return 'first receipt';
            }
            $context->heartbeat();

            return 'done';
        });
        $worker->registerWorkflow('storage.workflow', static function (WorkflowContext $context) use ($completedEarlier): mixed {
            if ($completedEarlier) {
                $context->localActivity('local.receipt');
            }

            return $context->localActivity('local.receipt', [], ['retry_policy' => ['max_attempts' => 3]]);
        });

        try {
            $worker->tick(0);
            self::fail('Shutdown must leave the refused task unacknowledged.');
        } catch (ServerException $exception) {
            self::assertSame('storage_pressure', $exception->reason);
            self::assertSame($completedEarlier ? 2 : 1, $calls);
            self::assertSame(2, $heartbeats);
        }
    }

    public static function localHeartbeatShutdownCases(): array
    {
        return [[false], [true]];
    }

    private static function registerTask(Worker $worker, string $kind, int &$calls, bool $fails = false): void
    {
        if ($kind === 'workflow') {
            $worker->registerWorkflow('storage.workflow', static function (WorkflowContext $context) use (&$calls, $fails): string {
                ++$calls;
                if ($fails) {
                    // Missing memo capability fails the task, not the workflow.
                    $context->upsertMemo(['stage' => 'ready']);
                }

                return 'done';
            });
        } elseif ($kind === 'activity') {
            $worker->registerActivity('storage.activity', static function (ActivityContext $context) use (&$calls, $fails): string {
                ++$calls;
                if ($fails) {
                    throw new \RuntimeException('Activity failed.');
                }

                return 'done';
            });
        } else {
            $worker->registerQuery('storage.workflow', 'status', static function (QueryContext $context) use (&$calls, $fails): string {
                ++$calls;
                if ($fails) {
                    throw new \RuntimeException('Query failed.');
                }

                return 'done';
            });
        }
    }

    private static function taskTransport(
        string $kind, \Closure $acknowledge, string $outcome = 'complete', ?\Closure $heartbeat = null,
    ): FakeTransport {
        return new FakeTransport(handler: static function (
            string $method, string $uri, array $headers, ?array $body,
        ) use ($kind, $acknowledge, $outcome, $heartbeat): array {
            if (str_ends_with($uri, "/{$kind}-tasks/poll")) {
                return ['poll_status' => 'leased', 'task' => [
                    'task_id' => 'storage-task', 'query_task_id' => 'storage-task',
                    'workflow_task_attempt' => 7, 'query_task_attempt' => 7,
                    'activity_attempt_id' => 'activity-attempt-7', 'attempt_number' => 7,
                    'lease_owner' => 'storage-worker', 'workflow_id' => 'storage-workflow',
                    'run_id' => 'storage-run', 'workflow_type' => 'storage.workflow',
                    'activity_type' => 'storage.activity', 'query_name' => 'status',
                    'payload_codec' => 'avro', 'history_events' => [],
                ]];
            }
            if (str_ends_with($uri, '/poll')) {
                return self::emptyPoll();
            }
            if (str_ends_with($uri, "/{$kind}-tasks/storage-task/{$outcome}")) {
                return $acknowledge($body);
            }
            if (str_ends_with($uri, "/{$kind}-tasks/storage-task/heartbeat")) {
                return $heartbeat !== null ? $heartbeat($body) : self::leaseRenewed($body);
            }

            self::fail("Unexpected request: {$method} {$uri}");
        });
    }

    private static function leaseRenewed(array $body): array
    {
        return [
            'task_id' => 'storage-task', 'renewed' => true,
            'lease_owner' => $body['lease_owner'], 'workflow_task_attempt' => $body['workflow_task_attempt'],
        ];
    }

    private static function worker(FakeTransport $transport, float &$now, ?\Closure $onSleep = null): Worker
    {
        return new Worker(
            new Client('https://server.example', transport: $transport), 'orders', workerId: 'storage-worker',
            clock: static function () use (&$now): float { return $now; },
            sleeper: static function (int $us) use (&$now, $onSleep): void {
                $now += $us / 1_000_000;
                self::assertLessThan(30, $now, 'Retries must recover or stop in this test.');
                $onSleep?->__invoke();
            },
        );
    }

    private static function emptyPoll(): array
    {
        return ['task' => null, 'poll_status' => 'empty'];
    }

    private static function pressure(?string $pollId = null, string $reason = 'storage_pressure'): array
    {
        return [
            'reason' => $reason,
            'storage_state' => $reason === 'storage_pressure' ? 'draining' : 'fenced',
            'retryable' => true, 'retry_after_seconds' => 1, 'request_admitted' => false,
            ...($pollId === null ? [] : [
                'task' => null, 'poll_status' => $reason, 'poll_request_id' => $pollId,
                'retry_same_poll_request_id' => true, 'claim_admitted' => false,
            ]),
        ];
    }

    private static function refused(array $response, int $status = 503): TransportException
    {
        return TransportException::fromResponse($status, $response, json_encode($response, JSON_THROW_ON_ERROR));
    }
}
