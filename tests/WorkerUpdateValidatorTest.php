<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Attribute\Update;
use DurableWorkflow\Attribute\UpdateValidator;
use DurableWorkflow\Attribute\Workflow as WorkflowAttribute;
use DurableWorkflow\Client;
use DurableWorkflow\Exception\InvalidWorkerDefinition;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\UpdateValidatorCommandRefused;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\WorkflowClaimAborted;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class WorkerUpdateValidatorTest extends TestCase
{
    /** @var list<array{string, string, ?array<string, mixed>}> */
    private array $requests = [];

    public function testRejectedUpdateNeverRunsTheHandlerAndCompletesNoWorkflowTask(): void
    {
        $handlerCalls = 0;
        $validatorCalls = 0;
        [$worker, $completions] = $this->validatingWorker(
            [$this->validationTask('v-1', 'approve', [0])],
            static function (QueryContext $context, int $amount) use (&$handlerCalls): int {
                ++$handlerCalls;

                return $amount;
            },
            static function (QueryContext $context, int $amount) use (&$validatorCalls): void {
                ++$validatorCalls;
                if ($amount <= count($context->events('UpdateAccepted'))) {
                    throw new RuntimeException('The amount must exceed every accepted update.');
                }
            },
        );

        $worker->run(0);

        self::assertSame(1, $validatorCalls);
        self::assertSame(0, $handlerCalls);
        self::assertSame([[
            'uri' => '/api/worker/update-validation-tasks/v-1/reject',
            'body' => [
                'lease_owner' => 'worker-1',
                'update_validation_attempt' => 1,
                'failure' => [
                    'message' => 'The amount must exceed every accepted update.',
                    'reason' => 'update_validator_rejected',
                    'type' => RuntimeException::class,
                ],
            ],
        ]], $completions());
        self::assertSame([], $this->workflowCompletions(), 'A validation task never completes a workflow task.');
    }

    public function testApprovedUpdateLetsTheServerAcceptIt(): void
    {
        [$worker, $completions] = $this->validatingWorker(
            [$this->validationTask('v-2', 'approve', [5])],
            static fn (QueryContext $context, int $amount): int => $amount,
            static function (QueryContext $context, int $amount): bool {
                return $context->workflowId === 'orders-1' && $amount > 0;
            },
        );

        $worker->run(0);

        self::assertSame([[
            'uri' => '/api/worker/update-validation-tasks/v-2/approve',
            'body' => ['lease_owner' => 'worker-1', 'update_validation_attempt' => 1],
        ]], $completions());
    }

    public function testValidatorReturningFalseRejects(): void
    {
        [$worker, $completions] = $this->validatingWorker(
            [$this->validationTask('v-3', 'approve', [5])],
            static fn (QueryContext $context, int $amount): int => $amount,
            static fn (QueryContext $context, int $amount): bool => false,
        );

        $worker->run(0);

        $completion = $completions()[0];
        self::assertSame('/api/worker/update-validation-tasks/v-3/reject', $completion['uri']);
        self::assertSame('update_validator_rejected', $completion['body']['failure']['reason']);
        self::assertSame('Update validator for orders.approve returned false.', $completion['body']['failure']['message']);
    }

    public function testValidatorThatSchedulesWorkIsRefused(): void
    {
        [$worker, $completions] = $this->validatingWorker(
            [$this->validationTask('v-4', 'approve', [5])],
            static fn (QueryContext $context, int $amount): int => $amount,
            static function (QueryContext $context, int $amount): void {
                // Workflow operations suspend the running Fiber to emit a command.
                \Fiber::suspend(['type' => 'schedule_activity', 'activity_type' => 'charge']);
            },
        );

        $worker->run(0);

        $completion = $completions()[0];
        self::assertSame('/api/worker/update-validation-tasks/v-4/reject', $completion['uri']);
        self::assertSame('update_validator_rejected', $completion['body']['failure']['reason']);
        self::assertSame(UpdateValidatorCommandRefused::class, $completion['body']['failure']['type']);
        self::assertStringContainsString('read-only', $completion['body']['failure']['message']);
        self::assertSame([], $this->workflowCompletions());
    }

    public function testValidationErrorsAreReported(): void
    {
        [$worker, $completions] = $this->validatingWorker(
            [$this->validationTask('v-5', 'approve', [5])],
            static fn (QueryContext $context, int $amount): int => $amount,
            static function (QueryContext $context, int $amount): void {
                throw new class('Too small.') extends RuntimeException {
                    /** @return array<string, list<string>> */
                    public function errors(): array
                    {
                        return ['amount' => ['The amount is too small.']];
                    }
                };
            },
        );

        $worker->run(0);

        self::assertSame(['amount' => ['The amount is too small.']], $completions()[0]['body']['failure']['validation_errors']);
    }

    public function testFencedCompletionIsNotAWorkerFailure(): void
    {
        [$worker, $completions] = $this->validatingWorker(
            [$this->validationTask('v-6', 'approve', [5])],
            static fn (QueryContext $context, int $amount): int => $amount,
            static fn (QueryContext $context, int $amount): mixed => null,
            fence: new ServerException('stale', 409, 'stale_update_validation_completion'),
        );

        $worker->run(0);

        self::assertCount(1, $completions());
    }

    public function testRegistrationAdvertisesValidatorsAndPollsBothKinds(): void
    {
        [$worker] = $this->validatingWorker(
            [],
            static fn (QueryContext $context, int $amount): int => $amount,
            static fn (QueryContext $context, int $amount): mixed => null,
        );

        $worker->run(0);

        $register = $this->request('/api/worker/register');
        self::assertContains('update_validation_tasks', $register['capabilities']);
        self::assertSame(['approve'], $register['workflow_command_contracts']['orders']['update_validators']);
        self::assertSame(['workflow', 'update_validation'], $this->request('/api/worker/workflow-tasks/poll')['task_kinds']);
    }

    public function testRegistrationIsRefusedWithoutServerValidationSupport(): void
    {
        [$worker] = $this->validatingWorker(
            [],
            static fn (QueryContext $context, int $amount): int => $amount,
            static fn (QueryContext $context, int $amount): mixed => null,
            capable: false,
        );

        try {
            $worker->run(0);
            self::fail('A validator-bearing worker must refuse an incapable Server.');
        } catch (WorkflowClaimAborted $error) {
            self::assertStringContainsString('update_validation_tasks_unavailable', $error->getMessage());
        }
        self::assertNull($this->request('/api/worker/register'));
    }

    public function testWorkersWithoutValidatorsKeepTheirPollShape(): void
    {
        $transport = new FakeTransport(handler: function (string $method, string $uri, array $headers, ?array $body): array {
            $this->requests[] = [$method, $uri, $body];

            return str_ends_with($uri, '/register') ? ['registered' => true]
                : ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
        });
        $worker = new Worker(new Client('https://server.example', transport: $transport), 'queue', workerId: 'worker-1');
        $worker->registerWorkflow('orders', static fn (WorkflowContext $context): string => 'done');

        $worker->run(0);

        self::assertNull($this->request('/api/cluster/info'));
        self::assertArrayNotHasKey('task_kinds', $this->request('/api/worker/workflow-tasks/poll') ?? []);
        self::assertSame([], $this->request('/api/worker/register')['workflow_command_contracts']['orders']['update_validators']);
    }

    public function testAttributeDiscoveryRegistersValidators(): void
    {
        $worker = new Worker(new Client('https://server.example', transport: new FakeTransport()), 'queue', workerId: 'worker-1');
        $worker->register(ValidatedOrderWorkflow::class);

        self::assertSame(['approve'], $worker->contracts()['workflow_commands']['orders.validated']['update_validators']);
        $validator = $worker->registeredHandler('update_validator', 'approve', 'orders.validated');
        self::assertNull($validator(new QueryContext('w', 'r', [], []), 5));

        $this->expectException(RuntimeException::class);
        $validator(new QueryContext('w', 'r', [], []), 0);
    }

    public function testAValidatorMustNameADeclaredUpdate(): void
    {
        $worker = new Worker(new Client('https://server.example', transport: new FakeTransport()), 'queue', workerId: 'worker-1');

        try {
            $worker->register(UndeclaredValidatorWorkflow::class);
            self::fail('A validator for an undeclared update is a definition error.');
        } catch (InvalidWorkerDefinition $error) {
            self::assertStringContainsString('no update is named missing', $error->getMessage());
        }

        $this->expectException(\InvalidArgumentException::class);
        $worker->registerWorkflow('orders', static fn (WorkflowContext $context): string => 'done')
            ->registerUpdateValidator('orders', 'approve', static fn (QueryContext $context): mixed => null);
    }

    public function testValidatorWorkersNeverCarryWorkflowOnlyPolls(): void
    {
        $polls = [];
        $transport = new FakeTransport(handler: function (string $method, string $uri, array $headers, ?array $body) use (&$polls): ?array {
            $this->requests[] = [$method, $uri, $body];
            if (str_ends_with($uri, '/api/cluster/info')) {
                return ['worker_protocol' => ['version' => '1.20', 'server_capabilities' => self::capabilities()]];
            }
            if (str_ends_with($uri, '/api/worker/register')) {
                return ['registered' => true];
            }
            if (str_ends_with($uri, '/api/worker/workflow-tasks/poll')) {
                $polls[] = $body['task_kinds'] ?? null;
                if (count($polls) > 1) {
                    return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
                }

                return ['poll_status' => 'leased', 'server_capabilities' => [
                    'workflow_task_completion_workflow_poll' => true,
                    'workflow_task_completion_activity_poll' => true,
                    'activity_completion_workflow_poll' => true,
                ], 'task' => [
                    'task_kind' => 'workflow', 'task_id' => 'wf-1', 'workflow_task_attempt' => 1, 'lease_owner' => 'worker-1',
                    'workflow_id' => 'orders-1', 'run_id' => 'run-1', 'workflow_type' => 'orders', 'payload_codec' => 'avro',
                    'history_events' => [],
                ]];
            }
            if (preg_match('#/workflow-tasks/wf-1/heartbeat$#', $uri) === 1) {
                return ['task_id' => 'wf-1', 'workflow_task_attempt' => 1, 'lease_owner' => 'worker-1', 'renewed' => true, 'reason' => null];
            }
            if (str_ends_with($uri, '/workflow-tasks/wf-1/complete')) {
                return ['completed' => true];
            }

            return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
        });
        $worker = new Worker(new Client('https://server.example', transport: $transport), 'queue', workerId: 'worker-1');
        $worker->registerWorkflow('orders', static fn (WorkflowContext $context): string => 'done')
            ->registerUpdate('orders', 'approve', static fn (QueryContext $context, int $amount): int => $amount)
            ->registerUpdateValidator('orders', 'approve', static fn (QueryContext $context, int $amount): mixed => null);

        $worker->run(0);

        $completion = $this->request('/api/worker/workflow-tasks/wf-1/complete');
        self::assertIsArray($completion);
        self::assertArrayNotHasKey('workflow_poll', $completion, 'Carried polls are workflow-only and would starve validation tasks.');
        self::assertNotSame([], $polls);
        self::assertSame(array_fill(0, count($polls), ['workflow', 'update_validation']), $polls);
    }

    public function testClientSurfacesTheValidatorRejection(): void
    {
        $http = new \GuzzleHttp\Client(['handler' => static function (\Psr\Http\Message\RequestInterface $request) {
            return \GuzzleHttp\Promise\Create::promiseFor(new \GuzzleHttp\Psr7\Response(422, [], json_encode([
                'workflow_id' => 'orders-1', 'update_name' => 'approve', 'accepted' => false, 'approved' => false,
                'outcome' => 'rejected_invalid_arguments', 'update_status' => 'rejected', 'command_status' => 'rejected',
                'reason' => 'update_validator_rejected', 'rejection_reason' => 'update_validator_rejected',
                'message' => 'too small', 'validation_errors' => ['amount' => ['too small']],
            ], JSON_THROW_ON_ERROR)));
        }]);
        $client = new Client('https://runtime.test', transport: new \DurableWorkflow\Transport\Psr18Transport($http),
            controlToken: 'fixture-client');

        try {
            $client->updateWorkflow('orders-1', 'approve', [0], requestId: 'approve-1');
            self::fail('A validator rejection must throw.');
        } catch (\DurableWorkflow\Exception\UpdateFailed $exception) {
            self::assertSame(422, $exception->status);
            self::assertTrue($exception->rejectedByValidator());
            self::assertFalse($exception->validationFailed());
            self::assertSame(['amount' => ['too small']], $exception->validationErrors());
            self::assertSame('approve-1', $exception->requestId());
        }
    }

    /**
     * @param list<array<string, mixed>> $tasks
     * @return array{Worker, \Closure(): list<array{uri: string, body: mixed}>}
     */
    private function validatingWorker(
        array $tasks,
        callable $handler,
        callable $validator,
        bool $capable = true,
        ?ServerException $fence = null,
    ): array {
        $transport = new FakeTransport(handler: function (string $method, string $uri, array $headers, ?array $body) use (
            &$tasks, $capable, $fence,
        ): ?array {
            $this->requests[] = [$method, $uri, $body];
            if (str_ends_with($uri, '/api/cluster/info')) {
                return ['worker_protocol' => ['version' => '1.20', 'server_capabilities' => $capable ? self::capabilities() : []]];
            }
            if (str_ends_with($uri, '/api/worker/register')) {
                return ['registered' => true];
            }
            if (str_ends_with($uri, '/api/worker/workflow-tasks/poll')) {
                $task = array_shift($tasks);

                return $task === null
                    ? ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped']
                    : ['poll_status' => 'leased', 'task' => $task];
            }
            if (str_contains($uri, '/api/worker/update-validation-tasks/')) {
                if ($fence !== null) {
                    throw $fence;
                }

                return ['outcome' => str_ends_with($uri, '/approve') ? 'approved' : 'rejected'];
            }
            if (str_ends_with($uri, '/api/worker/activity-tasks/poll') || str_ends_with($uri, '/api/worker/query-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
            }

            self::fail("Unexpected worker request: {$method} {$uri}");
        });
        $worker = new Worker(new Client('https://server.example', transport: $transport), 'queue', workerId: 'worker-1');
        $worker->registerWorkflow('orders', static fn (WorkflowContext $context): string => 'waiting')
            ->registerUpdate('orders', 'approve', $handler)
            ->registerUpdateValidator('orders', 'approve', $validator);

        return [$worker, fn (): array => array_values(array_map(
            static fn (array $request): array => [
                'uri' => (string) parse_url($request[1], PHP_URL_PATH),
                'body' => $request[2],
            ],
            array_filter($this->requests, static fn (array $request): bool => str_contains($request[1], '/update-validation-tasks/')),
        ))];
    }

    /** @return list<array{string, string, ?array<string, mixed>}> */
    private function workflowCompletions(): array
    {
        return array_values(array_filter(
            $this->requests,
            static fn (array $request): bool => str_contains($request[1], '/workflow-tasks/') && !str_ends_with($request[1], '/poll'),
        ));
    }

    /** @return array<string, mixed>|null */
    private function request(string $path): ?array
    {
        foreach ($this->requests as [, $uri, $body]) {
            if (str_ends_with($uri, $path)) {
                return $body ?? [];
            }
        }

        return null;
    }

    /**
     * @param list<mixed> $arguments
     * @return array<string, mixed>
     */
    private function validationTask(string $id, string $update, array $arguments): array
    {
        $codec = (new Client('https://server.example', transport: new FakeTransport()))->payloadCodec();

        return [
            'task_kind' => 'update_validation',
            'update_validation_task_id' => $id,
            'update_id' => $id,
            'update_validation_attempt' => 1,
            'lease_owner' => 'worker-1',
            'workflow_id' => 'orders-1',
            'run_id' => 'run-1',
            'workflow_type' => 'orders',
            'update_name' => $update,
            'payload_codec' => 'avro',
            'update_arguments' => $codec->envelope($arguments),
            'history_events' => [
                ['event_type' => 'WorkflowStarted', 'payload' => []],
                ['event_type' => 'UpdateAccepted', 'payload' => ['update_id' => 'earlier', 'update_name' => $update]],
            ],
        ];
    }

    /** @return array<string, mixed> */
    private static function capabilities(): array
    {
        return [
            'update_validation_tasks' => true,
            'synchronous_update_validation' => [
                'supported' => true,
                'acceptance_boundary' => 'validator_approved',
                'worker_capability' => 'update_validation_tasks',
                'workflow_contract_field' => 'update_validators',
                'task_poll' => [
                    'strategy' => 'multiplexed',
                    'endpoint' => '/worker/workflow-tasks/poll',
                    'request_field' => 'task_kinds',
                    'task_kinds' => ['workflow', 'update_validation'],
                    'response_discriminator' => 'task.task_kind',
                ],
            ],
        ];
    }
}

final class ValidatedOrderWorkflow
{
    #[WorkflowAttribute('orders.validated')]
    public function handle(WorkflowContext $context): string
    {
        return 'done';
    }

    #[Update('approve')]
    public function approve(QueryContext $context, int $amount): int
    {
        return $amount;
    }

    #[UpdateValidator('approve')]
    public function validateApprove(QueryContext $context, int $amount): void
    {
        if ($amount <= 0) {
            throw new RuntimeException('The amount must be positive.');
        }
    }
}

final class UndeclaredValidatorWorkflow
{
    #[WorkflowAttribute('orders.undeclared')]
    public function handle(WorkflowContext $context): string
    {
        return 'done';
    }

    #[UpdateValidator('missing')]
    public function validateMissing(QueryContext $context): void
    {
    }
}
