#!/usr/bin/env php
<?php

declare(strict_types=1);

use DurableWorkflow\Attribute\Workflow as WorkflowHandler;
use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ActivityFailed;
use DurableWorkflow\Exception\ChildWorkflowFailed;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Exception\WorkflowCancelled;
use DurableWorkflow\Model\WorkflowStreamAppendItem;
use DurableWorkflow\Transport\Transport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\Saga;
use DurableWorkflow\Worker\WorkflowContext;

try {
    $options = getopt('', ['vendor-root:', 'source-root:', 'fixture:']);
    if (!is_array($options)) {
        throw new RuntimeException('Unable to parse replay runner arguments.');
    }

    $vendorRoot = realpath((string) ($options['vendor-root'] ?? ''));
    $sourceRoot = realpath((string) ($options['source-root'] ?? ''));
    $fixturePath = realpath((string) ($options['fixture'] ?? ''));
    if ($vendorRoot === false || $sourceRoot === false || $fixturePath === false) {
        throw new RuntimeException(
            'Replay runner requires existing --vendor-root, --source-root, and --fixture paths.',
        );
    }
    $source = realpath($sourceRoot.'/src');
    $avroSource = realpath($vendorRoot.'/apache/avro/lang/php/lib');
    $vendorAutoload = $vendorRoot.'/autoload.php';
    if ($source === false || $avroSource === false || !is_file($vendorAutoload)) {
        throw new RuntimeException('Replay runner source dependencies are missing.');
    }

    require $vendorAutoload;
    $prefixes = [
        'DurableWorkflow\\' => $source,
        'Apache\\Avro\\' => $avroSource,
    ];
    spl_autoload_register(
        static function (string $class) use ($prefixes): void {
            foreach ($prefixes as $prefix => $directory) {
                if (!str_starts_with($class, $prefix)) {
                    continue;
                }
                $path = $directory.'/'.str_replace('\\', '/', substr($class, strlen($prefix))).'.php';
                if (is_file($path)) {
                    require $path;
                }

                return;
            }
        },
        true,
        true,
    );
} catch (Throwable $exception) {
    fwrite(STDERR, $exception::class.': '.$exception->getMessage().PHP_EOL);
    exit(1);
}

final class ReplayRegressionTransport implements Transport
{
    /** @var list<list<array<string, mixed>>> */
    private array $completedCommands = [];
    private int $workflowPoll = 0;

    /**
     * @param list<array<string, mixed>> $tasks
     * @param array<string, list<array<string, mixed>>> $pagedHistory
     */
    public function __construct(
        private readonly array $tasks,
        private readonly array $pagedHistory,
    ) {
    }

    public function send(string $method, string $uri, array $headers, ?array $body = null): array
    {
        if (str_ends_with($uri, '/api/worker/workflow-tasks/poll')) {
            if (!isset($this->tasks[$this->workflowPoll])) {
                return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
            }

            return ['task' => $this->tasks[$this->workflowPoll++], 'poll_status' => 'leased'];
        }

        $task = $this->taskFor($uri, 'history');
        if ($task !== null) {
            $taskId = (string) $task['task_id'];
            if (($body['next_history_page_token'] ?? null) !== 'regression-page-1') {
                throw new RuntimeException('Official replay consumer received an unexpected history page token.');
            }

            return [
                'history_events' => $this->pagedHistory[$taskId] ?? [],
                'next_history_page_token' => '',
            ];
        }

        $task = $this->taskFor($uri, 'heartbeat');
        if ($task !== null) {
            return [
                'task_id' => $task['task_id'],
                'workflow_task_attempt' => $task['workflow_task_attempt'],
                'lease_owner' => $task['lease_owner'],
                'renewed' => true,
                'reason' => null,
            ];
        }

        if ($this->taskFor($uri, 'complete') !== null) {
            $commands = $body['commands'] ?? null;
            if (!is_array($commands) || !array_is_list($commands)) {
                throw new RuntimeException('Official replay consumer received invalid workflow commands.');
            }
            /** @var list<array<string, mixed>> $commands */
            $this->completedCommands[] = $commands;

            return ['completed' => true];
        }

        if ($this->taskFor($uri, 'fail') !== null) {
            $failure = $body['failure'] ?? null;
            if (is_array($failure) && ($failure['type'] ?? null) === 'WorkflowTaskWaitingForHistory') {
                $this->completedCommands[] = [];

                return ['failed' => true];
            }
            if (!is_array($failure)
                || ($failure['type'] ?? null) !== NonDeterministicWorkflow::class
                || !is_string($failure['reason'] ?? null)
                || !is_int($failure['sequence'] ?? null)) {
                throw new RuntimeException('Official replay consumer reported an unexpected workflow task failure.');
            }
            $this->completedCommands[] = [[
                'type' => 'replay_error',
                'reason' => $failure['reason'],
                'sequence' => $failure['sequence'],
            ]];

            return ['failed' => true];
        }

        if (str_ends_with($uri, '/api/worker/activity-tasks/poll')) {
            return $this->workflowPoll < count($this->tasks)
                ? ['task' => null, 'poll_status' => 'empty']
                : ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
        }

        if (str_ends_with($uri, '/api/worker/query-tasks/poll')) {
            return ['task' => null, 'poll_status' => 'empty'];
        }

        throw new RuntimeException("Official replay consumer received an unexpected {$method} request.");
    }

    /** @return list<list<array<string, mixed>>> */
    public function completedCommands(): array
    {
        if (count($this->completedCommands) !== count($this->tasks)) {
            throw new RuntimeException('Official replay consumer did not complete every workflow task.');
        }

        return $this->completedCommands;
    }

    /** @return array<string, mixed>|null */
    private function taskFor(string $uri, string $operation): ?array
    {
        foreach ($this->tasks as $task) {
            $taskId = (string) $task['task_id'];
            if (str_ends_with($uri, "/api/worker/workflow-tasks/{$taskId}/{$operation}")) {
                return $task;
            }
        }

        return null;
    }
}

final class ReplayRegressionAttributedStatefulWorkflow
{
    private int $invocation = 0;

    /** @return array{workflow_id: string, run_id: string, invocation: int} */
    #[WorkflowHandler('golden.attributed-stateful')]
    public function run(WorkflowContext $context): array
    {
        return [
            'workflow_id' => $context->workflowId,
            'run_id' => $context->runId,
            'invocation' => ++$this->invocation,
        ];
    }
}

final class ReplayRegressionConsumer
{
    private const FIXTURE_SCHEMA = 'durable-workflow.replay-regression/v1';

    public static function executeFile(string $path): void
    {
        $fixture = json_decode(
            (string) file_get_contents($path),
            true,
            flags: JSON_THROW_ON_ERROR,
        );
        if (!is_array($fixture)) {
            throw new RuntimeException('Replay fixture must be a JSON object.');
        }

        self::execute($fixture);
    }

    /** @param array<string, mixed> $fixture */
    private static function execute(array $fixture): void
    {
        $identity = is_string($fixture['id'] ?? null) ? $fixture['id'] : '<unnamed>';
        if (($fixture['fixture_schema'] ?? null) !== self::FIXTURE_SCHEMA) {
            throw new RuntimeException("{$identity}.fixture_schema is unsupported.");
        }

        $bindings = $fixture['bindings'] ?? null;
        if (!is_array($bindings) || !in_array('php', $bindings, true)) {
            throw new RuntimeException("{$identity}.bindings must include php.");
        }

        $workflow = $fixture['workflow'] ?? null;
        if (!is_array($workflow)) {
            throw new RuntimeException("{$identity}.workflow must be an object.");
        }
        $workflowType = $workflow['type'] ?? null;
        if (!is_string($workflowType) || $workflowType === '') {
            throw new RuntimeException("{$identity}.workflow.type must be a non-empty string.");
        }
        $input = $workflow['input'] ?? [];
        if (!is_array($input) || !array_is_list($input)) {
            throw new RuntimeException("{$identity}.workflow.input must be a list.");
        }

        $history = $fixture['history'] ?? [];
        if (!is_array($history) || !array_is_list($history)) {
            throw new RuntimeException("{$identity}.history must be a list.");
        }
        if (array_key_exists('history', $fixture) && $history === []) {
            throw new RuntimeException("{$identity}.history must not be empty.");
        }

        $workflowTasks = $fixture['workflow_tasks'] ?? null;
        if ($workflowTasks !== null) {
            if (array_key_exists('command_sequence', $fixture)) {
                throw new RuntimeException(
                    "{$identity} cannot combine workflow_tasks with command_sequence.",
                );
            }
            if (!is_array($workflowTasks) || !array_is_list($workflowTasks) || $workflowTasks === []) {
                throw new RuntimeException("{$identity}.workflow_tasks must be a non-empty list.");
            }
            foreach ($workflowTasks as $index => $workflowTask) {
                if (!is_array($workflowTask)) {
                    throw new RuntimeException("{$identity}.workflow_tasks.{$index} must be an object.");
                }
                if (count($workflowTask) !== 2
                    || array_diff(array_keys($workflowTask), ['workflow_id', 'run_id']) !== []) {
                    throw new RuntimeException(
                        "{$identity}.workflow_tasks.{$index} must contain only workflow_id and run_id.",
                    );
                }
                foreach (['workflow_id', 'run_id'] as $field) {
                    if (!is_string($workflowTask[$field] ?? null) || $workflowTask[$field] === '') {
                        throw new RuntimeException(
                            "{$identity}.workflow_tasks.{$index}.{$field} must be a non-empty string.",
                        );
                    }
                }
            }
        }

        $layouts = $history === [] ? ['inline'] : ['inline', 'paginated'];
        foreach ($layouts as $layout) {
            self::executeLayout(
                $identity,
                $workflowType,
                $input,
                $history,
                $fixture,
                $layout,
                $workflowTasks,
            );
        }
    }

    /**
     * @param list<mixed> $input
     * @param list<array<string, mixed>> $history
     * @param array<string, mixed> $fixture
     * @param list<array<string, mixed>>|null $workflowTasks
     */
    private static function executeLayout(
        string $identity,
        string $workflowType,
        array $input,
        array $history,
        array $fixture,
        string $layout,
        ?array $workflowTasks,
    ): void {
        $codec = new AvroPayloadCodec();
        $taskIdentities = $workflowTasks ?? [[
            'workflow_id' => 'regression-workflow',
            'run_id' => "regression-{$layout}",
        ]];
        $tasks = [];
        $pagedHistory = [];
        foreach ($taskIdentities as $index => $taskIdentity) {
            $taskId = $workflowTasks === null
                ? "regression-{$layout}"
                : "regression-{$layout}-{$index}";
            $tasks[] = array_merge([
                'task_id' => $taskId,
                'workflow_task_attempt' => 1,
                'lease_owner' => 'regression-consumer',
                'workflow_id' => $taskIdentity['workflow_id'],
                'run_id' => $taskIdentity['run_id'],
                'workflow_type' => $workflowType,
                'payload_codec' => 'avro',
                'arguments' => $codec->envelope($input),
                'history_events' => $layout === 'inline' ? $history : [],
                'next_history_page_token' => $layout === 'paginated' ? 'regression-page-1' : '',
            ], self::taskAttributes($workflowType));
            $pagedHistory[$taskId] = $layout === 'paginated' ? $history : [];
        }
        $transport = new ReplayRegressionTransport(
            $tasks,
            $pagedHistory,
        );
        $worker = new Worker(
            new Client('https://replay.invalid', transport: $transport, codec: $codec),
            'regression-corpus',
            workerId: 'regression-consumer',
        );
        if ($workflowType === 'golden.attributed-stateful') {
            $worker->register(ReplayRegressionAttributedStatefulWorkflow::class);
        } else {
            $worker->registerWorkflow($workflowType, self::workflow($workflowType));
        }
        if ($workflowType === 'golden.worker-update') {
            $worker->registerUpdate(
                $workflowType,
                'golden.update',
                static fn (QueryContext $context, mixed $value = null): array => ['updated' => $value],
            );
        }
        foreach ($tasks as $_task) {
            $worker->tick(0);
        }

        $taskCommands = array_map(
            static fn (array $commands): array => array_map(
                static fn (array $command): array => self::decodeEnvelopes($command, $codec),
                $commands,
            ),
            $transport->completedCommands(),
        );
        if ($workflowTasks !== null) {
            $observedTasks = [];
            foreach ($taskCommands as $index => $commands) {
                $observed = [
                    'workflow_id' => $taskIdentities[$index]['workflow_id'],
                    'run_id' => $taskIdentities[$index]['run_id'],
                    'command_sequence' => $commands,
                ];
                if (count($commands) === 1) {
                    $observed = array_merge($observed, $commands[0]);
                }
                $observedTasks[] = $observed;
            }
            self::assertExpected($identity, $fixture, ['workflow_tasks' => $observedTasks]);

            return;
        }

        $commands = $taskCommands[0];
        $declaredCommands = $fixture['command_sequence'] ?? null;
        if ($declaredCommands !== null) {
            self::assertMatches(
                $declaredCommands,
                $commands,
                "{$identity}.command_sequence",
            );
        }

        $observed = ['command_sequence' => $commands];
        if (count($commands) === 1) {
            $observed = array_merge($observed, $commands[0]);
        }
        self::assertExpected($identity, $fixture, $observed);
    }

    /**
     * @param array<string, mixed> $fixture
     * @param array<string, mixed> $observed
     */
    private static function assertExpected(string $identity, array $fixture, array $observed): void
    {
        $expected = $fixture['expected'] ?? null;
        if (!is_array($expected) || $expected === []) {
            throw new RuntimeException("{$identity}.expected must be a non-empty object.");
        }
        self::assertMatches($expected, $observed, "{$identity}.expected");
    }

    /** @return callable(WorkflowContext, mixed ...$input): mixed */
    private static function workflow(string $workflowType): callable
    {
        return match ($workflowType) {
            'golden.single-activity' => static function (
                WorkflowContext $context,
                mixed $name,
                mixed $versionChangeId = null,
            ): mixed {
                $result = $context->activity('golden.greet', [$name]);
                if (is_string($versionChangeId)) {
                    $context->getVersion($versionChangeId, 1, 4);
                }

                return $result;
            },
            'golden.timer' => static function (WorkflowContext $context, mixed $seconds): string {
                $context->sleep((float) $seconds);

                return 'timer-fired';
            },
            'golden.child-workflow' => static function (
                WorkflowContext $context,
                mixed $workflowType,
            ): array {
                $result = $context->childWorkflow((string) $workflowType, ['golden-input']);

                return ['child' => $result];
            },
            'golden.side-effect' => static function (WorkflowContext $context, mixed $value): array {
                $result = $context->sideEffect(static fn (): mixed => $value);

                return ['side_effect' => $result];
            },
            'golden.continue-as-new' => static function (WorkflowContext $context, mixed $value): never {
                $context->continueAsNew([$value], taskQueue: 'regression-corpus');
            },
            'golden.search-attributes' => static function (WorkflowContext $context, mixed $status): string {
                $context->upsertSearchAttributes(['status' => $status]);

                return 'search-attributes-upserted';
            },
            'golden.signal' => static function (
                WorkflowContext $context,
                mixed $signalName,
            ): array {
                return match ($signalName) {
                    'condition:signal' => ['satisfied' => $context->waitCondition(
                        static fn (): bool => $context->signals('approve') !== [],
                        key: 'approval',
                        timeout: 30,
                    )],
                    'condition:timeout' => ['satisfied' => $context->waitCondition(
                        static fn (): bool => false,
                        key: 'approval-timeout',
                        timeout: 5,
                    )],
                    'condition:pending' => ['satisfied' => $context->waitCondition(
                        static fn (): bool => false,
                        key: 'approval-pending',
                        timeout: 60,
                    )],
                    default => ['signals' => $context->signals((string) $signalName)],
                };
            },
            'golden.update' => static function (
                WorkflowContext $context,
                mixed $updateName,
            ): array {
                if ($updateName === 'condition:update') {
                    return ['satisfied' => $context->waitCondition(
                        static fn (): bool => ($context->updates('approve')[0][0] ?? false) === true,
                        key: 'update-approval',
                    )];
                }

                return ['updates' => $context->updates((string) $updateName)];
            },
            'golden.context-identity' => static fn (WorkflowContext $context): array => [
                'workflow_id' => $context->workflowId,
                'run_id' => $context->runId,
            ],
            'golden.cancellation' => static function (WorkflowContext $context): string {
                if ($context->isCancellationRequested()) {
                    $context->throwIfCancellationRequested();
                }

                return 'not-cancelled';
            },
            'golden.worker-update' => static fn (WorkflowContext $context): array => [],
            'golden.workflow-stream' => static function (WorkflowContext $context): string {
                $context->appendWorkflowStream('tokens', [
                    new WorkflowStreamAppendItem(['token' => 'hello']),
                    new WorkflowStreamAppendItem(payloadReference: 's3://payloads/token-2'),
                ]);
                $context->closeWorkflowStream('tokens');

                return 'done';
            },
            'golden.saga' => static function (WorkflowContext $context, mixed $tripId): string {
                return $context->saga()->run(static function (Saga $saga) use ($context, $tripId): string {
                    $context->activity('golden.reserve-flight', [$tripId]);
                    $saga->addCompensation('python.cancel-flight', [$tripId]);

                    $context->activity('golden.reserve-hotel', [$tripId]);
                    $saga->addCompensation('python.cancel-hotel', [$tripId]);

                    $context->activity('golden.charge-card', [$tripId]);

                    return 'booked';
                });
            },
            'golden.parallel' => static function (WorkflowContext $context, mixed $mode): array {
                try {
                    return match ($mode) {
                        'mixed-flat' => $context->all([
                            static fn () => $context->activity('golden.activity-one'),
                            static fn () => $context->childWorkflow('golden.child'),
                            static fn () => $context->sleep(1),
                        ]),
                        'mixed-nested' => $context->all([
                            static fn () => $context->activity('golden.activity-one'),
                            static fn () => $context->parallel([
                                static fn () => $context->childWorkflow('golden.child'),
                                static fn () => $context->sleep(1),
                            ]),
                        ]),
                        'cancellation' => $context->all([
                            static fn () => $context->sleep(1),
                            static fn () => $context->sleep(2),
                        ]),
                        'shape-change' => $context->all([
                            static fn () => $context->activity('golden.activity-one'),
                            static fn () => $context->activity('golden.activity-two'),
                            static fn () => $context->activity('golden.activity-three'),
                        ]),
                        default => $context->all([
                            static fn () => $context->activity('golden.activity-one'),
                            static fn () => $context->activity('golden.activity-two'),
                        ]),
                    };
                } catch (ActivityFailed|ChildWorkflowFailed|WorkflowCancelled $exception) {
                    return ['failure' => $exception::class];
                }
            },
            default => throw new RuntimeException(
                "Replay fixture workflow {$workflowType} has no PHP implementation in the official consumer.",
            ),
        };
    }

    /** @return array<string, mixed> */
    private static function taskAttributes(string $workflowType): array
    {
        return match ($workflowType) {
            'golden.cancellation' => ['cancel_requested' => true],
            'golden.worker-update' => [
                'workflow_update_id' => 'update-1',
                'update_name' => 'golden.update',
            ],
            'golden.workflow-stream' => [
                'workflow_command_id' => '01JCOMMAND0000000000000000',
            ],
            default => [],
        };
    }

    /**
     * @param array<string, mixed> $value
     * @return array<string, mixed>
     */
    private static function decodeEnvelopes(array $value, AvroPayloadCodec $codec): array
    {
        if (($value['type'] ?? null) === 'record_side_effect'
            && isset($value['result'])
            && is_string($value['result'])) {
            $value['result'] = $codec->decode($value['result']);
        }

        $decoded = [];
        foreach ($value as $key => $item) {
            if (is_array($item)
                && isset($item['codec'], $item['blob'])
                && is_string($item['codec'])
                && is_string($item['blob'])) {
                $decoded[$key] = $codec->decodeEnvelope($item);
                continue;
            }
            $decoded[$key] = is_array($item)
                ? self::decodeEnvelopes($item, $codec)
                : $item;
        }

        return $decoded;
    }

    private static function assertMatches(mixed $expected, mixed $actual, string $context): void
    {
        if (is_array($expected)) {
            if (!is_array($actual)) {
                throw new RuntimeException("{$context} must be an array.");
            }
            if (array_is_list($expected)
                && (!array_is_list($actual) || count($actual) !== count($expected))) {
                throw new RuntimeException("{$context} has the wrong list shape.");
            }
            foreach ($expected as $key => $value) {
                if (!array_key_exists($key, $actual)) {
                    throw new RuntimeException("{$context} is missing {$key}.");
                }
                self::assertMatches($value, $actual[$key], "{$context}.{$key}");
            }

            return;
        }

        if ($expected !== $actual) {
            throw new RuntimeException("{$context} does not match.");
        }
    }
}

try {
    ReplayRegressionConsumer::executeFile($fixturePath);
} catch (Throwable $exception) {
    fwrite(STDERR, $exception::class.': '.$exception->getMessage().PHP_EOL);
    exit(1);
}
