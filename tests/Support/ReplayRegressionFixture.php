<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests\Support;

use DurableWorkflow\Attribute\Workflow;
use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ActivityFailed;
use DurableWorkflow\Exception\ChildWorkflowFailed;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Exception\WorkflowCancelled;
use DurableWorkflow\Testing\WorkerTestHarness;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use RuntimeException;

final class ReplayRegressionFixture
{
    private const FIXTURE_SCHEMA = 'durable-workflow.replay-regression/v1';

    /** @return list<array<string, mixed>> */
    public static function executeFile(string $path): array
    {
        $fixture = json_decode(
            (string) file_get_contents($path),
            true,
            flags: JSON_THROW_ON_ERROR,
        );
        if (!is_array($fixture)) {
            throw new RuntimeException("Replay fixture {$path} must be a JSON object.");
        }

        return self::execute($fixture);
    }

    /**
     * @param array<string, mixed> $fixture
     * @return list<array<string, mixed>>
     */
    public static function execute(array $fixture): array
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
            if (!is_array($workflowTasks) || !array_is_list($workflowTasks) || $workflowTasks === []) {
                throw new RuntimeException("{$identity}.workflow_tasks must be a non-empty list.");
            }

            return self::executeWorkflowTasks(
                $identity,
                $workflowType,
                $input,
                $history,
                $workflowTasks,
                $fixture,
            );
        }

        $codec = new AvroPayloadCodec();
        try {
            $result = (new Replayer($codec))->replay(
                self::workflow($workflowType),
                $history,
                $input,
                'regression-corpus',
                self::taskAttributes($workflowType),
            );
            $commands = array_map(
                static fn (array $command): array => self::decodeEnvelopes($command, $codec),
                $result->commands,
            );
            if ($workflowType === 'golden.worker-update'
                && count($commands) === 1
                && ($commands[0]['type'] ?? null) === 'complete_workflow') {
                $commands[0]['type'] = 'complete_update';
                $commands[0]['update_id'] = 'update-1';
            }
        } catch (WorkflowCancelled $exception) {
            $commands = [[
                'type' => 'fail_workflow',
                'message' => $exception->getMessage(),
                'exception_type' => $exception::class,
            ]];
        } catch (NonDeterministicWorkflow $exception) {
            $commands = [[
                'type' => 'replay_error',
                'message' => $exception->getMessage(),
                'reason' => $exception->reason,
                'sequence' => $exception->sequence,
            ]];
        }
        $declaredCommands = $fixture['command_sequence'] ?? null;
        if ($declaredCommands !== null) {
            self::assertMatches(
                $declaredCommands,
                $commands,
                "{$identity}.command_sequence",
            );
        }

        $expected = $fixture['expected'] ?? null;
        if (!is_array($expected) || $expected === []) {
            throw new RuntimeException("{$identity}.expected must be a non-empty object.");
        }
        $observed = ['command_sequence' => $commands];
        if (count($commands) === 1) {
            $observed = array_merge($observed, $commands[0]);
        }
        self::assertMatches($expected, $observed, "{$identity}.expected");

        return $commands;
    }

    /**
     * @param list<mixed> $input
     * @param list<array<string, mixed>> $history
     * @param list<mixed> $workflowTasks
     * @param array<string, mixed> $fixture
     * @return list<array<string, mixed>>
     */
    private static function executeWorkflowTasks(
        string $identity,
        string $workflowType,
        array $input,
        array $history,
        array $workflowTasks,
        array $fixture,
    ): array {
        $codec = new AvroPayloadCodec();
        $client = new Client(
            'https://replay.invalid',
            transport: new FakeTransport(),
            codec: $codec,
        );
        $worker = Worker::create($client, 'regression-corpus');
        if ($workflowType === 'golden.attributed-stateful') {
            $worker->register(ReplayRegressionAttributedStatefulWorkflow::class);
        } else {
            $worker->registerWorkflow($workflowType, self::workflow($workflowType));
        }
        $harness = new WorkerTestHarness($worker, $client);
        $observedTasks = [];

        foreach ($workflowTasks as $index => $task) {
            if (!is_array($task)) {
                throw new RuntimeException("{$identity}.workflow_tasks.{$index} must be an object.");
            }
            $workflowId = $task['workflow_id'] ?? null;
            $runId = $task['run_id'] ?? null;
            if (!is_string($workflowId) || $workflowId === '' || !is_string($runId) || $runId === '') {
                throw new RuntimeException(
                    "{$identity}.workflow_tasks.{$index} must name non-empty workflow_id and run_id values.",
                );
            }

            $commands = array_map(
                static fn (array $command): array => self::decodeEnvelopes($command, $codec),
                $harness->runWorkflow(
                    $workflowType,
                    $input,
                    $history,
                    ['workflow_id' => $workflowId, 'run_id' => $runId],
                )->commands,
            );
            $observed = [
                'workflow_id' => $workflowId,
                'run_id' => $runId,
                'command_sequence' => $commands,
            ];
            if (count($commands) === 1) {
                $observed = array_merge($observed, $commands[0]);
            }
            $observedTasks[] = $observed;
        }

        $expected = $fixture['expected'] ?? null;
        if (!is_array($expected) || $expected === []) {
            throw new RuntimeException("{$identity}.expected must be a non-empty object.");
        }
        self::assertMatches($expected, ['workflow_tasks' => $observedTasks], "{$identity}.expected");

        return $observedTasks;
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
            'golden.worker-update' => static function (WorkflowContext $context): array {
                $updates = $context->updates('golden.update');

                return ['updated' => $updates[0][0] ?? null];
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
                "Replay fixture workflow {$workflowType} has no PHP implementation; "
                .'register its reproducer workflow in ReplayRegressionFixture.',
            ),
        };
    }

    /** @return array<string, mixed> */
    private static function taskAttributes(string $workflowType): array
    {
        return [
            'workflow_id' => 'regression-workflow',
            'run_id' => 'regression-inline',
            'cancel_requested' => $workflowType === 'golden.cancellation',
        ];
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

final class ReplayRegressionAttributedStatefulWorkflow
{
    private int $invocation = 0;

    /** @return array{workflow_id: string, run_id: string, invocation: int} */
    #[Workflow('golden.attributed-stateful')]
    public function run(WorkflowContext $context): array
    {
        return [
            'workflow_id' => $context->workflowId,
            'run_id' => $context->runId,
            'invocation' => ++$this->invocation,
        ];
    }
}
