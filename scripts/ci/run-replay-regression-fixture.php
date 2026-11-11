#!/usr/bin/env php
<?php

declare(strict_types=1);

use DurableWorkflow\Attribute\Workflow as WorkflowHandler;
use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroBinaryValue;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ActivityFailed;
use DurableWorkflow\Exception\ChildWorkflowFailed;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Exception\WorkflowCancelled;
use DurableWorkflow\Exception\WorkflowEntryArgumentsMismatch;
use DurableWorkflow\Model\WorkflowStreamAppendItem;
use DurableWorkflow\Transport\Transport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\DurableOperationHandle;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\Saga;
use DurableWorkflow\Worker\WorkflowClaimAborted;
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
    private int $completionRefusals = 0;
    /** @var array<string, mixed>|null */
    private ?array $refusedCompletion = null;
    private bool $activityClaimed = false;
    private int $activityCompletionAttempts = 0;
    /** @var array<string, mixed>|null */
    private ?array $activityCompletionBody = null;
    private bool $activityCompleted = false;
    private bool $workflowClaimRepaired = false;
    private int $workflowRenewals = 0;

    /**
     * @param list<array<string, mixed>> $tasks
     * @param array<string, list<array<string, mixed>>> $pagedHistory
     */
    public function __construct(
        private readonly array $tasks,
        private readonly array $pagedHistory,
        private readonly bool $storagePressure = false,
        private readonly bool $backendUnavailable = false,
        private readonly bool $activityBackendUnavailable = false,
        private readonly bool $workflowLeaseRecovery = false,
        private readonly bool $completionLockPressure = false,
        private readonly bool $occurrenceDeadline = false,
    ) {
    }

    public function send(string $method, string $uri, array $headers, ?array $body = null): array
    {
        if (str_ends_with($uri, '/api/worker/workflow-tasks/poll')) {
            if (!isset($this->tasks[$this->workflowPoll])) {
                return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
            }

            $task = $this->tasks[$this->workflowPoll++];
            if ($this->workflowLeaseRecovery && $this->workflowClaimRepaired) {
                $task['workflow_task_attempt'] = 2;
            }

            $reply = ['task' => $task, 'poll_status' => 'leased'];
            if ($task['workflow_type'] === 'golden.workflow-fresh-grant') {
                $reply['server_time'] = '2026-07-18T00:00:00.000000Z';
                $reply['task']['lease_expires_at'] = '2026-07-18T00:00:30.000000Z';
            }
            if ($this->occurrenceDeadline) {
                $reply['server_capabilities'] = ['condition_wait_occurrence_deadline' => true];
            }

            return $reply;
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
            ++$this->workflowRenewals;
            if ($this->workflowLeaseRecovery && !$this->workflowClaimRepaired) {
                $this->workflowClaimRepaired = true;
                --$this->workflowPoll;
                $refusal = ['task_id' => $task['task_id'], 'workflow_task_attempt' => 1,
                    'task_status' => 'ready', 'reason' => 'task_not_leased',
                    'error' => 'Workflow task is not currently leased.'];
                throw TransportException::fromResponse(409, $refusal, json_encode($refusal, JSON_THROW_ON_ERROR));
            }
            return [
                'task_id' => $task['task_id'],
                'workflow_task_attempt' => $this->workflowClaimRepaired ? 2 : $task['workflow_task_attempt'],
                'lease_owner' => $task['lease_owner'],
                'renewed' => true,
                'reason' => null,
            ];
        }

        if (($completionTask = $this->taskFor($uri, 'complete')) !== null) {
            if ($this->workflowLeaseRecovery
                && (!$this->workflowClaimRepaired || ($body['workflow_task_attempt'] ?? null) !== 2)) {
                throw new RuntimeException('A repaired workflow must complete only under its fresh attempt.');
            }
            if ($this->backendUnavailable || $this->completionLockPressure) {
                if ($this->refusedCompletion !== null && $body !== $this->refusedCompletion) {
                    throw new RuntimeException('Backend recovery changed the workflow completion request.');
                }
                if ($this->completionRefusals++ === 0) {
                    $this->refusedCompletion = $body;
                    $refusal = [
                        'reason' => 'backend_unavailable',
                        'operation' => 'complete_workflow_task',
                        'outcome' => 'unknown',
                        'task_id' => $completionTask['task_id'],
                        'lease_owner' => $completionTask['lease_owner'],
                        'workflow_task_attempt' => $completionTask['workflow_task_attempt'],
                        'worker_id' => $completionTask['lease_owner'],
                        'task_queue' => null,
                        'retryable' => true,
                        'retry_after_seconds' => 1,
                    ];
                    if ($this->completionLockPressure) {
                        $refusal['reason'] = 'backend_lock_pressure';
                        $refusal['outcome'] = 'deferred';
                        $refusal['recorded'] = false;
                        $refusal['backend'] = ['driver' => 'sqlite', 'lock_pressure' => true];
                        unset($refusal['worker_id'], $refusal['task_queue']);
                    }
                    throw TransportException::fromResponse(503, $refusal, json_encode($refusal, JSON_THROW_ON_ERROR));
                }
            }
            if ($this->storagePressure) {
                if ($this->refusedCompletion !== null && $body !== $this->refusedCompletion) {
                    throw new RuntimeException('Storage recovery changed the workflow acknowledgement.');
                }
                if ($this->completionRefusals++ < 2) {
                    $this->refusedCompletion = $body;
                    $refusal = [
                        'reason' => 'storage_pressure', 'storage_state' => 'fenced',
                        'retryable' => true, 'retry_after_seconds' => 1, 'request_admitted' => false,
                    ];
                    throw TransportException::fromResponse(503, $refusal, json_encode($refusal, JSON_THROW_ON_ERROR));
                }
            }
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
            if (is_array($failure) && ($failure['type'] ?? null) === WorkflowEntryArgumentsMismatch::class
                && ($failure['reason'] ?? null) === 'workflow_entry_arguments_mismatch'
                && !array_key_exists('sequence', $failure)) {
                $this->completedCommands[] = [[
                    'type' => 'replay_error',
                    'reason' => 'workflow_entry_arguments_mismatch',
                    'sequence' => null,
                ]];

                return ['failed' => true];
            }
            $claimAborted = 'PHP workflow task execution failed: ';
            if (is_array($failure) && ($failure['type'] ?? null) === WorkflowClaimAborted::class
                && is_string($failure['message'] ?? null) && str_starts_with($failure['message'], $claimAborted)
                && str_contains(substr($failure['message'], strlen($claimAborted)), ': ')) {
                // An abandoned claim writes no command; record its stable reason prefix.
                $this->completedCommands[] = [[
                    'type' => 'claim_aborted',
                    'reason' => strstr(substr($failure['message'], strlen($claimAborted)), ': ', true),
                ]];

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

        if (str_ends_with($uri, '/api/worker/activity-tasks/regression-activity/complete')) {
            if (!$this->activityBackendUnavailable || $body === null) {
                throw new RuntimeException('Unexpected activity completion in the replay consumer.');
            }
            if ($this->activityCompletionBody !== null && $body !== $this->activityCompletionBody) {
                throw new RuntimeException('Backend recovery changed the activity completion request.');
            }
            if ($this->activityCompletionAttempts++ === 0) {
                $this->activityCompletionBody = $body;
                $refusal = [
                    'reason' => 'backend_unavailable',
                    'operation' => 'complete_activity_task',
                    'outcome' => 'unknown',
                    'task_id' => 'regression-activity',
                    'activity_attempt_id' => 'regression-activity-attempt',
                    'lease_owner' => 'regression-consumer',
                    'worker_id' => 'regression-consumer',
                    'task_queue' => null,
                    'retryable' => true,
                    'retry_after_seconds' => 1,
                ];
                throw TransportException::fromResponse(503, $refusal, json_encode($refusal, JSON_THROW_ON_ERROR));
            }

            $this->activityCompleted = true;
            $committed = [
                'reason' => 'stale_attempt',
                'outcome' => 'completed',
                'recorded' => false,
                'task_id' => 'regression-activity',
                'activity_attempt_id' => 'regression-activity-attempt',
                'lease_owner' => 'regression-consumer',
                'activity_status' => 'completed',
                'attempt_status' => 'completed',
                'task_status' => 'completed',
            ];
            throw TransportException::fromResponse(409, $committed, json_encode($committed, JSON_THROW_ON_ERROR));
        }

        if (str_ends_with($uri, '/api/worker/activity-tasks/poll')) {
            if ($this->activityBackendUnavailable && !$this->activityClaimed
                && $this->workflowPoll >= count($this->tasks)) {
                $this->activityClaimed = true;

                return ['task' => [
                    'task_id' => 'regression-activity',
                    'activity_attempt_id' => 'regression-activity-attempt',
                    'lease_owner' => 'regression-consumer',
                    'activity_type' => 'golden.backend-recovery',
                    'payload_codec' => 'avro',
                ], 'poll_status' => 'leased'];
            }

            return $this->workflowPoll < count($this->tasks)
                ? ['task' => null, 'poll_status' => 'empty']
                : ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
        }

        if (str_ends_with($uri, '/api/worker/query-tasks/poll')) {
            return ['task' => null, 'poll_status' => 'empty'];
        }

        throw new RuntimeException("Official replay consumer received an unexpected {$method} request.");
    }

    /**
     * A claim the Worker left without completing or failing it (#269 deferral):
     * record its stable reason as the task's outcome, only for the task most
     * recently handed out and only if that task sent no completion or failure.
     *
     * @param array<string, mixed> $context the worker.claim_deferred diagnostic
     */
    public function recordDeferredClaim(array $context): void
    {
        $reason = $context['reason'] ?? null;
        $current = $this->tasks[$this->workflowPoll - 1] ?? null;
        if (!is_string($reason) || $reason === '' || $current === null || ($context['task_id'] ?? null) !== $current['task_id']
            || count($this->completedCommands) !== $this->workflowPoll - 1) {
            throw new RuntimeException('Official replay consumer received an unexpected deferred workflow claim.');
        }
        $this->completedCommands[] = [['type' => 'claim_deferred', 'reason' => $reason]];
    }

    /** @return list<list<array<string, mixed>>> */
    public function completedCommands(): array
    {
        if (count($this->completedCommands) !== count($this->tasks)) {
            throw new RuntimeException('Official replay consumer did not complete every workflow task.');
        }
        if ($this->activityBackendUnavailable
            && (!$this->activityCompleted || $this->activityCompletionAttempts !== 2)) {
            throw new RuntimeException('Official replay consumer did not recover the activity completion.');
        }

        return $this->completedCommands;
    }

    public function workflowRenewals(): int
    {
        return $this->workflowRenewals;
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

class ReplayRegressionAttributedStatefulWorkflow
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

final class ReplayRegressionNumericWorkflow extends ReplayRegressionAttributedStatefulWorkflow
{
    #[WorkflowHandler('0')]
    public function run(WorkflowContext $context): array
    {
        return parent::run($context);
    }
}

final class ReplayRegressionAttributedNestedWorkflow
{
    private readonly object $state;

    public function __construct()
    {
        $this->state = (object) ['invocation' => 0];
    }

    #[WorkflowHandler('golden.attributed-nested-state')]
    public function run(WorkflowContext $context): array
    {
        return [
            'workflow_id' => $context->workflowId,
            'run_id' => $context->runId,
            'invocation' => ++$this->state->invocation,
        ];
    }
}

final class ReplayRegressionConsumer
{
    private const COMMAND_SEQUENCE_CONTEXT = '.command_sequence';
    private const EXPECTED_CONTEXT = '.expected';
    private const CLOCK_FORMAT = 'Y-m-d\TH:i:s.uP';

    private const FIXTURE_SCHEMA = 'durable-workflow.replay-regression/v1';
    private const BOUNDED_LOCAL_FAILURE = 'golden.bounded-local-failure';

    private static int $sideEffectCalls = 0;

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

        if ($workflowType === 'golden.activity-continuation-untimed-result') {
            // Server #1078: replay directly so the fixture can assert that the replayer
            // reports a business-time read it could not answer at an untimed result.
            $codec = new AvroPayloadCodec();
            $replayer = new \DurableWorkflow\Worker\Replayer($codec);
            $replay = $replayer->replay(self::workflow($workflowType), $history, $input, 'regression-corpus',
                ['workflow_type' => $workflowType, 'workflow_id' => 'continuation-workflow', 'run_id' => 'continuation-run']);
            $commands = array_map(static fn (array $command): array => self::decodeEnvelopes($command, $codec), $replay->commands);
            $observed = ['command_sequence' => $commands,
                'unavailable_business_time_read' => $replayer->lastReplayReadUnavailableTime()];
            if (count($commands) === 1) {
                $observed = array_merge($observed, $commands[0]);
            }
            self::assertExpected($identity, $fixture, $observed);

            return;
        }

        if ($workflowType === 'golden.completion-pipeline') {
            require_once __DIR__.'/../../tests/Support/CompletionPipelineScenario.php';
            require_once __DIR__.'/../../tests/Support/RecordingMetricsRecorder.php';
            require_once __DIR__.'/../../tests/Support/WorkerPipelineScenario.php';
            $observed = \DurableWorkflow\Tests\Support\WorkerPipelineScenario::run(true);
            if ($observed['error'] !== null) {
                throw $observed['error'];
            }
            $commands = array_map(static fn (array $command): array => self::decodeEnvelopes($command, new AvroPayloadCodec()),
                $observed['completions'][2]['commands']);
            self::assertMatches($fixture['command_sequence'], $commands, $identity.self::COMMAND_SEQUENCE_CONTEXT);
            $commands[0]['completion_order'] = \DurableWorkflow\Tests\Support\WorkerPipelineScenario::completionOrder($observed);
            self::assertMatches($fixture['expected'], ['command_sequence' => $commands], $identity.self::EXPECTED_CONTEXT);

            return;
        }

        if ($workflowType === 'golden.workflow-completion-poll') {
            require_once __DIR__.'/../../tests/Support/FakeTransport.php';
            require_once __DIR__.'/../../tests/Support/WorkflowCompletionPollScenario.php';
            $observed = \DurableWorkflow\Tests\Support\WorkflowCompletionPollScenario::run();
            self::assertMatches($fixture['command_sequence'], $observed['commands'], $identity.self::COMMAND_SEQUENCE_CONTEXT);
            self::assertMatches($fixture['expected'], $observed['commands'][0] + $observed, $identity.self::EXPECTED_CONTEXT);

            return;
        }

        if ($workflowType === 'golden.carried-workflow-poll') {
            require_once __DIR__.'/../../tests/Support/FakeTransport.php';
            require_once __DIR__.'/../../tests/Support/CarriedWorkflowScenario.php';
            $observed = \DurableWorkflow\Tests\Support\CarriedWorkflowScenario::run();
            self::assertMatches($fixture['command_sequence'], $observed['commands'], $identity.self::COMMAND_SEQUENCE_CONTEXT);
            self::assertMatches($fixture['expected'], $observed['commands'][0] + $observed, $identity.self::EXPECTED_CONTEXT);

            return;
        }

        if ($workflowType === 'golden.carried-activity-poll') {
            require_once __DIR__.'/../../tests/Support/FakeTransport.php';
            require_once __DIR__.'/../../tests/Support/CarriedActivityScenario.php';
            $observed = \DurableWorkflow\Tests\Support\CarriedActivityScenario::run();
            self::assertMatches($fixture['command_sequence'], $observed['commands'], $identity.self::COMMAND_SEQUENCE_CONTEXT);
            self::assertMatches($fixture['expected'], $observed['commands'][0] + $observed, $identity.self::EXPECTED_CONTEXT);

            return;
        }

        if ($workflowType === 'golden.sticky-suffix') {
            require_once __DIR__.'/../../tests/Support/FakeTransport.php';
            require_once __DIR__.'/../../tests/Support/StickySuffixScenario.php';
            $observed = \DurableWorkflow\Tests\Support\StickySuffixScenario::run();
            self::assertMatches($fixture['command_sequence'], $observed['commands'], $identity.self::COMMAND_SEQUENCE_CONTEXT);
            self::assertMatches($fixture['expected'], $observed['commands'][0]
                + ['authoritative_fetches' => $observed['authoritative_fetches']], $identity.self::EXPECTED_CONTEXT);

            return;
        }

        if ($workflowType === 'golden.activity-poll-transit') {
            require_once __DIR__.'/../../tests/Support/ActivityGrantScenario.php';
            $observed = \DurableWorkflow\Tests\Support\ActivityGrantScenario::runIsolated();
            self::assertMatches($fixture['command_sequence'], $observed['commands'], $identity.self::COMMAND_SEQUENCE_CONTEXT);
            self::assertMatches($fixture['expected'], $observed['commands'][0]
                + ['activity_callback_count' => $observed['callbacks']], $identity.self::EXPECTED_CONTEXT);

            return;
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
            storagePressure: $workflowType === 'golden.local-activity-recovered',
            backendUnavailable: $workflowType === 'golden.completion-backend-loss',
            activityBackendUnavailable: $workflowType === 'golden.activity-completion-backend-loss',
            workflowLeaseRecovery: $workflowType === 'golden.workflow-lease-recovery',
            completionLockPressure: $workflowType === 'golden.completion-lock-pressure',
            occurrenceDeadline: self::occurrenceDeadlineAdvertised($workflowType, $history),
        );
        $now = 0.0;
        $worker = new Worker(
            new Client('https://replay.invalid', transport: $transport, codec: $codec),
            'regression-corpus',
            workerId: 'regression-consumer',
            clock: static function () use (&$now): float { return $now; },
            sleeper: static function (int $us) use (&$now): void {
                $now += $us / 1_000_000;
                if ($now > 30) {
                    throw new RuntimeException('Storage recovery exceeded the fixture retry budget.');
                }
            },
            diagnosticListener: $workflowType === 'golden.signal-occurrence-deferred'
                ? static function (string $event, array $context) use ($transport): void {
                    if ($event === 'worker.claim_deferred') {
                        $transport->recordDeferredClaim($context);
                    }
                }
                : null,
        );
        if ($workflowType === 'golden.telemetry-context') {
            $worker = new Worker(new Client('https://replay.invalid', transport: $transport, codec: $codec),
                'regression-corpus', workerId: 'regression-consumer', clock: static fn (): float => 0,
                telemetry: new \DurableWorkflow\Telemetry\ExecutionTelemetry());
        }
        if ($workflowType === 'golden.attributed-stateful') {
            $worker->register(ReplayRegressionAttributedStatefulWorkflow::class);
        } elseif ($workflowType === '0') {
            $worker->register(ReplayRegressionNumericWorkflow::class);
        } elseif ($workflowType === 'golden.attributed-nested-state') {
            $worker->register(ReplayRegressionAttributedNestedWorkflow::class);
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
        $localActivityInvocations = 0;
        self::registerLocalActivities($worker, $workflowType, $localActivityInvocations);
        self::$sideEffectCalls = 0;
        foreach ($tasks as $_task) {
            $worker->tick(0);
            gc_collect_cycles();
        }
        if ($workflowType === 'golden.workflow-lease-recovery') {
            $worker->tick(0);
        }
        if ($localActivityInvocations !== (in_array($workflowType, ['golden.activity-completion-backend-loss', self::BOUNDED_LOCAL_FAILURE, 'golden.local-activity-context'], true) ? 1 : 0)) {
            throw new RuntimeException("{$identity} repeated a recorded local activity during cold replay.");
        }
        if (self::$sideEffectCalls !== 0) {
            throw new RuntimeException("{$identity} repeated a recorded side effect during cold replay.");
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
        if ($workflowType === 'golden.workflow-fresh-grant') {
            $observed['initial_workflow_renewals'] = $transport->workflowRenewals();
        }
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

    private static function registerLocalActivities(Worker $worker, string $workflowType, int &$localActivityInvocations): void
    {
        if (in_array($workflowType, ['golden.local-activity-terminal-failure', 'golden.local-activity-recovered'], true)) {
            $worker->registerActivity(
                $workflowType === 'golden.local-activity-recovered' ? 'golden.local-receipt' : 'golden.local-failure',
                static function (ActivityContext $_context) use (&$localActivityInvocations): never {
                    ++$localActivityInvocations;

                    throw new RuntimeException('Cold replay repeated the recorded local activity side effect.');
                },
            );
        }
        if ($workflowType === self::BOUNDED_LOCAL_FAILURE) {
            $worker->registerActivity(self::BOUNDED_LOCAL_FAILURE,
                static function (ActivityContext $context) use (&$localActivityInvocations): never {
                    if ($context->activityType !== self::BOUNDED_LOCAL_FAILURE) {
                        throw new UnexpectedValueException('Unexpected bounded-local activity context.');
                    }
                    ++$localActivityInvocations;
                    throw new RuntimeException(str_repeat('雪', 1024 * 1024));
                });
        }
        if ($workflowType === 'golden.local-activity-context') {
            $worker->registerActivity('golden.context',
                static function (ActivityContext $context) use (&$localActivityInvocations): array {
                    ++$localActivityInvocations;

                    return ['workflow_id' => $context->workflowId ?? null, 'run_id' => $context->runId ?? null,
                        'policy_provided' => $context->retryInfo->policyProvided ?? null,
                        'last_configured_attempt' => isset($context->retryInfo) ? $context->retryInfo->isLastConfiguredAttempt() : null];
                });
        }
        if ($workflowType === 'golden.activity-completion-backend-loss') {
            $worker->registerActivity(
                'golden.backend-recovery',
                static function (ActivityContext $_context) use (&$localActivityInvocations): string {
                    ++$localActivityInvocations;

                    return 'activity-after-backend-recovery';
                },
            );
        }
    }

    /**
     * Whether the replayed Server advertises condition_wait_occurrence_deadline
     * (#269). The rollback fixtures withdraw it; otherwise only a history that
     * already holds an anchored condition-wait record (which only a capable
     * Server can produce) replays against a capable Server.
     *
     * @param list<array<string, mixed>> $history
     */
    private static function occurrenceDeadlineAdvertised(string $workflowType, array $history): bool
    {
        if (in_array($workflowType, ['golden.signal-occurrence-unavailable', 'golden.signal-occurrence-deferred'], true)) {
            return false;
        }
        foreach ($history as $event) {
            if (($event['event_type'] ?? $event['type'] ?? null) === 'ConditionWaitOpened'
                && is_array($event['payload'] ?? null) && array_key_exists('deadline_policy', $event['payload'])) {
                return true;
            }
        }

        return false;
    }

    /** @return callable(WorkflowContext, mixed ...$input): mixed */
    private static function workflow(string $workflowType): callable
    {
        return match ($workflowType) {
            'golden.signal-occurrence-unavailable', 'golden.signal-occurrence-deferred' => self::workflow('golden.signal'),
            'golden.telemetry-context' => static function (WorkflowContext $context): array {
                $result = $context->activity('golden.greet', ['Ada']);
                $fields = \DurableWorkflow\Telemetry\ExecutionTelemetry::current();

                return ['correlated' => ($fields['workflow_instance_id'] ?? null) === $context->workflowId
                    && ($fields['workflow_run_id'] ?? null) === $context->runId,
                    'namespace' => $fields['namespace'] ?? null, 'result' => $result];
            },
            'golden.entry-arguments-required' => static fn (WorkflowContext $context, int $amount, int $required): int => $amount + $required,
            'golden.finally-disposal' => static function (WorkflowContext $context): void {
                try {
                    $context->sleep(1);
                } finally {
                    $context->activity('cleanup');
                }
            },
            'golden.nexus-caller' => static function (WorkflowContext $context): mixed {
                return $context->serviceOperation('billing', 'invoices', 'create', ['invoice_id' => 42],
                    ['idempotency_key' => 'call-key'])->responsePayload;
            },
            'golden.invalid-activity-options' => static function (WorkflowContext $context, array $options): mixed {
                try {
                    return $context->activity('golden.greet', [], $options);
                } catch (\InvalidArgumentException $error) {
                    return $error->getMessage();
                }
            },
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
            'golden.immediate-condition' => static function (WorkflowContext $context): array {
                $first = $context->activity('golden.first');
                $satisfied = $context->waitCondition(static fn (): bool => true, key: 'already-ready');
                $second = $context->activity('golden.second');

                return ['first' => $first, 'satisfied' => $satisfied, 'second' => $second];
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
            'golden.child-start-handle' => static function (
                WorkflowContext $context,
                mixed $workflowType,
            ): array {
                $child = $context->startChildWorkflow((string) $workflowType, ['golden-input']);
                $identity = $child->identity();

                return ['identity' => [$identity->callId, $identity->workflowId, $identity->initialRunId], 'child' => $child->await()];
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
            'golden.condition-sequential-legacy' => static function (WorkflowContext $context): bool {
                $state = (object) ['signal' => 'first'];
                $predicate = static function () use ($context, $state): bool {
                    return $context->signals($state->signal) !== [];
                };
                $context->waitCondition($predicate, key: 'approval', timeout: 30);
                $state->signal = 'second';

                return $context->waitCondition($predicate, key: 'approval', timeout: 20);
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
            'golden.mixed-version-zero' => static function (WorkflowContext $context): array {
                $version = $context->getVersion('corrected-zero', -7, 0);
                $patched = $context->patched('corrected-zero');
                $context->deprecatePatch('corrected-zero');
                $wider = $context->getVersion('corrected-zero', 0, 10);

                return [$version, $patched, $wider, $context->patched('corrected-zero')];
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
            'golden.workflow-fresh-grant' => static fn (WorkflowContext $context): string => 'fresh-grant',
            'golden.workflow-stream' => static function (WorkflowContext $context): string {
                $context->appendWorkflowStream('tokens', [
                    new WorkflowStreamAppendItem(['token' => 'hello']),
                    new WorkflowStreamAppendItem(payloadReference: 's3://payloads/token-2'),
                ]);
                $context->closeWorkflowStream('tokens');

                return 'done';
            },
            'golden.message-stream' => static function (WorkflowContext $context): array {
                return array_map(
                    static fn ($message): string => $message->messageId,
                    $context->messageStream('orders')->receive(),
                );
            },
            'golden.memo' => static function (WorkflowContext $context): string {
                $context->upsertMemo([
                    'text' => 'same',
                    'nested' => ['beta' => 2, 'alpha' => 1],
                    'long' => 7,
                    'double' => 7.0,
                    'negative_zero' => -0.0,
                    'binary' => AvroBinaryValue::fromBytes('same'),
                    'invalid_binary' => AvroBinaryValue::fromBytes("\xFF\x00"),
                ]);

                return 'memo-replayed';
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
            'golden.selection' => static function (WorkflowContext $context): array {
                $selected = $context->select([
                    'first' => static fn () => $context->activity('golden.activity-one'),
                    'second' => static fn () => $context->activity('golden.activity-two'),
                ]);

                return [
                    'key' => $selected->key,
                    'identity' => $selected->identity,
                    'value' => $selected->result(),
                ];
            },
            'golden.selection-handle-isolation' => self::selectionHandleIsolationWorkflow(),
            'golden.selection-incomplete-nested-member' => static function (WorkflowContext $context): array {
                $selected = $context->select([
                    'deadline' => static function () use ($context): void {
                        $context->sleep(0);
                    },
                    'nested' => static fn () => $context->all([
                        static fn () => $context->activity('golden.activity-one'),
                        static fn () => $context->activity('golden.activity-two'),
                    ]),
                ]);

                return ['key' => $selected->key];
            },
            self::BOUNDED_LOCAL_FAILURE => static function (WorkflowContext $context): array {
                try {
                    $context->localActivity('golden.bounded-local-failure', [], ['retry_policy' => ['max_attempts' => 1]]);
                    return ['unexpected_success' => true];
                } catch (ActivityFailed $failure) {
                    return ['marked' => str_contains($failure->getMessage(), '[truncated failure report;'),
                        'message_bytes' => strlen($failure->getMessage()), 'failure_type' => $failure->failureType,
                        'non_retryable' => $failure->nonRetryable];
                }
            },
            'golden.local-activity-context' => static fn (WorkflowContext $context): mixed => $context->localActivity('golden.context'),
            'golden.local-activity-recovered' => static fn (WorkflowContext $context): mixed =>
                $context->localActivity('golden.local-receipt'),
            'golden.local-activity-terminal-failure' => static function (WorkflowContext $context): never {
                $context->localActivity('golden.local-failure');

                throw new RuntimeException('Workflow failed after the local activity returned.');
            },
            'golden.completion-backend-loss', 'golden.completion-lock-pressure' => static fn (WorkflowContext $context): string =>
                'completion-after-backend-recovery',
            'golden.workflow-lease-recovery' => static fn (WorkflowContext $context): mixed =>
                $context->activity('golden.lease-receipt'),
            'golden.activity-completion-backend-loss' => static fn (WorkflowContext $context): mixed =>
                $context->activity('golden.backend-recovery'),
            'golden.redrive-reused-search-attributes-changed' => static function (WorkflowContext $context, mixed $name): mixed {
                $context->upsertMemo(['stage' => 'greeting', 'customer' => $name]);
                $context->upsertSearchAttributes(['Stage' => 'approved']);

                return $context->activity('Tests\\Fixtures\\V2\\TestRedriveFirstActivity', [$name]);
            },
            'golden.redrive-reused-search-attribute-types' => static function (WorkflowContext $context, mixed $name): mixed {
                $context->upsertMemo(['stage' => 'greeting', 'customer' => $name]);
                $context->upsertSearchAttributes(['Stage' => 'greeting'], ['Stage' => 'keyword']);

                return $context->activity('Tests\\Fixtures\\V2\\TestRedriveFirstActivity', [$name]);
            },
            'golden.redrive-reused-search-attributes-missing' => static function (WorkflowContext $context, mixed $name): mixed {
                $context->upsertMemo(['stage' => 'greeting', 'customer' => $name]);
                $context->upsertSearchAttributes(['Stage' => 'greeting']);

                return $context->activity('Tests\\Fixtures\\V2\\TestRedriveFirstActivity', [$name]);
            },
            'golden.redrive-reused-markers' => static function (WorkflowContext $context, mixed $name): mixed {
                $token = $context->sideEffect(static function (): string {
                    ++self::$sideEffectCalls;

                    return bin2hex(random_bytes(8));
                });
                $version = $context->getVersion('redrive-markers', -1, 1);

                return $context->activity('Tests\\Fixtures\\V2\\TestRedriveFirstActivity', [$name, $token, $version]);
            },
            'golden.business-replay-clock-group-failure' => static function (WorkflowContext $context): array {
                try { $context->all([$context->deferActivity('first'), $context->deferActivity('second')]); }
                catch (\DurableWorkflow\Exception\ActivityFailed $failure) { $message = $failure->getMessage(); }
                try { $failedTime = $context->replayNow()->format(self::CLOCK_FORMAT); }
                catch (\DurableWorkflow\Exception\ReplayClockUnavailable) { $failedTime = null; }
                $context->all([$context->deferActivity('first'), $context->deferTimer(2)]);
                try { $time = $context->replayNow()->format(self::CLOCK_FORMAT); }
                catch (\DurableWorkflow\Exception\ReplayClockUnavailable) { $time = null; }

                return ['failure' => $message ?? 'missing', 'failed_time' => $failedTime, 'time' => $time];
            },
            'golden.business-replay-clock-group-condition' => static function (WorkflowContext $context): array {
                $values = $context->all([$context->deferCondition(static fn (): bool => true, 'clock-condition'), $context->deferTimer(2)]);
                try { $time = $context->replayNow()->format(self::CLOCK_FORMAT); }
                catch (\DurableWorkflow\Exception\ReplayClockUnavailable) { $time = null; }

                return ['values' => $values, 'time' => $time];
            },
            'golden.business-replay-clock-group-child' => static function (WorkflowContext $context): array {
                $values = $context->all([$context->deferChildWorkflow('clock-child'), $context->deferTimer(2)]);
                try { $time = $context->replayNow()->format(self::CLOCK_FORMAT); }
                catch (\DurableWorkflow\Exception\ReplayClockUnavailable) { $time = null; }

                return ['values' => $values, 'time' => $time];
            },
            'golden.business-replay-clock-group' => static function (WorkflowContext $context): array {
                $values = $context->all([$context->deferActivity('first'), $context->deferTimer(2)]);
                try { $time = $context->replayNow()->format(self::CLOCK_FORMAT); }
                catch (\DurableWorkflow\Exception\ReplayClockUnavailable) { $time = null; }

                return ['values' => $values, 'time' => $time];
            },
            'golden.activity-continuation-untimed-result' => static function (WorkflowContext $context): array {
                // Server #1078: a speculative result has no server-assigned time, so
                // business time at its boundary is unavailable; earlier boundaries keep theirs.
                $time = static function () use ($context): ?string {
                    try { return $context->replayNow()->format(self::CLOCK_FORMAT); }
                    catch (\DurableWorkflow\Exception\ReplayClockUnavailable) { return null; }
                };
                $first = $context->activity('continuation.first');
                $firstTime = $time();
                $second = $context->activity('continuation.second');

                return ['first' => $first, 'first_time' => $firstTime, 'second' => $second, 'second_time' => $time()];
            },
            'golden.business-replay-clock-condition' => static function (WorkflowContext $context): array {
                $predicate = static fn (): bool => true;
                $outcomes = [];
                for ($index = 0; $index < 2; ++$index) {
                    $value = $context->waitCondition($predicate, key: 'clock-condition', timeout: 5);
                    try { $time = $context->replayNow()->format(self::CLOCK_FORMAT); }
                    catch (\DurableWorkflow\Exception\ReplayClockUnavailable) { $time = null; }
                    $outcomes[] = ['outcome' => $value, 'time' => $time];
                }

                return $outcomes;
            },
            'golden.business-replay-clock-child' => static function (WorkflowContext $context, int $count = 4): array {
                $outcomes = [];
                for ($index = 0; $index < $count; ++$index) {
                    try { $value = $context->childWorkflow('clock-child'); }
                    catch (\DurableWorkflow\Exception\ChildWorkflowFailed $failure) { $value = $failure->failureType; }
                    try { $time = $context->replayNow()->format(self::CLOCK_FORMAT); }
                    catch (\DurableWorkflow\Exception\ReplayClockUnavailable) { $time = null; }
                    $outcomes[] = ['outcome' => $value, 'time' => $time];
                }

                return $outcomes;
            },
            'golden.business-replay-clock' => static function (WorkflowContext $context, mixed $name, mixed $seconds): array {
                // Test-only compatibility observation: an old product has no accessor.
                $read = static function () use ($context): ?string {
                    if (!method_exists($context, 'replayNow')) { return null; }
                    try { return $context->replayNow()->format(self::CLOCK_FORMAT); }
                    catch (\DurableWorkflow\Exception\ReplayClockUnavailable) { return null; }
                };
                $times = [$read()];
                $context->activity('Tests\\Fixtures\\V2\\TestRedriveFirstActivity', [$name]);
                $times[] = $read();
                $context->sleep((int) $seconds);
                $times[] = $read();

                return $times;
            },
            'golden.redrive-reused-timer' => static function (WorkflowContext $context, mixed $name, mixed $seconds): mixed {
                $greeting = $context->activity('Tests\\Fixtures\\V2\\TestRedriveFirstActivity', [$name]);
                $context->sleep((int) $seconds);

                return $context->activity('Tests\\Fixtures\\V2\\TestRedriveSecondActivity', [$greeting]);
            },
            'golden.redrive-reused-cancellation-clock' => static function (WorkflowContext $context): float {
                try {
                    $context->sleep(10);
                } catch (WorkflowCancelled $cancelled) {
                    return $context->cancellationShield(static function () use ($context, $cancelled): float {
                        $context->activity('golden.redrive.cleanup');
                        $context->sleep(5);

                        return $cancelled->context->remaining();
                    });
                }

                return -1.0;
            },
            'golden.redrive-reused-upserts' => static function (WorkflowContext $context, mixed $name): mixed {
                $context->upsertMemo(['stage' => 'greeting', 'customer' => $name]);
                $context->upsertSearchAttributes(['Stage' => 'greeting']);
                $greeting = $context->activity('Tests\\Fixtures\\V2\\TestRedriveFirstActivity', [$name]);

                return $context->activity('Tests\\Fixtures\\V2\\TestRedriveSecondActivity', [$greeting]);
            },
            'golden.redrive-reused-signal-wait' => static function (WorkflowContext $context, mixed $name): mixed {
                $context->waitCondition(static fn (): bool => $context->signals('approve') !== [], 'approve');
                $greeting = $context->activity('Tests\\Fixtures\\V2\\TestRedriveFirstActivity', [$name]);

                return $context->activity('Tests\\Fixtures\\V2\\TestRedriveSecondActivity', [$greeting]);
            },
            'golden.redrive-successor-markers' => static function (WorkflowContext $context, mixed $name): mixed {
                $context->sideEffect(static function (): string {
                    ++self::$sideEffectCalls;

                    return bin2hex(random_bytes(8));
                });
                $context->getVersion('redrive-markers', -1, 1);
                $greeting = $context->activity('Tests\\Fixtures\\V2\\TestRedriveFirstActivity', [$name]);

                return $context->activity('Tests\\Fixtures\\V2\\TestRedriveSecondActivity', [$greeting]);
            },
            'golden.redrive-successor-timer' => static function (WorkflowContext $context, mixed $name, mixed $seconds): mixed {
                $greeting = $context->activity('Tests\\Fixtures\\V2\\TestRedriveFirstActivity', [$name]);
                $context->sleep((int) $seconds);

                return $context->activity('Tests\\Fixtures\\V2\\TestRedriveSecondActivity', [$greeting]);
            },
            'golden.redrive-successor-cancellation-clock' => static function (WorkflowContext $context, mixed $name, mixed $seconds): mixed {
                $greeting = $context->activity('Tests\\Fixtures\\V2\\TestRedriveFirstActivity', [$name]);
                $context->sleep((int) $seconds);
                try {
                    return $context->activity('Tests\\Fixtures\\V2\\TestRedriveSecondActivity', [$greeting]);
                } catch (WorkflowCancelled $cancelled) {
                    return $cancelled->context->remaining();
                }
            },
            'golden.redrive-reused-upserts-cancellation-clock' => static function (WorkflowContext $context): float {
                $context->upsertMemo(['stage' => 'greeting', 'customer' => 'Taylor']);
                $context->upsertSearchAttributes(['Stage' => 'greeting']);
                try {
                    $context->sleep(10);
                } catch (WorkflowCancelled $cancelled) {
                    return $cancelled->context->remaining();
                }

                return -1.0;
            },
            default => throw new RuntimeException(
                "Replay fixture workflow {$workflowType} has no PHP implementation in the official consumer.",
            ),
        };
    }

    /** @return callable(WorkflowContext): array<string, mixed> */
    private static function selectionHandleIsolationWorkflow(): callable
    {
        $handle = null;

        return static function (WorkflowContext $context) use (&$handle): array {
            $previousHandle = $handle;
            $selected = $context->select([
                'first' => static fn () => $context->activity('golden.activity-one'),
                'second' => static fn () => $context->activity('golden.activity-two'),
            ]);
            $handle = $selected->handles['second'];
            if ($previousHandle instanceof DurableOperationHandle) {
                return ['reused_value' => $previousHandle->await()];
            }

            return ['winner' => $selected->key];
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
