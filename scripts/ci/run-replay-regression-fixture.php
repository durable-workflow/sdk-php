#!/usr/bin/env php
<?php

declare(strict_types=1);

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Transport\Transport;
use DurableWorkflow\Worker;
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
    if ($source === false || $avroSource === false) {
        throw new RuntimeException('Replay runner source dependencies are missing.');
    }

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
    /** @var list<array<string, mixed>>|null */
    private ?array $completedCommands = null;
    private bool $workflowPolled = false;

    /**
     * @param array<string, mixed> $task
     * @param list<array<string, mixed>> $pagedHistory
     */
    public function __construct(
        private readonly array $task,
        private readonly array $pagedHistory,
    ) {
    }

    public function send(string $method, string $uri, array $headers, ?array $body = null): array
    {
        if (str_ends_with($uri, '/api/worker/workflow-tasks/poll')) {
            if ($this->workflowPolled) {
                return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
            }
            $this->workflowPolled = true;

            return ['task' => $this->task, 'poll_status' => 'leased'];
        }

        $taskId = (string) $this->task['task_id'];
        if (str_ends_with($uri, "/api/worker/workflow-tasks/{$taskId}/history")) {
            if (($body['next_history_page_token'] ?? null) !== 'regression-page-1') {
                throw new RuntimeException('Official replay consumer received an unexpected history page token.');
            }

            return [
                'history_events' => $this->pagedHistory,
                'next_history_page_token' => '',
            ];
        }

        if (str_ends_with($uri, "/api/worker/workflow-tasks/{$taskId}/heartbeat")) {
            return [
                'task_id' => $taskId,
                'workflow_task_attempt' => $this->task['workflow_task_attempt'],
                'lease_owner' => $this->task['lease_owner'],
                'renewed' => true,
                'reason' => null,
            ];
        }

        if (str_ends_with($uri, "/api/worker/workflow-tasks/{$taskId}/complete")) {
            $commands = $body['commands'] ?? null;
            if (!is_array($commands) || !array_is_list($commands)) {
                throw new RuntimeException('Official replay consumer received invalid workflow commands.');
            }
            /** @var list<array<string, mixed>> $commands */
            $this->completedCommands = $commands;

            return ['completed' => true];
        }

        if (str_ends_with($uri, "/api/worker/workflow-tasks/{$taskId}/fail")) {
            throw new RuntimeException('Official replay consumer reported a workflow task failure.');
        }

        if (str_ends_with($uri, '/api/worker/activity-tasks/poll')) {
            return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
        }

        throw new RuntimeException("Official replay consumer received an unexpected {$method} request.");
    }

    /** @return list<array<string, mixed>> */
    public function completedCommands(): array
    {
        if ($this->completedCommands === null) {
            throw new RuntimeException('Official replay consumer did not complete the workflow task.');
        }

        return $this->completedCommands;
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

        $layouts = $history === [] ? ['inline'] : ['inline', 'paginated'];
        foreach ($layouts as $layout) {
            self::executeLayout(
                $identity,
                $workflowType,
                $input,
                $history,
                $fixture,
                $layout,
            );
        }
    }

    /**
     * @param list<mixed> $input
     * @param list<array<string, mixed>> $history
     * @param array<string, mixed> $fixture
     */
    private static function executeLayout(
        string $identity,
        string $workflowType,
        array $input,
        array $history,
        array $fixture,
        string $layout,
    ): void {
        $codec = new AvroPayloadCodec();
        $task = [
            'task_id' => "regression-{$layout}",
            'workflow_task_attempt' => 1,
            'lease_owner' => 'regression-consumer',
            'workflow_id' => 'regression-workflow',
            'run_id' => "regression-{$layout}",
            'workflow_type' => $workflowType,
            'arguments' => $codec->envelope($input),
            'history_events' => $layout === 'inline' ? $history : [],
            'next_history_page_token' => $layout === 'paginated' ? 'regression-page-1' : '',
        ];
        $transport = new ReplayRegressionTransport(
            $task,
            $layout === 'paginated' ? $history : [],
        );
        $worker = new Worker(
            new Client('https://replay.invalid', transport: $transport, codec: $codec),
            'regression-corpus',
            workerId: 'regression-consumer',
        );
        $worker->registerWorkflow($workflowType, self::workflow($workflowType));
        $worker->tick(0);

        $commands = array_map(
            static fn (array $command): array => self::decodeEnvelopes($command, $codec),
            $transport->completedCommands(),
        );
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
    }

    /** @return callable(WorkflowContext, mixed ...$input): mixed */
    private static function workflow(string $workflowType): callable
    {
        if ($workflowType !== 'golden.single-activity') {
            throw new RuntimeException(
                "Replay fixture workflow {$workflowType} has no PHP implementation in the official consumer.",
            );
        }

        return static function (WorkflowContext $context, mixed $name): Generator {
            return yield $context->activity('golden.greet', [$name]);
        };
    }

    /**
     * @param array<string, mixed> $value
     * @return array<string, mixed>
     */
    private static function decodeEnvelopes(array $value, AvroPayloadCodec $codec): array
    {
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
