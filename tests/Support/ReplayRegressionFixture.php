<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests\Support;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use Generator;
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

        $codec = new AvroPayloadCodec();
        $result = (new Replayer($codec))->replay(
            self::workflow($workflowType),
            $history,
            $input,
            'regression-corpus',
        );
        $commands = array_map(
            static fn (array $command): array => self::decodeEnvelopes($command, $codec),
            $result->commands,
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

        return $commands;
    }

    /** @return callable(WorkflowContext, mixed ...$input): mixed */
    private static function workflow(string $workflowType): callable
    {
        if ($workflowType !== 'golden.single-activity') {
            throw new RuntimeException(
                "Replay fixture workflow {$workflowType} has no PHP implementation; "
                .'register its reproducer workflow in ReplayRegressionFixture.',
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
