<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests\Support;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroBinaryValue;
use DurableWorkflow\Codec\AvroMapValue;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\CodecException;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\WorkflowContext;
use InvalidArgumentException;
use RuntimeException;

final class CodecRegressionFixture
{
    public static function executeFile(string $format, string $path): void
    {
        $contents = file_get_contents($path);
        if ($contents === false) {
            throw new RuntimeException("Unable to read codec fixture {$path}.");
        }
        $fixture = json_decode($contents, true, flags: JSON_THROW_ON_ERROR);
        self::assertIsArray($fixture, $path);

        self::execute($format, $fixture, $path);
    }

    /** @param array<string, mixed> $fixture */
    public static function execute(string $format, array $fixture, string $path): void
    {
        $codec = new AvroPayloadCodec();

        match ($format) {
            'avro-value-golden-v1' => self::assertAvroGoldenFixture($codec, $fixture, $path),
            'codec-regression-v1' => self::assertCodecRegressionFixture($codec, $fixture, $path),
            default => throw new RuntimeException(
                "Codec fixture format {$format} has no official PHP consumer.",
            ),
        };
    }

    /** @param array<string, mixed> $value */
    public static function projectTaggedValue(array $value): mixed
    {
        return match ($value['type'] ?? null) {
            'null' => null,
            'boolean' => (bool) $value['value'],
            'long' => (int) $value['value'],
            'double' => (float) $value['value'],
            'bytes' => AvroBinaryValue::fromBytes(
                (string) base64_decode((string) $value['base64'], true),
            ),
            'string' => (string) $value['value'],
            'array' => array_map(
                self::projectTaggedValue(...),
                is_array($value['items'] ?? null) ? $value['items'] : [],
            ),
            'map' => AvroMapValue::fromPairs(array_map(
                static fn (array $entry): array => [
                    (string) $entry['key'],
                    self::projectTaggedValue($entry['value']),
                ],
                is_array($value['entries'] ?? null) ? $value['entries'] : [],
            )),
            default => throw new InvalidArgumentException('Unsupported tagged corpus value.'),
        };
    }

    /** @param array<string, mixed> $fixture */
    private static function assertCodecRegressionFixture(
        AvroPayloadCodec $codec,
        array $fixture,
        string $path,
    ): void {
        self::assertSame(
            'durable-workflow.codec-regression/v1',
            $fixture['fixture_schema'] ?? null,
        );
        self::assertContains('php', $fixture['bindings'] ?? []);
        self::assertSame(
            AvroPayloadCodec::VALUE_SCHEMA_FINGERPRINT_HEX,
            $fixture['protocol']['fingerprint'] ?? null,
        );

        $value = self::projectTaggedValue($fixture['value']);
        $wire = $fixture['framing']['wire_base64'] ?? null;
        $operation = $fixture['failure_policy']['operation'] ?? null;
        $error = $fixture['failure_policy']['error'] ?? null;
        $identity = is_string($fixture['id'] ?? null) ? $fixture['id'] : $path;

        if ($operation === 'round_trip') {
            self::assertIsString($wire);
            self::assertSame($wire, $codec->encode($value), $identity);
            $decoded = $codec->decode($wire);
            self::assertEquals($value, $decoded, $identity);
            self::assertSame($wire, $codec->encode($decoded), $identity);
        } else {
            try {
                if ($operation === 'decode_reject') {
                    self::assertIsString($wire);
                    $codec->decode($wire);
                } elseif ($operation === 'encode_reject') {
                    $codec->encode($value);
                } else {
                    throw new RuntimeException("Unsupported failure policy in {$path}.");
                }
                throw new CodecRegressionAssertionFailed("Expected {$identity} to be rejected.");
            } catch (CodecException $exception) {
                self::assertIsString($error);
                self::assertStringContainsString($error, $exception->getMessage());
            }
        }

        if (array_key_exists('task_transport', $fixture)) {
            self::assertTaskTransportCodecRejection($fixture, $path);
        }
    }

    /** @param array<string, mixed> $fixture */
    private static function assertTaskTransportCodecRejection(array $fixture, string $path): void
    {
        $transport = $fixture['task_transport'];
        self::assertIsArray($transport, $path);
        $paths = $transport['worker_paths'] ?? null;
        $cases = $transport['payload_codec_cases'] ?? null;
        $expectedError = $transport['expected_error'] ?? null;
        $forbiddenError = $transport['forbidden_error'] ?? null;
        self::assertIsArray($paths, $path);
        self::assertNotSame([], $paths, $path);
        self::assertIsArray($cases, $path);
        self::assertNotSame([], $cases, $path);
        self::assertIsString($expectedError);
        self::assertIsString($forbiddenError);

        foreach ($paths as $workerPath) {
            self::assertIsString($workerPath);
            foreach ($cases as $case) {
                self::assertIsArray($case);
                self::assertRejectedTaskCodecCase(
                    $workerPath,
                    $case,
                    $fixture,
                    $expectedError,
                    $forbiddenError,
                    $path,
                );
            }
        }
    }

    /**
     * @param array<string, mixed> $case
     * @param array<string, mixed> $fixture
     */
    private static function assertRejectedTaskCodecCase(
        string $workerPath,
        array $case,
        array $fixture,
        string $expectedError,
        string $forbiddenError,
        string $path,
    ): void {
        $identity = $fixture['id'] ?? $path;
        $caseName = $case['name'] ?? null;
        self::assertIsString($caseName);
        $present = $case['present'] ?? null;
        if (!is_bool($present) || ($present && !array_key_exists('value', $case))) {
            throw new RuntimeException("{$identity}.{$caseName} has an invalid task codec case.");
        }

        $handlerCalls = 0;
        $task = self::taskForCodecCase(
            $workerPath,
            $case,
            ['codec' => 'avro', 'blob' => $fixture['framing']['wire_base64'] ?? null],
        );
        $delivered = false;
        $fakeTransport = new FakeTransport(handler: static function (
            string $method,
            string $uri,
            array $headers,
            ?array $body,
        ) use ($workerPath, $task, &$delivered): ?array {
            foreach (['workflow', 'activity', 'query'] as $polledKind) {
                if (!str_ends_with($uri, "/{$polledKind}-tasks/poll")) {
                    continue;
                }
                $taskKind = $workerPath === 'update' ? 'workflow' : $workerPath;
                if (!$delivered && $polledKind === $taskKind) {
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

            throw new RuntimeException("Unexpected task codec probe request: {$method} {$uri}");
        });
        $worker = new Worker(
            new Client('https://codec-regression.invalid', transport: $fakeTransport),
            'codec-regression',
            workerId: 'codec-regression-consumer',
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
                static function (QueryContext $context, int $value) use (&$handlerCalls): int {
                    ++$handlerCalls;

                    return $value + 1;
                },
            )
            ->registerActivity(
                'codec.activity',
                static function (ActivityContext $context, mixed $input = null) use (&$handlerCalls): string {
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
        $worker->tick(0);

        $context = "{$identity}.{$workerPath}.{$caseName}";
        self::assertSame(0, $handlerCalls, $context);
        $failures = array_values(array_filter(
            $fakeTransport->requests,
            static fn (array $request): bool => str_ends_with($request['uri'], '/fail'),
        ));
        self::assertSame(1, count($failures), $context);
        $failure = json_encode($failures[0]['body'], JSON_THROW_ON_ERROR);
        self::assertStringContainsString($expectedError, $failure, $context);
        if (str_contains($failure, $forbiddenError)) {
            throw new CodecRegressionAssertionFailed(
                self::failureMessage("string contains forbidden {$forbiddenError}", $context),
            );
        }
        self::assertSame([], array_values(array_filter(
            $fakeTransport->requests,
            static fn (array $request): bool => str_ends_with($request['uri'], '/complete'),
        )), $context);
    }

    /**
     * @param array<string, mixed> $case
     * @param array<string, mixed> $arguments
     * @return array<string, mixed>
     */
    private static function taskForCodecCase(string $path, array $case, array $arguments): array
    {
        $task = match ($path) {
            'workflow' => [
                'task_id' => 'workflow-codec-task',
                'workflow_task_attempt' => 1,
                'lease_owner' => 'codec-regression-consumer',
                'workflow_id' => 'workflow-codec',
                'run_id' => 'workflow-codec-run',
                'workflow_type' => 'codec.workflow',
                'arguments' => $arguments,
                'history_events' => [],
            ],
            'update' => [
                'task_id' => 'update-codec-task',
                'workflow_task_attempt' => 1,
                'lease_owner' => 'codec-regression-consumer',
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
                'lease_owner' => 'codec-regression-consumer',
                'activity_type' => 'codec.activity',
                'arguments' => $arguments,
            ],
            'query' => [
                'query_task_id' => 'query-codec-task',
                'query_task_attempt' => 1,
                'lease_owner' => 'codec-regression-consumer',
                'workflow_id' => 'query-codec',
                'run_id' => 'query-codec-run',
                'workflow_type' => 'codec.workflow',
                'query_name' => 'status',
                'query_arguments' => $arguments,
                'history_events' => [],
            ],
            default => throw new RuntimeException("Unsupported worker path {$path}."),
        };
        if ($case['present'] === true) {
            $task['payload_codec'] = $case['value'];
        }

        return $task;
    }

    /** @param array<string, mixed> $fixture */
    private static function assertAvroGoldenFixture(
        AvroPayloadCodec $codec,
        array $fixture,
        string $path,
    ): void {
        self::assertSame(
            AvroPayloadCodec::VALUE_SCHEMA_FINGERPRINT_HEX,
            $fixture['fingerprint'] ?? null,
            $path,
        );

        $cases = $fixture['cases'] ?? null;
        self::assertIsArray($cases, $path);
        self::assertNotSame([], $cases, $path);
        foreach ($cases as $case) {
            self::assertIsArray($case);
            $wire = $case['wire_base64'] ?? null;
            self::assertIsString($wire);
            $identity = is_string($case['name'] ?? null) ? $case['name'] : $path;
            $decoded = $codec->decode($wire);
            self::assertSame($wire, $codec->encode($decoded), $identity);
        }

        $alternateMapOrders = $fixture['alternate_map_orders'] ?? null;
        self::assertIsArray($alternateMapOrders, $path);
        foreach ($alternateMapOrders as $case) {
            self::assertIsArray($case);
            $wires = $case['wire_base64'] ?? null;
            self::assertIsArray($wires);
            self::assertNotSame([], $wires);
            $decoded = array_map($codec->decode(...), $wires);
            foreach (array_slice($decoded, 1) as $value) {
                self::assertEquals($decoded[0], $value, $case['name'] ?? $path);
            }
        }

        $malformedFrames = $fixture['malformed_frames'] ?? null;
        self::assertIsArray($malformedFrames, $path);
        foreach ($malformedFrames as $case) {
            self::assertIsArray($case);
            $wire = $case['wire_base64'] ?? null;
            $error = $case['error'] ?? null;
            self::assertIsString($wire);
            self::assertIsString($error);
            try {
                $codec->decode($wire);
                throw new RuntimeException("Expected {$path} malformed frame to be rejected.");
            } catch (CodecException $exception) {
                self::assertStringContainsString($error, $exception->getMessage());
            }
        }
    }

    private static function assertSame(mixed $expected, mixed $actual, string $context = ''): void
    {
        if ($expected !== $actual) {
            throw new CodecRegressionAssertionFailed(
                self::failureMessage('values are not identical', $context),
            );
        }
    }

    private static function assertNotSame(mixed $expected, mixed $actual, string $context = ''): void
    {
        if ($expected === $actual) {
            throw new CodecRegressionAssertionFailed(
                self::failureMessage('values are identical', $context),
            );
        }
    }

    private static function assertEquals(mixed $expected, mixed $actual, string $context = ''): void
    {
        if ($expected != $actual) {
            throw new CodecRegressionAssertionFailed(
                self::failureMessage('values are not equal', $context),
            );
        }
    }

    /** @phpstan-assert array<mixed> $value */
    private static function assertIsArray(mixed $value, string $context = ''): void
    {
        if (!is_array($value)) {
            throw new CodecRegressionAssertionFailed(
                self::failureMessage('value is not an array', $context),
            );
        }
    }

    /** @phpstan-assert string $value */
    private static function assertIsString(mixed $value, string $context = ''): void
    {
        if (!is_string($value)) {
            throw new CodecRegressionAssertionFailed(
                self::failureMessage('value is not a string', $context),
            );
        }
    }

    private static function assertContains(mixed $needle, mixed $haystack, string $context = ''): void
    {
        if (!is_iterable($haystack)) {
            throw new RuntimeException(self::failureMessage('value is not iterable', $context));
        }
        foreach ($haystack as $value) {
            if ($needle === $value) {
                return;
            }
        }

        throw new CodecRegressionAssertionFailed(
            self::failureMessage('value does not contain the expected item', $context),
        );
    }

    private static function assertStringContainsString(
        string $needle,
        string $haystack,
        string $context = '',
    ): void {
        if (!str_contains($haystack, $needle)) {
            throw new CodecRegressionAssertionFailed(
                self::failureMessage("string does not contain {$needle}", $context),
            );
        }
    }

    private static function failureMessage(string $message, string $context): string
    {
        return $context === '' ? $message : "{$context}: {$message}";
    }
}
