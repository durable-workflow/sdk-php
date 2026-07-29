<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests\Support;

use DurableWorkflow\Codec\AvroBinaryValue;
use DurableWorkflow\Codec\AvroMapValue;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\CodecException;
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
    private static function taggedValue(array $value): mixed
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
                self::taggedValue(...),
                is_array($value['items'] ?? null) ? $value['items'] : [],
            ),
            'map' => AvroMapValue::fromPairs(array_map(
                static fn (array $entry): array => [
                    (string) $entry['key'],
                    self::taggedValue($entry['value']),
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

        $value = self::taggedValue($fixture['value']);
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

            return;
        }

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
