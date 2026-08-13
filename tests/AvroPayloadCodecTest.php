<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use Apache\Avro\Datum\AvroIOBinaryDecoder;
use Apache\Avro\Datum\AvroIOBinaryEncoder;
use Apache\Avro\Datum\AvroIODatumWriter;
use Apache\Avro\Datum\AvroIOSchemaMatchException;
use Apache\Avro\IO\AvroStringIO;
use DurableWorkflow\Codec\AvroBinaryValue;
use DurableWorkflow\Codec\AvroMapValue;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Codec\ValueDatumReader;
use DurableWorkflow\Exception\CodecException;
use DurableWorkflow\Tests\Support\CodecRegressionFixture;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class AvroPayloadCodecTest extends TestCase
{
    private AvroPayloadCodec $codec;

    protected function setUp(): void
    {
        $this->codec = new AvroPayloadCodec();
    }

    public function test_json_tagged_payload_fails_closed_with_actionable_diagnostic(): void
    {
        $this->expectException(\DurableWorkflow\Exception\CodecException::class);
        $this->expectExceptionMessage('unsupported_payload_codec');
        $this->expectExceptionMessage('HTTP document transport');

        $this->codec->decodeEnvelope([
            'codec' => 'json',
            'blob' => '{"stale":true}',
        ]);
    }

    public function testCanonicalSchemaAndFingerprintMatchCheckedInAuthority(): void
    {
        self::assertSame(
            trim((string) file_get_contents(__DIR__ . '/../resources/protocol/durable_workflow.protocol.Value.v1.avsc')),
            AvroPayloadCodec::VALUE_SCHEMA_JSON,
        );
        self::assertSame('e2a33dff55802237', AvroPayloadCodec::VALUE_SCHEMA_FINGERPRINT_HEX);
    }

    public function testEveryNamedBranchMatchesCrossLanguageGoldenBytes(): void
    {
        $cases = [
            'null' => null,
            'boolean_false' => false,
            'boolean_true' => true,
            'long_min' => PHP_INT_MIN,
            'long_max' => PHP_INT_MAX,
            'long_7' => 7,
            'double_7' => 7.0,
            'negative_zero' => -0.0,
            'bytes_00ff' => AvroBinaryValue::fromBytes("\x00\xFF"),
            'string_utf8' => 'héllo',
            'array' => [null, true, 7, 7.0, AvroBinaryValue::fromBytes("\x00\xFF"), 'text'],
            'map' => ['a' => 1, 'b' => [false]],
            'map_empty' => AvroMapValue::fromPairs([]),
            'map_key_0' => AvroMapValue::fromPairs([['0', 'zero']]),
            'map_keys_0_1' => AvroMapValue::fromPairs([['0', 'zero'], ['1', 'one']]),
            'nested' => [
                'items' => [
                    ['enabled' => true],
                    AvroBinaryValue::fromBytes('bytes'),
                    -2.5,
                ],
            ],
        ];
        $fixture = json_decode(
            (string) file_get_contents(__DIR__ . '/../resources/protocol/avro-value-v1-golden.json'),
            true,
            flags: JSON_THROW_ON_ERROR,
        );
        $golden = array_column($fixture['cases'], 'wire_base64', 'name');

        foreach ($cases as $name => $value) {
            $blob = $this->codec->encode($value);
            self::assertSame($golden[$name], $blob, $name);
            $decoded = $this->codec->decode($blob);
            self::assertEquals($value, $decoded, $name);
            self::assertSame($blob, $this->codec->encode($decoded), $name);
        }
    }

    public function testSharedTrailingBytesFrameIsRejected(): void
    {
        $fixture = json_decode(
            (string) file_get_contents(__DIR__ . '/../resources/protocol/avro-value-v1-golden.json'),
            true,
            flags: JSON_THROW_ON_ERROR,
        );

        foreach ($fixture['malformed_frames'] as $case) {
            try {
                $this->codec->decode($case['wire_base64']);
                self::fail("Expected {$case['name']} to fail.");
            } catch (CodecException $exception) {
                self::assertStringContainsString($case['error'], $exception->getMessage());
            }
        }
    }

    public function testSyntacticallyInvalidBase64IsRejected(): void
    {
        $this->expectException(CodecException::class);
        $this->expectExceptionMessage('invalid_payload_framing');

        $this->codec->decode('%%%');
    }

    public function testSharedAlternateMapOrdersDecodeToTheSameNestedValue(): void
    {
        $fixture = json_decode(
            (string) file_get_contents(__DIR__ . '/../resources/protocol/avro-value-v1-golden.json'),
            true,
            flags: JSON_THROW_ON_ERROR,
        );
        $expected = [
            'outer' => [[
                'left' => 1,
                'right' => AvroBinaryValue::fromBytes('x'),
            ]],
            'tail' => 'done',
        ];

        foreach ($fixture['alternate_map_orders'][0]['wire_base64'] as $blob) {
            $decoded = $this->codec->decode($blob);
            self::assertEquals($expected, $decoded);
            self::assertEquals($expected, $this->codec->decode($this->codec->encode($decoded)));
        }
    }

    public function testEveryPolicySelectedCodecFixtureUsesTheOfficialBinding(): void
    {
        $fixtures = self::policySelectedCodecFixtures();
        self::assertNotSame([], $fixtures);

        foreach ($fixtures as ['format' => $format, 'path' => $path]) {
            $fixture = json_decode((string) file_get_contents($path), true, flags: JSON_THROW_ON_ERROR);
            self::assertIsArray($fixture);
            CodecRegressionFixture::execute($format, $fixture, $path);
        }
    }

    public function testASelectorOutsideTheStandardDirectoryCannotAddInertCodecEvidence(): void
    {
        $root = sys_get_temp_dir().'/sdk-php-codec-corpus-'.bin2hex(random_bytes(8));
        $directory = $root.'/selected-elsewhere';
        self::assertTrue(mkdir($directory, 0777, true));
        $path = $directory.'/invalid-wire.json';
        $fixture = [
            '$schema' => 'https://example.invalid/evidence-schema.json',
            'fixture_schema' => 'durable-workflow.codec-regression/v1',
            'id' => 'invalid-policy-selected-wire',
            'protocol' => [
                'codec' => 'avro',
                'schema' => 'durable_workflow.protocol.Value',
                'version' => '1',
                'fingerprint' => AvroPayloadCodec::VALUE_SCHEMA_FINGERPRINT_HEX,
            ],
            'bindings' => ['php'],
            'value' => ['type' => 'long', 'value' => '7'],
            'framing' => [
                'encoding' => 'avro-single-object',
                'wire_base64' => 'AA==',
            ],
            'failure_policy' => ['operation' => 'round_trip', 'error' => null],
        ];

        try {
            self::assertNotFalse(file_put_contents($path, json_encode($fixture, JSON_THROW_ON_ERROR)));
            $selected = self::codecFixturesFromPolicy($root, [
                'categories' => [
                    'codec' => [
                        'fixtures' => [[
                            'glob' => 'selected-elsewhere/*.json',
                            'format' => 'codec-regression-v1',
                        ]],
                    ],
                ],
            ]);
            self::assertSame([['format' => 'codec-regression-v1', 'path' => $path]], $selected);

            $selectedFixture = json_decode(
                (string) file_get_contents($selected[0]['path']),
                true,
                flags: JSON_THROW_ON_ERROR,
            );
            self::assertIsArray($selectedFixture);
            $failure = null;
            try {
                CodecRegressionFixture::execute(
                    $selected[0]['format'],
                    $selectedFixture,
                    $selected[0]['path'],
                );
            } catch (RuntimeException $exception) {
                $failure = $exception;
            }
            self::assertNotNull($failure, 'Invalid policy-selected Avro wire escaped the binding.');
            self::assertStringContainsString('invalid-policy-selected-wire', $failure->getMessage());
        } finally {
            if (is_file($path)) {
                unlink($path);
            }
            if (is_dir($directory)) {
                rmdir($directory);
            }
            if (is_dir($root)) {
                rmdir($root);
            }
        }
    }

    public function testExplicitMapsPreserveAmbiguousKeysAndReencodeAsMaps(): void
    {
        $cases = [
            AvroMapValue::fromPairs([]),
            AvroMapValue::fromPairs([['0', 'zero']]),
            AvroMapValue::fromPairs([['0', 'zero'], ['1', 'one']]),
        ];

        foreach ($cases as $value) {
            $blob = $this->codec->encode($value);
            $decoded = $this->codec->decode($blob);

            self::assertInstanceOf(AvroMapValue::class, $decoded);
            self::assertSame($value->pairs, $decoded->pairs);
            self::assertSame($blob, $this->codec->encode($decoded));
        }

        self::assertNotSame(
            $this->codec->encode(AvroMapValue::fromPairs([])),
            $this->codec->encode([]),
        );
        self::assertNotSame(
            $this->codec->encode(AvroMapValue::fromPairs([['0', 'zero'], ['1', 'one']])),
            $this->codec->encode(['zero', 'one']),
        );
    }

    public function testListsAndMapsAndPrimitiveKindsRemainDistinct(): void
    {
        $value = [
            'integer' => 7,
            'double' => 7.0,
            'text' => 'bytes',
            'bytes' => AvroBinaryValue::fromBytes('bytes'),
            'list' => ['zero', 'one'],
            'map' => ['zero' => 0, 'one' => 1],
            'boolean' => true,
        ];
        $decoded = $this->codec->decode($this->codec->encode($value));

        self::assertIsInt($decoded['integer']);
        self::assertIsFloat($decoded['double']);
        self::assertIsString($decoded['text']);
        self::assertInstanceOf(AvroBinaryValue::class, $decoded['bytes']);
        self::assertTrue(array_is_list($decoded['list']));
        self::assertFalse(array_is_list($decoded['map']));
        self::assertIsBool($decoded['boolean']);
    }

    public function testRejectsInvalidMapKeysNonFiniteFloatsAndBinaryStrings(): void
    {
        foreach (
            [
                'invalid_map_key' => [1 => 'one'],
                'non_finite_float' => INF,
                'invalid_utf8_string' => "\xFF",
            ] as $reason => $value
        ) {
            try {
                $this->codec->encode($value);
                self::fail("Expected {$reason}.");
            } catch (CodecException $exception) {
                self::assertStringContainsString($reason, $exception->getMessage());
            }
        }
    }

    public function testUnknownFingerprintAndPrereleaseWrapperFailWithoutFallback(): void
    {
        $bytes = base64_decode($this->codec->encode(null), true);
        $bytes[2] = chr(ord($bytes[2]) ^ 0xFF);

        try {
            $this->codec->decode(base64_encode($bytes));
            self::fail('Expected unknown fingerprint failure.');
        } catch (CodecException $exception) {
            self::assertStringContainsString('unsupported_payload_schema', $exception->getMessage());
        }

        $this->expectException(CodecException::class);
        $this->expectExceptionMessage('invalid_payload_framing');
        $this->codec->decode(base64_encode("\x00legacy-wrapper"));
    }

    public function testRecursiveReaderPathEmitsNoWarnings(): void
    {
        set_error_handler(static function (int $severity, string $message): never {
            throw new \ErrorException($message, 0, $severity);
        }, E_WARNING);
        try {
            self::assertSame(
                ['nested' => [['value' => true]]],
                $this->codec->decode($this->codec->encode(['nested' => [['value' => true]]])),
            );
        } finally {
            restore_error_handler();
        }
    }

    public function testAppendedNamedBranchResolvesOldDataAndOldReaderRejectsNewBranch(): void
    {
        $v1 = json_decode(AvroPayloadCodec::VALUE_SCHEMA_JSON, true, flags: JSON_THROW_ON_ERROR);
        $v2 = $v1;
        $v2['fields'][0]['type'][] = [
            'type' => 'record',
            'name' => 'TimestampValue',
            'fields' => [['name' => 'timestamp', 'type' => 'string']],
        ];
        $writerSchema = AvroPayloadCodec::parseSchema(json_encode($v1, JSON_THROW_ON_ERROR));
        $readerSchema = AvroPayloadCodec::parseSchema(json_encode($v2, JSON_THROW_ON_ERROR));

        $oldIo = new AvroStringIO();
        (new AvroIODatumWriter($writerSchema))->write(
            ['value' => ['long' => 7]],
            new AvroIOBinaryEncoder($oldIo),
        );
        self::assertSame(
            ['value' => ['long' => 7]],
            (new ValueDatumReader($writerSchema, $readerSchema))->read(
                new AvroIOBinaryDecoder(new AvroStringIO($oldIo->string())),
            ),
        );

        $newIo = new AvroStringIO();
        (new AvroIODatumWriter($readerSchema))->write(
            ['value' => ['timestamp' => '2026-07-28T00:00:00Z']],
            new AvroIOBinaryEncoder($newIo),
        );

        $this->expectException(AvroIOSchemaMatchException::class);
        (new ValueDatumReader($readerSchema, $writerSchema))->read(
            new AvroIOBinaryDecoder(new AvroStringIO($newIo->string())),
        );
    }

    /** @return list<array{format: string, path: string}> */
    private static function policySelectedCodecFixtures(): array
    {
        $root = dirname(__DIR__);
        $policy = json_decode(
            (string) file_get_contents($root.'/regression-corpus-policy.json'),
            true,
            flags: JSON_THROW_ON_ERROR,
        );
        if (!is_array($policy)) {
            throw new RuntimeException('Regression corpus policy must be a JSON object.');
        }

        return self::codecFixturesFromPolicy($root, $policy);
    }

    /**
     * @param array<string, mixed> $policy
     * @return list<array{format: string, path: string}>
     */
    private static function codecFixturesFromPolicy(string $root, array $policy): array
    {
        $fixtures = $policy['categories']['codec']['fixtures'] ?? null;
        if (!is_array($fixtures)) {
            throw new RuntimeException('Regression corpus policy must declare codec fixtures.');
        }

        $selected = [];
        foreach ($fixtures as $fixture) {
            if (!is_array($fixture)) {
                throw new RuntimeException('Codec fixture selector must be an object.');
            }
            $format = $fixture['format'] ?? null;
            if (!is_string($format) || !in_array(
                $format,
                ['avro-value-golden-v1', 'codec-regression-v1'],
                true,
            )) {
                throw new RuntimeException('Codec fixture selector has no official PHP consumer.');
            }
            $glob = $fixture['glob'] ?? null;
            if (!is_string($glob) || $glob === '') {
                throw new RuntimeException('Codec fixture selector must have a glob.');
            }
            if (preg_match(
                '/\A(?:[A-Za-z0-9._-]+\/)*(?:[A-Za-z0-9._-]+|\*)\.json\z/D',
                $glob,
            ) !== 1) {
                throw new RuntimeException(
                    "Codec fixture selector {$glob} is not portable to the official PHP consumer.",
                );
            }

            foreach (glob($root.'/'.$glob) ?: [] as $path) {
                if (isset($selected[$path])) {
                    throw new RuntimeException("Codec fixture {$path} is selected more than once.");
                }
                $selected[$path] = $format;
            }
        }
        ksort($selected);

        return array_map(
            static fn (string $format, string $path): array => [
                'format' => $format,
                'path' => $path,
            ],
            array_values($selected),
            array_keys($selected),
        );
    }
}
