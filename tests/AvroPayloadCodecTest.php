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
use PHPUnit\Framework\TestCase;

final class AvroPayloadCodecTest extends TestCase
{
    private AvroPayloadCodec $codec;

    protected function setUp(): void
    {
        $this->codec = new AvroPayloadCodec();
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
}
