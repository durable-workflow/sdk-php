<?php

declare(strict_types=1);

namespace DurableWorkflow\Codec;

use Apache\Avro\Datum\AvroIOBinaryEncoder;
use Apache\Avro\IO\AvroStringIO;
use Apache\Avro\Schema\AvroSchema;
use DurableWorkflow\Exception\CodecException;
use InvalidArgumentException;
use Throwable;
use UnderflowException;

/** Fixed durable_workflow.protocol.Value codec using Avro single-object encoding. */
final class AvroPayloadCodec implements PayloadCodec
{
    public const SINGLE_OBJECT_MAGIC = "\xC3\x01";
    public const VALUE_SCHEMA_FINGERPRINT_HEX = 'e2a33dff55802237';
    public const VALUE_SCHEMA_FINGERPRINT = "\xE2\xA3\x3D\xFF\x55\x80\x22\x37";
    public const VALUE_SCHEMA_JSON = '{"type":"record","name":"Value","namespace":"durable_workflow.protocol","fields":[{"name":"value","type":["null",{"type":"record","name":"BooleanValue","fields":[{"name":"boolean","type":"boolean"}]},{"type":"record","name":"LongValue","fields":[{"name":"long","type":"long"}]},{"type":"record","name":"DoubleValue","fields":[{"name":"double","type":"double"}]},{"type":"record","name":"BytesValue","fields":[{"name":"bytes","type":"bytes"}]},{"type":"record","name":"StringValue","fields":[{"name":"string","type":"string"}]},{"type":"record","name":"ArrayValue","fields":[{"name":"items","type":{"type":"array","items":"Value"}}]},{"type":"record","name":"MapValue","fields":[{"name":"entries","type":{"type":"map","values":"Value"}}]}]}]}';

    private static ?AvroSchema $valueSchema = null;

    public function name(): string
    {
        return 'avro';
    }

    public static function parseSchema(string $schemaJson): AvroSchema
    {
        return self::withoutApacheDeprecations(
            static fn (): AvroSchema => AvroSchema::parse($schemaJson),
        );
    }

    public function encode(mixed $value): string
    {
        try {
            return self::withoutApacheDeprecations(static function () use ($value): string {
                $io = new AvroStringIO();
                $io->write(self::SINGLE_OBJECT_MAGIC . self::VALUE_SCHEMA_FINGERPRINT);
                (new ValueDatumWriter())->write(
                    self::toDatum($value),
                    new AvroIOBinaryEncoder($io),
                );

                return base64_encode($io->string());
            });
        } catch (CodecException $exception) {
            throw $exception;
        } catch (Throwable $exception) {
            throw new CodecException('avro_value_encode_failed: ' . $exception->getMessage(), $exception);
        }
    }

    public function decode(string $blob): mixed
    {
        $bytes = base64_decode($blob, true);
        if ($bytes !== false && self::looksLikeJsonDocument($bytes)) {
            throw new CodecException(
                'unsupported_payload_codec: raw JSON workflow payloads are not supported by Durable Workflow 2.0; use codec="avro" with the fixed Avro Value schema and single-object framing. JSON remains the HTTP document transport, not a workflow payload codec.',
            );
        }
        if ($bytes === false || strlen($bytes) < 10 || ! str_starts_with($bytes, self::SINGLE_OBJECT_MAGIC)) {
            throw new CodecException(
                'invalid_payload_framing: expected strict base64 Avro single-object bytes beginning c301.',
            );
        }

        $fingerprint = substr($bytes, 2, 8);
        if ($fingerprint !== self::VALUE_SCHEMA_FINGERPRINT) {
            throw new CodecException(sprintf(
                'unsupported_payload_schema: unknown CRC-64-AVRO fingerprint %s.',
                bin2hex($fingerprint),
            ));
        }

        try {
            return self::withoutApacheDeprecations(static function () use ($bytes): mixed {
                $payloadIo = new AvroStringIO(substr($bytes, 10));
                $reader = new ValueDatumReader(self::valueSchema(), self::valueSchema());
                $datum = $reader->read(new ValueDatumDecoder($payloadIo));
                if (! $payloadIo->isEof()) {
                    throw new CodecException(
                        'invalid_payload_framing: trailing bytes after Avro Value datum.',
                    );
                }

                return self::fromDatum($datum);
            });
        } catch (CodecException $exception) {
            throw $exception;
        } catch (UnderflowException $exception) {
            throw new CodecException(
                'invalid_payload_framing: truncated Avro Value datum.',
                $exception,
            );
        } catch (Throwable $exception) {
            throw new CodecException(
                'invalid_payload_framing: malformed Avro Value datum: ' . $exception->getMessage(),
                $exception,
            );
        }
    }

    private static function looksLikeJsonDocument(string $bytes): bool
    {
        $document = trim($bytes);
        if ($document === '') {
            return false;
        }

        try {
            json_decode($document, true, flags: JSON_THROW_ON_ERROR);

            return true;
        } catch (Throwable) {
            return false;
        }
    }

    /** @return array{codec: string, blob: string} */
    public function envelope(mixed $value): array
    {
        return ['codec' => $this->name(), 'blob' => $this->encode($value)];
    }

    /** @param array{codec?: mixed, blob?: mixed}|string|null $envelope */
    public function decodeEnvelope(array|string|null $envelope): mixed
    {
        if ($envelope === null) {
            return null;
        }
        if (is_string($envelope)) {
            return $this->decode($envelope);
        }
        if (($envelope['codec'] ?? $this->name()) !== $this->name()) {
            throw new CodecException(
                'unsupported_payload_codec: Durable Workflow 2.0 accepts only codec="avro" with the fixed Avro Value schema and single-object framing. JSON remains the HTTP document transport, not a workflow payload codec.',
            );
        }
        if (! isset($envelope['blob']) || ! is_string($envelope['blob'])) {
            throw new CodecException('Avro payload envelope is missing its string blob field.');
        }

        return $this->decode($envelope['blob']);
    }

    /** @return array{value: mixed} */
    private static function toDatum(mixed $value): array
    {
        if ($value === null) {
            return ['value' => null];
        }
        if (is_bool($value)) {
            return ['value' => ['boolean' => $value]];
        }
        if (is_int($value)) {
            return ['value' => ['long' => $value]];
        }
        if (is_float($value)) {
            if (! is_finite($value)) {
                throw new InvalidArgumentException('non_finite_float: Avro Value doubles must be finite.');
            }

            return ['value' => ['double' => $value]];
        }
        if ($value instanceof AvroBinaryValue) {
            return ['value' => ['bytes' => $value->bytes]];
        }
        if ($value instanceof AvroMapValue) {
            return [
                'value' => [
                    'entries' => AvroMapValue::fromPairs(array_map(
                        static fn (array $pair): array => [$pair[0], self::toDatum($pair[1])],
                        $value->pairs,
                    )),
                ],
            ];
        }
        if (is_string($value)) {
            if (preg_match('//u', $value) !== 1) {
                throw new InvalidArgumentException(
                    'invalid_utf8_string: wrap binary strings with AvroBinaryValue::fromBytes().',
                );
            }

            return ['value' => ['string' => $value]];
        }
        if (is_array($value)) {
            if (array_is_list($value)) {
                return [
                    'value' => [
                        'items' => array_map(
                            static fn (mixed $item): array => self::toDatum($item),
                            $value,
                        ),
                    ],
                ];
            }

            $entries = [];
            foreach ($value as $key => $item) {
                if (! is_string($key)) {
                    throw new InvalidArgumentException(
                        'invalid_map_key: Avro Value maps require string keys; keys are never stringified.',
                    );
                }
                $entries[] = [$key, self::toDatum($item)];
            }

            return ['value' => ['entries' => AvroMapValue::fromPairs($entries)]];
        }

        throw new InvalidArgumentException(sprintf(
            'unsupported_value_type: adapt %s before Avro encoding.',
            get_debug_type($value),
        ));
    }

    private static function fromDatum(mixed $datum): mixed
    {
        if (! is_array($datum) || ! array_key_exists('value', $datum)) {
            throw new CodecException(
                'invalid_payload_framing: datum is not a durable_workflow.protocol.Value record.',
            );
        }

        $branch = $datum['value'];
        if ($branch === null) {
            return null;
        }
        if (! is_array($branch)) {
            throw new CodecException('invalid_payload_framing: invalid Value union branch.');
        }
        foreach (['boolean', 'long', 'double', 'bytes', 'string'] as $field) {
            if (array_key_exists($field, $branch)) {
                return $field === 'bytes'
                    ? AvroBinaryValue::fromBytes((string) $branch[$field])
                    : $branch[$field];
            }
        }
        if (array_key_exists('items', $branch) && is_array($branch['items'])) {
            return array_map(
                static fn (mixed $item): mixed => self::fromDatum($item),
                $branch['items'],
            );
        }
        if (array_key_exists('entries', $branch) && is_array($branch['entries'])) {
            $entries = [];
            $pairs = [];
            $requiresAdapter = $branch['entries'] === [];
            foreach ($branch['entries'] as $key => $item) {
                $decoded = self::fromDatum($item);
                $pairs[] = [(string) $key, $decoded];
                if (! is_string($key)) {
                    $requiresAdapter = true;
                } else {
                    $entries[$key] = $decoded;
                }
            }

            return $requiresAdapter
                ? AvroMapValue::fromPairs($pairs)
                : $entries;
        }

        throw new CodecException('invalid_payload_framing: unknown named Value branch.');
    }

    private static function valueSchema(): AvroSchema
    {
        return self::$valueSchema ??= self::parseSchema(self::VALUE_SCHEMA_JSON);
    }

    /**
     * Apache Avro 1.12 emits PHP deprecations for legacy numeric casts. Schema
     * resolution warnings are intentionally not suppressed.
     */
    private static function withoutApacheDeprecations(callable $operation): mixed
    {
        set_error_handler(static fn (): bool => true, E_DEPRECATED);
        try {
            return $operation();
        } finally {
            restore_error_handler();
        }
    }
}
