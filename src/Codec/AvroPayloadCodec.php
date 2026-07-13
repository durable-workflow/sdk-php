<?php

declare(strict_types=1);

namespace DurableWorkflow\Codec;

use Apache\Avro\Datum\AvroIOBinaryDecoder;
use Apache\Avro\Datum\AvroIOBinaryEncoder;
use Apache\Avro\Datum\AvroIODatumReader;
use Apache\Avro\Datum\AvroIODatumWriter;
use Apache\Avro\IO\AvroStringIO;
use Apache\Avro\Schema\AvroSchema;
use DurableWorkflow\Exception\CodecException;
use JsonException;
use Throwable;

/** Apache Avro implementation of the Durable Workflow payload framing. */
final class AvroPayloadCodec implements PayloadCodec
{
    public const GENERIC_PREFIX = "\x00";
    public const TYPED_PREFIX = "\x01";
    public const WRAPPER_SCHEMA_JSON = '{"type":"record","name":"Payload","namespace":"durable_workflow","fields":[{"name":"json","type":"string"},{"name":"version","type":"int","default":1}]}';

    private static ?AvroSchema $wrapperSchema = null;

    public function __construct(private readonly ?AvroSchema $schema = null)
    {
    }

    public function name(): string
    {
        return 'avro';
    }

    public static function parseSchema(string $schemaJson): AvroSchema
    {
        return self::withoutApacheDeprecations(static fn (): AvroSchema => AvroSchema::parse($schemaJson));
    }

    public function withSchema(AvroSchema|string $schema): self
    {
        return new self(is_string($schema) ? self::parseSchema($schema) : $schema);
    }

    public function encode(mixed $value): string
    {
        try {
            return self::withoutApacheDeprecations(function () use ($value): string {
                $io = new AvroStringIO();
                if ($this->schema === null) {
                    $io->write(self::GENERIC_PREFIX);
                    $writer = new AvroIODatumWriter(self::wrapperSchema());
                    $writer->write([
                        'json' => json_encode(
                            $value,
                            JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION,
                        ),
                        'version' => 1,
                    ], new AvroIOBinaryEncoder($io));

                    return base64_encode($io->string());
                }

                $schemaJson = self::schemaJson($this->schema);
                $io->write(self::TYPED_PREFIX);
                $io->write(pack('N', strlen($schemaJson)));
                $io->write($schemaJson);
                $writer = new AvroIODatumWriter($this->schema);
                $writer->write($value, new AvroIOBinaryEncoder($io));

                return base64_encode($io->string());
            });
        } catch (JsonException $exception) {
            throw new CodecException('The value is not JSON-safe for the Avro generic wrapper: '.$exception->getMessage(), $exception);
        } catch (CodecException $exception) {
            throw $exception;
        } catch (Throwable $exception) {
            throw new CodecException('Apache Avro encoding failed: '.$exception->getMessage(), $exception);
        }
    }

    public function decode(string $blob): mixed
    {
        if ($blob === '') {
            return null;
        }

        $bytes = base64_decode($blob, true);
        if ($bytes === false || $bytes === '') {
            throw new CodecException('Avro payload must be non-empty, strict base64-encoded binary data.');
        }

        try {
            return self::withoutApacheDeprecations(function () use ($bytes): mixed {
                $prefix = $bytes[0];
                if ($prefix === self::GENERIC_PREFIX) {
                    $reader = new AvroIODatumReader(self::wrapperSchema());
                    $record = $reader->read(new AvroIOBinaryDecoder(new AvroStringIO(substr($bytes, 1))));
                    if (!is_array($record) || !isset($record['json']) || !is_string($record['json'])) {
                        throw new CodecException('Avro generic wrapper did not contain a JSON string.');
                    }

                    return json_decode($record['json'], true, 512, JSON_THROW_ON_ERROR);
                }

                if ($prefix !== self::TYPED_PREFIX) {
                    throw new CodecException(sprintf('Unknown Avro framing prefix 0x%s.', bin2hex($prefix)));
                }

                [$writerSchema, $datum] = self::readTypedFrame($bytes);
                $reader = $this->schema === null
                    ? new AvroIODatumReader($writerSchema)
                    : new AvroIODatumReader($writerSchema, $this->schema);

                return $reader->read(new AvroIOBinaryDecoder(new AvroStringIO($datum)));
            });
        } catch (CodecException $exception) {
            throw $exception;
        } catch (Throwable $exception) {
            throw new CodecException('Apache Avro decoding failed: '.$exception->getMessage(), $exception);
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
            throw new CodecException('Expected an Avro payload envelope.');
        }
        if (!isset($envelope['blob']) || !is_string($envelope['blob'])) {
            throw new CodecException('Avro payload envelope is missing its string blob field.');
        }

        return $this->decode($envelope['blob']);
    }

    private static function wrapperSchema(): AvroSchema
    {
        return self::$wrapperSchema ??= self::parseSchema(self::WRAPPER_SCHEMA_JSON);
    }

    /** @return array{0: AvroSchema, 1: string} */
    private static function readTypedFrame(string $bytes): array
    {
        $body = substr($bytes, 1);
        if (strlen($body) < 4) {
            throw new CodecException('Typed Avro frame is missing its writer-schema length.');
        }
        $unpacked = unpack('Nlength', substr($body, 0, 4));
        $length = is_array($unpacked) ? (int) ($unpacked['length'] ?? 0) : 0;
        if ($length < 1 || strlen($body) < 4 + $length) {
            throw new CodecException('Typed Avro frame contains an invalid writer-schema length.');
        }
        $schemaJson = substr($body, 4, $length);

        return [self::parseSchema($schemaJson), substr($body, 4 + $length)];
    }

    private static function schemaJson(AvroSchema $schema): string
    {
        return json_encode(
            $schema->toAvro(),
            JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION,
        );
    }

    /**
     * @template T
     * @param callable(): T $operation
     * @return T
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
