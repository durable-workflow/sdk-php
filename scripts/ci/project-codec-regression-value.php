#!/usr/bin/env php
<?php

declare(strict_types=1);

use DurableWorkflow\Codec\AvroBinaryValue;
use DurableWorkflow\Codec\AvroMapValue;
use DurableWorkflow\Tests\Support\CodecRegressionFixture;

const PROJECTION_SCHEMA = 'durable-workflow.php-codec-value-projection/v1';

/**
 * Preserve the PHP types and byte values presented to AvroPayloadCodec::encode().
 *
 * @return array<string, mixed>
 */
function projectCodecValue(mixed $value): array
{
    if ($value === null) {
        return ['type' => 'null'];
    }
    if (is_bool($value)) {
        return ['type' => 'bool', 'value' => $value];
    }
    if (is_int($value)) {
        return ['type' => 'int', 'value' => (string) $value];
    }
    if (is_float($value)) {
        return ['type' => 'float64', 'bits' => bin2hex(pack('E', $value))];
    }
    if (is_string($value)) {
        return ['type' => 'string', 'base64' => base64_encode($value)];
    }
    if ($value instanceof AvroBinaryValue) {
        return ['type' => 'avro-bytes', 'base64' => base64_encode($value->bytes)];
    }
    if ($value instanceof AvroMapValue) {
        return [
            'type' => 'avro-map',
            'entries' => array_map(
                static fn (array $pair): array => [
                    base64_encode($pair[0]),
                    projectCodecValue($pair[1]),
                ],
                $value->pairs,
            ),
        ];
    }
    if (is_array($value) && array_is_list($value)) {
        return [
            'type' => 'list',
            'items' => array_map(
                static fn (mixed $item): array => projectCodecValue($item),
                $value,
            ),
        ];
    }

    throw new RuntimeException(sprintf(
        'Official PHP codec consumer returned unsupported value type %s.',
        get_debug_type($value),
    ));
}

try {
    $options = getopt('', ['vendor-root:', 'consumer-root:', 'source-root:']);
    if (!is_array($options)) {
        throw new RuntimeException('Unable to parse codec value projection arguments.');
    }

    $vendorRoot = realpath((string) ($options['vendor-root'] ?? ''));
    $consumerRoot = realpath((string) ($options['consumer-root'] ?? ''));
    $sourceRoot = realpath((string) ($options['source-root'] ?? ''));
    if ($vendorRoot === false || $consumerRoot === false || $sourceRoot === false) {
        throw new RuntimeException(
            'Codec value projection requires existing --vendor-root, --consumer-root, '
            .'and --source-root paths.',
        );
    }
    $consumerTests = realpath($consumerRoot.'/tests');
    $source = realpath($sourceRoot.'/src');
    $avroSource = realpath($vendorRoot.'/apache/avro/lang/php/lib');
    if ($consumerTests === false || $source === false || $avroSource === false) {
        throw new RuntimeException('Codec value projection source dependencies are missing.');
    }

    $prefixes = [
        'DurableWorkflow\\Tests\\' => $consumerTests,
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

    $request = json_decode(
        stream_get_contents(STDIN),
        true,
        flags: JSON_THROW_ON_ERROR,
    );
    if (!is_array($request) || !is_array($request['value'] ?? null)) {
        throw new RuntimeException(
            'Codec value projection request must contain a tagged value object.',
        );
    }

    $projections = [];
    for ($attempt = 0; $attempt < 2; ++$attempt) {
        $projections[] = projectCodecValue(
            CodecRegressionFixture::projectTaggedValue($request['value']),
        );
    }
    echo json_encode(
        ['schema' => PROJECTION_SCHEMA, 'projections' => $projections],
        JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES,
    ).PHP_EOL;
} catch (Throwable $exception) {
    fwrite(STDERR, $exception::class.': '.$exception->getMessage().PHP_EOL);
    exit(1);
}
