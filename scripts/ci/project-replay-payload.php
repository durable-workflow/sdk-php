#!/usr/bin/env php
<?php

declare(strict_types=1);

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroBinaryValue;
use DurableWorkflow\Codec\AvroMapValue;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Transport\Transport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;

const PROJECTION_SCHEMA = 'durable-workflow.php-replay-payload-projection/v1';

/**
 * @return array<string, mixed>
 */
function projectReplayPayloadValue(mixed $value): array
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
                    projectReplayPayloadValue($pair[1]),
                ],
                $value->pairs,
            ),
        ];
    }
    if (is_array($value)) {
        if (array_is_list($value)) {
            return [
                'type' => 'list',
                'items' => array_map(
                    static fn (mixed $item): array => projectReplayPayloadValue($item),
                    $value,
                ),
            ];
        }

        $entries = [];
        foreach ($value as $key => $item) {
            $entries[] = [
                is_int($key)
                    ? ['type' => 'int', 'value' => (string) $key]
                    : ['type' => 'string', 'base64' => base64_encode($key)],
                projectReplayPayloadValue($item),
            ];
        }

        return ['type' => 'array', 'entries' => $entries];
    }

    throw new RuntimeException(sprintf(
        'Official PHP payload consumer returned unsupported value type %s.',
        get_debug_type($value),
    ));
}

function consumeReplayPayload(string $operation, mixed $value, AvroPayloadCodec $codec): mixed
{
    if ($operation === 'replayer-result') {
        $method = new ReflectionMethod(Replayer::class, 'decodeResult');

        return $method->invoke(new Replayer($codec), ['result' => $value]);
    }
    if ($operation === 'workflow-signal-arguments') {
        $context = new WorkflowContext(
            '',
            '',
            [[
                'event_type' => 'SignalReceived',
                'payload' => ['signal_name' => 'projection', 'value' => $value],
            ]],
            $codec,
        );

        return $context->signals('projection')[0] ?? null;
    }
    if ($operation === 'workflow-update-arguments') {
        $context = new WorkflowContext(
            '',
            '',
            [[
                'event_type' => 'UpdateAccepted',
                'payload' => [
                    'update_name' => 'projection',
                    'arguments' => $value,
                ],
            ]],
            $codec,
        );

        return $context->updates('projection')[0] ?? null;
    }
    if ($operation === 'worker-update-arguments') {
        $transport = new class implements Transport {
            public function send(
                string $method,
                string $uri,
                array $headers,
                ?array $body = null,
            ): ?array {
                throw new RuntimeException(
                    'Replay payload projection must not perform transport requests.',
                );
            }
        };
        $client = new Client(
            'https://projection.invalid',
            transport: $transport,
            codec: $codec,
        );
        $method = new ReflectionMethod(Worker::class, 'decodeArguments');

        return $method->invoke(new Worker($client, 'projection'), $value);
    }

    throw new RuntimeException("Unsupported replay payload projection operation {$operation}.");
}

try {
    $options = getopt('', ['vendor-root:', 'source-root:']);
    if (!is_array($options)) {
        throw new RuntimeException('Unable to parse replay payload projection arguments.');
    }

    $vendorRoot = realpath((string) ($options['vendor-root'] ?? ''));
    $sourceRoot = realpath((string) ($options['source-root'] ?? ''));
    if ($vendorRoot === false || $sourceRoot === false) {
        throw new RuntimeException(
            'Replay payload projection requires existing --vendor-root and --source-root paths.',
        );
    }
    $source = realpath($sourceRoot.'/src');
    $avroSource = realpath($vendorRoot.'/apache/avro/lang/php/lib');
    if ($source === false || $avroSource === false) {
        throw new RuntimeException('Replay payload projection source dependencies are missing.');
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

    $request = json_decode(
        stream_get_contents(STDIN),
        true,
        flags: JSON_THROW_ON_ERROR,
    );
    if (!is_array($request) || !array_key_exists('value', $request)) {
        throw new RuntimeException('Replay payload projection request must contain a value.');
    }
    $operation = $request['operation'] ?? null;
    if (!is_string($operation) || $operation === '') {
        throw new RuntimeException(
            'Replay payload projection request must contain a non-empty operation.',
        );
    }

    $projections = [];
    for ($attempt = 0; $attempt < 2; ++$attempt) {
        $codec = new AvroPayloadCodec();
        $projections[] = projectReplayPayloadValue(
            consumeReplayPayload($operation, $request['value'], $codec),
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
