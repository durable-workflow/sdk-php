<?php

declare(strict_types=1);

use Apache\Avro\Datum\AvroIOBinaryDecoder;
use Apache\Avro\Datum\AvroIOBinaryEncoder;
use Apache\Avro\Datum\AvroIODatumReader;
use Apache\Avro\Datum\AvroIODatumWriter;
use Apache\Avro\IO\AvroStringIO;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Codec\AvroBinaryValue;

require dirname(__DIR__) . '/vendor/autoload.php';

const ITERATIONS_DEFAULT = 500;
const OLD_SCHEMA = '{"type":"record","name":"Payload","namespace":"durable_workflow","fields":[{"name":"json","type":"string"},{"name":"version","type":"int","default":1}]}';

$corpusPath = dirname(__DIR__) . '/resources/protocol/avro-value-benchmark-v1.json';
$corpusBytes = file_get_contents($corpusPath);
if (! is_string($corpusBytes)) {
    throw new RuntimeException('Unable to read the shared Avro Value benchmark corpus.');
}
$corpus = json_decode($corpusBytes, true, flags: JSON_THROW_ON_ERROR);
if (! is_array($corpus) || ! is_array($corpus['value'] ?? null)) {
    throw new RuntimeException('Shared Avro Value benchmark corpus has an invalid shape.');
}
$jsonSample = $corpus['value'];
$adaptBytes = static function (mixed $value) use (&$adaptBytes): mixed {
    if (
        is_array($value)
        && array_keys($value) === ['$avro_bytes']
        && is_string($value['$avro_bytes'])
    ) {
        $bytes = base64_decode($value['$avro_bytes'], true);
        if (! is_string($bytes)) {
            throw new RuntimeException('Benchmark bytes adapter received invalid base64.');
        }

        return AvroBinaryValue::fromBytes($bytes);
    }
    if (is_array($value)) {
        return array_map($adaptBytes, $value);
    }

    return $value;
};
$sample = $adaptBytes($jsonSample);
$iterations = ITERATIONS_DEFAULT;
$enforce = false;
foreach (array_slice($argv, 1) as $index => $argument) {
    if ($argument === '--enforce') {
        $enforce = true;
    }
    if ($argument === '--iterations' && isset($argv[$index + 2])) {
        $iterations = max(1, (int) $argv[$index + 2]);
    }
}

$codec = new AvroPayloadCodec();
$json = json_encode(
    $jsonSample,
    JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION,
);
$oldSchema = AvroPayloadCodec::parseSchema(OLD_SCHEMA);

$oldEncode = static function () use ($jsonSample, $oldSchema): string {
    $io = new AvroStringIO();
    $io->write("\x00");
    (new AvroIODatumWriter($oldSchema))->write([
        'json' => json_encode(
            $jsonSample,
            JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION,
        ),
        'version' => 1,
    ], new AvroIOBinaryEncoder($io));

    return $io->string();
};
$oldPayload = $oldEncode();
$oldDecode = static function () use ($oldPayload, $oldSchema): mixed {
    $datum = (new AvroIODatumReader($oldSchema))->read(
        new AvroIOBinaryDecoder(new AvroStringIO(substr($oldPayload, 1))),
    );

    return json_decode((string) $datum['json'], true, flags: JSON_THROW_ON_ERROR);
};
$typedBlob = $codec->encode($sample);
$typedPayload = base64_decode($typedBlob, true);
if ($typedPayload === false) {
    throw new RuntimeException('Production codec returned invalid base64.');
}

$measure = static function (callable $operation) use ($iterations): float {
    $samples = [];
    for ($pass = 0; $pass < 5; $pass++) {
        $started = hrtime(true);
        for ($index = 0; $index < $iterations; $index++) {
            $operation();
        }
        $samples[] = (hrtime(true) - $started) / $iterations / 1_000;
    }
    sort($samples);

    return $samples[2];
};
$httpSize = static fn (string $codecName, string $blob): int => strlen(json_encode(
    ['codec' => $codecName, 'blob' => $blob],
    JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES,
));

$results = [
    'implementation' => 'apache/avro',
    'corpus' => [
        'schema' => $corpus['schema'] ?? null,
        'case' => $corpus['case'] ?? null,
        'sha256' => hash('sha256', $corpusBytes),
    ],
    'iterations' => $iterations,
    'sizes_bytes' => [
        'plain_json' => [
            'raw' => strlen($json),
            'http_envelope' => $httpSize('json', $json),
        ],
        'old_json_wrapper' => [
            'raw_datum' => strlen($oldPayload) - 1,
            'framed' => strlen($oldPayload),
            'http_envelope' => $httpSize('avro', base64_encode($oldPayload)),
        ],
        'fixed_typed_value' => [
            'raw_datum' => strlen($typedPayload) - 10,
            'single_object' => strlen($typedPayload),
            'http_envelope' => $httpSize('avro', $typedBlob),
        ],
    ],
    'latency_us' => [
        'plain_json_encode' => $measure(static fn (): string => json_encode(
            $jsonSample,
            JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION,
        )),
        'plain_json_decode' => $measure(
            static fn (): mixed => json_decode($json, true, flags: JSON_THROW_ON_ERROR),
        ),
        'old_json_wrapper_encode' => $measure($oldEncode),
        'old_json_wrapper_decode' => $measure($oldDecode),
        'fixed_typed_value_encode' => $measure(static fn (): string => $codec->encode($sample)),
        'fixed_typed_value_decode' => $measure(static fn (): mixed => $codec->decode($typedBlob)),
    ],
];

echo json_encode($results, JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) . PHP_EOL;

if (! $enforce) {
    exit(0);
}

$encodeBudget = (float) (getenv('AVRO_VALUE_ENCODE_BUDGET_US') ?: 450);
$decodeBudget = (float) (getenv('AVRO_VALUE_DECODE_BUDGET_US') ?: 250);
if (
    $results['latency_us']['fixed_typed_value_encode'] > $encodeBudget
    || $results['latency_us']['fixed_typed_value_decode'] > $decodeBudget
) {
    fwrite(
        STDERR,
        sprintf(
            "Avro Value production-path regression budget exceeded: encode <= %g us, decode <= %g us.\n",
            $encodeBudget,
            $decodeBudget,
        ),
    );
    exit(1);
}
