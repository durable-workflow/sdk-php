#!/usr/bin/env php
<?php

declare(strict_types=1);

use DurableWorkflow\Tests\Support\CodecRegressionFixture;

try {
    $options = getopt('', ['vendor-root:', 'consumer-root:', 'source-root:', 'fixture:', 'format:']);
    if (!is_array($options)) {
        throw new RuntimeException('Unable to parse codec runner arguments.');
    }

    $vendorRoot = realpath((string) ($options['vendor-root'] ?? ''));
    $consumerRoot = realpath((string) ($options['consumer-root'] ?? ''));
    $sourceRoot = realpath((string) ($options['source-root'] ?? ''));
    $fixture = realpath((string) ($options['fixture'] ?? ''));
    $format = $options['format'] ?? null;
    if (
        $vendorRoot === false
        || $consumerRoot === false
        || $sourceRoot === false
        || $fixture === false
        || !is_string($format)
        || $format === ''
    ) {
        throw new RuntimeException(
            'Codec runner requires existing --vendor-root, --consumer-root, --source-root, '
            .'and --fixture paths plus --format.',
        );
    }
    $consumerTests = realpath($consumerRoot.'/tests');
    $source = realpath($sourceRoot.'/src');
    $avroSource = realpath($vendorRoot.'/apache/avro/lang/php/lib');
    if ($consumerTests === false || $source === false || $avroSource === false) {
        throw new RuntimeException('Codec runner source dependencies are missing.');
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
} catch (Throwable $exception) {
    fwrite(STDERR, $exception::class.': '.$exception->getMessage().PHP_EOL);
    exit(2);
}

try {
    CodecRegressionFixture::executeFile($format, $fixture);
} catch (Throwable $exception) {
    fwrite(STDERR, $exception::class.': '.$exception->getMessage().PHP_EOL);
    exit(1);
}
