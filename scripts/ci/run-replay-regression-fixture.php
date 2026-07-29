#!/usr/bin/env php
<?php

declare(strict_types=1);

use Composer\Autoload\ClassLoader;
use DurableWorkflow\Tests\Support\ReplayRegressionFixture;

try {
    $options = getopt('', ['autoload:', 'source-root:', 'fixture:']);
    if (!is_array($options)) {
        throw new RuntimeException('Unable to parse replay runner arguments.');
    }

    $autoload = realpath((string) ($options['autoload'] ?? ''));
    $sourceRoot = realpath((string) ($options['source-root'] ?? ''));
    $fixture = realpath((string) ($options['fixture'] ?? ''));
    if ($autoload === false || $sourceRoot === false || $fixture === false) {
        throw new RuntimeException(
            'Replay runner requires existing --autoload, --source-root, and --fixture paths.',
        );
    }
    $source = realpath($sourceRoot.'/src');
    if ($source === false) {
        throw new RuntimeException("Replay source tree {$sourceRoot}/src is missing.");
    }

    $loader = require $autoload;
    if (!$loader instanceof ClassLoader) {
        throw new RuntimeException("Composer autoload {$autoload} did not return a class loader.");
    }
    $loader->setPsr4('DurableWorkflow\\', [$source]);

    ReplayRegressionFixture::executeFile($fixture);
} catch (Throwable $exception) {
    fwrite(STDERR, $exception::class.': '.$exception->getMessage().PHP_EOL);
    exit(1);
}
