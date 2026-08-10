<?php

declare(strict_types=1);

(static function (): void {
    $candidates = array_unique([
        __DIR__.'/vendor/autoload.php',
        dirname(__DIR__).'/vendor/autoload.php',
        dirname(__DIR__, 2).'/vendor/autoload.php',
        dirname(__DIR__, 3).'/autoload.php',
        getcwd().'/vendor/autoload.php',
    ]);

    foreach ($candidates as $candidate) {
        if (is_file($candidate)) {
            require $candidate;

            return;
        }
    }

    throw new RuntimeException(
        'Composer autoload.php was not found. Run the example from a Composer project or copy all three example files beside vendor/.',
    );
})();

function quickstartEnvironment(string $name): string
{
    $value = getenv($name);
    if (!is_string($value) || trim($value) === '') {
        throw new RuntimeException("Set the {$name} environment variable before running this example.");
    }

    return trim($value);
}
