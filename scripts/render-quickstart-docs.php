<?php

declare(strict_types=1);

$repository = dirname(__DIR__);
$buildDirectory = $argv[1] ?? $repository.'/build/api';
$indexPath = rtrim($buildDirectory, '/').'/index.html';
$contractPath = $repository.'/docs/quickstart-contract.json';

$index = file_get_contents($indexPath);
$contractSource = file_get_contents($contractPath);
if ($index === false || $contractSource === false) {
    throw new RuntimeException('Rendered documentation or quickstart contract is unavailable.');
}

$contract = json_decode($contractSource, true, flags: JSON_THROW_ON_ERROR);
if (!is_array($contract) || ($contract['schema_version'] ?? null) !== 1) {
    throw new RuntimeException('Unsupported quickstart contract.');
}

/** @return string */
$escape = static fn (string $value): string => htmlspecialchars(
    $value,
    ENT_QUOTES | ENT_SUBSTITUTE | ENT_HTML5,
    'UTF-8',
);

$source = static function (string $relativePath) use ($repository): string {
    $contents = file_get_contents($repository.'/'.$relativePath);
    if ($contents === false) {
        throw new RuntimeException("Quickstart source {$relativePath} is unavailable.");
    }

    return $contents;
};

$expectedResult = json_encode(
    ['workflow_id' => 'php-quickstart-…', ...$contract['expected_result']],
    JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE,
);

$replacements = [
    '__QUICKSTART_COMPOSER_REQUIREMENT__' => $escape($contract['package']['composer_requirement']),
    '__QUICKSTART_SERVER_URI__' => $escape($contract['runtime_targets']['server']['client_input']),
    '__QUICKSTART_SERVER_NAMESPACE__' => $escape($contract['runtime_targets']['server']['namespace_input']),
    '__QUICKSTART_CLOUD_URI__' => $escape($contract['runtime_targets']['cloud']['client_input']),
    '__QUICKSTART_CLOUD_NAMESPACE__' => $escape($contract['runtime_targets']['cloud']['namespace_input']),
    '__QUICKSTART_BOOTSTRAP_SOURCE__' => $escape($source($contract['sources']['bootstrap'])),
    '__QUICKSTART_WORKER_SOURCE__' => $escape($source($contract['sources']['worker'])),
    '__QUICKSTART_CLIENT_SOURCE__' => $escape($source($contract['sources']['client'])),
    '__QUICKSTART_EXPECTED_RESULT__' => $escape($expectedResult),
];

foreach ($replacements as $placeholder => $replacement) {
    if (substr_count($index, $placeholder) !== 1) {
        throw new RuntimeException("Rendered quickstart must contain one {$placeholder} placeholder.");
    }
    $index = str_replace($placeholder, $replacement, $index);
}

if (preg_match('/__QUICKSTART_[A-Z_]+__/', $index) === 1) {
    throw new RuntimeException('Rendered quickstart contains an unresolved placeholder.');
}
if (file_put_contents($indexPath, $index) === false) {
    throw new RuntimeException('Could not write the rendered quickstart.');
}
if (file_put_contents(rtrim($buildDirectory, '/').'/quickstart-contract.json', $contractSource) === false) {
    throw new RuntimeException('Could not publish the machine-readable quickstart contract.');
}

fwrite(STDOUT, "Rendered the executable PHP quickstart at the documentation root.\n");
