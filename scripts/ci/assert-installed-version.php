<?php

declare(strict_types=1);

/*
 * Prove that a qualification consumer runs exactly the release it claims.
 *
 * Usage: php assert-installed-version.php <project directory> <package> <exact x.y.z>
 *
 * Reads what Composer installed (vendor/composer/installed.json). For
 * laravel/framework it also reads the framework's own Application::VERSION,
 * so a caret range that resolved to a later release cannot pass as the floor.
 */

/** @return list<string> */
function installedVersionErrors(string $project, string $package, string $expected): array
{
    if (preg_match('/\A[0-9]+\.[0-9]+\.[0-9]+\z/', $expected) !== 1) {
        return ['The expected version must be one exact x.y.z release: '.$expected];
    }
    $path = rtrim($project, '/').'/vendor/composer/installed.json';
    $contents = @file_get_contents($path);
    if ($contents === false) {
        return ['Cannot read '.$path.'.'];
    }
    try {
        $installed = json_decode($contents, true, 512, JSON_THROW_ON_ERROR);
    } catch (JsonException $error) {
        return [$path.' is not valid JSON: '.$error->getMessage()];
    }
    $packages = is_array($installed) && array_key_exists('packages', $installed) ? $installed['packages'] : $installed;
    $matches = array_values(array_filter(is_array($packages) ? $packages : [],
        static fn (mixed $entry): bool => is_array($entry) && ($entry['name'] ?? null) === $package));
    if (count($matches) !== 1) {
        return [sprintf('%s is not installed exactly once in %s.', $package, $project)];
    }
    $version = (string) ($matches[0]['version'] ?? '');
    $errors = [];
    if (ltrim($version, 'v') !== $expected || ($matches[0]['version_normalized'] ?? null) !== $expected.'.0') {
        $errors[] = sprintf('%s resolved to %s, not exactly %s.', $package, $version === '' ? 'no version' : $version, $expected);
    }
    if ($package === 'laravel/framework') {
        $application = @file_get_contents(rtrim($project, '/')
            .'/vendor/laravel/framework/src/Illuminate/Foundation/Application.php');
        if ($application === false
            || preg_match("/const VERSION = '([^']+)';/", $application, $constant) !== 1) {
            $errors[] = 'The installed Laravel framework does not declare Application::VERSION.';
        } elseif ($constant[1] !== $expected) {
            $errors[] = sprintf('Laravel Application::VERSION is %s, not exactly %s.', $constant[1], $expected);
        }
    }

    return $errors;
}

if (realpath($_SERVER['SCRIPT_FILENAME'] ?? '') === __FILE__) {
    if ($argc !== 4) {
        fwrite(STDERR, "Usage: php assert-installed-version.php <project directory> <package> <exact x.y.z>\n");
        exit(2);
    }
    $errors = installedVersionErrors($argv[1], $argv[2], $argv[3]);
    foreach ($errors as $error) {
        fwrite(STDERR, $error."\n");
    }
    if ($errors === []) {
        fwrite(STDOUT, sprintf("%s is exactly %s.\n", $argv[2], $argv[3]));
    }
    exit($errors === [] ? 0 : 1);
}
