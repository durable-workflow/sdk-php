<?php

declare(strict_types=1);

/*
 * Embedded v2 to service mode must not change the supported Laravel boundary.
 *
 * Usage: php check-laravel-transition-authority.php <sdk laravel-adoption-contract.json> <durable-workflow/workflow package directory>
 *
 * The SDK's published service-mode authority must equal the installed Workflow
 * package's embedded v2 authority: the same manifest identity, the exact same
 * Laravel and PHP constraints (patched floors included, not only majors), and
 * the same Laravel/PHP cells. Each qualification cell must name the floor that
 * the constraint publishes for its Laravel major.
 */

/** @return array<string, mixed> */
function readJsonDocument(string $path): array
{
    $contents = @file_get_contents($path);
    if ($contents === false) {
        throw new RuntimeException('Cannot read '.$path.'.');
    }
    $document = json_decode($contents, true, 512, JSON_THROW_ON_ERROR);
    if (!is_array($document)) {
        throw new RuntimeException($path.' is not a JSON object.');
    }

    return $document;
}

/**
 * @param array<string, mixed> $service
 * @param array<string, mixed> $embedded
 * @return list<string>
 */
function laravelTransitionAuthorityErrors(array $service, array $embedded): array
{
    $errors = [];
    $reference = $service['embedded_transition_authority'] ?? [];
    if (($embedded['schema'] ?? null) !== ($reference['schema'] ?? false)
        || ($embedded['version'] ?? null) !== ($reference['version'] ?? false)
        || ($reference['package'] ?? null) !== 'durable-workflow/workflow') {
        $errors[] = 'The service adoption contract did not consume the installed embedded transition authority.';
    }

    $framework = $service['framework'] ?? [];
    $authority = $embedded['supported_intersection']['authority']['embedded_v2'] ?? [];
    if (!is_string($framework['constraint'] ?? null) || $framework['constraint'] !== ($authority['laravel'] ?? null)) {
        $errors[] = 'Embedded and service modes publish different Laravel constraints.';
    }
    if (!is_string($framework['php_constraint'] ?? null) || $framework['php_constraint'] !== ($authority['php'] ?? null)) {
        $errors[] = 'Embedded and service modes publish different PHP constraints.';
    }

    $matrix = is_array($framework['qualification_matrix'] ?? null) ? $framework['qualification_matrix'] : [];
    $embeddedCells = array_map(
        static fn (array $cell): array => ['laravel' => rtrim((string) $cell['laravel'], '.*'), 'php' => $cell['php']],
        $embedded['supported_intersection']['cells'] ?? [],
    );
    $serviceCells = array_map(
        static fn (array $cell): array => ['laravel' => $cell['laravel'] ?? null, 'php' => $cell['php'] ?? null],
        $matrix,
    );
    if ($embeddedCells !== $serviceCells) {
        $errors[] = 'Embedded and service modes publish different Laravel/PHP support cells.';
    }

    $floors = [];
    foreach (explode('|', is_string($framework['constraint'] ?? null) ? $framework['constraint'] : '') as $range) {
        if (preg_match('/\A\^([1-9][0-9]*)\.[0-9]+\.[0-9]+\z/', $range, $match) !== 1) {
            $errors[] = 'The service Laravel constraint must name an exact patched floor for each major: '.$range;
            continue;
        }
        $floors[$match[1]] = $range;
    }
    foreach ($matrix as $cell) {
        $floor = $floors[$cell['laravel'] ?? ''] ?? null;
        if ($floor === null || ($cell['package'] ?? null) !== 'laravel/framework:'.$floor) {
            $errors[] = sprintf('Laravel %s on PHP %s does not qualify the published floor.',
                (string) ($cell['laravel'] ?? '?'), (string) ($cell['php'] ?? '?'));
        }
    }

    return array_values(array_unique($errors));
}

if (realpath($_SERVER['SCRIPT_FILENAME'] ?? '') === __FILE__) {
    if ($argc !== 3) {
        fwrite(STDERR, "Usage: php check-laravel-transition-authority.php <adoption contract> <workflow package directory>\n");
        exit(2);
    }
    try {
        $service = readJsonDocument($argv[1]);
        $manifest = $service['embedded_transition_authority']['manifest'] ?? null;
        if (!is_string($manifest) || $manifest === '' || str_contains($manifest, '..')) {
            throw new RuntimeException('The service adoption contract names no embedded transition manifest.');
        }
        $errors = laravelTransitionAuthorityErrors($service, readJsonDocument(rtrim($argv[2], '/').'/'.$manifest));
    } catch (Throwable $error) {
        $errors = [$error->getMessage()];
    }
    foreach ($errors as $error) {
        fwrite(STDERR, $error."\n");
    }
    exit($errors === [] ? 0 : 1);
}
