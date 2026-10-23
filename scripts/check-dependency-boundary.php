<?php

declare(strict_types=1);

$manifest = json_decode((string) file_get_contents($argv[1] ?? 'composer.json'), true, flags: JSON_THROW_ON_ERROR);
$installed = json_decode((string) stream_get_contents(STDIN), true, flags: JSON_THROW_ON_ERROR);
$policy = json_decode((string) file_get_contents(__DIR__.'/../resources/dependency-boundary.json'), true, flags: JSON_THROW_ON_ERROR);
if (!is_array($installed['installed'] ?? null) || !is_array($manifest['require'] ?? null)) {
    fwrite(STDERR, "Production dependency inventory is missing or malformed.\n");
    exit(1);
}
$failures = [];
foreach ($policy['required_constraints'] as $name => $constraint) {
    if (str_replace(' ', '', $manifest['require'][$name] ?? '') !== $constraint) {
        $failures[] = "{$name} must retain reviewed compatibility and security floors {$constraint}.";
    }
}
$packages = [];
$installedNames = [];
foreach (array_keys($manifest['require']) as $name) {
    $packages[$name] = null;
}
foreach ($installed['installed'] as $package) {
    if (!is_string($package['name'] ?? null) || $package['name'] === ''
        || !is_string($package['version'] ?? null) || $package['version'] === '') {
        $failures[] = 'Installed production package lacks its name or version.';
        continue;
    }
    $packages[$package['name']] = $package['version'];
    $installedNames[] = $package['name'];
}
foreach (array_keys($policy['required_constraints']) as $name) {
    if (!in_array($name, $installedNames, true)) {
        $failures[] = "Required production package {$name} is absent from the installed inventory.";
    }
}
foreach ($packages as $name => $version) {
    if (str_starts_with($name, 'laravel/') || str_starts_with($name, 'illuminate/')
        || in_array($name, ['durable-workflow/workflow', 'durable-workflow/server', 'laravel-workflow/laravel-workflow'], true)
        || (str_starts_with($name, 'symfony/') && !in_array($name, $policy['allowed_symfony_packages'], true))) {
        $failures[] = "Production dependency {$name} crosses the framework-neutral SDK boundary.";
    }
    if ($version !== null && isset($policy['minimum_stable_versions'][$name])) {
        $stable = str_starts_with($version, 'v') ? substr($version, 1) : $version;
        $major = explode('.', $stable)[0];
        $minimum = $policy['minimum_stable_versions'][$name][$major] ?? null;
        if (!preg_match('/^\d+\.\d+\.\d+$/D', $stable) || $minimum === null || version_compare($stable, $minimum, '<')) {
            $failures[] = "Production dependency {$name}:{$version} is outside its reviewed stable security floors.";
        }
    }
}
if ($failures !== []) {
    fwrite(STDERR, implode("\n", $failures)."\n");
    exit(1);
}
fwrite(STDOUT, "Production dependency boundary and HTTP-client security floors passed.\n");
