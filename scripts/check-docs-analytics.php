<?php

declare(strict_types=1);

$buildDirectory = $argv[1] ?? __DIR__.'/../build/api';
$runtime = file_get_contents(__DIR__.'/../.phpdoc/template/analytics/analytics.js');
$referenceStyles = file_get_contents(__DIR__.'/../.phpdoc/template/assets/api-reference.css');

if ($runtime === false || $referenceStyles === false) {
    throw new RuntimeException('API-reference assets are unavailable.');
}
if (file_get_contents($buildDirectory.'/analytics/analytics.js') !== $runtime) {
    throw new RuntimeException('Rendered phpDocumentor analytics runtime is stale.');
}
if (file_get_contents($buildDirectory.'/assets/api-reference.css') !== $referenceStyles) {
    throw new RuntimeException('Rendered phpDocumentor API-reference styles are stale.');
}

foreach ([
    'https://static.cloudflareinsights.com/beacon.min.js',
    'document.querySelector(BEACON_SELECTOR)',
    "loader.dataset.cfBeacon = JSON.stringify({token: TOKEN, spa: true})",
    "'php.durable-workflow.com'",
    "'cloud.durable-workflow.com': new Set(['/', '/early-access', '/early-access/'])",
    "'status.durable-workflow.com': new Set(['/'])",
] as $required) {
    if (! str_contains($runtime, $required)) {
        throw new RuntimeException("Analytics runtime is missing required configuration: {$required}");
    }
}
if (preg_match('/localStorage|sessionStorage|document\.cookie|googletagmanager|google-analytics|G-HD1YHT442Y|_ga(?:\\b|_)/i', $runtime)) {
    throw new RuntimeException('Analytics runtime contains retired Google or browser-storage behavior.');
}

$iterator = new RecursiveIteratorIterator(new RecursiveDirectoryIterator($buildDirectory));
$htmlCount = 0;
$nestedHtmlCount = 0;
$forbidden = '/googletagmanager\.com|google-analytics\.com|G-HD1YHT442Y|durable-workflow\.analytics-consent|durable-workflow-analytics-(?:consent|preferences)|localStorage|_ga(?:\\b|_)/i';

foreach ($iterator as $file) {
    if (! $file->isFile() || $file->getExtension() !== 'html') {
        continue;
    }

    $htmlCount++;
    $relativePath = str_replace('\\', '/', substr($file->getPathname(), strlen(rtrim($buildDirectory, '/\\')) + 1));
    $isNestedPage = str_contains($relativePath, '/');
    $html = file_get_contents($file->getPathname());
    if ($html === false || substr_count($html, 'src="/analytics/analytics.js"') !== 1) {
        throw new RuntimeException("{$file->getPathname()} must load one root-relative cookie-free analytics runtime.");
    }
    if (preg_match($forbidden, $html)) {
        throw new RuntimeException("{$file->getPathname()} contains retired Google analytics or consent state.");
    }
    if (str_contains($html, 'analytics/analytics.css')) {
        throw new RuntimeException("{$file->getPathname()} still loads retired analytics UI styles.");
    }
    if (substr_count($html, 'href="/assets/api-reference.css"') !== 1) {
        throw new RuntimeException("{$file->getPathname()} does not load the shared API-reference styles.");
    }
    foreach (['phpdocumentor-header__menu-button', 'phpdocumentor-header__menu-icon', 'phpdocumentor-topnav'] as $emptyHeaderMenuClass) {
        if (str_contains($html, $emptyHeaderMenuClass)) {
            throw new RuntimeException("{$file->getPathname()} renders an empty top-navigation control.");
        }
    }

    if ($isNestedPage) {
        $nestedHtmlCount++;
        foreach (['/assets/api-reference.css', '/analytics/analytics.js'] as $assetPath) {
            if (! is_file($buildDirectory.$assetPath)) {
                throw new RuntimeException("{$relativePath} resolves {$assetPath} to a missing rendered asset.");
            }
        }
    }
}

if ($htmlCount === 0 || $nestedHtmlCount === 0) {
    throw new RuntimeException('phpDocumentor did not render root and nested HTML pages.');
}

fwrite(STDOUT, "Validated cookie-free analytics in {$htmlCount} rendered pages, including {$nestedHtmlCount} nested pages.\n");
