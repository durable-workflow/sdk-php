<?php

declare(strict_types=1);

$buildDirectory = $argv[1] ?? __DIR__.'/../build/api';
$runtime = file_get_contents(__DIR__.'/../.phpdoc/template/analytics/analytics.js');
$referenceStyles = file_get_contents(__DIR__.'/../.phpdoc/template/assets/api-reference.css');
$referenceRuntime = file_get_contents(__DIR__.'/../.phpdoc/template/assets/api-reference.js');

if ($runtime === false || $referenceStyles === false || $referenceRuntime === false) {
    throw new RuntimeException('API-reference assets are unavailable.');
}
if (file_get_contents($buildDirectory.'/analytics/analytics.js') !== $runtime) {
    throw new RuntimeException('Rendered phpDocumentor analytics runtime is stale.');
}
if (file_get_contents($buildDirectory.'/assets/api-reference.css') !== $referenceStyles) {
    throw new RuntimeException('Rendered phpDocumentor API-reference styles are stale.');
}
if (file_get_contents($buildDirectory.'/assets/api-reference.js') !== $referenceRuntime) {
    throw new RuntimeException('Rendered phpDocumentor API-reference runtime is stale.');
}
if (
    ! is_file($buildDirectory.'/favicon.ico')
    || file_get_contents($buildDirectory.'/favicon.ico') !== file_get_contents($buildDirectory.'/images/favicon.ico')
) {
    throw new RuntimeException('Rendered documentation root favicon is unavailable.');
}
if (! preg_match('/\.dw-cloud-promotion__eyebrow\s*\{[^}]*letter-spacing:\s*0;/s', $referenceStyles)) {
    throw new RuntimeException('Promotion eyebrow letter spacing must remain zero.');
}

foreach ([
    'https://static.cloudflareinsights.com/beacon.min.js',
    'document.querySelector(BEACON_SELECTOR)',
    "loader.type = 'module'",
    'loader.dataset.cfBeacon = JSON.stringify({token: TOKEN})',
    "'php.durable-workflow.com'",
    "'cloud.durable-workflow.com': new Set(['/', '/early-access', '/early-access/'])",
    "'status.durable-workflow.com': new Set(['/'])",
] as $required) {
    if (! str_contains($runtime, $required)) {
        throw new RuntimeException("Analytics runtime is missing required configuration: {$required}");
    }
}
if (preg_match('/\b(?:async|defer|spa)\b|localStorage|sessionStorage|document\.cookie|googletagmanager|google-analytics|G-HD1YHT442Y|_ga(?:\\b|_)/i', $runtime)) {
    throw new RuntimeException('Analytics runtime contains unsupported loader, retired Google, or browser-storage behavior.');
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
    if ($html === false || substr_count($html, 'type="module" src="/analytics/analytics.js"') !== 1) {
        throw new RuntimeException("{$file->getPathname()} must load one root-relative module analytics runtime.");
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
    if (substr_count($html, 'type="module" src="/assets/api-reference.js"') !== 1) {
        throw new RuntimeException("{$file->getPathname()} does not load the shared API-reference runtime.");
    }
    if (substr_count($html, 'data-promotion-source="sdk-php-reference"') !== 1) {
        throw new RuntimeException("{$file->getPathname()} must render one bounded PHP SDK promotion.");
    }
    if (! str_contains($html, 'href="https://cloud.durable-workflow.com/early-access#source=sdk-php-reference"')) {
        throw new RuntimeException("{$file->getPathname()} promotion must resolve to the public early-access form.");
    }
    foreach (['phpdocumentor-header__menu-button', 'phpdocumentor-header__menu-icon', 'phpdocumentor-topnav'] as $emptyHeaderMenuClass) {
        if (str_contains($html, $emptyHeaderMenuClass)) {
            throw new RuntimeException("{$file->getPathname()} renders an empty top-navigation control.");
        }
    }

    if ($isNestedPage) {
        $nestedHtmlCount++;
        foreach (['/assets/api-reference.css', '/assets/api-reference.js', '/analytics/analytics.js'] as $assetPath) {
            if (! is_file($buildDirectory.$assetPath)) {
                throw new RuntimeException("{$relativePath} resolves {$assetPath} to a missing rendered asset.");
            }
        }
    }
}

foreach ([
    "PROMOTION_SOURCE = 'sdk-php-reference'",
    "credentials: 'omit'",
    "referrerPolicy: 'origin'",
    'JSON.stringify({source: PROMOTION_SOURCE, event})',
] as $promotionBoundary) {
    if (! str_contains($runtime, $promotionBoundary)) {
        throw new RuntimeException("Promotion analytics is missing its bounded contract: {$promotionBoundary}");
    }
}

if ($htmlCount === 0 || $nestedHtmlCount === 0) {
    throw new RuntimeException('phpDocumentor did not render root and nested HTML pages.');
}

fwrite(STDOUT, "Validated cookie-free analytics in {$htmlCount} rendered pages, including {$nestedHtmlCount} nested pages.\n");
