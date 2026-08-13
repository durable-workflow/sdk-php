<?php

declare(strict_types=1);

$siteDirectory = rtrim($argv[1] ?? __DIR__.'/../build/site', '/\\');
$runtimeSource = file_get_contents(__DIR__.'/../.phpdoc/template/analytics/analytics.js');
$runtime = file_get_contents($siteDirectory.'/analytics/analytics.js');
$referenceStyles = file_get_contents(__DIR__.'/../.phpdoc/template/assets/api-reference.css');
$referenceRuntime = file_get_contents(__DIR__.'/../.phpdoc/template/assets/api-reference.js');

if ($runtimeSource === false || $runtime === false || $runtime !== $runtimeSource) {
    throw new RuntimeException('Rendered analytics runtime is missing or stale.');
}
if ($referenceStyles === false || file_get_contents($siteDirectory.'/assets/api-reference.css') !== $referenceStyles) {
    throw new RuntimeException('Rendered API-reference styles are missing or stale.');
}
if ($referenceRuntime === false || file_get_contents($siteDirectory.'/assets/api-reference.js') !== $referenceRuntime) {
    throw new RuntimeException('Rendered API-reference runtime is missing or stale.');
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

$iterator = new RecursiveIteratorIterator(new RecursiveDirectoryIterator($siteDirectory));
$htmlCount = 0;
$portalCount = 0;
$apiCount = 0;
$nestedApiCount = 0;
$forbidden = '/googletagmanager\.com|google-analytics\.com|G-HD1YHT442Y|durable-workflow\.analytics-consent|durable-workflow-analytics-(?:consent|preferences)|localStorage|_ga(?:\\b|_)/i';
$externalFont = '/fonts\.(?:googleapis|gstatic)\.com|font-awesome|@font-face\s*\{[^}]*url\(\s*[\'\"]?https?:/is';

foreach ($iterator as $file) {
    if (! $file->isFile() || ! in_array($file->getExtension(), ['css', 'html'], true)) {
        continue;
    }

    $contents = file_get_contents($file->getPathname());
    if ($contents === false) {
        throw new RuntimeException("{$file->getPathname()} could not be read.");
    }
    if (preg_match($externalFont, $contents)) {
        throw new RuntimeException("{$file->getPathname()} depends on an external font resource.");
    }
    if ($file->getExtension() !== 'html') {
        continue;
    }

    $htmlCount++;
    $relative = str_replace('\\', '/', substr($file->getPathname(), strlen($siteDirectory) + 1));
    $isApi = str_starts_with($relative, 'api/');
    $html = $contents;
    if (substr_count($html, 'type="module" src="/analytics/analytics.js"') !== 1) {
        throw new RuntimeException("{$relative} must load one root-relative module analytics runtime.");
    }
    if (preg_match($forbidden, $html)) {
        throw new RuntimeException("{$relative} contains retired Google analytics or consent state.");
    }
    if (str_contains($html, 'analytics/analytics.css')) {
        throw new RuntimeException("{$relative} still loads retired analytics UI styles.");
    }

    if ($isApi) {
        $apiCount++;
        if ($relative !== 'api/index.html') {
            $nestedApiCount++;
        }
        if (substr_count($html, 'href="/assets/api-reference.css"') !== 1
            || substr_count($html, 'type="module" src="/assets/api-reference.js"') !== 1) {
            throw new RuntimeException("{$relative} does not load the shared API-reference assets.");
        }
        if (substr_count($html, 'data-promotion-source="sdk-php-reference"') !== 1) {
            throw new RuntimeException("{$relative} must render one bounded PHP SDK promotion.");
        }
        if (! str_contains($html, 'href="https://cloud.durable-workflow.com/early-access#source=sdk-php-reference"')) {
            throw new RuntimeException("{$relative} promotion must resolve to the public early-access form.");
        }
        foreach (['phpdocumentor-header__menu-button', 'phpdocumentor-header__menu-icon', 'phpdocumentor-topnav'] as $emptyHeaderClass) {
            if (str_contains($html, $emptyHeaderClass)) {
                throw new RuntimeException("{$relative} renders an empty top-navigation control.");
            }
        }
    } else {
        $portalCount++;
        if (substr_count($html, 'href="/assets/portal.css"') !== 1) {
            throw new RuntimeException("{$relative} does not load the portal stylesheet.");
        }
        $promotionCount = substr_count($html, 'data-promotion-source="sdk-php-reference"');
        if ($relative === 'index.html') {
            if ($promotionCount !== 1
                || ! str_contains($html, 'href="https://cloud.durable-workflow.com/early-access#source=sdk-php-reference"')) {
                throw new RuntimeException('The portal home must render one bounded, source-qualified Cloud promotion.');
            }
        } elseif ($promotionCount !== 0) {
            throw new RuntimeException("{$relative} must not repeat the Cloud promotion outside the portal home.");
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

if ($htmlCount === 0 || $portalCount === 0 || $apiCount === 0 || $nestedApiCount === 0) {
    throw new RuntimeException('Documentation output must contain authored guides and root/nested API pages.');
}

fwrite(STDOUT, "Validated cookie-free analytics across {$portalCount} portal and {$apiCount} API-reference pages.\n");
