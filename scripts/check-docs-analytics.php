<?php

declare(strict_types=1);

$buildDirectory = $argv[1] ?? __DIR__.'/../build/api';
$runtime = file_get_contents(__DIR__.'/../.phpdoc/template/analytics/analytics.js');

if ($runtime === false) {
    throw new RuntimeException('Analytics runtime is unavailable.');
}

if (file_get_contents($buildDirectory.'/analytics/analytics.js') !== $runtime) {
    throw new RuntimeException('Rendered phpDocumentor analytics runtime is stale.');
}

foreach (['G-HD1YHT442Y', 'php.durable-workflow.com', "analytics_storage: 'granted'", 'send_page_view: true'] as $required) {
    if (! str_contains($runtime, $required)) {
        throw new RuntimeException("Analytics runtime is missing required configuration: {$required}");
    }
}

if (str_contains($runtime, "gtag('event', 'page_view'")) {
    throw new RuntimeException('Analytics runtime must not duplicate automatic navigation page views.');
}

$iterator = new RecursiveIteratorIterator(new RecursiveDirectoryIterator($buildDirectory));
$htmlCount = 0;
$nestedHtmlCount = 0;

foreach ($iterator as $file) {
    if (! $file->isFile() || $file->getExtension() !== 'html') {
        continue;
    }

    $htmlCount++;
    $relativePath = str_replace('\\', '/', substr($file->getPathname(), strlen(rtrim($buildDirectory, '/\\')) + 1));
    $isNestedPage = str_contains($relativePath, '/');
    $html = file_get_contents($file->getPathname());
    if ($html === false || substr_count($html, 'src="/analytics/analytics.js"') !== 1) {
        throw new RuntimeException("{$file->getPathname()} must load one root-relative local analytics runtime.");
    }
    if (substr_count($html, 'href="/analytics/analytics.css"') !== 1 || str_contains($html, 'googletagmanager.com')) {
        throw new RuntimeException("{$file->getPathname()} does not preserve consent-gated analytics loading.");
    }

    if ($isNestedPage) {
        $nestedHtmlCount++;

        foreach (['/analytics/analytics.js', '/analytics/analytics.css'] as $assetPath) {
            if (! is_file($buildDirectory.$assetPath)) {
                throw new RuntimeException("{$relativePath} resolves {$assetPath} to a missing rendered asset.");
            }
        }
    }
}

if ($htmlCount === 0) {
    throw new RuntimeException('phpDocumentor did not render HTML pages.');
}

if ($nestedHtmlCount === 0) {
    throw new RuntimeException('phpDocumentor did not render nested class or namespace pages.');
}

fwrite(STDOUT, "Validated consent-gated analytics in {$htmlCount} rendered pages, including {$nestedHtmlCount} nested pages.\n");
