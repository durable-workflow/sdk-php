<?php

declare(strict_types=1);

function normalizeApiReference(string $reference): string
{
    if (
        $reference === ''
        || str_starts_with($reference, '/')
        || str_starts_with($reference, '#')
        || str_starts_with($reference, '?')
        || preg_match('#^[a-z][a-z0-9+.-]*:#i', $reference) === 1
    ) {
        return $reference;
    }

    $suffixOffset = strcspn($reference, '?#');
    $path = substr($reference, 0, $suffixOffset);
    $suffix = substr($reference, $suffixOffset);
    $path = preg_replace('#^(?:(?:\.\.?)/)+#', '', $path);
    if ($path === null) {
        throw new RuntimeException("Unable to normalize generated API reference: {$reference}");
    }

    return '/api/'.ltrim($path, '/').$suffix;
}

function removeIncompleteGraphPages(string $buildDirectory): int
{
    $graphPages = glob($buildDirectory.'/graphs/*.html');
    if ($graphPages === false) {
        throw new RuntimeException('Unable to inspect generated API graph pages.');
    }

    $removed = 0;
    foreach ($graphPages as $graphPage) {
        $graphAsset = substr($graphPage, 0, -strlen('.html')).'.svg';
        if (is_file($graphAsset)) {
            continue;
        }

        if (! unlink($graphPage)) {
            throw new RuntimeException("Unable to remove incomplete generated API graph page: {$graphPage}");
        }
        $removed++;
    }

    return $removed;
}

$buildDirectory = rtrim($argv[1] ?? __DIR__.'/../build/api', '/\\');
$removedGraphPages = removeIncompleteGraphPages($buildDirectory);
$iterator = new RecursiveIteratorIterator(new RecursiveDirectoryIterator($buildDirectory));
$description = 'Generated classes, methods, models, exceptions, and worker types for the Durable Workflow PHP SDK.';
$count = 0;
$faviconSource = __DIR__.'/../docs/portal/assets/favicon.svg';
$faviconTarget = $buildDirectory.'/assets/favicon.svg';

if (! is_file($faviconSource) || ! is_dir(dirname($faviconTarget)) || ! copy($faviconSource, $faviconTarget)) {
    throw new RuntimeException('Unable to publish the API-reference favicon.');
}

foreach ($iterator as $file) {
    if (! $file->isFile() || $file->getExtension() !== 'html') {
        continue;
    }

    $path = $file->getPathname();
    $relative = str_replace('\\', '/', substr($path, strlen($buildDirectory) + 1));
    $canonicalPath = preg_replace('#(?:^|/)index\.html$#', '$1', $relative) ?? $relative;
    $canonicalPath = '/api/'.ltrim($canonicalPath, '/');
    $canonical = 'https://php.durable-workflow.com'.htmlspecialchars($canonicalPath, ENT_QUOTES | ENT_HTML5);
    $metadata = <<<HTML
    <meta name="description" content="{$description}">
    <meta name="robots" content="index,follow">
    <meta name="theme-color" content="#101820">
    <link rel="canonical" href="{$canonical}">
    <link rel="icon" href="/assets/favicon.svg" type="image/svg+xml">
    <meta property="og:type" content="website">
    <meta property="og:site_name" content="Durable Workflow PHP SDK">
    <meta property="og:title" content="Durable Workflow PHP SDK — API Reference">
    <meta property="og:description" content="{$description}">
    <meta property="og:url" content="{$canonical}">
    <meta property="og:image" content="https://php.durable-workflow.com/assets/social-card.png">
    <meta name="twitter:card" content="summary_large_image">
    <meta name="twitter:image" content="https://php.durable-workflow.com/assets/social-card.png">
HTML;

    $html = file_get_contents($path);
    if ($html === false || ! str_contains($html, '</head>')) {
        throw new RuntimeException("Generated API page has no document head: {$relative}");
    }
    $basePattern = '#<base\s+href=(["\'])[^"\']*\1\s*/?>#i';
    if (preg_match_all($basePattern, $html) !== 1) {
        throw new RuntimeException("Generated API page must have exactly one document base: {$relative}");
    }
    $html = preg_replace($basePattern, '<base href="/api/">', $html);
    if ($html === null) {
        throw new RuntimeException("Unable to normalize the generated API document base: {$relative}");
    }
    $html = str_replace('</head>', $metadata."\n</head>", $html);

    $document = new DOMDocument();
    $previousLibxmlState = libxml_use_internal_errors(true);
    $loaded = $document->loadHTML($html, LIBXML_NONET | LIBXML_NOERROR | LIBXML_NOWARNING);
    libxml_clear_errors();
    libxml_use_internal_errors($previousLibxmlState);
    if (! $loaded) {
        throw new RuntimeException("Unable to parse generated API page: {$relative}");
    }

    $elements = (new DOMXPath($document))->query('//*[@href or @src]');
    if ($elements === false) {
        throw new RuntimeException("Unable to inspect generated API references: {$relative}");
    }
    foreach ($elements as $element) {
        if (! $element instanceof DOMElement) {
            throw new RuntimeException("Generated API reference is not attached to an element: {$relative}");
        }
        foreach (['href', 'src'] as $attribute) {
            if ($element->hasAttribute($attribute)) {
                $element->setAttribute($attribute, normalizeApiReference($element->getAttribute($attribute)));
            }
        }
    }

    $html = $document->saveHTML();
    if ($html === false) {
        throw new RuntimeException("Unable to serialize generated API page: {$relative}");
    }
    if (file_put_contents($path, $html) === false) {
        throw new RuntimeException("Unable to finalize generated API page: {$relative}");
    }
    $count++;
}

if ($count === 0) {
    throw new RuntimeException('phpDocumentor generated no API pages to finalize.');
}

fwrite(STDOUT, "Finalized metadata for {$count} API-reference pages.\n");
if ($removedGraphPages > 0) {
    fwrite(STDOUT, "Removed {$removedGraphPages} graph page(s) without generated SVG assets.\n");
}
