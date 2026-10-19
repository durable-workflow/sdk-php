<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DOMDocument;
use DOMXPath;
use PHPUnit\Framework\TestCase;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;

final class ApiReferenceMetadataTest extends TestCase
{
    public function testGeneratedCanonicalUrlsPreserveIndexDirectories(): void
    {
        $directory = sys_get_temp_dir().'/dw-api-metadata-'.bin2hex(random_bytes(8));
        $pages = [
            'index.html' => '/api/',
            'classes/Worker.html' => '/api/classes/Worker.html',
            'guides/index.html' => '/api/guides/',
        ];

        try {
            mkdir($directory.'/assets', 0700, true);
            mkdir($directory.'/classes', 0700);
            mkdir($directory.'/guides', 0700);
            foreach ($pages as $page => $canonical) {
                file_put_contents($directory.'/'.$page, '<!doctype html><html><head><base href="./"></head><body>Reference</body></html>');
            }

            $process = proc_open(
                [PHP_BINARY, dirname(__DIR__).'/scripts/finalize-api-reference.php', $directory],
                [1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
                $pipes,
            );
            self::assertIsResource($process);
            $output = stream_get_contents($pipes[1]).stream_get_contents($pipes[2]);
            fclose($pipes[1]);
            fclose($pipes[2]);
            self::assertSame(0, proc_close($process), $output);

            foreach ($pages as $page => $canonical) {
                $document = new DOMDocument();
                self::assertTrue($document->loadHTMLFile($directory.'/'.$page, LIBXML_NONET));
                $xpath = new DOMXPath($document);
                $expected = 'https://php.durable-workflow.com'.$canonical;
                self::assertSame($expected, $xpath->evaluate('string(//link[@rel="canonical"]/@href)'), $page);
                self::assertSame($expected, $xpath->evaluate('string(//meta[@property="og:url"]/@content)'), $page);
            }
        } finally {
            if (is_dir($directory)) {
                $files = new RecursiveIteratorIterator(
                    new RecursiveDirectoryIterator($directory, RecursiveDirectoryIterator::SKIP_DOTS),
                    RecursiveIteratorIterator::CHILD_FIRST,
                );
                foreach ($files as $file) {
                    $file->isDir() ? rmdir($file->getPathname()) : unlink($file->getPathname());
                }
                rmdir($directory);
            }
        }
    }
}
