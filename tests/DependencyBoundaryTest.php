<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use PHPUnit\Framework\TestCase;

final class DependencyBoundaryTest extends TestCase
{
    /** @return array<string, mixed> */
    private function manifest(): array
    {
        return json_decode((string) file_get_contents(dirname(__DIR__).'/composer.json'), true, 512, JSON_THROW_ON_ERROR);
    }

    public function testProductionManifestHasNoEmbeddedFrameworkDependencies(): void
    {
        $manifest = $this->manifest();
        $requirements = array_keys($manifest['require']);
        $forbidden = [
            'laravel/',
            'illuminate/',
            'symfony/',
            'durable-workflow/workflow',
            'durable-workflow/server',
        ];

        foreach ($requirements as $requirement) {
            foreach ($forbidden as $prefix) {
                self::assertFalse(str_starts_with(strtolower($requirement), $prefix), "Forbidden dependency {$requirement}");
            }
        }
    }

    public function testPrereleaseMetadataBindsTheSdkToItsQualifiedServer(): void
    {
        $metadata = $this->manifest()['extra']['durable-workflow'];

        self::assertSame('2.0.0-rc.12', $metadata['product-train']);
        self::assertSame('2.0.0-rc.23', $metadata['supported-server-versions']);
    }
}
