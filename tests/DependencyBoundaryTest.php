<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use PHPUnit\Framework\TestCase;

final class DependencyBoundaryTest extends TestCase
{
    public function testProductionManifestHasNoEmbeddedFrameworkDependencies(): void
    {
        $manifest = json_decode((string) file_get_contents(dirname(__DIR__).'/composer.json'), true, 512, JSON_THROW_ON_ERROR);
        $requirements = array_keys($manifest['require']);
        $forbidden = ['laravel/', 'illuminate/', 'durable-workflow/workflow', 'durable-workflow/server'];

        foreach ($requirements as $requirement) {
            foreach ($forbidden as $prefix) {
                self::assertFalse(str_starts_with(strtolower($requirement), $prefix), "Forbidden dependency {$requirement}");
            }
        }
    }
}
