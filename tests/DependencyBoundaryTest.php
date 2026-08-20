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

    /** @return array<string, mixed> */
    private function quickstartContract(): array
    {
        return json_decode((string) file_get_contents(dirname(__DIR__).'/docs/quickstart-contract.json'), true, 512, JSON_THROW_ON_ERROR);
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

    public function testPrereleaseMetadataDeclaresExactQualifiedArtifacts(): void
    {
        $metadata = $this->manifest()['extra']['durable-workflow'];
        $quickstart = $this->quickstartContract();

        self::assertMatchesRegularExpression('/^2\.0\.0-rc\.(?:0|[1-9][0-9]*)$/D', $metadata['product-train']);
        self::assertMatchesRegularExpression('/^2\.0\.0-rc\.(?:0|[1-9][0-9]*)$/D', $metadata['supported-server-versions']);
        self::assertSame($metadata['product-train'], $quickstart['package']['published_version']);
        self::assertSame($metadata['product-train'].'@RC', $quickstart['package']['composer_requirement']);
        self::assertSame(
            'durableworkflow/server:'.$metadata['supported-server-versions'],
            $quickstart['runtime_targets']['server']['image'],
        );
    }
}
