<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class DependencyBoundaryPolicyTest extends TestCase
{
    #[DataProvider('inventories')]
    public function testProductionBoundaryAndStableFloorAreEnforced(array $require, array $installed, bool $passes): void
    {
        $file = tempnam(sys_get_temp_dir(), 'dw-dependency-policy-');
        file_put_contents($file, json_encode(['require' => $require], JSON_THROW_ON_ERROR));
        try {
            $process = proc_open([PHP_BINARY, dirname(__DIR__).'/scripts/check-dependency-boundary.php', $file],
                [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
            self::assertIsResource($process);
            fwrite($pipes[0], json_encode(['installed' => $installed], JSON_THROW_ON_ERROR));
            fclose($pipes[0]);
            $output = stream_get_contents($pipes[1]).stream_get_contents($pipes[2]);
            fclose($pipes[1]);
            fclose($pipes[2]);
            self::assertSame($passes ? 0 : 1, proc_close($process), $output);
        } finally {
            unlink($file);
        }
    }

    public static function inventories(): iterable
    {
        $require = json_decode(file_get_contents(dirname(__DIR__).'/resources/dependency-boundary.json'), true,
            flags: JSON_THROW_ON_ERROR)['required_constraints'] + ['php' => '^8.1'];
        $installed = [];
        foreach ($require as $name => $constraint) {
            if ($name !== 'php') {
                $version = substr($constraint, 1);
                $installed[] = ['name' => $name, 'version' => substr_count($version, '.') === 1 ? $version.'.0' : $version];
            }
        }
        yield 'standalone HTTP component' => [$require, $installed, true];
        yield 'neutral support contracts' => [$require, [...$installed,
            ['name' => 'symfony/deprecation-contracts', 'version' => 'v3.6.0']], true];
        foreach (['laravel/framework', 'illuminate/support', 'symfony/framework-bundle', 'symfony/http-kernel',
            'durable-workflow/workflow', 'durable-workflow/server', 'laravel-workflow/laravel-workflow'] as $forbidden) {
            yield 'transitive '.$forbidden => [$require, [...$installed, ['name' => $forbidden, 'version' => '1.0.0']], false];
            yield 'declared '.$forbidden => [$require + [$forbidden => '^1.0'], $installed, false];
        }
        yield 'missing inventory' => [$require, [], false];
        yield 'malformed inventory package' => [$require, [['name' => 'rmccue/requests']], false];
        yield 'old floor in manifest' => [array_replace($require, ['rmccue/requests' => '^2.0']), $installed, false];
        foreach (['v2.0.20', 'v1.8.0', 'v3.0.0', 'dev-main', 'v2.0.22-rc.1'] as $version) {
            $inventory = $installed;
            $inventory[array_key_last($inventory)]['version'] = $version;
            yield 'unreviewed version '.$version => [$require, $inventory, false];
        }
    }
}
