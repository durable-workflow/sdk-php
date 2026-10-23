<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class DependencyBoundaryPolicyTest extends TestCase
{
    #[DataProvider('inventories')]
    public function testProductionBoundaryAndStableFloorAreEnforced(array $require, array $installed, bool $passes, ?array $conflicts = null): void
    {
        $file = tempnam(sys_get_temp_dir(), 'dw-dependency-policy-');
        $policy = json_decode(file_get_contents(dirname(__DIR__).'/resources/dependency-boundary.json'), true, flags: JSON_THROW_ON_ERROR);
        file_put_contents($file, json_encode(['require' => $require, 'conflict' => $conflicts ?? $policy['required_conflicts']], JSON_THROW_ON_ERROR));
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
        $policy = json_decode(file_get_contents(dirname(__DIR__).'/resources/dependency-boundary.json'), true, flags: JSON_THROW_ON_ERROR);
        $installed = [];
        foreach ($policy['minimum_stable_versions'] as $name => $minimums) {
            $installed[] = ['name' => $name, 'version' => reset($minimums)];
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
        yield 'malformed inventory package' => [$require, [['name' => 'pear/http_request2']], false];
        yield 'missing transitive floor' => [$require, $installed, false, []];
        yield 'weakened transitive floor' => [$require, $installed, false,
            ['pear/net_url2' => '<2.2.2', 'pear/pear_exception' => '<1.0.2']];
        yield 'old floor in manifest' => [array_replace($require, ['pear/http_request2' => '^2.7']), $installed, false];
        foreach (['v2.7.0', 'v1.8.0', 'v3.0.0', 'dev-main', 'v2.8.1-rc.1'] as $version) {
            $inventory = $installed;
            $inventory[0]['version'] = $version;
            yield 'unreviewed version '.$version => [$require, $inventory, false];
        }
        foreach (['pear/net_url2' => 'v2.2.2', 'pear/pear_exception' => 'v1.0.1'] as $name => $version) {
            $inventory = array_map(static fn (array $p): array => $p['name'] === $name ? ['name' => $name, 'version' => $version] : $p, $installed);
            yield 'old transitive '.$name => [$require, $inventory, false];
            yield 'missing transitive '.$name => [$require, array_values(array_filter($installed,
                static fn (array $p): bool => $p['name'] !== $name)), false];
        }

    }
}
