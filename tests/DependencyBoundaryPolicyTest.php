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
        $require = ['php' => '^8.1', 'amphp/http-client' => '^5.3.6'];
        $installed = [['name' => 'amphp/http-client', 'version' => 'v5.3.6']];
        yield 'standalone HTTP component' => [$require, $installed, true];
        yield 'neutral support contracts' => [$require, [...$installed,
            ['name' => 'symfony/deprecation-contracts', 'version' => 'v3.6.0']], true];
        foreach (['laravel/framework', 'illuminate/support', 'symfony/framework-bundle', 'symfony/http-kernel',
            'durable-workflow/workflow', 'durable-workflow/server', 'laravel-workflow/laravel-workflow'] as $forbidden) {
            yield 'transitive '.$forbidden => [$require, [...$installed, ['name' => $forbidden, 'version' => '1.0.0']], false];
            yield 'declared '.$forbidden => [$require + [$forbidden => '^1.0'], $installed, false];
        }
        yield 'missing inventory' => [$require, [], false];
        yield 'malformed inventory package' => [$require, [['name' => 'amphp/http-client']], false];
        yield 'old floor in manifest' => [['amphp/http-client' => '^5.0'], $installed, false];
        foreach (['v5.3.5', 'v4.6.0', 'v6.0.0', 'dev-main', 'v5.3.7-rc.1'] as $version) {
            yield 'unreviewed version '.$version => [$require, [['name' => 'amphp/http-client', 'version' => $version]], false];
        }
    }
}
