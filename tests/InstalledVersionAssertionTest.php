<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/** The CI guard that each framework-compat Laravel cell runs exactly its floor. */
final class InstalledVersionAssertionTest extends TestCase
{
    private string $project;

    protected function setUp(): void
    {
        $this->project = sys_get_temp_dir().'/dw-installed-version-'.bin2hex(random_bytes(8));
        mkdir($this->project.'/vendor/composer', 0700, true);
        mkdir($this->project.'/vendor/laravel/framework/src/Illuminate/Foundation', 0700, true);
    }

    protected function tearDown(): void
    {
        foreach (['vendor/composer/installed.json',
            'vendor/laravel/framework/src/Illuminate/Foundation/Application.php'] as $file) {
            @unlink($this->project.'/'.$file);
        }
        foreach (['vendor/laravel/framework/src/Illuminate/Foundation', 'vendor/laravel/framework/src/Illuminate',
            'vendor/laravel/framework/src', 'vendor/laravel/framework', 'vendor/laravel', 'vendor/composer',
            'vendor', ''] as $directory) {
            @rmdir($this->project.'/'.$directory);
        }
    }

    public function testExactMinimumReleasePasses(): void
    {
        $this->install('v11.56.1', '11.56.1.0', '11.56.1');

        self::assertSame([0, "laravel/framework is exactly 11.56.1.\n", ''], $this->assert('11.56.1'));
    }

    #[DataProvider('laterOrMissingReleases')]
    public function testAnythingButTheExactMinimumFails(?string $version, ?string $normalized, ?string $constant,
        string $expected, string $error): void
    {
        if ($version !== null) {
            $this->install($version, (string) $normalized, $constant);
        }

        [$status, , $errors] = $this->assert($expected);

        self::assertSame(1, $status);
        self::assertStringContainsString($error, $errors);
    }

    /** @return iterable<string, array{?string, ?string, ?string, string, string}> */
    public static function laterOrMissingReleases(): iterable
    {
        // What SDK 3.8.2's first hosted run resolved from caret inputs.
        yield 'caret resolved 11.57.1' => ['v11.57.1', '11.57.1.0', '11.57.1', '11.56.1',
            'laravel/framework resolved to v11.57.1, not exactly 11.56.1.'];
        yield 'caret resolved 12.70.2' => ['v12.70.2', '12.70.2.0', '12.70.2', '12.69.0',
            'laravel/framework resolved to v12.70.2, not exactly 12.69.0.'];
        yield 'caret resolved 13.39.0' => ['v13.39.0', '13.39.0.0', '13.39.0', '13.30.0',
            'laravel/framework resolved to v13.39.0, not exactly 13.30.0.'];
        yield 'framework constant disagrees' => ['v10.50.4', '10.50.4.0', '10.50.5', '10.50.4',
            'Laravel Application::VERSION is 10.50.5, not exactly 10.50.4.'];
        yield 'framework constant missing' => ['v10.50.4', '10.50.4.0', null, '10.50.4',
            'does not declare Application::VERSION'];
        yield 'not installed' => [null, null, null, '10.50.4', 'Cannot read'];
        yield 'caret expectation' => ['v10.50.4', '10.50.4.0', '10.50.4', '^10.50.4',
            'The expected version must be one exact x.y.z release'];
    }

    private function install(string $version, string $normalized, ?string $constant): void
    {
        file_put_contents($this->project.'/vendor/composer/installed.json', json_encode(['packages' => [
            ['name' => 'durable-workflow/sdk', 'version' => 'dev-main', 'version_normalized' => 'dev-main'],
            ['name' => 'laravel/framework', 'version' => $version, 'version_normalized' => $normalized],
        ], 'dev' => true, 'dev-package-names' => []], JSON_THROW_ON_ERROR));
        if ($constant !== null) {
            file_put_contents($this->project.'/vendor/laravel/framework/src/Illuminate/Foundation/Application.php',
                "<?php\n\nclass Application\n{\n    const VERSION = '".$constant."';\n}\n");
        }
    }

    /** @return array{int, string, string} */
    private function assert(string $expected): array
    {
        $process = proc_open([PHP_BINARY, dirname(__DIR__).'/scripts/ci/assert-installed-version.php',
            $this->project, 'laravel/framework', $expected], [1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
        self::assertIsResource($process);
        $output = (string) stream_get_contents($pipes[1]);
        $errors = (string) stream_get_contents($pipes[2]);
        fclose($pipes[1]);
        fclose($pipes[2]);

        return [proc_close($process), $output, $errors];
    }
}
