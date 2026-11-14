<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use Composer\InstalledVersions;
use DurableWorkflow\Bridge\Laravel\DurableWorkflowServiceProvider;
use DurableWorkflow\Bridge\Laravel\LaravelPlatform;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\PreserveGlobalState;
use PHPUnit\Framework\Attributes\RunInSeparateProcess;
use PHPUnit\Framework\TestCase;
use ReflectionClass;
use RuntimeException;

final class LaravelPlatformTest extends TestCase
{
    public function testAdmissionMatchesEveryPublishedSupportCell(): void
    {
        $contract = json_decode((string) file_get_contents(dirname(__DIR__).'/docs/laravel-adoption-contract.json'),
            true, 512, JSON_THROW_ON_ERROR);
        self::assertSame($contract['framework']['constraint'], LaravelPlatform::laravelConstraint());
        foreach ($contract['framework']['qualification_matrix'] as $cell) {
            [$major, $minor] = array_map('intval', explode('.', $cell['php']));
            $version = substr($cell['exercised_package'], strlen('laravel/framework:'));
            LaravelPlatform::assertSupported($version.'.0', $major * 10000 + $minor * 100);
            $this->addToAssertionCount(1);
        }
    }

    #[DataProvider('platforms')]
    public function testPlatformAdmissionIsClosed(?string $version, int $php, bool $accepted): void
    {
        if (!$accepted) {
            $this->expectException(RuntimeException::class);
        }
        LaravelPlatform::assertSupported($version, $php);
        if ($accepted) {
            $this->addToAssertionCount(1);
        }
    }

    /** @return iterable<string, array{?string, int, bool}> */
    public static function platforms(): iterable
    {
        yield 'Laravel 12 floor' => ['12.72.0.0', 80200, true];
        yield 'Laravel 12 later patch' => ['12.72.1.0', 80400, true];
        yield 'Laravel 12 stable version' => ['12.72.0', 80300, true];
        yield 'Laravel 13 floor' => ['13.41.0.0', 80300, true];
        yield 'Laravel 13 later release' => ['13.42.0.0', 80400, true];
        yield 'removed Laravel 10' => ['10.50.4.0', 80400, false];
        yield 'removed Laravel 11' => ['11.56.1.0', 80400, false];
        yield 'unqualified future major' => ['14.0.0.0', 80400, false];
        yield 'Laravel 12 before patch' => ['12.71.99.0', 80400, false];
        yield 'Laravel 13 before patch' => ['13.40.99.0', 80400, false];
        yield 'previous Laravel 12 floor is affected' => ['12.69.0.0', 80400, false];
        yield 'previous example Laravel is affected' => ['12.69.3.0', 80400, false];
        yield 'last Laravel 12 release before fix' => ['12.70.2.0', 80400, false];
        yield 'previous Laravel 13 floor is affected' => ['13.30.0.0', 80400, false];
        yield 'last Laravel 13 release before fix' => ['13.39.0.0', 80400, false];
        yield 'concurrency-only Laravel 12 fix lacks signed-URL fixes' => ['12.71.0.0', 80400, false];
        yield 'concurrency-only Laravel 13 fix lacks signed-URL fixes' => ['13.40.0.0', 80400, false];
        yield 'bridge PHP before floor' => ['12.72.0.0', 80199, false];
        yield 'Laravel 13 PHP before floor' => ['13.41.0.0', 80299, false];
        yield 'unsupported PHP major' => ['13.41.0.0', 90000, false];
        yield 'missing framework version' => [null, 80400, false];
        yield 'empty framework version' => ['', 80400, false];
        yield 'unresolved development version' => ['12.x-dev', 80400, false];
        yield 'unqualified prerelease' => ['13.41.0.0-beta', 80400, false];
        yield 'unrecognized version spelling' => ['v12.72.0', 80400, false];
        yield 'version controls' => ["12.72.0\n", 80400, false];
    }

    #[RunInSeparateProcess]
    #[PreserveGlobalState(false)]
    #[DataProvider('rejectedProviderVersions')]
    public function testAutoDiscoveryRefusesBeforeConfigOrBindings(?string $version): void
    {
        if (!class_exists(\Illuminate\Support\ServiceProvider::class)) {
            eval('namespace Illuminate\\Support; class ServiceProvider {}');
        }
        $original = InstalledVersions::getRawData();
        $installed = $original;
        if ($version === null) {
            unset($installed['versions']['laravel/framework']);
        } else {
            $installed['versions']['laravel/framework'] = [
                'pretty_version' => $version,
                'version' => $version,
                'reference' => 'unsupported-test-framework',
            ];
        }
        InstalledVersions::reload($installed);
        try {
            // No application was initialized. Any config/binding access is an Error,
            // so only the early platform refusal can satisfy this control.
            $provider = (new ReflectionClass(DurableWorkflowServiceProvider::class))->newInstanceWithoutConstructor();
            $this->expectException(RuntimeException::class);
            $provider->register();
        } finally {
            InstalledVersions::reload($original);
        }
    }

    /** @return iterable<string, array{?string}> */
    public static function rejectedProviderVersions(): iterable
    {
        yield 'old supported major removed' => ['11.56.1.0'];
        yield 'supported major below patch' => ['12.70.99.0'];
        yield 'previous example below security floor' => ['12.69.3.0'];
        yield 'previous Laravel 13 floor' => ['13.30.0.0'];
        yield 'unknown installed framework' => [null];
    }
}
