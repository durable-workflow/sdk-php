<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Bridge\FrameworkRuntimeException;
use DurableWorkflow\Bridge\ServiceConfiguration;
use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class FrameworkBridgeTest extends TestCase
{
    public function testServiceConfigurationSupportsCloudScopedCredentials(): void
    {
        $configuration = ServiceConfiguration::fromArray([
            'endpoint' => 'https://cloud.example/runtime/acme',
            'namespace' => 'orders',
            'task_queue' => 'orders-php',
            'credentials' => [
                'control_token' => 'control-secret',
                'worker_token' => 'worker-secret',
            ],
            'handlers' => [BridgeFixtureHandler::class],
            'poll_timeout_seconds' => 12,
        ]);

        self::assertSame('https://cloud.example/runtime/acme', $configuration->endpoint);
        self::assertSame('orders', $configuration->namespace);
        self::assertSame('orders-php', $configuration->taskQueue);
        self::assertSame(12, $configuration->pollTimeoutSeconds);
        self::assertSame('priority-orders', $configuration->taskQueue('priority-orders'));
        self::assertSame('orders', $configuration->client()->namespace);
    }

    /** @param array<string, mixed> $values */
    #[DataProvider('invalidConfiguration')]
    public function testInvalidConfigurationIsActionable(array $values, string $message): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($message);

        ServiceConfiguration::fromArray(array_merge([
            'endpoint' => 'http://localhost:8080',
            'namespace' => 'default',
            'task_queue' => 'php-workers',
            'handlers' => [BridgeFixtureHandler::class],
        ], $values));
    }

    /** @return iterable<string, array{array<string, mixed>, string}> */
    public static function invalidConfiguration(): iterable
    {
        yield 'missing endpoint' => [['endpoint' => ''], 'endpoint is required'];
        yield 'relative endpoint' => [['endpoint' => '/server'], 'absolute http:// or https://'];
        yield 'sdk api suffix' => [['endpoint' => 'https://cloud.example/api'], 'omit the SDK-owned /api suffix'];
        yield 'empty namespace' => [['namespace' => ''], 'namespace must be non-empty'];
        yield 'empty queue' => [['task_queue' => ''], 'task queue must be non-empty'];
        yield 'mixed credentials' => [[
            'credentials' => ['token' => 'shared', 'worker_token' => 'worker'],
        ], 'either the shared Durable Workflow token or scoped'];
        yield 'invalid timeout' => [['poll_timeout_seconds' => 61], 'between 0 and 60'];
        yield 'invalid handlers' => [['handlers' => ['']], 'Each Durable Workflow handler'];
        yield 'duplicate handlers' => [[
            'handlers' => [BridgeFixtureHandler::class, BridgeFixtureHandler::class],
        ], 'configured more than once'];
    }

    #[DataProvider('runtimeFailures')]
    public function testRuntimeFailuresProvideFrameworkConsoleRemediation(
        ServerException $failure,
        string $message,
    ): void {
        self::assertStringContainsString($message, FrameworkRuntimeException::fromThrowable($failure)->getMessage());
    }

    /** @return iterable<string, array{ServerException, string}> */
    public static function runtimeFailures(): iterable
    {
        yield 'unreachable' => [new ServerException('connection refused', 0), 'runtime is unreachable'];
        yield 'authentication' => [new ServerException('denied', 401), 'authentication failed'];
        yield 'contract' => [new ServerException('mismatch', 409, 'worker_protocol_mismatch'), 'contract is incompatible'];
    }

    public function testComposerMetadataAdvertisesOptionalSupportedFrameworks(): void
    {
        $manifest = json_decode(
            (string) file_get_contents(dirname(__DIR__).'/composer.json'),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );

        self::assertSame(
            ['DurableWorkflow\\Bridge\\Laravel\\DurableWorkflowServiceProvider'],
            $manifest['extra']['laravel']['providers'],
        );
        self::assertSame('^12.0|^13.0', $manifest['extra']['durable-workflow']['frameworks']['laravel']);
        self::assertSame('^6.4|^7.0|^8.0', $manifest['extra']['durable-workflow']['frameworks']['symfony']);
        self::assertArrayHasKey('symfony/console', $manifest['suggest']);
        self::assertArrayNotHasKey('laravel/framework', $manifest['require']);
        self::assertArrayNotHasKey('symfony/framework-bundle', $manifest['require']);
        self::assertArrayNotHasKey('symfony/console', $manifest['require']);
    }

    public function testPlainPhpClientDoesNotLoadFrameworkBridgeFiles(): void
    {
        new Client('http://localhost:8080');

        $bridgeFiles = array_values(array_filter(
            get_included_files(),
            static fn (string $file): bool => str_contains($file, '/src/Bridge/Laravel/')
                || str_contains($file, '/src/Bridge/Symfony/'),
        ));

        self::assertSame([], $bridgeFiles);
    }

    public function testPublishedLaravelConfigurationContainsOnlyEnvironmentReferences(): void
    {
        $configuration = (string) file_get_contents(
            dirname(__DIR__).'/resources/laravel/durable-workflow.php',
        );

        foreach ([
            'DURABLE_WORKFLOW_ENDPOINT',
            'DURABLE_WORKFLOW_NAMESPACE',
            'DURABLE_WORKFLOW_TASK_QUEUE',
            'DURABLE_WORKFLOW_TOKEN',
            'DURABLE_WORKFLOW_CONTROL_TOKEN',
            'DURABLE_WORKFLOW_WORKER_TOKEN',
        ] as $environmentVariable) {
            self::assertStringContainsString("env('{$environmentVariable}'", $configuration);
        }
        self::assertStringNotContainsString('dev-token', $configuration);
    }
}

final class BridgeFixtureHandler
{
}
