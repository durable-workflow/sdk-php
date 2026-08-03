<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Bridge\FrameworkRuntimeException;
use DurableWorkflow\Bridge\ServiceConfiguration;
use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Tests\Support\FakeTransport;
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

    public function testRoleSpecificClientsCannotSendTheOppositeScopedCredential(): void
    {
        $configuration = ServiceConfiguration::fromArray([
            'endpoint' => 'https://cloud.example/runtime/acme',
            'namespace' => 'orders',
            'task_queue' => 'orders-php',
            'credentials' => [
                'control_token' => 'control-secret',
                'worker_token' => 'worker-secret',
            ],
        ]);
        $controlTransport = new FakeTransport([[], []]);
        $workerTransport = new FakeTransport([
            ['registered' => true],
            [
                'worker_id' => 'worker-1',
                'outcome' => 'deregistered',
                'recovered_workflow_task_count' => 0,
            ],
        ]);
        $controlClient = $configuration->controlClient($controlTransport);
        $workerClient = $configuration->workerClient($workerTransport);

        $controlClient->health();
        $controlClient->deregisterWorker('operator-worker');
        $workerClient->registerWorker('worker-1', 'orders-php', [], []);
        $deregistration = $workerClient->deregisterWorkerRegistration('worker-1');

        self::assertSame('Bearer control-secret', $controlTransport->requests[0]['headers']['Authorization']);
        self::assertSame('Bearer control-secret', $controlTransport->requests[1]['headers']['Authorization']);
        self::assertSame(
            'https://cloud.example/runtime/acme/api/workers/operator-worker',
            $controlTransport->requests[1]['uri'],
        );
        self::assertSame(
            '2',
            $controlTransport->requests[1]['headers']['X-Durable-Workflow-Control-Plane-Version'],
        );
        self::assertSame('Bearer worker-secret', $workerTransport->requests[0]['headers']['Authorization']);
        self::assertSame('Bearer worker-secret', $workerTransport->requests[1]['headers']['Authorization']);
        self::assertSame(
            'https://cloud.example/runtime/acme/api/worker/registrations/worker-1',
            $workerTransport->requests[1]['uri'],
        );
        self::assertSame(
            '1.13',
            $workerTransport->requests[1]['headers']['X-Durable-Workflow-Protocol-Version'],
        );
        self::assertSame([
            'worker_id' => 'worker-1',
            'outcome' => 'deregistered',
            'recovered_workflow_task_count' => 0,
        ], $deregistration);

        try {
            $controlClient->registerWorker('worker-2', 'orders-php', [], []);
            self::fail('The control client accepted a worker-plane request.');
        } catch (InvalidArgumentException $exception) {
            self::assertStringContainsString('worker credential is required', $exception->getMessage());
        }
        try {
            $workerClient->health();
            self::fail('The worker client accepted a control-plane request.');
        } catch (InvalidArgumentException $exception) {
            self::assertStringContainsString('control credential is required', $exception->getMessage());
        }
        try {
            $controlClient->deregisterWorkerRegistration('worker-2');
            self::fail('The control client accepted worker registration cleanup.');
        } catch (InvalidArgumentException $exception) {
            self::assertStringContainsString('worker credential is required', $exception->getMessage());
        }
        try {
            $workerClient->deregisterWorker('operator-worker');
            self::fail('The worker client accepted operator worker removal.');
        } catch (InvalidArgumentException $exception) {
            self::assertStringContainsString('control credential is required', $exception->getMessage());
        }

        self::assertCount(2, $controlTransport->requests);
        self::assertCount(2, $workerTransport->requests);
    }

    public function testSharedTokenAuthorizesBothRoleSpecificClients(): void
    {
        $configuration = ServiceConfiguration::fromArray([
            'endpoint' => 'http://localhost:8080',
            'namespace' => 'default',
            'task_queue' => 'php-workers',
            'credentials' => ['token' => 'shared-secret'],
        ]);
        $controlTransport = new FakeTransport([[]]);
        $workerTransport = new FakeTransport([['registered' => true]]);

        $configuration->controlClient($controlTransport)->health();
        $configuration->workerClient($workerTransport)->registerWorker('worker-1', 'php-workers', [], []);

        self::assertSame('Bearer shared-secret', $controlTransport->requests[0]['headers']['Authorization']);
        self::assertSame('Bearer shared-secret', $workerTransport->requests[0]['headers']['Authorization']);
    }

    public function testRoleSpecificClientConstructionFailsClosedWithoutItsCredential(): void
    {
        $controlOnly = ServiceConfiguration::fromArray([
            'endpoint' => 'https://cloud.example/runtime/acme',
            'namespace' => 'orders',
            'task_queue' => 'orders-php',
            'credentials' => ['control_token' => 'control-secret'],
        ]);
        $workerOnly = ServiceConfiguration::fromArray([
            'endpoint' => 'https://cloud.example/runtime/acme',
            'namespace' => 'orders',
            'task_queue' => 'orders-php',
            'credentials' => ['worker_token' => 'worker-secret'],
        ]);
        $controlTransport = new FakeTransport([[]]);
        $workerTransport = new FakeTransport([['registered' => true]]);

        $controlOnly->controlClient($controlTransport)->health();
        $workerOnly->workerClient($workerTransport)->registerWorker('worker-1', 'orders-php', [], []);

        self::assertSame('Bearer control-secret', $controlTransport->requests[0]['headers']['Authorization']);
        self::assertSame('Bearer worker-secret', $workerTransport->requests[0]['headers']['Authorization']);

        try {
            $controlOnly->workerClient();
            self::fail('The worker client was constructed without a worker credential.');
        } catch (InvalidArgumentException $exception) {
            self::assertStringContainsString('worker credential is required', $exception->getMessage());
        }
        try {
            $workerOnly->controlClient();
            self::fail('The application client was constructed without a control credential.');
        } catch (InvalidArgumentException $exception) {
            self::assertStringContainsString('control credential is required', $exception->getMessage());
        }
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
