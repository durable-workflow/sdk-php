<?php

declare(strict_types=1);

use Composer\InstalledVersions;
use DurableWorkflow\Attribute\Query;
use DurableWorkflow\Attribute\Signal;
use DurableWorkflow\Attribute\Update;
use DurableWorkflow\Attribute\Workflow;
use DurableWorkflow\Client;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\WorkflowContext;
use Psr\Container\ContainerInterface;

require $argv[1];

$version = InstalledVersions::getPrettyVersion('durable-workflow/sdk');
$root = realpath((string) InstalledVersions::getInstallPath('durable-workflow/sdk'));
$implementation = realpath((string) (new ReflectionClass(Worker::class))->getFileName());
if ($version !== getenv('SDK_VERSION') || $root === false || $implementation === false
    || !str_starts_with($implementation, $root.'/')) {
    throw new RuntimeException('The lifetime check must execute the selected installed SDK without a source fallback.');
}
$reference = InstalledVersions::getReference('durable-workflow/sdk');
$expectedReference = getenv('SDK_EXPECTED_REFERENCE');
if (is_string($expectedReference) && $expectedReference !== '' && $reference !== $expectedReference) {
    throw new RuntimeException('The installed SDK reference does not match the selected artifact.');
}

final class PackageLifetimeWorkflow
{
    public static ?object $expectedDependency = null;
    private readonly object $state;

    public function __construct(object $dependency)
    {
        if ($dependency !== self::$expectedDependency) {
            throw new RuntimeException('Workflow construction replaced the shared application dependency.');
        }
        $this->state = (object) ['counter' => 0];
    }

    #[Workflow('smoke.instance_lifetime')]
    public function run(WorkflowContext $context): int
    {
        ++$this->state->counter;
        if (!$context->waitCondition(
            static fn (): bool => $context->signals('finish') !== [],
            key: 'finish',
            timeout: 45,
        )) {
            throw new RuntimeException('The original lifetime observation window expired.');
        }

        return $this->state->counter;
    }

    #[Query('counter')]
    public function counter(QueryContext $_context): int
    {
        return $this->state->counter;
    }

    #[Update('increment')]
    public function increment(QueryContext $_context): int
    {
        return ++$this->state->counter;
    }

    #[Signal('finish')]
    public function finish(): void
    {
        // WorkflowContext reads this signal from committed history.
    }
}

$url = getenv('LIFETIME_RUNTIME_URL') ?: 'http://localhost:8080';
$token = getenv('LIFETIME_TOKEN') ?: null;
$clientToken = getenv('DURABLE_WORKFLOW_CLIENT_TOKEN') ?: null;
$workerToken = getenv('DURABLE_WORKFLOW_WORKER_TOKEN') ?: null;
if (($token === null && ($clientToken === null || $workerToken === null))
    || !function_exists('pcntl_fork') || !function_exists('posix_kill')) {
    throw new RuntimeException('Provide scoped client/worker tokens or LIFETIME_TOKEN, with pcntl and posix.');
}
$namespace = getenv('LIFETIME_NAMESPACE') ?: 'default';
$prefix = 'lifetime-'.bin2hex(random_bytes(6));
$queue = $prefix.'-workers';
$pid = pcntl_fork();
if ($pid === -1) {
    throw new RuntimeException('The lifetime worker process could not start.');
}
if ($pid === 0) {
    try {
        $dependency = new stdClass();
        PackageLifetimeWorkflow::$expectedDependency = $dependency;
        $container = new class($dependency) implements ContainerInterface {
            public function __construct(private readonly object $dependency) {}

            public function has(string $id): bool
            {
                return $id === PackageLifetimeWorkflow::class;
            }

            public function get(string $id): mixed
            {
                if (!$this->has($id)) {
                    throw new RuntimeException('Unexpected lifetime handler resolution.');
                }

                return new PackageLifetimeWorkflow($this->dependency);
            }
        };
        Worker::create(new Client(
            $url, token: $token, namespace: $namespace,
            workerToken: $token === null ? $workerToken : null,
        ), $queue, $container)
            ->register(PackageLifetimeWorkflow::class)
            ->run(1);
        exit(0);
    } catch (Throwable $failure) {
        fwrite(STDERR, $failure::class.': '.$failure->getMessage().PHP_EOL);
        exit(1);
    }
}

$client = new Client(
    $url, token: $token, namespace: $namespace,
    controlToken: $token === null ? $clientToken : null,
);
$handles = [];
$failure = null;
try {
    $registrationDeadline = microtime(true) + 20;
    while (($client->listWorkers($queue)['workers'] ?? []) === []) {
        if (microtime(true) >= $registrationDeadline) {
            throw new RuntimeException('The installed worker did not register within the original observation window.');
        }
        usleep(50_000);
    }
    foreach (['a', 'b'] as $suffix) {
        $handles[] = $client->startWorkflow(
            'smoke.instance_lifetime', $prefix.'-'.$suffix, $queue,
            executionTimeoutSeconds: 90, runTimeoutSeconds: 60,
        );
    }
    foreach ($handles as $handle) {
        if ($handle->query('counter') !== 0
            || $handle->update('increment', waitTimeoutSeconds: 10) !== 1
            || $handle->query('counter') !== 0) {
            throw new RuntimeException('An execution, query or update retained another invocation state.');
        }
    }
    foreach ($handles as $handle) {
        $handle->signal('finish');
        if ($handle->result(timeoutSeconds: 20, pollIntervalSeconds: 0.1) !== 1) {
            throw new RuntimeException('Cold workflow replay retained nested instance state.');
        }
    }
} catch (Throwable $exception) {
    $failure = $exception;
}
if ($failure !== null) {
    foreach ($handles as $handle) {
        try {
            $handle->terminate('Lifetime qualification cleanup');
        } catch (Throwable $cleanupFailure) {
            fwrite(STDERR, 'Cleanup: '.$cleanupFailure->getMessage().PHP_EOL);
        }
    }
}
try {
    posix_kill($pid, SIGTERM);
    $shutdownDeadline = microtime(true) + 15;
    while (pcntl_waitpid($pid, $status, WNOHANG) === 0) {
        if (microtime(true) >= $shutdownDeadline) {
            posix_kill($pid, SIGKILL);
            pcntl_waitpid($pid, $status);
            throw new RuntimeException('The lifetime worker failed to stop gracefully.');
        }
        usleep(50_000);
    }
    if (!pcntl_wifexited($status) || pcntl_wexitstatus($status) !== 0) {
        throw new RuntimeException('The lifetime worker exited unsuccessfully.');
    }
} catch (Throwable $cleanupFailure) {
    $failure ??= $cleanupFailure;
}
if ($failure !== null) {
    throw $failure;
}
if (($client->listWorkers($queue)['workers'] ?? []) !== []) {
    throw new RuntimeException('The lifetime worker registration survived orderly shutdown.');
}
echo json_encode([
    'sdk_version' => $version, 'sdk_reference' => $reference,
    'workflow_ids' => array_map(static fn ($handle): string => $handle->workflowId, $handles),
    'nested_state_fresh' => true, 'query_update_state_fresh' => true,
    'shared_dependency_preserved' => true, 'worker_deregistered' => true,
], JSON_THROW_ON_ERROR).PHP_EOL;
