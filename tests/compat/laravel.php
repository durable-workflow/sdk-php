<?php

declare(strict_types=1);

require $argv[1];

use DurableWorkflow\Attribute\Activity;
use DurableWorkflow\Attribute\Workflow;
use DurableWorkflow\Auth\Authentication;
use DurableWorkflow\Bridge\Event\WorkerDiagnosticEvent;
use DurableWorkflow\Bridge\Laravel\DurableWorkflowServiceProvider;
use DurableWorkflow\Bridge\Laravel\Facades\DurableWorkflow as DurableWorkflowFacade;
use DurableWorkflow\Bridge\Laravel\WorkerFactory;
use DurableWorkflow\Client;
use DurableWorkflow\Testing\WorkflowClientFake;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\WorkflowContext;
use DurableWorkflow\WorkflowClientInterface;
use Illuminate\Config\Repository;
use Illuminate\Events\Dispatcher;
use Illuminate\Foundation\Application;
use Illuminate\Support\Facades\Facade;
use Psr\Log\AbstractLogger;
use Psr\Log\LoggerInterface;

final class LaravelGreetingHandler
{
    #[Workflow('laravel.greeting')]
    public function workflow(WorkflowContext $context, string $name): string
    {
        return $context->activity('laravel.greet', [$name]);
    }

    #[Activity('laravel.greet')]
    public function activity(ActivityContext $context, string $name): string
    {
        return "hello, {$name}";
    }
}

final class LaravelBridgeLogger extends AbstractLogger
{
    /** @var list<string> */
    public array $messages = [];

    public function log($level, string|Stringable $message, array $context = []): void
    {
        $this->messages[] = (string) $message;
    }
}

function laravelClientAuthentication(Client $client): Authentication
{
    $property = (new ReflectionClass($client))->getProperty('authentication');
    $authentication = $property->getValue($client);
    if (!$authentication instanceof Authentication) {
        throw new RuntimeException('Laravel role-specific client did not retain authentication.');
    }

    return $authentication;
}

function laravelAuthenticationRejectsRole(Authentication $authentication, bool $workerRequest): bool
{
    try {
        $authentication->headers($workerRequest);
    } catch (InvalidArgumentException) {
        return true;
    }

    return false;
}

$values = [
    'endpoint' => 'http://localhost:8080',
    'namespace' => 'default',
    'task_queue' => 'laravel-workers',
    'credentials' => [
        'control_token' => 'configuration-control-decoy',
        'worker_token' => 'configuration-worker-decoy',
    ],
    'handlers' => [LaravelGreetingHandler::class],
    'poll_timeout_seconds' => 5,
];
$app = new Application(sys_get_temp_dir().'/durable-workflow-laravel-compat');
$app->instance('config', new Repository(['durable-workflow' => $values]));
$events = new Dispatcher($app);
$diagnostics = [];
$events->listen(WorkerDiagnosticEvent::class, static function (WorkerDiagnosticEvent $event) use (&$diagnostics): void {
    $diagnostics[] = $event->name;
});
$logger = new LaravelBridgeLogger();
$app->instance(Illuminate\Contracts\Events\Dispatcher::class, $events);
$app->instance(LoggerInterface::class, $logger);
$provider = new DurableWorkflowServiceProvider($app);
$provider->register();

$registeredClient = $app->make(Client::class);
if ($app->make(WorkflowClientInterface::class) !== $registeredClient) {
    throw new RuntimeException('Laravel provider did not register injectable client services.');
}
$factory = $app->make(WorkerFactory::class);
$workerClientProperty = (new ReflectionClass($factory))->getProperty('client');
$workerClient = $workerClientProperty->getValue($factory);
if (!$workerClient instanceof Client) {
    throw new RuntimeException('Laravel worker factory did not receive a client.');
}
$controlAuthentication = laravelClientAuthentication($registeredClient);
$workerAuthentication = laravelClientAuthentication($workerClient);
if (($controlAuthentication->headers(false)['Authorization'] ?? null) !== 'Bearer control-secret'
    || !laravelAuthenticationRejectsRole($controlAuthentication, true)
    || ($workerAuthentication->headers(true)['Authorization'] ?? null) !== 'Bearer worker-secret'
    || !laravelAuthenticationRejectsRole($workerAuthentication, false)
) {
    throw new RuntimeException('Laravel provider did not preserve role-specific client credentials.');
}
$worker = $factory->make();
if ($worker->contracts()['workflows'] !== ['laravel.greeting']
    || $worker->contracts()['activities'] !== ['laravel.greet']
) {
    throw new RuntimeException('Laravel container handlers were not registered.');
}
$worker->requestShutdown();
if ($diagnostics !== ['worker.shutdown_requested']
    || $logger->messages !== ['worker.shutdown_requested']
) {
    throw new RuntimeException('Laravel logging and event diagnostics were not connected.');
}

Facade::setFacadeApplication($app);
$fake = DurableWorkflowFacade::fake();
if ($app->make(WorkflowClientInterface::class) !== $fake) {
    throw new RuntimeException('Laravel testing fake did not replace the injectable workflow client.');
}

fwrite(STDOUT, 'Laravel bridge compatibility passed.'.PHP_EOL);
