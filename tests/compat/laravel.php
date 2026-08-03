<?php

declare(strict_types=1);

require $argv[1];

use DurableWorkflow\Attribute\Activity;
use DurableWorkflow\Attribute\Workflow;
use DurableWorkflow\Bridge\Event\WorkerDiagnosticEvent;
use DurableWorkflow\Bridge\Laravel\DurableWorkflowServiceProvider;
use DurableWorkflow\Bridge\Laravel\Facades\DurableWorkflow as DurableWorkflowFacade;
use DurableWorkflow\Bridge\Laravel\WorkerFactory;
use DurableWorkflow\Bridge\ServiceConfiguration;
use DurableWorkflow\Client;
use DurableWorkflow\Testing\WorkflowClientFake;
use DurableWorkflow\Transport\Transport;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\WorkflowContext;
use DurableWorkflow\WorkflowClientInterface;
use Illuminate\Config\Repository;
use Illuminate\Events\Dispatcher;
use Illuminate\Foundation\Application;
use Illuminate\Support\Facades\Facade;
use Psr\Log\AbstractLogger;

final class LaravelGreetingHandler
{
    #[Workflow('laravel.greeting')]
    public function workflow(WorkflowContext $context, string $name): Generator
    {
        return yield $context->activity('laravel.greet', [$name]);
    }

    #[Activity('laravel.greet')]
    public function activity(ActivityContext $context, string $name): string
    {
        return "hello, {$name}";
    }
}

final class LaravelBridgeTransport implements Transport
{
    public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
    {
        throw new RuntimeException('Compatibility construction must not contact a runtime.');
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

$values = [
    'endpoint' => 'http://localhost:8080',
    'namespace' => 'default',
    'task_queue' => 'laravel-workers',
    'credentials' => [],
    'handlers' => [LaravelGreetingHandler::class],
    'poll_timeout_seconds' => 5,
];
$app = new Application(sys_get_temp_dir().'/durable-workflow-laravel-compat');
$app->instance('config', new Repository(['durable-workflow' => $values]));
$app->instance(Illuminate\Contracts\Events\Dispatcher::class, new Dispatcher($app));
$provider = new DurableWorkflowServiceProvider($app);
$provider->register();

$registeredClient = $app->make(Client::class);
if ($app->make(WorkflowClientInterface::class) !== $registeredClient) {
    throw new RuntimeException('Laravel provider did not register injectable client services.');
}

$events = new Dispatcher($app);
$diagnostics = [];
$events->listen(WorkerDiagnosticEvent::class, static function (WorkerDiagnosticEvent $event) use (&$diagnostics): void {
    $diagnostics[] = $event->name;
});
$logger = new LaravelBridgeLogger();
$factory = new WorkerFactory(
    $app,
    ServiceConfiguration::fromArray($values),
    new Client('http://localhost:8080', transport: new LaravelBridgeTransport()),
    $logger,
    $events,
);
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
