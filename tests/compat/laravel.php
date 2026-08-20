<?php

declare(strict_types=1);

require $argv[1];

use DurableWorkflow\Attribute\Activity;
use DurableWorkflow\Attribute\Workflow;
use DurableWorkflow\Auth\Authentication;
use DurableWorkflow\Bridge\Event\WorkerDiagnosticEvent;
use DurableWorkflow\Bridge\Laravel\DurableWorkflowServiceProvider;
use DurableWorkflow\Bridge\Laravel\Facades\DurableWorkflow as DurableWorkflowFacade;
use DurableWorkflow\Bridge\Laravel\LaravelWorkflowClientInterface;
use DurableWorkflow\Bridge\Laravel\WorkerFactory;
use DurableWorkflow\Bridge\Laravel\WorkflowStartOptions;
use DurableWorkflow\Client;
use DurableWorkflow\Testing\WorkerTestHarness;
use DurableWorkflow\Transport\Transport;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\WorkflowContext;
use DurableWorkflow\WorkflowClientInterface;
use Illuminate\Config\Repository;
use Illuminate\Events\Dispatcher;
use Illuminate\Foundation\Application;
use Illuminate\Support\Facades\Facade;
use Psr\Log\AbstractLogger;
use Psr\Log\LoggerInterface;

final class LaravelGreetingPrefix
{
    public function __construct(public readonly string $value)
    {
    }
}

final class LaravelGreetingWorkflow
{
    public function __construct(private readonly LaravelGreetingPrefix $prefix)
    {
    }

    #[Workflow('laravel.greeting')]
    public function workflow(WorkflowContext $context, string $name): string
    {
        if ($this->prefix->value === '') {
            throw new RuntimeException('Laravel did not inject the workflow application dependency.');
        }

        return $context->activity('laravel.greet', [$name]);
    }
}

final class LaravelGreetingActivity
{
    public function __construct(private readonly LaravelGreetingPrefix $prefix)
    {
    }

    #[Activity('laravel.greet')]
    public function activity(ActivityContext $context, string $name): string
    {
        return "{$this->prefix->value}, {$name}";
    }
}

final class LaravelBridgeLogger extends AbstractLogger
{
    /** @var list<string> */
    public array $messages = [];
    /** @var list<array<string, mixed>> */
    public array $contexts = [];

    public function log($level, string|Stringable $message, array $context = []): void
    {
        $this->messages[] = (string) $message;
        $this->contexts[] = $context;
    }
}

final class LaravelBridgeTransport implements Transport
{
    /** @param list<array<string, mixed>> $responses */
    public function __construct(private array $responses)
    {
    }

    public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
    {
        return array_shift($this->responses);
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
    'runtime_url' => 'http://localhost:8080',
    'namespace' => 'default',
    'task_queue' => 'laravel-workers',
    'credentials' => [
        'client_token' => 'configuration-client-decoy',
        'worker_token' => 'configuration-worker-decoy',
    ],
    'handlers' => [LaravelGreetingWorkflow::class, LaravelGreetingActivity::class],
    'poll_timeout_seconds' => 5,
];
$app = new Application(sys_get_temp_dir().'/durable-workflow-laravel-compat');
$app->instance('config', new Repository(['durable-workflow' => $values]));
$app->instance(LaravelGreetingPrefix::class, new LaravelGreetingPrefix('hello from Laravel'));
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
$app->make(LaravelWorkflowClientInterface::class);
$factory = $app->make(WorkerFactory::class);
$workerClientProperty = (new ReflectionClass($factory))->getProperty('client');
$workerClient = $workerClientProperty->getValue($factory);
if (!$workerClient instanceof Client) {
    throw new RuntimeException('Laravel worker factory did not receive a client.');
}
$clientAuthentication = laravelClientAuthentication($registeredClient);
$workerAuthentication = laravelClientAuthentication($workerClient);
if (($clientAuthentication->headers(false)['Authorization'] ?? null) !== 'Bearer client-secret'
    || !laravelAuthenticationRejectsRole($clientAuthentication, true)
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
$harness = new WorkerTestHarness($worker);
$harness->assertActivityResult('laravel.greet', 'hello from Laravel, Ada', ['Ada']);
$codec = $registeredClient->payloadCodec();
$completed = $harness->runWorkflow('laravel.greeting', ['Ada'], [
    ['event_type' => 'ActivityScheduled', 'payload' => ['sequence' => 1, 'activity_type' => 'laravel.greet']],
    [
        'event_type' => 'ActivityCompleted',
        'payload' => [
            'sequence' => 1,
            'activity_type' => 'laravel.greet',
            'result' => $codec->envelope('hello from Laravel, Ada'),
        ],
    ],
]);
if (($completed->commands[0]['type'] ?? null) !== 'complete_workflow'
    || $codec->decodeEnvelope($completed->commands[0]['result'] ?? []) !== 'hello from Laravel, Ada'
) {
    throw new RuntimeException('Laravel container-resolved workflow did not complete with its injected activity.');
}
$worker->requestShutdown();
if ($diagnostics !== ['worker.shutdown_requested']
    || $logger->messages !== ['worker.shutdown_requested']
) {
    throw new RuntimeException('Laravel logging and event diagnostics were not connected.');
}

$readinessDiagnostics = [];
$events->listen(WorkerDiagnosticEvent::class, static function (WorkerDiagnosticEvent $event) use (&$readinessDiagnostics): void {
    $readinessDiagnostics[$event->name] = $event->context;
});
$readinessLogger = new LaravelBridgeLogger();
$readinessFactory = new WorkerFactory(
    $app,
    $app->make(DurableWorkflow\Bridge\ServiceConfiguration::class),
    DurableWorkflow\Bridge\Laravel\ProcessCredentialResolver::workerClient(
        $app->make(DurableWorkflow\Bridge\ServiceConfiguration::class),
        new LaravelBridgeTransport([
            ['registered' => true],
            ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'],
        ]),
    ),
    $readinessLogger,
    $events,
    'worker',
);
$readinessFactory->make()->run(0);
$readiness = $readinessDiagnostics['worker.registered_and_polling'] ?? null;
if ($readiness !== [
    'runtime_host' => 'localhost:8080',
    'namespace' => 'default',
    'task_queue' => 'laravel-workers',
    'workflow_types' => ['laravel.greeting'],
    'activity_types' => ['laravel.greet'],
    'credential_role' => 'worker',
] || !in_array('worker.registered_and_polling', $readinessLogger->messages, true)) {
    throw new RuntimeException('Laravel worker readiness did not identify its runtime and registered contracts.');
}

Facade::setFacadeApplication($app);
$fake = DurableWorkflowFacade::fake();
if ($app->make(LaravelWorkflowClientInterface::class) !== $fake
    || $app->make(WorkflowClientInterface::class) !== $fake->workflowClient()
) {
    throw new RuntimeException('Laravel testing fake did not replace both injectable workflow client layers.');
}
$fake->setWorkflowResult('laravel-greeting-1', 'hello from Laravel, Ada');
$fakeHandle = DurableWorkflowFacade::start(
    LaravelGreetingWorkflow::class,
    ['Ada'],
    workflowId: 'laravel-greeting-1',
);
if ($fakeHandle->result() !== 'hello from Laravel, Ada') {
    throw new RuntimeException('Laravel testing fake did not return the configured workflow result.');
}
if ($app->make(LaravelWorkflowClientInterface::class)
    ->handle(LaravelGreetingWorkflow::class, 'laravel-greeting-1')
    ->result() !== 'hello from Laravel, Ada'
) {
    throw new RuntimeException('Laravel class-shaped workflow handle did not return the configured result.');
}
$fake->assertWorkflowStarted(
    LaravelGreetingWorkflow::class,
    ['Ada'],
    workflowId: 'laravel-greeting-1',
);
$fake->assertResultRequested('laravel-greeting-1');
$app->make(LaravelWorkflowClientInterface::class)->start(
    LaravelGreetingWorkflow::class,
    ['Grace'],
    workflowId: 'laravel-greeting-priority',
    options: new WorkflowStartOptions(taskQueue: 'priority-laravel-workers'),
);
$fake->assertWorkflowStarted(
    LaravelGreetingWorkflow::class,
    ['Grace'],
    workflowId: 'laravel-greeting-priority',
    options: new WorkflowStartOptions(taskQueue: 'priority-laravel-workers'),
);

fwrite(STDOUT, 'Laravel bridge compatibility passed.'.PHP_EOL);
