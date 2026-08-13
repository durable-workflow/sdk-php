<?php

declare(strict_types=1);

require $argv[1];

use DurableWorkflow\Attribute\Activity;
use DurableWorkflow\Attribute\Workflow;
use DurableWorkflow\Auth\Authentication;
use DurableWorkflow\Bridge\Event\WorkerDiagnosticEvent;
use DurableWorkflow\Bridge\Symfony\DependencyInjection\DurableWorkflowExtension;
use DurableWorkflow\Bridge\Symfony\DurableWorkflowBundle;
use DurableWorkflow\Bridge\Symfony\Testing\InteractsWithDurableWorkflow;
use DurableWorkflow\Bridge\Symfony\WorkerFactory;
use DurableWorkflow\Client;
use DurableWorkflow\Testing\WorkflowClientFake;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\WorkflowContext;
use DurableWorkflow\WorkflowClientInterface;
use Psr\Log\AbstractLogger;
use Psr\Log\LoggerInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\EventDispatcher\EventDispatcher;
use Symfony\Contracts\EventDispatcher\EventDispatcherInterface;

final class SymfonyGreetingDependency
{
}

final class SymfonyGreetingHandler
{
    public function __construct(public readonly SymfonyGreetingDependency $dependency)
    {
    }

    #[Workflow('symfony.greeting')]
    public function workflow(WorkflowContext $context, string $name): string
    {
        return $context->activity('symfony.greet', [$name]);
    }

    #[Activity('symfony.greet')]
    public function activity(ActivityContext $context, string $name): string
    {
        return "hello, {$name}";
    }
}

final class SymfonyBridgeLogger extends AbstractLogger
{
    /** @var list<string> */
    public array $messages = [];

    public function log($level, string|Stringable $message, array $context = []): void
    {
        $this->messages[] = (string) $message;
    }
}

function symfonyClientAuthentication(Client $client): Authentication
{
    $property = (new ReflectionClass($client))->getProperty('authentication');
    $authentication = $property->getValue($client);
    if (!$authentication instanceof Authentication) {
        throw new RuntimeException('Symfony role-specific client did not retain authentication.');
    }

    return $authentication;
}

function symfonyAuthenticationRejectsRole(Authentication $authentication, bool $workerRequest): bool
{
    try {
        $authentication->headers($workerRequest);
    } catch (InvalidArgumentException) {
        return true;
    }

    return false;
}

final class SymfonyBridgeTestingContainer
{
    public ?object $workflowClient = null;

    public function set(string $id, object $service): void
    {
        if ($id === WorkflowClientInterface::class) {
            $this->workflowClient = $service;
        }
    }
}

final class SymfonyBridgeKernelTest
{
    use InteractsWithDurableWorkflow;

    public static SymfonyBridgeTestingContainer $container;

    public static function getContainer(): SymfonyBridgeTestingContainer
    {
        return self::$container;
    }

    public function fake(): WorkflowClientFake
    {
        return $this->fakeDurableWorkflow();
    }
}

$logger = new SymfonyBridgeLogger();
$events = new EventDispatcher();
$diagnostics = [];
$events->addListener('worker.shutdown_requested', static function (WorkerDiagnosticEvent $event) use (&$diagnostics): void {
    $diagnostics[] = $event->name;
});
$container = new ContainerBuilder();
$container->register(SymfonyGreetingDependency::class, SymfonyGreetingDependency::class);
$container->register(LoggerInterface::class)->setSynthetic(true)->setPublic(true);
$container->register(EventDispatcherInterface::class)->setSynthetic(true)->setPublic(true);
$bundle = new DurableWorkflowBundle();
$bundle->build($container);
$extension = new DurableWorkflowExtension();
$extension->load([[
    'endpoint' => 'http://localhost:8080',
    'namespace' => 'default',
    'task_queue' => 'symfony-workers',
    'credentials' => [
        'control_token' => 'control-secret',
        'worker_token' => 'worker-secret',
    ],
]], $container);
$container->register(SymfonyGreetingHandler::class, SymfonyGreetingHandler::class)
    ->setAutowired(true)
    ->setAutoconfigured(true);
$container->compile();
$container->set(LoggerInterface::class, $logger);
$container->set(EventDispatcherInterface::class, $events);

$bundleExtension = $bundle->getContainerExtension();
if (!$bundleExtension instanceof DurableWorkflowExtension || !trait_exists(InteractsWithDurableWorkflow::class)) {
    throw new RuntimeException('Symfony Bundle or testing helper could not be discovered.');
}
$testingContainer = new SymfonyBridgeTestingContainer();
SymfonyBridgeKernelTest::$container = $testingContainer;
$testingFake = (new SymfonyBridgeKernelTest())->fake();
if ($testingContainer->workflowClient !== $testingFake) {
    throw new RuntimeException('Symfony testing helper did not replace the workflow client.');
}

$registeredClient = $container->get(Client::class);
if (!$registeredClient instanceof Client || $container->get(WorkflowClientInterface::class) !== $registeredClient) {
    throw new RuntimeException('Symfony extension did not register autowired client services.');
}
$factory = $container->get(WorkerFactory::class);
if (!$factory instanceof WorkerFactory) {
    throw new RuntimeException('Symfony extension did not register the worker factory.');
}
$workerClientProperty = (new ReflectionClass($factory))->getProperty('client');
$workerClient = $workerClientProperty->getValue($factory);
if (!$workerClient instanceof Client) {
    throw new RuntimeException('Symfony worker factory did not receive a client.');
}
$controlAuthentication = symfonyClientAuthentication($registeredClient);
$workerAuthentication = symfonyClientAuthentication($workerClient);
if (($controlAuthentication->headers(false)['Authorization'] ?? null) !== 'Bearer control-secret'
    || !symfonyAuthenticationRejectsRole($controlAuthentication, true)
    || ($workerAuthentication->headers(true)['Authorization'] ?? null) !== 'Bearer worker-secret'
    || !symfonyAuthenticationRejectsRole($workerAuthentication, false)
) {
    throw new RuntimeException('Symfony extension did not preserve role-specific client credentials.');
}
$worker = $factory->make();
if ($worker->contracts()['workflows'] !== ['symfony.greeting']
    || $worker->contracts()['activities'] !== ['symfony.greet']
) {
    throw new RuntimeException('Symfony autowired handlers were not registered.');
}
$worker->requestShutdown();
if ($diagnostics !== ['worker.shutdown_requested']
    || $logger->messages !== ['worker.shutdown_requested']
) {
    throw new RuntimeException('Symfony logging and event diagnostics were not connected.');
}

fwrite(STDOUT, 'Symfony bridge compatibility passed.'.PHP_EOL);
