<?php

declare(strict_types=1);

namespace DurableWorkflow\Bridge\Laravel;

use DurableWorkflow\Bridge\Event\WorkerDiagnosticEvent;
use DurableWorkflow\Bridge\ServiceConfiguration;
use DurableWorkflow\Client;
use DurableWorkflow\Worker;
use Illuminate\Contracts\Container\Container;
use Illuminate\Contracts\Events\Dispatcher;
use InvalidArgumentException;
use Psr\Log\LoggerInterface;

/** Builds workers whose attributed handlers come from Laravel's container. */
final class WorkerFactory
{
    public function __construct(
        private readonly Container $container,
        private readonly ServiceConfiguration $configuration,
        private readonly Client $client,
        private readonly LoggerInterface $logger,
        private readonly ?Dispatcher $events = null,
    ) {
    }

    public function make(?string $taskQueue = null): Worker
    {
        if ($this->configuration->handlers === []) {
            throw new InvalidArgumentException(
                'No Durable Workflow handlers are configured. Add attributed service classes to durable-workflow.handlers.',
            );
        }

        $worker = Worker::create(
            $this->client,
            $this->configuration->taskQueue($taskQueue),
            logger: $this->logger,
            diagnosticListener: function (string $name, array $context): void {
                $this->events?->dispatch(new WorkerDiagnosticEvent($name, $context));
            },
        );

        $handlers = [];
        foreach ($this->configuration->handlers as $handler) {
            $handlers[] = $this->container->make($handler);
        }

        return $worker->register(...$handlers);
    }

    public function pollTimeoutSeconds(): int
    {
        return $this->configuration->pollTimeoutSeconds;
    }
}
