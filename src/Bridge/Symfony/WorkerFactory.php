<?php

declare(strict_types=1);

namespace DurableWorkflow\Bridge\Symfony;

use DurableWorkflow\Bridge\Event\WorkerDiagnosticEvent;
use DurableWorkflow\Bridge\ServiceConfiguration;
use DurableWorkflow\Client;
use DurableWorkflow\Worker;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Symfony\Contracts\EventDispatcher\EventDispatcherInterface;

/** Builds workers from Symfony's tagged, autowired handler services. */
final class WorkerFactory
{
    /**
     * @param iterable<object> $handlers
     */
    public function __construct(
        private readonly ServiceConfiguration $configuration,
        private readonly Client $client,
        private readonly iterable $handlers,
        private readonly ?LoggerInterface $logger = null,
        private readonly ?EventDispatcherInterface $events = null,
    ) {
    }

    public function make(?string $taskQueue = null): Worker
    {
        $worker = Worker::create(
            $this->client,
            $this->configuration->taskQueue($taskQueue),
            logger: $this->logger ?? new NullLogger(),
            diagnosticListener: function (string $name, array $context): void {
                $event = new WorkerDiagnosticEvent($name, $context);
                $this->events?->dispatch($event, $name);
            },
        );

        $handlers = [];
        foreach ($this->handlers as $handler) {
            $handlers[] = $handler;
        }
        if ($handlers === []) {
            throw new \InvalidArgumentException(
                'No Durable Workflow handlers are available. Configure at least one attributed service under durable_workflow.handlers.',
            );
        }

        return $worker->register(...$handlers);
    }

    public function pollTimeoutSeconds(): int
    {
        return $this->configuration->pollTimeoutSeconds;
    }
}
