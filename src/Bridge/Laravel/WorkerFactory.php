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
        private readonly string $credentialRole = 'worker',
    ) {
        if (!in_array($this->credentialRole, ['worker', 'shared'], true)) {
            throw new InvalidArgumentException('The Laravel worker credential role must be worker or shared.');
        }
    }

    public function make(?string $taskQueue = null): Worker
    {
        if ($this->configuration->handlers === []) {
            throw new InvalidArgumentException(
                'No Durable Workflow handlers are configured. Add attributed service classes to durable-workflow.handlers.',
            );
        }

        $worker = null;
        $worker = Worker::create(
            $this->client,
            $this->configuration->taskQueue($taskQueue),
            logger: $this->logger,
            diagnosticListener: function (string $name, array $context) use (&$worker): void {
                $this->events?->dispatch(new WorkerDiagnosticEvent($name, $context));
                if ($name !== 'worker.registered' || !$worker instanceof Worker) {
                    return;
                }

                $contracts = $worker->contracts();
                $readiness = [
                    'runtime_host' => $this->runtimeHost(),
                    'namespace' => $this->configuration->namespace,
                    'task_queue' => $worker->taskQueue,
                    'workflow_types' => $contracts['workflows'],
                    'activity_types' => $contracts['activities'],
                    'credential_role' => $this->credentialRole,
                ];
                $this->logger->info('worker.registered_and_polling', $readiness);
                $this->events?->dispatch(new WorkerDiagnosticEvent('worker.registered_and_polling', $readiness));
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

    private function runtimeHost(): string
    {
        $parts = parse_url($this->configuration->endpoint);
        $host = is_array($parts) ? (string) ($parts['host'] ?? '') : '';
        $port = is_array($parts) ? ($parts['port'] ?? null) : null;

        return is_int($port) ? "{$host}:{$port}" : $host;
    }
}
