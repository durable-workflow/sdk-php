<?php

declare(strict_types=1);

namespace DurableWorkflow\Bridge\Laravel;

use DurableWorkflow\Bridge\Event\WorkerDiagnosticEvent;
use DurableWorkflow\Bridge\FrameworkRuntimeException;
use DurableWorkflow\Bridge\ServiceConfiguration;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Console\Command;
use Throwable;

/** Runs a supervised service-mode worker from Artisan. */
final class WorkerCommand extends Command
{
    protected $signature = 'durable-workflow:worker
        {--queue= : Override the configured task queue}
        {--poll-timeout= : Long-poll timeout in seconds (0-60)}';

    protected $description = 'Run the Durable Workflow service-mode worker';

    public function __construct(
        private readonly WorkerFactory $workers,
        private readonly ?Dispatcher $events = null,
    ) {
        parent::__construct();
    }

    public function handle(): int
    {
        try {
            ServiceConfiguration::assertWorkerExtensions();
            $queue = $this->option('queue');
            $timeout = $this->pollTimeout();
            $worker = $this->workers->make(is_string($queue) && $queue !== '' ? $queue : null);
            $this->events?->listen(WorkerDiagnosticEvent::class, function (WorkerDiagnosticEvent $event): void {
                if ($event->name !== 'worker.registered_and_polling') {
                    return;
                }

                $workflowTypes = $event->context['workflow_types'] ?? [];
                $activityTypes = $event->context['activity_types'] ?? [];
                $workflows = is_array($workflowTypes)
                    ? implode(', ', array_filter($workflowTypes, 'is_string'))
                    : '';
                $activities = is_array($activityTypes)
                    ? implode(', ', array_filter($activityTypes, 'is_string'))
                    : '';
                $this->components->info(sprintf(
                    'Registered and polling: runtime=%s namespace=%s queue=%s workflows=[%s] activities=[%s] credential_role=%s.',
                    (string) ($event->context['runtime_host'] ?? ''),
                    (string) ($event->context['namespace'] ?? ''),
                    (string) ($event->context['task_queue'] ?? ''),
                    $workflows,
                    $activities,
                    (string) ($event->context['credential_role'] ?? ''),
                ));
            });
            $this->components->info("Starting Durable Workflow worker on task queue {$worker->taskQueue}.");
            $worker->run($timeout);

            return self::SUCCESS;
        } catch (FrameworkRuntimeException $exception) {
            throw $exception;
        } catch (Throwable $exception) {
            throw FrameworkRuntimeException::fromThrowable($exception);
        }
    }

    private function pollTimeout(): int
    {
        $value = $this->option('poll-timeout');
        if ($value === null || $value === '') {
            return $this->workers->pollTimeoutSeconds();
        }
        if (!is_string($value) || !ctype_digit($value) || (int) $value > 60) {
            throw new \InvalidArgumentException('The --poll-timeout option must be an integer between 0 and 60.');
        }

        return (int) $value;
    }
}
