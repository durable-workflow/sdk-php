<?php

declare(strict_types=1);

namespace DurableWorkflow\Bridge\Laravel;

use DurableWorkflow\Bridge\FrameworkRuntimeException;
use DurableWorkflow\Bridge\ServiceConfiguration;
use Illuminate\Console\Command;
use Throwable;

/** Runs a supervised service-mode worker from Artisan. */
final class WorkerCommand extends Command
{
    protected $signature = 'durable-workflow:worker
        {--queue= : Override the configured task queue}
        {--poll-timeout= : Long-poll timeout in seconds (0-60)}';

    protected $description = 'Run the Durable Workflow service-mode worker';

    public function __construct(private readonly WorkerFactory $workers)
    {
        parent::__construct();
    }

    public function handle(): int
    {
        try {
            ServiceConfiguration::assertWorkerExtensions();
            $queue = $this->option('queue');
            $timeout = $this->pollTimeout();
            $worker = $this->workers->make(is_string($queue) && $queue !== '' ? $queue : null);
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
