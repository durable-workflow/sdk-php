<?php

declare(strict_types=1);

namespace DurableWorkflow\Bridge\Symfony;

use DurableWorkflow\Bridge\FrameworkRuntimeException;
use DurableWorkflow\Bridge\ServiceConfiguration;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Throwable;

/** Runs a supervised service-mode worker from Symfony Console. */
final class WorkerCommand extends Command
{
    public function __construct(private readonly WorkerFactory $workers)
    {
        parent::__construct('durable-workflow:worker');
    }

    protected function configure(): void
    {
        $this
            ->setDescription('Run the Durable Workflow service-mode worker')
            ->addOption('queue', null, InputOption::VALUE_REQUIRED, 'Override the configured task queue')
            ->addOption('poll-timeout', null, InputOption::VALUE_REQUIRED, 'Long-poll timeout in seconds (0-60)');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        try {
            ServiceConfiguration::assertWorkerExtensions();
            $queue = $input->getOption('queue');
            $worker = $this->workers->make(is_string($queue) && $queue !== '' ? $queue : null);
            $output->writeln("<info>Starting Durable Workflow worker on task queue {$worker->taskQueue}.</info>");
            $worker->run($this->pollTimeout($input->getOption('poll-timeout')));

            return self::SUCCESS;
        } catch (FrameworkRuntimeException $exception) {
            throw $exception;
        } catch (Throwable $exception) {
            throw FrameworkRuntimeException::fromThrowable($exception);
        }
    }

    private function pollTimeout(mixed $value): int
    {
        if ($value === null || $value === '') {
            return $this->workers->pollTimeoutSeconds();
        }
        if (!is_string($value) || !ctype_digit($value) || (int) $value > 60) {
            throw new \InvalidArgumentException('The --poll-timeout option must be an integer between 0 and 60.');
        }

        return (int) $value;
    }
}
