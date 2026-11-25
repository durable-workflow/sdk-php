<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests\Support;

use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowContext;
use Psr\Log\AbstractLogger;
use Stringable;

/**
 * durable-workflow/.github#227: each workflow task replays the run from its
 * committed history with a fresh Replayer, as a restarted worker does. The
 * replay-aware logger writes each step's line once, and a cold replay of the
 * completed run writes nothing.
 */
final class WorkflowLoggerScenario
{
    /**
     * @param list<array<string, mixed>> $history the completed run's history, in task order
     * @param list<int> $taskBoundaries history lengths at which each workflow task starts
     * @param list<mixed> $input
     * @param 'activities'|'selection-cancel'|string $variant
     * @return array{logged: list<string>, cold_replay_logged: list<string>, commands: list<list<string>>}
     */
    public static function run(array $history, array $taskBoundaries, array $input, string $variant = 'activities'): array
    {
        $codec = new AvroPayloadCodec();
        $logger = new class() extends AbstractLogger {
            /** @var list<string> */
            public array $messages = [];

            public function log($level, string|Stringable $message, array $context = []): void
            {
                $this->messages[] = (string) $message;
            }
        };
        $workflow = $variant === 'selection-cancel' ? self::selectionCancelWorkflow() : static function (WorkflowContext $context, string $name): string {
            $context->logger()->info('logger scenario started');
            $reserved = $context->activity('golden.reserve', [$name]);
            $context->logger()->info('logger scenario reserved', ['result' => $reserved]);
            $charged = $context->activity('golden.charge', [$name]);
            $context->logger()->info('logger scenario charged', ['result' => $charged]);

            return 'done';
        };
        $task = ['workflow_id' => 'logger-workflow', 'run_id' => 'logger-run', 'workflow_type' => 'golden.workflow-logger'];
        $commands = [];
        foreach ($taskBoundaries as $length) {
            $replayer = new Replayer($codec);
            $replayer->useWorkflowLogger($logger, 'default');
            $result = $replayer->replay($workflow, array_slice($history, 0, $length), $input, 'regression-corpus', $task);
            $commands[] = array_column($result->commands, 'type');
            self::write($logger, $replayer);
        }
        $logged = $logger->messages;

        $logger->messages = [];
        $replayer = new Replayer($codec);
        $replayer->useWorkflowLogger($logger, 'default');
        $replayer->replay($workflow, $history, $input, 'regression-corpus', $task);
        self::write($logger, $replayer);

        return ['logged' => $logged, 'cold_replay_logged' => $logger->messages, 'commands' => $commands];
    }

    /** Write a submitted replay's lines, as the Worker does after completing its task. */
    private static function write(\Psr\Log\LoggerInterface $logger, Replayer $replayer): void
    {
        foreach ($replayer->takeWorkflowLogs() as $record) {
            $logger->log($record['level'], $record['message'], $record['context']);
        }
    }

    /**
     * durable-workflow/.github#227: a select cancels one loser, then parks
     * awaiting the other. The cancellation is replayed from its marker without
     * a step, so it is not durable progress for the parked lines.
     */
    private static function selectionCancelWorkflow(): \Closure
    {
        return static function (WorkflowContext $context): mixed {
            $selected = $context->select([
                'a' => static fn () => $context->activity('lookup-a'),
                'b' => static fn () => $context->activity('lookup-b'),
                'deadline' => static function () use ($context): void {
                    $context->sleep(0);
                },
            ]);
            $context->logger()->info('logger scenario selection resolved');
            $selected->handles['a']->cancel();
            $context->logger()->info('logger scenario loser cancelled');
            $result = $selected->handles['b']->await();
            $context->logger()->info('logger scenario other loser done', ['result' => $result]);

            return $result;
        };
    }
}
