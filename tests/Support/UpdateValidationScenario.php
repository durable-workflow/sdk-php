<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests\Support;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\WorkflowContext;
use RuntimeException;

/**
 * durable-workflow/.github#234: a validator-capable Server leases one update
 * validation task through the multiplexed workflow poll. The worker runs only
 * the declared validator against the task's committed history and approves or
 * rejects the update. The validator refuses an amount that does not exceed
 * every update the history already accepted.
 */
final class UpdateValidationScenario
{
    /**
     * @param list<mixed> $arguments the update arguments
     * @param list<array<string, mixed>> $history
     * @return array{commands: list<array<string, mixed>>, handler_calls: int}
     */
    public static function run(array $arguments, array $history): array
    {
        $codec = new AvroPayloadCodec();
        $commands = [];
        $handlerCalls = 0;
        $leased = false;
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use (
            $arguments, $history, $codec, &$commands, &$leased,
        ): array {
            if (str_ends_with($uri, '/cluster/info')) {
                return ['worker_protocol' => ['version' => '1.20', 'server_capabilities' => [
                    'update_validation_tasks' => true,
                    'synchronous_update_validation' => [
                        'supported' => true, 'acceptance_boundary' => 'validator_approved',
                        'worker_capability' => 'update_validation_tasks', 'workflow_contract_field' => 'update_validators',
                        'task_poll' => ['strategy' => 'multiplexed', 'endpoint' => '/worker/workflow-tasks/poll',
                            'request_field' => 'task_kinds', 'task_kinds' => ['workflow', 'update_validation'],
                            'response_discriminator' => 'task.task_kind'],
                    ],
                ]]];
            }
            if (str_ends_with($uri, '/worker/register')) {
                return ['registered' => true];
            }
            if (str_ends_with($uri, '/workflow-tasks/poll') && !$leased) {
                $leased = true;
                if (($body['task_kinds'] ?? null) !== ['workflow', 'update_validation']) {
                    throw new RuntimeException('A validator worker must poll both workflow task kinds.');
                }

                return ['poll_status' => 'leased', 'task' => [
                    'task_kind' => 'update_validation', 'update_validation_task_id' => 'validation-1',
                    'update_id' => 'validation-1', 'update_validation_attempt' => 1, 'lease_owner' => 'validation-worker',
                    'workflow_id' => 'validated-workflow', 'run_id' => 'validated-run',
                    'workflow_type' => 'golden.update-validation', 'update_name' => 'approve', 'payload_codec' => 'avro',
                    'update_arguments' => $codec->envelope($arguments), 'history_events' => $history,
                ]];
            }
            if (preg_match('#/update-validation-tasks/validation-1/(approve|reject)$#', $uri, $match) === 1) {
                $failure = is_array($body['failure'] ?? null) ? $body['failure'] : [];
                $commands[] = array_filter([
                    'type' => $match[1].'_update_validation',
                    'update_validation_attempt' => $body['update_validation_attempt'] ?? null,
                    'reason' => $failure['reason'] ?? null,
                    'message' => $failure['message'] ?? null,
                    'validation_errors' => $failure['validation_errors'] ?? null,
                ], static fn (mixed $value): bool => $value !== null);

                return ['outcome' => $match[1] === 'approve' ? 'approved' : 'rejected'];
            }
            if (str_ends_with($uri, '/poll')) {
                return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
            }

            throw new RuntimeException('Unexpected update validation request: '.$method.' '.$uri);
        });
        $worker = new Worker(new Client('https://replay.invalid', transport: $transport, codec: $codec),
            'regression-corpus', workerId: 'validation-worker');
        $worker->registerWorkflow('golden.update-validation', static fn (WorkflowContext $context): string => 'waiting')
            ->registerUpdate('golden.update-validation', 'approve', static function (QueryContext $context, int $amount) use (&$handlerCalls): int {
                ++$handlerCalls;

                return $amount;
            })
            ->registerUpdateValidator('golden.update-validation', 'approve', static function (QueryContext $context, int $amount): void {
                $accepted = count($context->events('UpdateAccepted'));
                if ($amount <= $accepted) {
                    throw new \DomainException(sprintf('The amount must exceed the %d accepted updates.', $accepted));
                }
            });
        $worker->run(0);

        return ['commands' => $commands, 'handler_calls' => $handlerCalls];
    }
}
