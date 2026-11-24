<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests\Support;

use DateTimeImmutable;
use DateTimeZone;
use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\WorkflowContext;
use RuntimeException;

/**
 * An offer/accept worker whose activity completion carries the next workflow lease without its answer.
 * The Server binds that lease to the legacy poll identity, so the worker must replay it through the legacy
 * route and run the carried workflow task to its result.
 */
final class OfferCarriedWorkflowScenario
{
    /**
     * @return array{commands: list<array<string, mixed>>, activity_callback_count: int,
     *     carried_legacy_polls: int, carried_offer_polls: int}
     */
    public static function run(): array
    {
        $codec = new AvroPayloadCodec();
        $callbacks = $legacy = $offered = $activityOffers = 0;
        $commands = $carriedId = null;
        $clock = new DateTimeImmutable('now', new DateTimeZone('UTC'));
        $caps = ['activity_completion_workflow_poll' => true];
        $workflow = ['task' => ['task_id' => 'workflow', 'workflow_task_attempt' => 1, 'task_kind' => 'workflow',
            'lease_owner' => 'corpus-worker', 'workflow_id' => 'workflow-instance', 'run_id' => 'run',
            'workflow_type' => 'corpus.offer-carried-workflow', 'payload_codec' => 'avro', 'history_events' => [],
            'lease_expires_at' => $clock->modify('+30 seconds')->format('Y-m-d\TH:i:s.u\Z')],
            'poll_status' => 'leased', 'protocol_version' => '1.20', 'server_capabilities' => $caps,
            'server_time' => $clock->format('Y-m-d\TH:i:s.u\Z')];
        $empty = ['task' => null, 'poll_status' => 'empty', 'server_capabilities' => $caps];
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use (
            $workflow, $caps, $empty, &$commands, &$carriedId, &$legacy, &$offered, &$activityOffers,
        ): array {
            if (str_ends_with($uri, '/worker/register')) {
                return ['registered' => true, 'registration_token' => str_repeat('c', 32), 'server_capabilities' => $caps + [
                    'task_delivery' => ['schema' => 'durable-workflow.task-delivery.v1', 'supported' => true,
                        'minimum_protocol_version' => '1.20', 'worker_capability' => 'task_delivery_offers',
                        'header' => 'X-Durable-Workflow-Task-Delivery', 'version' => 'offer-accept.v1',
                        'endpoint_suffix' => '/offers', 'acceptance_timeout_seconds' => 2,
                        'unaccepted_offer_reserves_task' => false],
                ]];
            }
            $pollId = $body['poll_request_id'] ?? null;
            if ($carriedId !== null && $pollId === $carriedId) {
                // Only the legacy route holds the carried lease's binding.
                if (str_ends_with($uri, '/workflow-tasks/poll')) {
                    ++$legacy;
                    return $workflow;
                }
                ++$offered;
                return $empty;
            }
            if (str_ends_with($uri, '/activity-tasks/offers')) {
                if (isset($body['task_offer_token'])) {
                    return ['task' => ['task_id' => 'activity', 'task_kind' => 'activity', 'activity_attempt_id' => 'attempt',
                        'lease_owner' => 'corpus-worker', 'activity_type' => 'corpus.activity', 'payload_codec' => 'avro'],
                        'poll_status' => 'leased', 'server_capabilities' => $caps];
                }
                return ++$activityOffers === 1 ? ['task' => null, 'poll_status' => 'offered', 'task_offer' => [
                    'schema' => 'durable-workflow.task-delivery.v1', 'task_kind' => 'activity', 'task_id' => 'activity',
                    'token' => 'synthetic-packet', 'expires_at_ms' => 1000,
                ]] : $empty;
            }
            if (str_ends_with($uri, '/activity-tasks/activity/complete')) {
                $carriedId = $body['workflow_poll']['poll_request_id'] ?? null;
                // The completion committed the carried lease, but this reply omits its answer.
                return ['completed' => true, 'workflow_poll' => ['leased' => true]];
            }
            if (str_ends_with($uri, '/workflow-tasks/workflow/complete')) {
                $commands = $body['commands'];
                return ['completed' => true];
            }
            if (str_ends_with($uri, '/workflow-tasks/workflow/heartbeat')) {
                return ['task_id' => 'workflow', 'workflow_task_attempt' => 1, 'lease_owner' => 'corpus-worker', 'renewed' => true];
            }
            if (preg_match('#/worker/(workflow|activity|query)-tasks/(poll|offers)$#', $uri) === 1) {
                return $empty;
            }
            throw new RuntimeException('Unexpected offer-carried workflow fixture request.');
        });
        $client = new Client('https://replay.invalid', transport: $transport);
        // tick() does not register; negotiate offer delivery on the Client the worker shares.
        $client->registerWorker('corpus-worker', 'corpus-queue', [], []);
        $worker = new Worker($client, 'corpus-queue', workerId: 'corpus-worker');
        $worker->registerWorkflow('corpus.offer-carried-workflow', static fn (WorkflowContext $context): string => 'offer-carried-workflow');
        $worker->registerActivity('corpus.activity', static function (ActivityContext $context) use (&$callbacks): int {
            return ++$callbacks;
        });
        $worker->tick(0);
        $worker->tick(0);
        if (!is_array($commands)) {
            throw new RuntimeException('The offer-carried workflow fixture did not complete its carried workflow task.');
        }
        foreach ($commands as &$command) {
            if (isset($command['result'])) {
                $command['result'] = $codec->decodeEnvelope($command['result']);
            }
        }
        unset($command);

        return ['commands' => $commands, 'activity_callback_count' => $callbacks,
            'carried_legacy_polls' => $legacy, 'carried_offer_polls' => $offered];
    }
}
