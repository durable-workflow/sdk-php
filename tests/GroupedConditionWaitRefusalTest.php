<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowClaimAborted;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\TestCase;

/**
 * sdk-php#278: native Server refuses condition waits inside all()/parallel() (server#1142).
 * On native, without the grouped_condition_waits capability, a new grouped wait fails the
 * workflow task with a clear message instead of a refused completion. PHP Server is unchanged.
 */
final class GroupedConditionWaitRefusalTest extends TestCase
{
    public function testNativeServerRefusesANewGroupedWaitAsATaskFailureAndTheWorkerKeepsRunning(): void
    {
        $run = self::runWorker(['implementation' => 'rust', 'version' => '3.4.0']);

        self::assertNull($run['completion']);
        self::assertStringContainsString('grouped_condition_wait_unsupported', $run['failure']['message'] ?? '');
        self::assertStringContainsString('Refused by Server native 3.4.0', $run['failure']['message'] ?? '');
        self::assertSame(1, $run['cluster_info_requests']);
        self::assertSame(2, $run['workflow_polls'], 'The worker kept polling after the refused task.');
    }

    public function testPhpServerKeepsSendingTheGroupedWait(): void
    {
        $run = self::runWorker(['version' => '2.11.8']);

        self::assertNull($run['failure']);
        self::assertContains('open_condition_wait', array_column($run['completion']['commands'] ?? [], 'type'));
    }

    public function testTheCapabilityOnNativeLiftsTheRefusal(): void
    {
        $run = self::runWorker(['implementation' => 'rust', 'version' => '3.6.0',
            'worker_protocol' => ['server_capabilities' => ['grouped_condition_waits' => ['supported' => true, 'version' => 1]]]]);

        self::assertNull($run['failure']);
        self::assertContains('open_condition_wait', array_column($run['completion']['commands'] ?? [], 'type'));
    }

    public function testAWorkflowWithoutGroupedWaitsNeverAsksForClusterInfo(): void
    {
        $run = self::runWorker(['implementation' => 'rust', 'version' => '3.4.0'],
            static fn (WorkflowContext $context): array => $context->all([$context->deferTimer(2), $context->deferTimer(3)]));

        self::assertSame(0, $run['cluster_info_requests']);
        self::assertNull($run['failure']);
    }

    public function testARecordedGroupStillReplaysWhenRefused(): void
    {
        $handler = self::groupedWorkflow();
        $commands = (new Replayer(new AvroPayloadCodec()))->replay($handler, [], [], 'queue')->commands;
        $history = [];
        foreach ($commands as $offset => $command) {
            $sequence = $offset + 1;
            $metadata = array_intersect_key($command, array_flip(['parallel_group_id', 'parallel_group_kind', 'parallel_group_mode',
                'parallel_group_base_sequence', 'parallel_group_size', 'parallel_group_index', 'parallel_group_path']));
            $body = ['sequence' => $sequence];
            if ($command['type'] === 'open_condition_wait') {
                $body += ['condition_wait_id' => 'wait-'.$sequence, 'condition_key' => $command['condition_key'],
                    'condition_definition_fingerprint' => $command['condition_definition_fingerprint'], 'timeout_seconds' => 300];
                $history[] = ['event_type' => 'ConditionWaitOpened', 'payload' => $body + $metadata];
                $history[] = ['event_type' => 'ConditionWaitSatisfied', 'payload' => $body + $metadata];
            } else {
                $body += ['timer_id' => 'timer-'.$sequence, 'delay_seconds' => 3];
                $history[] = ['event_type' => 'TimerScheduled', 'payload' => $body + $metadata];
                $history[] = ['event_type' => 'TimerFired', 'payload' => $body + $metadata];
            }
        }

        $replayer = new Replayer(new AvroPayloadCodec());
        $replayer->refuseNewGroupedConditionWaits(static fn (): string => 'native 3.4.0');
        $result = $replayer->replay($handler, $history, [], 'queue', ['workflow_id' => 'w', 'run_id' => 'r']);
        self::assertSame(['complete_workflow'], array_column($result->commands, 'type'));

        $this->expectException(WorkflowClaimAborted::class);
        $replayer->replay($handler, [], [], 'queue');
    }

    private static function groupedWorkflow(): \Closure
    {
        return static fn (WorkflowContext $context): array => $context->all([
            $context->deferCondition(static fn (): bool => $context->signals('approve') !== [], 'approval', 300),
            $context->deferTimer(3),
        ]);
    }

    /**
     * @param array<string, mixed> $clusterInfo
     * @return array{completion: ?array<string, mixed>, failure: ?array<string, mixed>, cluster_info_requests: int, workflow_polls: int}
     */
    private static function runWorker(array $clusterInfo, ?\Closure $workflow = null): array
    {
        $state = ['completion' => null, 'failure' => null, 'cluster_info_requests' => 0, 'workflow_polls' => 0];
        $worker = null;
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use (&$state, &$worker, $clusterInfo): array {
            if (str_ends_with($uri, '/api/worker/register')) {
                return ['registered' => true, 'heartbeat_interval_seconds' => 30];
            }
            if (str_contains($uri, '/cluster/info')) {
                ++$state['cluster_info_requests'];

                return $clusterInfo;
            }
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                if (++$state['workflow_polls'] > 1) {
                    $worker?->requestShutdown();

                    return ['task' => null, 'poll_status' => 'empty'];
                }

                return ['poll_status' => 'leased', 'task' => ['task_id' => 'task', 'workflow_task_attempt' => 1,
                    'workflow_id' => 'w', 'run_id' => 'r', 'workflow_type' => 'grouped', 'lease_owner' => 'worker-1',
                    'payload_codec' => 'avro', 'history_events' => []]];
            }
            if (str_ends_with($uri, '/workflow-tasks/task/complete')) {
                $state['completion'] = $body;

                return ['completed' => true];
            }
            if (str_ends_with($uri, '/workflow-tasks/task/fail')) {
                $state['failure'] = $body['failure'] ?? $body;

                return ['failed' => true];
            }
            if (str_ends_with($uri, '/workflow-tasks/task/heartbeat')) {
                return ['task_id' => 'task', 'workflow_task_attempt' => 1, 'lease_owner' => 'worker-1', 'renewed' => true];
            }
            if (str_ends_with($uri, '/deregister') || str_ends_with($uri, '/heartbeat')) {
                return ['acknowledged' => true];
            }

            return ['task' => null, 'poll_status' => 'empty'];
        });
        $worker = new Worker(new Client('https://server.example', transport: $transport), 'queue', workerId: 'worker-1',
            clock: static fn (): float => 0.0);
        $worker->registerWorkflow('grouped', $workflow ?? self::groupedWorkflow());
        $worker->run(0);

        return $state;
    }
}
