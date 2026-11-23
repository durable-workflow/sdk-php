<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\NonDeterministicWorkflow;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ContinueAsNewTargetBuild;
use DurableWorkflow\Worker\Replayer;
use DurableWorkflow\Worker\WorkflowClaimDeferred;
use DurableWorkflow\Worker\WorkflowContext;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ContinueAsNewTargetBuildTest extends TestCase
{
    private const PROTOCOL_HEADER = 'X-Durable-Workflow-Protocol-Version';

    public function testClosedPredecessorReplaysItsRecordedChoiceWithoutCurrentCapability(): void
    {
        $replayer = new Replayer(new AvroPayloadCodec());
        $start = ['event_type' => 'WorkflowStarted'];
        $closed = ['event_type' => 'WorkflowContinuedAsNew', 'payload' => [
            'sequence' => 1,
            'build_transition' => ['from_compatibility' => null, 'to_compatibility' => 'build-b', 'resolution' => 'promoted'],
        ]];
        $promoted = static fn (WorkflowContext $context): never => $context->continueAsNew(
            ['next'], targetBuild: ContinueAsNewTargetBuild::Promoted,
        );
        self::assertSame([], $replayer->replay($promoted, [$start, $closed], [], 'queue')->commands);
        $inherited = static fn (WorkflowContext $context): never => $context->continueAsNew(['next']);
        $legacyClose = ['event_type' => 'WorkflowContinuedAsNew', 'payload' => ['sequence' => 1]];
        $legacy = $replayer->replay($inherited, [$start, $legacyClose], [], 'queue')->commands;
        self::assertSame(['continue_as_new'], array_column($legacy, 'type'));
        self::assertArrayNotHasKey('target_build', $legacy[0]);

        $successor = ['event_type' => 'WorkflowStarted', 'payload' => $closed['payload']];
        $result = $replayer->replay(static fn (WorkflowContext $context, string $state): string => $state, [$successor], ['next'], 'queue');
        self::assertSame('complete_workflow', $result->commands[0]['type']);
        $this->expectException(WorkflowClaimDeferred::class);
        $this->expectExceptionMessage('unsupported_target_build');
        $replayer->replay($promoted, [$successor], [], 'queue');
    }

    public function testChangingTheAuthoredTargetOnReplayRefuses(): void
    {
        $history = [['event_type' => 'WorkflowContinuedAsNew', 'payload' => [
            'sequence' => 1,
            'build_transition' => ['from_compatibility' => 'build-a', 'to_compatibility' => 'build-b', 'resolution' => 'promoted'],
        ]]];
        try {
            (new Replayer(new AvroPayloadCodec()))->replay(
                static fn (WorkflowContext $context): never => $context->continueAsNew(),
                $history, [], 'queue',
            );
            self::fail('Replay accepted a changed target.');
        } catch (NonDeterministicWorkflow $error) {
            self::assertSame('continue_as_new_target_changed', $error->reason);
        }
    }

    public function testClientGatesExplicitTargetsOnTheCurrentRegistrationAndUsesTheNewProtocolOnlyForContinuation(): void
    {
        $registration = static fn (string $version, mixed $flag): array => [
            'registered' => true,
            'worker_id' => 'worker',
            'protocol_version' => $version,
            'server_capabilities' => ['continue_as_new_target_build' => $flag],
        ];
        $transport = new FakeTransport([
            $registration('1.20', true),
            $registration('1.21', 'true'),
            $registration('1.21', true),
            ['recorded' => true],
            ['recorded' => true],
            ['recorded' => true],
            $registration('1.20', false),
        ]);
        $client = new Client('https://server.example', transport: $transport);
        $explicit = [['type' => 'continue_as_new', 'target_build' => 'inherited']];
        $assertUnsupported = static function (Client $client, string $owner) use ($explicit): void {
            try {
                $client->completeWorkflowTask('task', $owner, 1, $explicit);
                self::fail('The client sent an unnegotiated target.');
            } catch (WorkflowClaimDeferred $error) {
                self::assertStringContainsString('unsupported_target_build', $error->getMessage());
            }
        };
        $client->registerWorker('worker', 'queue', [], []);
        $assertUnsupported($client, 'worker');
        $client->registerWorker('worker', 'queue', [], []);
        $assertUnsupported($client, 'worker');
        $client->registerWorker('worker', 'queue', [], []);
        $assertUnsupported($client, 'another-worker');
        foreach ([null, false, [], 1, 'Promoted', 'promoted '] as $invalid) {
            try {
                $client->completeWorkflowTask('task', 'worker', 1, [['type' => 'continue_as_new', 'target_build' => $invalid]]);
                self::fail('The client sent an invalid target.');
            } catch (InvalidArgumentException $error) {
                self::assertStringContainsString('invalid_target_build', $error->getMessage());
            }
        }
        self::assertCount(3, $transport->requests);
        $client->completeWorkflowTask('task', 'worker', 1, $explicit);
        $client->completeWorkflowTask('legacy-task', 'worker', 1, [['type' => 'continue_as_new']]);
        $client->completeWorkflowTask('other-task', 'worker', 1, [['type' => 'complete_workflow']]);
        $client->registerWorker('worker', 'queue', [], []);
        $assertUnsupported($client, 'worker');
        self::assertCount(7, $transport->requests);
        self::assertSame('1.21', $transport->requests[3]['headers'][self::PROTOCOL_HEADER]);
        self::assertSame('inherited', $transport->requests[3]['body']['commands'][0]['target_build']);
        self::assertSame('1.21', $transport->requests[4]['headers'][self::PROTOCOL_HEADER]);
        self::assertArrayNotHasKey('target_build', $transport->requests[4]['body']['commands'][0]);
        self::assertSame('1.20', $transport->requests[5]['headers'][self::PROTOCOL_HEADER]);
    }

    #[DataProvider('workerCapabilities')]
    public function testWorkerReportsOnlyTheTypedPromotionRefusalAndContinuesPolling(bool $supported): void
    {
        $polls = 0;
        $diagnostics = [];
        $transport = new FakeTransport(handler: static function (string $method, string $uri) use ($supported, &$polls): array {
            if (str_ends_with($uri, '/worker/register')) {
                return [
                    'registered' => true, 'worker_id' => 'worker', 'protocol_version' => $supported ? '1.21' : '1.20',
                    'server_capabilities' => $supported ? ['continue_as_new_target_build' => true] : [],
                ];
            }
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                if (++$polls > ($supported ? 2 : 1)) {
                    return ['task' => null, 'poll_status' => 'stopped', 'reason' => 'worker_stopped'];
                }

                return ['poll_status' => 'leased', 'server_time' => '2026-01-01T00:00:00.000000Z', 'task' => [
                    'task_id' => "task-{$polls}", 'workflow_task_attempt' => $polls, 'lease_owner' => 'worker',
                    'lease_expires_at' => '2026-01-01T00:01:00.000000Z',
                    'workflow_id' => 'workflow', 'run_id' => 'run', 'workflow_type' => 'continue',
                    'payload_codec' => 'avro', 'arguments' => (new AvroPayloadCodec())->envelope([]),
                    'history_events' => [['event_type' => 'WorkflowStarted', 'sequence' => 1]],
                ]];
            }
            if (str_ends_with($uri, '/task-1/complete')) {
                throw new TransportException('The promoted build is unavailable.', 409, [
                    'reason' => 'promoted_build_unavailable', 'retryable' => true, 'request_admitted' => false,
                ]);
            }
            if (str_ends_with($uri, '/task-1/fail') || str_ends_with($uri, '/task-2/complete')) {
                return ['recorded' => true];
            }
            if (str_ends_with($uri, '/activity-tasks/poll') || str_ends_with($uri, '/query-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }
            if ($method === 'DELETE' || str_ends_with($uri, '/heartbeat')) {
                return [];
            }

            self::fail("Unexpected request {$method} {$uri}.");
        });
        $worker = new Worker(new Client('https://server.example', transport: $transport), 'queue', workerId: 'worker',
            diagnosticListener: static function (string $event, array $context) use (&$diagnostics): void {
                if ($event === 'worker.claim_deferred') {
                    $diagnostics[] = $context['reason'];
                }
            });
        $worker->registerWorkflow('continue', static fn (WorkflowContext $context): never => $context->continueAsNew(
            ['next'], targetBuild: ContinueAsNewTargetBuild::Promoted,
        ));
        $worker->run(0);
        $writes = array_values(array_filter($transport->requests,
            static fn (array $request): bool => preg_match('#/workflow-tasks/task-[12]/(?:complete|fail)$#', $request['uri']) === 1));
        if (!$supported) {
            self::assertSame(2, $polls);
            self::assertSame(['unsupported_target_build'], $diagnostics);
            self::assertSame([], $writes);

            return;
        }
        self::assertSame(3, $polls);
        self::assertCount(3, $writes);
        self::assertSame(['1.21', '1.21', '1.21'], array_map(
            static fn (array $request): string => $request['headers'][self::PROTOCOL_HEADER], $writes,
        ));
        self::assertSame('promoted', $writes[0]['body']['commands'][0]['target_build']);
        self::assertSame('promoted_build_unavailable', $writes[1]['body']['failure']['reason']);
        self::assertSame('worker', $writes[1]['body']['lease_owner']);
        self::assertSame(1, $writes[1]['body']['workflow_task_attempt']);
        self::assertSame(2, $writes[2]['body']['workflow_task_attempt']);
        self::assertSame('promoted', $writes[2]['body']['commands'][0]['target_build']);
    }

    public static function workerCapabilities(): array
    {
        return ['installed' => [true], 'older server' => [false]];
    }
}
