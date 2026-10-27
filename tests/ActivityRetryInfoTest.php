<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\ActivityRetryInfo;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ActivityRetryInfoTest extends TestCase
{
    public static function policies(): array
    {
        return [
            'bounded first' => [1, ['retry_policy' => ['max_attempts' => 3]], 3, false, false],
            'bounded final' => [3, ['retry_policy' => ['max_attempts' => 3]], 3, false, true],
            'beyond budget' => [4, ['retry_policy' => ['max_attempts' => 3]], 3, false, true],
            'one attempt' => [1, ['retry_policy' => ['max_attempts' => 1]], 1, false, true],
            'absent' => [1, [], null, false, null],
            'legacy null' => [1, ['retry_policy' => null], null, false, null],
            'missing limit' => [1, ['retry_policy' => ['future_field' => true]], null, false, null],
            'unlimited' => [99, ['retry_policy' => ['max_attempts' => null]], null, true, false],
            'zero limit' => [1, ['retry_policy' => ['max_attempts' => 0]], null, false, null],
            'string limit' => [1, ['retry_policy' => ['max_attempts' => '3']], null, false, null],
            'boolean limit' => [1, ['retry_policy' => ['max_attempts' => true]], null, false, null],
            'float limit' => [1, ['retry_policy' => ['max_attempts' => 3.0]], null, false, null],
            'legacy scalar' => [1, ['retry_policy' => 'legacy'], null, false, null],
            'invalid attempt' => [0, ['retry_policy' => ['max_attempts' => 1]], 1, false, null],
        ];
    }

    #[DataProvider('policies')]
    public function testReceivedPolicyDoesNotAcquireDefaults(int $attempt, array $task, ?int $limit, bool $unlimited, ?bool $last): void
    {
        $info = ActivityRetryInfo::fromTask($attempt, $task);
        self::assertSame($attempt, $info->attemptNumber);
        self::assertSame(array_key_exists('retry_policy', $task), $info->policyProvided);
        self::assertSame($task['retry_policy'] ?? null, $info->policy());
        self::assertSame($limit, $info->maxAttempts());
        self::assertSame($unlimited, $info->isUnlimited());
        self::assertSame($last, $info->isLastConfiguredAttempt());
    }

    public function testPolicySnapshotRetainsUnknownFieldsAndCannotBeChangedThroughCopies(): void
    {
        $task = ['retry_policy' => ['max_attempts' => 3, 'future' => ['values' => [1, 2.0, null]]]];
        $original = $task['retry_policy'];
        $info = ActivityRetryInfo::fromTask(3, $task);
        $task['retry_policy']['max_attempts'] = 10;
        $copy = $info->policy();
        $copy['future']['values'][0] = 99;
        self::assertSame($original, $info->policy());
        self::assertTrue($info->isLastConfiguredAttempt());
    }

    public function testLegacyConstructorAndServerFreeActivitiesHaveUnknownMetadata(): void
    {
        $context = new ActivityContext(new Client('https://server.example'), 'task', 'attempt', 'owner', 'activity', 1);
        self::assertFalse($context->retryInfo->policyProvided);
        self::assertNull($context->retryInfo->isLastConfiguredAttempt());
        self::assertNull($context->workflowId);
        self::assertNull($context->runId);
    }

    public function testInlineLocalActivityRetainsParentIdentityWithoutInventingRetryPolicy(): void
    {
        $completion = null;
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use (&$completion): array {
            if (str_ends_with($uri, '/worker/register')) {
                return ['registered' => true];
            }
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                return ['task' => ['task_id' => 'task', 'workflow_id' => 'original-workflow', 'run_id' => 'original-run',
                    'workflow_type' => 'flow', 'lease_owner' => 'owner', 'payload_codec' => 'avro', 'history_events' => [],
                    'retry_policy' => ['max_attempts' => 9]]];
            }
            if (str_ends_with($uri, '/workflow-tasks/task/complete')) {
                $completion = $body;
                return ['completed' => true];
            }
            if (str_ends_with($uri, '/workflow-tasks/task/heartbeat')) {
                return ['task_id' => 'task', 'workflow_task_attempt' => 1, 'lease_owner' => 'owner', 'renewed' => true];
            }
            if (str_ends_with($uri, '/workflow-tasks/task/fail')) {
                self::fail('Workflow callback failed: '.($body['failure']['message'] ?? 'missing failure message'));
            }
            if (str_ends_with($uri, '/poll')) {
                return ['task' => null];
            }
            self::fail('Unexpected request: '.$method.' '.$uri);
        });
        $worker = new Worker(new Client('https://server.example', transport: $transport), 'queue', workerId: 'owner');
        $worker->registerWorkflow('flow', static fn (WorkflowContext $context): mixed => $context->localActivity('metadata'));
        $worker->registerActivity('metadata', static fn (ActivityContext $context): array => [
            'workflow_id' => $context->workflowId, 'run_id' => $context->runId,
            'provided' => $context->retryInfo->policyProvided, 'last' => $context->retryInfo->isLastConfiguredAttempt(),
        ]);
        self::assertTrue($worker->tick(0));
        self::assertNotNull($completion);
        $finished = array_values(array_filter($completion['commands'], static fn (array $command): bool => $command['type'] === 'complete_workflow'));
        self::assertCount(1, $finished);
        self::assertSame(['workflow_id' => 'original-workflow', 'run_id' => 'original-run', 'provided' => false, 'last' => null],
            (new AvroPayloadCodec())->decodeEnvelope($finished[0]['result']));
    }

    public static function claimedMetadata(): array
    {
        return [
            'bounded' => [['retry_policy' => ['max_attempts' => 3, 'future' => [true, null]], 'attempt_number' => 3,
                'workflow_instance_id' => '0', 'workflow_run_id' => '017'], true, true],
            'absent legacy' => [[], false, null],
            'explicit root null' => [['retry_policy' => null], true, null],
            'unlimited' => [['retry_policy' => ['max_attempts' => null]], true, false],
        ];
    }

    #[DataProvider('claimedMetadata')]
    public function testOrdinaryWorkerExposesTheReceivedClaim(array $metadata, bool $provided, ?bool $last): void
    {
        $completion = null;
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use ($metadata, &$completion): array {
            if (str_ends_with($uri, '/worker/register')) {
                return ['registered' => true];
            }
            if (str_ends_with($uri, '/activity-tasks/poll')) {
                return ['task' => $metadata + ['task_id' => 'task', 'activity_attempt_id' => 'attempt', 'lease_owner' => 'owner',
                    'activity_type' => 'metadata', 'attempt_number' => 1, 'payload_codec' => 'avro']];
            }
            if (str_ends_with($uri, '/activity-tasks/task/complete')) {
                $completion = $body;
                return ['completed' => true];
            }
            if (str_ends_with($uri, '/poll')) {
                return ['task' => null];
            }
            self::fail('Unexpected request: '.$method.' '.$uri);
        });
        $worker = new Worker(new Client('https://server.example', transport: $transport), 'queue', workerId: 'owner');
        $worker->registerActivity('metadata', static fn (ActivityContext $context): array => [
            'attempt' => $context->retryInfo->attemptNumber,
            'provided' => $context->retryInfo->policyProvided,
            'policy' => $context->retryInfo->policy(),
            'last' => $context->retryInfo->isLastConfiguredAttempt(),
            'workflow_id' => $context->workflowId,
            'run_id' => $context->runId,
        ]);
        self::assertTrue($worker->tick(0));
        self::assertNotNull($completion);
        $result = (new AvroPayloadCodec())->decode($completion['result']['blob']);
        self::assertSame($metadata['attempt_number'] ?? 1, $result['attempt']);
        self::assertSame($provided, $result['provided']);
        self::assertSame($metadata['retry_policy'] ?? null, $result['policy']);
        self::assertSame($last, $result['last']);
        self::assertSame($metadata['workflow_instance_id'] ?? null, $result['workflow_id']);
        self::assertSame($metadata['workflow_run_id'] ?? null, $result['run_id']);
    }
}
