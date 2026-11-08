<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use Closure;
use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Qualification\DeferredChildAllReplayTransport;
use DurableWorkflow\Qualification\DeferredChildAllScenario as Scenario;
use DurableWorkflow\Qualification\DeferredChildAllTurnTransport;
use DurableWorkflow\Transport\BoundedTransport;
use DurableWorkflow\Worker;
use PHPUnit\Framework\TestCase;
use RuntimeException;

require_once dirname(__DIR__).'/scripts/Support/DeferredChildAllScenario.php';
require_once dirname(__DIR__).'/scripts/Support/DeferredChildAllTurnTransport.php';
require_once dirname(__DIR__).'/scripts/Support/DeferredChildAllReplayTransport.php';

/**
 * #185 cell 03: the deferred-all duplicate check and its one-turn workers,
 * checked against a transcription of frozen Native 3.2.0 (3c6f1eda) duplicate
 * handling: carriers are stripped before a receipt is recorded or compared; an
 * identical repeat is 200 recorded:false already_completed; any other body is a
 * conflicting completion that the ownership guard refuses by run state.
 */
final class DeferredChildAllDuplicateTest extends TestCase
{
    private const COMPLETE = 'http://adapter.invalid/api/worker/workflow-tasks/parent-task/complete';

    /** The real Worker carries a workflow poll here; the one-turn transport sends and records the completion without it. */
    public function testAOneTurnWorkerSendsAndRecordsItsCompletionWithoutACarriedWorkflowPoll(): void
    {
        $direct = self::runParentTurn(null);
        self::assertArrayHasKey('workflow_poll', $direct['sent'], 'control: the Worker carries a poll on a carried-poll Server');

        $receipt = (string) tempnam(sys_get_temp_dir(), 'child-all-duplicate-');
        try {
            $turn = self::runParentTurn($receipt);
            self::assertArrayNotHasKey('workflow_poll', $turn['sent']);
            self::assertSame($direct['sent']['commands'], $turn['sent']['commands']);
            $recorded = json_decode((string) file_get_contents($receipt), true, flags: JSON_THROW_ON_ERROR);
            self::assertSame($turn['sent'], $recorded['body'], 'the receipt is the body actually sent');
        } finally {
            @unlink($receipt);
        }
    }

    public function testAOneTurnCompletionWithAnActivityPollIsRefusedBeforeSending(): void
    {
        $wire = new DuplicateWire(static fn (): array => ['recorded' => true]);
        $transport = new DeferredChildAllTurnTransport($wire, (string) tempnam(sys_get_temp_dir(), 'child-all-duplicate-'), static function (): void {});
        try {
            $transport->sendBounded('POST', self::COMPLETE, [], ['commands' => [['type' => 'complete_workflow']], 'activity_poll' => ['worker_id' => 'w']], 5);
            self::fail('An activity carrier on a one-turn completion must refuse.');
        } catch (RuntimeException $error) {
            self::assertSame('A one-turn worker completion carried an activity poll.', $error->getMessage());
            self::assertSame([], $wire->bodies);
        }
    }

    /** Through the real Client, an armed replay sends the recorded body byte for byte, once, to that task only. */
    public function testAnArmedReplaySendsTheExactRecordedBodyOnce(): void
    {
        $wire = new DuplicateWire(static fn (): array => ['recorded' => false, 'reason' => 'already_completed']);
        $replay = new DeferredChildAllReplayTransport($wire);
        $client = new Client('http://adapter.invalid', workerToken: 'unretained-worker-auth', transport: $replay);
        $recorded = self::recordedBody();

        $replay->arm('parent-task', $recorded);
        $client->completeWorkflowTask('parent-task', 'worker', 1, $recorded['commands']);
        self::assertSame($recorded, $wire->bodies[0]);
        self::assertSame($recorded, $replay->sent());

        $client->completeWorkflowTask('parent-task', 'worker', 1, $recorded['commands']);
        self::assertSame(['lease_owner' => 'worker', 'workflow_task_attempt' => 1, 'commands' => $recorded['commands']], $wire->bodies[1],
            'unarmed, the Client body passes through unchanged');

        $replay->arm('parent-task', $recorded);
        try {
            $client->completeWorkflowTask('other-task', 'worker', 1, $recorded['commands']);
            self::fail('A replay armed for one task must not send to another.');
        } catch (RuntimeException $error) {
            self::assertSame('Duplicate replay targeted another request or task.', $error->getMessage());
            self::assertCount(2, $wire->bodies);
        }
    }

    /**
     * The observed failure, reproduced: the predecessor's fresh Client body is a
     * conflicting completion, refused 409 run_closed once the child run closed
     * (task_not_leased only while it is open). The exact replay is the explicit
     * no-op whatever the run state, and Native ignores the carriers.
     */
    public function testOnlyTheExactReplayIsTheExplicitNoOpUnderTheFrozenNativeRule(): void
    {
        $recorded = self::recordedBody();
        foreach (['closed' => 'run_closed', 'open' => 'task_not_leased'] as $run => $conflict) {
            $native = new FrozenDuplicateNative($recorded, $run === 'closed');
            $replay = new DeferredChildAllReplayTransport($native);
            $client = new Client('http://adapter.invalid', workerToken: 'unretained-worker-auth', transport: $replay);
            try {
                $client->completeWorkflowTask('parent-task', 'worker', 1, $recorded['commands']);
                self::fail('A different body must not be the explicit no-op.');
            } catch (ServerException $error) {
                self::assertSame([409, $conflict, false], [$error->status, $error->reason, $error->details['recorded'] ?? null], $run);
            }
            $replay->arm('parent-task', $recorded);
            $response = $client->completeWorkflowTask('parent-task', 'worker', 1, $recorded['commands']);
            self::assertSame([false, 'already_completed'], [$response['recorded'] ?? null, $response['reason'] ?? null], $run);
            $replay->arm('parent-task', ['workflow_poll' => ['worker_id' => 'w']] + $recorded);
            $response = $client->completeWorkflowTask('parent-task', 'worker', 1, $recorded['commands']);
            self::assertSame('already_completed', $response['reason'] ?? null, 'Native strips carriers before comparing');
        }
    }

    /** The qualifier arms the exact replay before every duplicate and accepts only the explicit no-op. */
    public function testTheQualifierDuplicateCheckReplaysExactlyAndAcceptsOnlyTheNoOp(): void
    {
        $source = (string) file_get_contents(dirname(__DIR__).'/scripts/qualify-deferred-child-all.php');
        preg_match('~function childAllDuplicate\(.*?\n}\n~s', $source, $match);
        self::assertNotEmpty($match);
        $body = $match[0];
        self::assertLessThan(strpos($body, '->completeWorkflowTask('), strpos($body, '$replay->arm('));
        self::assertStringContainsString("=== 'already_completed'", $body);
        self::assertStringContainsString('$replay->sent() === $body', $body);
        self::assertStringNotContainsString("reason === 'task_not_leased'", $body);
        self::assertStringNotContainsString('status === 409', $body);
        self::assertStringContainsString('childAllRefusal(', $body);
        self::assertStringContainsString("transport: \$replay)", $source);
        self::assertSame(3, substr_count($source, 'childAllDuplicate($worker, $control, '), 'parent start, each member and parent finish');
    }

    public function testRefusalDiagnosticsAreBoundedAndCarryNoPayload(): void
    {
        $source = (string) file_get_contents(dirname(__DIR__).'/scripts/qualify-deferred-child-all.php');
        preg_match('~function childAllRefusal\(.*?\n}\n~s', $source, $match);
        self::assertNotEmpty($match);
        eval('namespace DurableWorkflow\Tests\Refusal; use DurableWorkflow\Exception\ServerException; '.$match[0]);
        $error = new ServerException('Workflow run is already closed.', 409, 'run_closed', [
            'recorded' => false, 'validation_errors' => ['commands' => ['x', ['nested-secret']]], 'arguments' => 'secret-payload',
            'registration_token' => 'secret-token',
        ]);
        $text = \DurableWorkflow\Tests\Refusal\childAllRefusal('Exact duplicate completion was refused', $error);
        self::assertSame('Exact duplicate completion was refused: status 409, reason run_closed, recorded false, validation_errors {"commands":["x"]}', $text);
        $long = new ServerException('m', 422, str_repeat('r', 900));
        self::assertLessThanOrEqual(600, strlen(\DurableWorkflow\Tests\Refusal\childAllRefusal('x', $long)));
    }

    /** @return array{sent: array<string, mixed>} */
    private static function runParentTurn(?string $receipt): array
    {
        $scenario = new Scenario('single', ['child-0']);
        $codec = new AvroPayloadCodec();
        $sent = null;
        $capabilities = ['workflow_task_completion_workflow_poll' => true];
        $wire = new DuplicateWire(static function (string $uri, ?array $body) use ($scenario, $codec, $capabilities, &$sent): array {
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                return ['poll_status' => 'leased', 'server_capabilities' => $capabilities, 'task' => [
                    'task_id' => 'parent-task', 'workflow_id' => 'parent', 'run_id' => 'run',
                    'workflow_type' => $scenario->parentType(), 'task_queue' => 'parents',
                    'arguments' => $codec->envelope([$scenario->fixture['input']]), 'payload_codec' => 'avro',
                    'history_events' => [], 'lease_owner' => 'worker', 'workflow_task_attempt' => 1,
                ]];
            }
            if (str_ends_with($uri, '/complete')) {
                $sent = $body;

                // Native answers a carried member only when the completion sent one.
                return ['recorded' => true] + (isset($body['workflow_poll']) ? ['workflow_poll' => ['leased' => false]] : []);
            }
            if (str_ends_with($uri, '/parent-task/heartbeat')) {
                return ['task_id' => 'parent-task', 'workflow_task_attempt' => 1, 'lease_owner' => 'worker', 'renewed' => true];
            }
            if (str_ends_with($uri, '/register')) {
                return ['registered' => true, 'worker_id' => 'worker', 'registration_token' => str_repeat('a', 32), 'heartbeat_interval_seconds' => 30,
                    'server_capabilities' => $capabilities + ['worker_deregistration_fencing' => [
                        'schema' => 'durable-workflow.v2.worker-deregistration.v1', 'supported' => true,
                        'endpoint' => '/worker/registrations/{workerId}/deregister', 'receipt_retention_seconds' => 600,
                    ]]];
            }
            if (str_ends_with($uri, '/deregister')) {
                return ['worker_id' => 'worker', 'registration_token' => str_repeat('a', 32), 'outcome' => 'deregistered', 'recovered_workflow_task_count' => 0];
            }

            return ['task' => null, 'poll_status' => 'empty'];
        });
        $worker = null;
        $transport = $receipt === null ? $wire : new DeferredChildAllTurnTransport($wire, $receipt, static function () use (&$worker): void {
            /** @var Worker|null $worker */
            $worker?->requestShutdown();
        });
        $worker = new Worker(new Client('http://adapter.invalid', workerToken: 'unretained-worker-auth', transport: $transport), 'parents', workerId: 'worker');
        $worker->registerWorkflow($scenario->parentType(), $scenario->parent());
        if ($receipt === null) {
            $wire->afterComplete = static function () use (&$worker): void {
                /** @var Worker|null $worker */
                $worker?->requestShutdown();
            };
        }
        $worker->run(0);
        self::assertIsArray($sent);

        return ['sent' => $sent];
    }

    /** @return array<string, mixed> A worker's completion body with fields the fresh Client body omits. */
    private static function recordedBody(): array
    {
        return ['lease_owner' => 'worker', 'workflow_task_attempt' => 1,
            'commands' => [['type' => 'complete_workflow', 'result' => (new AvroPayloadCodec())->envelope(['ok'])]],
            'sticky_cache' => ['cache_key' => 'k', 'history_sequence' => 4]];
    }
}

final class DuplicateWire implements BoundedTransport
{
    /** @var list<array<string, mixed>|null> */
    public array $bodies = [];

    public ?Closure $afterComplete = null;

    public function __construct(private readonly Closure $respond)
    {
    }

    public function supportsBoundedRequests(): bool
    {
        return true;
    }

    public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
    {
        if (str_ends_with($uri, '/complete')) {
            $this->bodies[] = $body;
        }
        $response = ($this->respond)($uri, $body);
        if (str_ends_with($uri, '/complete') && $this->afterComplete !== null) {
            ($this->afterComplete)();
        }

        return $response;
    }

    public function sendBounded(string $method, string $uri, array $headers, ?array $body, int $timeoutSeconds): ?array
    {
        return $this->send($method, $uri, $headers, $body);
    }
}

/** Frozen 3c6f1eda duplicate handling for one completed workflow task (store.rs ≈3147–3190, 4233–4260, guard order). */
final class FrozenDuplicateNative implements BoundedTransport
{
    /** @var array<string, mixed> */
    private array $receipt;

    /** @param array<string, mixed> $recorded */
    public function __construct(array $recorded, private readonly bool $runClosed)
    {
        $this->receipt = self::withoutCarriers($recorded);
    }

    public function supportsBoundedRequests(): bool
    {
        return true;
    }

    public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
    {
        if (self::canonical(self::withoutCarriers($body ?? [])) === self::canonical($this->receipt)) {
            return ['task_id' => 'parent-task', 'outcome' => 'completed', 'recorded' => false, 'reason' => 'already_completed', 'created_task_ids' => []];
        }
        $reason = $this->runClosed ? 'run_closed' : 'task_not_leased';
        throw new \DurableWorkflow\Exception\TransportException('Workflow task is not currently leased.', 409,
            ['reason' => $reason, 'recorded' => false, 'task_id' => 'parent-task', 'run_id' => 'run', 'workflow_task_attempt' => 1]);
    }

    public function sendBounded(string $method, string $uri, array $headers, ?array $body, int $timeoutSeconds): ?array
    {
        return $this->send($method, $uri, $headers, $body);
    }

    /**
     * @param array<string, mixed> $body
     * @return array<string, mixed>
     */
    private static function withoutCarriers(array $body): array
    {
        unset($body['activity_poll'], $body['workflow_poll']);

        return $body;
    }

    /** JSON value equality: object key order is not significant, scalar types are. */
    private static function canonical(mixed $value): mixed
    {
        if (!is_array($value)) {
            return $value;
        }
        $value = array_map(self::canonical(...), $value);
        if (!array_is_list($value)) {
            ksort($value, SORT_STRING);
        }

        return $value;
    }
}
