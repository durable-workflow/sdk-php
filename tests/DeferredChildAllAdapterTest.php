<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use Closure;
use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Qualification\ChildAllProbeFailure;
use DurableWorkflow\Qualification\DeferredChildAllScenario as Scenario;
use DurableWorkflow\Qualification\DeferredChildAllTurnTransport;
use DurableWorkflow\Transport\BoundedTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\Replayer;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;

require_once dirname(__DIR__).'/scripts/Support/DeferredChildAllScenario.php';
require_once dirname(__DIR__).'/scripts/Support/DeferredChildAllTurnTransport.php';

/** Offline consumer source controls, without a Server or installed-artifact claim. */
final class DeferredChildAllAdapterTest extends TestCase
{
    #[DataProvider('cases')]
    public function testManagedDefinitionsAuthorFlatChildrenAndColdReplayTheExactOutcome(string $case, int $size): void
    {
        $scenario = self::scenario($case, $size);
        $codec = new AvroPayloadCodec();
        $commands = (new Replayer($codec))->replay($scenario->parent(), [], [$scenario->fixture['input']], 'parents')->commands;
        self::assertCount($size, $commands);
        self::assertSame(9007199254740993, $scenario->fixture['input']['count']);
        foreach ($commands as $index => $command) {
            self::assertSame('start_child_workflow', $command['type']);
            self::assertSame($scenario->childType(), $command['workflow_type']);
            self::assertSame('child-'.$index, $command['queue']);
            self::assertNotSame('started', $command['await'] ?? null);
            self::assertCount(1, $command['parallel_group_path']);
            self::assertSame('child', $command['parallel_group_path'][0]['parallel_group_kind']);
            self::assertSame($index, $command['parallel_group_path'][0]['parallel_group_index']);
            self::assertSame([['index' => $index, 'value' => $scenario->fixture['input']]], $codec->decodeEnvelope($command['arguments']));
            self::assertSame($case === 'retry' && $index === 0 ? ['max_attempts' => 2, 'backoff_seconds' => [0]] : null, $command['retry_policy'] ?? null);
        }
        $history = self::history($commands);
        $identities = $scenario->identities($history, $commands);
        $scenario->assertStable($history, $commands, $identities);
        self::assertSame([], (new Replayer($codec))->replay($scenario->parent(), $history, [$scenario->fixture['input']], 'parents')->commands);
        foreach ($scenario->order() as $offset => $index) {
            $terminal = self::terminal($history[$index * 2]['payload'], $codec->envelope(['echo' => [
                'index' => $index, 'value' => $scenario->fixture['input'],
            ]]));
            if ($scenario->failsEarly() && $offset === 0) {
                $terminal['event_type'] = $scenario->expected()['failure_type'];
                $terminal['payload']['message'] = $scenario->expected()['message'];
                unset($terminal['payload']['result']);
            }
            $history[] = $terminal;
            $scenario->assertStable($history, $commands, $identities);
            $resumed = (new Replayer($codec))->replay($scenario->parent(), $history, [$scenario->fixture['input']], 'parents')->commands;
            if (!$scenario->failsEarly() && $offset < $size - 1) {
                self::assertSame([], $resumed);
            } else {
                self::assertCount(1, $resumed);
                self::assertSame('complete_workflow', $resumed[0]['type']);
                self::assertSame($scenario->expected(), $codec->decodeEnvelope($resumed[0]['result']));
            }
        }
    }

    /** @return iterable<string, array{string, int}> */
    public static function cases(): iterable
    {
        yield 'single' => ['single', 1];
        yield 'out of order' => ['out_of_order', 3];
        yield 'failed' => ['failed', 2];
        yield 'cancelled' => ['cancelled', 2];
        yield 'terminated' => ['terminated', 2];
        yield 'retry extension' => ['retry', 2];
        yield 'continue extension' => ['continue', 2];
    }

    public function testRetryFailsOnlyItsFixedInitialRunAndReplacementReturnsTheSameTypedValue(): void
    {
        $scenario = self::scenario('retry', 2);
        $codec = new AvroPayloadCodec();
        $input = ['index' => 0, 'value' => $scenario->fixture['input']];
        try {
            (new Replayer($codec))->replay($scenario->child('initial'), [], [$input], 'child-0', ['run_id' => 'initial']);
            self::fail('Initial retry run must fail.');
        } catch (ChildAllProbeFailure $error) {
            self::assertSame('retry group member', $error->getMessage());
        }
        $commands = (new Replayer($codec))->replay($scenario->child('initial'), [], [$input], 'child-0', ['run_id' => 'replacement'])->commands;
        self::assertSame('complete_workflow', $commands[0]['type']);
        self::assertSame(['echo' => $input], $codec->decodeEnvelope($commands[0]['result']));
    }

    public function testContinuePassesTheSameTypedArgumentsToItsSecondRun(): void
    {
        $scenario = self::scenario('continue', 2);
        $codec = new AvroPayloadCodec();
        $input = ['index' => 0, 'value' => $scenario->fixture['input']];
        $first = (new Replayer($codec))->replay($scenario->child(), [], [$input], 'child-0')->commands;
        self::assertSame('continue_as_new', $first[0]['type']);
        $arguments = $codec->decodeEnvelope($first[0]['arguments']);
        self::assertSame([$input, 1], $arguments);
        $second = (new Replayer($codec))->replay($scenario->child(), [], $arguments, 'child-0')->commands;
        self::assertSame(['echo' => $input], $codec->decodeEnvelope($second[0]['result']));
    }

    #[DataProvider('changedHistories')]
    public function testIdentityGuardRejectsChangedOrDuplicatedProducerRows(string $mutation): void
    {
        $scenario = self::scenario('out_of_order', 3);
        $codec = new AvroPayloadCodec();
        $commands = (new Replayer($codec))->replay($scenario->parent(), [], [$scenario->fixture['input']], 'parents')->commands;
        $history = self::history($commands);
        $identities = $scenario->identities($history, $commands);
        $history[] = self::terminal($history[0]['payload'], $codec->envelope('done'));
        switch ($mutation) {
            case 'extra start': $history[] = $history[0]; break;
            case 'duplicate run start': $history[] = $history[1]; break;
            case 'missing run start': unset($history[1]); break;
            case 'changed initial run': $history[0]['payload']['child_workflow_run_id'] = 'different'; break;
            case 'shared call': $history[2]['payload']['child_call_id'] = $history[0]['payload']['child_call_id']; break;
            case 'changed terminal call': $history[6]['payload']['child_call_id'] = 'different'; break;
            case 'changed terminal link': $history[6]['payload']['workflow_link_id'] = 'different'; break;
            case 'changed terminal instance': $history[6]['payload']['child_workflow_instance_id'] = 'different'; break;
            case 'changed terminal run': $history[6]['payload']['child_workflow_run_id'] = 'different'; break;
            case 'changed terminal path': $history[6]['payload']['parallel_group_path'][0]['parallel_group_index'] = 2; break;
            case 'unknown member': $history[6]['payload']['sequence'] = 4; break;
        }
        $this->expectException(RuntimeException::class);
        $scenario->assertStable($history, $commands, $identities);
    }

    /** @return iterable<string, array{string}> */
    public static function changedHistories(): iterable
    {
        foreach (['extra start', 'duplicate run start', 'missing run start', 'changed initial run', 'shared call', 'changed terminal call', 'changed terminal link',
            'changed terminal instance', 'changed terminal run', 'changed terminal path', 'unknown member'] as $mutation) {
            yield $mutation => [$mutation];
        }
    }

    public function testReplacementRunKeepsInitialIdentityAndOriginalMemberPath(): void
    {
        $scenario = self::scenario('continue', 2);
        $codec = new AvroPayloadCodec();
        $commands = (new Replayer($codec))->replay($scenario->parent(), [], [$scenario->fixture['input']], 'parents')->commands;
        $history = self::history($commands);
        $identities = $scenario->identities($history, $commands);
        $next = $history[1];
        $next['payload']['child_workflow_run_id'] = 'replacement';
        $history[] = $next;
        $history[] = self::terminal($next['payload'], $codec->envelope('done'));
        $scenario->assertStable($history, $commands, $identities);
        self::assertSame('run-0', $scenario->identities($history, $commands)[0]['child_workflow_run_id']);
    }

    public function testActualManagedWorkerStopsAfterItsRecordedTurnAndReceiptOmitsHeaders(): void
    {
        $scenario = self::scenario('single', 1);
        $codec = new AvroPayloadCodec();
        $requests = [];
        $wire = new ChildAllWire(static function (string $uri, ?array $body) use ($scenario, $codec, &$requests): array {
            $requests[] = $uri;
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                return ['poll_status' => 'leased', 'task' => [
                    'task_id' => 'parent-task', 'workflow_id' => 'parent', 'run_id' => 'run',
                    'workflow_type' => $scenario->parentType(), 'task_queue' => 'parents',
                    'arguments' => $codec->envelope([$scenario->fixture['input']]), 'payload_codec' => 'avro',
                    'history_events' => [], 'lease_owner' => 'worker', 'workflow_task_attempt' => 1,
                    'registration_token' => 'unretained-registration',
                ]];
            }
            if (str_ends_with($uri, '/complete')) {
                self::assertCount(1, $body['commands']);
                return ['recorded' => true];
            }
            if (str_ends_with($uri, '/parent-task/heartbeat')) {
                return ['task_id' => 'parent-task', 'workflow_task_attempt' => 1, 'lease_owner' => 'worker', 'renewed' => true];
            }
            if (str_ends_with($uri, '/register')) {
                return ['registered' => true, 'worker_id' => 'worker', 'registration_token' => str_repeat('a', 32), 'heartbeat_interval_seconds' => 30,
                    'server_capabilities' => ['worker_deregistration_fencing' => [
                        'schema' => 'durable-workflow.v2.worker-deregistration.v1', 'supported' => true,
                        'endpoint' => '/worker/registrations/{workerId}/deregister', 'receipt_retention_seconds' => 600,
                    ]]];
            }
            if (str_ends_with($uri, '/deregister')) {
                return ['worker_id' => 'worker', 'registration_token' => str_repeat('a', 32),
                    'outcome' => 'deregistered', 'recovered_workflow_task_count' => 0];
            }

            return ['task' => null, 'poll_status' => 'empty'];
        });
        $receipt = tempnam(sys_get_temp_dir(), 'child-all-test-');
        self::assertIsString($receipt);
        try {
            $worker = null;
            $transport = new DeferredChildAllTurnTransport($wire, $receipt, static function () use (&$worker): void {
                /** @var Worker|null $worker */
                $worker?->requestShutdown();
            });
            $worker = new Worker(new Client('http://adapter.invalid', workerToken: 'unretained-worker-auth', transport: $transport), 'parents', workerId: 'worker');
            $worker->registerWorkflow($scenario->parentType(), $scenario->parent());
            $worker->run(0);
            $bytes = (string) file_get_contents($receipt);
            $value = json_decode($bytes, true, flags: JSON_THROW_ON_ERROR);
            self::assertSame('parents', $value['task']['task_queue']);
            self::assertSame('parent-task', $value['task']['task_id']);
            self::assertSame('child-0', $value['body']['commands'][0]['queue']);
            self::assertStringNotContainsString('unretained', $bytes);
            self::assertCount(1, array_filter($requests, static fn (string $uri): bool => str_ends_with($uri, '/workflow-tasks/poll')));
            self::assertCount(1, array_filter($requests, static fn (string $uri): bool => str_ends_with($uri, '/deregister')));
        } finally {
            unlink($receipt);
        }
    }

    #[DataProvider('unrecordedTurns')]
    public function testUnrecordedOrUnboundCompletionCannotStopTheWorkerOrCreateAReceipt(bool $recorded, bool $poll): void
    {
        $receipt = tempnam(sys_get_temp_dir(), 'child-all-test-');
        $stopped = false;
        $transport = new DeferredChildAllTurnTransport(new ChildAllWire(static fn (string $uri): array => str_ends_with($uri, '/poll')
            ? ['task' => ['task_id' => 'task']] : ['recorded' => $recorded]), $receipt,
            static function () use (&$stopped): void { $stopped = true; });
        try {
            if ($poll) {
                $transport->sendBounded('POST', 'http://adapter.invalid/workflow-tasks/poll', [], [], 1);
            }
            try {
                $transport->sendBounded('POST', 'http://adapter.invalid/workflow-tasks/task/complete', [], ['commands' => []], 1);
                self::fail('Unrecorded or unbound completion must refuse.');
            } catch (RuntimeException) {
                self::assertFalse($stopped);
                self::assertSame('', file_get_contents($receipt));
            }
        } finally {
            unlink($receipt);
        }
    }

    /** @return iterable<string, array{bool, bool}> */
    public static function unrecordedTurns(): iterable
    {
        yield 'not recorded' => [false, true];
        yield 'no matching poll' => [true, false];
    }

    private static function scenario(string $case, int $size): Scenario
    {
        return new Scenario($case, array_map(static fn (int $index): string => 'child-'.$index, range(0, $size - 1)));
    }

    /**
     * @param list<array<string, mixed>> $commands
     * @return list<array<string, mixed>>
     */
    private static function history(array $commands): array
    {
        $history = [];
        foreach ($commands as $index => $command) {
            $metadata = array_filter($command, static fn (string $key): bool => str_starts_with($key, 'parallel_group_'), ARRAY_FILTER_USE_KEY);
            $payload = ['sequence' => $index + 1, 'child_workflow_type' => $command['workflow_type'],
                'child_call_id' => 'call-'.$index, 'workflow_link_id' => 'link-'.$index,
                'child_workflow_instance_id' => 'child-'.$index, 'child_workflow_run_id' => 'run-'.$index, ...$metadata];
            $history[] = ['event_type' => 'ChildWorkflowScheduled', 'payload' => $payload];
            $history[] = ['event_type' => 'ChildRunStarted', 'payload' => $payload];
        }

        return $history;
    }

    /**
     * @param array<string, mixed> $payload
     * @param array<string, mixed> $result
     * @return array<string, mixed>
     */
    private static function terminal(array $payload, array $result): array
    {
        return ['event_type' => 'ChildRunCompleted', 'payload' => [...$payload, 'result' => $result]];
    }
}

final class ChildAllWire implements BoundedTransport
{
    public function __construct(private readonly Closure $handler)
    {
    }

    public function supportsBoundedRequests(): bool
    {
        return true;
    }

    public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
    {
        return ($this->handler)($uri, $body);
    }

    public function sendBounded(string $method, string $uri, array $headers, ?array $body, int $timeoutSeconds): ?array
    {
        return $this->send($method, $uri, $headers, $body);
    }
}
