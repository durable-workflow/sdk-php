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
            $payload = $history[$index * 2]['payload'];
            if ($index === 0 && in_array($case, ['retry', 'continue'], true)) {
                $replacement = self::replacement($case, $payload);
                $history[] = $replacement;
                $scenario->assertStable($history, $commands, $identities);
                $payload = $replacement['payload'];
            }
            $terminal = self::terminal($payload, $codec->envelope(['echo' => [
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
        $history[] = ['event_type' => 'WorkflowCompleted', 'payload' => []];
        self::assertCount($size, $scenario->assertStable($history, $commands, $identities, parentFinished: true));
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

    #[DataProvider('replacementCases')]
    public function testReplacementRunKeepsInitialIdentityAndOriginalMemberPath(string $case): void
    {
        $scenario = self::scenario($case, 2);
        $codec = new AvroPayloadCodec();
        $commands = (new Replayer($codec))->replay($scenario->parent(), [], [$scenario->fixture['input']], 'parents')->commands;
        $history = self::history($commands);
        $identities = $scenario->identities($history, $commands);
        $next = self::replacement($case, $history[1]['payload']);
        $history[] = $next;
        $history[] = self::terminal($next['payload'], $codec->envelope('done'));
        $chains = $scenario->assertStable($history, $commands, $identities);
        self::assertSame('run-0', $scenario->identities($history, $commands)[0]['child_workflow_run_id']);
        self::assertSame('link-0', $scenario->identities($history, $commands)[0]['workflow_link_id']);
        self::assertSame('replacement-link', $chains[0][1]['workflow_link_id']);
        self::assertSame('ChildRunCompleted', $chains[0][1]['terminal']);
    }

    /** @return iterable<string, array{string}> */
    public static function replacementCases(): iterable
    {
        yield 'retry' => ['retry'];
        yield 'continue' => ['continue'];
    }

    #[DataProvider('replacementLedgerMutations')]
    public function testReplacementLedgerRejectsReusedClosedOrUnprovenRuns(string $case, string $mutation): void
    {
        $scenario = self::scenario($case, 2);
        $codec = new AvroPayloadCodec();
        $commands = (new Replayer($codec))->replay($scenario->parent(), [], [$scenario->fixture['input']], 'parents')->commands;
        $history = self::history($commands);
        $initial = $scenario->identities($history, $commands);
        $next = self::replacement($case, $history[1]['payload']);
        $terminal = self::terminal($next['payload'], $codec->envelope('done'));
        switch ($mutation) {
            case 'string replacement sequence': $next['payload']['sequence'] = '1'; break;
            case 'wrong predecessor': $next['payload']['retry_of_child_workflow_run_id'] = 'other'; break;
            case 'shared replacement link': $next['payload']['workflow_link_id'] = 'link-1'; break;
            case 'wrong run number': $next['payload']['child_run_number'] = 1; break;
            case 'initial terminal before replacement': $history[] = self::terminal($history[0]['payload'], $codec->envelope('done')); break;
        }
        $history[] = $next;
        switch ($mutation) {
            case 'duplicate replacement start': $history[] = $next; break;
            case 'third run':
                $third = $next;
                $third['payload']['child_workflow_run_id'] = 'third';
                $third['payload']['workflow_link_id'] = 'third-link';
                $third['payload']['child_run_number'] = 3;
                $history[] = $third;
                break;
            case 'event on replaced run': $history[] = self::terminal($history[0]['payload'], $codec->envelope('done')); break;
            case 'wrong final link': $terminal['payload']['workflow_link_id'] = 'link-0'; $history[] = $terminal; break;
            case 'repeat final terminal': $history[] = $terminal; $history[] = $terminal; break;
            case 'replacement after terminal':
                $history[] = $terminal;
                $next['payload']['child_workflow_run_id'] = 'after-terminal';
                $next['payload']['workflow_link_id'] = 'after-terminal-link';
                $history[] = $next;
                break;
        }
        $this->expectException(RuntimeException::class);
        $scenario->assertStable($history, $commands, $initial);
    }

    /** @return iterable<string, array{string, string}> */
    public static function replacementLedgerMutations(): iterable
    {
        foreach (['retry', 'continue'] as $case) {
            foreach (['string replacement sequence', 'wrong predecessor', 'shared replacement link', 'wrong run number',
                'initial terminal before replacement', 'duplicate replacement start', 'third run', 'event on replaced run',
                'wrong final link', 'repeat final terminal', 'replacement after terminal'] as $mutation) {
                yield $case.' '.$mutation => [$case, $mutation];
            }
        }
    }

    #[DataProvider('replacementCases')]
    public function testChildHistoryProvesReplacementWithoutEquatingTheContinuationAndParentLinks(string $case): void
    {
        $proof = self::replacementProof($case);
        self::scenario($case, 2)->assertReplacementHistory(...$proof);
        self::assertSame($case === 'continue' ? 'continuation-link' : 'replacement-link', $proof[0][0]['payload']['workflow_link_id']);
        if ($case === 'continue') {
            self::assertNotSame($proof[1]['workflow_link_id'], $proof[0][0]['payload']['workflow_link_id']);
            self::assertNotSame($proof[2]['workflow_link_id'], $proof[0][0]['payload']['workflow_link_id']);
        } else {
            self::assertSame($proof[2]['workflow_link_id'], $proof[0][0]['payload']['workflow_link_id']);
        }
    }

    #[DataProvider('parentChildLinkRoles')]
    public function testContinuationRefusesCoherentTwoFieldParentChildLinkSubstitution(string $role): void
    {
        [$history, $original, $replacement, $parent, $predecessor] = self::replacementProof('continue');
        $link = $role === 'initial' ? $original['workflow_link_id'] : $replacement['workflow_link_id'];
        $history[0]['payload']['workflow_link_id'] = $link;
        $predecessor[0]['payload']['workflow_link_id'] = $link;
        $this->expectException(RuntimeException::class);
        self::scenario('continue', 2)->assertReplacementHistory($history, $original, $replacement, $parent, $predecessor);
    }

    /** @return iterable<string, array{string}> */
    public static function parentChildLinkRoles(): iterable
    {
        yield 'initial parent-child link' => ['initial'];
        yield 'replacement parent-child link' => ['replacement'];
    }

    #[DataProvider('replacementProofMutations')]
    public function testChildReplacementProofRefusesChangedPredecessorOrAuthority(string $case, string $mutation): void
    {
        [$history, $original, $replacement, $parent, $predecessor] = self::replacementProof($case);
        switch ($mutation) {
            case 'wrong parent run': $history[0]['payload']['parent_workflow_run_id'] = 'other'; break;
            case 'wrong child call': $history[0]['payload']['child_call_id'] = 'other'; break;
            case 'wrong workflow type': $history[0]['payload']['workflow_type'] = 'other'; break;
            case 'string parent sequence': $history[0]['payload']['parent_sequence'] = '1'; break;
            case 'duplicate child start': $history[] = $history[0]; break;
            case 'missing predecessor terminal': $predecessor = []; break;
            case 'duplicate predecessor terminal': $predecessor[] = $predecessor[0]; break;
            case 'wrong predecessor':
                $history[0]['payload'][$case === 'retry' ? 'retry_of_child_workflow_run_id' : 'continued_from_run_id'] = 'other';
                break;
            case 'wrong predecessor link':
                if ($case === 'retry') {
                    $history[0]['payload']['workflow_link_id'] = 'other';
                } else {
                    $predecessor[0]['payload']['workflow_link_id'] = 'replacement-link';
                }
                break;
        }
        $this->expectException(RuntimeException::class);
        self::scenario($case, 2)->assertReplacementHistory($history, $original, $replacement, $parent, $predecessor);
    }

    /** @return iterable<string, array{string, string}> */
    public static function replacementProofMutations(): iterable
    {
        foreach (['retry', 'continue'] as $case) {
            foreach (['wrong parent run', 'wrong child call', 'wrong workflow type', 'string parent sequence',
                'duplicate child start', 'missing predecessor terminal', 'duplicate predecessor terminal', 'wrong predecessor', 'wrong predecessor link'] as $mutation) {
                yield $case.' '.$mutation => [$case, $mutation];
            }
        }
    }

    #[DataProvider('heldHistoryAdmissions')]
    public function testHeldHistoryAdmissionsAreRefused(string $case, string $mutation): void
    {
        $scenario = self::scenario($case, $case === 'single' ? 1 : ($case === 'out_of_order' ? 3 : 2));
        $codec = new AvroPayloadCodec();
        $commands = (new Replayer($codec))->replay($scenario->parent(), [], [$scenario->fixture['input']], 'parents')->commands;
        $history = self::history($commands);
        $initial = $scenario->identities($history, $commands);
        $terminal = self::terminal($history[0]['payload'], $codec->envelope('done'));
        if ($scenario->failsEarly()) {
            $terminal['event_type'] = $scenario->expected()['failure_type'];
        }
        $history[] = $terminal;
        switch ($mutation) {
            case 'duplicate terminal': $history[] = $terminal; break;
            case 'conflicting terminal':
                $terminal['event_type'] = $terminal['event_type'] === 'ChildRunCompleted' ? 'ChildRunFailed' : 'ChildRunCompleted';
                $history[] = $terminal;
                break;
            case 'unknown ChildRun type': $history[array_key_last($history)]['event_type'] = 'ChildRunAnything'; break;
            case 'unknown Child type': $history[array_key_last($history)]['event_type'] = 'ChildWorkflowAnything'; break;
            case 'string sequence': $history[array_key_last($history)]['payload']['sequence'] = '1'; break;
            case 'leading-zero sequence': $history[array_key_last($history)]['payload']['sequence'] = '01'; break;
            case 'float sequence': $history[array_key_last($history)]['payload']['sequence'] = 1.0; break;
            case 'boolean sequence': $history[array_key_last($history)]['payload']['sequence'] = true; break;
            case 'wrong workflow type': $history[array_key_last($history)]['payload']['child_workflow_type'] = 'other'; break;
            case 'wrong run number': $history[array_key_last($history)]['payload']['child_run_number'] = '1'; break;
            case 'post-parent-terminal event':
                array_pop($history);
                $history[] = ['event_type' => 'WorkflowCompleted', 'payload' => []];
                $history[] = $terminal;
                break;
            case 'post-terminal unknown event':
                $terminal['event_type'] = 'ChildRunProgress';
                $history[] = $terminal;
                break;
        }
        $this->expectException(RuntimeException::class);
        $scenario->assertStable($history, $commands, $initial);
    }

    /** @return iterable<string, array{string, string}> */
    public static function heldHistoryAdmissions(): iterable
    {
        foreach (['single', 'out_of_order', 'failed', 'cancelled', 'terminated'] as $case) {
            foreach (['duplicate terminal', 'conflicting terminal'] as $mutation) {
                yield $case.' '.$mutation => [$case, $mutation];
            }
        }
        foreach (['unknown ChildRun type', 'unknown Child type', 'string sequence', 'leading-zero sequence',
            'float sequence', 'boolean sequence', 'wrong workflow type', 'wrong run number',
            'post-parent-terminal event', 'post-terminal unknown event'] as $mutation) {
            yield $mutation => ['out_of_order', $mutation];
        }
    }

    #[DataProvider('abandonedCases')]
    public function testFinishedEarlyFailurePreservesAStartedButAbandonedSibling(string $case): void
    {
        $scenario = self::scenario($case, 2);
        $codec = new AvroPayloadCodec();
        $commands = (new Replayer($codec))->replay($scenario->parent(), [], [$scenario->fixture['input']], 'parents')->commands;
        $history = self::history($commands);
        $initial = $scenario->identities($history, $commands);
        $terminal = self::terminal($history[0]['payload'], $codec->envelope('unused'));
        $terminal['event_type'] = $scenario->expected()['failure_type'];
        $history[] = $terminal;
        $history[] = ['event_type' => 'WorkflowCompleted', 'payload' => []];
        $chains = $scenario->assertStable($history, $commands, $initial, parentFinished: true);
        self::assertSame($scenario->expected()['failure_type'], $chains[0][0]['terminal']);
        self::assertNull($chains[1][0]['terminal']);
        self::assertSame('run-1', $chains[1][0]['run_id']);
    }

    /** @return iterable<string, array{string}> */
    public static function abandonedCases(): iterable
    {
        foreach (['failed', 'cancelled', 'terminated'] as $case) {
            yield $case => [$case];
        }
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

    public function testOnlyExplicitEmptyPollsProveNoParentTask(): void
    {
        foreach (['empty', 'timeout'] as $status) {
            Scenario::assertEmptyPoll(['task' => null, 'poll_status' => $status]);
            Scenario::assertEmptyPoll(['task' => null, 'poll_status' => $status, 'reason' => null]);
        }
        $this->addToAssertionCount(4);
    }

    /** @param array<string, mixed> $response */
    #[DataProvider('uncertainPolls')]
    public function testUnavailableMalformedOrLeasedPollsCannotProveNoParentTask(array $response): void
    {
        $this->expectException(RuntimeException::class);
        Scenario::assertEmptyPoll($response);
    }

    /** @return iterable<string, array{array<string, mixed>}> */
    public static function uncertainPolls(): iterable
    {
        foreach (['draining', 'stopped', 'stale_worker_registration', 'backend_lock_pressure', 'leased', 'future_status'] as $status) {
            yield $status => [['task' => null, 'poll_status' => $status]];
        }
        yield 'missing status' => [['task' => null]];
        yield 'missing task' => [['poll_status' => 'empty']];
        yield 'leased task' => [['task' => ['task_id' => 'task'], 'poll_status' => 'empty']];
        yield 'mixed terminal reason' => [['task' => null, 'poll_status' => 'empty', 'reason' => 'worker_draining']];
        yield 'unknown reason' => [['task' => null, 'poll_status' => 'timeout', 'reason' => 'future_reason']];
    }

    public function testParentReceiptBindsTheIntendedWorkflowRunAndQueue(): void
    {
        $task = ['workflow_id' => 'parent', 'run_id' => 'initial-run', 'task_queue' => 'parents'];
        Scenario::assertParentTask($task, 'parent', 'initial-run', 'parents');
        $this->addToAssertionCount(1);
        foreach (array_keys($task) as $key) {
            try {
                Scenario::assertParentTask(array_replace($task, [$key => 'other']), 'parent', 'initial-run', 'parents');
                self::fail('Another parent identity must refuse.');
            } catch (RuntimeException $error) {
                self::assertSame('Managed parent received another workflow, run or queue.', $error->getMessage());
            }
        }
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
            $history[] = ['event_type' => 'ChildRunStarted', 'payload' => [...$payload, 'child_run_number' => 1]];
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
        return ['event_type' => 'ChildRunCompleted', 'payload' => [...$payload,
            'child_run_number' => $payload['child_run_number'] ?? 1, 'result' => $result]];
    }

    /**
     * @param array<string, mixed> $payload
     * @return array<string, mixed>
     */
    private static function replacement(string $case, array $payload): array
    {
        return ['event_type' => 'ChildRunStarted', 'payload' => [...$payload, 'child_workflow_run_id' => 'replacement',
            'workflow_link_id' => 'replacement-link', 'child_run_number' => 2,
            ...($case === 'retry' ? ['retry_of_child_workflow_run_id' => $payload['child_workflow_run_id'], 'retry_attempt' => 2] : [])]];
    }

    /**
     * @return array{list<array<string, mixed>>, array<string, mixed>, array{run_id: string, workflow_link_id: string, terminal: ?string}, array{workflow_id: string, run_id: string}, list<array<string, mixed>>}
     */
    private static function replacementProof(string $case): array
    {
        $original = ['child_call_id' => 'call-0', 'child_workflow_instance_id' => 'child-0', 'child_workflow_run_id' => 'run-0', 'workflow_link_id' => 'link-0'];
        $replacement = ['run_id' => 'replacement', 'workflow_link_id' => 'replacement-link', 'terminal' => 'ChildRunCompleted'];
        $parent = ['workflow_id' => 'parent', 'run_id' => 'parent-run'];
        $history = [['event_type' => 'WorkflowStarted', 'payload' => [
            'workflow_instance_id' => 'child-0', 'workflow_run_id' => 'replacement',
            'child_call_id' => 'call-0', 'parent_workflow_instance_id' => 'parent', 'parent_workflow_run_id' => 'parent-run',
            'parent_sequence' => 1, 'workflow_type' => 'parity.v1.child_all_child',
            ...($case === 'retry' ? ['retry_of_child_workflow_run_id' => 'run-0', 'retry_attempt' => 2, 'workflow_link_id' => 'replacement-link']
                : ['continued_from_run_id' => 'run-0', 'workflow_link_id' => 'continuation-link']),
        ]]];
        $predecessor = $case === 'retry' ? [['event_type' => 'WorkflowFailed', 'payload' => []]]
            : [['event_type' => 'WorkflowContinuedAsNew', 'payload' => [
                'continued_to_run_id' => 'replacement', 'continued_to_run_number' => 2, 'sequence' => 1, 'workflow_link_id' => 'continuation-link',
            ]]];

        return [$history, $original, $replacement, $parent, $predecessor];
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
