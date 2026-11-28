<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests\Integration;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Exception\WorkflowCancelled;
use DurableWorkflow\Transport\BoundedPayloadTransport;
use DurableWorkflow\Transport\BoundedTransport;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\WorkflowContext;
use GuzzleHttp\Client as HttpClient;
use GuzzleHttp\Handler\CurlMultiHandler;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\TestCase;

/**
 * Activity workflow continuation v2 (Server #1181) against a real native Server.
 *
 * Opt-in: set DURABLE_WORKFLOW_CONTINUATION_V2_QUALIFICATION=1 and DURABLE_WORKFLOW_RUNTIME_URL
 * (and DURABLE_WORKFLOW_AUTH_TOKEN). The cases skip when the Server's cluster info does not
 * advertise the v2 member, so a Server release without it never fails them.
 */
final class ActivityContinuationV2Test extends TestCase
{
    private string $url = '';
    private string $token = '';
    /** The activity lease helper recreates the default HTTP client; the recording transport stays in this process. */
    private string $bootstrap = '';
    /** @var list<array{0: string, 1: array<string, mixed>}> */
    private array $diagnostics = [];

    protected function setUp(): void
    {
        if (getenv('DURABLE_WORKFLOW_CONTINUATION_V2_QUALIFICATION') !== '1') {
            self::markTestSkipped('Activity workflow continuation v2 qualification is opt-in.');
        }
        $url = getenv('DURABLE_WORKFLOW_RUNTIME_URL');
        if (!is_string($url) || trim($url) === '') {
            self::markTestSkipped('DURABLE_WORKFLOW_RUNTIME_URL is not set.');
        }
        $token = getenv('DURABLE_WORKFLOW_AUTH_TOKEN');
        $this->url = $url;
        $this->token = is_string($token) && $token !== '' ? $token : 'test-token';
        $capability = (new Client($this->url, token: $this->token))->clusterInfo()
            ->raw['worker_protocol']['server_capabilities']['activity_workflow_continuation'] ?? null;
        if (!is_array($capability) || !in_array(2, $capability['supported_versions'] ?? [], true)
            || ($capability['v2']['version'] ?? null) !== 2) {
            self::markTestSkipped('The Server does not advertise activity_workflow_continuation v2.');
        }
        $this->bootstrap = (string) tempnam(sys_get_temp_dir(), 'php-continuation-v2-lease-');
        file_put_contents($this->bootstrap, '<?php return new \\DurableWorkflow\\Client('.var_export($this->url, true)
            .', token: '.var_export($this->token, true).');');
        chmod($this->bootstrap, 0600);
    }

    protected function tearDown(): void
    {
        if ($this->bootstrap !== '' && is_file($this->bootstrap)) {
            unlink($this->bootstrap);
        }
    }

    public function testAcceptedContinuationsCarryTheRunFromTailToTail(): void
    {
        $run = $this->driveRun('accept', static fn (array $request): ?array => null);
        self::assertSame(21, $run['result']);
        self::assertSame(3, $run['activity_completed'], 'Each activity is recorded once.');
        self::assertSame([], $run['requests']['history'], 'v2 never asks for history.');
        $decisions = $this->decisions();
        self::assertSame(['accepted', 'accepted', 'accepted'], array_column($decisions, 'outcome'));
        self::assertSame(['appended', 'appended', 'appended'], array_column($decisions, 'tail'));
        self::assertSame([], $run['requests']['ordinary'], 'No activity needed the ordinary completion.');
    }

    #[DataProvider('lostResponses')]
    public function testAnUnknownOutcomeIsResentIdenticallyAndAnsweredFromTheReceipt(?int $status, ?string $reason): void
    {
        $dropped = false;
        $run = $this->driveRun('unknown', static function (array $request) use (&$dropped, $status, $reason): ?array {
            if (!$dropped && str_ends_with($request['uri'], '/continuation/complete')) {
                $dropped = true;

                // The real Server commits the request before the response is lost or replaced by a gateway error.
                return ['response_failure' => $status === null
                    ? new TransportException('Response connection lost.', transientConnectionFailure: true)
                    : new TransportException('Response replaced.', $status, $reason === null ? null : ['reason' => $reason])];
            }

            return null;
        });
        self::assertTrue($dropped);
        self::assertSame(21, $run['result']);
        self::assertSame(3, $run['activity_completed']);
        $first = array_values(array_filter($run['requests']['continuation'],
            static fn (array $row): bool => $row['uri'] === $run['requests']['continuation'][0]['uri']));
        self::assertCount(2, $first);
        self::assertSame($first[0]['body'], $first[1]['body'], 'The resent request is identical.');
        self::assertSame('already_completed', $first[1]['answer']['reason'] ?? null);
        self::assertSame('accepted', $first[1]['answer']['continuation']['outcome'] ?? null, 'The receipt keeps the decision.');
        self::assertSame([], $run['requests']['ordinary']);
    }

    public static function lostResponses(): iterable
    {
        yield 'lost connection' => [null, null];
        yield 'bodiless gateway error' => [502, null];
        yield 'typed server error' => [500, 'database_error'];
        yield 'typed service unavailable' => [503, 'transition_fault'];
    }

    #[DataProvider('wireRefusals')]
    public function testAServerRefusalFallsBackToTheOrdinaryCompletion(int $status, string $reason, \Closure $changeBody): void
    {
        if ($status === 413 && getenv('DURABLE_WORKFLOW_CONTINUATION_V2_FIXTURE_RESULT_BUDGET') !== '1') {
            self::markTestSkipped('The result-budget case needs the fixture structural payload limit set to 524288 bytes.');
        }
        $refused = false;
        $run = $this->driveRun('refusal', static function (array $request) use (&$refused, $changeBody): ?array {
            if (!$refused && str_ends_with($request['uri'], '/continuation/complete')) {
                $refused = true;

                return ['body' => $changeBody($request['body'])];
            }

            return null;
        });
        self::assertTrue($refused);
        // The refused activity completed the ordinary way with the same carried workflow poll.
        $refusedRequest = $run['requests']['continuation'][0];
        $ordinary = $run['requests']['ordinary'][0] ?? null;
        self::assertSame($status, $refusedRequest['status'] ?? null, (string) ($refusedRequest['answer']['reason'] ?? ''));
        self::assertSame($reason, $refusedRequest['answer']['reason'] ?? null);
        self::assertSame(21, $run['result']);
        self::assertSame(3, $run['activity_completed']);
        self::assertIsArray($ordinary);
        self::assertSame(str_replace('/continuation/complete', '/complete', $refusedRequest['uri']), $ordinary['uri']);
        self::assertSame($refusedRequest['original_body']['workflow_poll'], $ordinary['body']['workflow_poll']);
        self::assertSame($refusedRequest['original_body']['result'], $ordinary['body']['result']);
        self::assertTrue($ordinary['answer']['recorded'] ?? false);
        $refusals = array_values(array_filter($this->diagnostics, static fn (array $row): bool => $row[0] === 'worker.activity_continuation_refused'));
        self::assertSame([[$status, $reason]], array_map(static fn (array $row): array => [$row[1]['status'], $row[1]['reason']], $refusals));
        if ($reason === 'unsupported_continuation_version') {
            self::assertCount(1, array_filter($run['requests']['continuation'],
                static fn (array $request): bool => ($request['original_body']['continuation']['version'] ?? null) === 2));
        }
    }

    public static function wireRefusals(): iterable
    {
        yield 'unknown field' => [422, 'invalid_continuation', static function (array $body): array {
            $body['continuation']['unknown_member'] = true;

            return $body;
        }];
        yield 'unsupported version' => [422, 'unsupported_continuation_version', static function (array $body): array {
            $body['continuation']['version'] = 99;
            unset($body['continuation']['sticky_cache']);

            return $body;
        }];
        yield 'unsupported command' => [422, 'unsupported_continuation_command', static function (array $body): array {
            $body['continuation']['commands'] = [['type' => 'open_signal_wait', 'signal_name' => 'note']];

            return $body;
        }];
        yield 'missing carried poll' => [422, 'continuation_requires_workflow_poll', static function (array $body): array {
            unset($body['workflow_poll']);

            return $body;
        }];
        yield 'result exceeds the native fixture budget' => [413, 'continuation_result_exceeds_budget', static function (array $body): array {
            // This encodes above the fixture's 512 KiB structural limit and below the wire envelope limit.
            $body['result'] = (new AvroPayloadCodec())->envelope(str_repeat('x', 450_000));

            return $body;
        }];
    }

    #[Group('continuation-v2-nested-budget')]
    public function testAnOverBudgetNestedCompletionRollsBackBeforeOrdinaryFallback(): void
    {
        if (getenv('DURABLE_WORKFLOW_CONTINUATION_V2_FIXTURE_NESTED_BUDGET') !== '1') {
            self::markTestSkipped('This case needs the disposable Server history transaction event limit set to 12.');
        }

        $client = new Client($this->url, token: $this->token);
        $changed = false;
        $rolledBack = false;
        $run = $this->driveRun('nested-budget', static function (array $request) use ($client, &$changed, &$rolledBack): ?array {
            if ($changed || !str_ends_with($request['uri'], '/continuation/complete')) {
                return null;
            }

            $changed = true;
            $body = $request['body'];
            $cache = $body['continuation']['sticky_cache'];
            $history = static fn (): array => iterator_to_array($client->iterateWorkflowHistory($cache['workflow_id'], $cache['run_id']), false);
            $before = $history();
            $body['continuation']['commands'] = array_fill(0, 13, ['type' => 'start_timer', 'delay_seconds' => 60]);

            return [
                'body' => $body,
                'after_error' => static function (TransportException $error) use ($history, $before, &$rolledBack): void {
                    self::assertSame(409, $error->status);
                    self::assertSame('completion_budget_exceeded', $error->response['reason']);
                    self::assertSame('history_transaction_size', $error->response['limit_kind']);
                    self::assertSame(12, $error->response['configured_limit']);
                    self::assertSame(13, $error->response['current_value']);
                    self::assertFalse($error->response['recorded']);
                    self::assertSame($before, $history(), 'The refused transaction left the complete run history intact.');
                    $rolledBack = true;
                },
            ];
        });

        self::assertTrue($rolledBack);
        self::assertSame(21, $run['result']);
        self::assertSame(3, $run['activity_completed']);
        $refused = $run['requests']['continuation'][0];
        $ordinary = $run['requests']['ordinary'][0];
        self::assertSame(str_replace('/continuation/complete', '/complete', $refused['uri']), $ordinary['uri']);
        self::assertSame($refused['original_body']['workflow_poll'], $ordinary['body']['workflow_poll']);
        self::assertSame($refused['original_body']['result'], $ordinary['body']['result']);
        self::assertTrue($ordinary['answer']['recorded']);
        self::assertSame([], $run['requests']['failure']);
    }

    public function testAClosedActivityAttemptIsAnsweredWithoutAnOrdinaryResend(): void
    {
        $client = new Client($this->url, token: $this->token);
        $committed = null;
        $run = $this->driveRun('stale', static function (array $request) use ($client, &$committed): ?array {
            if ($committed === null && preg_match('~/activity-tasks/([^/]+)/continuation/complete$~', $request['uri'], $match) === 1) {
                $body = $request['body'];
                $committed = $client->completeActivityTask($match[1], $body['activity_attempt_id'], $body['lease_owner'], 7);
            }

            return null;
        });
        self::assertTrue($committed['recorded'] ?? false);
        self::assertSame(21, $run['result']);
        self::assertSame(3, $run['activity_completed']);
        $first = $run['requests']['continuation'][0];
        self::assertSame(409, $first['status']);
        self::assertSame('stale_attempt', $first['answer']['reason']);
        self::assertFalse($first['answer']['recorded']);
        self::assertSame($committed['task_id'], $first['answer']['task_id']);
        self::assertSame($committed['activity_attempt_id'], $first['answer']['activity_attempt_id']);
        self::assertSame('completed', $first['answer']['attempt_status']);
        self::assertCount(1, array_filter($run['requests']['continuation'], static fn (array $row): bool => $row['uri'] === $first['uri']));
        $ordinaryUri = str_replace('/continuation/complete', '/complete', $first['uri']);
        self::assertNotContains($ordinaryUri, array_column($run['requests']['ordinary'], 'uri'));
        self::assertSame([], $run['requests']['failure']);
    }

    public function testAChangedContinuationConflictsWithTheCommittedReceipt(): void
    {
        $conflicted = false;
        $run = $this->driveRun('conflict', static function (array $request) use (&$conflicted): ?array {
            if ($conflicted || !str_ends_with($request['uri'], '/continuation/complete')
                || ($request['body']['continuation']['commands'][0]['type'] ?? null) !== 'complete_workflow') {
                return null;
            }
            $conflicted = true;
            $first = $request['body'];
            $first['workflow_poll']['poll_request_id'] .= '-original';

            return ['send_first' => $first];
        }, expectedTerminalReason: 'conflicting_completion');
        self::assertTrue($conflicted);
        self::assertSame(21, $run['result']);
        self::assertSame(3, $run['activity_completed']);
        $requests = $run['requests']['continuation'];
        [$first, $conflict] = array_slice($requests, -2);
        self::assertSame($first['uri'], $conflict['uri']);
        self::assertNotSame($first['body']['workflow_poll']['poll_request_id'], $conflict['body']['workflow_poll']['poll_request_id']);
        self::assertTrue($first['answer']['recorded']);
        self::assertSame('accepted', $first['answer']['continuation']['outcome']);
        self::assertSame(409, $conflict['status']);
        self::assertSame('conflicting_completion', $conflict['answer']['reason']);
        self::assertCount(2, array_filter($requests, static fn (array $row): bool => $row['uri'] === $conflict['uri']));
        self::assertSame([], $run['requests']['ordinary']);
        self::assertSame([], $run['requests']['failure']);
    }

    #[DataProvider('wireDeclines')]
    public function testAServerDeclineRecordsTheActivityOnceAndContinuesTheRun(string $reason, \Closure $changeBody): void
    {
        $changed = false;
        $run = $this->driveRun('decline', static function (array $request) use (&$changed, $changeBody): ?array {
            if (!$changed && str_ends_with($request['uri'], '/continuation/complete')) {
                $changed = true;

                return ['body' => $changeBody($request['body'])];
            }

            return null;
        });
        self::assertTrue($changed);
        $this->assertDeclineRecordedOnce($run, $reason);
    }

    /**
     * @param array{result: mixed, activity_completed: int, requests: array<string, list<array<string, mixed>>>} $run
     * @return array<string, mixed>
     */
    private function assertDeclineRecordedOnce(array $run, string $reason): array
    {
        self::assertSame(21, $run['result']);
        self::assertSame(3, $run['activity_completed']);
        $first = $run['requests']['continuation'][0];
        self::assertTrue($first['answer']['recorded']);
        self::assertSame(['version' => 2, 'outcome' => 'declined', 'reason' => $reason], $first['answer']['continuation']);
        self::assertSame($reason, $this->decisions()[0]['reason']);
        self::assertArrayNotHasKey('continuation_tail', $first['answer']);
        self::assertArrayNotHasKey('activity_poll', $first['answer']);
        $ordinaryUri = str_replace('/continuation/complete', '/complete', $first['uri']);
        self::assertNotContains($ordinaryUri, array_column($run['requests']['ordinary'], 'uri'));

        return $first['answer'];
    }

    public static function wireDeclines(): iterable
    {
        yield 'different cache build' => ['cache_identity_mismatch', static function (array $body): array {
            $body['continuation']['sticky_cache']['build_id'] = 'unmatched-build';

            return $body;
        }];
        yield 'different cached event' => ['history_changed', static function (array $body): array {
            $body['continuation']['history_last_event_id'] = 'different-cached-event';

            return $body;
        }];
        yield 'unadmitted carried poll' => ['workflow_slot_unavailable', static function (array $body): array {
            $body['workflow_poll']['worker_id'] = 'unregistered-worker';

            return $body;
        }];
    }

    #[Group('continuation-v2-external-payload')]
    public function testAnExternalActivityResultDeclinesAndResumesThroughTheCarriedPoll(): void
    {
        if (getenv('DURABLE_WORKFLOW_CONTINUATION_V2_FIXTURE_EXTERNAL_PAYLOAD') !== '1') {
            self::markTestSkipped('This case needs local external payload storage on the disposable native Server.');
        }

        $reference = null;
        $run = $this->driveRun('external-result', function (array $request) use (&$reference): ?array {
            if ($reference !== null || !str_ends_with($request['uri'], '/continuation/complete')) {
                return null;
            }

            $body = $request['body'];
            $blob = $body['result']['blob'];
            $upload = (new Psr18Transport())->uploadPayload(rtrim($this->url, '/').'/api/external-payloads/v1', [
                'Authorization' => 'Bearer '.$this->token,
                'Content-Type' => 'application/octet-stream',
                'X-Durable-Workflow-Payload-Codec' => 'avro',
                'X-Durable-Workflow-Payload-Size' => (string) strlen($blob),
                'X-Durable-Workflow-Payload-SHA256' => hash('sha256', $blob),
            ], $blob, 5);
            $reference = $upload['reference'];
            self::assertSame(hash('sha256', $blob), $reference['sha256']);
            $body['result'] = ['codec' => 'avro', 'external_payload' => $reference];

            return ['body' => $body];
        });

        $answer = $this->assertDeclineRecordedOnce($run, 'external_payload');
        self::assertNotNull($reference);
        self::assertTrue($answer['workflow_poll']['leased']);
        self::assertSame($answer['next_task_id'], $answer['workflow_poll']['answer']['task']['task_id']);
        self::assertContains('accepted', array_column($this->decisions(), 'outcome'));
    }

    public function testARequestedCancellationDeclinesAndTheWorkflowClosesAsCancelled(): void
    {
        $client = new Client($this->url, token: $this->token);
        $receipt = null;
        $run = $this->driveRun('cancellation', static function (array $request) use ($client, &$receipt): ?array {
            if ($receipt === null && str_ends_with($request['uri'], '/continuation/complete')) {
                $cache = $request['body']['continuation']['sticky_cache'];
                $receipt = $client->requestWorkflowCancellation($cache['workflow_id'], 'Continuation fixture', 10, $cache['run_id']);
            }

            return null;
        }, expectCancelled: true);

        self::assertTrue($receipt['accepted'] ?? false);
        self::assertFalse($receipt['duplicate']);
        self::assertSame(1, $run['activity_completed']);
        self::assertCount(1, $run['requests']['continuation']);
        $answer = $run['requests']['continuation'][0]['answer'];
        self::assertTrue($answer['recorded']);
        self::assertSame(['version' => 2, 'outcome' => 'declined', 'reason' => 'cancellation_requested'], $answer['continuation']);
        self::assertSame('cancellation_requested', $this->decisions()[0]['reason']);
        self::assertSame([], $run['requests']['ordinary']);
    }

    public function testAStandaloneActivityDeclinesAndReplaysItsCompletion(): void
    {
        $client = new Client($this->url, token: $this->token);
        $identity = $this->registerLeaseFixture($client, 'standalone');
        // The SDK's control client has no standalone activity submission method.
        $started = (new Psr18Transport())->send('POST', rtrim($this->url, '/').'/api/activities', [
            'Authorization' => 'Bearer '.$this->token,
            'Content-Type' => 'application/json',
            'X-Durable-Workflow-Control-Plane-Version' => '2',
        ], [
            'activity_id' => $identity,
            'activity_type' => 'tests.php-continuation-v2.lease',
            'task_queue' => $identity,
            'input' => $client->payloadCodec()->envelope([]),
        ]);
        self::assertSame($identity, $started['workflow_id']);
        $activity = $client->pollActivityTask($identity, $identity, timeoutSeconds: 2);
        self::assertSame($identity, $activity['workflow_id']);
        self::assertSame($started['workflow_run_id'], $activity['run_id']);
        $history = iterator_to_array($client->iterateWorkflowHistory($identity, $activity['run_id']), false);
        $last = $history[array_key_last($history)];
        self::assertSame('ActivityStarted', $last['event_type']);

        // This host has no sticky tail. The Server checks the activity kind before the event ID.
        $last['id'] = 'standalone-host-boundary';
        $answer = $this->assertLeasedActivityDecline($client, $activity, $last, 'standalone_activity');
        self::assertNull($answer['next_task_id']);
        self::assertFalse($answer['workflow_poll']['leased']);
        self::assertSame(7, $client->workflowResult($identity, timeoutSeconds: 5));
    }

    public function testAParallelMemberDeclinesAndItsSiblingCanFinishTheRun(): void
    {
        $client = new Client($this->url, token: $this->token);
        $identity = $this->registerLeaseFixture($client, 'parallel');
        $client->startWorkflow('tests.php-continuation-v2.lease', $identity, $identity);
        $commands = [];
        for ($index = 0; $index < 2; $index++) {
            $group = [
                'parallel_group_id' => 'parallel-activities:1:2', 'parallel_group_kind' => 'activity',
                'parallel_group_base_sequence' => 1, 'parallel_group_size' => 2, 'parallel_group_index' => $index,
            ];
            $commands[] = ['type' => 'schedule_activity', 'activity_type' => 'tests.php-continuation-v2.lease',
                'arguments' => $client->payloadCodec()->envelope([]), 'parallel_group_path' => [$group], ...$group];
        }
        [$activity, $last] = $this->seedActivityLease($client, $identity, $commands);
        $answer = $this->assertLeasedActivityDecline($client, $activity, $last, 'parallel_group');
        self::assertNull($answer['next_task_id']);
        self::assertFalse($answer['workflow_poll']['leased']);

        $sibling = $client->pollActivityTask($identity, $identity, timeoutSeconds: 2);
        self::assertSame($activity['run_id'], $sibling['run_id']);
        self::assertNotSame($activity['task_id'], $sibling['task_id']);
        $done = $client->completeActivityTask($sibling['task_id'], $sibling['activity_attempt_id'], $identity, 13);
        self::assertTrue($done['recorded']);
        $resumed = $client->pollWorkflowTask($identity, $identity, timeoutSeconds: 2);
        self::assertSame($done['next_task_id'], $resumed['task_id']);
        self::assertSame($activity['run_id'], $resumed['run_id']);
        $history = iterator_to_array($client->iterateWorkflowHistory($identity, $activity['run_id']), false);
        $results = [];
        foreach ($history as $event) {
            if ($event['event_type'] === 'ActivityCompleted') {
                $results[] = $client->payloadCodec()->decodeEnvelope($event['payload']['result']);
            }
        }
        self::assertSame([7, 13], $results);
        $closed = $client->completeWorkflowTask($resumed['task_id'], $identity, $resumed['workflow_task_attempt'], [
            ['type' => 'complete_workflow', 'result' => $client->payloadCodec()->envelope(array_sum($results))],
        ]);
        self::assertTrue($closed['recorded']);
        self::assertSame(20, $client->workflowResult($identity, timeoutSeconds: 5));
    }

    #[Group('continuation-v2-run-deadline')]
    public function testAnExpiredRunDeclinesAndRecordsItsAlreadyLeasedActivity(): void
    {
        if (getenv('DURABLE_WORKFLOW_CONTINUATION_V2_FIXTURE_RUN_DEADLINE') !== '1') {
            self::markTestSkipped('This case needs the disposable native Server maintenance repair loop disabled.');
        }

        $client = new Client($this->url, token: $this->token);
        $identity = $this->registerLeaseFixture($client, 'deadline');
        $runTimeoutSeconds = 3;
        $client->startWorkflow('tests.php-continuation-v2.lease', $identity, $identity, runTimeoutSeconds: $runTimeoutSeconds);
        // Start has completed, so its Server-side deadline will pass by this monotonic instant.
        $waitUntil = hrtime(true) + $runTimeoutSeconds * 1_000_000_000;
        [$activity, $last] = $this->seedActivityLease($client, $identity, [
            ['type' => 'schedule_activity', 'activity_type' => 'tests.php-continuation-v2.lease',
                'arguments' => $client->payloadCodec()->envelope([])],
        ]);
        usleep((int) max(0, ceil(($waitUntil - hrtime(true)) / 1000)));

        // Leave the independent expired-run poll policy out of the fenced activity completion.
        $answer = $this->assertLeasedActivityDecline($client, $activity, $last, 'run_not_open', $identity.'-unregistered');
        self::assertArrayNotHasKey('workflow_poll', $answer);
    }

    private function seedActivityLease(Client $client, string $identity, array $commands): array
    {
        $workflow = $client->pollWorkflowTask($identity, $identity, timeoutSeconds: 2);
        self::assertSame($identity, $workflow['workflow_id']);
        $seed = $client->completeWorkflowTask($workflow['task_id'], $identity, $workflow['workflow_task_attempt'], $commands,
            stickyCache: $this->leaseFixtureCache($workflow, $workflow['history_last_sequence']),
            activityPoll: ['worker_id' => $identity, 'task_queue' => $identity, 'poll_request_id' => $identity.'-seed', 'timeout_seconds' => 0],
            continuationTail: ['version' => 2]);
        self::assertTrue($seed['activity_poll']['leased']);
        $activity = $seed['activity_poll']['answer']['task'];
        self::assertSame($workflow['run_id'], $activity['run_id']);
        $tail = $seed['continuation_tail'];
        $last = $tail['history_events'][array_key_last($tail['history_events'])];
        self::assertSame('ActivityStarted', $last['event_type']);

        return [$activity, $last];
    }

    private function registerLeaseFixture(Client $client, string $case): string
    {
        $identity = 'php-continuation-v2-'.$case.'-'.bin2hex(random_bytes(4));
        $type = 'tests.php-continuation-v2.lease';
        $client->registerWorker($identity, $identity, [$type], [$type], ['sticky_execution', 'sticky_history_suffix'],
            buildId: 'php-v2-build');
        $client->heartbeatWorker($identity);

        return $identity;
    }

    /** @return array{worker_id: string, workflow_id: string, run_id: string, build_id: string, ttl_seconds: int, history_last_sequence: int} */
    private function leaseFixtureCache(array $task, int $through): array
    {
        return ['worker_id' => $task['lease_owner'], 'workflow_id' => $task['workflow_id'], 'run_id' => $task['run_id'],
            'build_id' => 'php-v2-build', 'ttl_seconds' => 300, 'history_last_sequence' => $through];
    }

    private function assertLeasedActivityDecline(Client $client, array $activity, array $last, string $reason, ?string $pollWorkerId = null): array
    {
        $history = static fn (): array => iterator_to_array($client->iterateWorkflowHistory($activity['workflow_id'], $activity['run_id']), false);
        $before = $history();
        $identity = $activity['lease_owner'];
        $poll = ['worker_id' => $pollWorkerId ?? $identity, 'task_queue' => $identity, 'poll_request_id' => $identity.'-workflow',
            'timeout_seconds' => 0, 'history_page_size' => 100, 'task_kinds' => ['workflow']];
        $through = $last['sequence'];
        $continuation = ['version' => 2, 'history_through_sequence' => $through, 'history_last_event_id' => $last['id'],
            'sticky_cache' => $this->leaseFixtureCache($activity, $through),
            'commands' => [['type' => 'start_timer', 'delay_seconds' => 60]]];
        $complete = static fn (): array => $client->completeActivityTaskWithContinuation(
            $activity['task_id'], $activity['activity_attempt_id'], $identity, 7, $poll, $continuation);
        $answer = $complete();
        self::assertTrue($answer['recorded']);
        self::assertSame(['version' => 2, 'outcome' => 'declined', 'reason' => $reason], $answer['continuation']);
        self::assertArrayNotHasKey('continuation_tail', $answer);
        self::assertArrayNotHasKey('activity_poll', $answer);
        $after = $history();
        self::assertSame($before, array_slice($after, 0, count($before)));
        $added = array_column(array_slice($after, count($before)), 'event_type');
        self::assertCount(1, array_keys($added, 'ActivityCompleted', true));
        self::assertNotContains('TimerScheduled', $added);
        $retry = $complete();
        self::assertFalse($retry['recorded']);
        self::assertSame('already_completed', $retry['reason']);
        // JSON object field ordering can change when a backend persists the receipt.
        $decision = $answer['continuation'];
        $replayedDecision = $retry['continuation'];
        ksort($decision);
        ksort($replayedDecision);
        self::assertSame($decision, $replayedDecision);
        self::assertSame($after, $history());

        return $answer;
    }

    #[DataProvider('carriedPollDecisions')]
    public function testAReplayedCarriedPollDeclinesWhilePreservingItsOriginalAnswer(bool $leaseOtherWorkflow, string $reason): void
    {
        $client = new Client($this->url, token: $this->token);
        $other = $leaseOtherWorkflow ? 'php-continuation-other-'.bin2hex(random_bytes(4)) : null;
        $otherType = 'tests.php-continuation-v2.other';
        $primed = null;
        $pollUri = rtrim($this->url, '/').'/api/worker/workflow-tasks/poll';
        $run = $this->driveRun('carried-replay', static function (array $request) use ($client, $other, $otherType, $pollUri, &$primed): ?array {
            if ($primed !== null || !str_ends_with($request['uri'], '/continuation/complete')) {
                return null;
            }

            $body = $request['body'];
            $poll = $body['workflow_poll'];
            $poll['poll_request_id'] .= '-primed';
            if ($other !== null) {
                $client->startWorkflow($otherType, $other, $poll['task_queue']);
            }

            $primed = (new Psr18Transport())->send('POST', $pollUri, $request['headers'], $poll);
            self::assertIsArray($primed);
            self::assertSame($other === null ? 'empty' : 'leased', $primed['poll_status']);
            if ($other !== null) {
                self::assertSame($other, $primed['task']['workflow_id']);
            }

            $body['workflow_poll'] = $poll;

            return ['body' => $body];
        }, configureWorker: $other === null ? null : static function (Worker $worker) use ($otherType): void {
            $worker->registerWorkflow($otherType, static fn (WorkflowContext $context): int => 43);
        });
        $carried = $this->assertDeclineRecordedOnce($run, $reason)['workflow_poll'];
        self::assertSame($leaseOtherWorkflow, $carried['leased']);
        if ($other !== null) {
            self::assertSame($primed['task']['task_id'], $carried['answer']['task']['task_id']);
            self::assertSame(43, $client->workflowResult($other, timeoutSeconds: 5));
        }
    }

    public static function carriedPollDecisions(): iterable
    {
        yield 'admitted poll replayed empty' => [false, 'no_workflow_task'];
        yield 'poll replayed another run' => [true, 'dispatch_selected_other_task'];
    }

    public function testASignalDeclinesTheContinuationAndTheRunStillCompletes(): void
    {
        $run = $this->driveRun('signal', static fn (array $request): ?array => null, signalFirstActivity: true);
        self::assertSame(21, $run['result']);
        self::assertSame(3, $run['activity_completed']);
        $first = $this->decisions()[0];
        self::assertSame('declined', $first['outcome']);
        self::assertContains($first['reason'], ['history_changed', 'pending_workflow_task']);
    }

    public function testAnAcceptedUpdateDeclinesTheContinuationWhileItsWorkflowTaskIsPending(): void
    {
        $client = new Client($this->url, token: $this->token);
        $receipt = null;
        $run = $this->driveRun('update', static function (array $request) use ($client, &$receipt): ?array {
            if ($receipt === null && str_ends_with($request['uri'], '/continuation/complete')) {
                $workflowId = $request['body']['continuation']['sticky_cache']['workflow_id'];
                $receipt = $client->updateWorkflowReceipt($workflowId, 'adjust', [5], waitFor: 'accepted');
            }

            return null;
        }, withUpdate: true);
        self::assertSame('accepted', $receipt['update_status'] ?? null);
        self::assertSame(21, $run['result']);
        self::assertSame(3, $run['activity_completed']);
        $first = $run['requests']['continuation'][0]['answer'];
        self::assertTrue($first['recorded']);
        self::assertSame(['version' => 2, 'outcome' => 'declined', 'reason' => 'pending_workflow_task'], $first['continuation']);
    }

    public function testAnUpdateWaitingForValidationDeclinesTheContinuation(): void
    {
        if (!extension_loaded('curl')) {
            self::markTestSkipped('The validation case needs cURL to keep the control request open.');
        }

        $handler = new CurlMultiHandler(['select_timeout' => 0.01]);
        $control = new HttpClient(['handler' => $handler, 'timeout' => 10, 'http_errors' => false]);
        $workerTransport = new Psr18Transport();
        $client = new Client($this->url, token: $this->token);
        $requestId = 'php-v2-validation-'.bin2hex(random_bytes(4));
        $updateReply = null;
        $run = $this->driveRun('pending-validation', function (array $request) use (
            $handler, $control, $workerTransport, $client, $requestId, &$updateReply,
        ): ?array {
            if (str_ends_with($request['uri'], '/worker/register')) {
                // A validator-enabled SDK worker intentionally uses its own poll.
                // Advertise the validator at the fixture boundary to hold a real
                // Server validation lease while the managed worker exercises v2.
                $registration = $request['body'];
                $registration['capabilities'][] = 'update_validation_tasks';
                $registration['workflow_command_contracts']['tests.php-continuation-v2']['update_validators'] = ['adjust'];

                return ['body' => $registration];
            }
            if ($updateReply !== null || !str_ends_with($request['uri'], '/continuation/complete')) {
                return null;
            }

            $workflowId = $request['body']['continuation']['sticky_cache']['workflow_id'];
            $controlUri = rtrim($this->url, '/').'/api/workflows/'.rawurlencode($workflowId).'/update/adjust';
            $pending = $control->postAsync($controlUri, [
                'headers' => [
                    'Authorization' => 'Bearer '.$this->token,
                    'X-Durable-Workflow-Control-Plane-Version' => '2',
                ],
                'json' => [
                    'request_id' => $requestId,
                    'wait_for' => 'accepted',
                    'input' => (new AvroPayloadCodec())->envelope([5]),
                ],
            ]);

            $pollUri = rtrim($this->url, '/').'/api/worker/update-validation-tasks/poll';
            $poll = $request['body']['workflow_poll'];
            unset($poll['task_kinds'], $poll['history_page_size']);
            $task = null;
            $deadline = microtime(true) + 5;
            do {
                $handler->tick();
                $answer = $workerTransport->send('POST', $pollUri, $request['headers'], $poll);
                if (($answer['poll_status'] ?? null) === 'leased') {
                    $task = $answer['task'];
                    break;
                }
                usleep(20_000);
            } while ($pending->getState() === 'pending' && microtime(true) < $deadline);
            $reason = $pending->getState() === 'fulfilled'
                ? (json_decode((string) $pending->wait()->getBody(), true)['reason'] ?? 'request completed') : $pending->getState();
            self::assertIsArray($task, 'The control request reached a real validation lease. '.$reason);
            self::assertSame($workflowId, $task['workflow_id']);
            self::assertSame('adjust', $task['update_name']);
            self::assertSame('pending', $pending->getState());

            return ['after_response' => static function () use ($client, $task, $pending, &$updateReply): void {
                $rejected = $client->rejectUpdateValidationTask($task['update_validation_task_id'],
                    $task['lease_owner'], $task['update_validation_attempt'], 'Fixture validator declined.');
                self::assertSame('rejected', $rejected['outcome']);
                $reply = $pending->wait();
                self::assertSame(422, $reply->getStatusCode());
                $updateReply = json_decode((string) $reply->getBody(), true, flags: JSON_THROW_ON_ERROR);
                self::assertSame($task['update_validation_task_id'], $updateReply['update_id']);
            }];
        }, withUpdate: true);
        self::assertSame('update_validator_rejected', $updateReply['reason'] ?? null);
        $this->assertDeclineRecordedOnce($run, 'pending_update');
    }

    public function testAnOverBudgetTailIsOmittedAndTheLeasedActivityCompletesTheOrdinaryWay(): void
    {
        $run = $this->driveRun('budget', static fn (array $request): ?array => null, bigFirstResult: true);
        self::assertSame(300_014, $run['result']);
        $first = $this->decisions()[0];
        self::assertSame(['accepted', 'omitted'], [$first['outcome'], $first['tail']]);
        self::assertSame('tail_byte_budget_exceeded', $run['requests']['continuation'][0]['answer']['continuation_tail']['omitted']);
        self::assertCount(1, array_filter($this->diagnostics, static fn (array $row): bool
            => $row[0] === 'worker.activity_continuation_skipped' && ($row[1]['reason'] ?? null) === 'cache_tail_not_this_activity'));
        self::assertNotSame([], $run['requests']['ordinary']);
    }

    public function testARefusedWorkflowTailIsRetriedWithoutTheTail(): void
    {
        $changed = false;
        $run = $this->driveRun('invalid-tail', static function (array $request) use (&$changed): ?array {
            if ($changed || preg_match('~/workflow-tasks/[^/]+/complete$~', $request['uri']) !== 1
                || !isset($request['body']['continuation_tail'])) {
                return null;
            }
            $changed = true;
            $body = $request['body'];
            $body['sticky_cache']['history_last_sequence'] += 10;

            return ['body' => $body];
        });
        self::assertTrue($changed);
        self::assertSame(21, $run['result']);
        self::assertSame(3, $run['activity_completed']);
        [$first, $retry] = $run['requests']['workflow'];
        self::assertSame($first['uri'], $retry['uri']);
        self::assertSame(422, $first['status']);
        self::assertSame('invalid_continuation_tail', $first['answer']['reason']);
        self::assertArrayNotHasKey('continuation_tail', $retry['body']);
        self::assertSame($first['original_body']['commands'], $retry['body']['commands']);
        self::assertTrue($retry['answer']['recorded']);
        self::assertStringContainsString('/'.$retry['answer']['created_task_ids'][0].'/complete',
            $run['requests']['ordinary'][0]['uri']);
    }

    public function testAnExactWorkflowCompletionReplayReturnsAnUnavailableTail(): void
    {
        $replayed = false;
        $run = $this->driveRun('workflow-replay', static function (array $request) use (&$replayed): ?array {
            if ($replayed || preg_match('~/workflow-tasks/[^/]+/complete$~', $request['uri']) !== 1
                || !isset($request['body']['continuation_tail'])) {
                return null;
            }
            $replayed = true;
            $body = $request['body'];
            // Keep the first activity ready for polling while both completions address the same workflow task.
            unset($body['activity_poll']);

            return ['body' => $body, 'replay' => true];
        });
        self::assertTrue($replayed);
        self::assertSame(21, $run['result']);
        self::assertSame(3, $run['activity_completed']);
        [$first, $retry] = $run['requests']['workflow'];
        self::assertSame($first['uri'], $retry['uri']);
        self::assertSame($first['body'], $retry['body']);
        self::assertTrue($first['answer']['recorded']);
        self::assertArrayHasKey('history_events', $first['answer']['continuation_tail']);
        self::assertFalse($retry['answer']['recorded']);
        self::assertSame('already_completed', $retry['answer']['reason']);
        self::assertSame('tail_unavailable', $retry['answer']['continuation_tail']['omitted']);
        self::assertSame($first['answer']['created_task_ids'], $retry['answer']['created_task_ids']);
        self::assertStringContainsString('/'.$first['answer']['created_task_ids'][0].'/complete',
            $run['requests']['ordinary'][0]['uri']);
    }

    #[Group('continuation-v2-disposable-sqlite')]
    public function testAnExactActivityContinuationRetryReceivesAnUnavailableTail(): void
    {
        $database = $this->disposableSqliteDatabase();
        $pruned = null;
        $restore = static function () use ($database, &$pruned): void {
            if ($pruned === null) {
                return;
            }
            $insert = $database->prepare('INSERT INTO workflow_history_events
                (id, workflow_run_id, sequence, event_type, payload, workflow_task_id, workflow_command_id,
                 recorded_at, created_at, updated_at, recorded_at_utc)
                VALUES (:id, :workflow_run_id, :sequence, :event_type, :payload, :workflow_task_id, :workflow_command_id,
                        :recorded_at, :created_at, :updated_at, :recorded_at_utc)');
            $insert->execute($pruned);
            $pruned = null;
        };
        $firstUri = null;
        try {
            $run = $this->driveRun('pruned-tail', static function (array $request) use ($database, &$firstUri, &$pruned, $restore): ?array {
                if (!str_ends_with($request['uri'], '/continuation/complete')) {
                    return null;
                }
                if ($firstUri !== null) {
                    return $request['uri'] === $firstUri ? ['after_response' => $restore] : null;
                }

                $firstUri = $request['uri'];
                $runId = $request['body']['continuation']['sticky_cache']['run_id'];
                return [
                    'after_response' => static function (array $answer) use ($database, $runId, &$pruned): void {
                        self::assertTrue($answer['recorded']);
                        self::assertSame('accepted', $answer['continuation']['outcome']);
                        $tail = $answer['continuation_tail'];
                        $event = $tail['history_events'][0];
                        self::assertSame($tail['after_sequence'] + 1, $event['sequence']);
                        $select = $database->prepare('SELECT id, workflow_run_id, sequence, event_type, payload, workflow_task_id,
                            workflow_command_id, recorded_at, created_at, updated_at, recorded_at_utc
                            FROM workflow_history_events WHERE id = :id AND workflow_run_id = :run AND sequence = :sequence');
                        $identity = ['id' => $event['id'], 'run' => $runId, 'sequence' => $event['sequence']];
                        $select->execute($identity);
                        $record = $select->fetch(\PDO::FETCH_ASSOC);
                        self::assertIsArray($record, 'The fixture database contains this test run and its exact committed event.');
                        $select->closeCursor();
                        $delete = $database->prepare('DELETE FROM workflow_history_events
                            WHERE id = :id AND workflow_run_id = :run AND sequence = :sequence');
                        $delete->execute($identity);
                        self::assertSame(1, $delete->rowCount());
                        $pruned = $record;
                    },
                    'response_failure' => new TransportException('Response connection lost.', transientConnectionFailure: true),
                ];
            });
        } finally {
            $restore();
        }
        self::assertSame(21, $run['result']);
        self::assertSame(3, $run['activity_completed']);
        [$first, $retry] = $run['requests']['continuation'];
        self::assertSame($firstUri, $retry['uri']);
        self::assertCount(2, array_filter($run['requests']['continuation'], static fn (array $row): bool => $row['uri'] === $firstUri));
        self::assertSame($first['body'], $retry['body']);
        self::assertFalse($retry['answer']['recorded']);
        self::assertSame('already_completed', $retry['answer']['reason']);
        self::assertSame($first['answer']['continuation'], $retry['answer']['continuation']);
        self::assertSame(['version' => 2, 'after_sequence' => $first['answer']['continuation_tail']['after_sequence'],
            'omitted' => 'tail_unavailable'], $retry['answer']['continuation_tail']);
        $carried = $first['answer']['activity_poll']['answer']['task'];
        $replayed = $retry['answer']['activity_poll']['answer']['task'];
        self::assertSame($carried['task_id'], $replayed['task_id']);
        self::assertSame($carried['lease_expires_at'], $replayed['lease_expires_at']);
        self::assertCount(1, array_filter($run['requests']['ordinary'], static fn (array $row): bool
            => str_ends_with($row['uri'], '/'.$carried['task_id'].'/complete')));
        self::assertSame('omitted', $this->decisions()[0]['tail']);
    }

    #[Group('continuation-v2-disposable-sqlite')]
    public function testAnInvalidContinuationReceiptFallsBackToTheOrdinaryCompletion(): void
    {
        $database = $this->disposableSqliteDatabase();
        $saved = null;
        $replace = $database->prepare('UPDATE dw_task_completions SET receipt = :next WHERE task_id = :task AND receipt = :previous');
        $restore = static function () use ($replace, &$saved): void {
            if ($saved !== null) {
                $replace->execute(['next' => $saved['original'], 'task' => $saved['task'], 'previous' => $saved['changed']]);
                self::assertSame(1, $replace->rowCount());
                $saved = null;
            }
        };

        try {
            $run = $this->driveRun('invalid-receipt', static function (array $request) use ($database, $replace, &$saved, $restore): ?array {
                if (!str_ends_with($request['uri'], '/continuation/complete')
                    || ($request['body']['continuation']['commands'][0]['type'] ?? null) !== 'complete_workflow') {
                    return null;
                }
                if ($saved !== null) {
                    return ['after_error' => $restore];
                }

                $runId = $request['body']['continuation']['sticky_cache']['run_id'];

                return [
                    'after_response' => static function (array $answer) use ($database, $replace, $runId, &$saved): void {
                        self::assertTrue($answer['recorded']);
                        self::assertSame('accepted', $answer['continuation']['outcome']);
                        $task = $answer['task_id'];
                        $select = $database->prepare('SELECT c.receipt FROM dw_task_completions c JOIN workflow_tasks t ON t.id = c.task_id
                            WHERE t.workflow_run_id = :run AND c.task_id = :task');
                        $select->execute(['run' => $runId, 'task' => $task]);
                        $original = $select->fetchColumn();
                        self::assertIsString($original, 'The disposable database has this test run and its exact receipt.');
                        $select->closeCursor();
                        $receipt = json_decode($original, true, flags: JSON_THROW_ON_ERROR);
                        self::assertSame(2, $receipt['activity_workflow_continuation_v2']['version']);
                        $receipt['activity_workflow_continuation_v2']['version'] = 99;
                        $changed = json_encode($receipt, JSON_THROW_ON_ERROR);
                        $replace->execute(['next' => $changed, 'task' => $task, 'previous' => $original]);
                        self::assertSame(1, $replace->rowCount());
                        $saved = ['original' => $original, 'changed' => $changed, 'task' => $task];
                    },
                    'response_failure' => new TransportException('Response connection lost.', transientConnectionFailure: true),
                ];
            });
        } finally {
            $restore();
        }

        self::assertSame(21, $run['result']);
        self::assertSame(3, $run['activity_completed']);
        [$first, $retry] = array_slice($run['requests']['continuation'], -2);
        self::assertSame($first['uri'], $retry['uri']);
        self::assertSame($first['body'], $retry['body']);
        self::assertSame(500, $retry['status']);
        self::assertSame('invalid_continuation_receipt', $retry['answer']['reason']);
        self::assertCount(1, $run['requests']['ordinary']);
        $ordinary = $run['requests']['ordinary'][0];
        self::assertSame(str_replace('/continuation/complete', '/complete', $retry['uri']), $ordinary['uri']);
        self::assertSame($retry['body']['result'], $ordinary['body']['result']);
        self::assertSame($retry['body']['workflow_poll'], $ordinary['body']['workflow_poll']);
        self::assertSame(409, $ordinary['status']);
        self::assertSame('stale_attempt', $ordinary['answer']['reason']);
        self::assertSame([], $run['requests']['failure']);
        self::assertCount(1, array_filter($this->diagnostics, static fn (array $row): bool
            => $row[0] === 'worker.activity_continuation_refused' && ($row[1]['reason'] ?? null) === 'invalid_continuation_receipt'));
    }

    private function disposableSqliteDatabase(): \PDO
    {
        $path = getenv('DURABLE_WORKFLOW_CONTINUATION_V2_FIXTURE_SQLITE');
        if ($path === false || $path === '') {
            self::markTestSkipped('This case needs the disposable native Server SQLite fixture file.');
        }
        self::assertSame($path, realpath($path), 'Use the absolute path of the disposable fixture file.');
        self::assertFileIsWritable($path);
        $database = new \PDO('sqlite:'.$path, options: [\PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION]);
        $database->exec('PRAGMA busy_timeout = 5000');

        return $database;
    }

    /**
     * Start one run of three sequential activities and drive a managed Worker until it is closed.
     *
     * @param \Closure(array{uri: string, headers: array<string, string>, body: array<string, mixed>|null}): (array{
     *     body?: array<string, mixed>, send_first?: array<string, mixed>, after_response?: \Closure, after_error?: \Closure,
     *     response_failure?: TransportException, replay?: bool
     * }|null) $intercept
     * @return array{result: mixed, activity_completed: int, requests: array<string, list<array<string, mixed>>>}
     */
    private function driveRun(string $case, \Closure $intercept, bool $signalFirstActivity = false, bool $bigFirstResult = false,
        bool $withUpdate = false, ?string $expectedTerminalReason = null, ?\Closure $configureWorker = null,
        bool $expectCancelled = false): array
    {
        $suffix = $case.'-'.bin2hex(random_bytes(4));
        $queue = 'php-continuation-v2-'.$suffix;
        $workflowId = 'php-continuation-v2-'.$suffix;
        $type = 'tests.php-continuation-v2';
        $client = new Client($this->url, token: $this->token);
        $worker = null;
        $deadline = microtime(true) + 60;
        $transport = new class($intercept, $client, $workflowId, $deadline, static function () use (&$worker): void {
            $worker?->requestShutdown();
        }) implements BoundedTransport, BoundedPayloadTransport {
            /** @var array<string, list<array<string, mixed>>> */
            public array $requests = ['continuation' => [], 'ordinary' => [], 'history' => [], 'workflow' => [], 'failure' => []];
            private readonly Psr18Transport $inner;
            private float $checkedAt = 0.0;

            public function __construct(private readonly \Closure $intercept, private readonly Client $client, private readonly string $workflowId, private readonly float $deadline,
                private readonly \Closure $stop)
            {
                $this->inner = new Psr18Transport();
            }

            public function supportsBoundedRequests(): bool
            {
                return $this->inner->supportsBoundedRequests();
            }

            public function fetchPayload(string $uri, array $headers, int $maxBytes): string
            {
                return $this->inner->fetchPayload($uri, $headers, $maxBytes);
            }

            public function fetchPayloadBounded(string $uri, array $headers, int $maxBytes, int $timeoutSeconds): string
            {
                return $this->inner->fetchPayloadBounded($uri, $headers, $maxBytes, $timeoutSeconds);
            }

            public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
            {
                return $this->exchange($uri, $headers, $body, fn (?array $body): ?array => $this->inner->send($method, $uri, $headers, $body));
            }

            public function sendBounded(string $method, string $uri, array $headers, ?array $body, int $timeoutSeconds): ?array
            {
                return $this->exchange($uri, $headers, $body,
                    fn (?array $body): ?array => $this->inner->sendBounded($method, $uri, $headers, $body, $timeoutSeconds));
            }

            /** @param \Closure(?array<string, mixed>): ?array<string, mixed> $send */
            private function exchange(string $uri, array $headers, ?array $body, \Closure $send): ?array
            {
                if (str_ends_with($uri, '/workflow-tasks/poll')) {
                    $this->stopWhenClosed();
                }
                $change = ($this->intercept)(['uri' => $uri, 'headers' => $headers, 'body' => $body]) ?? [];
                $originalBody = $body;
                $body = $change['body'] ?? $body;
                $row = ['uri' => $uri, 'body' => $body, 'original_body' => $originalBody];
                if (isset($change['send_first'])) {
                    $first = $change['send_first'];
                    $this->record($uri, ['uri' => $uri, 'body' => $first, 'original_body' => $originalBody, 'answer' => $send($first)]);
                }
                try {
                    $answer = $send($body);
                } catch (TransportException $error) {
                    $this->record($uri, $row + ['status' => $error->status, 'answer' => $error->response]);
                    if (isset($change['after_error'])) {
                        $change['after_error']($error);
                    }
                    throw $error;
                }
                $this->record($uri, $row + ['answer' => $answer]);
                if (isset($change['after_response'])) {
                    $change['after_response']($answer);
                }
                if (isset($change['response_failure'])) {
                    throw $change['response_failure'];
                }
                if (($change['replay'] ?? false) === true) {
                    $answer = $send($body);
                    $this->record($uri, $row + ['answer' => $answer]);
                }

                return $answer;
            }

            /** @param array<string, mixed> $row */
            private function record(string $uri, array $row): void
            {
                if (str_ends_with($uri, '/continuation/complete')) {
                    $this->requests['continuation'][] = $row;
                } elseif (str_ends_with($uri, '/continuation/history')) {
                    $this->requests['history'][] = $row;
                } elseif (preg_match('~/activity-tasks/[^/]+/complete$~', $uri) === 1) {
                    $this->requests['ordinary'][] = $row;
                } elseif (preg_match('~/activity-tasks/[^/]+/fail$~', $uri) === 1) {
                    $this->requests['failure'][] = $row;
                } elseif (preg_match('~/workflow-tasks/[^/]+/complete$~', $uri) === 1) {
                    $this->requests['workflow'][] = $row;
                }
            }

            private function stopWhenClosed(): void
            {
                if (microtime(true) >= $this->deadline) {
                    ($this->stop)();

                    return;
                }
                if (microtime(true) - $this->checkedAt < 0.2) {
                    return;
                }
                $this->checkedAt = microtime(true);
                try {
                    $status = strtolower((string) $this->client->describeWorkflow($this->workflowId)->status);
                } catch (\Throwable) {
                    return;
                }
                if (!in_array($status, ['running', 'waiting', 'pending'], true)) {
                    ($this->stop)();
                }
            }
        };
        $worker = new Worker((new Client($this->url, token: $this->token, transport: $transport))
            ->withActivityLeaseBootstrap($this->bootstrap), $queue,
            workerId: 'php-v2-'.$suffix, buildId: 'php-v2-build',
            diagnosticListener: function (string $event, array $context): void {
                $this->diagnostics[] = [$event, $context];
            },
            enableActivityContinuation: true);
        $worker->declareSignal($type, 'note');
        if ($withUpdate) {
            $worker->registerUpdate($type, 'adjust', static fn (QueryContext $context, int $value): int => $value * 2);
        }
        $worker->registerWorkflow($type, static fn (WorkflowContext $context): mixed
            => self::lengthOf($context->activity('tests.php-continuation-v2.first'))
                + $context->activity('tests.php-continuation-v2.step', [7])
                + $context->activity('tests.php-continuation-v2.step', [7]));
        $worker->registerActivity('tests.php-continuation-v2.first', function (ActivityContext $context) use (
            $client, $workflowId, $signalFirstActivity, $bigFirstResult,
        ): int|string {
            if ($signalFirstActivity) {
                $client->signalWorkflow($workflowId, 'note', [1]);
            }

            return $bigFirstResult ? str_repeat('x', 300_000) : 7;
        });
        $worker->registerActivity('tests.php-continuation-v2.step', static fn (ActivityContext $context, int $value): int => $value);

        $configureWorker?->__invoke($worker);
        $client->startWorkflow($type, $workflowId, $queue);
        $terminal = null;
        try {
            $worker->run(0);
        } catch (ServerException $error) {
            if ($expectedTerminalReason === null || $error->status !== 409 || $error->reason !== $expectedTerminalReason) {
                throw $error;
            }
            $terminal = $error->reason;
        }
        self::assertSame($expectedTerminalReason, $terminal);
        self::assertLessThan($deadline, microtime(true), 'The run closed before the deadline.');
        try {
            $result = $client->workflowResult($workflowId, timeoutSeconds: 5);
            if ($expectCancelled) {
                self::fail('The workflow must close as cancelled.');
            }
        } catch (WorkflowCancelled $error) {
            if (!$expectCancelled) {
                throw $error;
            }
            $result = null;
        }
        $runId = $client->describeWorkflow($workflowId)->runId;
        $completed = 0;
        foreach ($client->iterateWorkflowHistory($workflowId, (string) $runId) as $event) {
            $completed += ($event['event_type'] ?? null) === 'ActivityCompleted' ? 1 : 0;
        }

        return ['result' => $result, 'activity_completed' => $completed, 'requests' => $transport->requests];
    }

    private static function lengthOf(int|string $value): int
    {
        return is_string($value) ? strlen($value) : $value;
    }

    /** @return list<array<string, mixed>> */
    private function decisions(): array
    {
        return array_values(array_map(static fn (array $row): array => $row[1], array_filter($this->diagnostics,
            static fn (array $row): bool => $row[0] === 'worker.activity_continuation' && ($row[1]['version'] ?? null) === 2)));
    }
}
