<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests\Integration;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Transport\BoundedTransport;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\WorkflowContext;
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

    public function testAnUnknownOutcomeIsResentIdenticallyAndAnsweredFromTheReceipt(): void
    {
        $dropped = false;
        $run = $this->driveRun('unknown', static function (array $request) use (&$dropped): ?array {
            if (!$dropped && str_ends_with($request['uri'], '/continuation/complete')) {
                $dropped = true;

                // The Server commits the request; its answer is lost on the way back.
                return ['drop_answer' => true];
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

    public function testARefusalFallsBackToTheOrdinaryCompletion(): void
    {
        $refused = false;
        $run = $this->driveRun('refusal', static function (array $request) use (&$refused): ?array {
            if (!$refused && str_ends_with($request['uri'], '/continuation/complete')) {
                $refused = true;
                $request['body']['continuation']['unknown_member'] = true;

                return ['body' => $request['body']];
            }

            return null;
        });
        self::assertTrue($refused);
        self::assertSame(21, $run['result']);
        self::assertSame(3, $run['activity_completed']);
        // The refused activity completed the ordinary way with the same carried workflow poll.
        $refusedRequest = $run['requests']['continuation'][0];
        $ordinary = $run['requests']['ordinary'][0] ?? null;
        self::assertSame(422, $refusedRequest['status'] ?? null);
        self::assertIsArray($ordinary);
        self::assertSame(str_replace('/continuation/complete', '/complete', $refusedRequest['uri']), $ordinary['uri']);
        self::assertSame($refusedRequest['body']['workflow_poll'], $ordinary['body']['workflow_poll']);
        self::assertSame($refusedRequest['body']['result'], $ordinary['body']['result']);
        self::assertTrue($ordinary['answer']['recorded'] ?? false);
        $refusals = array_values(array_filter($this->diagnostics, static fn (array $row): bool => $row[0] === 'worker.activity_continuation_refused'));
        self::assertSame([[422, 'invalid_continuation']], array_map(static fn (array $row): array => [$row[1]['status'], $row[1]['reason']], $refusals));
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

    public function testAnOverBudgetTailIsOmittedAndTheLeasedActivityCompletesTheOrdinaryWay(): void
    {
        $run = $this->driveRun('budget', static fn (array $request): ?array => null, bigFirstResult: true);
        self::assertSame(300_014, $run['result']);
        $first = $this->decisions()[0];
        self::assertSame(['accepted', 'omitted'], [$first['outcome'], $first['tail']]);
        self::assertCount(1, array_filter($this->diagnostics, static fn (array $row): bool
            => $row[0] === 'worker.activity_continuation_skipped' && ($row[1]['reason'] ?? null) === 'cache_tail_not_this_activity'));
        self::assertNotSame([], $run['requests']['ordinary']);
    }

    /**
     * Start one run of three sequential activities and drive a managed Worker until it is closed.
     *
     * @param \Closure(array{uri: string, body: array<string, mixed>|null}): (array<string, mixed>|null) $intercept
     * @return array{result: mixed, activity_completed: int, requests: array<string, list<array<string, mixed>>>}
     */
    private function driveRun(string $case, \Closure $intercept, bool $signalFirstActivity = false, bool $bigFirstResult = false): array
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
        }) implements BoundedTransport {
            /** @var array<string, list<array<string, mixed>>> */
            public array $requests = ['continuation' => [], 'ordinary' => [], 'history' => []];
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

            public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
            {
                return $this->exchange($uri, $body, fn (?array $body): ?array => $this->inner->send($method, $uri, $headers, $body));
            }

            public function sendBounded(string $method, string $uri, array $headers, ?array $body, int $timeoutSeconds): ?array
            {
                return $this->exchange($uri, $body,
                    fn (?array $body): ?array => $this->inner->sendBounded($method, $uri, $headers, $body, $timeoutSeconds));
            }

            /** @param \Closure(?array<string, mixed>): ?array<string, mixed> $send */
            private function exchange(string $uri, ?array $body, \Closure $send): ?array
            {
                if (str_ends_with($uri, '/workflow-tasks/poll')) {
                    $this->stopWhenClosed();
                }
                $change = ($this->intercept)(['uri' => $uri, 'body' => $body]) ?? [];
                $body = $change['body'] ?? $body;
                $row = ['uri' => $uri, 'body' => $body];
                try {
                    $answer = $send($body);
                } catch (TransportException $error) {
                    $this->record($uri, $row + ['status' => $error->status, 'answer' => $error->response]);
                    throw $error;
                }
                $this->record($uri, $row + ['answer' => $answer]);
                if (($change['drop_answer'] ?? false) === true) {
                    throw new TransportException('The answer was lost.', transientConnectionFailure: true);
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

        $client->startWorkflow($type, $workflowId, $queue);
        $worker->run(0);
        self::assertLessThan($deadline, microtime(true), 'The run closed before the deadline.');
        $result = $client->workflowResult($workflowId, timeoutSeconds: 5);
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
