<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests\Integration;

use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Exception\WorkflowCancelled;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Transport\Transport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\ActivityContext;
use DurableWorkflow\Worker\WorkflowContext;
use DurableWorkflow\WorkflowHandle;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Throwable;

/** Connected source qualification. Published defaults remain protocol 1.19. */
final class CooperativeCancellationTest extends TestCase
{
    private string $runtimeUrl;
    private string $token;
    private string $directory;

    protected function setUp(): void
    {
        if (getenv('DURABLE_WORKFLOW_COOPERATIVE_QUALIFICATION') !== '1') {
            self::markTestSkipped('Candidate cooperative Server qualification is opt-in.');
        }
        $url = getenv('DURABLE_WORKFLOW_RUNTIME_URL');
        if (!is_string($url) || trim($url) === '') {
            self::fail('The cooperative qualification requires DURABLE_WORKFLOW_RUNTIME_URL.');
        }
        if (!function_exists('pcntl_fork') || !function_exists('posix_kill')) {
            self::fail('The connected worker proof requires pcntl and posix.');
        }
        $this->directory = sys_get_temp_dir().'/dw-connected-cooperative-'.bin2hex(random_bytes(8));
        self::assertTrue(mkdir($this->directory, 0700));
        $this->runtimeUrl = $url;
        $token = getenv('DURABLE_WORKFLOW_AUTH_TOKEN');
        $this->token = is_string($token) && $token !== '' ? $token : 'test-token';
        $protocol = $this->client()->clusterInfo()->raw['worker_protocol'];
        self::assertSame('1.20', $protocol['version']);
        self::assertTrue($protocol['server_capabilities']['cooperative_cancellation']);
    }

    protected function tearDown(): void
    {
        if (isset($this->directory)) {
            foreach (glob($this->directory.'/*') ?: [] as $file) { unlink($file); }
            rmdir($this->directory);
        }
    }

    #[DataProvider('booleanProvider')]
    public function testWaitingTimerRunsCleanupAfterLiveOrColdWorkerDelivery(bool $coldReplacement): void
    {
        $queue = $this->queue('timer');
        $client = $this->client();
        [$pid, $messages] = $this->spawnWorker($queue);
        try {
            $this->awaitMessage($messages, 'registered');
            $handle = $client->startWorkflow('tests.php-cooperative', $queue, $queue, ['timer']);
            $this->awaitEvent($client, $handle, 'TimerScheduled');
            if ($coldReplacement) {
                $this->stopWorker($pid, true);
                $pid = 0;
                fclose($messages);
            }
            $accepted = $handle->requestSelectedRunCancellation(cleanupTimeoutSeconds: 60);
            if ($coldReplacement) {
                [$pid, $messages] = $this->spawnWorker($queue);
                $this->awaitMessage($messages, 'registered');
            }
            $events = $this->assertCancelledCleanup($client, $handle, $accepted['cancellation_request']['request_id'], $messages);
            self::assertSame(1, count(array_filter($events, static fn (array $event): bool => $event['event_type'] === 'TimerCancelled')));
            self::assertNotContains('TimerFired', array_column($events, 'event_type'));
        } finally {
            if (is_resource($messages)) {
                fclose($messages);
            }
            $this->stopWorker($pid);
        }
    }

    #[DataProvider('booleanProvider')]
    public function testRequestBeforeClaimRetainsIdentityAfterAcceptedOrDiscardedReply(bool $loseReply): void
    {
        $queue = $this->queue('before-claim');
        $client = $this->client();
        $handle = $client->startWorkflow('tests.php-cooperative', $queue, $queue, ['timer']);
        $accepted = $handle->requestSelectedRunCancellation(cleanupTimeoutSeconds: 60);
        $repeated = $handle->requestSelectedRunCancellation(cleanupTimeoutSeconds: 300);
        self::assertFalse($accepted['duplicate']);
        self::assertTrue($repeated['duplicate']);
        self::assertSame($accepted['cancellation_request'], $repeated['cancellation_request']);
        [$pid, $messages] = $this->spawnWorker($queue, $loseReply);
        try {
            $this->awaitMessage($messages, 'registered');
            if ($loseReply) {
                $this->awaitMessage($messages, 'delivery-reply-discarded');
            }
            $events = $this->assertCancelledCleanup($client, $handle, $accepted['cancellation_request']['request_id'], $messages);
            self::assertNotContains('TimerScheduled', array_column($events, 'event_type'));
        } finally {
            fclose($messages);
            $this->stopWorker($pid);
        }
    }

    #[DataProvider('booleanProvider')]
    public function testLocalRequestStopsBlockedCallbackBeforeReturn(bool $userHeartbeat): void
    {
        $queue = $this->queue('local');
        $client = $this->client();
        [$pid, $messages] = $this->spawnWorker($queue, userHeartbeat: $userHeartbeat);
        try {
            $this->awaitMessage($messages, 'registered');
            $handle = $client->startWorkflow('tests.php-cooperative', $queue, $queue, ['local']);
            $this->awaitMessage($messages, 'local-entered');
            $pids = json_decode((string) file_get_contents($this->directory.'/processes'), true, flags: JSON_THROW_ON_ERROR);
            $started = microtime(true);
            $accepted = $handle->requestSelectedRunCancellation(cleanupTimeoutSeconds: 60);
            $events = $this->assertCancelledCleanup($client, $handle, $accepted['cancellation_request']['request_id'], $messages);
            self::assertLessThan(10, microtime(true) - $started, 'Do not wait for the 60-second callback to return.');
            self::assertFileDoesNotExist($this->directory.'/late');
            foreach ($pids as $activityPid) { $this->assertProcessStops($activityPid); }
            $delivery = array_values(array_filter($events, static fn (array $event): bool => $event['event_type'] === 'CooperativeCancellationDelivered'))[0];
            self::assertSame('local_activity', $delivery['payload']['call_kind']);
            self::assertSame(1, $delivery['payload']['sequence']);
        } finally {
            fclose($messages);
            $this->stopWorker($pid);
        }
    }

    public static function booleanProvider(): array
    {
        return [[false], [true]];
    }

    public function testCooperativeWorkerHydratesAndPublishesAboveInlinePayloads(): void
    {
        $namespace = $this->queue('payloads');
        $admin = $this->client();
        $admin->createNamespace($namespace);
        $admin->setNamespaceExternalStorage($namespace, 'local', thresholdBytes: 64,
            config: ['uri' => 'file:///app/storage/app/cooperative-payloads/'.$namespace]);
        $client = $this->client(namespace: $namespace);
        $value = str_repeat('bounded-payload-', 131072);
        self::assertGreaterThan(2097152, strlen($client->payloadCodec()->encode($value)));
        [$pid, $messages] = $this->spawnWorker($namespace, namespace: $namespace);
        try {
            $this->awaitMessage($messages, 'registered');
            $handle = $client->startWorkflow('tests.php-cooperative-payload', $namespace, $namespace, [$value]);
            self::assertSame($value, $handle->result(timeoutSeconds: 30));
            $raw = (new Psr18Transport())->send('GET', $this->runtimeUrl.'/api/workflows/'.$handle->workflowId,
                ['Authorization' => 'Bearer '.$this->token, 'X-Namespace' => $namespace,
                    'X-Durable-Workflow-Control-Plane-Version' => '2']);
            self::assertArrayHasKey('external_payload', $raw['output_envelope']);
            $reference = $raw['output_envelope']['external_payload'];
            $blob = $client->payloadCodec()->encode($value);
            self::assertSame(strlen($blob), $reference['size_bytes']);
            self::assertSame(hash('sha256', $blob), $reference['sha256']);
        } finally {
            fclose($messages);
            $this->stopWorker($pid);
        }
    }

    private function client(?Transport $transport = null, string $namespace = 'default'): Client
    {
        return new Client($this->runtimeUrl, namespace: $namespace, token: $this->token,
            transport: $transport, workerProtocolVersion: '1.20');
    }

    private function queue(string $kind): string
    {
        return 'php-cooperative-'.$kind.'-'.bin2hex(random_bytes(4));
    }

    /** @return array{int, resource} */
    private function spawnWorker(string $queue, bool $loseReply = false, bool $userHeartbeat = true, string $namespace = 'default'): array
    {
        $sockets = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
        if ($sockets === false) {
            self::fail('Could not create the worker observation socket pair.');
        }
        [$parent, $child] = $sockets;
        $pid = pcntl_fork();
        if ($pid === -1) {
            fclose($parent);
            fclose($child);
            self::fail('Could not fork the cooperative worker.');
        }
        if ($pid === 0) {
            fclose($parent);
            $notify = static function (string $message) use ($child): void {
                fwrite($child, $message."\n");
                fflush($child);
            };
            try {
                // Construct transport and worker after fork. No inherited HTTP connection is used.
                $transport = $loseReply ? new DiscardFirstDeliveryReplyTransport($notify) : null;
                $failureReported = false;
                $worker = new Worker($this->client($transport, $namespace), $queue,
                    workerId: $queue.'-'.getmypid(), enableCooperativeCancellation: true,
                    diagnosticListener: function (string $event, array $context) use ($notify, &$failureReported): void {
                        if ($event === 'worker.registered') {
                            $notify('registered');
                        }
                        if ($event === 'worker.activity_process_started'
                            && ($context['activity_type'] ?? null) === 'tests.php-cooperative-work') {
                            file_put_contents($this->directory.'/processes', json_encode([
                                $context['relay_pid'], $context['callback_pid'],
                            ], JSON_THROW_ON_ERROR));
                        }
                        if (!$failureReported && ($context['exception'] ?? null) instanceof Throwable) {
                            $failureReported = true;
                            $error = $context['exception'];
                            $causes = [];
                            do {
                                $causes[] = $error::class.': '.$error->getMessage();
                                $error = $error->getPrevious();
                            } while ($error !== null && count($causes) < 4);
                            fwrite(STDERR, 'Connected worker failure: '.implode(' | ', $causes)."\n");
                            $notify('worker-failure: '.implode(' | ', $causes));
                        }
                    });
                $worker->registerWorkflow('tests.php-cooperative', static function (WorkflowContext $context, string $kind): string {
                    try {
                        if ($kind === 'local') {
                            $context->localActivity('tests.php-cooperative-work');
                        } else {
                            $context->sleep(300);
                        }
                    } catch (WorkflowCancelled $error) {
                        $context->cancellationShield(static fn () => $context->localActivity('tests.php-cooperative-cleanup', [$error->requestId]));
                        return (string) $error->requestId;
                    }
                    return 'not-cancelled';
                });
                $worker->registerWorkflow('tests.php-cooperative-payload', static fn (WorkflowContext $context, string $value): string => $value);
                $worker->registerActivity('tests.php-cooperative-work', function (ActivityContext $context) use ($notify, $userHeartbeat): \stdClass {
                    $notify('local-entered');
                    $deadline = microtime(true) + 60;
                    while (microtime(true) < $deadline) {
                        usleep(100_000);
                        if ($userHeartbeat) {
                            $context->heartbeat(['qualification' => 'local-in-flight']);
                        }
                    }
                    file_put_contents($this->directory.'/late', 'late callback returned');
                    // Encoding this value would fail. Cancellation must discard it first.
                    return new \stdClass();
                });
                $worker->registerActivity('tests.php-cooperative-cleanup', static function (ActivityContext $context, string $requestId): string {
                    $context->heartbeat(['request_id' => $requestId]);
                    return $requestId;
                });
                $worker->run(1);
                fclose($child);
                exit(0);
            } catch (Throwable $error) {
                $notify('error:'.$error::class.':'.$error->getMessage());
                fclose($child);
                exit(1);
            }
        }
        fclose($child);
        return [$pid, $parent];
    }

    /** @param resource $messages */
    private function awaitMessage($messages, string $expected): void
    {
        stream_set_timeout($messages, 15);
        self::assertSame($expected, trim((string) fgets($messages)), 'Unexpected worker observation.');
    }

    private function stopWorker(int $pid, bool $kill = false): void
    {
        if ($pid <= 0) {
            return;
        }
        posix_kill($pid, $kill ? SIGKILL : SIGTERM);
        $deadline = microtime(true) + 10;
        do {
            $result = pcntl_waitpid($pid, $status, WNOHANG);
            if ($result === $pid) {
                if ($kill) {
                    self::assertTrue(pcntl_wifsignaled($status));
                    self::assertSame(SIGKILL, pcntl_wtermsig($status));
                } else {
                    self::assertTrue(pcntl_wifexited($status));
                    self::assertSame(0, pcntl_wexitstatus($status), 'Cooperative worker failed.');
                }
                return;
            }
            usleep(50_000);
        } while (microtime(true) < $deadline);
        posix_kill($pid, SIGKILL);
        pcntl_waitpid($pid, $status);
        self::fail('Cooperative worker did not stop after its shutdown request.');
    }

    private function assertProcessStops(int $pid): void
    {
        $deadline = microtime(true) + 3;
        do {
            if (!posix_kill($pid, 0)) { self::assertFalse(posix_kill($pid, 0)); return; }
            usleep(10_000);
        } while (microtime(true) < $deadline);
        self::fail('Activity process survived cancellation: '.$pid);
    }

    /** @return list<array<string, mixed>> */
    private function history(Client $client, WorkflowHandle $handle): array
    {
        $history = $client->workflowHistory($handle->workflowId, (string) $handle->selectedRunId);
        self::assertNull($history['next_page_token'] ?? null);
        return $history['events'] ?? $history['history_events'] ?? [];
    }

    private function awaitEvent(Client $client, WorkflowHandle $handle, string $kind): void
    {
        $deadline = microtime(true) + 20;
        do {
            if (in_array($kind, array_column($this->history($client, $handle), 'event_type'), true)) {
                return;
            }
            usleep(100_000);
        } while (microtime(true) < $deadline);
        throw new RuntimeException('Workflow did not record '.$kind.'.');
    }

    /** @param resource $messages
     *  @return list<array<string, mixed>>
     */
    private function assertCancelledCleanup(Client $client, WorkflowHandle $handle, string $requestId, $messages): array
    {
        stream_set_blocking($messages, false);
        $deadline = microtime(true) + 30;
        do {
            $message = fgets($messages);
            if (is_string($message) && trim($message) !== '') {
                self::fail(trim($message));
            }
            $status = strtolower((string) $handle->describe()->status);
            if (in_array($status, ['completed', 'failed', 'cancelled', 'terminated', 'timed_out'], true)) {
                break;
            }
            usleep(100_000);
        } while (microtime(true) < $deadline);
        self::assertSame('cancelled', $status, 'Workflow did not finish its bounded cooperative cleanup.');
        try {
            $handle->result(1, 0.1);
            self::fail('A cooperatively cancelled run must retain its cancelled result.');
        } catch (WorkflowCancelled) {
        }
        $events = $this->history($client, $handle);
        $kinds = array_column($events, 'event_type');
        foreach (['CooperativeCancellationRequested', 'CooperativeCancellationDelivered', 'WorkflowCancelled', 'ActivityCompleted'] as $kind) {
            self::assertSame(1, count(array_filter($kinds, static fn (string $value): bool => $value === $kind)), $kind);
        }
        foreach (['WorkflowCompleted', 'WorkflowFailed', 'ActivityFailed', 'ActivityTimedOut'] as $kind) {
            self::assertNotContains($kind, $kinds);
        }
        foreach ($events as $event) {
            if (in_array($event['event_type'], ['CooperativeCancellationRequested', 'CooperativeCancellationDelivered', 'WorkflowCancelled'], true)) {
                self::assertSame($requestId, $event['payload']['workflow_command_id']);
            }
            if ($event['event_type'] === 'ActivityCompleted') {
                self::assertSame($requestId, (new AvroPayloadCodec())->decodeEnvelope($event['payload']['result']));
            }
        }
        return $events;
    }
}

/** Discards one successful Server response after its real durable commit. */
final class DiscardFirstDeliveryReplyTransport implements \DurableWorkflow\Transport\BoundedTransport
{
    private Psr18Transport $inner;
    private bool $discarded = false;

    public function __construct(private readonly \Closure $notify)
    {
        $this->inner = new Psr18Transport();
    }

    public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
    {
        return $this->maybeDiscard($uri, $this->inner->send($method, $uri, $headers, $body));
    }

    public function supportsBoundedRequests(): bool
    {
        return $this->inner->supportsBoundedRequests();
    }

    public function sendBounded(string $method, string $uri, array $headers, ?array $body, int $timeoutSeconds): ?array
    {
        return $this->maybeDiscard($uri, $this->inner->sendBounded($method, $uri, $headers, $body, $timeoutSeconds));
    }

    private function maybeDiscard(string $uri, ?array $response): ?array
    {
        if (!$this->discarded && str_ends_with($uri, '/deliver-cancellation')) {
            $this->discarded = true;
            ($this->notify)('delivery-reply-discarded');
            throw new TransportException('Qualification discarded a successful delivery reply.');
        }
        return $response;
    }
}
