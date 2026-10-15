<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests\Integration;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\QueryFailed;
use DurableWorkflow\Transport\BoundedTransport;
use DurableWorkflow\Transport\BoundedPayloadTransport;
use DurableWorkflow\Transport\BoundedPayloadUploadTransport;
use DurableWorkflow\Transport\BoundedProgressPayloadTransport;
use DurableWorkflow\Transport\Psr18Transport;
use DurableWorkflow\Transport\Transport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\QueryContext;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Throwable;

/** Actual normal Worker.run dispatch against an isolated four-second query runtime. */
final class QueryTaskAuthorityTest extends TestCase
{
    public static function modes(): array
    {
        return ['timely 1.20' => ['timely', '1.20'], 'timely 1.19' => ['timely', '1.19'],
            'late-poll' => ['late-poll', '1.20'], 'late-handler' => ['late-handler', '1.20'],
            'loading-expiry' => ['loading-expiry', '1.20']];
    }

    #[DataProvider('modes')]
    public function testOriginalQueryAuthorityAcrossDispatch(string $mode, string $protocol): void
    {
        if (getenv('DURABLE_WORKFLOW_QUERY_AUTHORITY_QUALIFICATION') !== '1') {
            self::markTestSkipped('Requires an isolated Server with DW_QUERY_TASK_TIMEOUT=4 and shared polling cache.');
        }
        self::assertTrue(function_exists('pcntl_fork') && function_exists('posix_kill'));
        $url = (string) getenv('DURABLE_WORKFLOW_RUNTIME_URL');
        self::assertNotSame('', $url);
        $token = getenv('DURABLE_WORKFLOW_AUTH_TOKEN') ?: 'test-token';
        $namespace = 'php-query-authority-'.bin2hex(random_bytes(6));
        $admin = new Client($url, token: $token);
        $admin->createNamespace($namespace);
        $client = new Client($url, namespace: $namespace, token: $token);
        if ($mode === 'loading-expiry') {
            $admin->setNamespaceExternalStorage($namespace, 'local', thresholdBytes: 1024);
            self::assertSame('available', $client->clusterInfo()->raw['namespace']['external_payload_storage']['status']);
        }
        $directory = sys_get_temp_dir().'/'.$namespace;
        self::assertTrue(mkdir($directory, 0700));
        $journal = $directory.'/events.jsonl';
        $record = static function (string $event, array $details = []) use ($journal): void {
            file_put_contents($journal, json_encode(['event' => $event] + $details, JSON_THROW_ON_ERROR)."\n", FILE_APPEND | LOCK_EX);
        };
        $pid = 0;
        $handle = null;
        try {
            $pid = pcntl_fork();
            self::assertGreaterThanOrEqual(0, $pid);
            if ($pid === 0) {
                try {
                    $transport = new class($mode, $record) implements Transport, BoundedTransport,
                        BoundedPayloadTransport, BoundedPayloadUploadTransport, BoundedProgressPayloadTransport {
                        private readonly Psr18Transport $inner;
                        private bool $queryLoading = false;
                        public function __construct(private readonly string $mode, private readonly \Closure $record)
                        { $this->inner = new Psr18Transport(); }
                        public function supportsBoundedRequests(): bool { return $this->inner->supportsBoundedRequests(); }
                        public function send(string $method, string $uri, array $headers, ?array $body = null): ?array
                        { return $this->observe($uri, $this->inner->send($method, $uri, $headers, $body)); }
                        public function sendBounded(string $method, string $uri, array $headers, ?array $body, int $timeoutSeconds): ?array
                        { return $this->observe($uri, $this->inner->sendBounded($method, $uri, $headers, $body, $timeoutSeconds)); }
                        public function uploadPayload(string $uri, array $headers, string $blob, int $timeoutSeconds): array
                        { return $this->inner->uploadPayload($uri, $headers, $blob, $timeoutSeconds); }
                        public function uploadPayloadBounded(string $uri, array $headers, string $blob, int $timeoutSeconds): array
                        { return $this->inner->uploadPayloadBounded($uri, $headers, $blob, $timeoutSeconds); }
                        public function fetchPayload(string $uri, array $headers, int $maxBytes): string
                        { return $this->inner->fetchPayload($uri, $headers, $maxBytes); }
                        public function fetchPayloadBounded(string $uri, array $headers, int $maxBytes, int $timeoutSeconds): string
                        { return $this->inner->fetchPayloadBounded($uri, $headers, $maxBytes, $timeoutSeconds); }
                        public function fetchPayloadWithProgress(string $uri, array $headers, int $maxBytes, \Closure $progress): string
                        { return $this->inner->fetchPayloadWithProgress($uri, $headers, $maxBytes, $progress); }
                        public function fetchPayloadWithBoundedProgress(string $uri, array $headers, int $maxBytes, \Closure $progress, ?int $timeoutSeconds = null): string
                        {
                            ($this->record)('payload-read');
                            return $this->inner->fetchPayloadWithBoundedProgress($uri, $headers, $maxBytes,
                                function () use ($progress): void {
                                    $progress();
                                    if ($this->queryLoading && $this->mode === 'loading-expiry') {
                                        $this->queryLoading = false;
                                        $until = microtime(true) + 6;
                                        while (microtime(true) < $until) { $progress(); usleep(50000); }
                                    }
                                }, $timeoutSeconds);
                        }
                        private function observe(string $uri, ?array $reply): ?array
                        {
                            if (str_ends_with($uri, '/query-tasks/poll') && is_array($reply['task'] ?? null)) {
                                ($this->record)('claim', $reply['task']);
                                $this->queryLoading = true;
                                if ($this->mode === 'late-poll') { sleep(6); }
                            } elseif (str_contains($uri, '/query-tasks/') && str_contains($uri, '/status?')) {
                                ($this->record)('status', ['can_continue' => $reply['can_continue'] ?? null]);
                            } elseif (str_contains($uri, '/query-tasks/') && preg_match('~/(complete|fail)$~', $uri)) {
                                ($this->record)('acknowledgement');
                            }
                            return $reply;
                        }
                    };
                    $worker = new Worker(new Client($url, namespace: $namespace, token: $token,
                        transport: $transport, workerProtocolVersion: $protocol), $namespace,
                        diagnosticListener: static function (string $event, array $details) use ($record): void {
                            if ($event === 'worker.claim_aborted') { $record('aborted', $details); }
                        }, requireQueryTaskAuthority: $protocol === '1.20');
                    $worker->registerWorkflow('tests.php-query-authority', static function (WorkflowContext $context, mixed $input = null): int {
                        $context->sleep(60);
                        return 7;
                    });
                    $worker->registerQuery('tests.php-query-authority', 'value', static function (QueryContext $context) use ($mode, $record): array {
                        $record('handler');
                        if ($mode === 'late-handler') { sleep(6); }
                        $record('returned');
                        return ['long' => PHP_INT_MAX, 'nil' => null, 'text' => 'λ'];
                    });
                    pcntl_async_signals(true);
                    pcntl_signal(SIGTERM, static fn () => $worker->requestShutdown());
                    $worker->run(0);
                    $record('stopped');
                    exit(0);
                } catch (Throwable $error) {
                    $record('actor-error', ['type' => $error::class, 'message' => $error->getMessage()]);
                    fwrite(STDERR, json_encode(['mode' => $mode, 'actor_error' => $error::class,
                        'message' => $error->getMessage()], JSON_THROW_ON_ERROR)."\n");
                    exit(1);
                }
            }
            $until = microtime(true) + 10;
            do {
                if ($client->listWorkers($namespace) !== []) { break; }
                usleep(50000);
            } while (microtime(true) < $until);
            $handle = $client->startWorkflow('tests.php-query-authority', $namespace, $namespace,
                $mode === 'loading-expiry' ? [str_repeat('x', 65536)] : []);
            do {
                if (strtolower((string) $handle->describe()->status) === 'waiting') { break; }
                usleep(50000);
            } while (microtime(true) < $until);
            self::assertSame('waiting', strtolower((string) $handle->describe()->status));
            $before = $client->workflowHistory($namespace, $handle->selectedRunId);
            if ($mode === 'timely') {
                self::assertSame(['long' => PHP_INT_MAX, 'nil' => null, 'text' => 'λ'], $handle->query('value'));
            } else {
                try {
                    $handle->query('value');
                    self::fail('The expired original query succeeded.');
                } catch (QueryFailed $error) {
                    self::assertSame(504, $error->status);
                    self::assertSame('query_worker_execution_timeout', $error->details['reason'] ?? null);
                }
                $until = microtime(true) + 8;
                do {
                    $events = self::readJournal($journal);
                    if (in_array('aborted', array_column($events, 'event'), true)) { break; }
                    usleep(50000);
                } while (microtime(true) < $until);
                self::assertContains('aborted', array_column($events, 'event'));
            }
            $events = self::readJournal($journal);
            $names = array_column($events, 'event');
            $claims = array_values(array_filter($events, static fn (array $event): bool => $event['event'] === 'claim'));
            self::assertCount(1, $claims);
            $admitted = new \DateTimeImmutable($claims[0]['query_admitted_at']);
            $deadline = new \DateTimeImmutable($claims[0]['query_deadline_at']);
            self::assertSame(4.0, (float) $deadline->format('U.u') - (float) $admitted->format('U.u'));
            self::assertSame($before, $client->workflowHistory($namespace, $handle->selectedRunId));
            self::assertSame(in_array($mode, ['late-poll', 'loading-expiry'], true) ? 0 : 1,
                count(array_filter($names, static fn (string $event): bool => $event === 'handler')));
            self::assertSame($mode === 'timely' ? 1 : 0, count(array_filter($names, static fn (string $event): bool => $event === 'acknowledgement')));
            self::assertSame($protocol === '1.20', in_array('status', $names, true));
            self::assertNotContains('actor-error', $names);
            if ($mode === 'loading-expiry') {
                self::assertContains('payload-read', $names);
                self::assertSame(\DurableWorkflow\Transport\RuntimePayloads::SCHEMA,
                    $claims[0]['workflow_arguments']['external_payload']['schema'] ?? null);
            }
            fwrite(STDOUT, json_encode(['mode' => $mode, 'protocol' => $protocol,
                'query_task_id' => $claims[0]['query_task_id'], 'outcome' => 'pass'], JSON_THROW_ON_ERROR)."\n");
        } finally {
            if ($pid > 0) {
                posix_kill($pid, SIGTERM);
                $until = microtime(true) + 10;
                do {
                    $ended = pcntl_waitpid($pid, $status, WNOHANG);
                    if ($ended === $pid) { break; }
                    usleep(50000);
                } while (microtime(true) < $until);
                if ($ended !== $pid) { posix_kill($pid, SIGKILL); pcntl_waitpid($pid, $status); }
                $drained = pcntl_wifexited($status) && pcntl_wexitstatus($status) === 0;
            }
            try {
                try {
                    if ($handle !== null) { $client->terminateWorkflow($namespace, 'qualification complete', $handle->selectedRunId); }
                } finally {
                    $admin->deleteNamespace($namespace);
                }
            } finally {
                if (is_file($journal)) { unlink($journal); }
                rmdir($directory);
            }
            if ($pid > 0) {
                self::assertTrue($drained, 'The actual Worker must drain and deregister normally.');
            }
        }
    }

    private static function readJournal(string $path): array
    {
        return array_map(static fn (string $line): array => json_decode($line, true, flags: JSON_THROW_ON_ERROR),
            is_file($path) ? file($path, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES) : []);
    }
}
