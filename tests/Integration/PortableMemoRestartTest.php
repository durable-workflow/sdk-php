<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests\Integration;

use DurableWorkflow\Attribute\Signal;
use DurableWorkflow\Attribute\Workflow;
use DurableWorkflow\Client;
use DurableWorkflow\Codec\AvroBinaryValue;
use DurableWorkflow\Codec\AvroPayloadCodec;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\WorkflowContext;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Throwable;

final class PortableMemoRestartTest extends TestCase
{
    private const MEMO_BLOB = 'wwHioz3/VYAiNw4MDGJpbmFyeQgIc2FtZQxkb3VibGUGAAAAAAAAHEAcaW52YWxpZF9iaW5hcnkIBP8ACGxvbmcEDgxuZXN0ZWQOBAphbHBoYQQCCGJldGEEBAAIdGV4dAoIc2FtZQA=';

    public function testFreshPhpWorkerReplaysPersistedServerMemoHistory(): void
    {
        $runtimeUrl = getenv('DURABLE_WORKFLOW_RUNTIME_URL');
        if (!is_string($runtimeUrl) || trim($runtimeUrl) === '') {
            self::markTestSkipped('DURABLE_WORKFLOW_RUNTIME_URL is not set.');
        }
        if (!function_exists('pcntl_fork') || !function_exists('posix_kill')) {
            self::markTestSkipped('The pcntl and posix extensions are required for the worker restart proof.');
        }

        $token = getenv('DURABLE_WORKFLOW_AUTH_TOKEN');
        $token = is_string($token) && $token !== '' ? $token : 'test-token';
        $suffix = bin2hex(random_bytes(4));
        $taskQueue = "memo-restart-php-{$suffix}";
        $workflowId = "memo-restart-php-{$suffix}";
        $client = new Client($runtimeUrl, namespace: 'default', token: $token);

        [$firstPid, $firstReady] = $this->spawnWorker(
            $runtimeUrl,
            $token,
            $taskQueue,
            "memo-php-before-{$suffix}",
        );

        try {
            $this->awaitWorkerRegistration($firstReady, $firstPid);
            $handle = $client->startWorkflow(
                workflowType: 'tests.memo-restart-php',
                workflowId: $workflowId,
                taskQueue: $taskQueue,
            );

            $waiting = $this->awaitStatus($handle, 'waiting');
            self::assertEquals([
                'binary' => ['$type' => 'bytes', 'base64' => 'c2FtZQ=='],
                'double' => 7.0,
                'invalid_binary' => ['$type' => 'bytes', 'base64' => '/wA='],
                'long' => 7,
                'nested' => ['alpha' => 1, 'beta' => 2],
                'text' => 'same',
            ], $waiting->memo);

            $firstHistory = $client->workflowHistory($workflowId, (string) $handle->selectedRunId);
            $firstMemoEvents = $this->memoEvents($firstHistory);
            self::assertCount(1, $firstMemoEvents);
            $this->assertTypedMemoEvent($firstMemoEvents[0]);
        } finally {
            fclose($firstReady);
            $this->stopWorker($firstPid);
        }

        [$replacementPid, $replacementReady] = $this->spawnWorker(
            $runtimeUrl,
            $token,
            $taskQueue,
            "memo-php-after-{$suffix}",
        );

        try {
            $this->awaitWorkerRegistration($replacementReady, $replacementPid);
            $handle->signal('finish');

            self::assertSame('php-replayed-memo', $handle->result(30, 0.1));
            $completed = $this->awaitStatus($handle, 'completed');
            self::assertEquals($waiting->memo, $completed->memo);

            $finalHistory = $client->workflowHistory($workflowId, (string) $handle->selectedRunId);
            $finalMemoEvents = $this->memoEvents($finalHistory);
            self::assertCount(1, $finalMemoEvents);
            $this->assertTypedMemoEvent($finalMemoEvents[0]);
        } finally {
            fclose($replacementReady);
            $this->stopWorker($replacementPid);
        }
    }

    /** @return array{int, resource} */
    private function spawnWorker(
        string $runtimeUrl,
        string $token,
        string $taskQueue,
        string $workerId,
    ): array {
        $sockets = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
        if ($sockets === false) {
            self::fail('Could not create the worker readiness socket pair.');
        }

        [$parentSocket, $childSocket] = $sockets;
        $pid = pcntl_fork();
        if ($pid === -1) {
            self::fail('Could not fork the memo integration worker.');
        }

        if ($pid === 0) {
            fclose($parentSocket);
            try {
                $workerClient = new Client($runtimeUrl, namespace: 'default', token: $token);
                $worker = new Worker(
                    $workerClient,
                    $taskQueue,
                    workerId: $workerId,
                    diagnosticListener: static function (string $event) use ($childSocket): void {
                        if ($event === 'worker.registered') {
                            fwrite($childSocket, "registered\n");
                            fflush($childSocket);
                        }
                    },
                );
                $worker->register(MemoRestartPhpWorkflow::class)->run(1);
                fclose($childSocket);
                exit(0);
            } catch (Throwable $exception) {
                fwrite($childSocket, 'error:'.$exception::class.':'.$exception->getMessage()."\n");
                fflush($childSocket);
                fclose($childSocket);
                exit(1);
            }
        }

        fclose($childSocket);

        return [$pid, $parentSocket];
    }

    /** @param resource $readiness */
    private function awaitWorkerRegistration($readiness, int $pid): void
    {
        stream_set_timeout($readiness, 10);
        $message = fgets($readiness);
        if ($message === false || trim($message) !== 'registered') {
            $this->stopWorker($pid);
            self::fail('Memo worker did not register: '.trim((string) $message));
        }
    }

    private function awaitStatus(object $handle, string $status): object
    {
        $deadline = microtime(true) + 30;
        $last = null;
        while (microtime(true) < $deadline) {
            $last = $handle->describe();
            if (strtolower((string) $last->status) === $status
                && ($status !== 'waiting' || $last->memo !== null)
            ) {
                return $last;
            }
            usleep(100_000);
        }

        throw new RuntimeException(sprintf(
            'Workflow did not reach %s (last status: %s).',
            $status,
            is_object($last) ? (string) $last->status : 'none',
        ));
    }

    /**
     * @param array<string, mixed> $history
     * @return list<array<string, mixed>>
     */
    private function memoEvents(array $history): array
    {
        $events = $history['events'] ?? $history['history_events'] ?? [];

        return array_values(array_filter(
            is_array($events) ? $events : [],
            static fn (mixed $event): bool => is_array($event)
                && ($event['event_type'] ?? null) === 'MemoUpserted',
        ));
    }

    /** @param array<string, mixed> $event */
    private function assertTypedMemoEvent(array $event): void
    {
        $codec = new AvroPayloadCodec();
        foreach (['entries', 'merged'] as $field) {
            $envelope = $event['payload'][$field] ?? null;
            self::assertSame(['codec' => 'avro', 'blob' => self::MEMO_BLOB], $envelope);
            $decoded = $codec->decodeEnvelope($envelope);
            self::assertIsInt($decoded['long']);
            self::assertSame(7, $decoded['long']);
            self::assertIsFloat($decoded['double']);
            self::assertSame(7.0, $decoded['double']);
            self::assertInstanceOf(AvroBinaryValue::class, $decoded['binary']);
            self::assertSame('same', $decoded['binary']->bytes);
            self::assertInstanceOf(AvroBinaryValue::class, $decoded['invalid_binary']);
            self::assertSame("\xFF\x00", $decoded['invalid_binary']->bytes);
            self::assertSame('ff00', bin2hex($decoded['invalid_binary']->bytes));
            self::assertIsString($decoded['text']);
            self::assertSame('same', $decoded['text']);
            self::assertSame(['alpha', 'beta'], array_keys($decoded['nested']));
            self::assertSame(['alpha' => 1, 'beta' => 2], $decoded['nested']);
        }
    }

    private function stopWorker(int $pid): void
    {
        if ($pid <= 0) {
            return;
        }
        posix_kill($pid, SIGTERM);
        $deadline = microtime(true) + 10;
        do {
            $result = pcntl_waitpid($pid, $status, WNOHANG);
            if ($result === $pid) {
                self::assertSame(0, pcntl_wexitstatus($status), 'Memo worker process failed.');

                return;
            }
            usleep(50_000);
        } while (microtime(true) < $deadline);

        posix_kill($pid, SIGKILL);
        pcntl_waitpid($pid, $status);
        self::fail('Memo worker did not stop after SIGTERM.');
    }
}

final class MemoRestartPhpWorkflow
{
    #[Workflow('tests.memo-restart-php')]
    public function run(WorkflowContext $context): string
    {
        $context->upsertMemo([
            'text' => 'same',
            'nested' => ['beta' => 2, 'alpha' => 1],
            'long' => 7,
            'double' => 7.0,
            'binary' => AvroBinaryValue::fromBytes('same'),
            'invalid_binary' => AvroBinaryValue::fromBytes("\xFF\x00"),
        ]);
        $context->waitCondition(
            static fn (): bool => $context->signals('finish') !== [],
            key: 'portable-memo-finished',
            timeout: 300,
        );

        return 'php-replayed-memo';
    }

    #[Signal('finish')]
    public function finish(): void
    {
    }
}
