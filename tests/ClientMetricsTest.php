<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Metrics\MetricsRecorder;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Tests\Support\RecordingMetricsRecorder;
use DurableWorkflow\Worker;
use PHPUnit\Framework\TestCase;

final class ClientMetricsTest extends TestCase
{
    public function testSharedContractBytesStayPinned(): void
    {
        self::assertSame('c456f4f434c601fa26f0caba07ef0fd29f548ca8f5fc29ba2f6430f01e32c0f0',
            hash_file('sha256', dirname(__DIR__).'/resources/protocol/sdk-metrics-v1.json'));
    }

    public function testActualRequestsUseTemplatesAndExcludeIdsAuthorityAndQueries(): void
    {
        $recorder = new RecordingMetricsRecorder();
        $transport = new FakeTransport([[], [], ['events' => []]]);
        $client = new Client('https://secret-host.invalid', namespace: 'secret-namespace',
            controlToken: 'secret-control-token', transport: $transport, metrics: $recorder);
        $client->describeWorkflow('private-workflow-one', 'private-run-one');
        $client->describeWorkflow('private-workflow-two', 'private-run-two');
        $client->workflowHistory('private-workflow-two', 'private-run-two', 10, 'secret-page-token');

        $pairs = $this->pairs($recorder, 'client_request');
        self::assertCount(3, $pairs);
        self::assertSame($pairs[0][0]['tags'], $pairs[1][0]['tags']);
        self::assertSame(['method' => 'GET', 'route' => '/workflows/{workflow_id}/runs/{run_id}',
            'plane' => 'control', 'status_code' => 'none', 'outcome' => 'ok'], $pairs[0][0]['tags']);
        self::assertSame('/workflows/{workflow_id}/runs/{run_id}/history', $pairs[2][0]['tags']['route']);
        $recorded = json_encode($recorder->events, JSON_THROW_ON_ERROR);
        foreach (['private-workflow', 'private-run', 'secret-namespace', 'secret-control-token', 'secret-host', 'secret-page-token'] as $secret) {
            self::assertStringNotContainsString($secret, $recorded);
        }
    }

    public function testTransientPollRetryRemainsOneObservedClientRequest(): void
    {
        $recorder = new RecordingMetricsRecorder();
        $transport = new FakeTransport([new TransportException('reset', transientConnectionFailure: true),
            ['task' => null, 'poll_status' => 'empty']]);
        $client = new Client('https://server.invalid', workerToken: 'secret-worker-token',
            transport: $transport, metrics: $recorder);
        self::assertNull($client->pollWorkflowTaskResponse('private-worker', 'queue', 0, 'private-poll')['task']);
        self::assertCount(2, $transport->requests);
        $pairs = $this->pairs($recorder, 'client_request');
        self::assertCount(1, $pairs);
        self::assertSame(['method' => 'POST', 'route' => '/worker/workflow-tasks/poll', 'plane' => 'worker',
            'status_code' => 'none', 'outcome' => 'ok'], $pairs[0][0]['tags']);
    }

    public function testAnHttpFailureRetainsItsOriginalExceptionAndMetricStatus(): void
    {
        $recorder = new RecordingMetricsRecorder();
        $transport = new FakeTransport([TransportException::fromResponse(429, ['reason' => 'busy'], '')]);
        $client = new Client('https://server.invalid', transport: $transport, metrics: $recorder);
        try {
            $client->health();
            self::fail('Expected the original HTTP refusal.');
        } catch (ServerException $error) {
            self::assertSame(429, $error->status);
            self::assertSame('busy', $error->reason);
        }
        $pairs = $this->pairs($recorder, 'client_request');
        self::assertCount(1, $pairs);
        self::assertSame('429', $pairs[0][0]['tags']['status_code']);
        self::assertSame('http_error', $pairs[0][0]['tags']['outcome']);
    }

    public function testExporterExceptionsCannotChangeSuccessOrFailure(): void
    {
        $recorder = new class implements MetricsRecorder {
            public function increment(string $name, float $value = 1.0, array $tags = []): void
            {
                throw new \RuntimeException('exporter secret');
            }

            public function record(string $name, float $value, array $tags = []): void
            {
                throw new \RuntimeException('exporter secret');
            }
        };
        $client = new Client('https://server.invalid', metrics: $recorder,
            transport: new FakeTransport([['healthy' => true], TransportException::fromResponse(503, ['reason' => 'unavailable'], '')]));
        self::assertSame(['healthy' => true], $client->health());
        try {
            $client->health();
            self::fail('Expected the original HTTP failure.');
        } catch (ServerException $error) {
            self::assertSame(503, $error->status);
            self::assertSame('unavailable', $error->reason);
        }
    }

    public function testWorkerRecorderInheritanceAndExplicitOverrideRemainSeparate(): void
    {
        $clientRecorder = new RecordingMetricsRecorder();
        $workerRecorder = new RecordingMetricsRecorder();
        $empty = ['task' => null, 'poll_status' => 'empty'];
        $client = new Client('https://server.invalid', transport: new FakeTransport(array_fill(0, 6, $empty)),
            metrics: $clientRecorder);
        self::assertFalse((new Worker($client, 'bounded-queue'))->tick(0));
        self::assertCount(3, $this->pairs($clientRecorder, 'client_request'));
        self::assertCount(3, $this->pairs($clientRecorder, 'worker_poll'));

        $clientRecorder->events = [];
        self::assertFalse((new Worker($client, 'bounded-queue', metrics: $workerRecorder))->tick(0));
        self::assertCount(3, $this->pairs($clientRecorder, 'client_request'));
        self::assertCount(0, $this->pairs($clientRecorder, 'worker_poll'));
        self::assertCount(0, $this->pairs($workerRecorder, 'client_request'));
        $polls = $this->pairs($workerRecorder, 'worker_poll');
        self::assertCount(3, $polls);
        self::assertSame(['workflow', 'activity', 'query'], array_column(array_map(static fn (array $pair): array => $pair[0]['tags'], $polls), 'task_kind'));
        foreach ($polls as $pair) {
            self::assertSame('bounded-queue', $pair[0]['tags']['task_queue']);
            self::assertSame('empty', $pair[0]['tags']['outcome']);
        }
    }

    /** @return list<array{array<string, mixed>, array<string, mixed>}> */
    private function pairs(RecordingMetricsRecorder $recorder, string $scope): array
    {
        $contract = json_decode((string) file_get_contents(dirname(__DIR__).'/resources/protocol/sdk-metrics-v1.json'), true, flags: JSON_THROW_ON_ERROR);
        $instruments = array_values(array_filter($contract['metrics'], static fn (array $metric): bool => $metric['scope'] === $scope));
        $events = array_values(array_filter($recorder->events, static fn (array $event): bool => in_array($event['name'], array_column($instruments, 'name'), true)));
        self::assertSame(0, count($events) % 2);
        $pairs = [];
        for ($index = 0; $index < count($events); $index += 2) {
            $pair = [$events[$index], $events[$index + 1]];
            foreach ($pair as $offset => $event) {
                self::assertSame($instruments[$offset]['name'], $event['name']);
                self::assertSame($instruments[$offset]['recorder_method'], $event['method']);
                self::assertEqualsCanonicalizing($instruments[$offset]['tags'], array_keys($event['tags']));
                foreach ($event['tags'] as $value) {
                    self::assertIsString($value);
                }
            }
            self::assertSame(1.0, $pair[0]['value']);
            self::assertGreaterThanOrEqual(0.0, $pair[1]['value']);
            self::assertSame($pair[0]['tags'], $pair[1]['tags']);
            $pairs[] = $pair;
        }

        return $pairs;
    }
}
