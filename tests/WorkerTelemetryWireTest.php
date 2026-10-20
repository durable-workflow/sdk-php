<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Telemetry\ExecutionTelemetry;
use DurableWorkflow\Tests\Support\FakeTransport;
use DurableWorkflow\Worker;
use DurableWorkflow\Worker\WorkflowContext;
use OpenTelemetry\API\Baggage\Baggage;
use OpenTelemetry\SDK\Trace\SpanExporter\InMemoryExporter;
use OpenTelemetry\SDK\Trace\SpanProcessor\SimpleSpanProcessor;
use OpenTelemetry\SDK\Trace\TracerProvider;
use PHPUnit\Framework\TestCase;

final class WorkerTelemetryWireTest extends TestCase
{
    private const ORIGIN = '00-12345678901234567890123456789012-1234567890123456-01';
    private const DISPATCH = '00-abcdef78901234567890123456789012-abcdef7890123456-01';

    public function testPolledWorkflowRecoversParentAndSecondaryLinksAfterTheOriginalLeaseFence(): void
    {
        $exporter = new InMemoryExporter();
        $provider = new TracerProvider(new SimpleSpanProcessor($exporter));
        $telemetry = new ExecutionTelemetry($provider, baggageAllowlist: ['tenant']);
        $task = self::task();
        $task['observability_context'] = self::context(self::ORIGIN, ['baggage' => 'tenant=public,credential=private']);
        $task['dispatch_observability_context'] = ['schema' => 'durable-workflow.queue-trace', 'version' => 1,
            'task_id' => 'task', 'context' => self::context(self::DISPATCH)];
        $task['observability_links'] = ['schema' => 'durable-workflow.trace-links', 'version' => 1,
            'task_id' => 'task', 'run_id' => 'run', 'namespace' => 'tenant-a', 'links' => [
                ['kind' => 'command', 'command_id' => 'accepted-command', 'context' => self::context(self::DISPATCH)],
                ['kind' => 'child_outcome', 'workflow_link_id' => 'child-link', 'child_run_id' => 'child-run',
                    'child_task_id' => null, 'event_id' => 'child-terminal', 'sequence' => 7,
                    'event_type' => 'WorkflowCancelled', 'context' => self::context(self::DISPATCH)],
            ]];
        [$client, $transport] = self::client($task);
        $client->workerClusterInfo();
        $worker = (new Worker($client, 'orders', workerId: 'worker', telemetry: $telemetry))
            ->registerWorkflow('orders', static function (WorkflowContext $context): string {
                self::assertSame('run', ExecutionTelemetry::current()['workflow_run_id']);
                self::assertSame('public', Baggage::getCurrent()->getValue('tenant'));
                self::assertNull(Baggage::getCurrent()->getValue('credential'));

                return 'original-result';
            });
        self::assertTrue($worker->tick(0));
        $spans = $exporter->getSpans();
        self::assertCount(1, $spans);
        self::assertSame(substr(self::ORIGIN, 3, 32), $spans[0]->getContext()->getTraceId());
        self::assertSame(substr(self::ORIGIN, 36, 16), $spans[0]->getParentContext()->getSpanId());
        self::assertCount(3, $spans[0]->getLinks());
        foreach ($spans[0]->getLinks() as $link) {
            self::assertSame(substr(self::DISPATCH, 3, 32), $link->getSpanContext()->getTraceId());
        }
        $completions = array_values(array_filter($transport->requests,
            static fn (array $request): bool => str_ends_with($request['uri'], '/complete')));
        self::assertCount(1, $completions);
        self::assertSame('original-result', $client->payloadCodec()->decodeEnvelope($completions[0]['body']['commands'][0]['result']));
        self::assertSame([], ExecutionTelemetry::current());
    }

    public function testRefusedClaimNeverOpensARecoveredScopeOrInvokesTheWorkflow(): void
    {
        $exporter = new InMemoryExporter();
        $provider = new TracerProvider(new SimpleSpanProcessor($exporter));
        $task = self::task();
        $task['observability_context'] = self::context(self::ORIGIN);
        [$client] = self::client($task, renewed: false);
        $client->workerClusterInfo();
        $calls = 0;
        $worker = (new Worker($client, 'orders', workerId: 'worker', telemetry: new ExecutionTelemetry($provider)))
            ->registerWorkflow('orders', static function (WorkflowContext $context) use (&$calls): string {
                ++$calls;

                return 'must-not-run';
            });
        try {
            $worker->tick(0);
            self::fail('Original lease refusal was swallowed.');
        } catch (ServerException $failure) {
            self::assertSame('invalid_workflow_task_lease_response', $failure->reason);
        }
        self::assertSame(0, $calls);
        self::assertSame([], $exporter->getSpans());
        self::assertSame([], ExecutionTelemetry::current());
    }

    public function testMalformedObservationDoesNotReplaceTheBusinessOutcome(): void
    {
        $exporter = new InMemoryExporter();
        $provider = new TracerProvider(new SimpleSpanProcessor($exporter));
        $task = self::task();
        $task['observability_context'] = ['schema' => 'unknown', 'version' => 9, 'carrier' => ['traceparent' => 'private']];
        $task['dispatch_observability_context'] = ['schema' => 'durable-workflow.queue-trace', 'version' => 1,
            'task_id' => 'wrong-task', 'context' => self::context(self::DISPATCH)];
        [$client] = self::client($task);
        $client->workerClusterInfo();
        $worker = (new Worker($client, 'orders', workerId: 'worker', telemetry: new ExecutionTelemetry($provider)))
            ->registerWorkflow('orders', static fn (WorkflowContext $context): string => 'original-result');
        self::assertTrue($worker->tick(0));
        $span = $exporter->getSpans()[0];
        self::assertFalse($span->getParentContext()->isValid());
        self::assertSame([], $span->getLinks());
        self::assertSame('discarded', $span->getAttributes()->get('durable.observability_context'));
        self::assertSame('discarded', $span->getAttributes()->get('durable.dispatch_context'));
        self::assertStringNotContainsString('private', json_encode($span->getAttributes()->toArray(), JSON_THROW_ON_ERROR));
        self::assertSame([], ExecutionTelemetry::current());
    }

    /** @return array<string, mixed> */
    private static function task(): array
    {
        return ['task_id' => 'task', 'workflow_id' => 'workflow', 'run_id' => 'run', 'workflow_type' => 'orders',
            'workflow_task_attempt' => 1, 'lease_owner' => 'worker', 'payload_codec' => 'avro', 'history_events' => []];
    }

    /** @param array<string, string> $headers
     * @return array<string, mixed>
     */
    private static function context(string $parent, array $headers = []): array
    {
        return ['schema' => 'durable-workflow.trace-context', 'version' => 1,
            'carrier' => ['traceparent' => $parent] + $headers];
    }

    /** @param array<string, mixed> $task
     * @return array{Client, FakeTransport}
     */
    private static function client(array $task, bool $renewed = true): array
    {
        $transport = new FakeTransport(handler: static function (string $method, string $uri, array $headers, ?array $body) use ($task, $renewed): array {
            if (str_ends_with($uri, '/cluster/info')) {
                return ['worker_protocol' => ['server_capabilities' => ['observability_context' => [
                    'supported' => true, 'schema' => 'durable-workflow.trace-context', 'version' => 1,
                    'dispatch_schema' => 'durable-workflow.queue-trace', 'links_schema' => 'durable-workflow.trace-links',
                    'max_context_bytes' => 4096, 'max_links' => 32, 'max_links_bytes' => 4096,
                ]]]];
            }
            if (str_ends_with($uri, '/workflow-tasks/poll')) {
                return ['task' => $task, 'poll_status' => 'leased'];
            }
            if (str_ends_with($uri, '/activity-tasks/poll') || str_ends_with($uri, '/query-tasks/poll')) {
                return ['task' => null, 'poll_status' => 'empty'];
            }
            if (str_ends_with($uri, '/heartbeat')) {
                return ['renewed' => $renewed, 'task_id' => $task['task_id'], 'lease_owner' => $body['lease_owner'],
                    'workflow_task_attempt' => $body['workflow_task_attempt'], 'retryable' => false];
            }
            if (str_ends_with($uri, '/complete') || str_ends_with($uri, '/fail')) {
                return ['recorded' => true];
            }
            throw new \LogicException('Unexpected request '.$method.' '.$uri);
        });

        return [new Client('https://server.example', namespace: 'tenant-a', transport: $transport), $transport];
    }
}
