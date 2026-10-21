<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Client;
use DurableWorkflow\Telemetry\ExecutionTelemetry;
use DurableWorkflow\Tests\Support\FakeTransport;
use OpenTelemetry\API\Instrumentation\Configurator;
use OpenTelemetry\API\Trace\Propagation\TraceContextPropagator;
use OpenTelemetry\Context\Context;
use OpenTelemetry\SDK\Trace\SpanExporter\InMemoryExporter;
use OpenTelemetry\SDK\Trace\SpanProcessor\SimpleSpanProcessor;
use OpenTelemetry\SDK\Trace\TracerProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class ClientTelemetryTest extends TestCase
{
    public function testActualDiscoveryGatesCallerCaptureBeforeHttpAndPreservesBusinessInputAndRequestKeys(): void
    {
        $provider = new TracerProvider(new SimpleSpanProcessor(new InMemoryExporter()));
        $caller = $provider->getTracer('application')->spanBuilder('caller')->startSpan();
        $scope = Configurator::create()->withTracerProvider($provider)->withPropagator(TraceContextPropagator::getInstance())
            ->storeInContext($caller->storeInContext(Context::getRoot()))->activate();
        $supported = true;
        $failDiscovery = false;
        $transport = new FakeTransport(handler: static function (string $method, string $uri) use ($provider, &$supported, &$failDiscovery): array {
            $http = $provider->getTracer('http-client')->spanBuilder('HTTP '.$method)->startSpan();
            $httpScope = $http->activate();
            try {
                if (str_ends_with($uri, '/cluster/info')) {
                    if ($failDiscovery) {
                        throw new RuntimeException('discovery unavailable');
                    }
                    return ['worker_protocol' => ['server_capabilities' => ['observability_context' => $supported ? [
                        'supported' => true, 'schema' => 'durable-workflow.trace-context', 'version' => 1,
                        'dispatch_schema' => 'durable-workflow.queue-trace', 'links_schema' => 'durable-workflow.trace-links',
                        'max_context_bytes' => 4096, 'max_links' => 32, 'max_links_bytes' => 4096,
                    ] : ['supported' => true, 'version' => 99]]]];
                }
                return ['workflow_id' => 'order', 'run_id' => 'run', 'workflow_type' => 'orders', 'status' => 'pending'];
            } finally {
                $httpScope->detach();
                $http->end();
            }
        });
        $client = (new Client('https://server.example', transport: $transport))->withTelemetry(new ExecutionTelemetry($provider));
        try {
            $input = [['observability_context' => ['business' => 'preserved']]];
            $client->startWorkflow('orders', 'order', 'orders', $input);
            self::assertCount(1, $transport->requests);
            self::assertArrayNotHasKey('observability_context', $transport->requests[0]['body']);
            $client->clusterInfo();
            self::assertTrue($client->observabilityContextAvailable());
            $client->startWorkflow('orders', 'order', 'orders', $input);
            $client->signalWorkflow('order', 'ready', $input, requestId: 'signal-original');
            $client->updateWorkflowReceipt('order', 'status', $input, requestId: 'update-original');
            $client->cancelWorkflow('order', 'requested');
            $commands = array_slice($transport->requests, 2);
            self::assertCount(4, $commands);
            foreach ($commands as $command) {
                $context = $command['body']['observability_context'];
                self::assertSame('durable-workflow.trace-context', $context['schema']);
                self::assertSame($caller->getContext()->getSpanId(), substr($context['carrier']['traceparent'], 36, 16));
                self::assertArrayNotHasKey('baggage', $context['carrier']);
            }
            self::assertSame($input, $client->payloadCodec()->decodeEnvelope($commands[0]['body']['input']));
            self::assertSame('signal-original', $commands[1]['body']['request_id']);
            self::assertSame('update-original', $commands[2]['body']['request_id']);
            $supported = false;
            $client->clusterInfo();
            self::assertFalse($client->observabilityContextAvailable());
            $client->startWorkflow('orders', 'order', 'orders');
            self::assertArrayNotHasKey('observability_context', $transport->requests[array_key_last($transport->requests)]['body']);
            $supported = true;
            $client->clusterInfo();
            $failDiscovery = true;
            try {
                $client->clusterInfo();
                self::fail('Explicit failed discovery returned success.');
            } catch (RuntimeException $failure) {
                self::assertSame('discovery unavailable', $failure->getMessage());
            }
            self::assertFalse($client->observabilityContextAvailable());
            $before = count($transport->requests);
            $client->startWorkflow('orders', 'order', 'orders');
            self::assertCount($before + 1, $transport->requests);
            self::assertArrayNotHasKey('observability_context', $transport->requests[array_key_last($transport->requests)]['body']);
        } finally {
            $scope->detach();
            $caller->end();
        }
    }
}
