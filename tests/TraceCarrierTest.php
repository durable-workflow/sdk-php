<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Telemetry\TraceCarrier;
use OpenTelemetry\API\Trace\Propagation\TraceContextPropagator;
use OpenTelemetry\API\Trace\Span;
use OpenTelemetry\Context\Context;
use PHPUnit\Framework\TestCase;

final class TraceCarrierTest extends TestCase
{
    private const PARENT = '00-12345678901234567890123456789012-1234567890123456-01';

    public function testExactNegotiationRejectsMissingUnknownAndCoercedCapabilityValues(): void
    {
        $capability = ['supported' => true, 'schema' => 'durable-workflow.trace-context', 'version' => 1,
            'dispatch_schema' => 'durable-workflow.queue-trace', 'links_schema' => 'durable-workflow.trace-links',
            'max_context_bytes' => 4096, 'max_links' => 32, 'max_links_bytes' => 4096];
        $info = ['worker_protocol' => ['server_capabilities' => ['observability_context' => $capability]]];
        self::assertTrue(TraceCarrier::supported($info));
        foreach (array_keys($capability) as $key) {
            foreach ([null, false, '1', 1.0, [], 'unknown'] as $invalid) {
                $candidate = $info;
                $candidate['worker_protocol']['server_capabilities']['observability_context'][$key] = $invalid;
                self::assertFalse(TraceCarrier::supported($candidate));
            }
        }
        self::assertFalse(TraceCarrier::supported(['version' => '999.0.0']));
        self::assertFalse(TraceCarrier::supported(['worker_protocol' => ['server_capabilities' => ['observability_context' => true]]]));
    }

    public function testNormalizationUsesW3cValidationAndNeverFallsBackToTheCaller(): void
    {
        $caller = TraceContextPropagator::getInstance()->extract(['traceparent' => self::PARENT], context: Context::getRoot());
        $scope = $caller->activate();
        try {
            foreach ([
                '00-00000000000000000000000000000000-1234567890123456-01',
                '00-12345678901234567890123456789012-0000000000000000-01',
                'ff-12345678901234567890123456789012-1234567890123456-01',
                'private-header', str_repeat('x', 257),
            ] as $invalid) {
                self::assertNull(TraceCarrier::normalize(self::envelope(['traceparent' => $invalid]), diagnostic: $diagnostic));
                self::assertSame('discarded', $diagnostic);
                self::assertSame(substr(self::PARENT, 3, 32), Span::getCurrent()->getContext()->getTraceId());
            }
            self::assertSame(self::envelope(), TraceCarrier::normalize(self::envelope(), diagnostic: $diagnostic));
            self::assertNull($diagnostic);
        } finally {
            $scope->detach();
        }
    }

    public function testBaggageDefaultsEmptyAndAllowlistRetainsAtMost32Entries(): void
    {
        $value = self::envelope(['baggage' => 'tenant=public,credential=private']);
        self::assertSame(self::envelope(), TraceCarrier::normalize($value, diagnostic: $diagnostic));
        self::assertSame('filtered', $diagnostic);
        self::assertSame(self::envelope(['baggage' => 'tenant=public']), TraceCarrier::normalize($value, ['tenant'], $diagnostic));
        self::assertSame('filtered', $diagnostic);
        self::assertSame('tenant=public,credential=private', $value['carrier']['baggage']);
        $keys = array_map(static fn (int $number): string => 'k'.$number, range(1, 33));
        $members = implode(',', array_map(static fn (string $key): string => $key.'=v', $keys));
        self::assertNull(TraceCarrier::normalize(self::envelope(['baggage' => $members]), $keys, $diagnostic));
        self::assertSame('discarded', $diagnostic);
        self::assertNotNull(TraceCarrier::normalize(self::envelope(['baggage' => substr($members, 0, (int) strrpos($members, ','))]), $keys));
    }

    public function testByteBoundsAndUnknownFieldsAreDiscardedWithoutChangingInput(): void
    {
        foreach (['traceparent' => 257, 'tracestate' => 513, 'baggage' => 2049] as $key => $length) {
            $value = self::envelope([$key => str_repeat('x', $length)]);
            self::assertNull(TraceCarrier::normalize($value, diagnostic: $diagnostic));
            self::assertSame('discarded', $diagnostic);
            self::assertSame(str_repeat('x', $length), $value['carrier'][$key]);
        }
        foreach ([null, [], true, 'carrier', self::envelope() + ['unknown' => 'private']] as $invalid) {
            self::assertNull(TraceCarrier::normalize($invalid, diagnostic: $diagnostic));
            self::assertSame($invalid === null ? null : 'discarded', $diagnostic);
        }
        self::assertNull(TraceCarrier::normalize(self::envelope(['authorization' => 'private']), diagnostic: $diagnostic));
        self::assertSame('discarded', $diagnostic);
    }

    public function testDispatchRequiresExactTaskBindingAndForbidsBaggage(): void
    {
        $value = ['schema' => 'durable-workflow.queue-trace', 'version' => 1, 'task_id' => 'task', 'context' => self::envelope()];
        self::assertSame(self::envelope(), TraceCarrier::dispatch($value, 'task', $diagnostic));
        self::assertNull($diagnostic);
        self::assertNull(TraceCarrier::dispatch($value, 'other', $diagnostic));
        self::assertSame('discarded', $diagnostic);
        $value['context'] = self::envelope(['baggage' => 'tenant=public']);
        self::assertNull(TraceCarrier::dispatch($value, 'task', $diagnostic));
        self::assertSame('discarded', $diagnostic);
        foreach (['', str_repeat('x', 513), "task\n", "task\u{0085}", "\xff"] as $invalid) {
            self::assertFalse(TraceCarrier::identity($invalid));
        }
        self::assertTrue(TraceCarrier::identity(str_repeat('x', 512)));
        self::assertTrue(TraceCarrier::identity('任务'));
    }

    /** @param array<string, string> $headers
     *  @return array{schema: string, version: int, carrier: array<string, string>}
     */
    private static function envelope(array $headers = []): array
    {
        return ['schema' => 'durable-workflow.trace-context', 'version' => 1,
            'carrier' => array_replace(['traceparent' => self::PARENT], $headers)];
    }
}
