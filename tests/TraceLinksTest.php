<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Telemetry\TraceLinks;
use PHPUnit\Framework\TestCase;

final class TraceLinksTest extends TestCase
{
    public function testCanonicalOutcomeFieldsAndNullableChildProducerRemainSeparate(): void
    {
        $value = self::envelope();
        $entries = TraceLinks::normalize($value, 'task', 'run', 'tenant', $diagnostic);
        self::assertNull($diagnostic);
        self::assertSame($value['links'], $entries);
        self::assertSame('activity', $entries[0]['activity_execution_id']);
        self::assertNull($entries[1]['child_task_id']);
        self::assertArrayNotHasKey('activity_id', $entries[0]);
        unset($value['links'][1]['child_task_id']);
        self::assertSame($value['links'], TraceLinks::normalize($value, 'task', 'run', 'tenant'));
    }

    public function testWrongBindingAliasBooleanSequenceAndBaggageCannotCreateLinks(): void
    {
        $original = self::envelope();
        foreach (['task_id', 'run_id', 'namespace'] as $key) {
            $value = $original;
            $value[$key] = 'wrong';
            self::assertSame([], TraceLinks::normalize($value, 'task', 'run', 'tenant', $diagnostic));
            self::assertSame('discarded', $diagnostic);
        }
        foreach ([true, false, 0, -1, 1.0, '1', PHP_INT_MAX + 1] as $sequence) {
            $value = $original;
            $value['links'][0]['sequence'] = $sequence;
            self::assertSame([], TraceLinks::normalize($value, 'task', 'run', 'tenant', $diagnostic));
            self::assertSame('discarded', $diagnostic);
        }
        $value = $original;
        $value['links'][0]['activity_id'] = $value['links'][0]['activity_execution_id'];
        self::assertSame([], TraceLinks::normalize($value, 'task', 'run', 'tenant'));
        $value = $original;
        $value['links'][0]['context']['carrier']['baggage'] = 'tenant=private';
        self::assertSame([], TraceLinks::normalize($value, 'task', 'run', 'tenant', $diagnostic));
        self::assertSame('discarded', $diagnostic);
        self::assertSame(self::envelope(), $original);
    }

    public function testEntryAndJsonBoundsRejectWithoutInventingMissingSources(): void
    {
        $value = self::envelope();
        $value['links'] = array_fill(0, 33, $value['links'][0]);
        self::assertSame([], TraceLinks::normalize($value, 'task', 'run', 'tenant', $diagnostic));
        self::assertSame('discarded', $diagnostic);
        $value = self::envelope();
        $value['links'] = array_fill(0, 16, $value['links'][0]);
        self::assertGreaterThan(4096, strlen(json_encode($value, JSON_THROW_ON_ERROR)));
        self::assertSame([], TraceLinks::normalize($value, 'task', 'run', 'tenant'));
        foreach (['task_id', 'event_id', 'sequence', 'event_type'] as $key) {
            $value = self::envelope();
            unset($value['links'][0][$key]);
            self::assertSame([], TraceLinks::normalize($value, 'task', 'run', 'tenant'));
        }
        $value = self::envelope();
        $value['links'][0]['sequence'] = PHP_INT_MAX;
        self::assertNotSame([], TraceLinks::normalize($value, 'task', 'run', 'tenant'));
    }

    /** @return array<string, mixed> */
    private static function envelope(): array
    {
        $context = ['schema' => 'durable-workflow.trace-context', 'version' => 1,
            'carrier' => ['traceparent' => '00-12345678901234567890123456789012-1234567890123456-01']];

        return ['schema' => 'durable-workflow.trace-links', 'version' => 1, 'task_id' => 'task', 'run_id' => 'run',
            'namespace' => 'tenant', 'links' => [
                ['kind' => 'task_outcome', 'task_id' => 'producer', 'event_id' => 'event', 'sequence' => 3,
                    'event_type' => 'ActivityCompleted', 'activity_execution_id' => 'activity',
                    'activity_attempt_id' => 'attempt', 'context' => $context],
                ['kind' => 'child_outcome', 'workflow_link_id' => 'link', 'child_run_id' => 'child',
                    'child_task_id' => null, 'event_id' => 'terminal', 'sequence' => 7,
                    'event_type' => 'WorkflowCancelled', 'context' => $context],
            ]];
    }
}
