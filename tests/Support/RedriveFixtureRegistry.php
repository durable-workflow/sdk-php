<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests\Support;

/**
 * Every replay-regression fixture that names redrive, carries a reuse key or
 * opens with a redrive WorkflowStarted, and what it is evidence of.
 *
 * The corpus is immutable, so fixtures that are not valid redrive successor
 * histories stay in it byte-identical and still replay. This registry is the
 * test-side record that they are not qualification evidence: each one must
 * keep failing RedriveSuccessorHistory with the violation named here, and its
 * named replacement must pass.
 */
final class RedriveFixtureRegistry
{
    /**
     * Valid Workflow 2.19 redrive successors, with the reused prefix each one
     * carries in the order FailedRunRedrivePlan::successorEvents() writes it.
     *
     * @var array<string, list<string>>
     */
    public const SUCCESSORS = [
        'redrive-reused-upserts-clock' => ['MemoUpserted', 'SearchAttributesUpserted'],
        'redrive-successor-reused-upserts-cancellation-clock' => ['MemoUpserted', 'SearchAttributesUpserted'],
        'redrive-reused-signal-wait-clock' => ['SignalWaitOpened', 'SignalReceived', 'SignalApplied'],
        'redrive-reused-search-attributes-changed' => ['MemoUpserted', 'SearchAttributesUpserted'],
        'redrive-reused-search-attributes-missing' => ['MemoUpserted', 'SearchAttributesUpserted'],
        'redrive-reused-search-attribute-type-changed' => ['MemoUpserted', 'SearchAttributesUpserted'],
        'redrive-successor-reused-markers' => ['SideEffectRecorded', 'VersionMarkerRecorded', 'ActivityCompleted'],
        'redrive-successor-reused-timer' => ['ActivityCompleted', 'TimerScheduled', 'TimerFired'],
        'redrive-successor-reused-cancellation-clock' => ['ActivityCompleted', 'TimerScheduled', 'TimerFired'],
    ];

    /**
     * Fixtures shipped in 3.8.3 that are not redrive successor histories. They
     * are not evidence for any redrive claim.
     *
     * @var array<string, array{prefix: list<string>, violation: string, sha256: string, replacement: string}>
     */
    public const NON_QUALIFYING = [
        // #242 (issue #240): no WorkflowStarted, and the cancellation is
        // requested and delivered before the reused prefix.
        'redrive-reused-cancellation-clock' => [
            'prefix' => ['ActivityCompleted', 'TimerScheduled', 'TimerFired'],
            'violation' => 'history must open with WorkflowStarted at sequence 1',
            'sha256' => '3eda135a17ec7111b7f2b15ddd6b7acfbabcbe2a6f29c1a07490c727c4dc06d6',
            'replacement' => 'redrive-successor-reused-cancellation-clock',
        ],
        // #241: an empty WorkflowStarted, with no redrive envelope.
        'redrive-reused-markers-clock' => [
            'prefix' => ['SideEffectRecorded', 'VersionMarkerRecorded'],
            'violation' => 'WorkflowStarted workflow_type must be a non-blank string',
            'sha256' => 'c138bae2849f7442fb824d765f08b1fcac655ef056951378ac7cbe9581387c81',
            'replacement' => 'redrive-successor-reused-markers',
        ],
        // #241: a partial WorkflowStarted (recovery_kind and resume only).
        'redrive-reused-timer-clock' => [
            'prefix' => ['ActivityCompleted', 'TimerScheduled', 'TimerFired'],
            'violation' => 'WorkflowStarted workflow_type must be a non-blank string',
            'sha256' => 'c353e90d99bea68e73cf4b22e720d81aae1e37e7a58a797d3eed3786e4ca5b9d',
            'replacement' => 'redrive-successor-reused-timer',
        ],
        // #250: Core's WorkflowStub::recordCancellationRequest() writes
        // command_type, workflow_instance_id and workflow_run_id on every
        // request, and CooperativeCancellationDelivery::record() copies the
        // request's context onto the delivery. This request lacks command_type
        // and workflow_instance_id, and its delivery has no context.
        'redrive-reused-upserts-cancellation-clock' => [
            'prefix' => ['MemoUpserted', 'SearchAttributesUpserted'],
            'violation' => 'the cancellation request must carry the successor workflow_instance_id and workflow_run_id',
            'sha256' => '4de4433d2c3ce2ed24ef0cb82ad5d633817c162a560ea17aecff1dbfcc41391c',
            'replacement' => 'redrive-successor-reused-upserts-cancellation-clock',
        ],
    ];

    /**
     * Failed source runs that redrive starts from. They are not successors
     * and must carry no reuse key or redrive envelope.
     *
     * @var list<string>
     */
    public const SOURCE_RUNS = [
        'uncaught-activity-redrive-boundary',
    ];

    public static function directory(): string
    {
        return dirname(__DIR__).'/fixtures/replay-regressions';
    }

    /**
     * Successor histories that replay exactly like a corpus fixture but add the
     * fields Core writes and replay ignores. The replay corpus would count them
     * as semantic duplicates, so they live outside it.
     */
    public static function successorShapeDirectory(): string
    {
        return dirname(__DIR__).'/fixtures/redrive-successors';
    }

    /** @return list<string> */
    public static function directories(): array
    {
        return [self::directory(), self::successorShapeDirectory()];
    }

    public static function path(string $name): string
    {
        $shape = self::successorShapeDirectory()."/{$name}.json";

        return is_file($shape) ? $shape : self::directory()."/{$name}.json";
    }

    /** @return array<string, mixed> */
    public static function fixture(string $name): array
    {
        $fixture = json_decode((string) file_get_contents(self::path($name)), true, flags: JSON_THROW_ON_ERROR);
        if (!is_array($fixture)) {
            throw new \RuntimeException("{$name} must be a JSON object.");
        }

        return $fixture;
    }
}
