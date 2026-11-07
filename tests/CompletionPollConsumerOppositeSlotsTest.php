<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Qualification\CompletionPollOppositeSlots;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;

require_once __DIR__.'/../scripts/conformance/Support/CompletionPollOppositeSlots.php';

final class CompletionPollConsumerOppositeSlotsTest extends TestCase
{
    private const WORKER = 'opposite-0123456789abcdef-worker';

    public function testAValidCompletionWithOmittedOppositeCarrierIsTheRequiredOutcome(): void
    {
        CompletionPollOppositeSlots::omitted(['recorded' => true], 'workflow_poll');
        $this->addToAssertionCount(1);
    }

    #[DataProvider('notDeclined')]
    public function testAnEmptyClaimOrInvalidParentIsNotAnOppositeSlotDecline(array $answer): void
    {
        $this->expectExceptionMessage('completion_opposite_carrier_not_declined');
        CompletionPollOppositeSlots::omitted($answer, 'workflow_poll');
    }

    public static function notDeclined(): iterable
    {
        yield 'empty probe' => [['recorded' => true, 'workflow_poll' => ['leased' => false]]];
        yield 'extra lease' => [['recorded' => true, 'workflow_poll' => ['leased' => true]]];
        yield 'parent not recorded' => [['recorded' => false]];
    }

    /**
     * The ordinary activity-poll answer Native 3.2.0 returned in phase D3 row 17.
     * Native's ActivityTaskClaim task has no task_kind.
     */
    public function testTheObservedNativeActivityPollAnswerIsAnActivityClaim(): void
    {
        $answer = self::activityAnswer();
        self::assertSame($answer['task'], self::claim($answer, 'activity'));
    }

    public function testTheObservedNativeWorkflowPollAnswerIsAWorkflowClaim(): void
    {
        $answer = self::workflowAnswer();
        self::assertSame($answer['task'], self::claim($answer, 'workflow'));
    }

    /** The task object allows additional properties, including a matching kind. */
    public function testAnActivityClaimToleratesAdditionalFieldsAndAnActivityKind(): void
    {
        $answer = self::activityAnswer(['task_kind' => 'activity', 'ready_age_ms' => 4]);
        self::assertSame($answer['task'], self::claim($answer, 'activity'));
    }

    #[DataProvider('notActivityClaims')]
    public function testAnInvalidActivityPollAnswerIsNotAnActivityClaim(array $answer): void
    {
        $this->expectExceptionMessage('completion_opposite_claim_required');
        self::claim($answer, 'activity');
    }

    public static function notActivityClaims(): iterable
    {
        yield 'workflow task on the activity route' => [self::workflowAnswer()];
        yield 'workflow kind' => [self::activityAnswer(['task_kind' => 'workflow'])];
        yield 'update validation kind' => [self::activityAnswer(['task_kind' => 'update_validation'])];
        yield 'empty poll' => [['poll_status' => 'empty', 'task' => null]];
        yield 'not leased' => [array_replace(self::activityAnswer(), ['poll_status' => 'draining'])];
        yield 'other owner' => [self::activityAnswer(['lease_owner' => 'other-worker'])];
        yield 'missing owner' => [self::activityAnswer(['lease_owner' => null])];
        yield 'missing task id' => [self::activityAnswer(['task_id' => null])];
        yield 'empty task id' => [self::activityAnswer(['task_id' => ''])];
        yield 'missing lease expiry' => [self::activityAnswer(['lease_expires_at' => null])];
        yield 'missing activity attempt' => [self::activityAnswer(['activity_attempt_id' => null])];
        yield 'empty activity attempt' => [self::activityAnswer(['activity_attempt_id' => ''])];
        yield 'missing execution' => [self::activityAnswer(['activity_execution_id' => null])];
        yield 'missing activity type' => [self::activityAnswer(['activity_type' => null])];
        yield 'missing payload codec' => [self::activityAnswer(['payload_codec' => null])];
        yield 'missing attempt number' => [self::activityAnswer(['attempt_number' => null])];
        yield 'zero attempt number' => [self::activityAnswer(['attempt_number' => 0])];
        yield 'string attempt number' => [self::activityAnswer(['attempt_number' => '1'])];
    }

    #[DataProvider('notWorkflowClaims')]
    public function testAnInvalidWorkflowPollAnswerIsNotAWorkflowClaim(array $answer): void
    {
        $this->expectExceptionMessage('completion_opposite_claim_required');
        self::claim($answer, 'workflow');
    }

    public static function notWorkflowClaims(): iterable
    {
        yield 'activity task on the workflow route' => [self::activityAnswer()];
        yield 'missing kind' => [self::workflowAnswer(['task_kind' => null])];
        yield 'update validation kind' => [self::workflowAnswer(['task_kind' => 'update_validation'])];
        yield 'other owner' => [self::workflowAnswer(['lease_owner' => 'other-worker'])];
        yield 'missing lease expiry' => [self::workflowAnswer(['lease_expires_at' => null])];
        yield 'zero attempt' => [self::workflowAnswer(['workflow_task_attempt' => 0])];
        yield 'missing attempt' => [self::workflowAnswer(['workflow_task_attempt' => null])];
    }

    public function testAnUnknownRouteIsNotAClaim(): void
    {
        $this->expectExceptionMessage('completion_opposite_claim_required');
        self::claim(self::activityAnswer(), 'query');
    }

    /** @param array<string, mixed> $answer
     * @return array<string, mixed>
     */
    private static function claim(array $answer, string $kind): array
    {
        return (new ReflectionMethod(CompletionPollOppositeSlots::class, 'claim'))->invoke(null, $answer, self::WORKER, $kind);
    }

    /**
     * Row 17's traced fields with the prefix sanitized. The trace keeps task_kind
     * whenever present and recorded none. Fields the trace does not keep are the
     * values Native always sets for this workload.
     *
     * @param array<string, mixed> $task
     * @return array<string, mixed>
     */
    private static function activityAnswer(array $task = []): array
    {
        return ['poll_status' => 'leased', 'server_time' => '2026-11-07T23:02:36.736000Z', 'protocol_version' => '1.20',
            'task' => self::merge([
                'task_id' => '01M6W3ZRDAVRPW4FGDR5KM8RXG',
                'workflow_id' => 'opposite-0123456789abcdef-1',
                'run_id' => '01M6W3ZRAPVSXR3PT0CY2S60C1',
                'activity_execution_id' => '01M6W3ZRDAVRPW4FGDR5KM8RXH',
                'activity_attempt_id' => '01M6W3ZRDX74PC3V2TA00CH76W',
                'idempotency_key' => '01M6W3ZRDAVRPW4FGDR5KM8RXH',
                'attempt_number' => 1,
                'activity_type' => 'qualification.remote',
                'payload_codec' => 'avro',
                'task_queue' => 'opposite-0123456789abcdef-queue',
                'lease_owner' => self::WORKER,
                'lease_expires_at' => '2026-11-07T23:07:36.732000Z',
            ], $task)];
    }

    /** @param array<string, mixed> $task
     * @return array<string, mixed>
     */
    private static function workflowAnswer(array $task = []): array
    {
        return ['poll_status' => 'leased', 'server_time' => '2026-11-07T23:02:36.671000Z', 'protocol_version' => '1.20',
            'task' => self::merge([
                'task_kind' => 'workflow',
                'task_id' => '01M6W3ZRAQA4FJEZGDY7FX6YQN',
                'workflow_id' => 'opposite-0123456789abcdef-1',
                'run_id' => '01M6W3ZRAPVSXR3PT0CY2S60C1',
                'lease_owner' => self::WORKER,
                'lease_expires_at' => '2026-11-07T23:02:51.668000Z',
                'workflow_task_attempt' => 1,
            ], $task)];
    }

    /** A null override removes the field, as Native omits null task fields.
     * @param array<string, mixed> $base
     * @param array<string, mixed> $override
     * @return array<string, mixed>
     */
    private static function merge(array $base, array $override): array
    {
        return array_filter(array_replace($base, $override), static fn (mixed $value): bool => $value !== null);
    }
}
