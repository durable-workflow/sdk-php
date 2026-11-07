<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Qualification\CompletionPollWorkflowRefence;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

require_once __DIR__.'/../scripts/conformance/Support/CompletionPollWorkflowRefence.php';

final class CompletionPollConsumerWorkflowRefenceTest extends TestCase
{
    public function testOriginalIdRecoveryIsANewAttemptWithUnchangedHistory(): void
    {
        $original = self::task();
        CompletionPollWorkflowRefence::recovered($original, array_replace($original, ['workflow_task_attempt' => 2]));
        $this->addToAssertionCount(1);
    }

    #[DataProvider('changedRecovery')]
    public function testRecoveryCannotReuseTheAttemptOrChangeHistory(string $field, mixed $value): void
    {
        $this->expectExceptionMessage('completion_refence_recovery_changed');
        CompletionPollWorkflowRefence::recovered(self::task(), array_replace(self::task(), [$field => $value]));
    }

    public static function changedRecovery(): iterable
    {
        yield ['workflow_task_attempt', 1];
        yield ['task_id', 'different-task'];
        yield ['history_events', [['type' => 'invented_repair']]];
    }

    public function testRefusalPrecedenceIsBoundToTheOriginalTaskAndCurrentAttempt(): void
    {
        $before = new ServerException('fixture', 409, 'task_not_leased',
            ['task_id' => 'task', 'workflow_task_attempt' => 1, 'task_status' => 'ready', 'recorded' => false]);
        CompletionPollWorkflowRefence::refusal($before, self::task(), 'task_not_leased', null, true);
        $after = new ServerException('fixture', 409, 'workflow_task_attempt_mismatch',
            ['task_id' => 'task', 'workflow_task_attempt' => 1, 'current_attempt' => 2]);
        CompletionPollWorkflowRefence::refusal($after, self::task(), 'workflow_task_attempt_mismatch', 2, true);
        $this->addToAssertionCount(2);
        $this->expectExceptionMessage('completion_refence_wrong_refusal');
        CompletionPollWorkflowRefence::refusal($before, self::task(), 'workflow_task_attempt_mismatch', 2, true);
    }

    public function testAnUnrelatedFailureCannotStandInForRevokedAuthority(): void
    {
        $this->expectExceptionMessage('completion_refence_wrong_refusal');
        CompletionPollWorkflowRefence::refusal(new ServerException('fixture', 503, 'backend_unavailable'), self::task(), 'task_not_leased', null, false);
    }

    private static function task(): array
    {
        return ['task_id' => 'task', 'workflow_id' => 'workflow', 'run_id' => 'run', 'workflow_type' => 'qualification.wire',
            'lease_owner' => 'worker', 'workflow_task_attempt' => 1, 'lease_expires_at' => '2026-11-07T08:00:30.000000Z',
            'input' => ['synthetic' => true], 'history_events' => [['type' => 'activity_completed']]];
    }
}
