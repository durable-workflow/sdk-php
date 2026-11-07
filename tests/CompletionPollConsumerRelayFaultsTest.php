<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Qualification\CompletionPollRelayFaults;
use DurableWorkflow\Qualification\CompletionPollTrace;
use PHPUnit\Framework\TestCase;

require_once __DIR__.'/../scripts/conformance/Support/CompletionPollRelayFaults.php';

final class CompletionPollConsumerRelayFaultsTest extends TestCase
{
    private string $file;

    protected function setUp(): void
    {
        $this->file = tempnam(sys_get_temp_dir(), 'completion-fault-');
        file_put_contents($this->file, json_encode(['events' => [], 'scenario' => 'carrier-loss-activity',
            'prefix' => 'fixture', 'deadline_at' => hrtime(true) / 1e9 + 10], JSON_THROW_ON_ERROR));
    }

    protected function tearDown(): void { unlink($this->file); }

    public function testOnlyOneActualSuccessfulLeasedAnswerIsWithheld(): void
    {
        $trace = new CompletionPollTrace($this->file);
        $answer = ['activity_poll' => ['leased' => true, 'answer' => ['task' => ['task_id' => 'fixture-task']]]];
        $path = '/api/worker/workflow-tasks/parent/complete';
        self::assertFalse(CompletionPollRelayFaults::after($trace, $path, $answer, 500, 'failed'));
        self::assertTrue(CompletionPollRelayFaults::after($trace, $path, $answer, 200, 'selected'));
        self::assertFalse(CompletionPollRelayFaults::after($trace, $path, $answer, 200, 'retry'));
        self::assertSame('selected', $trace->state()['fault_request']);
    }

    public function testAnEmptyOrOmittedCarrierCannotSatisfyTheLossControl(): void
    {
        $trace = new CompletionPollTrace($this->file);
        foreach ([[], ['activity_poll' => ['leased' => false]], ['activity_poll' => ['leased' => true]]] as $answer) {
            self::assertFalse(CompletionPollRelayFaults::after($trace, '/api/worker/workflow-tasks/parent/complete', $answer, 200, 'not-leased'));
        }
        self::assertArrayNotHasKey('fault_request', $trace->state());
    }

    public function testWrongCompletionKindDoesNotConsumeASelectedWorkflowLoss(): void
    {
        $trace = new CompletionPollTrace($this->file);
        $trace->change(static function (array &$state): void { $state['scenario'] = 'carrier-loss-workflow'; });
        $answer = ['workflow_poll' => ['leased' => true, 'answer' => ['task' => ['task_id' => 'fixture-task']]]];
        self::assertFalse(CompletionPollRelayFaults::after($trace, '/api/worker/workflow-tasks/parent/complete', $answer, 200, 'wrong-kind'));
        self::assertTrue(CompletionPollRelayFaults::after($trace, '/api/worker/activity-tasks/parent/complete', $answer, 200, 'right-kind'));
    }

    public function testRawRegistrationTokensAndHistoryDoNotEnterThePublicTrace(): void
    {
        $token = str_repeat('a', 32);
        $answer = CompletionPollTrace::answer(['registration_token' => $token, 'task' => ['task_id' => 'fixture-task',
            'lease_owner' => 'worker', 'lease_expires_at' => '2026-11-07T08:00:30.000000Z', 'history_events' => [['payload' => 'synthetic-payload-canary']]]]);
        self::assertSame(hash('sha256', $token), $answer['registration_sha256']);
        self::assertArrayNotHasKey('registration_token', $answer);
        self::assertStringNotContainsString($token, json_encode($answer, JSON_THROW_ON_ERROR));
        self::assertStringNotContainsString('synthetic-payload-canary', json_encode($answer, JSON_THROW_ON_ERROR));
        self::assertMatchesRegularExpression('/\A[a-f0-9]{64}\z/', $answer['task']['identity_sha256']);
    }
}
