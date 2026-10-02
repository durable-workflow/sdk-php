<?php

declare(strict_types=1);

namespace DurableWorkflow\Tests;

use DurableWorkflow\Worker\PreparedLocalActivityAttempt;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class PreparedLocalActivityAttemptTest extends TestCase
{
    public function test_admission_uses_backend_identity_and_keeps_the_original_claim(): void
    {
        $attempt = self::attempt();
        self::assertSame('backend-attempt', $attempt->attemptId);
        self::assertSame('sdk-nonce', $attempt->workerAttemptId);
        self::assertSame(4, $attempt->workflowTaskAttempt);
        self::assertSame(2, $attempt->attemptNumber);
        $attempt->validateControl(self::control(), true);
        $attempt->validateOutcome(self::outcome());
    }

    #[DataProvider('invalidPreparationProvider')]
    public function test_refused_or_malformed_admission_cannot_authorize_a_callback(array $changes): void
    {
        $this->expectException(InvalidArgumentException::class);
        self::attempt([...self::preparation(), ...$changes]);
    }

    public static function invalidPreparationProvider(): array
    {
        $cases = [];
        foreach ([
            'prepared' => false, 'duplicate' => 'false', 'reason' => 'stale_claim',
            'workflow_task_id' => 'other', 'workflow_task_attempt' => 5, 'lease_owner' => 'replacement',
            'worker_attempt_id' => 'other-nonce', 'attempt_number' => '2', 'activity_attempt_id' => '',
            'server_time' => 'tomorrow', 'lease_expires_at' => '2026-10-02T00:00:00Z',
            'start_to_close_deadline_at' => '2026-10-01T23:59:59Z',
            'schedule_to_close_deadline_at' => '2026-02-30T01:00:00Z',
        ] as $field => $value) {
            $cases[$field] = [[$field => $value]];
        }
        return $cases;
    }

    #[DataProvider('invalidControlProvider')]
    public function test_altered_control_cannot_keep_a_callback_alive(array $changes): void
    {
        $this->expectException(InvalidArgumentException::class);
        self::attempt()->validateControl([...self::control(), ...$changes], true);
    }

    public static function invalidControlProvider(): array
    {
        $cases = [];
        foreach ([
            'activity_attempt_id' => 'replacement-attempt', 'activity_execution_id' => 'other-execution',
            'workflow_task_attempt' => 5, 'workflow_task_id' => 'other-task', 'lease_owner' => 'replacement',
            'active' => 'true', 'stop_required' => true, 'renewed' => false, 'reason' => 'stale_claim',
            'start_to_close_deadline_at' => '2026-10-02T00:01:30Z',
            'schedule_to_close_deadline_at' => null, 'heartbeat_deadline_at' => '2026-10-02T00:00:45Z',
            'workflow_lease_expires_at' => '2026-10-02T00:00:00Z',
        ] as $field => $value) {
            $cases[$field] = [[$field => $value]];
        }
        return $cases;
    }

    public function test_lease_renewal_does_not_extend_execution_deadlines(): void
    {
        $attempt = self::attempt();
        $attempt->validateControl([...self::control(), 'server_time' => '2026-10-02T00:00:05Z',
            'lease_expires_at' => '2026-10-02T00:00:15Z', 'workflow_lease_expires_at' => '2026-10-02T00:00:15Z',
        ], true);
        self::expectNotToPerformAssertions();
    }

    public function test_a_fenced_attempt_can_report_stop_without_a_live_lease(): void
    {
        self::attempt()->validateControl([...self::control(), 'active' => false, 'renewed' => false,
            'stop_required' => true, 'reason' => 'cancellation_requested', 'lease_expires_at' => null,
        ], true);
        self::expectNotToPerformAssertions();
    }

    public function test_durable_retry_receipt_releases_exactly_one_hosting_claim(): void
    {
        self::attempt()->validateOutcome([...self::outcome(), 'event_type' => 'ActivityRetryScheduled',
            'claim_released' => true, 'created_task_ids' => ['durable-retry-task'],
        ]);
        $this->expectException(InvalidArgumentException::class);
        self::attempt()->validateOutcome([...self::outcome(), 'event_type' => 'ActivityRetryScheduled',
            'claim_released' => false, 'created_task_ids' => [],
        ]);
    }

    public function test_a_receipt_from_another_attempt_or_run_cannot_resume_the_workflow(): void
    {
        foreach (['activity_attempt_id' => 'stale-attempt', 'workflow_run_id' => 'other-run',
            'worker_attempt_id' => 'other-nonce', 'workflow_task_attempt' => 3] as $key => $value) {
            try {
                self::attempt()->validateOutcome([...self::outcome(), $key => $value]);
                self::fail('Another activity authority was accepted.');
            } catch (InvalidArgumentException) {
                self::assertTrue(true);
            }
        }
    }

    private static function attempt(?array $response = null): PreparedLocalActivityAttempt
    {
        return PreparedLocalActivityAttempt::fromPreparation($response ?? self::preparation(), 'task', 'run', 'original', 4, 'sdk-nonce');
    }

    private static function preparation(): array
    {
        return ['prepared' => true, 'duplicate' => false, 'reason' => null,
            'workflow_task_id' => 'task', 'workflow_task_attempt' => 4, 'lease_owner' => 'original',
            'activity_execution_id' => 'backend-execution', 'activity_attempt_id' => 'backend-attempt',
            'worker_attempt_id' => 'sdk-nonce', 'attempt_number' => 2,
            'server_time' => '2026-10-02T00:00:00Z', 'lease_expires_at' => '2026-10-02T00:00:10Z',
            'start_to_close_deadline_at' => '2026-10-02T00:00:30Z',
            'schedule_to_close_deadline_at' => '2026-10-02T00:01:00Z', 'heartbeat_deadline_at' => null,
        ];
    }

    private static function control(): array
    {
        $control = self::preparation();
        unset($control['prepared'], $control['duplicate']);
        return [...$control, 'active' => true, 'renewed' => true, 'stop_required' => false,
            'workflow_lease_expires_at' => '2026-10-02T00:00:10Z'];
    }

    private static function outcome(): array
    {
        return ['recorded' => true, 'duplicate' => false, 'reason' => null,
            'workflow_run_id' => 'run', 'workflow_task_id' => 'task', 'workflow_task_attempt' => 4,
            'activity_execution_id' => 'backend-execution', 'activity_attempt_id' => 'backend-attempt',
            'worker_attempt_id' => 'sdk-nonce', 'event_type' => 'ActivityCompleted', 'event_id' => 'canonical-event',
            'recorded_at' => '2026-10-02T00:00:03Z', 'claim_released' => false, 'created_task_ids' => [],
        ];
    }
}
