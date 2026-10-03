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

    public function test_only_a_canonical_application_heartbeat_receipt_advances_its_deadline(): void
    {
        $attempt = self::heartbeatAttempt();
        $heartbeat = self::heartbeat();
        $attempt->validateHeartbeat($heartbeat);
        $control = [...$heartbeat, 'heartbeat_recorded' => false, 'heartbeat_history_event_id' => null, 'renewed' => true];
        $attempt->validateControl($control, true);
        self::assertSame('2026-10-02T00:00:30Z', $control['start_to_close_deadline_at']);
        self::assertSame('2026-10-02T00:01:00Z', $control['schedule_to_close_deadline_at']);
        $this->expectException(InvalidArgumentException::class);
        $attempt->validateControl([...$control, 'heartbeat_deadline_at' => '2026-10-02T00:00:10Z'], true);
    }

    #[DataProvider('invalidHeartbeatProvider')]
    public function test_invalid_heartbeat_cannot_advance_authority_or_change_fixed_budgets(array $changes): void
    {
        $attempt = self::heartbeatAttempt();
        try {
            $attempt->validateHeartbeat([...self::heartbeat(), ...$changes]);
            self::fail('An invalid heartbeat acknowledgment advanced callback authority.');
        } catch (InvalidArgumentException) {
            // The original deadline remains valid after rejecting a partially valid acknowledgment.
            $attempt->validateControl([...self::control(), 'heartbeat_deadline_at' => '2026-10-02T00:00:08Z'], true);
            self::addToAssertionCount(1);
        }
    }

    public static function invalidHeartbeatProvider(): array
    {
        $cases = [];
        foreach ([
            'heartbeat_recorded' => false, 'heartbeat_history_event_id' => '', 'renewed' => true,
            'activity_attempt_id' => 'replacement', 'start_to_close_deadline_at' => '2026-10-02T00:00:31Z',
            'schedule_to_close_deadline_at' => '2026-10-02T00:02:00Z',
            'heartbeat_deadline_at' => '2026-10-02T00:00:11Z', 'workflow_lease_expires_at' => '2026-10-02T00:00:00Z',
            'cancellation_cleanup' => self::cleanup(),
        ] as $field => $value) {
            $cases[$field] = [[$field => $value]];
        }
        $cases['backwards deadline'] = [['heartbeat_deadline_at' => '2026-10-02T00:00:07Z']];
        $cases['missing deadline'] = [['heartbeat_deadline_at' => null]];
        $cases['elapsed original heartbeat deadline'] = [['server_time' => '2026-10-02T00:00:08Z']];
        return $cases;
    }

    public function test_cleanup_admission_and_control_preserve_canonical_root_budget(): void
    {
        $cleanup = self::cleanup();
        $response = [...self::preparation(), 'cancellation_cleanup' => array_reverse($cleanup, true),
            'schedule_to_close_deadline_at' => $cleanup['cleanup_deadline_at'],
            'heartbeat_deadline_at' => $cleanup['cleanup_deadline_at']];
        $attempt = PreparedLocalActivityAttempt::fromPreparation($response, 'task', 'run', 'original', 4, 'sdk-nonce', expectedCleanup: $cleanup);
        $attempt->validateControl([...$response, 'active' => true, 'renewed' => true, 'stop_required' => false,
            'workflow_lease_expires_at' => '2026-10-02T00:00:10Z'], true);
        self::addToAssertionCount(1);
        $this->expectException(InvalidArgumentException::class);
        PreparedLocalActivityAttempt::fromPreparation([...$response, 'schedule_to_close_deadline_at' => '2026-10-02T00:00:31Z'],
            'task', 'run', 'original', 4, 'sdk-nonce', expectedCleanup: $cleanup);
    }

    #[DataProvider('invalidCleanupProvider')]
    public function test_changed_cleanup_identity_or_budget_is_refused(array $changes): void
    {
        $cleanup = self::cleanup();
        $response = [...self::preparation(), 'cancellation_cleanup' => [...$cleanup, ...$changes],
            'schedule_to_close_deadline_at' => $cleanup['cleanup_deadline_at'], 'heartbeat_deadline_at' => $cleanup['cleanup_deadline_at']];
        $this->expectException(InvalidArgumentException::class);
        PreparedLocalActivityAttempt::fromPreparation($response, 'task', 'run', 'original', 4, 'sdk-nonce', expectedCleanup: $cleanup);
    }

    public static function invalidCleanupProvider(): array
    {
        return ['local request' => [['request_id' => 'other']], 'root request' => [['root_request_id' => 'other']],
            'delivery' => [['delivery_history_event_id' => 'other']], 'renewed budget' => [['cleanup_deadline_at' => '2026-10-02T00:00:31Z']],
            'extra authority' => [['lease_owner' => 'other']]];
    }

    public function test_cleanup_application_heartbeat_cannot_extend_the_original_root_deadline(): void
    {
        $cleanup = self::cleanup();
        $response = [...self::preparation(), 'cancellation_cleanup' => $cleanup,
            'schedule_to_close_deadline_at' => $cleanup['cleanup_deadline_at'], 'heartbeat_deadline_at' => '2026-10-02T00:00:08Z'];
        $attempt = PreparedLocalActivityAttempt::fromPreparation($response, 'task', 'run', 'original', 4, 'sdk-nonce', 8, $cleanup);
        $heartbeat = [...self::heartbeat(),
            'lease_expires_at' => '2026-10-02T00:00:30Z', 'workflow_lease_expires_at' => '2026-10-02T00:00:30Z',
            'schedule_to_close_deadline_at' => $cleanup['cleanup_deadline_at'], 'cancellation_cleanup' => $cleanup];
        foreach (['07' => '15', '14' => '22', '21' => '29'] as $at => $until) {
            $attempt->validateHeartbeat([...$heartbeat, 'server_time' => '2026-10-02T00:00:'.$at.'Z',
                'heartbeat_deadline_at' => '2026-10-02T00:00:'.$until.'Z']);
        }
        $heartbeat = [...$heartbeat, 'server_time' => '2026-10-02T00:00:27Z',
            'heartbeat_deadline_at' => $cleanup['cleanup_deadline_at']];
        $attempt->validateHeartbeat($heartbeat);
        self::addToAssertionCount(1);
        $this->expectException(InvalidArgumentException::class);
        $attempt->validateHeartbeat([...$heartbeat, 'heartbeat_deadline_at' => '2026-10-02T00:00:31Z']);
    }

    public function test_readonly_supervisor_observation_cannot_renew_a_lease(): void
    {
        $this->expectException(InvalidArgumentException::class);
        self::attempt()->validateControl(self::control(), false);
    }

    public function test_stopped_application_heartbeat_cannot_revive_an_elapsed_attempt(): void
    {
        self::heartbeatAttempt()->validateHeartbeat([...self::control(), 'active' => false, 'renewed' => false,
            'stop_required' => true, 'reason' => 'local_activity_deadline_expired',
            'server_time' => '2026-10-02T00:00:09Z', 'heartbeat_deadline_at' => '2026-10-02T00:00:08Z',
            'heartbeat_recorded' => false, 'heartbeat_history_event_id' => null]);
        self::expectNotToPerformAssertions();
    }

    private static function heartbeatAttempt(): PreparedLocalActivityAttempt
    {
        return PreparedLocalActivityAttempt::fromPreparation([...self::preparation(),
            'heartbeat_deadline_at' => '2026-10-02T00:00:08Z'], 'task', 'run', 'original', 4, 'sdk-nonce', 8);
    }

    private static function heartbeat(): array
    {
        return [...self::control(), 'server_time' => '2026-10-02T00:00:01Z', 'renewed' => false,
            'heartbeat_deadline_at' => '2026-10-02T00:00:09Z', 'heartbeat_recorded' => true,
            'heartbeat_history_event_id' => 'canonical-heartbeat'];
    }

    private static function cleanup(): array
    {
        return ['request_id' => 'child-request', 'root_request_id' => 'root-request',
            'delivery_history_event_id' => 'canonical-delivery', 'cleanup_deadline_at' => '2026-10-02T00:00:30Z'];
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
