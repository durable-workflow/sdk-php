<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DurableWorkflow\Exception\NonDeterministicWorkflow;
use Throwable;

/** @internal */
final class ConditionInputOrder
{
    public const CAPABILITY = 'condition_wait_input_order';

    public const ORDER = 'durable_receipt_order';

    public const PENDING_INPUT = 'condition_wait_pending_input_sequence';

    /** @var array<string, array{open: int, schedule: ?int, timer: ?string, fire: ?int}> */
    private array $waits = [];

    /** @var list<int> */
    private array $appliedPositions = [];

    /** @var array<string, array{id: string, sequence: int}> */
    private array $competingSelections = [];

    /** @param list<array<string, mixed>> $history */
    public function __construct(array $history)
    {
        $receipts = [];
        $terminals = [];
        foreach ($history as $event) {
            $type = $event['event_type'] ?? $event['type'] ?? null;
            $payload = is_array($event['payload'] ?? null) ? $event['payload'] : [];
            $sequence = $event['sequence'] ?? null;
            if (is_int($sequence) && in_array($type, ['ChildRunCompleted', 'ChildRunFailed', 'ChildRunCancelled', 'ChildRunTerminated'], true)) {
                $terminals[$sequence] = [$event['id'] ?? $event['event_id'] ?? null, $type];
            }
            if ($type === 'SelectionCandidateReady') {
                $group = $payload['selection_group_id'] ?? null;
                $position = $payload['resolution_event_sequence'] ?? null;
                $terminalId = $payload['resolution_event_id'] ?? null;
                if (!is_string($group) || $group === '' || isset($this->competingSelections[$group])
                    || !is_int($position) || $position < 1 || !is_int($sequence) || $position >= $sequence
                    || !is_string($terminalId) || $terminalId === ''
                    || ($terminals[$position] ?? null) !== [$terminalId, $payload['resolution_event_type'] ?? null]) {
                    throw $this->mismatch();
                }
                $this->competingSelections[$group] = ['id' => $terminalId, 'sequence' => $position];
            }
            $commandId = $event['workflow_command_id'] ?? $payload['workflow_command_id'] ?? null;
            if (is_int($sequence) && $sequence > 0 && is_string($commandId) && $commandId !== '') {
                if ($type === 'SignalReceived' || $type === 'UpdateAccepted') {
                    $receipts[$type][$commandId] ??= $sequence;
                } elseif ($type === 'SignalApplied' || $type === 'UpdateApplied') {
                    $receiptType = $type === 'SignalApplied' ? 'SignalReceived' : 'UpdateAccepted';
                    if (isset($receipts[$receiptType][$commandId])) {
                        $this->appliedPositions[] = $receipts[$receiptType][$commandId];
                    }
                }
            }
            $wait = $payload['condition_wait_id'] ?? null;
            if (!is_string($wait) || $wait === '') {
                continue;
            }
            if ($type === 'ConditionWaitOpened' && self::marked($payload)) {
                if (!is_int($sequence) || $sequence < 1 || isset($this->waits[$wait])) {
                    throw $this->mismatch();
                }
                $this->waits[$wait] = ['open' => $sequence, 'schedule' => null, 'timer' => null, 'fire' => null];
                continue;
            }
            $opening = $this->waits[$wait] ?? null;
            if ($opening === null) {
                continue;
            }
            if ($type === 'TimerScheduled') {
                $timer = $payload['timer_id'] ?? null;
                if (!self::marked($payload) || ($payload['timer_kind'] ?? null) !== 'condition_timeout'
                    || !is_int($sequence) || $sequence <= $opening['open'] || $opening['schedule'] !== null
                    || !is_string($timer) || $timer === '') {
                    throw $this->mismatch();
                }
                $this->waits[$wait]['schedule'] = $sequence;
                $this->waits[$wait]['timer'] = $timer;
            } elseif ($type === 'TimerFired') {
                if (!self::marked($payload) || ($payload['timer_kind'] ?? null) !== 'condition_timeout'
                    || ($payload['timer_id'] ?? null) !== $opening['timer'] || $opening['schedule'] === null
                    || !is_int($sequence) || $sequence <= $opening['schedule'] || $opening['fire'] !== null) {
                    throw $this->mismatch();
                }
                $this->waits[$wait]['fire'] = $sequence;
            } elseif ($type === 'ConditionWaitSatisfied' || $type === 'ConditionWaitTimedOut') {
                $fire = $opening['fire'];
                $receipt = $payload['input_receipt_sequence'] ?? null;
                if (!self::marked($payload) || ($payload['timer_fired_sequence'] ?? null) !== $fire
                    || !is_int($sequence) || $sequence <= ($fire ?? $opening['open'])
                    || ($receipt !== null && (!is_int($receipt) || $receipt < 1 || $receipt >= $sequence))
                    || ($type === 'ConditionWaitTimedOut' && ($fire === null || $receipt !== null))
                    || ($type === 'ConditionWaitSatisfied' && $fire !== null && ($receipt === null || $receipt >= $fire))) {
                    throw $this->mismatch();
                }
            }
        }
        foreach ($this->waits as $wait) {
            if ($wait['schedule'] === null) {
                throw $this->mismatch();
            }
        }
        sort($this->appliedPositions, SORT_NUMERIC);
    }

    /** @param array<string, mixed> $payload */
    public static function marked(array $payload): bool
    {
        return ($payload[self::CAPABILITY] ?? null) === self::ORDER;
    }

    public function has(?string $id): bool
    {
        return $id !== null && isset($this->waits[$id]);
    }

    /**
     * @param array<string, mixed> $task
     * @return array<string, mixed>|null
     */
    public function decide(
        string $id,
        array $task,
        WorkflowContext $context,
        WorkflowCommand $command,
        ?string $selectionGroup = null,
    ): ?array
    {
        if (!array_key_exists(self::PENDING_INPUT, $task)) {
            throw new WorkflowClaimDeferred('condition_input_snapshot_missing: ordered condition replay requires the original worker snapshot.');
        }
        $pending = $task[self::PENDING_INPUT];
        if ($pending !== null && (!is_int($pending) || $pending < 1)) {
            throw $this->mismatch();
        }
        $wait = $this->waits[$id];
        $fire = $wait['fire'];
        $candidate = $selectionGroup === null ? null : ($this->competingSelections[$selectionGroup] ?? null);
        $terminal = min($fire ?? PHP_INT_MAX, $candidate['sequence'] ?? PHP_INT_MAX);
        $limit = min($pending ?? PHP_INT_MAX, $terminal);
        $prefix = [];
        $entryReceipt = null;
        foreach ($this->appliedPositions as $receipt) {
            if ($receipt > $wait['open'] || $receipt >= $limit) {
                break;
            }
            $prefix[$receipt] = true;
            $entryReceipt = $receipt;
        }
        $result = ['type' => 'resolve_condition_wait', 'condition_wait_id' => $id];
        $error = null;
        $evaluate = static function (array $view) use ($context, $command, &$error): bool {
            try {
                $error = null;

                return $context->withConditionInputReceipts($view, $command->conditionSatisfied(...));
            } catch (Throwable $failure) {
                $error = $failure;

                return false;
            }
        };
        if ($evaluate($prefix) && ($entryReceipt !== null || $fire === null)) {
            return $result + ['satisfied' => true] + ($entryReceipt === null ? [] : ['input_receipt_sequence' => $entryReceipt]);
        }
        foreach ($this->appliedPositions as $receipt) {
            if ($receipt >= $limit) {
                break;
            }
            if (isset($prefix[$receipt])) {
                continue;
            }
            $prefix[$receipt] = true;
            if ($evaluate($prefix)) {
                return $result + ['satisfied' => true, 'input_receipt_sequence' => $receipt];
            }
        }
        if ($pending === null || $pending >= $terminal) {
            if ($error !== null) {
                throw $error;
            }
            if ($candidate !== null && $candidate['sequence'] === $terminal) {
                return $result + ['satisfied' => false, 'competing_resolution_event_id' => $candidate['id']];
            }
            if ($fire !== null) {
                return $result + ['satisfied' => false];
            }
        }

        return null;
    }

    private function mismatch(): NonDeterministicWorkflow
    {
        return new NonDeterministicWorkflow('Ordered condition history does not match its original timer and receipt boundary.',
            reason: 'condition_input_order_mismatch');
    }
}
