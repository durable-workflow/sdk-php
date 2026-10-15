<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DateTimeImmutable;
use DateTimeZone;
use DurableWorkflow\Client;
use DurableWorkflow\Exception\TransportException;
use DurableWorkflow\Transport\RequestBudget;
use Throwable;

/** @internal Original, non-renewing authority shared by query loading and dispatch. */
final class QueryTaskAuthority
{
    private float $until;
    /** @var array<string, mixed> */
    private array $identity;
    /** @var array<string, string> */
    private array $timing;

    /** @param array<string, mixed> $task */
    public function __construct(
        private readonly Client $client,
        array $task,
        string $owner,
        string $queue,
        public readonly bool $observed,
    ) {
        if (!is_string($task['query_task_id'] ?? null) || $task['query_task_id'] === ''
            || !is_int($task['query_task_attempt'] ?? null) || $task['query_task_attempt'] < 1
            || ($task['lease_owner'] ?? null) !== $owner || ($task['task_queue'] ?? null) !== $queue
            || ($task['namespace'] ?? $client->namespace) !== $client->namespace) {
            throw new WorkflowClaimRevoked('invalid_query_claim', 'The query delivery does not identify this Worker claim.');
        }
        $this->identity = array_intersect_key($task, array_flip([
            'query_task_id', 'query_task_attempt', 'lease_owner', 'task_queue',
        ])) + ['workflow_id' => $task['workflow_id'] ?? null, 'run_id' => $task['run_id'] ?? null,
            'namespace' => $task['namespace'] ?? $client->namespace];
        if ($observed && (!is_string($this->identity['workflow_id']) || $this->identity['workflow_id'] === ''
            || !is_string($this->identity['run_id']) || $this->identity['run_id'] === '')) {
            throw new WorkflowClaimRevoked('invalid_query_claim', 'The query delivery lacks its original workflow and run.');
        }
        try {
            $lease = self::timestamp($task['lease_expires_at'] ?? null, $observed);
            $this->timing = ['lease_expires_at' => $task['lease_expires_at']];
            $bound = $lease;
            if ($observed || array_key_exists('query_admitted_at', $task) || array_key_exists('query_deadline_at', $task)) {
                $admitted = self::timestamp($task['query_admitted_at'] ?? null, true);
                $deadline = self::timestamp($task['query_deadline_at'] ?? null, true);
                if ($deadline < $admitted || ($observed && $lease > $deadline)) {
                    throw new \UnexpectedValueException('Inconsistent query admission or lease timing.');
                }
                $this->timing['query_admitted_at'] = $task['query_admitted_at'];
                $this->timing['query_deadline_at'] = $task['query_deadline_at'];
                $bound = min($bound, $deadline);
            }
            $reference = $observed ? self::timestamp($task['server_time'] ?? null, true) : microtime(true);
            $this->until = hrtime(true) / 1e9 + $bound - $reference;
        } catch (Throwable $error) {
            throw new WorkflowClaimRevoked('invalid_query_timing', 'The query delivery lacks valid original timing.', previous: $error);
        }
        $this->remaining();
    }

    public function remaining(): float
    {
        $remaining = $this->until - hrtime(true) / 1e9;
        if ($remaining <= 0) {
            throw new WorkflowClaimRevoked('query_execution_budget_expired', 'The original query execution budget expired.');
        }

        return $remaining;
    }

    public function budget(): RequestBudget
    {
        $this->remaining();

        $budget = new RequestBudget(5, $this->until);
        $this->assertRequestBudget($budget);

        return $budget;
    }

    public function assertRequestBudget(RequestBudget $budget): void
    {
        try {
            $budget->remainingSeconds();
        } catch (TransportException $error) {
            throw new WorkflowClaimRevoked('query_control_budget_exhausted',
                'The remaining query budget cannot cover bounded control I/O.', previous: $error);
        }
    }

    /** @param array<string, mixed> $task */
    public function matches(array $task): bool
    {
        foreach ($this->identity as $key => $value) {
            if (($task[$key] ?? ($key === 'namespace' ? $this->client->namespace : null)) !== $value) {
                return false;
            }
        }
        foreach ($this->timing as $key => $value) {
            if (($task[$key] ?? null) !== $value) {
                return false;
            }
        }

        return true;
    }

    public function confirm(): void
    {
        $this->remaining();
        if (!$this->observed) {
            return;
        }
        $started = hrtime(true) / 1e9;
        try {
            $reply = $this->client->queryTaskStatus($this->identity['query_task_id'],
                $this->identity['lease_owner'], $this->identity['query_task_attempt'], $this->budget());
            foreach (['query_task_id', 'query_task_attempt', 'lease_owner'] as $key) {
                if (($reply[$key] ?? null) !== $this->identity[$key]) {
                    throw new \UnexpectedValueException('The observation identifies a different query claim.');
                }
            }
            foreach ($this->timing as $key => $value) {
                if (($reply[$key] ?? null) !== $value) {
                    throw new \UnexpectedValueException('The observation changed the original query timing.');
                }
            }
            if (($reply['can_continue'] ?? null) !== true || ($reply['query_task_status'] ?? null) !== 'leased'
                || ($reply['reason'] ?? null) !== null) {
                throw new WorkflowClaimRevoked('query_claim_unavailable', 'The Server refused query continuation.');
            }
            $bounds = [self::timestamp($this->timing['lease_expires_at'], true)];
            if (isset($this->timing['query_deadline_at'])) {
                $bounds[] = self::timestamp($this->timing['query_deadline_at'], true);
            }
            $reference = self::timestamp($reply['server_time'] ?? null, true);
            // Charge the complete observation round trip. An observation can
            // shorten the original budget, never restore elapsed execution time.
            $this->until = min($this->until, $started + min($bounds) - $reference);
        } catch (WorkflowClaimRevoked $error) {
            throw $error;
        } catch (Throwable $error) {
            throw new WorkflowClaimRevoked('query_authority_unavailable', 'Query ownership could not be confirmed.', previous: $error);
        }
        $this->remaining();
    }

    private static function timestamp(mixed $value, bool $canonical): float
    {
        if (!is_string($value) || ($canonical
            ? preg_match('/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$/D', $value) !== 1
            : preg_match('/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/D', $value) !== 1)) {
            throw new \UnexpectedValueException('Query timing must have an explicit timezone.');
        }
        $date = new DateTimeImmutable($value, new DateTimeZone('UTC'));
        $errors = DateTimeImmutable::getLastErrors();
        if (is_array($errors) && ($errors['warning_count'] || $errors['error_count'])) {
            throw new \UnexpectedValueException('Query timing is invalid.');
        }

        return (float) $date->format('U.u');
    }
}
