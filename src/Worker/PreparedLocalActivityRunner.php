<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use Closure;
use DateTimeImmutable;
use DurableWorkflow\Client;
use DurableWorkflow\Exception\InvalidLocalActivityReport;
use DurableWorkflow\Exception\ServerException;
use DurableWorkflow\Transport\RequestBudget;
use Throwable;

/** @internal Executes one durably admitted callback. Native owns retries and timeouts. */
final class PreparedLocalActivityRunner
{
    private float $authorityDeadline;
    private readonly float $clockOrigin;
    private float $nextControl = 0.0;
    private ?CancellationContext $stopRequest = null;
    private bool $stopAcknowledged = false;

    /**
     * @param array<string, mixed> $admission
     * @param Closure(): bool $shutdown
     * @param Closure(array<string, mixed>): void $observeCancellation
     * @param Closure(int, int): void $started
     * @param Closure(RequestBudget): void $keepAlive
     */
    public function __construct(
        private readonly Client $client,
        private readonly PreparedLocalActivityAttempt $attempt,
        array $admission,
        float $requestStartedAt,
        private readonly Closure $shutdown,
        private readonly Closure $observeCancellation,
        private readonly Closure $started,
        private readonly Closure $keepAlive,
    ) {
        $this->clockOrigin = $requestStartedAt - (float) (new DateTimeImmutable($admission['server_time']))->format('U.u');
        $this->authorityDeadline = $this->deadline($admission, $requestStartedAt);
    }

    /**
     * @param Closure(Closure(array<array-key, mixed>): mixed): mixed $callback
     * @return array<string, mixed> Canonical outcome receipt, never the callback's in-memory result.
     */
    public function execute(Closure $callback): array
    {
        try {
            $result = (new CooperativeActivityExecutor($this->client->payloadCodec()))->execute(
                $callback,
                $this->heartbeat(...),
                $this->check(...), $this->started, $this->acknowledgeJoinedStop(...),
            );
        } catch (CooperativeCancellationObserved $error) {
            // A cancellation check may fence an admitted attempt before any
            // fork. No application code ran, or the executor already joined it.
            $this->acknowledgeJoinedStop();
            throw $error;
        } catch (WorkflowClaimAborted $error) {
            throw $error;
        } catch (ActivityExecutionFailure $error) {
            return $this->publishResult(null, $error);
        } catch (Throwable $error) {
            // Supervisor/transport failures cannot become application failures.
            throw new WorkflowClaimAborted('Prepared local execution lost trustworthy callback authority.', previous: $error);
        }

        return $this->publishResult($result, null);
    }

    /**
     * @param list<array{runner: self, callback: Closure}> $members
     * @return non-empty-array<int, array<string, mixed>> Receipts in settlement order.
     */
    public static function executeGroup(array $members): array
    {
        if ($members === [] || count($members) > 100) {
            throw new \InvalidArgumentException('Prepared local execution requires 1 to 100 admitted group members.');
        }
        $operations = [];
        foreach ($members as $member) {
            $runner = $member['runner'];
            $operations[] = [
                'callback' => $member['callback'], 'heartbeat' => $runner->heartbeat(...),
                'check' => $runner->check(...), 'started' => $runner->started,
                'stopped' => $runner->acknowledgeJoinedStop(...),
            ];
        }
        $receipts = [];
        try {
            (new CooperativeActivityExecutor($members[0]['runner']->client->payloadCodec()))->executeConcurrent($operations,
                static function (int $index, mixed $value, ?ActivityExecutionFailure $failure) use ($members, &$receipts): void {
                    $receipt = $members[$index]['runner']->publishResult($value, $failure);
                    if ($receipt['claim_released']) {
                        throw new WorkflowClaimDeferred('Native scheduled a group member retry and released this workflow claim.');
                    }
                    $receipts[$index] = $receipt;
                });
        } catch (CooperativeCancellationObserved $error) {
            // The executor joined every process before returning this error.
            // Members not yet forked also have admitted Native attempts.
            $acknowledgmentError = null;
            foreach ($members as $member) {
                $runner = $member['runner'];
                if ($runner->stopAcknowledged) {
                    continue;
                }
                try {
                    try { $runner->check(true); } catch (CooperativeCancellationObserved) {}
                    $runner->acknowledgeJoinedStop();
                } catch (Throwable $failure) { $acknowledgmentError ??= $failure; }
            }
            if ($acknowledgmentError !== null) {
                throw $acknowledgmentError;
            }
            throw $error;
        }
        if ($receipts === []) {
            throw new WorkflowClaimAborted('Prepared local group returned no canonical outcome receipts.');
        }

        return $receipts;
    }

    /** @param array<array-key, mixed> $progress
     * @return array<string, mixed>
     */
    private function heartbeat(array $progress): array
    {
        $started = hrtime(true) / 1e9;
        $reply = $this->operation('heartbeat', ['progress' => $progress === [] ? [] : ['details' => $progress]]);
        $this->attempt->validateHeartbeat($reply);
        $this->acceptControl($reply, $started);

        return $reply;
    }

    /** @internal Only before execute/executeGroup has started any process. */
    public function acknowledgeUnstartedCancellation(): void
    {
        try {
            $this->check(true);
        } catch (CooperativeCancellationObserved) {
            $this->acknowledgeJoinedStop();
            return;
        }
        throw new WorkflowClaimAborted('An unstarted admitted group member lacks its canonical cancellation fence.');
    }

    /** @return array<string, mixed> */
    private function publishResult(mixed $result, ?ActivityExecutionFailure $error): array
    {
        if ($error === null) {
            $report = ['outcome' => 'completed', 'result' => $this->client->payloadCodec()->envelope($result),
                'payload_codec' => $this->client->payloadCodec()->name()];
        } else {
            if ($error->storageAdmissionFailure) {
                throw new WorkflowClaimAborted('The prepared callback received a storage admission refusal.', previous: $error);
            }
            $invalid = $error->duringEncoding || $error->invalidReport || $error->cancelled || $error->timeoutKind !== null;
            $report = ['outcome' => 'failed', 'message' => $error->getMessage(),
                'exception_type' => $invalid ? InvalidLocalActivityReport::class : $error->originalType,
                'non_retryable' => $invalid];
        }
        try {
            $this->check(true);
            // Encoding and external-payload upload share this original authority budget.
            $reply = $this->operation('outcome', ['report' => $report]);
            $this->attempt->validateOutcome($reply);

            return $reply;
        } catch (CooperativeCancellationObserved $error) {
            $this->acknowledgeJoinedStop();
            throw $error;
        } catch (WorkflowClaimAborted $error) {
            throw $error;
        } catch (Throwable $error) {
            // The outcome may already be committed. Never execute the callback again here.
            throw new WorkflowClaimAborted('Prepared local outcome lacks a trustworthy canonical receipt.', previous: $error);
        }
    }

    private function check(bool $force): void
    {
        if (($this->shutdown)()) {
            throw new WorkflowClaimRevoked('worker_shutdown', 'Worker stopped the prepared local callback.');
        }
        $this->budget()->remainingSeconds();
        if (!$force && hrtime(true) / 1e9 < $this->nextControl) {
            return;
        }
        $started = hrtime(true) / 1e9;
        $reply = $this->operation('control', ['renew_lease' => true]);
        $this->attempt->validateControl($reply, true);
        $this->acceptControl($reply, $started);
        $this->nextControl = hrtime(true) / 1e9 + 1;
        ($this->keepAlive)($this->budget());
        $this->budget()->remainingSeconds();
    }

    /** @param array<string, mixed> $reply */
    private function acceptControl(array $reply, float $started): void
    {
        if (!$reply['active']) {
            if (in_array($reply['reason'], ['cancellation_requested', 'cancellation_deadline_expired'], true)) {
                $snapshot = $reply['cancellation_request'] ?? null;
                if (!is_array($snapshot)) {
                    throw new WorkflowClaimAborted('Prepared stop lacks its canonical cancellation context.');
                }
                $request = CancellationContext::fromArray($snapshot);
                $last = $request->lineage[count($request->lineage) - 1];
                if ($last['workflow_run_id'] !== $this->attempt->runId
                    || ($reply['fenced'] ?? null) !== true
                    || !is_string($reply['cancellation_history_event_id'] ?? null)
                    || trim($reply['cancellation_history_event_id']) === '') {
                    throw new WorkflowClaimAborted('Prepared stop does not fence this original callback.');
                }
                ($this->observeCancellation)([...$request->toArray(),
                    'history_refresh_page_token' => $reply['history_refresh_page_token'] ?? null]);
                $this->stopRequest = $request;
                // Join and acknowledge before workflow replay can deliver this request.
                throw new CooperativeCancellationObserved('The admitted local callback observed cooperative cancellation.');
            }
            throw new WorkflowClaimRevoked((string) $reply['reason'], 'Prepared local callback lost its original authority.');
        }
        $this->authorityDeadline = $this->deadline($reply, $started);
        $this->budget()->remainingSeconds();
    }

    private function acknowledgeJoinedStop(): void
    {
        if ($this->stopRequest === null || $this->stopAcknowledged) {
            return;
        }
        // Stopping is already proved locally. This report grants no callback or claim authority.
        try {
            $reply = $this->client->preparedLocalActivityOperation(
                $this->attempt->taskId, $this->attempt->leaseOwner, $this->attempt->workflowTaskAttempt,
                'acknowledge-cancellation', ['request_id' => $this->stopRequest->requestId], $this->attempt->attemptId,
            );
            if (($reply['acknowledged'] ?? null) !== true || !is_bool($reply['duplicate'] ?? null)
                || !array_key_exists('reason', $reply) || $reply['reason'] !== null
                || !is_string($reply['history_event_id'] ?? null) || trim($reply['history_event_id']) === '') {
                throw new \UnexpectedValueException('Malformed joined-stop receipt.');
            }
            $this->stopAcknowledged = true;
        } catch (Throwable $error) {
            throw new WorkflowClaimAborted('Joined prepared callback stop was not acknowledged.', previous: $error);
        }
    }

    /** @param array<string, mixed> $body
     * @return array<string, mixed>
     */
    private function operation(string $operation, array $body): array
    {
        try {
            return $this->client->preparedLocalActivityOperation($this->attempt->taskId, $this->attempt->leaseOwner,
                $this->attempt->workflowTaskAttempt, $operation, $body, $this->attempt->attemptId, $this->budget());
        } catch (Throwable $error) {
            $reason = $error instanceof ServerException ? $error->reason : null;
            throw new WorkflowClaimAborted('Prepared local '.$operation.' could not prove its original authority'
                .($reason === null ? '.' : ': '.$reason.'.'), previous: $error);
        }
    }

    private function budget(): RequestBudget
    {
        return new RequestBudget(5, $this->authorityDeadline);
    }

    /** @param array<string, mixed> $receipt */
    private function deadline(array $receipt, float $started): float
    {
        $server = (float) (new DateTimeImmutable($receipt['server_time']))->format('U.u');
        $deadline = INF;
        foreach (['lease_expires_at', 'workflow_lease_expires_at', 'start_to_close_deadline_at',
            'schedule_to_close_deadline_at', 'heartbeat_deadline_at'] as $field) {
            if (($receipt[$field] ?? null) !== null) {
                $epoch = (float) (new DateTimeImmutable($receipt[$field]))->format('U.u');
                $deadline = min($deadline, $this->clockOrigin + $epoch, $started + $epoch - $server);
            }
        }
        $cleanup = $receipt['cancellation_cleanup']['cleanup_deadline_at'] ?? null;
        if ($cleanup !== null) {
            $epoch = (float) (new DateTimeImmutable($cleanup))->format('U.u');
            $deadline = min($deadline, $this->clockOrigin + $epoch, $started + $epoch - $server);
        }
        // Count the entire request's elapsed time. Clock skew on this host grants no extra budget.
        return $deadline;
    }
}
