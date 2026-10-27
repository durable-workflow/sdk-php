<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

/** Immutable attempt identity and retry snapshot received with an activity claim. */
final class ActivityRetryInfo
{
    private function __construct(
        public readonly int $attemptNumber,
        public readonly bool $policyProvided,
        private readonly mixed $policy,
    ) {
    }

    /** @param array<string, mixed> $task */
    public static function fromTask(int $attemptNumber, array $task): self
    {
        return new self($attemptNumber, array_key_exists('retry_policy', $task), $task['retry_policy'] ?? null);
    }

    /** Original decoded policy, including unknown fields and explicit legacy null. */
    public function policy(): mixed
    {
        return $this->policy;
    }

    /** A positive configured limit; null alone does not imply unlimited retries. */
    public function maxAttempts(): ?int
    {
        $limit = is_array($this->policy) ? ($this->policy['max_attempts'] ?? null) : null;

        return is_int($limit) && $limit > 0 ? $limit : null;
    }

    /** Only an explicit max_attempts: null inside a policy means unlimited. */
    public function isUnlimited(): bool
    {
        return is_array($this->policy)
            && array_key_exists('max_attempts', $this->policy)
            && $this->policy['max_attempts'] === null;
    }

    /**
     * True at or beyond a bounded final attempt, false for a remaining budget or
     * explicit unlimited policy, and null for unknown policy or an invalid attempt.
     * Cancellation, non-retryable failure, run closure and deadlines may end
     * execution before the configured budget is exhausted.
     */
    public function isLastConfiguredAttempt(): ?bool
    {
        if ($this->attemptNumber < 1) {
            return null;
        }
        $limit = $this->maxAttempts();
        if ($limit !== null) {
            return $this->attemptNumber >= $limit;
        }

        return $this->isUnlimited() ? false : null;
    }
}
