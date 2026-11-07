<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use Closure;
use DurableWorkflow\Transport\RequestBodySent;
use DurableWorkflow\Exception\ServerException;
use Fiber;
use Throwable;

/** @internal One completion owner and at most one already-leased workflow ahead. */
final class CompletionPipeline
{
    public ?WorkflowPrefetch $prefetched = null;
    public ?WorkflowPollRetry $recovery = null;
    public bool $prefetchPending = false;
    /** @var Fiber<mixed, mixed, mixed, mixed>|null */
    private ?Fiber $current = null;
    /** @var array{fiber: Fiber<mixed, mixed, mixed, mixed>, state: array<string, mixed>}|null */
    private ?array $pending = null;
    private ?Throwable $failure = null;
    private bool $resuming = false;
    private bool $stopping = false;
    private bool $authorityLost = false;
    /** @var Fiber<mixed, mixed, mixed, mixed>|null */
    private ?Fiber $abandoning = null;

    /**
     * @param Closure(int): bool $tick
     * @param Closure(): array<string, mixed> $capture
     * @param Closure(): void $reset
     * @param Closure(Fiber<mixed, mixed, mixed, mixed>, array<string, mixed>): void $resume
     * @param Closure(Fiber<mixed, mixed, mixed, mixed>): void $disarm
     */
    public function __construct(
        private readonly Closure $tick,
        private readonly Closure $capture,
        private readonly Closure $reset,
        private readonly Closure $resume,
        private readonly Closure $disarm,
    ) {
    }

    public function current(): bool
    {
        return !$this->resuming && $this->current !== null && Fiber::getCurrent() === $this->current;
    }

    public function hasAhead(): bool
    {
        return $this->prefetched !== null || $this->recovery !== null || $this->pending !== null || $this->prefetchPending;
    }

    public function canPrefetch(): bool
    {
        return $this->current() && !$this->hasAhead() && $this->failure === null && !$this->stopping;
    }

    public function requestStop(): void
    {
        $this->stopping = true;
    }

    public function stopping(): bool
    {
        return $this->stopping;
    }

    public function authorityLost(): bool
    {
        return $this->authorityLost;
    }

    public function abandoningCurrent(): bool
    {
        return $this->abandoning !== null && Fiber::getCurrent() === $this->abandoning;
    }

    public function defer(Throwable $error): void
    {
        $this->failure ??= $error;
        if ($error instanceof ServerException && (in_array($error->status, [401, 403], true)
            || in_array($error->reason, ['stale_worker_registration', 'worker_registration_fenced'], true))) {
            $this->authorityLost = true;
            $this->stopping = true;
            $this->prefetched = null;
            $this->recovery = null;
            $this->prefetchPending = false;
        }
    }

    public function join(): void
    {
        $parent = $this->pending;
        if ($parent === null) {
            return;
        }
        $this->pending = null;
        $this->resuming = true;
        try {
            ($this->resume)($parent['fiber'], $parent['state']);
        } catch (Throwable $error) {
            $this->defer($error);
        } finally {
            ($this->disarm)($parent['fiber']);
            $this->resuming = false;
        }
    }

    public function tick(int $timeout): void
    {
        do {
            ($this->reset)();
            $fiber = $this->current = new Fiber(fn (): bool => ($this->tick)($timeout));
            try {
                $yielded = $fiber->start();
                while ($fiber->isSuspended() && !($yielded instanceof RequestBodySent && $this->prefetched !== null)) {
                    $yielded = $fiber->resume();
                }
                if ($fiber->isSuspended()) {
                    if ($this->pending !== null) {
                        throw new \LogicException('A second completion cannot be held ahead.');
                    }
                    $this->pending = ['fiber' => $fiber, 'state' => ($this->capture)()];
                    $timeout = 0;
                    continue;
                }
            } catch (Throwable $error) {
                $this->defer($error);
            } finally {
                if (($this->pending['fiber'] ?? null) !== $fiber) {
                    // Client-side validation may fail before the armed HTTP request begins.
                    ($this->disarm)($fiber);
                }
            }
            $this->join();
            $timeout = 0;
        } while ($this->prefetched !== null || ($this->recovery !== null && !$this->stopping));
        $this->current = null;
        if ($this->failure !== null) {
            throw $this->failure;
        }
    }

    public function finish(): void
    {
        $this->requestStop();
        $parent = $this->pending;
        if ($parent !== null) {
            $this->abandoning = $parent['fiber'];
            ($this->disarm)($parent['fiber']);
            $this->join();
        }
        $this->abandoning = null;
        $this->prefetched = null;
        $this->recovery = null;
        $this->prefetchPending = false;
        $this->current = null;
    }
}
