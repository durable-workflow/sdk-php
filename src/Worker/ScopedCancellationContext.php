<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use DateTimeImmutable;
use Closure;
use InvalidArgumentException;
use LogicException;

/** Immutable original scope ancestry carried by a candidate cooperative child request. */
final class ScopedCancellationContext
{
    public const SCHEMA = 'durable-workflow.scoped-cancellation-context/v1';

    /** @var (Closure(): DateTimeImmutable)|null */
    private ?Closure $replayClock = null;

    private ?DateTimeImmutable $authorityDeadline = null;

    public readonly string $requestId;
    public readonly ?string $parentRequestId;
    public readonly string $workflowInstanceId;
    public readonly string $workflowRunId;
    public readonly string $scopeId;
    public readonly string $rootScopeId;

    public readonly string $rootRequestId;

    public readonly string $rootWorkflowInstanceId;

    public readonly string $rootWorkflowRunId;

    public readonly ?string $reason;

    /** @var array<string, string> */
    public readonly array $requester;

    public readonly string $source;

    /** @param list<array{request_id: string, workflow_instance_id: string, workflow_run_id: string, scope_id: string, cleanup_deadline_at: string}> $lineage */
    private function __construct(
        public readonly CancellationContext $rootContext,
        public readonly array $lineage,
        private readonly DateTimeImmutable $cleanupDeadline,
    ) {
        $last = $lineage[count($lineage) - 1];
        $this->requestId = $last['request_id'];
        $this->parentRequestId = count($lineage) === 1 ? null : $lineage[count($lineage) - 2]['request_id'];
        $this->workflowInstanceId = $last['workflow_instance_id'];
        $this->workflowRunId = $last['workflow_run_id'];
        $this->scopeId = $last['scope_id'];
        $this->rootScopeId = $lineage[0]['scope_id'];
        $this->rootRequestId = $rootContext->rootRequestId;
        $this->rootWorkflowInstanceId = $rootContext->rootWorkflowInstanceId;
        $this->rootWorkflowRunId = $rootContext->rootWorkflowRunId;
        $this->reason = $rootContext->reason;
        $this->requester = $rootContext->requester;
        $this->source = $rootContext->source;
    }

    /** @param array<string, mixed> $snapshot */
    public static function fromArray(array $snapshot): self
    {
        self::assertKeys($snapshot, ['schema', 'root_context', 'lineage']);
        if ($snapshot['schema'] !== self::SCHEMA || !is_array($snapshot['root_context'])
            || ($snapshot['root_context']['schema'] ?? null) !== 'durable-workflow.cancellation-context/v1') {
            throw new InvalidArgumentException('Scoped cancellation requires its original root context.');
        }
        $root = CancellationContext::fromArray($snapshot['root_context']);
        self::assertKeys($snapshot['root_context'], array_keys($root->toArray()));
        if ($root->requestId !== $root->rootRequestId || $root->parentRequestId !== null || count($root->lineage) !== 1) {
            throw new InvalidArgumentException('Scoped cancellation root must contain the original root request.');
        }
        $lineage = $snapshot['lineage'];
        if (!is_array($lineage) || !array_is_list($lineage) || $lineage === []) {
            throw new InvalidArgumentException('Scoped cancellation lineage must contain its root address.');
        }
        $normalized = [];
        $requests = [];
        $addresses = [];
        $instancesByRun = [];
        $lastRun = null;
        $deadline = $root->deadline();
        foreach ($lineage as $index => $entry) {
            if (!is_array($entry)) {
                throw new InvalidArgumentException('Scoped cancellation lineage entry is invalid.');
            }
            self::assertKeys($entry, ['request_id', 'workflow_instance_id', 'workflow_run_id', 'scope_id', 'cleanup_deadline_at']);
            $entry = [
                'request_id' => self::text($entry, 'request_id'),
                'workflow_instance_id' => self::text($entry, 'workflow_instance_id'),
                'workflow_run_id' => self::text($entry, 'workflow_run_id'),
                'scope_id' => self::text($entry, 'scope_id'),
                'cleanup_deadline_at' => self::text($entry, 'cleanup_deadline_at'),
            ];
            if ($index === 0 && ($entry['request_id'] !== $root->requestId
                || $entry['workflow_instance_id'] !== $root->rootWorkflowInstanceId
                || $entry['workflow_run_id'] !== $root->rootWorkflowRunId)) {
                throw new InvalidArgumentException('Scoped cancellation root address does not match its request.');
            }
            $address = json_encode([$entry['workflow_run_id'], $entry['scope_id']], JSON_THROW_ON_ERROR);
            if (isset($requests[$entry['request_id']]) || isset($addresses[$address])) {
                throw new InvalidArgumentException('Scoped cancellation lineage cannot repeat a request or address.');
            }
            if (isset($instancesByRun[$entry['workflow_run_id']])
                && ($instancesByRun[$entry['workflow_run_id']] !== $entry['workflow_instance_id']
                    || $lastRun !== $entry['workflow_run_id'])) {
                throw new InvalidArgumentException('Scoped cancellation lineage cannot reenter or reassign an earlier run.');
            }
            $deadlineSnapshot = $root->toArray();
            $deadlineSnapshot['cleanup_deadline_at'] = $entry['cleanup_deadline_at'];
            $nextDeadline = CancellationContext::fromArray($deadlineSnapshot)->deadline();
            if (($index === 0 && $nextDeadline != $root->deadline()) || $nextDeadline > $deadline) {
                throw new InvalidArgumentException('Scoped cancellation cannot change its root or extend a descendant budget.');
            }
            $entry['cleanup_deadline_at'] = $nextDeadline->format('Y-m-d\TH:i:s.u\Z');
            $normalized[] = $entry;
            $requests[$entry['request_id']] = true;
            $addresses[$address] = true;
            $instancesByRun[$entry['workflow_run_id']] = $entry['workflow_instance_id'];
            $lastRun = $entry['workflow_run_id'];
            $deadline = $nextDeadline;
        }

        return new self($root, $normalized, $deadline);
    }

    /** @internal Preserve the canonical run request as the implicit root scope. */
    public static function fromRunContext(CancellationContext $context): self
    {
        $last = $context->lineage[count($context->lineage) - 1];
        $deadline = $context->deadline()->format('Y-m-d\TH:i:s.u\Z');
        if ($context->scopeOrigin !== null) {
            $snapshot = $context->scopeOrigin->toArray();
            $snapshot['lineage'][] = [
                'request_id' => $context->requestId,
                'workflow_instance_id' => $last['workflow_instance_id'],
                'workflow_run_id' => $last['workflow_run_id'],
                'scope_id' => 'root',
                'cleanup_deadline_at' => $deadline,
            ];
            return self::fromArray($snapshot);
        }
        $root = $context->toArray();
        $root['request_id'] = $context->rootRequestId;
        $root['parent_request_id'] = null;
        $root['lineage'] = [$context->lineage[0]];
        return self::fromArray([
            'schema' => self::SCHEMA,
            'root_context' => $root,
            'lineage' => array_map(static fn (array $entry): array => [...$entry,
                'scope_id' => 'root', 'cleanup_deadline_at' => $deadline], $context->lineage),
        ]);
    }

    public function requestedAt(): DateTimeImmutable
    {
        return $this->rootContext->requestedAt();
    }

    public function rootDeadline(): DateTimeImmutable
    {
        return $this->rootContext->deadline();
    }

    public function deadline(): DateTimeImmutable
    {
        return $this->cleanupDeadline;
    }

    /** Remaining scoped budget at the consumed boundary, never the host clock. */
    public function remaining(): float
    {
        if ($this->replayClock === null) {
            throw new LogicException('Cancellation remaining() requires deterministic workflow time.');
        }
        $time = ($this->replayClock)();
        $deadline = $this->authorityDeadline ?? $this->cleanupDeadline;
        $seconds = (int) $deadline->format('U') - (int) $time->format('U');
        $microseconds = (int) $deadline->format('u') - (int) $time->format('u');

        return max(0.0, $seconds + $microseconds / 1_000_000);
    }

    /** @internal @param Closure(): DateTimeImmutable $clock */
    public function withReplayClock(Closure $clock, ?DateTimeImmutable $authorityDeadline = null): self
    {
        if ($authorityDeadline !== null && ($authorityDeadline < $this->requestedAt()
            || $authorityDeadline > ($this->authorityDeadline ?? $this->cleanupDeadline))) {
            throw new InvalidArgumentException('Scoped cancellation cannot extend or replace its original authority ceiling.');
        }
        $context = clone $this;
        $context->replayClock = $clock;
        if ($authorityDeadline !== null) { $context->authorityDeadline = $authorityDeadline; }

        return $context;
    }

    /** @return array<string, mixed> */
    public function toArray(): array
    {
        return ['schema' => self::SCHEMA, 'root_context' => $this->rootContext->toArray(), 'lineage' => $this->lineage];
    }

    /** @param array<string, mixed> $value */
    private static function text(array $value, string $key): string
    {
        $text = $value[$key] ?? null;
        if (!is_string($text) || trim($text) === '') {
            throw new InvalidArgumentException('Scoped cancellation address and deadline must be nonempty strings.');
        }

        return $text;
    }

    /**
     * @param array<string, mixed> $value
     * @param list<string> $keys
     */
    private static function assertKeys(array $value, array $keys): void
    {
        $actual = array_keys($value);
        sort($actual);
        sort($keys);
        if ($actual !== $keys) {
            throw new InvalidArgumentException('Scoped cancellation context has missing or unsupported fields.');
        }
    }
}
