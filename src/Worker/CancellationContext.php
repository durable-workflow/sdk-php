<?php

declare(strict_types=1);

namespace DurableWorkflow\Worker;

use Closure;
use DateTimeImmutable;
use DateTimeZone;
use InvalidArgumentException;
use LogicException;

/** Immutable root request and cleanup budget read from canonical history. */
final class CancellationContext
{
    /** @var (Closure(): DateTimeImmutable)|null */
    private ?Closure $replayClock = null;

    /**
     * @param array<string, string> $requester
     * @param list<array{request_id: string, workflow_instance_id: string, workflow_run_id: string}> $lineage
     */
    private function __construct(
        public readonly string $requestId,
        public readonly string $rootRequestId,
        public readonly string $rootWorkflowInstanceId,
        public readonly string $rootWorkflowRunId,
        public readonly ?string $parentRequestId,
        public readonly ?string $reason,
        public readonly array $requester,
        public readonly string $source,
        private readonly DateTimeImmutable $originalRequestedAt,
        private readonly DateTimeImmutable $cleanupDeadline,
        public readonly array $lineage,
        public readonly ?ScopedCancellationContext $scopeOrigin = null,
    ) {
    }

    /** @param array<string, mixed> $snapshot */
    public static function fromArray(array $snapshot): self
    {
        $schema = $snapshot['schema'] ?? null;
        if (!in_array($schema, ['durable-workflow.cancellation-context/v1', 'durable-workflow.cancellation-context/v2'], true)) {
            throw new InvalidArgumentException('Unsupported cancellation context schema.');
        }
        $scopeOrigin = null;
        if ($schema === 'durable-workflow.cancellation-context/v2') {
            if (!is_array($snapshot['scope_origin'] ?? null)) {
                throw new InvalidArgumentException('Scoped run cancellation requires its original scope context.');
            }
            $scopeOrigin = ScopedCancellationContext::fromArray($snapshot['scope_origin']);
        } elseif (array_key_exists('scope_origin', $snapshot) || array_key_exists('scope_authority_deadline_at', $snapshot)) {
            throw new InvalidArgumentException('Legacy cancellation context cannot discard a scoped origin.');
        }
        $requester = $snapshot['requester'] ?? null;
        if (!is_array($requester) || array_is_list($requester)) {
            throw new InvalidArgumentException('Cancellation requester must identify its caller.');
        }
        foreach ($requester as $key => $value) {
            if (!in_array($key, ['type', 'id', 'label'], true) || !is_string($value) || $value === '') {
                throw new InvalidArgumentException('Cancellation requester contains unsupported metadata.');
            }
        }
        ksort($requester);
        $lineage = $snapshot['lineage'] ?? null;
        if (!is_array($lineage) || !array_is_list($lineage) || $lineage === []) {
            throw new InvalidArgumentException('Cancellation lineage must contain the root request.');
        }
        $normalized = [];
        foreach ($lineage as $entry) {
            if (!is_array($entry)) {
                throw new InvalidArgumentException('Cancellation lineage entry is invalid.');
            }
            $normalized[] = [
                'request_id' => self::text($entry, 'request_id'),
                'workflow_instance_id' => self::text($entry, 'workflow_instance_id'),
                'workflow_run_id' => self::text($entry, 'workflow_run_id'),
            ];
        }
        $requestId = self::text($snapshot, 'request_id');
        $rootRequestId = self::text($snapshot, 'root_request_id');
        $rootInstanceId = self::text($snapshot, 'root_workflow_instance_id');
        $rootRunId = self::text($snapshot, 'root_workflow_run_id');
        $parentRequestId = $snapshot['parent_request_id'] ?? null;
        $reason = $snapshot['reason'] ?? null;
        if (($parentRequestId !== null && (!is_string($parentRequestId) || trim($parentRequestId) === ''))
            || ($reason !== null && !is_string($reason))) {
            throw new InvalidArgumentException('Cancellation parent identity or reason is invalid.');
        }
        if (count(array_unique(array_column($normalized, 'request_id'))) !== count($normalized)
            || count(array_unique(array_column($normalized, 'workflow_run_id'))) !== count($normalized)) {
            throw new InvalidArgumentException('Cancellation lineage cannot contain a cycle.');
        }
        if ($normalized[0] !== [
            'request_id' => $rootRequestId,
            'workflow_instance_id' => $rootInstanceId,
            'workflow_run_id' => $rootRunId,
        ] || $normalized[count($normalized) - 1]['request_id'] !== $requestId
            || ($scopeOrigin === null && (count($normalized) === 1
                ? $parentRequestId !== null
                : $parentRequestId !== $normalized[count($normalized) - 2]['request_id']))) {
            throw new InvalidArgumentException('Cancellation lineage does not match its request identities.');
        }
        $requestedAt = self::timestamp(self::text($snapshot, 'requested_at'));
        $deadline = self::timestamp(self::text($snapshot, 'cleanup_deadline_at'));
        if ($deadline <= $requestedAt) {
            throw new InvalidArgumentException('Cancellation deadline must follow the original request.');
        }
        if ($scopeOrigin !== null) {
            self::assertScopedOrigin($snapshot, $scopeOrigin, $normalized, $requester, $requestedAt, $deadline);
        }

        return new self(
            $requestId, $rootRequestId, $rootInstanceId, $rootRunId, $parentRequestId,
            $reason, $requester, self::text($snapshot, 'source'), $requestedAt, $deadline, $normalized, $scopeOrigin,
        );
    }

    public function requestedAt(): DateTimeImmutable
    {
        return $this->originalRequestedAt;
    }

    public function deadline(): DateTimeImmutable
    {
        return $this->cleanupDeadline;
    }

    /** Remaining seconds at the consumed replay boundary, never the host clock. */
    public function remaining(): float
    {
        if ($this->replayClock === null) {
            throw new LogicException('Cancellation remaining() requires deterministic workflow time.');
        }
        $time = ($this->replayClock)();
        $seconds = (int) $this->cleanupDeadline->format('U') - (int) $time->format('U');
        $microseconds = (int) $this->cleanupDeadline->format('u') - (int) $time->format('u');

        return max(0.0, $seconds + $microseconds / 1_000_000);
    }

    /**
     * @internal
     * @param Closure(): DateTimeImmutable $clock
     */
    public function withReplayClock(Closure $clock): self
    {
        $context = clone $this;
        $context->replayClock = $clock;

        return $context;
    }

    /** @return array<string, mixed> */
    public function toArray(): array
    {
        return [
            'schema' => $this->scopeOrigin === null ? 'durable-workflow.cancellation-context/v1'
                : 'durable-workflow.cancellation-context/v2',
            'request_id' => $this->requestId,
            'root_request_id' => $this->rootRequestId,
            'root_workflow_instance_id' => $this->rootWorkflowInstanceId,
            'root_workflow_run_id' => $this->rootWorkflowRunId,
            'parent_request_id' => $this->parentRequestId,
            'reason' => $this->reason,
            'requester' => $this->requester,
            'source' => $this->source,
            'requested_at' => $this->originalRequestedAt->format('Y-m-d\TH:i:s.u\Z'),
            'cleanup_deadline_at' => $this->cleanupDeadline->format('Y-m-d\TH:i:s.u\Z'),
            'lineage' => $this->lineage,
            ...($this->scopeOrigin === null ? [] : [
                'scope_origin' => $this->scopeOrigin->toArray(),
                'scope_authority_deadline_at' => $this->cleanupDeadline->format('Y-m-d\TH:i:s.u\Z'),
            ]),
        ];
    }

    /**
     * @param array<string, mixed> $snapshot
     * @param list<array{request_id: string, workflow_instance_id: string, workflow_run_id: string}> $lineage
     * @param array<string, string> $requester
     */
    private static function assertScopedOrigin(
        array $snapshot,
        ScopedCancellationContext $origin,
        array $lineage,
        array $requester,
        DateTimeImmutable $requestedAt,
        DateTimeImmutable $deadline,
    ): void {
        $root = $origin->rootContext;
        $last = $lineage[count($lineage) - 1];
        if (($snapshot['parent_request_id'] ?? null) !== $origin->requestId
            || $snapshot['root_request_id'] !== $root->rootRequestId
            || $snapshot['root_workflow_instance_id'] !== $root->rootWorkflowInstanceId
            || $snapshot['root_workflow_run_id'] !== $root->rootWorkflowRunId
            || ($snapshot['reason'] ?? null) !== $root->reason || $requester !== $root->requester
            || self::text($snapshot, 'source') !== $root->source || $requestedAt != $root->requestedAt()
            || $deadline != self::timestamp(self::text($snapshot, 'scope_authority_deadline_at'))
            || $deadline > $origin->deadline()
            || in_array($last['request_id'], array_column($origin->lineage, 'request_id'), true)
            || in_array($last['workflow_run_id'], array_column($origin->lineage, 'workflow_run_id'), true)) {
            throw new InvalidArgumentException('Run cancellation does not preserve its original scope context.');
        }
        $expected = [$root->lineage[0]];
        foreach ($origin->lineage as $entry) {
            if ($entry['workflow_run_id'] === $root->rootWorkflowRunId) {
                continue;
            }
            $address = [
                'request_id' => $entry['request_id'],
                'workflow_instance_id' => $entry['workflow_instance_id'],
                'workflow_run_id' => $entry['workflow_run_id'],
            ];
            $index = count($expected) - 1;
            if ($expected[$index]['workflow_run_id'] === $entry['workflow_run_id']) {
                $expected[$index] = $address;
            } else {
                $expected[] = $address;
            }
        }
        $expected[] = $last;
        if ($lineage !== $expected) {
            throw new InvalidArgumentException('Run cancellation lineage discards or replaces its original scope ancestry.');
        }
    }

    /** @param array<string, mixed> $value */
    private static function text(array $value, string $key): string
    {
        $text = $value[$key] ?? null;
        if (!is_string($text) || trim($text) === '') {
            throw new InvalidArgumentException("Cancellation {$key} must be a non-empty string.");
        }

        return $text;
    }

    private static function timestamp(string $value): DateTimeImmutable
    {
        if (preg_match('/\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})\z/', $value) !== 1) {
            throw new InvalidArgumentException('Cancellation timestamps require an ISO date, time and timezone.');
        }
        try {
            $timestamp = new DateTimeImmutable($value);
        } catch (\Throwable $error) {
            throw new InvalidArgumentException('Cancellation timestamp is invalid.', previous: $error);
        }
        $errors = DateTimeImmutable::getLastErrors();
        if ($errors !== false && ($errors['warning_count'] !== 0 || $errors['error_count'] !== 0)) {
            throw new InvalidArgumentException('Cancellation timestamp is invalid.');
        }

        return $timestamp->setTimezone(new DateTimeZone('UTC'));
    }
}
